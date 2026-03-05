from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, List, Optional
from datetime import datetime

from flask import Flask, jsonify, redirect, render_template, request, send_file, url_for, abort

from core.config import settings
from core.run_history_db import RunHistoryDb
from core.run_viewer import get_run_board, get_last_run_id, list_runs, search_templates


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
    )

    def _db() -> RunHistoryDb:
        # Make schema exists even if user only runs the web UI
        return RunHistoryDb(settings.run_history_db_path)

    @app.template_filter("datetime_ymd")
    def datetime_ymd_filter(ts: int) -> str:
        try:
            return datetime.fromtimestamp(int(ts)).strftime("%Y/%m/%d")
        except Exception:
            return ""

    @app.get("/")
    def index():
        # --- item checklist stats ---
        items = get_item_checklist(settings.templates_db_path, settings.run_history_db_path)

        total = len(items)
        win_done = sum(1 for x in items if x.get("won_this"))
        other_done = sum(1 for x in items if x.get("won_other"))

        win_pct = (win_done * 100 / total) if total else 0.0
        other_pct = (other_done * 100 / total) if total else 0.0

        by_group: dict[str, dict[str, Any]] = {}
        for x in items:
            g = x.get("group") or "Common"
            s = by_group.setdefault(g, {"group": g, "total": 0, "win": 0, "other": 0})
            s["total"] += 1
            if x.get("won_this"):
                s["win"] += 1
            if x.get("won_other"):
                s["other"] += 1

        def gkey(name: str) -> tuple[int, str]:
            if name.strip().lower() == "common":
                return (1, "common")
            return (0, name.strip().lower())

        group_stats = []
        for g, s in sorted(by_group.items(), key=lambda kv: gkey(kv[0])):
            total_g = s["total"] or 0
            win_g = s["win"] or 0
            other_g = s["other"] or 0
            group_stats.append(
                {
                    "group": g,
                    "total": total_g,
                    "win": win_g,
                    "other": other_g,
                    "win_pct": (win_g * 100 / total_g) if total_g else 0.0,
                    "other_pct": (other_g * 100 / total_g) if total_g else 0.0,
                }
            )

        overall = {
            "total": total,
            "win": win_done,
            "other": other_done,
            "win_pct": win_pct,
            "other_pct": other_pct,
        }

        # --- hero pie chart + last-10 W/L + streaks ---
        runs = list_runs(settings.run_history_db_path, limit=200)

        hero_counts: dict[str, int] = {}
        for r in runs:
            hero = (r.get("hero_effective") or "(unknown)").strip() or "(unknown)"
            hero_counts[hero] = hero_counts.get(hero, 0) + 1

        # load hero colors for pie chart (so JS can draw with stable colors)
        conn = sqlite3.connect(settings.run_history_db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT hero, color FROM hero_colors")
        hero_colors = {r["hero"]: r["color"] for r in cur.fetchall()}
        conn.close()

        hero_pie = []
        for h, c in sorted(hero_counts.items(), key=lambda kv: (-kv[1], kv[0].lower())):
            hero_pie.append({"hero": h, "count": c, "color": hero_colors.get(h)})

        def outcome(r: dict) -> str:
            if r.get("won") is True:
                return "W"
            if r.get("wins") is not None:
                return "L"
            return "?"

        last10 = runs[:10]
        last10_list = [{"ch": outcome(r)} for r in last10]
        last10_str = "".join(x["ch"] for x in last10_list)

        cur_type: str | None = None
        cur_len = 0
        for r in runs:
            ch = outcome(r)
            if ch == "?":
                break
            if cur_type is None:
                cur_type = ch
                cur_len = 1
            elif ch == cur_type:
                cur_len += 1
            else:
                break

        best_win = 0
        w_run = 0
        for r in runs:
            ch = outcome(r)
            if ch == "W":
                w_run += 1
                best_win = max(best_win, w_run)
            elif ch == "L" or ch == "?":
                w_run = 0

        streaks = {"current_type": cur_type, "current_len": cur_len, "best_win": best_win}

        # --- hero stats table (FULL hero list, even with 0 runs) ---
        heroes = set(get_hero_list(settings.templates_db_path))  # excludes "common"
        heroes.update(get_hero_colors(settings.run_history_db_path).keys())
        heroes.add("(unknown)")
        hero_list = sorted(heroes, key=lambda x: (x == "(unknown)", x.lower()))

        db = _db()
        try:
            cur = db.conn.cursor()

            cur.execute(
                """
                SELECT
                  hero,
                  SUM(cnt) AS runs,
                  SUM(wins) AS wins,
                  SUM(losses) AS losses,
                  SUM(unknowns) AS unknowns,
                  AVG(avg_wins) AS avg_wins
                FROM (
                  SELECT
                    COALESCE(o.hero_override, r.hero, '(unknown)') AS hero,
                    1 AS cnt,
                    CASE WHEN COALESCE(m.won, 0) = 1 THEN 1 ELSE 0 END AS wins,
                    CASE WHEN COALESCE(m.won, 0) = 0 AND m.wins IS NOT NULL THEN 1 ELSE 0 END AS losses,
                    CASE WHEN m.wins IS NULL THEN 1 ELSE 0 END AS unknowns,
                    CASE WHEN m.won = 1 AND m.wins IS NOT NULL THEN m.wins ELSE NULL END AS avg_wins
                  FROM runs r
                  LEFT JOIN run_overrides o ON o.run_id = r.run_id
                  LEFT JOIN run_metrics  m ON m.run_id = r.run_id
                  WHERE COALESCE(o.is_confirmed, 0) = 1
                )
                GROUP BY hero
                """
            )
            rows = [dict(r) for r in cur.fetchall()]

            stats_by_hero: dict[str, dict] = {}
            for r in rows:
                hero = (r.get("hero") or "(unknown)").strip() or "(unknown)"
                w = int(r.get("wins") or 0)
                l = int(r.get("losses") or 0)
                u = int(r.get("unknowns") or 0)
                runs_n = int(r.get("runs") or 0)
                denom = w + l  # ignore unknowns

                stats_by_hero[hero] = {
                    "hero": hero,
                    "runs": runs_n,
                    "wins": w,
                    "losses": l,
                    "unknowns": u,
                    "winrate": (w * 100 / denom) if denom else 0.0,
                    "avg_wins": float(r.get("avg_wins") or 0.0),
                }

            hero_stats = []
            for hero in hero_list:
                hero_stats.append(
                    stats_by_hero.get(
                        hero,
                        {"hero": hero, "runs": 0, "wins": 0, "losses": 0, "unknowns": 0, "winrate": 0.0, "avg_wins": 0.0},
                    )
                )

            # --- achievements list for dashboard ---
            cur.execute(
                """
                SELECT
                  a.key,
                  a.title,
                  a.description,
                  u.unlocked_at_unix,
                  u.run_id
                FROM achievements a
                LEFT JOIN achievement_unlocks u ON u.key = a.key
                ORDER BY
                  CASE WHEN u.unlocked_at_unix IS NOT NULL THEN 0 ELSE 1 END,
                  u.unlocked_at_unix DESC,
                  a.title COLLATE NOCASE ASC
                """
            )
            ach_rows = [dict(r) for r in cur.fetchall()]

        finally:
            db.close()

        ach_unlocked = sum(1 for r in ach_rows if r.get("unlocked_at_unix"))
        ach_total = len(ach_rows)

        # keep dashboard tidy: show first 12 (unlocked first due to ORDER BY)
        achievements = ach_rows[:]

        return render_template(
            "index.html",
            overall=overall,
            group_stats=group_stats,
            hero_pie=hero_pie,
            last10_list=last10_list,
            last10_str=last10_str,
            streaks=streaks,
            hero_stats=hero_stats,
            achievements=achievements,
            ach_unlocked=ach_unlocked,
            ach_total=ach_total,
        )

    @app.get("/runs")
    def runs_view():
        runs = list_runs(settings.run_history_db_path, limit=50)
        hero_colors = get_hero_colors(settings.run_history_db_path)
        return render_template("runs_view.html", runs=runs, hero_colors=hero_colors)

    @app.get("/run/latest")
    def run_latest():
        rid = get_last_run_id(settings.run_history_db_path)
        if rid is None:
            return redirect(url_for("index"))
        return redirect(url_for("run_detail", run_id=rid))

    @app.get("/run/<int:run_id>")
    def run_detail(run_id: int):
        import json
        import sqlite3
    
        run = get_run_board(settings.run_history_db_path, settings.templates_db_path, run_id)
        grid = build_board_grid(run.get("items", []))
    
        edit_mode = request.args.get("edit") == "1"
        heroes = get_hero_list(settings.templates_db_path)
    
        db = _db()
        try:
            metrics = db.get_run_metrics(run_id)
    
            # -----------------------------
            # Build "progress from this run"
            # -----------------------------
            progress = {
                "confirmed": bool(run.get("is_confirmed")),
                "won": bool(metrics and metrics.get("won")),
                "rows": [],
            }
    
            # Only compute item progress for verified wins
            if progress["confirmed"] and progress["won"]:
                cur = db.conn.cursor()
    
                # effective hero for THIS run
                hero_eff = (run.get("hero_effective") or "").strip() or "(unknown)"
    
                # load templates: name + origins + is_common
                tconn = sqlite3.connect(settings.templates_db_path)
                tconn.row_factory = sqlite3.Row
                try:
                    tcur = tconn.cursor()
                    tcur.execute("SELECT template_id, name, heroes_json FROM templates WHERE template_id IS NOT NULL")
                    trows = tcur.fetchall()
                finally:
                    tconn.close()
    
                def parse_origin_set(heroes_json: str) -> set[str]:
                    s = (heroes_json or "").strip()
                    if not s:
                        return set()
                    try:
                        data = json.loads(s)
                    except Exception:
                        return set()
                    if isinstance(data, list):
                        vals = data
                    elif isinstance(data, dict):
                        vals = data.get("heroes", [])
                    else:
                        vals = [data]
                    out: set[str] = set()
                    for v in vals:
                        if isinstance(v, str):
                            name = v.strip()
                            if name:
                                out.add(name)
                    return out
    
                origin_by_tid: dict[str, set[str]] = {}
                is_common_tid: dict[str, bool] = {}
                name_by_tid: dict[str, str] = {}
    
                for r in trows:
                    tid = (r["template_id"] or "").strip()
                    if not tid:
                        continue
                    origins = parse_origin_set(r["heroes_json"] or "")
                    origin_by_tid[tid] = origins
                    is_common_tid[tid] = any(h.lower() == "common" for h in origins) or (not origins)
                    name_by_tid[tid] = (r["name"] or "").strip() or tid
    
                # effective template_ids used in THIS run (apply overrides)
                cur.execute("SELECT socket_number, template_id FROM run_items WHERE run_id=?", (run_id,))
                base = {int(r["socket_number"]): (r["template_id"] or "").strip() for r in cur.fetchall()}
    
                cur.execute("SELECT socket_number, template_id_override FROM run_item_overrides WHERE run_id=?", (run_id,))
                ov = {int(r["socket_number"]): r["template_id_override"] for r in cur.fetchall()}
    
                cur_tids: set[str] = set()
                for sock, tid in base.items():
                    if sock in ov and ov[sock] is not None:
                        tid_eff = (ov[sock] or "").strip()
                    else:
                        tid_eff = tid
                    if tid_eff:
                        cur_tids.add(tid_eff)
    
                # Build "already had before this run" sets by scanning confirmed+won runs with run_id < this run
                prior_any: set[str] = set()
                prior_cross: set[str] = set()
    
                cur.execute(
                    """
                    SELECT
                      r.run_id,
                      COALESCE(o.hero_override, r.hero, '(unknown)') AS hero_eff
                    FROM runs r
                    LEFT JOIN run_overrides o ON o.run_id = r.run_id
                    LEFT JOIN run_metrics  m ON m.run_id = r.run_id
                    WHERE COALESCE(o.is_confirmed, 0) = 1
                      AND COALESCE(m.won, 0) = 1
                      AND r.run_id < ?
                    ORDER BY r.run_id ASC
                    """,
                    (run_id,),
                )
                prior_runs = [(int(r["run_id"]), (r["hero_eff"] or "(unknown)")) for r in cur.fetchall()]
    
                for rid, h_prev in prior_runs:
                    # base
                    cur.execute("SELECT socket_number, template_id FROM run_items WHERE run_id=?", (rid,))
                    b2 = {int(r["socket_number"]): (r["template_id"] or "").strip() for r in cur.fetchall()}
    
                    # overrides
                    cur.execute("SELECT socket_number, template_id_override FROM run_item_overrides WHERE run_id=?", (rid,))
                    o2 = {int(r["socket_number"]): r["template_id_override"] for r in cur.fetchall()}
    
                    tids_prev: set[str] = set()
                    for sock, tid in b2.items():
                        if sock in o2 and o2[sock] is not None:
                            tid_eff = (o2[sock] or "").strip()
                        else:
                            tid_eff = tid
                        if tid_eff:
                            tids_prev.add(tid_eff)
    
                    for tid in tids_prev:
                        prior_any.add(tid)
    
                        if is_common_tid.get(tid, False):
                            prior_cross.add(tid)
                        else:
                            origins = origin_by_tid.get(tid, set())
                            if h_prev not in origins:
                                prior_cross.add(tid)
    
                # Build rows ONLY for items that get newly marked by THIS run
                rows = []
                for tid in sorted(cur_tids, key=lambda x: name_by_tid.get(x, x).lower()):
                    name = name_by_tid.get(tid, tid)
    
                    qualifies_cross = False
                    if is_common_tid.get(tid, False):
                        qualifies_cross = True
                    else:
                        origins = origin_by_tid.get(tid, set())
                        qualifies_cross = (hero_eff not in origins)
    
                    new_won_this = tid not in prior_any
                    new_won_other = qualifies_cross and (tid not in prior_cross)
    
                    # hide items that weren't marked this run
                    if not (new_won_this or new_won_other):
                        continue
    
                    won_this_after = True  # because this run is a verified win and used this item
                    won_other_after = (tid in prior_cross) or qualifies_cross
    
                    rows.append(
                        {
                            "template_id": tid,
                            "name": name,
                            "won_this": won_this_after,
                            "won_other": bool(won_other_after),
                            "new_won_this": bool(new_won_this),
                            "new_won_other": bool(new_won_other),
                        }
                    )
    
                progress["rows"] = rows
    
        finally:
            db.close()
    
        return render_template(
            "run.html",
            run=run,
            grid=grid,
            edit_mode=edit_mode,
            heroes=heroes,
            metrics=metrics,
            progress=progress,
    )



    @app.get("/screenshot/<int:run_id>")
    def screenshot(run_id: int):
        # Serve screenshot_path from the runs table
        run = get_run_board(settings.run_history_db_path, settings.templates_db_path, run_id)
        path = run.get("screenshot_path")
        if not path:
            return ("No screenshot_path for this run", 404)

        # If relative, interpret relative to project root (same as tracker)
        if not os.path.isabs(path):
            path = os.path.abspath(path)

        if not os.path.exists(path):
            return (f"Screenshot not found: {path}", 404)

        return send_file(path, mimetype="image/png")

    @app.get("/api/templates")
    def api_templates():
        q = (request.args.get("q") or "").strip()
        if not q:
            return jsonify([])
    
        size = (request.args.get("size") or "").strip().lower()
        if size not in ("small", "medium", "large"):
            size = ""
    
        rows = search_templates(settings.templates_db_path, q, limit=6, size=size)
        out = [{"template_id": r["template_id"], "name": r["name"], "size": r.get("size")} for r in rows]
        return jsonify(out)

    @app.get("/item-image/<template_id>")
    def item_image(template_id: str):
        conn = sqlite3.connect(settings.templates_db_path)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            cur.execute("SELECT image_path FROM templates WHERE template_id = ?", (template_id,))
            row = cur.fetchone()
            if not row or not row["image_path"]:
                abort(404)
    
            path = row["image_path"]
            if not os.path.isabs(path):
                path = os.path.abspath(path)
    
            if not os.path.exists(path):
                abort(404)
    
            # images are webp in your cache
            return send_file(path, mimetype="image/webp")
        finally:
            conn.close()

    @app.get("/items")
    def items_view():
        items = get_item_checklist(settings.templates_db_path, settings.run_history_db_path)
        hero_colors = get_hero_colors(settings.run_history_db_path)
        return render_template("items_view.html", items=items, hero_colors=hero_colors)

    @app.context_processor
    def inject_hero_colors():
        return {"hero_colors": get_hero_colors(settings.run_history_db_path)}

    @app.get("/heroes")
    def heroes_index():
        runs = list_runs(settings.run_history_db_path, limit=2000)
        hero_colors = get_hero_colors(settings.run_history_db_path)

        # aggregate per hero
        stats: dict[str, dict[str, Any]] = {}
        for r in runs:
            hero = (r.get("hero_effective") or "(unknown)").strip() or "(unknown)"
            s = stats.setdefault(hero, {"hero": hero, "runs": 0, "verified": 0, "wins": 0})
            s["runs"] += 1
            if r.get("is_confirmed"):
                s["verified"] += 1
            if r.get("won"):
                s["wins"] += 1

        # compute winrate (over verified runs only, when possible)
        out = []
        for hero, s in stats.items():
            verified = int(s["verified"])
            wins = int(s["wins"])
            out.append(
                {
                    **s,
                    "winrate": (wins * 100 / verified) if verified else 0.0,
                    "color": hero_colors.get(hero),
                }
            )

        out.sort(key=lambda x: (-x["runs"], x["hero"].lower()))
        return render_template("heroes.html", heroes=out, hero_colors=hero_colors)


    @app.get("/heroes/<hero>")
    def hero_page(hero: str):
        hero = (hero or "").strip()
        if not hero:
            return redirect(url_for("heroes_index"))

        runs_all = list_runs(settings.run_history_db_path, limit=2000)
        runs = [r for r in runs_all if (r.get("hero_effective") or "(unknown)") == hero]

        hero_colors = get_hero_colors(settings.run_history_db_path)
        color = hero_colors.get(hero)

        # Prefer verified runs for "real" stats
        verified = [r for r in runs if r.get("is_confirmed")]
        verified_count = len(verified)

        def outcome(r: dict) -> str:
            if r.get("won") is True:
                return "W"
            # if OCR existed but not won
            if r.get("wins") is not None:
                return "L"
            return "?"

        wins = sum(1 for r in verified if outcome(r) == "W")
        losses = sum(1 for r in verified if outcome(r) == "L")
        unknown = sum(1 for r in verified if outcome(r) == "?")

        winrate = (wins * 100 / verified_count) if verified_count else 0.0

        # last 10 (verified only, newest first)
        last10 = verified[:10]
        last10_str = "".join(outcome(r) for r in last10)

        # current streak (verified only)
        cur_type = None
        cur_len = 0
        for r in verified:
            ch = outcome(r)
            if ch == "?":
                break
            if cur_type is None:
                cur_type = ch
                cur_len = 1
            elif ch == cur_type:
                cur_len += 1
            else:
                break

        # best win streak (verified only)
        best_win = 0
        w_run = 0
        for r in verified:
            ch = outcome(r)
            if ch == "W":
                w_run += 1
                best_win = max(best_win, w_run)
            elif ch in ("L", "?"):
                w_run = 0

        # avg wins (only runs with wins present)
        wins_vals = [int(r["wins"]) for r in verified if r.get("wins") is not None]
        avg_wins = (sum(wins_vals) / len(wins_vals)) if wins_vals else 0.0

        return render_template(
            "hero.html",
            hero=hero,
            color=color,
            runs=runs,                 # show all runs for browsing
            verified_count=verified_count,
            wins=wins,
            losses=losses,
            unknown=unknown,
            winrate=winrate,
            avg_wins=avg_wins,
            last10_str=last10_str,
            streaks={"current_type": cur_type, "current_len": cur_len, "best_win": best_win},
            hero_colors=hero_colors,
        )

    @app.get("/achievements")
    def achievements_view():
        db = _db()
        try:
            cur = db.conn.cursor()
            cur.execute(
                """
                SELECT
                  a.key,
                  a.title,
                  a.description,
                  u.unlocked_at_unix,
                  u.run_id
                FROM achievements a
                LEFT JOIN achievement_unlocks u ON u.key = a.key
                ORDER BY
                  CASE WHEN u.unlocked_at_unix IS NOT NULL THEN 0 ELSE 1 END,
                  u.unlocked_at_unix DESC,
                  a.title COLLATE NOCASE ASC
                """
            )
            rows = [dict(r) for r in cur.fetchall()]
        finally:
            db.close()
    
        unlocked = sum(1 for r in rows if r.get("unlocked_at_unix") is not None)
        total = len(rows)
    
        return render_template(
            "achievements.html",
            achievements=rows,
            unlocked=unlocked,
            total=total,
        )

    # ----- Actions (POST) -----


    @app.post("/run/<int:run_id>/confirm")
    def run_confirm(run_id: int):
        confirmed = request.form.get("confirmed") == "1"
        db = _db()
        try:
            db.confirm_run(run_id, confirmed=confirmed)
    
            # Rebuild achievements so edits/unconfirms stay consistent
            db.rebuild_item_hero_wins()
            db.rebuild_achievements(settings.templates_db_path)
            db.rebuild_item_firsts(settings.templates_db_path)
        finally:
            db.close()
    
        return_edit = request.form.get("return_edit") == "1"
        if return_edit:
            return redirect(url_for("run_detail", run_id=run_id, edit=1))
        return redirect(url_for("run_detail", run_id=run_id))

    @app.post("/run/<int:run_id>/notes")
    def run_notes(run_id: int):
        notes = request.form.get("notes") or ""
        db = _db()
        try:
            db.set_run_notes(run_id, notes)
        finally:
            db.close()
        return_edit = request.form.get("return_edit") == "1"
        if return_edit:
            return redirect(url_for("run_detail", run_id=run_id, edit=1))
        return redirect(url_for("run_detail", run_id=run_id))

    @app.post("/run/<int:run_id>/hero")
    def run_hero_override(run_id: int):
        hero = (request.form.get("hero") or "").strip()
        db = _db()
        try:
            if hero:
                db.set_run_hero_override(run_id, hero)
            else:
                # clearing hero override: store empty -> better: implement explicit clear method later
                db.upsert_run_override(run_id, hero_override="")
    
            # ✅ ensure achievements reflect edits
            db.rebuild_item_hero_wins()
            db.rebuild_achievements(settings.templates_db_path)
            db.rebuild_item_firsts(settings.templates_db_path)
        finally:
            db.close()
    
        return_edit = request.form.get("return_edit") == "1"
        if return_edit:
            return redirect(url_for("run_detail", run_id=run_id, edit=1))
        return redirect(url_for("run_detail", run_id=run_id))
    
    @app.post("/run/<int:run_id>/item/set")
    def item_set(run_id: int):
        socket_s = request.form.get("socket") or ""
        template_id = (request.form.get("template_id") or "").strip()
    
        try:
            socket = int(socket_s)
        except ValueError:
            return ("Invalid socket", 400)
    
        if socket < 0 or socket > 9:
            return ("Invalid socket (0-9)", 400)
    
        db = _db()
        try:
            db.upsert_item_override(run_id, socket_number=socket, template_id_override=template_id)
    
            # ✅ ensure achievements reflect edits
            db.rebuild_item_hero_wins()
            db.rebuild_achievements(settings.templates_db_path)
            db.rebuild_item_firsts(settings.templates_db_path)
        finally:
            db.close()
    
        return_edit = request.form.get("return_edit") == "1"
        if return_edit:
            return redirect(url_for("run_detail", run_id=run_id, edit=1))
        return redirect(url_for("run_detail", run_id=run_id))

    @app.post("/run/<int:run_id>/item/clear")
    def item_clear(run_id: int):
        socket_s = request.form.get("socket") or ""
        try:
            socket = int(socket_s)
        except ValueError:
            return ("Invalid socket", 400)
    
        db = _db()
        try:
            db.clear_item_override(run_id, socket)
    
            # ✅ keep achievements consistent after edits
            db.rebuild_item_hero_wins()
            db.rebuild_achievements(settings.templates_db_path)
            db.rebuild_item_firsts(settings.templates_db_path)
        finally:
            db.close()
    
        return_edit = request.form.get("return_edit") == "1"
        if return_edit:
            return redirect(url_for("run_detail", run_id=run_id, edit=1))
        return redirect(url_for("run_detail", run_id=run_id))
    return app

def size_to_span(size: Optional[str]) -> int:
    if not size:
        return 1
    s = size.lower()
    if s == "medium":
        return 2
    if s == "large":
        return 3
    return 1


def build_board_grid(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Build a 10-slot visual grid.

    - Items span 1/2/3 sockets based on size.
    - If an item has template_id missing but size is present, it still spans (shows empty placeholder).
    - Any remaining sockets become explicit empty 1-span blocks, so the UI always shows all sockets.
    """
    items_sorted = sorted(items, key=lambda x: int(x.get("socket_number", 999)))

    occupied = [False] * 10
    blocks: List[Dict[str, Any]] = []

    def occupy(start: int, span: int) -> None:
        for s in range(start, start + span):
            if 0 <= s < 10:
                occupied[s] = True

    # 1) Place known item blocks
    for it in items_sorted:
        start = int(it.get("socket_number", 999))
        if start < 0 or start > 9:
            continue
        if occupied[start]:
            continue

        span = size_to_span(it.get("size"))
        span = max(1, min(span, 10 - start))

        # If any of the target cells already occupied, shrink span to fit the first free stretch
        # (shouldn't happen normally, but avoids visual overlap)
        while span > 1 and any(occupied[s] for s in range(start, start + span)):
            span -= 1
        if any(occupied[s] for s in range(start, start + span)):
            continue

        occupy(start, span)

        blocks.append(
            {
                "start": start,
                "span": span,
                "name": it.get("name") or "(unknown item)",
                "size": it.get("size") or "small",
                "template_id": it.get("template_id"),  # may be None
                "socket_number": start,  # edit/clear uses the first socket
            }
        )

    # 2) Fill remaining sockets with explicit empties (span=1)
    for s in range(10):
        if not occupied[s]:
            blocks.append(
                {
                    "start": s,
                    "span": 1,
                    "name": "(empty)",
                    "size": "small",
                    "template_id": None,
                    "socket_number": s,
                }
            )

    # Return blocks sorted by socket
    return sorted(blocks, key=lambda b: b["start"])


def get_hero_list(templates_db_path: str) -> list[str]:
    import sqlite3, json

    conn = sqlite3.connect(templates_db_path)
    conn.row_factory = sqlite3.Row
    heroes: set[str] = set()

    try:
        cur = conn.cursor()
        cur.execute("SELECT heroes_json FROM templates WHERE heroes_json IS NOT NULL AND TRIM(heroes_json) <> ''")
        for row in cur.fetchall():
            s = row["heroes_json"]
            if not s:
                continue
            try:
                data = json.loads(s)
            except Exception:
                continue

            # Support a few shapes safely:
            # - ["Vanessa","Dooley"]
            # - {"heroes":["Vanessa","Dooley"]}
            # - "Vanessa"  (unlikely, but harmless)
            if isinstance(data, list):
                values = data
            elif isinstance(data, dict):
                values = data.get("heroes", [])
            else:
                values = [data]

            for h in values:
                if not isinstance(h, str):
                    continue
                name = h.strip()
                if not name:
                    continue
                if name.lower() == "common":
                    continue
                heroes.add(name)

        return sorted(heroes, key=lambda x: x.lower())
    finally:
        conn.close()

def _parse_origin_set(heroes_json: str) -> set[str]:
    import json
    s = (heroes_json or "").strip()
    if not s:
        return set()
    try:
        data = json.loads(s)
    except Exception:
        return set()
    if isinstance(data, list):
        vals = data
    elif isinstance(data, dict):
        vals = data.get("heroes", [])
    else:
        vals = [data]
    out: set[str] = set()
    for v in vals:
        if isinstance(v, str):
            name = v.strip()
            if name:
                out.add(name)
    return out


def get_run_item_progress_table(run_id: int, templates_db_path: str, run_history_db_path: str) -> dict:
    """
    Returns:
      {
        "hero_eff": str,
        "won": bool,
        "confirmed": bool,
        "rows": [
          {
            "template_id": str,
            "name": str,
            "size": str,
            "won_this": bool,
            "won_other": bool,
            "new_won_this": bool,   # first time ever won (any hero)
            "new_won_other": bool,  # first time ever won-as-cross (your 'other hero' rule)
          }, ...
        ]
      }
    """
    import sqlite3

    # --- load run hero/flags ---
    hconn = sqlite3.connect(run_history_db_path)
    hconn.row_factory = sqlite3.Row
    try:
        cur = hconn.cursor()
        cur.execute(
            """
            SELECT
              r.hero AS hero_base,
              o.hero_override,
              COALESCE(o.is_confirmed, 0) AS is_confirmed,
              COALESCE(m.won, 0) AS won
            FROM runs r
            LEFT JOIN run_overrides o ON o.run_id = r.run_id
            LEFT JOIN run_metrics  m ON m.run_id = r.run_id
            WHERE r.run_id=?
            """,
            (int(run_id),),
        )
        rr = cur.fetchone()
        if not rr:
            return {"hero_eff": "(unknown)", "won": False, "confirmed": False, "rows": []}

        hero_eff = (rr["hero_override"] or rr["hero_base"] or "(unknown)").strip() or "(unknown)"
        confirmed = int(rr["is_confirmed"] or 0) == 1
        won = int(rr["won"] or 0) == 1

        # --- effective items for this run (tid + size) ---
        cur.execute(
            "SELECT socket_number, template_id, size FROM run_items WHERE run_id=?",
            (int(run_id),),
        )
        base = {int(r["socket_number"]): (r["template_id"] or "").strip() for r in cur.fetchall()}

        cur.execute(
            "SELECT socket_number, template_id_override FROM run_item_overrides WHERE run_id=?",
            (int(run_id),),
        )
        ov = {int(r["socket_number"]): r["template_id_override"] for r in cur.fetchall()}

        tids: list[str] = []
        for sock, tid in base.items():
            if sock in ov and ov[sock] is not None:
                tid_eff = (ov[sock] or "").strip()
            else:
                tid_eff = tid
            if tid_eff:
                tids.append(tid_eff)

        # de-dup for checklist display
        tids = sorted(set(tids))
        if not tids:
            return {"hero_eff": hero_eff, "won": won, "confirmed": confirmed, "rows": []}

        # --- load template names + origins ---
        tconn = sqlite3.connect(templates_db_path)
        tconn.row_factory = sqlite3.Row
        try:
            tcur = tconn.cursor()
            qmarks = ",".join("?" for _ in tids)
            tcur.execute(
                f"SELECT template_id, name, heroes_json, size FROM templates WHERE template_id IN ({qmarks})",
                tuple(tids),
            )
            trows = {r["template_id"]: dict(r) for r in tcur.fetchall()}
        finally:
            tconn.close()

        origin_by_tid: dict[str, set[str]] = {}
        is_common_tid: dict[str, bool] = {}
        for tid, tr in trows.items():
            origins = _parse_origin_set(tr.get("heroes_json") or "")
            origin_by_tid[tid] = origins
            is_common_tid[tid] = any(h.lower() == "common" for h in origins) or (not origins)

        # --- current checklist state (after rebuilds) ---
        cur.execute(
            """
            SELECT template_id, hero
            FROM item_hero_wins
            WHERE win_count > 0
            """
        )
        winners_by_item: dict[str, set[str]] = {}
        for r in cur.fetchall():
            tid = (r["template_id"] or "").strip()
            h = (r["hero"] or "").strip()
            if tid and h:
                winners_by_item.setdefault(tid, set()).add(h)

        # --- compute "new in this run" by checking earlier confirmed+won runs ---
        # Any win before this run for that item?
        # Any cross-hero win before this run for that item?
        # (cross-hero is relative to the hero that used it in that earlier run)
        cur.execute(
            """
            SELECT
              r.run_id,
              COALESCE(o.hero_override, r.hero, '(unknown)') AS hero_eff
            FROM runs r
            LEFT JOIN run_overrides o ON o.run_id = r.run_id
            LEFT JOIN run_metrics  m ON m.run_id = r.run_id
            WHERE COALESCE(o.is_confirmed, 0)=1
              AND COALESCE(m.won, 0)=1
              AND r.run_id < ?
            """,
            (int(run_id),),
        )
        earlier_wins = cur.fetchall()

        earlier_any: set[str] = set()
        earlier_cross: set[str] = set()

        # for speed: pre-load earlier run items in one pass
        for er in earlier_wins:
            erid = int(er["run_id"])
            ehero = (er["hero_eff"] or "(unknown)").strip() or "(unknown)"

            cur.execute("SELECT socket_number, template_id FROM run_items WHERE run_id=?", (erid,))
            b2 = {int(r["socket_number"]): (r["template_id"] or "").strip() for r in cur.fetchall()}

            cur.execute("SELECT socket_number, template_id_override FROM run_item_overrides WHERE run_id=?", (erid,))
            o2 = {int(r["socket_number"]): r["template_id_override"] for r in cur.fetchall()}

            etids: set[str] = set()
            for sock, tid in b2.items():
                if sock in o2 and o2[sock] is not None:
                    tid_eff = (o2[sock] or "").strip()
                else:
                    tid_eff = tid
                if tid_eff:
                    etids.add(tid_eff)

            for tid in etids:
                earlier_any.add(tid)

                if is_common_tid.get(tid, False):
                    # your rule: common counts as "other hero" automatically
                    earlier_cross.add(tid)
                else:
                    origins = origin_by_tid.get(tid, set())
                    if ehero not in origins:
                        earlier_cross.add(tid)

        # --- build table rows ---
        out_rows: list[dict] = []
        for tid in tids:
            tr = trows.get(tid, {})
            name = tr.get("name") or "(unknown item)"
            size = (tr.get("size") or "").strip().lower()

            winners = winners_by_item.get(tid, set())
            won_this = bool(winners)

            common = is_common_tid.get(tid, False)
            origins = origin_by_tid.get(tid, set())

            if not won_this:
                won_other = False
            elif common:
                won_other = True
            else:
                won_other = any(h not in origins for h in winners)

            new_won_this = (tid not in earlier_any) and confirmed and won
            new_won_other = (tid not in earlier_cross) and won_other and confirmed and won

            out_rows.append(
                {
                    "template_id": tid,
                    "name": name,
                    "size": size,
                    "won_this": won_this,
                    "won_other": won_other,
                    "new_won_this": new_won_this,
                    "new_won_other": new_won_other,
                }
            )

        # keep stable: alphabetical by name (like your sections)
        out_rows.sort(key=lambda x: (x["name"] or "").lower())
        return {"hero_eff": hero_eff, "won": won, "confirmed": confirmed, "rows": out_rows}

    finally:
        hconn.close()


def get_item_checklist(templates_db_path: str, run_history_db_path: str) -> list[dict]:
    import sqlite3, json

    def parse_origin_heroes(heroes_json: str) -> set[str]:
        """
        Returns a set of origin heroes from heroes_json.
        Keeps "common" if present (special meaning).
        """
        s = (heroes_json or "").strip()
        if not s:
            return set()

        try:
            data = json.loads(s)
        except Exception:
            return set()

        if isinstance(data, list):
            vals = data
        elif isinstance(data, dict):
            vals = data.get("heroes", [])
        else:
            vals = [data]

        out: set[str] = set()
        for v in vals:
            if not isinstance(v, str):
                continue
            name = v.strip()
            if not name:
                continue
            out.add(name)
        return out

    # 1) all templates
    tconn = sqlite3.connect(templates_db_path)
    tconn.row_factory = sqlite3.Row
    try:
        tcur = tconn.cursor()
        tcur.execute("SELECT template_id, name, heroes_json, size FROM templates WHERE template_id IS NOT NULL")
        templates = [dict(r) for r in tcur.fetchall()]
    finally:
        tconn.close()

    # 2) all wins from run history (per template_id, hero)
    hconn = sqlite3.connect(run_history_db_path)
    hconn.row_factory = sqlite3.Row
    try:
        hcur = hconn.cursor()
        hcur.execute(
            """
            SELECT template_id, hero
            FROM item_hero_wins
            WHERE win_count > 0
            """
        )
        rows = hcur.fetchall()
    finally:
        hconn.close()

    # Build: template_id -> set(heroes_that_won_with_it)
    winners_by_item: dict[str, set[str]] = {}
    for r in rows:
        tid = (r["template_id"] or "").strip()
        hero = (r["hero"] or "").strip()
        if not tid or not hero:
            continue
        winners_by_item.setdefault(tid, set()).add(hero)

    # 3) merge
    out: list[dict] = []
    for t in templates:
        tid = (t.get("template_id") or "").strip()
        if not tid:
            continue

        name = t.get("name") or ""
        origin_heroes = parse_origin_heroes(t.get("heroes_json") or "")
        winners = winners_by_item.get(tid, set())

        # ✅ win = used in any won+verified run (any hero)
        won_any = bool(winners)

        # display origin (exclude Common from display)
        origin_no_common = sorted(
            [h for h in origin_heroes if h.strip().lower() != "common"],
            key=str.lower,
        )
        origin_display = ", ".join(origin_no_common)

        # group (for sections)
        is_common = any(h.lower() == "common" for h in origin_heroes) or (not origin_heroes)
        if is_common or not origin_no_common:
            group = "Common"
        elif len(origin_no_common) == 1:
            group = origin_no_common[0]
        else:
            # future-proof: multi-hero items
            group = " / ".join(origin_no_common)

        # ✅ win_other:
        # - if item is Common => any win counts as "win with another hero"
        # - else => true if there exists a winner hero NOT in origin heroes
        if not won_any:
            won_other = False
        elif is_common:
            won_other = True
        else:
            won_other = any(h not in origin_heroes for h in winners)

        out.append(
            {
                "template_id": tid,
                "name": name,
                "size": (t.get("size") or "").strip().lower(),
                "origin": origin_display,
                "group": group,
                "won_this": won_any,
                "won_other": won_other,
            }
        )

    def group_key(g: str) -> tuple[int, str]:
        # Put Common at the end
        if (g or "").strip().lower() == "common":
            return (1, "common")
        return (0, (g or "").strip().lower())

    # sort: group, then name
    out.sort(key=lambda r: (group_key(r["group"]), r["name"].lower()))
    return out


def get_hero_colors(run_history_db_path: str) -> dict[str, str]:
    import sqlite3
    conn = sqlite3.connect(run_history_db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute("SELECT hero, color FROM hero_colors")
        return {r["hero"]: r["color"] for r in cur.fetchall() if r["hero"] and r["color"]}
    finally:
        conn.close()


if __name__ == "__main__":
    app = create_app()
    # localhost-only by default (safe)
    app.run(host="127.0.0.1", port=5000, debug=True)
