from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, List, Optional
from datetime import datetime

from flask import Flask, jsonify, redirect, render_template, request, send_file, url_for

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
        # --- item checklist stats (existing) ---
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

        hero_pie = [
            {"hero": h, "count": c}
            for h, c in sorted(hero_counts.items(), key=lambda kv: (-kv[1], kv[0].lower()))
        ]

        def outcome(r: dict) -> str:
            # if OCR not present yet, we may not know => ?
            if r.get("won") is True:
                return "W"
            if r.get("wins") is not None:
                return "L"
            return "?"

        last10 = runs[:10]
        last10_list = [{"ch": outcome(r)} for r in last10]
        last10_str = "".join(x["ch"] for x in last10_list)

        # Current streak (most recent contiguous W or L; stops on '?' or change)
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

        # Best win streak (contiguous W; '?' breaks)
        best_win = 0
        w_run = 0
        for r in runs:
            ch = outcome(r)
            if ch == "W":
                w_run += 1
                best_win = max(best_win, w_run)
            elif ch == "L" or ch == "?":
                w_run = 0

        streaks = {
            "current_type": cur_type,   # "W" / "L" / None
            "current_len": cur_len,
            "best_win": best_win,
        }

        return render_template(
            "index.html",
            overall=overall,
            group_stats=group_stats,
            hero_pie=hero_pie,
            last10_list=last10_list,
            last10_str=last10_str,
            streaks=streaks,
        )


    @app.get("/runs")
    def runs_view():
        runs = list_runs(settings.run_history_db_path, limit=50)
        return render_template("run_view.html", runs=runs)

    @app.get("/run/latest")
    def run_latest():
        rid = get_last_run_id(settings.run_history_db_path)
        if rid is None:
            return redirect(url_for("index"))
        return redirect(url_for("run_detail", run_id=rid))

    @app.get("/run/<int:run_id>")
    def run_detail(run_id: int):
        run = get_run_board(settings.run_history_db_path, settings.templates_db_path, run_id)
        grid = build_board_grid(run.get("items", []))
    
        edit_mode = request.args.get("edit") == "1"
        heroes = get_hero_list(settings.templates_db_path)
    
        # ✅ Fetch OCR metrics
        db = _db()
        try:
            metrics = db.get_run_metrics(run_id)
        finally:
            db.close()

        return render_template(
            "run.html",
            run=run,
            grid=grid,
            edit_mode=edit_mode,
            heroes=heroes,
            metrics=metrics,
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

    @app.get("/achievements/items")
    def items_view():
        items = get_item_checklist(settings.templates_db_path, settings.run_history_db_path)
        return render_template("items_view.html", items=items)

    # ----- Actions (POST) -----


    @app.post("/run/<int:run_id>/confirm")
    def run_confirm(run_id: int):
        confirmed = request.form.get("confirmed") == "1"
        db = _db()
        try:
            db.confirm_run(run_id, confirmed=confirmed)
    
            # Rebuild achievements so edits/unconfirms stay consistent
            db.rebuild_item_hero_wins()
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


if __name__ == "__main__":
    app = create_app()
    # localhost-only by default (safe)
    app.run(host="127.0.0.1", port=5000, debug=True)
