from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, List, Optional
from datetime import datetime
import json

from flask import Flask, jsonify, redirect, render_template, request, send_file, url_for, abort, g

from core.config import settings
from core.run_history_db import RunHistoryDb
from core.run_viewer import get_run_board, get_last_run_id, list_runs, search_templates
from core.board_layout import build_board_grid
from web.services import (
        build_index_context,
        get_run_item_progress_table,
        get_item_checklist,
        get_hero_list,
        )


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
    )

    # --- DB lifecycle -------------------------------------------------
    # One RunHistoryDb connection per request.
    def get_db() -> RunHistoryDb:
        db = g.get("run_history_db")
        if db is None:
            # Make schema exists even if user only runs the web UI
            db = RunHistoryDb(settings.run_history_db_path)
            g.run_history_db = db
        return db


    # One templates sqlite connection per request (avoid scattered sqlite3.connect calls in routes).
    def get_templates_conn() -> sqlite3.Connection:
        conn = g.get("templates_conn")
        if conn is None:
            conn = sqlite3.connect(settings.templates_db_path)
            conn.row_factory = sqlite3.Row
            g.templates_conn = conn
        return conn

    @app.teardown_appcontext
    def close_db(exception=None):
        db = getattr(g, "run_history_db", None)
        if db is not None:
            try:
                db.close()
            finally:
                g.run_history_db = None


        tconn = getattr(g, "templates_conn", None)
        if tconn is not None:
            try:
                tconn.close()
            finally:
                g.templates_conn = None

    def hero_colors_map() -> dict[str, str]:
        db = get_db()
        cur = db.conn.cursor()
        cur.execute("SELECT hero, color FROM hero_colors")
        return {r["hero"]: r["color"] for r in cur.fetchall() if r["hero"] and r["color"]}

    @app.template_filter("datetime_ymd")
    def datetime_ymd_filter(ts: int) -> str:
        try:
            return datetime.fromtimestamp(int(ts)).strftime("%Y/%m/%d")
        except Exception:
            return ""

    @app.get("/")
    def index():
        season_raw = (request.args.get("season") or "").strip()
    
        ctx = build_index_context(
            settings=settings,
            get_db=get_db,
            get_templates_conn=get_templates_conn,
            hero_colors_map=hero_colors_map,
            get_item_checklist=get_item_checklist,
            get_hero_list=get_hero_list,
            season_filter=season_raw,
        )
        return render_template("index.html", **ctx)

    @app.get("/runs")
    def runs_view():
        runs = list_runs(settings.run_history_db_path, limit=50)
        hero_colors = hero_colors_map()
        return render_template("runs_view.html", runs=runs, hero_colors=hero_colors)

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
        heroes = get_hero_list(settings.templates_db_path, conn=get_templates_conn())

        db = get_db()
        metrics = db.get_run_metrics(run_id)

        cur = db.conn.cursor()
        
        cur.execute(
            """
            SELECT a.key, a.title, a.description
            FROM achievement_unlocks u
            JOIN achievements a ON a.key = u.key
            WHERE u.run_id = ?
            ORDER BY a.title
            """,
            (run_id,),
        )
        
        achievements_unlocked = [dict(r) for r in cur.fetchall()]

        db = get_db()
        
        progress = get_run_item_progress_table(
            settings.templates_db_path,
            settings.run_history_db_path,
            run_id,
            hconn=db.conn,
            tconn=get_templates_conn(),
        )       

        return render_template(
            "run.html",
            run=run,
            grid=grid,
            edit_mode=edit_mode,
            heroes=heroes,
            metrics=metrics,
            progress=progress,
            achievements_unlocked=achievements_unlocked,
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
        conn = get_templates_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT image_path FROM templates WHERE template_id = ? AND COALESCE(ignored, 0) = 0",
            (template_id,),
        )
        row = cur.fetchone()
        if not row or not row["image_path"]:
            abort(404)

        path = row["image_path"]
        if not os.path.isabs(path):
            path = os.path.abspath(path)

        if not os.path.exists(path):
            abort(404)

        return send_file(path, mimetype="image/webp")


    @app.get("/items")
    def items_view():
        items = get_item_checklist(settings.templates_db_path, settings.run_history_db_path, tconn=get_templates_conn(), hconn=get_db().conn)
        hero_colors = hero_colors_map()
        return render_template("items_view.html", items=items, hero_colors=hero_colors)

    @app.context_processor
    def inject_hero_colors():
        return {"hero_colors": hero_colors_map()}

    @app.get("/heroes")
    def heroes_index():
        runs_all = list_runs(settings.run_history_db_path, limit=2000)
        hero_colors = hero_colors_map()
    
        season_raw = (request.args.get("season") or "").strip()
    
        season_options = sorted(
            {r.get("season_id") for r in runs_all if r.get("season_id") is not None},
            reverse=True,
        )
    
        if season_raw == "":
            season_selected = ""
            runs = runs_all
        elif season_raw == "__NONE__":
            season_selected = "__NONE__"
            runs = [r for r in runs_all if r.get("season_id") is None]
        else:
            try:
                season_value = int(season_raw)
                season_selected = str(season_value)
                runs = [r for r in runs_all if r.get("season_id") == season_value]
            except ValueError:
                season_selected = ""
                runs = runs_all
    
        # aggregate per hero
        stats: dict[str, dict[str, Any]] = {}
        for r in runs:
            hero = (r.get("hero_effective") or "(unknown)").strip() or "(unknown)"
            s = stats.setdefault(
                hero,
                {
                    "hero": hero,
                    "runs": 0,
                    "verified": 0,
                    "wins": 0,
                    "wins_vals": [],
                },
            )
            s["runs"] += 1
            if r.get("is_confirmed"):
                s["verified"] += 1
                if r.get("won"):
                    s["wins"] += 1
                if r.get("wins") is not None:
                    s["wins_vals"].append(int(r["wins"]))
        
        # compute winrate (over verified runs only, when possible)
        out = []
        for hero, s in stats.items():
            verified = int(s["verified"])
            wins = int(s["wins"])
            wins_vals = s.get("wins_vals") or []
            out.append(
                {
                    "hero": s["hero"],
                    "runs": s["runs"],
                    "wins": wins,
                    "winrate": (wins * 100 / verified) if verified else 0.0,
                    "avg_wins": (sum(wins_vals) / len(wins_vals)) if wins_vals else 0.0,
                    "color": hero_colors.get(hero),
                }
            )
    
        out.sort(key=lambda x: (-x["runs"], x["hero"].lower()))
        return render_template(
            "heroes.html",
            heroes=out,
            hero_colors=hero_colors,
            season_options=season_options,
            season_selected=season_selected,
        )
    
    
    @app.get("/heroes/<hero>")
    def hero_page(hero: str):
        hero = (hero or "").strip()
        if not hero:
            return redirect(url_for("heroes_index"))
    
        runs_all = list_runs(settings.run_history_db_path, limit=2000)
        hero_colors = hero_colors_map()
        color = hero_colors.get(hero)
    
        # all season options from all runs, so dropdown is stable
        season_options = sorted(
            {r.get("season_id") for r in runs_all if r.get("season_id") is not None},
            reverse=True,
        )
    
        season_raw = (request.args.get("season") or "").strip()
    
        # first filter by hero
        runs = [r for r in runs_all if (r.get("hero_effective") or "(unknown)") == hero]
    
        # then filter by season
        if season_raw == "":
            season_selected = ""
        elif season_raw == "__NONE__":
            season_selected = "__NONE__"
            runs = [r for r in runs if r.get("season_id") is None]
        else:
            try:
                season_value = int(season_raw)
                season_selected = str(season_value)
                runs = [r for r in runs if r.get("season_id") == season_value]
            except ValueError:
                season_selected = ""
    
        # Prefer verified runs for "real" stats
        verified = [r for r in runs if r.get("is_confirmed")]
        verified_count = len(verified)
    
        def outcome(r: dict) -> str:
            if r.get("won") is True:
                return "W"
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
            runs=runs,
            verified_count=verified_count,
            wins=wins,
            losses=losses,
            unknown=unknown,
            winrate=winrate,
            avg_wins=avg_wins,
            last10_str=last10_str,
            streaks={"current_type": cur_type, "current_len": cur_len, "best_win": best_win},
            hero_colors=hero_colors,
            season_options=season_options,
            season_selected=season_selected,
    )

    @app.get("/achievements")
    def achievements_view():
        db = get_db()
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
        db = get_db()
        db.confirm_run(run_id, confirmed=confirmed)

        # Rebuild achievements so edits/unconfirms stay consistent
        db.rebuild_item_hero_wins()
        db.rebuild_achievements(settings.templates_db_path)
        db.rebuild_item_firsts(settings.templates_db_path)

        return_edit = request.form.get("return_edit") == "1"
        if return_edit:
            return redirect(url_for("run_detail", run_id=run_id, edit=1))
        return redirect(url_for("run_detail", run_id=run_id))

    @app.post("/run/<int:run_id>/notes")
    def run_notes(run_id: int):
        notes = request.form.get("notes") or ""
        db = get_db()
        db.set_run_notes(run_id, notes)
        return_edit = request.form.get("return_edit") == "1"
        if return_edit:
            return redirect(url_for("run_detail", run_id=run_id, edit=1))
        return redirect(url_for("run_detail", run_id=run_id))

    @app.post("/run/<int:run_id>/hero")
    def run_hero_override(run_id: int):
        hero = (request.form.get("hero") or "").strip()
        db = get_db()
        if hero:
            db.set_run_hero_override(run_id, hero)
        else:
            # clearing hero override: store empty -> better: implement explicit clear method later
            db.upsert_run_override(run_id, hero_override="")

        # ✅ ensure achievements reflect edits
        db.rebuild_item_hero_wins()
        db.rebuild_achievements(settings.templates_db_path)
        db.rebuild_item_firsts(settings.templates_db_path)

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

        db = get_db()
        db.upsert_item_override(run_id, socket_number=socket, template_id_override=template_id)

        # ✅ ensure achievements reflect edits
        db.rebuild_item_hero_wins()
        db.rebuild_achievements(settings.templates_db_path)
        db.rebuild_item_firsts(settings.templates_db_path)

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

        db = get_db()
        db.clear_item_override(run_id, socket)

        # ✅ keep achievements consistent after edits
        db.rebuild_item_hero_wins()
        db.rebuild_achievements(settings.templates_db_path)
        db.rebuild_item_firsts(settings.templates_db_path)

        return_edit = request.form.get("return_edit") == "1"
        if return_edit:
            return redirect(url_for("run_detail", run_id=run_id, edit=1))
        return redirect(url_for("run_detail", run_id=run_id))
    return app

def get_hero_colors(run_history_db_path: str, conn: sqlite3.Connection | None = None) -> dict[str, str]:
    owns = conn is None
    if conn is None:
        conn = sqlite3.connect(run_history_db_path)
        conn.row_factory = sqlite3.Row

    try:
        cur = conn.cursor()
        cur.execute("SELECT hero, color FROM hero_colors")
        return {r["hero"]: r["color"] for r in cur.fetchall() if r["hero"] and r["color"]}
    finally:
        if owns:
            conn.close()
if __name__ == "__main__":
    app = create_app()
    # localhost-only by default (safe)
    app.run(host="127.0.0.1", port=5000, debug=True)
