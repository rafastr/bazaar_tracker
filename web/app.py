from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, List, Optional


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

    @app.get("/")
    def index():
        runs = list_runs(settings.run_history_db_path, limit=50)
        return render_template("index.html", runs=runs)

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
    
        return render_template(
            "run.html",
            run=run,
            grid=grid,
            edit_mode=edit_mode,
            heroes=heroes,
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

    # ----- Actions (POST) -----

    @app.post("/run/<int:run_id>/confirm")
    def run_confirm(run_id: int):
        confirmed = request.form.get("confirmed") == "1"
        db = _db()
        try:
            db.confirm_run(run_id, confirmed=confirmed)
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


if __name__ == "__main__":
    app = create_app()
    # localhost-only by default (safe)
    app.run(host="127.0.0.1", port=5000, debug=True)
