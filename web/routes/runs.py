from __future__ import annotations

import os
import tempfile

from pathlib import Path
from flask import Blueprint, flash, redirect, render_template, request, send_file, url_for

from core.config import settings
from core.board_layout import build_board_grid
from core.run_viewer import get_last_run_id, get_run_board, list_runs, count_runs
from web.db_context import get_db, get_hero_colors_map, get_templates_conn
from web.services import get_hero_list, get_run_item_progress_table
from web.services.run_edits import (
    clear_item_override,
    confirm_run,
    create_manual_run,
    delete_run,
    reread_run_metrics_from_screenshot,
    set_hero_override,
    set_item_override,
    set_run_notes,
    set_run_screenshot,
    update_run_metrics,
)




runs_bp = Blueprint("runs", __name__)


def _parse_optional_int(value: str | None):
    s = (value or "").strip()
    if s == "":
        return None
    return int(s)


@runs_bp.get("/runs")
def runs_view():
    per_page = 100

    try:
        page = int(request.args.get("page", "1"))
    except ValueError:
        page = 1

    if page < 1:
        page = 1

    total_runs = count_runs(settings.run_history_db_path)
    total_pages = max(1, (total_runs + per_page - 1) // per_page)

    if page > total_pages:
        page = total_pages

    offset = (page - 1) * per_page

    runs = list_runs(
        settings.run_history_db_path,
        limit=per_page,
        offset=offset,
    )

    hero_colors = get_hero_colors_map()

    return render_template(
        "runs_view.html",
        runs=runs,
        hero_colors=hero_colors,
        page=page,
        total_pages=total_pages,
        total_runs=total_runs,
        has_prev=(page > 1),
        has_next=(page < total_pages),
    )


@runs_bp.get("/run/latest")
def run_latest():
    rid = get_last_run_id(settings.run_history_db_path)
    if rid is None:
        return redirect(url_for("main.index"))
    return redirect(url_for("runs.run_detail", run_id=rid))


@runs_bp.get("/run/<int:run_id>")
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

    progress = get_run_item_progress_table(
        settings.templates_db_path,
        settings.run_history_db_path,
        run_id,
        hconn=db.conn,
        tconn=get_templates_conn(),
    )

    cur.execute(
        "SELECT MAX(run_id) AS prev_id FROM runs WHERE run_id < ?",
        (run_id,),
    )
    prev_row = cur.fetchone()
    prev_run_id = prev_row["prev_id"] if prev_row else None

    cur.execute(
        "SELECT MIN(run_id) AS next_id FROM runs WHERE run_id > ?",
        (run_id,),
    )
    next_row = cur.fetchone()
    next_run_id = next_row["next_id"] if next_row else None

    hero_name = run.get("hero_effective")
    hero_colors = get_hero_colors_map()
    hero_color = hero_colors.get(hero_name) if hero_name else None

    return render_template(
        "run.html",
        run=run,
        grid=grid,
        edit_mode=edit_mode,
        heroes=heroes,
        metrics=metrics,
        progress=progress,
        achievements_unlocked=achievements_unlocked,
        prev_run_id=prev_run_id,
        next_run_id=next_run_id,
        hero_color=hero_color,
    )


@runs_bp.post("/run/<int:run_id>/metrics")
def run_metrics_update(run_id: int):
    try:
        season_id = _parse_optional_int(request.form.get("season_id"))
        rank = _parse_optional_int(request.form.get("rank"))
        wins = _parse_optional_int(request.form.get("wins"))
        max_health = _parse_optional_int(request.form.get("max_health"))
        prestige = _parse_optional_int(request.form.get("prestige"))
        level = _parse_optional_int(request.form.get("level"))
        income = _parse_optional_int(request.form.get("income"))
        gold = _parse_optional_int(request.form.get("gold"))

        if wins is not None and wins > 10:
            wins = 10
    except ValueError as e:
        return (str(e), 400)

    update_run_metrics(
        run_id,
        season_id=season_id,
        rank=rank,
        wins=wins,
        max_health=max_health,
        prestige=prestige,
        level=level,
        income=income,
        gold=gold,
    )

    return_edit = request.form.get("return_edit") == "1"
    if return_edit:
        return redirect(url_for("runs.run_detail", run_id=run_id, edit=1))
    return redirect(url_for("runs.run_detail", run_id=run_id))


@runs_bp.post("/runs/new-empty")
def run_create_empty():
    run_id = create_manual_run(
        hero=None,
        season_id=None,
        wins=None,
        max_health=None,
        prestige=None,
        level=None,
        income=None,
        gold=None,
        notes="",
        confirmed=False,
    )
    return redirect(url_for("runs.run_detail", run_id=run_id, edit=1))


@runs_bp.post("/run/<int:run_id>/delete")
def run_delete(run_id: int):
    delete_run(run_id)
    return redirect(url_for("runs.runs_view"))


@runs_bp.get("/screenshot/<int:run_id>")
def screenshot(run_id: int):
    run = get_run_board(settings.run_history_db_path, settings.templates_db_path, run_id)
    path = run.get("screenshot_path")
    if not path:
        return ("No screenshot_path for this run", 404)

    if not os.path.isabs(path):
        path = os.path.abspath(path)

    if not os.path.exists(path):
        return (f"Screenshot not found: {path}", 404)

    return send_file(path)


@runs_bp.post("/run/<int:run_id>/screenshot")
def run_screenshot_update(run_id: int):
    file = request.files.get("screenshot")
    if not file or not file.filename:
        flash("No screenshot uploaded.", "error")
        return redirect(url_for("runs.run_detail", run_id=run_id, edit=1))

    reread = request.form.get("reread_metrics") == "1"

    suffix = Path(file.filename).suffix or ".png"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        file.save(tmp.name)
        tmp_path = tmp.name

    try:
        set_run_screenshot(
            run_id,
            source_path=tmp_path,
            reread_metrics=reread,
        )
        if reread:
            flash("Screenshot uploaded and metrics updated.", "success")
        else:
            flash("Screenshot uploaded.", "success")
    except Exception as e:
        flash(f"Could not read screenshot metrics: {e}", "error")
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    return redirect(url_for("runs.run_detail", run_id=run_id, edit=1))


@runs_bp.post("/run/<int:run_id>/screenshot/reread")
def run_screenshot_reread(run_id: int):
    try:
        reread_run_metrics_from_screenshot(run_id)
        flash("Metrics re-read from screenshot.", "success")
    except Exception as e:
        flash(f"Could not re-read screenshot metrics: {e}", "error")

    return redirect(url_for("runs.run_detail", run_id=run_id, edit=1))


@runs_bp.post("/run/<int:run_id>/confirm")
def run_confirm(run_id: int):
    confirmed = request.form.get("confirmed") == "1"
    confirm_run(run_id, confirmed=confirmed)

    return_edit = request.form.get("return_edit") == "1"
    if return_edit:
        return redirect(url_for("runs.run_detail", run_id=run_id, edit=1))
    return redirect(url_for("runs.run_detail", run_id=run_id))


@runs_bp.post("/run/<int:run_id>/notes")
def run_notes(run_id: int):
    notes = request.form.get("notes") or ""
    set_run_notes(run_id, notes)

    return_edit = request.form.get("return_edit") == "1"
    if return_edit:
        return redirect(url_for("runs.run_detail", run_id=run_id, edit=1))
    return redirect(url_for("runs.run_detail", run_id=run_id))


@runs_bp.post("/run/<int:run_id>/hero")
def run_hero_override(run_id: int):
    hero = (request.form.get("hero") or "").strip()
    set_hero_override(run_id, hero)

    return_edit = request.form.get("return_edit") == "1"
    if return_edit:
        return redirect(url_for("runs.run_detail", run_id=run_id, edit=1))
    return redirect(url_for("runs.run_detail", run_id=run_id))


@runs_bp.post("/run/<int:run_id>/item/set")
def item_set(run_id: int):
    socket_s = request.form.get("socket") or ""
    template_id = (request.form.get("template_id") or "").strip()

    try:
        socket = int(socket_s)
    except ValueError:
        return ("Invalid socket", 400)

    if socket < 0 or socket > 9:
        return ("Invalid socket (0-9)", 400)

    set_item_override(run_id, socket, template_id)

    return_edit = request.form.get("return_edit") == "1"
    if return_edit:
        return redirect(url_for("runs.run_detail", run_id=run_id, edit=1))
    return redirect(url_for("runs.run_detail", run_id=run_id))


@runs_bp.post("/run/<int:run_id>/item/size")
def item_size_set(run_id: int):
    try:
        socket = int(request.form.get("socket", ""))
    except ValueError:
        return ("Invalid socket", 400)

    size = (request.form.get("size") or "").strip().lower()
    if size not in ("small", "medium", "large"):
        return ("Invalid size", 400)

    template_id = request.form.get("template_id")
    set_item_override(run_id, socket, template_id=template_id, size=size)
    return redirect(url_for("runs.run_detail", run_id=run_id, edit=1))


@runs_bp.post("/run/<int:run_id>/item/clear")
def item_clear(run_id: int):
    try:
        socket = int(request.form.get("socket", ""))
    except ValueError:
        return ("Invalid socket", 400)

    clear_item_override(run_id, socket)

    return_edit = request.form.get("return_edit") == "1"
    if return_edit:
        return redirect(url_for("runs.run_detail", run_id=run_id, edit=1))
    return redirect(url_for("runs.run_detail", run_id=run_id))
