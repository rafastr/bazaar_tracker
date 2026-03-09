from __future__ import annotations

import os
import tempfile

from flask import after_this_request, Blueprint, redirect, render_template, request, send_file, session, url_for
from werkzeug.utils import secure_filename

from scripts.import_templates import default_cards_path
from core.config import settings
from web.services.manage import (
    doctor_summary,
    export_everything_temp,
    export_runs_temp,
    import_completion_csv_upload,
    import_runs_upload,
    update_item_images,
    update_templates,
)


manage_bp = Blueprint("manage", __name__)


@manage_bp.get("/manage")
def manage():
    result = session.pop("manage_result", None)
    return render_template(
        "manage.html",
        result=result,
        default_cards_path=default_cards_path(),
        data_dir=settings.data_dir,
    )


@manage_bp.post("/manage/export")
def manage_export():
    try:
        path, download_name, result = export_runs_temp()
        session["manage_result"] = {
            "kind": "success",
            "title": "Export completed",
            "body": "\n".join(
                [f"{k}: {v}" for k, v in result.get("counts", {}).items()]
            ) or "Export file created successfully.",
        }
        return send_file(path, as_attachment=True, download_name=download_name)
    except Exception as e:
        session["manage_result"] = {
            "kind": "error",
            "title": "Export failed",
            "body": str(e),
        }
        return redirect(url_for("manage.manage"))


@manage_bp.post("/manage/import-json")
def manage_import_json():
    f = request.files.get("file")
    if not f or not f.filename:
        session["manage_result"] = {
            "kind": "error",
            "title": "No file selected",
            "body": "Please choose a JSON file to import.",
        }
        return redirect(url_for("manage.manage"))

    suffix = os.path.splitext(secure_filename(f.filename))[1] or ".json"
    fd, tmp_path = tempfile.mkstemp(suffix=suffix, prefix="bazaar_import_runs_")
    os.close(fd)

    try:
        f.save(tmp_path)
        result = import_runs_upload(tmp_path)

        lines = []
        for table_name, inserted in result.get("inserted", {}).items():
            skipped = result.get("skipped", {}).get(table_name, 0)
            lines.append(f"{table_name}: inserted={inserted}, skipped={skipped}")

        session["manage_result"] = {
            "kind": "success",
            "title": "Runs imported",
            "body": "\n".join(lines) or "Import completed.",
        }
    except Exception as e:
        session["manage_result"] = {
            "kind": "error",
            "title": "Import failed",
            "body": str(e),
        }
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    return redirect(url_for("manage.manage"))


@manage_bp.post("/manage/export-everything")
def manage_export_everything():
    try:
        path, download_name, result = export_everything_temp()

        @after_this_request
        def cleanup_temp_file(response):
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass
            return response

        files = result.get("included", {}).get("files", [])
        dirs = result.get("included", {}).get("dirs", {})

        lines = []
        if files:
            lines.append("Files:")
            lines.extend(f"- {name}" for name in files)

        if dirs:
            if lines:
                lines.append("")
            lines.append("Directories:")
            for name, count in dirs.items():
                lines.append(f"- {name}: {count} files")

        session["manage_result"] = {
            "kind": "success",
            "title": "Full backup created",
            "body": "\n".join(lines) or "Backup zip created successfully.",
        }

        return send_file(path, as_attachment=True, download_name=download_name)

    except Exception as e:
        session["manage_result"] = {
            "kind": "error",
            "title": "Full backup failed",
            "body": str(e),
        }
        return redirect(url_for("manage.manage"))


@manage_bp.post("/manage/import-csv")
def manage_import_csv():
    f = request.files.get("file")
    replace = request.form.get("replace") == "1"

    if not f or not f.filename:
        session["manage_result"] = {
            "kind": "error",
            "title": "No file selected",
            "body": "Please choose a CSV file to import.",
        }
        return redirect(url_for("manage.manage"))

    suffix = os.path.splitext(secure_filename(f.filename))[1] or ".csv"
    fd, tmp_path = tempfile.mkstemp(suffix=suffix, prefix="bazaar_import_completion_")
    os.close(fd)

    try:
        f.save(tmp_path)
        result = import_completion_csv_upload(tmp_path, replace=replace)

        session["manage_result"] = {
            "kind": "success",
            "title": "Completion CSV imported",
            "body": (
                f"matched: {result.get('matched', 0)}\n"
                f"imported: {result.get('imported', 0)}\n"
                f"unmatched: {result.get('unmatched', 0)}\n"
                f"ambiguous: {result.get('ambiguous', 0)}\n"
                f"skipped_empty: {result.get('skipped_empty', 0)}"
            ),
        }
    except Exception as e:
        session["manage_result"] = {
            "kind": "error",
            "title": "CSV import failed",
            "body": str(e),
        }
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    return redirect(url_for("manage.manage"))


@manage_bp.post("/manage/update-templates")
def manage_update_templates():
    use_default = request.form.get("use_default_cards_path") == "1"
    cards_json = (request.form.get("cards_json") or "").strip()

    if use_default and default_cards_path():
        cards_json = default_cards_path()

    if not cards_json:
        session["manage_result"] = {
            "kind": "error",
            "title": "Missing cards.json path",
            "body": "cards.json path is required.",
        }
        return redirect(url_for("manage.manage"))

    try:
        result = update_templates(cards_json)
        session["manage_result"] = {
            "kind": "success",
            "title": "Templates updated",
            "body": (
                f"cards_seen: {result.get('cards_seen', 0)}\n"
                f"items_imported: {result.get('items_imported', 0)}\n"
                f"templates_skipped: {result.get('templates_skipped', 0)}"
            ),
        }
    except Exception as e:
        session["manage_result"] = {
            "kind": "error",
            "title": "Template import failed",
            "body": str(e),
        }

    return redirect(url_for("manage.manage"))


@manage_bp.post("/manage/cache-images")
def manage_cache_images():
    force = request.form.get("force") == "1"
    insecure = request.form.get("insecure") == "1"

    try:
        limit = int(request.form.get("limit") or "0")
    except ValueError:
        limit = 0

    try:
        sleep = float(request.form.get("sleep") or "0.7")
    except ValueError:
        sleep = 0.7

    try:
        result = update_item_images(
            force=force,
            insecure=insecure,
            limit=limit,
            sleep=sleep,
        )
        session["manage_result"] = {
            "kind": "success",
            "title": "Item image cache updated",
            "body": (
                f"downloaded: {result.get('downloaded', 0)}\n"
                f"skipped: {result.get('skipped', 0)}\n"
                f"fixed: {result.get('fixed', 0)}\n"
                f"unresolved: {result.get('unresolved', 0)}\n"
                f"failed: {result.get('failed', 0)}"
            ),
        }
    except Exception as e:
        session["manage_result"] = {
            "kind": "error",
            "title": "Image cache update failed",
            "body": str(e),
        }

    return redirect(url_for("manage.manage"))


@manage_bp.post("/manage/doctor")
def manage_doctor():
    try:
        result = doctor_summary()

        lines = []
        if result.get("problems"):
            lines.append("Problems:")
            lines.extend(f"- {p}" for p in result["problems"])

        if result.get("notes"):
            if lines:
                lines.append("")
            lines.append("Notes:")
            lines.extend(f"- {n}" for n in result["notes"])

        session["manage_result"] = {
            "kind": "success" if result.get("ok") else "error",
            "title": "Doctor finished",
            "body": "\n".join(lines) or result.get("message", "Doctor completed."),
        }
    except Exception as e:
        session["manage_result"] = {
            "kind": "error",
            "title": "Doctor failed",
            "body": str(e),
        }

    return redirect(url_for("manage.manage"))
