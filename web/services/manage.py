from __future__ import annotations

import os
import tempfile

from core.config import settings
from scripts.export_runs import export_runs_to_json
from scripts.import_runs import import_runs_from_json
from scripts.import_completion_csv import import_completion_csv_file
from scripts.import_templates import import_templates_from_cards
from scripts.cache_item_images import cache_item_images
from scripts.doctor import run_doctor


def export_runs_temp() -> tuple[str, str, dict]:
    fd, path = tempfile.mkstemp(suffix=".json", prefix="bazaar_tracker_export_")
    os.close(fd)

    result = export_runs_to_json(
        db_path=settings.run_history_db_path,
        out_json=path,
        pretty=True,
    )
    return path, os.path.basename(path), result


def import_runs_upload(upload_path: str) -> dict:
    return import_runs_from_json(
        in_json=upload_path,
        db_path=settings.run_history_db_path,
        templates_db_path=settings.templates_db_path,
        rebuild=True,
    )


def import_completion_csv_upload(upload_path: str, replace: bool = False) -> dict:
    return import_completion_csv_file(
        csv_path=upload_path,
        db_path=settings.run_history_db_path,
        templates_db_path=settings.templates_db_path,
        replace=replace,
    )


def update_templates(cards_json_path: str) -> dict:
    return import_templates_from_cards(
        cards_json=cards_json_path,
        db_path=settings.templates_db_path,
    )


def update_item_images(
    force: bool = False,
    insecure: bool = False,
    limit: int = 0,
    sleep: float = 0.7,
) -> dict:
    return cache_item_images(
        db_path=settings.templates_db_path,
        out_dir="assets/images/items",
        sleep=sleep,
        limit=limit,
        force=force,
        timeout=30,
        insecure=insecure,
        debug=False,
    )


def doctor_summary() -> dict:
    return run_doctor(
        db_path=settings.run_history_db_path,
        templates_db_path=settings.templates_db_path,
    )
