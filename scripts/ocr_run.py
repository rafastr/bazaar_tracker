from __future__ import annotations

import argparse

from core.config import settings
from core.run_history_db import RunHistoryDb
from core.run_viewer import get_run_board
from core.ocr_metrics import extract_run_metrics
from core.ocr_rois import ROIS


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", type=int, required=True)
    p.add_argument("--version", default="v1")
    p.add_argument("--debug", default="", help="Write debug crops to this folder")
    args = p.parse_args()

    run = get_run_board(settings.run_history_db_path, settings.templates_db_path, args.run_id)
    shot = run.get("screenshot_path")
    if not shot:
        raise SystemExit("Run has no screenshot_path")


    debug_dir = args.debug.strip() or None
    metrics = extract_run_metrics(shot, ROIS, ocr_version=args.version, debug_dir=debug_dir)



    metrics = extract_run_metrics(shot, ROIS, ocr_version=args.version)

    db = RunHistoryDb(settings.run_history_db_path)
    try:
        db.upsert_run_metrics(
            args.run_id,
            wins=metrics.get("wins"),
            max_health=metrics.get("max_health"),
            prestige=metrics.get("prestige"),
            level=metrics.get("level"),
            income=metrics.get("income"),
            gold=metrics.get("gold"),
            won=metrics.get("won"),
            ocr_json=metrics.get("ocr_json"),
            ocr_version=metrics.get("ocr_version"),
        )
    finally:
        db.close()

    print("OK:", {k: metrics.get(k) for k in ["wins", "max_health", "prestige", "level", "income", "gold", "won"]})


if __name__ == "__main__":
    main()
