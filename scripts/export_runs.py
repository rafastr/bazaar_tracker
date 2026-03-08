from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

from core.config import settings
from core.run_history_db import RunHistoryDb


TABLE_SPECS: dict[str, tuple[str, tuple[str, ...]]] = {
    "runs": (
        "SELECT run_id, ended_at_unix, screenshot_path, hero, rank, metrics_json, is_confirmed, notes, season_id FROM runs ORDER BY run_id ASC",
        (
            "run_id",
            "ended_at_unix",
            "screenshot_path",
            "hero",
            "rank",
            "metrics_json",
            "is_confirmed",
            "notes",
            "season_id",
        ),
    ),
    "run_items": (
        "SELECT run_id, socket_number, template_id, size FROM run_items ORDER BY run_id ASC, socket_number ASC",
        ("run_id", "socket_number", "template_id", "size"),
    ),
    "run_metrics": (
        "SELECT run_id, wins, max_health, prestige, level, income, gold, won, ocr_json, ocr_version, updated_at_unix FROM run_metrics ORDER BY run_id ASC",
        (
            "run_id",
            "wins",
            "max_health",
            "prestige",
            "level",
            "income",
            "gold",
            "won",
            "ocr_json",
            "ocr_version",
            "updated_at_unix",
        ),
    ),
    "run_overrides": (
        "SELECT run_id, hero_override, rank_override, notes, is_confirmed, updated_at_unix FROM run_overrides ORDER BY run_id ASC",
        ("run_id", "hero_override", "rank_override", "notes", "is_confirmed", "updated_at_unix"),
    ),
    "run_item_overrides": (
        "SELECT run_id, socket_number, template_id_override, size_override, note, updated_at_unix FROM run_item_overrides ORDER BY run_id ASC, socket_number ASC",
        ("run_id", "socket_number", "template_id_override", "size_override", "note", "updated_at_unix"),
    ),
    "season_markers": (
        "SELECT season_id, first_seen_at_unix, source_run_id, note FROM season_markers ORDER BY season_id ASC",
        ("season_id", "first_seen_at_unix", "source_run_id", "note"),
    ),
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export Bazaar Tracker runs to JSON")
    p.add_argument("out_json", help="Output JSON path")
    p.add_argument(
        "--db",
        dest="db_path",
        default=settings.run_history_db_path,
        help="Path to run history sqlite db",
    )
    p.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output",
    )
    return p.parse_args()


def rows_to_dicts(db: RunHistoryDb, sql: str, columns: tuple[str, ...]) -> list[dict[str, Any]]:
    cur = db.conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    return [{col: row[col] for col in columns} for row in rows]


def main() -> None:
    args = parse_args()

    out_parent = os.path.dirname(args.out_json)
    if out_parent:
        os.makedirs(out_parent, exist_ok=True)

    db = RunHistoryDb(args.db_path)
    try:
        payload: dict[str, Any] = {
            "export_version": 1,
            "app": "bazaar_tracker",
            "exported_at_unix": int(time.time()),
            "source_db": args.db_path,
        }

        counts: dict[str, int] = {}
        for table_name, (sql, columns) in TABLE_SPECS.items():
            rows = rows_to_dicts(db, sql, columns)
            payload[table_name] = rows
            counts[table_name] = len(rows)

        payload["counts"] = counts

        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2 if args.pretty else None)

        print("\nExport completed")
        for table_name in TABLE_SPECS:
            print(f"{table_name}: {counts.get(table_name, 0)}")

        print(
            {
                "type": "RunsExported",
                "db": args.db_path,
                "out": args.out_json,
                "counts": counts,
            }
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
