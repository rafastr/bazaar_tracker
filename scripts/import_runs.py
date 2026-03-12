from __future__ import annotations

import argparse
import json
from typing import Any

from core.config import settings
from core.run_history_db import RunHistoryDb


TABLE_INSERTS: dict[str, tuple[str, tuple[str, ...]]] = {
    "runs": (
        """
        INSERT OR IGNORE INTO runs (
            run_id, ended_at_unix, screenshot_path, hero, rank, metrics_json, is_confirmed, notes, season_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
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
        """
        INSERT OR IGNORE INTO run_items (
            run_id, socket_number, template_id, size
        ) VALUES (?, ?, ?, ?)
        """,
        ("run_id", "socket_number", "template_id", "size"),
    ),
    "run_metrics": (
        """
        INSERT OR IGNORE INTO run_metrics (
            run_id, wins, max_health, prestige, level, income, gold, won, ocr_json, ocr_version, updated_at_unix
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
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
        """
        INSERT OR IGNORE INTO run_overrides (
            run_id, hero_override, rank_override, notes, is_confirmed, updated_at_unix
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("run_id", "hero_override", "rank_override", "notes", "is_confirmed", "updated_at_unix"),
    ),
    "run_item_overrides": (
        """
        INSERT OR IGNORE INTO run_item_overrides (
            run_id, socket_number, template_id_override, size_override, note, updated_at_unix
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("run_id", "socket_number", "template_id_override", "size_override", "note", "updated_at_unix"),
    ),
    "season_markers": (
        """
        INSERT OR IGNORE INTO season_markers (
            season_id, first_seen_at_unix, source_run_id, note
        ) VALUES (?, ?, ?, ?)
        """,
        ("season_id", "first_seen_at_unix", "source_run_id", "note"),
    ),
    "imported_item_completion": (
        """
        INSERT OR IGNORE INTO imported_item_completion (
            template_id, win_this, win_other, ten_wins, source, imported_at_unix
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "template_id",
            "win_this",
            "win_other",
            "ten_wins",
            "source",
            "imported_at_unix",
        ),
    ),
}


BASE_TABLES = (
    "runs",
    "run_items",
    "run_metrics",
    "run_overrides",
    "run_item_overrides",
    "season_markers",
    "imported_item_completion",
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import Bazaar Chronicle runs from JSON export")
    p.add_argument("in_json", help="Input JSON export path")
    p.add_argument(
        "--db",
        dest="db_path",
        default=settings.run_history_db_path,
        help="Path to run history sqlite db",
    )
    p.add_argument(
        "--templates-db",
        dest="templates_db_path",
        default=settings.templates_db_path,
        help="Path to templates sqlite db (used for rebuilds)",
    )
    p.add_argument(
        "--no-rebuild",
        action="store_true",
        help="Skip rebuilding derived tables after import",
    )
    return p.parse_args()


def validate_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise RuntimeError("Invalid export: root JSON object expected")

    version = payload.get("export_version")
    if version != 1:
        raise RuntimeError(f"Unsupported export_version: {version!r}")


def insert_rows(db: RunHistoryDb, table_name: str, rows: list[dict[str, Any]]) -> tuple[int, int]:
    sql, columns = TABLE_INSERTS[table_name]
    cur = db.conn.cursor()

    inserted = 0
    skipped = 0

    for row in rows:
        values = tuple(row.get(col) for col in columns)

        before = db.conn.total_changes
        cur.execute(sql, values)

        if db.conn.total_changes > before:
            inserted += 1
        else:
            skipped += 1

    return inserted, skipped


# ------------------------------------------------------------
# Core logic function (callable from Flask / scripts / launcher)
# ------------------------------------------------------------

def import_runs_from_json(
    in_json: str,
    db_path: str,
    templates_db_path: str,
    rebuild: bool = True,
) -> dict[str, Any]:

    with open(in_json, "r", encoding="utf-8") as f:
        payload = json.load(f)

    validate_payload(payload)

    db = RunHistoryDb(db_path)

    try:
        inserted_counts: dict[str, int] = {}
        skipped_counts: dict[str, int] = {}

        db.conn.execute("BEGIN")

        try:
            for table_name in BASE_TABLES:
                rows = payload.get(table_name, []) or []

                if not isinstance(rows, list):
                    raise RuntimeError(f"Invalid export: {table_name} must be a list")

                inserted, skipped = insert_rows(db, table_name, rows)

                inserted_counts[table_name] = inserted
                skipped_counts[table_name] = skipped

            db.conn.commit()

        except Exception:
            db.conn.rollback()
            raise

        if rebuild:
            db.rebuild_item_hero_wins()
            db.rebuild_item_firsts(templates_db_path)
            db.rebuild_achievements(templates_db_path)

        return {
            "ok": True,
            "message": "Import completed",
            "db": db_path,
            "in": in_json,
            "inserted": inserted_counts,
            "skipped": skipped_counts,
            "rebuilt": rebuild,
        }

    finally:
        db.close()


# ------------------------------------------------------------
# CLI wrapper
# ------------------------------------------------------------

def main() -> None:
    args = parse_args()

    result = import_runs_from_json(
        in_json=args.in_json,
        db_path=args.db_path,
        templates_db_path=args.templates_db_path,
        rebuild=not args.no_rebuild,
    )

    print("\nImport summary")

    for table_name in BASE_TABLES:
        print(
            f"{table_name}: inserted={result['inserted'].get(table_name, 0)} "
            f"skipped={result['skipped'].get(table_name, 0)}"
        )

    print(
        {
            "type": "RunsImported",
            "db": result["db"],
            "in": result["in"],
            "inserted": result["inserted"],
            "skipped": result["skipped"],
            "rebuilt": result["rebuilt"],
        }
    )


if __name__ == "__main__":
    main()
