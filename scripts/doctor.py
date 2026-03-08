from __future__ import annotations

import argparse
import os
import sqlite3
from typing import Iterable, Any

from core.config import settings
from core.db_utils import connect_db


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Check Bazaar Tracker database health")
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
        help="Path to templates sqlite db",
    )
    return p.parse_args()


def scalar(conn: sqlite3.Connection, sql: str, params: Iterable = ()) -> int:
    cur = conn.cursor()
    cur.execute(sql, tuple(params))
    row = cur.fetchone()
    if not row:
        return 0
    return int(row[0] or 0)


def has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == column for r in cur.fetchall())


def run_doctor(db_path: str, templates_db_path: str) -> dict[str, Any]:
    problems: list[str] = []
    notes: list[str] = []

    with connect_db(templates_db_path) as tconn, connect_db(db_path) as hconn:
        tcur = tconn.cursor()
        hcur = hconn.cursor()

        templates_has_ignored = has_column(tconn, "templates", "ignored")

        # --- template stats ---
        total_templates = scalar(tconn, "SELECT COUNT(*) FROM templates")
        ignored_templates = (
            scalar(tconn, "SELECT COUNT(*) FROM templates WHERE COALESCE(ignored, 0) = 1")
            if templates_has_ignored
            else 0
        )
        active_templates = total_templates - ignored_templates

        image_missing = 0
        image_set = 0

        if has_column(tconn, "templates", "image_path"):
            tcur.execute(
                """
                SELECT image_path
                FROM templates
                WHERE image_path IS NOT NULL
                  AND TRIM(image_path) <> ''
                """
            )
            rows = tcur.fetchall()
            image_set = len(rows)
            for r in rows:
                path = (r["image_path"] or "").strip()
                if path and not os.path.exists(os.path.normpath(path)):
                    image_missing += 1

        # --- runs missing metrics ---
        runs_missing_metrics = scalar(
            hconn,
            """
            SELECT COUNT(*)
            FROM runs r
            LEFT JOIN run_metrics m ON m.run_id = r.run_id
            WHERE m.run_id IS NULL
            """,
        )

        # --- orphan child rows ---
        orphan_run_metrics = scalar(
            hconn,
            """
            SELECT COUNT(*)
            FROM run_metrics m
            LEFT JOIN runs r ON r.run_id = m.run_id
            WHERE r.run_id IS NULL
            """,
        )

        orphan_run_items = scalar(
            hconn,
            """
            SELECT COUNT(*)
            FROM run_items i
            LEFT JOIN runs r ON r.run_id = i.run_id
            WHERE r.run_id IS NULL
            """,
        )

        orphan_run_overrides = scalar(
            hconn,
            """
            SELECT COUNT(*)
            FROM run_overrides o
            LEFT JOIN runs r ON r.run_id = o.run_id
            WHERE r.run_id IS NULL
            """,
        )

        orphan_run_item_overrides = scalar(
            hconn,
            """
            SELECT COUNT(*)
            FROM run_item_overrides o
            LEFT JOIN runs r ON r.run_id = o.run_id
            WHERE r.run_id IS NULL
            """,
        )

        # --- cross-db template reference checks ---
        hcur.execute("ATTACH DATABASE ? AS templates_db", (templates_db_path,))

        try:
            missing_run_item_templates = scalar(
                hconn,
                """
                SELECT COUNT(*)
                FROM run_items i
                WHERE i.template_id IS NOT NULL
                  AND TRIM(i.template_id) <> ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM templates_db.templates t
                      WHERE t.template_id = i.template_id
                  )
                """,
            )

            missing_item_override_templates = scalar(
                hconn,
                """
                SELECT COUNT(*)
                FROM run_item_overrides o
                WHERE o.template_id_override IS NOT NULL
                  AND TRIM(o.template_id_override) <> ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM templates_db.templates t
                      WHERE t.template_id = o.template_id_override
                  )
                """,
            )
        finally:
            hcur.execute("DETACH DATABASE templates_db")

        # --- broken screenshots ---
        hcur.execute(
            """
            SELECT screenshot_path
            FROM runs
            WHERE screenshot_path IS NOT NULL
              AND TRIM(screenshot_path) <> ''
            """
        )
        screenshot_rows = hcur.fetchall()

        broken_screenshots = 0
        for r in screenshot_rows:
            path = (r["screenshot_path"] or "").strip()
            if path and not os.path.exists(os.path.normpath(path)):
                broken_screenshots += 1

        # --- build problems list ---
        if image_missing:
            problems.append(f"{image_missing} templates have image_path set but file is missing")
        if runs_missing_metrics:
            problems.append(f"{runs_missing_metrics} runs have no run_metrics row")
        if orphan_run_metrics:
            problems.append(f"{orphan_run_metrics} orphan run_metrics rows")
        if orphan_run_items:
            problems.append(f"{orphan_run_items} orphan run_items rows")
        if orphan_run_overrides:
            problems.append(f"{orphan_run_overrides} orphan run_overrides rows")
        if orphan_run_item_overrides:
            problems.append(f"{orphan_run_item_overrides} orphan run_item_overrides rows")
        if missing_run_item_templates:
            problems.append(f"{missing_run_item_templates} run_items reference missing templates")
        if missing_item_override_templates:
            problems.append(f"{missing_item_override_templates} run_item_overrides reference missing templates")
        if broken_screenshots:
            problems.append(f"{broken_screenshots} runs have broken screenshot paths")

        if ignored_templates:
            notes.append(f"{ignored_templates} templates are marked ignored")
        notes.append(f"{active_templates} active templates")
        notes.append(f"{image_set} templates have image_path set")

    return {
        "ok": len(problems) == 0,
        "message": "No problems found." if not problems else "Problems found.",
        "problems": problems,
        "notes": notes,
        "db": db_path,
        "templates_db": templates_db_path,
    }


def main() -> None:
    args = parse_args()
    result = run_doctor(
        db_path=args.db_path,
        templates_db_path=args.templates_db_path,
    )

    print("\nBazaar Tracker doctor\n")

    if result["problems"]:
        print("Problems found:")
        for p in result["problems"]:
            print(f"- {p}")
    else:
        print("No problems found.")

    if result["notes"]:
        print("\nNotes:")
        for n in result["notes"]:
            print(f"- {n}")


if __name__ == "__main__":
    main()
