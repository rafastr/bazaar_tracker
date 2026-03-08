from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import time
from collections import defaultdict
from typing import Any

from core.config import settings
from core.db_utils import connect_db


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import item completion from CSV")
    p.add_argument("csv_path", help="Path to exported CSV checklist")
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
    p.add_argument(
        "--replace",
        action="store_true",
        help="Clear previously imported completion before importing",
    )
    return p.parse_args()


def ensure_imported_item_completion_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS imported_item_completion (
            template_id TEXT PRIMARY KEY,
            win_this INTEGER NOT NULL DEFAULT 0,
            win_other INTEGER NOT NULL DEFAULT 0,
            ten_wins INTEGER NOT NULL DEFAULT 0,
            source TEXT,
            imported_at_unix INTEGER NOT NULL
        )
        """
    )


def norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def norm_size(s: str) -> str:
    s = norm_text(s)
    if s in ("small", "medium", "large"):
        return s
    return ""


def truthy(v: Any) -> bool:
    s = norm_text(str(v or ""))
    return s in {"x", "true", "yes", "1", "y"}


def get_field(row: dict[str, Any], name: str) -> str:
    for k, v in row.items():
        if norm_text(k) == norm_text(name):
            return str(v or "")
    return ""


def load_templates(templates_db_path: str) -> dict[str, list[dict[str, str]]]:
    by_name: dict[str, list[dict[str, str]]] = defaultdict(list)

    with connect_db(templates_db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT template_id, name, size
            FROM templates
            WHERE COALESCE(ignored, 0) = 0
              AND name IS NOT NULL
            """
        )
        for r in cur.fetchall():
            entry = {
                "template_id": r["template_id"],
                "name": r["name"],
                "size": (r["size"] or "").strip().lower(),
            }
            by_name[norm_text(r["name"])].append(entry)

    return by_name


def choose_template(
    candidates: list[dict[str, str]],
    csv_size: str,
) -> tuple[str | None, str]:
    if not candidates:
        return None, "unmatched"

    if len(candidates) == 1:
        return candidates[0]["template_id"], "matched"

    if csv_size:
        narrowed = [c for c in candidates if norm_size(c.get("size", "")) == csv_size]
        if len(narrowed) == 1:
            return narrowed[0]["template_id"], "matched"
        if len(narrowed) > 1:
            return None, "ambiguous"

    return None, "ambiguous"


# ------------------------------------------------------------
# Core logic function (used by CLI + Flask)
# ------------------------------------------------------------

def import_completion_csv_file(
    csv_path: str,
    db_path: str,
    templates_db_path: str,
    replace: bool = False,
) -> dict[str, Any]:

    templates_by_name = load_templates(templates_db_path)
    now = int(time.time())

    matched = 0
    unmatched = 0
    ambiguous = 0
    imported = 0
    skipped_empty = 0

    with connect_db(db_path) as conn:
        ensure_imported_item_completion_table(conn)
        cur = conn.cursor()

        if replace:
            cur.execute("DELETE FROM imported_item_completion")

        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)

            for row in reader:
                item_name = get_field(row, "Item").strip()
                if not item_name:
                    continue

                ten_wins = truthy(get_field(row, "10 Wins"))
                win_other = truthy(get_field(row, "Win with Another Hero"))

                if not ten_wins and not win_other:
                    skipped_empty += 1
                    continue

                win_this = ten_wins or win_other

                csv_size = norm_size(get_field(row, "Size"))
                candidates = templates_by_name.get(norm_text(item_name), [])
                template_id, status = choose_template(candidates, csv_size)

                if status == "unmatched":
                    unmatched += 1
                    continue

                if status == "ambiguous":
                    ambiguous += 1
                    continue

                matched += 1

                cur.execute(
                    """
                    INSERT INTO imported_item_completion (
                        template_id,
                        win_this,
                        win_other,
                        ten_wins,
                        source,
                        imported_at_unix
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(template_id) DO UPDATE SET
                        win_this = excluded.win_this,
                        win_other = excluded.win_other,
                        ten_wins = excluded.ten_wins,
                        source = excluded.source,
                        imported_at_unix = excluded.imported_at_unix
                    """,
                    (
                        template_id,
                        1 if win_this else 0,
                        1 if win_other else 0,
                        1 if ten_wins else 0,
                        csv_path,
                        now,
                    ),
                )

                imported += 1

    return {
        "ok": True,
        "message": "Completion CSV import completed",
        "csv": csv_path,
        "matched": matched,
        "imported": imported,
        "unmatched": unmatched,
        "ambiguous": ambiguous,
        "skipped_empty": skipped_empty,
    }


# ------------------------------------------------------------
# CLI wrapper
# ------------------------------------------------------------

def main() -> None:
    args = parse_args()

    result = import_completion_csv_file(
        csv_path=args.csv_path,
        db_path=args.db_path,
        templates_db_path=args.templates_db_path,
        replace=args.replace,
    )

    print("\nCompletion CSV import summary")
    print(f"matched: {result['matched']}")
    print(f"imported: {result['imported']}")
    print(f"unmatched: {result['unmatched']}")
    print(f"ambiguous: {result['ambiguous']}")
    print(f"skipped_empty: {result['skipped_empty']}")


if __name__ == "__main__":
    main()
