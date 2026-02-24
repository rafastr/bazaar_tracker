import os
import sqlite3
from typing import Any, Dict, List, Optional


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def list_runs(run_history_db_path: str, limit: int = 20) -> List[Dict[str, Any]]:
    conn = _connect(run_history_db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT run_id, ended_at_unix, screenshot_path
            FROM runs
            ORDER BY run_id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_last_run_id(run_history_db_path: str) -> Optional[int]:
    conn = _connect(run_history_db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT run_id
            FROM runs
            ORDER BY run_id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        return int(row["run_id"]) if row else None
    finally:
        conn.close()


def get_run_board(
    run_history_db_path: str,
    templates_db_path: str,
    run_id: int,
) -> Dict[str, Any]:
    rh = _connect(run_history_db_path)
    td = _connect(templates_db_path)
    try:
        cur = rh.cursor()
        cur.execute(
            "SELECT run_id, ended_at_unix, screenshot_path, hero, rank FROM runs WHERE run_id = ?",
            (run_id,),
        )
        run_row = cur.fetchone()
        if not run_row:
            raise RuntimeError(f"Run {run_id} not found in {run_history_db_path}")

        cur.execute(
            """
            SELECT socket_number, template_id, size
            FROM run_items
            WHERE run_id = ?
            ORDER BY socket_number ASC
            """,
            (run_id,),
        )
        items = cur.fetchall()

        # Resolve names from templates DB (if template_id not null)
        resolved_items: List[Dict[str, Any]] = []
        tcur = td.cursor()

        for it in items:
            template_id = it["template_id"]
            name: Optional[str] = None
            art_key: Optional[str] = None

            if template_id:
                tcur.execute(
                    "SELECT name, art_key FROM templates WHERE template_id = ?",
                    (template_id,),
                )
                trow = tcur.fetchone()
                if trow:
                    name = trow["name"]
                    art_key = trow["art_key"]

            resolved_items.append(
                {
                    "socket_number": it["socket_number"],
                    "size": it["size"],
                    "template_id": template_id,
                    "name": name,
                    "art_key": art_key,
                }
            )

        return {
            "run_id": run_row["run_id"],
            "ended_at_unix": run_row["ended_at_unix"],
            "hero": run_row["hero"],
            "rank": run_row["rank"],           "screenshot_path": run_row["screenshot_path"],
            "items": resolved_items,
        }
    finally:
        rh.close()
        td.close()
