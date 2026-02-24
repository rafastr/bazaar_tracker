import os
import sqlite3
import time
from typing import Any, Dict, List, Optional


class RunHistoryDb:
    def __init__(self, path: str) -> None:
        self.path = path
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def _init_schema(self) -> None:
        cur = self.conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                ended_at_unix INTEGER NOT NULL,
                screenshot_path TEXT,
                hero TEXT,
                rank INTEGER,
                metrics_json TEXT,
                is_confirmed INTEGER DEFAULT 0,
                notes TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS run_items (
                run_id INTEGER NOT NULL,
                socket_number INTEGER NOT NULL,
                template_id TEXT,
                size TEXT NOT NULL,
                PRIMARY KEY (run_id, socket_number),
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            )
            """
        )

        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_items_template_id ON run_items(template_id)"
        )

        self._ensure_column("runs", "hero TEXT")
        self._ensure_column("runs", "rank INTEGER")
        self._ensure_column("runs", "metrics_json TEXT")
        self._ensure_column("runs", "is_confirmed INTEGER DEFAULT 0")
        self._ensure_column("runs", "notes TEXT")

        self.conn.commit()


    def _ensure_column(self, table: str, coldef: str) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {coldef}")
            self.conn.commit()
        except sqlite3.OperationalError:
            # column probably exists
            pass


    def insert_run(self, board_items_sorted, screenshot_path: Optional[str], hero: Optional[str]) -> int:

        """
        board_items_sorted: list of dicts in socket order:
          {"socket_number": int, "size": str, "template_id": str, ...}

        Returns run_id.
        """
        ended_at = int(time.time())
        cur = self.conn.cursor()

        cur.execute(
            "INSERT INTO runs (ended_at_unix, screenshot_path, hero) VALUES (?, ?, ?)",
            (ended_at, screenshot_path, hero),
        )
        lastrowid = cur.lastrowid
        if lastrowid is None:
            raise RuntimeError("Failed to get lastrowid after inserting run")
        run_id = int(lastrowid)

        rows = []
        for it in board_items_sorted:
            socket = int(it["socket_number"])
            template_id = it.get("template_id")
            size = it.get("size")

            rows.append((run_id, socket, template_id, str(size)))

        cur.executemany(
            """
            INSERT INTO run_items (run_id, socket_number, template_id, size)
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )

        self.conn.commit()
        return run_id

    def update_run_rank(self, run_id: int, rank: int) -> None:
        cur = self.conn.cursor()
        cur.execute("UPDATE runs SET rank = ? WHERE run_id = ?", (rank, run_id))
        self.conn.commit()
