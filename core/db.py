import sqlite3
import time
from typing import Dict, Optional


class Db:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
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
            CREATE TABLE IF NOT EXISTS instance_map (
                instance_id TEXT PRIMARY KEY,
                template_id TEXT NOT NULL,
                first_seen_unix INTEGER NOT NULL,
                last_seen_unix INTEGER NOT NULL
            )
            """
        )

        # Helpful index
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_instance_map_template_id
            ON instance_map(template_id)
            """
        )

        self.conn.commit()

    def load_instance_map(self) -> Dict[str, str]:
        cur = self.conn.cursor()
        cur.execute("SELECT instance_id, template_id FROM instance_map")
        rows = cur.fetchall()
        return {row["instance_id"]: row["template_id"] for row in rows}

    def upsert_instance_map(self, instance_id: str, template_id: str) -> None:
        now = int(time.time())
        cur = self.conn.cursor()

        # If it exists, update template_id + last_seen_unix (and keep first_seen_unix)
        # If it doesn't exist, insert with first_seen_unix=last_seen_unix=now
        cur.execute(
            """
            INSERT INTO instance_map (instance_id, template_id, first_seen_unix, last_seen_unix)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(instance_id) DO UPDATE SET
                template_id=excluded.template_id,
                last_seen_unix=excluded.last_seen_unix
            """,
            (instance_id, template_id, now, now),
        )
        self.conn.commit()

    def get_template_id(self, instance_id: str) -> Optional[str]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT template_id FROM instance_map WHERE instance_id = ?",
            (instance_id,),
        )
        row = cur.fetchone()
        return row["template_id"] if row else None

