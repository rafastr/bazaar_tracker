import os
import sqlite3
import time
from typing import Any, Dict, List


class TemplatesDb:
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
            CREATE TABLE IF NOT EXISTS templates (
                template_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                heroes_json TEXT,
                size TEXT,
                tags_json TEXT,
                art_key TEXT,
                image_path TEXT,
                internal_name TEXT,
                version TEXT
                updated_at_unix INTEGER NOT NULL
            )
            """
        )

        # Safe migrations if table existed before
        self._ensure_column("templates", "hero TEXT")
        self._ensure_column("templates", "size TEXT")
        self._ensure_column("templates", "tags_json TEXT")
        self._ensure_column("templates", "art_key TEXT")
        self._ensure_column("templates", "image_path TEXT")
        self._ensure_column("templates", "updated_at_unix INTEGER NOT NULL DEFAULT 0")

        cur.execute("CREATE INDEX IF NOT EXISTS idx_templates_name ON templates(name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_templates_art_key ON templates(art_key)")

        self.conn.commit()


    def _ensure_column(self, table: str, coldef: str) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {coldef}")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass


    def upsert_templates(
        self,
        template_id: str,
        name: str,
        hero: Optional[str],
        size: Optional[str],
        tags_json: Optional[str],
        art_key: Optional[str],
    ) -> None:

        now = int(time.time())
        cur = self.conn.cursor()

        cur.executemany(
            """





            INSERT INTO templates (
                template_id, name, size,
                heroes_json, tags_json,
                art_key, internal_name, version
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(template_id) DO UPDATE SET
                name=excluded.name,
                size=excluded.size,
                heroes_json=excluded.heroes_json,
                tags_json=excluded.tags_json,
                art_key=excluded.art_key,
                internal_name=excluded.internal_name,
                version=excluded.version
            """,
            [
                (
                    r["template_id"],
                    r["name"],
                    r.get("size"),
                    r.get("heroes_json"),
                    r.get("tags_json"),
                    r.get("art_key"),
                    r.get("internal_name"),
                    r.get("version"),
                )
                for r in rows
            ],
        )
        self.conn.commit()
