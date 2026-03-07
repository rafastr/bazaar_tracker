import os
import sqlite3
import time
from typing import Any, Dict, List, Optional


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

        # Canonical schema (only what you want + image_path for local cache)
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
                version TEXT,
                updated_at_unix INTEGER NOT NULL
            )
            """
        )

        # Safe migrations if table existed before
        self._ensure_column("templates", "heroes_json TEXT")
        self._ensure_column("templates", "size TEXT")
        self._ensure_column("templates", "tags_json TEXT")
        self._ensure_column("templates", "art_key TEXT")
        self._ensure_column("templates", "image_path TEXT")
        self._ensure_column("templates", "internal_name TEXT")
        self._ensure_column("templates", "version TEXT")
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
            # Column probably exists
            pass

    def upsert_templates(self, rows: List[Dict[str, Any]]) -> None:
        """
        Bulk upsert templates.
        Each row dict should include:
          template_id (str), name (str)
        Optional:
          heroes_json (str), size (str), tags_json (str), art_key (str),
          internal_name (str), version (str)
        """
        if not rows:
            return

        now = int(time.time())
        cur = self.conn.cursor()

        cur.executemany(
            """
            INSERT INTO templates (
                template_id,
                name,
                heroes_json,
                size,
                tags_json,
                art_key,
                internal_name,
                version,
                updated_at_unix
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(template_id) DO UPDATE SET
                name=excluded.name,
                heroes_json=excluded.heroes_json,
                size=excluded.size,
                tags_json=excluded.tags_json,
                art_key=excluded.art_key,
                internal_name=excluded.internal_name,
                version=excluded.version,
                updated_at_unix=excluded.updated_at_unix
            """,
            [
                (
                    str(r["template_id"]),
                    str(r["name"]),
                    r.get("heroes_json"),
                    r.get("size"),
                    r.get("tags_json"),
                    r.get("art_key"),
                    r.get("internal_name"),
                    r.get("version"),
                    now,
                )
                for r in rows
            ],
        )

        self.conn.commit()

    def set_image_path(self, template_id: str, image_path: str) -> None:
        """
        Store local cached image path (relative preferred).
        Example: assets/images/items/<template_id>.webp
        """
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE templates SET image_path = ? WHERE template_id = ?",
            (image_path, template_id),
        )
        self.conn.commit()

    def get_missing_images(self, limit: int = 0) -> List[Dict[str, Any]]:
        """
        Returns templates where image_path is NULL/empty.
        Useful for the cache downloader script.
        """
        cur = self.conn.cursor()
        sql = """
            SELECT template_id, name
            FROM templates
            WHERE image_path IS NULL OR image_path='' 
              AND COALESCE(ignored, 0) = 0
            ORDER BY name ASC
        """
        if limit and limit > 0:
            sql += " LIMIT ?"
            cur.execute(sql, (int(limit),))
        else:
            cur.execute(sql)

        return [dict(r) for r in cur.fetchall()]
