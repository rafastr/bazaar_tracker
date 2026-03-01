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

        # OCR/derived metrics (safe to recompute/overwrite)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS run_metrics (
                run_id INTEGER PRIMARY KEY,

                wins INTEGER,
                max_health INTEGER,
                prestige INTEGER,
                level INTEGER,
                income INTEGER,
                gold INTEGER,

                won INTEGER,            -- 0/1

                ocr_json TEXT,          -- raw OCR/debug payload
                ocr_version TEXT,
                updated_at_unix INTEGER NOT NULL,

                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            )
            """
        )

        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_metrics_updated ON run_metrics(updated_at_unix)"
        )

        # Manual overrides for run-level fields
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS run_overrides (
                run_id INTEGER PRIMARY KEY,
                hero_override TEXT,
                rank_override INTEGER,
                notes TEXT,
                is_confirmed INTEGER DEFAULT 0,
                updated_at_unix INTEGER NOT NULL,
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            )
            """
        )

        # Manual overrides per socket
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS run_item_overrides (
                run_id INTEGER NOT NULL,
                socket_number INTEGER NOT NULL,
                template_id_override TEXT,
                size_override TEXT,
                note TEXT,
                updated_at_unix INTEGER NOT NULL,
                PRIMARY KEY (run_id, socket_number),
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            )
            """
        )

        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_item_overrides_template_id ON run_item_overrides(template_id_override)"
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


    def _now(self) -> int:
        return int(time.time())


    def upsert_run_override(
        self,
        run_id: int,
        hero_override: Optional[str] = None,
        rank_override: Optional[int] = None,
        notes: Optional[str] = None,
        is_confirmed: Optional[int] = None,
    ) -> None:
        """
        Upserts run_overrides row. Only updates fields you pass as not-None.
        """
        now = self._now()
        cur = self.conn.cursor()

        # Ensure row exists
        cur.execute(
            """
            INSERT INTO run_overrides (run_id, updated_at_unix)
            VALUES (?, ?)
            ON CONFLICT(run_id) DO UPDATE SET updated_at_unix=excluded.updated_at_unix
            """,
            (run_id, now),
        )

        # Patch fields selectively
        if hero_override is not None:
            cur.execute(
                "UPDATE run_overrides SET hero_override=?, updated_at_unix=? WHERE run_id=?",
                (hero_override, now, run_id),
            )
        if rank_override is not None:
            cur.execute(
                "UPDATE run_overrides SET rank_override=?, updated_at_unix=? WHERE run_id=?",
                (int(rank_override), now, run_id),
            )
        if notes is not None:
            notes = notes.strip()
            if notes == "":
                notes = None

            cur.execute(
                "UPDATE run_overrides SET notes=?, updated_at_unix=? WHERE run_id=?",
                (notes, now, run_id),
            )
        if is_confirmed is not None:
            cur.execute(
                "UPDATE run_overrides SET is_confirmed=?, updated_at_unix=? WHERE run_id=?",
                (int(is_confirmed), now, run_id),
            )

        self.conn.commit()

    def confirm_run(self, run_id: int, confirmed: bool = True) -> None:
        self.upsert_run_override(run_id, is_confirmed=1 if confirmed else 0)

    def set_run_hero_override(self, run_id: int, hero: str) -> None:
        self.upsert_run_override(run_id, hero_override=hero)

    def set_run_rank_override(self, run_id: int, rank: int) -> None:
        self.upsert_run_override(run_id, rank_override=int(rank))

    def set_run_notes(self, run_id: int, notes: str) -> None:
        self.upsert_run_override(run_id, notes=notes)

    def upsert_run_metrics(
        self,
        run_id: int,
        wins: Optional[int] = None,
        max_health: Optional[int] = None,
        prestige: Optional[int] = None,
        level: Optional[int] = None,
        income: Optional[int] = None,
        gold: Optional[int] = None,
        won: Optional[bool] = None,
        ocr_json: Optional[str] = None,
        ocr_version: Optional[str] = None,
    ) -> None:
        now = self._now()
        cur = self.conn.cursor()
    
        cur.execute(
            """
            INSERT INTO run_metrics (run_id, updated_at_unix)
            VALUES (?, ?)
            ON CONFLICT(run_id) DO UPDATE SET updated_at_unix=excluded.updated_at_unix
            """,
            (int(run_id), now),
        )
    
        def set_int(col: str, v: Optional[int]) -> None:
            if v is None:
                return
            cur.execute(
                f"UPDATE run_metrics SET {col}=?, updated_at_unix=? WHERE run_id=?",
                (int(v), now, int(run_id)),
            )
    
        set_int("wins", wins)
        set_int("max_health", max_health)
        set_int("prestige", prestige)
        set_int("level", level)
        set_int("income", income)
        set_int("gold", gold)
    
        if won is not None:
            cur.execute(
                "UPDATE run_metrics SET won=?, updated_at_unix=? WHERE run_id=?",
                (1 if won else 0, now, int(run_id)),
            )
    
        if ocr_json is not None:
            cur.execute(
                "UPDATE run_metrics SET ocr_json=?, updated_at_unix=? WHERE run_id=?",
                (ocr_json, now, int(run_id)),
            )
    
        if ocr_version is not None:
            cur.execute(
                "UPDATE run_metrics SET ocr_version=?, updated_at_unix=? WHERE run_id=?",
                (ocr_version, now, int(run_id)),
            )
    
        self.conn.commit()
    
    
    def get_run_metrics(self, run_id: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT run_id, wins, max_health, prestige, level, income, gold, won,
                   ocr_json, ocr_version, updated_at_unix
            FROM run_metrics
            WHERE run_id=?
            """,
            (int(run_id),),
        )
        row = cur.fetchone()
        return dict(row) if row else None


    def upsert_item_override(
        self,
        run_id: int,
        socket_number: int,
        template_id_override: Optional[str] = None,
        size_override: Optional[str] = None,
        note: Optional[str] = None,
    ) -> None:
        now = self._now()
        cur = self.conn.cursor()

        cur.execute(
            """
            INSERT INTO run_item_overrides (run_id, socket_number, updated_at_unix)
            VALUES (?, ?, ?)
            ON CONFLICT(run_id, socket_number) DO UPDATE SET updated_at_unix=excluded.updated_at_unix
            """,
            (run_id, int(socket_number), now),
        )

        if template_id_override is not None:
            cur.execute(
                """
                UPDATE run_item_overrides
                SET template_id_override=?, updated_at_unix=?
                WHERE run_id=? AND socket_number=?
                """,
                (template_id_override, now, run_id, int(socket_number)),
            )
        if size_override is not None:
            cur.execute(
                """
                UPDATE run_item_overrides
                SET size_override=?, updated_at_unix=?
                WHERE run_id=? AND socket_number=?
                """,
                (size_override, now, run_id, int(socket_number)),
            )
        if note is not None:
            cur.execute(
                """
                UPDATE run_item_overrides
                SET note=?, updated_at_unix=?
                WHERE run_id=? AND socket_number=?
                """,
                (note, now, run_id, int(socket_number)),
            )

        self.conn.commit()

    def clear_item_override(self, run_id: int, socket_number: int) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "DELETE FROM run_item_overrides WHERE run_id=? AND socket_number=?",
            (run_id, int(socket_number)),
        )
        self.conn.commit()
