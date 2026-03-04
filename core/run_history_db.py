import os
import sqlite3
import time
from typing import Any, Dict, List, Optional
import threading

from core.ocr_metrics import extract_run_metrics
from core.ocr_rois import ROIS


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

        # Checklist of items used for runs.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS item_hero_wins (
              template_id TEXT NOT NULL,
              hero TEXT NOT NULL,
              win_count INTEGER NOT NULL DEFAULT 0,
              updated_at_unix INTEGER NOT NULL,
              PRIMARY KEY (template_id, hero)
            )
            """
        )
        
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_item_hero_wins_template_id ON item_hero_wins(template_id)"
        )
        
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_item_hero_wins_hero ON item_hero_wins(hero)"
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


        # Color of heroes for UI
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hero_colors (
                hero TEXT PRIMARY KEY,
                color TEXT NOT NULL
            )
            """
        )

        cur.executemany(
            """
            INSERT OR IGNORE INTO hero_colors(hero, color)
            VALUES (?, ?)
            """,
            [
                ("Vanessa", "#c73b3b"),
                ("Dooley", "#b58900"),
                ("Jules", "#7a3bd1"),
                ("Mak", "#1f6b3d"),
                ("Pygmalien", "#3b6bc7"),
                ("Stelle", "#e2c53b"),
                ("Karnok", "#a13b3b"),
            ],
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
        # Auto OCR metrics if we have a screenshot (async)
        if screenshot_path:
            self.run_ocr_for_run_async(run_id, screenshot_path, ocr_version="v1")

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

    def confirm_run(self, run_id: int, confirmed: bool = True, templates_db_path: str | None = None) -> None:
        self.upsert_run_override(run_id, is_confirmed=1 if confirmed else 0)
    
        if confirmed and templates_db_path:
            # Apply achievement progress only when user verifies
            try:
                self.apply_confirmed_run_item_wins(run_id, templates_db_path)
            except Exception as e:
                print(f"[ACH] apply_confirmed_run_item_wins failed for run {run_id}: {e}")

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
    
    
    def run_ocr_for_run(self, run_id: int, screenshot_path: str, ocr_version: str = "v1") -> None:
        """
        Extract OCR metrics from the run screenshot and store in run_metrics.
        Safe to call multiple times (upsert).
        """
        if not screenshot_path:
            return

        # Import here to avoid import overhead / circulars at module import time
        from core.ocr_metrics import extract_run_metrics
        from core.ocr_rois import ROIS

        metrics = extract_run_metrics(screenshot_path, ROIS, ocr_version=ocr_version)

        self.upsert_run_metrics(
            run_id,
            wins=metrics.get("wins"),
            max_health=metrics.get("max_health"),
            prestige=metrics.get("prestige"),
            level=metrics.get("level"),
            income=metrics.get("income"),
            gold=metrics.get("gold"),
            won=metrics.get("won"),
            ocr_json=metrics.get("ocr_json"),
            ocr_version=metrics.get("ocr_version"),
        )


    def run_ocr_for_run_async(self, run_id: int, screenshot_path: str, ocr_version: str = "v1") -> None:
        """
        Fire-and-forget OCR in a background thread.
        Uses a new DB connection (important for SQLite thread safety).
        """
    
        db_path = self.path  # capture now
    
        def _job() -> None:
            try:
                db = RunHistoryDb(db_path)
                try:
                    db.run_ocr_for_run(run_id, screenshot_path, ocr_version=ocr_version)
                finally:
                    db.close()
            except Exception as e:
                print(f"[OCR] failed for run {run_id}: {e}")
    
        t = threading.Thread(target=_job, daemon=True)
        t.start()


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


    def apply_confirmed_run_item_wins(self, run_id: int, templates_db_path: str) -> None:
        """
        If run is confirmed and won, increment item_hero_wins for each item on board.
        Uses effective hero (override if present) and effective items (apply item overrides).
        """
        cur = self.conn.cursor()
    
        # Need run + overrides + metrics
        cur.execute(
            """
            SELECT
              r.hero AS hero_base,
              o.hero_override,
              o.is_confirmed,
              m.won
            FROM runs r
            LEFT JOIN run_overrides o ON o.run_id = r.run_id
            LEFT JOIN run_metrics  m ON m.run_id = r.run_id
            WHERE r.run_id = ?
            """,
            (run_id,),
        )
        row = cur.fetchone()
        if not row:
            return
    
        is_confirmed = int(row["is_confirmed"] or 0)
        won = int(row["won"] or 0)
        if not is_confirmed or not won:
            return
    
        hero_eff = (row["hero_override"] or row["hero_base"] or "").strip()
        if not hero_eff:
            return
    
        # Base items
        cur.execute(
            """
            SELECT socket_number, template_id
            FROM run_items
            WHERE run_id=?
            """,
            (run_id,),
        )
        base = {int(r["socket_number"]): (r["template_id"] or "").strip() for r in cur.fetchall()}
    
        # Overrides (if template_id_override is NULL, we treat it as "no change"; if it's "", you may be using it to clear)
        cur.execute(
            """
            SELECT socket_number, template_id_override
            FROM run_item_overrides
            WHERE run_id=?
            """,
            (run_id,),
        )
        ov = {int(r["socket_number"]): r["template_id_override"] for r in cur.fetchall()}
    
        # Effective template_id per socket
        effective: list[str] = []
        for sock, tid in base.items():
            if sock in ov and ov[sock] is not None:
                tid_eff = (ov[sock] or "").strip()
            else:
                tid_eff = tid
            if tid_eff:
                effective.append(tid_eff)
    
        # De-dup (same item could theoretically appear twice; you can choose to count once or twice)
        # For checklist booleans, counting once is enough.
        template_ids = sorted(set(effective))
        if not template_ids:
            return
    
        now = self._now()
        rows = [(tid, hero_eff, now) for tid in template_ids]
    
        cur.executemany(
            """
            INSERT INTO item_hero_wins (template_id, hero, win_count, updated_at_unix)
            VALUES (?, ?, 1, ?)
            ON CONFLICT(template_id, hero) DO UPDATE SET
              win_count = win_count + 1,
              updated_at_unix = excluded.updated_at_unix
            """,
            rows,
        )
        self.conn.commit()


    def rebuild_item_hero_wins(self) -> None:
        """
        Recompute item_hero_wins from all confirmed + won runs.
        This keeps achievements consistent even if confirmed runs are edited later.
        """
        cur = self.conn.cursor()
        now = self._now()
    
        # wipe
        cur.execute("DELETE FROM item_hero_wins")
    
        # all confirmed + won runs
        cur.execute(
            """
            SELECT r.run_id,
                   r.hero AS hero_base,
                   o.hero_override,
                   o.is_confirmed,
                   m.won
            FROM runs r
            LEFT JOIN run_overrides o ON o.run_id = r.run_id
            LEFT JOIN run_metrics  m ON m.run_id = r.run_id
            WHERE COALESCE(o.is_confirmed, 0) = 1
              AND COALESCE(m.won, 0) = 1
            """
        )
        runs = cur.fetchall()
    
        for rr in runs:
            run_id = int(rr["run_id"])
            hero_eff = (rr["hero_override"] or rr["hero_base"] or "").strip()
            if not hero_eff:
                continue
    
            # base items
            cur.execute(
                "SELECT socket_number, template_id FROM run_items WHERE run_id=?",
                (run_id,),
            )
            base = {int(r["socket_number"]): (r["template_id"] or "").strip() for r in cur.fetchall()}
    
            # overrides
            cur.execute(
                "SELECT socket_number, template_id_override FROM run_item_overrides WHERE run_id=?",
                (run_id,),
            )
            ov = {int(r["socket_number"]): r["template_id_override"] for r in cur.fetchall()}
    
            # effective items
            effective = []
            for sock, tid in base.items():
                if sock in ov and ov[sock] is not None:
                    tid_eff = (ov[sock] or "").strip()
                else:
                    tid_eff = tid
                if tid_eff:
                    effective.append(tid_eff)
    
            # unique for checklist purposes
            template_ids = set(effective)
    
            for tid in template_ids:
                cur.execute(
                    """
                    INSERT INTO item_hero_wins (template_id, hero, win_count, updated_at_unix)
                    VALUES (?, ?, 1, ?)
                    ON CONFLICT(template_id, hero) DO UPDATE SET
                      win_count = win_count + 1,
                      updated_at_unix = excluded.updated_at_unix
                    """,
                    (tid, hero_eff, now),
                )
    
        self.conn.commit()
