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
        self._ensure_column("runs", "season_id INTEGER")

        self.conn.commit()

        # Achievements progress
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS achievements (
              key TEXT PRIMARY KEY,
              title TEXT NOT NULL,
              description TEXT NOT NULL
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS achievement_unlocks (
              key TEXT NOT NULL,
              unlocked_at_unix INTEGER NOT NULL,
              run_id INTEGER, -- optional: which run unlocked it
              meta_json TEXT, -- optional details
              PRIMARY KEY (key),
              FOREIGN KEY (run_id) REFERENCES runs(run_id)
            );
            """
        )


        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS item_firsts (
              template_id TEXT PRIMARY KEY,
              first_win_run_id INTEGER,
              first_cross_win_run_id INTEGER,
              FOREIGN KEY (first_win_run_id) REFERENCES runs(run_id),
              FOREIGN KEY (first_cross_win_run_id) REFERENCES runs(run_id)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_item_firsts_first_win ON item_firsts(first_win_run_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_item_firsts_first_cross ON item_firsts(first_cross_win_run_id)")


        # Seed achievement definitions so the UI always has the full list.
        self.ensure_achievements_seeded()

        self.conn.commit()


    def _ensure_column(self, table: str, coldef: str) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {coldef}")
            self.conn.commit()
        except sqlite3.OperationalError:
            # column probably exists
            pass

    def insert_run(
        self,
        board_items_sorted,
        screenshot_path: Optional[str],
        hero: Optional[str],
        season_id: Optional[int] = None,
    ) -> int:
        """
        board_items_sorted: list of dicts in socket order:
          {"socket_number": int, "size": str, "template_id": str, ...}
    
        Returns run_id.
        """
        ended_at = int(time.time())
        cur = self.conn.cursor()
    
        # Fallback: if we couldn't detect season, reuse last known season.
        if season_id is None:
            season_id = self.get_last_season_id()
    
        # IMPORTANT: always insert the run (not only when season_id is None)
        cur.execute(
            "INSERT INTO runs (ended_at_unix, screenshot_path, hero, season_id) VALUES (?, ?, ?, ?)",
            (ended_at, screenshot_path, hero, season_id),
        )
    
        run_id = cur.lastrowid
        if not run_id:
            raise RuntimeError("Failed to get lastrowid after inserting run")
        run_id = int(run_id)
    
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
    

    def get_last_season_id(self) -> Optional[int]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT season_id
            FROM runs
            WHERE season_id IS NOT NULL
            ORDER BY run_id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            return None
        v = row["season_id"]
        return int(v) if v is not None else None
    
    
    def set_run_season_id(self, run_id: int, season_id: Optional[int]) -> None:
        cur = self.conn.cursor()
        cur.execute("UPDATE runs SET season_id=? WHERE run_id=?", (season_id, int(run_id)))
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

    def ensure_achievements_seeded(self) -> None:
        """
        Inserts achievement definitions if missing.
        Safe to call many times.
        """
        defs = [
            ("hero_champion", "Hero Champion", "Win at least once with every hero."),
            ("featherweight_win", "Featherweight Win", "Win a run using only Small items."),
            ("middleweight_win", "Middleweight Win", "Win a run using only Medium items."),
            ("heavyweight_win", "Heavyweight Win", "Win a run using only Large items."),
            ("solo_carry", "Solo Carry", "Win a run with a single item on the final board."),
            ("commoners_crown", "Commoner’s Crown", "Win a run using only Common items."),
            ("foreign_exchange", "Foreign Exchange", "Win a run where every item is from heroes other than the hero played."),
            ("win_streak_15", "15 Win Streak", "Reach a win streak of 15."),
            ("grand_tour", "Grand Tour", "Win with every hero in consecutive wins (no repeats; losses break the chain)."),
            ("collector", "Collector", "Use every item at least once."),
            ("cross_class_collector", "Cross-Class Collector", "Use every item in a win with a hero that is not the item's origin."),
            ("hp_millionaire", "HP Millionaire", "Win a run with 25,000+ Max HP."),
            ("prestige_25", "Prestige 25", "Win a run with 25+ Prestige."),
            ("level_20", "Level 20", "Win a run with 20+ level."),
            ("income_25", "Income 25", "Win a run with 25+ income."),
            ("gold_500", "Gold Hoarder", "Win a run with 500+ gold in the bank."),
        ]

        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT OR IGNORE INTO achievements(key, title, description)
            VALUES (?, ?, ?)
            """,
            defs,
        )

    def rebuild_achievements(self, templates_db_path: str) -> None:
        """
        Recompute achievement_unlocks from current DB state.
        Deterministic + rebuildable (edits/unconfirms won't drift).
        """
        import json

        cur = self.conn.cursor()
        now = self._now()

        # Ensure definitions exist
        self.ensure_achievements_seeded()

        # Clear unlocks
        cur.execute("DELETE FROM achievement_unlocks")

        # ---- load templates origins: template_id -> set(origin heroes), and is_common ----
        tconn = sqlite3.connect(templates_db_path)
        tconn.row_factory = sqlite3.Row
        try:
            tcur = tconn.cursor()
            tcur.execute("SELECT template_id, heroes_json FROM templates WHERE template_id IS NOT NULL")
            template_rows = tcur.fetchall()
        finally:
            tconn.close()

        def parse_origin_set(heroes_json: str) -> set[str]:
            s = (heroes_json or "").strip()
            if not s:
                return set()
            try:
                data = json.loads(s)
            except Exception:
                return set()
            if isinstance(data, list):
                vals = data
            elif isinstance(data, dict):
                vals = data.get("heroes", [])
            else:
                vals = [data]
            out: set[str] = set()
            for v in vals:
                if isinstance(v, str):
                    name = v.strip()
                    if name:
                        out.add(name)
            return out

        origin_by_tid: dict[str, set[str]] = {}
        is_common_tid: dict[str, bool] = {}
        for r in template_rows:
            tid = (r["template_id"] or "").strip()
            if not tid:
                continue
            origins = parse_origin_set(r["heroes_json"] or "")
            origin_by_tid[tid] = origins
            is_common_tid[tid] = any(h.lower() == "common" for h in origins) or (not origins)

        # All heroes known from templates (excluding Common)
        all_heroes: set[str] = set()
        for origins in origin_by_tid.values():
            for h in origins:
                if h.strip().lower() != "common":
                    all_heroes.add(h.strip())

        # ---- get confirmed runs in order, with effective hero, and won flag ----
        cur.execute(
            """
            SELECT
              r.run_id,
              r.ended_at_unix,
              r.hero AS hero_base,
              o.hero_override,
              COALESCE(o.is_confirmed, 0) AS is_confirmed,
              COALESCE(m.won, 0) AS won,
              m.wins AS wins,
              m.max_health AS max_health,
              m.prestige AS prestige,
              m.level AS level,
              m.income AS income,
              m.gold AS gold
            FROM runs r
            LEFT JOIN run_overrides o ON o.run_id = r.run_id
            LEFT JOIN run_metrics  m ON m.run_id = r.run_id
            WHERE COALESCE(o.is_confirmed, 0) = 1
            ORDER BY r.run_id ASC
            """
        )
        runs = cur.fetchall()

        # Helpers to load effective items for a run (template_id + size)
        def get_effective_items(run_id: int) -> list[dict]:
            # base
            cur.execute(
                "SELECT socket_number, template_id, size FROM run_items WHERE run_id=?",
                (run_id,),
            )
            base = {int(r["socket_number"]): {"tid": (r["template_id"] or "").strip(), "size": (r["size"] or "").strip().lower()} for r in cur.fetchall()}

            # overrides
            cur.execute(
                "SELECT socket_number, template_id_override, size_override FROM run_item_overrides WHERE run_id=?",
                (run_id,),
            )
            ov = {int(r["socket_number"]): {"tid": r["template_id_override"], "size": r["size_override"]} for r in cur.fetchall()}

            out = []
            for sock, b in base.items():
                tid = b["tid"]
                size = b["size"]

                if sock in ov:
                    if ov[sock]["tid"] is not None:
                        tid = (ov[sock]["tid"] or "").strip()
                    if ov[sock]["size"] is not None:
                        size = (ov[sock]["size"] or "").strip().lower()

                if tid:
                    out.append({"template_id": tid, "size": size or "small"})
            return out

        def unlock(key: str, run_id: int | None = None, meta: dict | None = None) -> None:
            cur.execute(
                """
                INSERT OR IGNORE INTO achievement_unlocks(key, unlocked_at_unix, run_id, meta_json)
                VALUES (?, ?, ?, ?)
                """,
                (
                    key,
                    now,
                    int(run_id) if run_id is not None else None,
                    json.dumps(meta, ensure_ascii=False) if meta else None,
                ),
            )

        # ---- Trackers for streaks / hero coverage ----
        heroes_won_with: set[str] = set()

        # win streak of consecutive wins
        cur_win_streak = 0
        best_win_streak = 0

        # consecutive unique-hero win chain
        unique_chain: list[str] = []  # list of heroes in current chain
        best_unique_chain = 0

        # ---- scan runs ----
        for rr in runs:
            run_id = int(rr["run_id"])
            hero_eff = (rr["hero_override"] or rr["hero_base"] or "").strip() or "(unknown)"
            won = int(rr["won"] or 0) == 1

            # streak accounting
            if won:
                cur_win_streak += 1
                best_win_streak = max(best_win_streak, cur_win_streak)
            else:
                cur_win_streak = 0

            # unique chain accounting (wins only)
            if won:
                if hero_eff in unique_chain:
                    # repeat breaks chain; restart at this hero
                    unique_chain = [hero_eff]
                else:
                    unique_chain.append(hero_eff)
                best_unique_chain = max(best_unique_chain, len(unique_chain))
            else:
                unique_chain = []

            # If not won, skip per-win achievements
            if not won:
                continue

            max_health = rr["max_health"]
            prestige = rr["prestige"]
            level = rr["level"]
            income = rr["income"]
            gold = rr["gold"]
            
            # Metric-threshold achievements (wins only)
            if isinstance(max_health, int) and max_health >= 1_000_000:
                unlock("hp_millionaire", run_id, {"max_health": max_health})
            
            if isinstance(prestige, int) and prestige >= 25:
                unlock("prestige_25", run_id, {"prestige": prestige})
            
            if isinstance(level, int) and level >= 20:
                unlock("level_20", run_id, {"level": level})
            
            if isinstance(income, int) and income >= 25:
                unlock("income_25", run_id, {"income": income})
            
            if isinstance(gold, int) and gold >= 500:
                unlock("gold_500", run_id, {"gold": gold})
            

            # record hero win
            if hero_eff != "(unknown)":
                heroes_won_with.add(hero_eff)

            items = get_effective_items(run_id)
            non_empty = items[:]  # already excludes empty
            sizes = {it["size"] for it in non_empty}
            tids = [it["template_id"] for it in non_empty]

            # ---- per-run achievements ----
            if len(non_empty) == 1:
                unlock("solo_carry", run_id)

            if non_empty:
                # only size X (ignore empty sockets)
                if sizes == {"small"}:
                    unlock("featherweight_win", run_id)
                if sizes == {"medium"}:
                    unlock("middleweight_win", run_id)
                if sizes == {"large"}:
                    unlock("heavyweight_win", run_id)

                # only common items
                all_common = all(is_common_tid.get(tid, False) for tid in tids)
                if all_common:
                    unlock("commoners_crown", run_id)

                # foreign exchange: every non-common item must NOT belong to played hero
                ok = True
                for tid in tids:
                    if is_common_tid.get(tid, False):
                        continue
                    origins = origin_by_tid.get(tid, set())
                    if hero_eff in origins:
                        ok = False
                        break
                if ok:
                    unlock("foreign_exchange", run_id)

        # ---- end-of-scan achievements ----

        # win streak 15
        if best_win_streak >= 15:
            unlock("win_streak_15", None, {"best_win_streak": best_win_streak})

        # hero champion (win with every hero)
        if all_heroes and all_heroes.issubset(heroes_won_with):
            unlock("hero_champion", None, {"heroes": sorted(all_heroes)})

        # grand tour (unique-hero chain reaches all heroes)
        # (only meaningful if you have hero list)
        if all_heroes and best_unique_chain >= len(all_heroes):
            unlock("grand_tour", None, {"best_unique_chain": best_unique_chain, "needed": len(all_heroes)})

        # collector / cross-class collector from item checklist state
        # We derive from templates list so it adapts as items update.
        all_tids = [tid for tid in origin_by_tid.keys()]
        if all_tids:
            # Build won_any and won_other using item_hero_wins
            cur.execute(
                """
                SELECT template_id, hero
                FROM item_hero_wins
                WHERE win_count > 0
                """
            )
            rows = cur.fetchall()

            winners_by_item: dict[str, set[str]] = {}
            for r in rows:
                tid = (r["template_id"] or "").strip()
                h = (r["hero"] or "").strip()
                if tid and h:
                    winners_by_item.setdefault(tid, set()).add(h)

            # collector: any hero has won with it
            all_won_any = True
            all_won_other = True
            for tid in all_tids:
                winners = winners_by_item.get(tid, set())
                if not winners:
                    all_won_any = False
                    all_won_other = False
                    break

                origins = origin_by_tid.get(tid, set())
                common = is_common_tid.get(tid, False)

                if common:
                    # your rule: any win counts as "other hero"
                    pass
                else:
                    if not any(h not in origins for h in winners):
                        all_won_other = False

            if all_won_any:
                unlock("collector", None)
            if all_won_other:
                unlock("cross_class_collector", None)

        # ensure seeded and commit unlocks
        self.ensure_achievements_seeded()
        self.conn.commit()

    def rebuild_item_firsts(self, templates_db_path: str) -> None:
        """
        Recompute item_firsts from confirmed+won runs in ascending run order.
        - first_win_run_id: first time item appears in any confirmed win.
        - first_cross_win_run_id: first time item appears in a confirmed win
          where the winning hero is NOT in item's origin heroes
          (Common items count as cross immediately).
        Deterministic and safe to rebuild after edits/unconfirms.
        """
        import json
    
        cur = self.conn.cursor()
    
        # ---- load origins from templates DB ----
        tconn = sqlite3.connect(templates_db_path)
        tconn.row_factory = sqlite3.Row
        try:
            tcur = tconn.cursor()
            tcur.execute("SELECT template_id, heroes_json FROM templates WHERE template_id IS NOT NULL")
            rows = tcur.fetchall()
        finally:
            tconn.close()
    
        def parse_origin_set(heroes_json: str) -> set[str]:
            s = (heroes_json or "").strip()
            if not s:
                return set()
            try:
                data = json.loads(s)
            except Exception:
                return set()
            if isinstance(data, list):
                vals = data
            elif isinstance(data, dict):
                vals = data.get("heroes", [])
            else:
                vals = [data]
            out: set[str] = set()
            for v in vals:
                if isinstance(v, str):
                    name = v.strip()
                    if name:
                        out.add(name)
            return out
    
        origin_by_tid: dict[str, set[str]] = {}
        is_common_tid: dict[str, bool] = {}
        for r in rows:
            tid = (r["template_id"] or "").strip()
            if not tid:
                continue
            origins = parse_origin_set(r["heroes_json"] or "")
            origin_by_tid[tid] = origins
            is_common_tid[tid] = any(h.lower() == "common" for h in origins) or (not origins)
    
        # ---- wipe ----
        cur.execute("DELETE FROM item_firsts")
    
        # ---- confirmed+won runs in order ----
        cur.execute(
            """
            SELECT
              r.run_id,
              r.hero AS hero_base,
              o.hero_override
            FROM runs r
            LEFT JOIN run_overrides o ON o.run_id = r.run_id
            LEFT JOIN run_metrics  m ON m.run_id = r.run_id
            WHERE COALESCE(o.is_confirmed, 0) = 1
              AND COALESCE(m.won, 0) = 1
            ORDER BY r.run_id ASC
            """
        )
        win_runs = cur.fetchall()
    
        seen_any: set[str] = set()
        seen_cross: set[str] = set()
    
        def effective_items(run_id: int) -> list[str]:
            # base
            cur.execute("SELECT socket_number, template_id FROM run_items WHERE run_id=?", (run_id,))
            base = {int(r["socket_number"]): (r["template_id"] or "").strip() for r in cur.fetchall()}
    
            # overrides
            cur.execute("SELECT socket_number, template_id_override FROM run_item_overrides WHERE run_id=?", (run_id,))
            ov = {int(r["socket_number"]): r["template_id_override"] for r in cur.fetchall()}
    
            out: list[str] = []
            for sock, tid in base.items():
                if sock in ov and ov[sock] is not None:
                    tid_eff = (ov[sock] or "").strip()
                else:
                    tid_eff = tid
                if tid_eff:
                    out.append(tid_eff)
    
            # unique per run is fine for “first time”
            return sorted(set(out))
    
        for rr in win_runs:
            run_id = int(rr["run_id"])
            hero_eff = (rr["hero_override"] or rr["hero_base"] or "").strip() or "(unknown)"
    
            tids = effective_items(run_id)
            for tid in tids:
                if tid not in seen_any:
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO item_firsts(template_id, first_win_run_id)
                        VALUES (?, ?)
                        """,
                        (tid, run_id),
                    )
                    seen_any.add(tid)
    
                # cross-hero logic
                if tid in seen_cross:
                    continue
    
                if is_common_tid.get(tid, False):
                    cur.execute(
                        """
                        INSERT INTO item_firsts(template_id, first_cross_win_run_id)
                        VALUES (?, ?)
                        ON CONFLICT(template_id) DO UPDATE SET
                          first_cross_win_run_id=COALESCE(item_firsts.first_cross_win_run_id, excluded.first_cross_win_run_id)
                        """,
                        (tid, run_id),
                    )
                    seen_cross.add(tid)
                    continue
    
                origins = origin_by_tid.get(tid, set())
                if hero_eff and hero_eff not in origins:
                    cur.execute(
                        """
                        INSERT INTO item_firsts(template_id, first_cross_win_run_id)
                        VALUES (?, ?)
                        ON CONFLICT(template_id) DO UPDATE SET
                          first_cross_win_run_id=COALESCE(item_firsts.first_cross_win_run_id, excluded.first_cross_win_run_id)
                        """,
                        (tid, run_id),
                    )
                    seen_cross.add(tid)
    
        self.conn.commit()

