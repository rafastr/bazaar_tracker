import os
import sqlite3
from typing import Any, Dict, List, Optional

from .run_history_db import RunHistoryDb


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def list_runs(run_history_db_path: str, limit: int = 20) -> List[Dict[str, Any]]:
    """
    List recent runs with effective hero/rank (applies overrides when present).
    """

    # Ensure schema exists (so running --last-run before watch mode still works)
    tmp = RunHistoryDb(run_history_db_path)
    tmp.close()

    conn = _connect(run_history_db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                r.run_id,
                r.ended_at_unix,
                r.screenshot_path,

                -- base values
                r.hero AS hero_base,
                r.rank AS rank_base,

                -- overrides (nullable)
                o.hero_override,
                o.rank_override,
                o.is_confirmed,
                o.notes,

                -- effective values
                COALESCE(o.hero_override, r.hero) AS hero_effective,
                COALESCE(o.rank_override, r.rank) AS rank_effective
            FROM runs r
            LEFT JOIN run_overrides o ON o.run_id = r.run_id
            ORDER BY r.run_id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_last_run_id(run_history_db_path: str) -> Optional[int]:
    # Ensure schema exists (so running --last-run before watch mode still works)
    tmp = RunHistoryDb(run_history_db_path)
    tmp.close()

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

    # Ensure schema exists (so running --last-run before watch mode still works)
    tmp = RunHistoryDb(run_history_db_path)
    tmp.close()

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

        cur.execute(
                "SELECT hero_override, rank_override, notes, is_confirmed FROM run_overrides WHERE run_id=?",
                (run_id,),
                )
        ov = cur.fetchone()

        hero_eff = (ov["hero_override"] if ov and ov["hero_override"] else run_row["hero"])
        rank_eff = (ov["rank_override"] if ov and ov["rank_override"] is not None else run_row["rank"])
        notes = (ov["notes"] if ov else None)
        is_confirmed = (ov["is_confirmed"] if ov else 0)

        cur.execute(
            """
            SELECT socket_number, template_id_override, size_override, note
            FROM run_item_overrides
            WHERE run_id=?
            """,
            (run_id,),
        )
        ov_items = {int(r["socket_number"]): dict(r) for r in cur.fetchall()}

        # Resolve names from templates DB (if template_id not null)
        resolved_items: List[Dict[str, Any]] = []
        tcur = td.cursor()

        for it in items:
            base_template_id = it["template_id"]
            base_size = it["size"]

            ovi = ov_items.get(int(it["socket_number"]))
            template_eff = base_template_id
            size_eff = base_size
            override_note = None

            if ovi:
                # If the override column exists (even if NULL), apply it.
                # We store "clear override" by deleting the override row via clear_item_override().
                if ovi.get("template_id_override") is not None:
                    template_eff = ovi.get("template_id_override")
                if ovi.get("size_override") is not None:
                    size_eff = ovi.get("size_override")
                override_note = ovi.get("note")

            name: Optional[str] = None
            art_key: Optional[str] = None

            # Resolve using EFFECTIVE template id (not base)
            if template_eff:
                tcur.execute(
                    "SELECT name, art_key FROM templates WHERE template_id = ?",
                    (template_eff,),
                )
                trow = tcur.fetchone()
                if trow:
                    name = trow["name"]
                    art_key = trow["art_key"]

            resolved_items.append(
                {
                    "socket_number": it["socket_number"],
                    "size": size_eff,
                    "template_id": template_eff,
                    "name": name,
                    "art_key": art_key,

                    # debug fields (helpful)
                    "base_template_id": base_template_id,
                    "base_size": base_size,
                    "overridden": bool(ovi),
                    "override_note": override_note,
                }
            )

        return {
            "run_id": run_row["run_id"],
            "ended_at_unix": run_row["ended_at_unix"],
            "screenshot_path": run_row["screenshot_path"],
        
            "hero": run_row["hero"],
            "rank": run_row["rank"],
        
            "hero_effective": hero_eff,
            "rank_effective": rank_eff,
            "notes": notes,
            "is_confirmed": is_confirmed,
        
            "items": resolved_items,
        }

    finally:
        rh.close()
        td.close()

def search_templates(
    templates_db_path: str,
    q: str,
    limit: int = 8,
    size: str = "",
) -> List[Dict[str, Any]]:
    import re
    from difflib import SequenceMatcher

    def norm(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip().lower())

    def score(query: str, name: str) -> float:
        query = norm(query)
        name_n = norm(name)
        if not query or not name_n:
            return 0.0
        if name_n.startswith(query):
            return 3.0
        if query in name_n:
            return 2.0
        return SequenceMatcher(None, query, name_n).ratio()

    q = (q or "").strip()
    if not q:
        return []

    size = (size or "").strip().lower()
    if size not in ("small", "medium", "large"):
        size = ""

    conn = _connect(templates_db_path)
    try:
        cur = conn.cursor()

        qn = norm(q)
        like = f"%{qn}%"

        params = [like]
        size_clause = ""
        if size:
            size_clause = " AND LOWER(size) = ? "
            params.append(size.lower())

        cur.execute(
            f"""
            SELECT template_id, name, size
            FROM templates
            WHERE name IS NOT NULL
              AND LOWER(name) LIKE ?
              {size_clause}
            LIMIT 250
            """,
            tuple(params),
        )
        rows = cur.fetchall()

        # Fallback if LIKE returns nothing
        if not rows:
            params2 = []
            size_clause2 = ""
            if size:
                size_clause2 = " AND LOWER(size) = ? "
                params2.append(size.lower())

            cur.execute(
                f"""
                SELECT template_id, name, size
                FROM templates
                WHERE name IS NOT NULL
                  {size_clause2}
                LIMIT 250
                """,
                tuple(params2),
            )
            rows = cur.fetchall()

        scored = []
        for r in rows:
            name = r["name"] or ""
            s = score(q, name)
            if s > 0:
                scored.append((s, name, r["template_id"], r["size"]))

        scored.sort(key=lambda t: (-t[0], len(t[1]), t[1].lower()))
        top = scored[: int(limit)]

        return [{"template_id": tid, "name": name, "size": sz} for _, name, tid, sz in top]

    finally:
        conn.close()
