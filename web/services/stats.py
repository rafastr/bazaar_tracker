from __future__ import annotations

import sqlite3
from typing import Any


def perfect_runs_count(cur: sqlite3.Cursor) -> int:
    cur.execute(
        """
        SELECT COUNT(*) AS n
        FROM runs r
        LEFT JOIN run_overrides o ON o.run_id = r.run_id
        LEFT JOIN run_metrics  m ON m.run_id = r.run_id
        WHERE COALESCE(o.is_confirmed, 0) = 1
          AND COALESCE(m.won, 0) = 1
          AND COALESCE(m.prestige, 0) >= 20
        """
    )
    row = cur.fetchone()
    return int(row["n"]) if row else 0


def perfect_runs_by_hero(cur: sqlite3.Cursor) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
          COALESCE(o.hero_override, r.hero, '(unknown)') AS hero,
          COUNT(*) AS perfect_runs
        FROM runs r
        LEFT JOIN run_overrides o ON o.run_id = r.run_id
        LEFT JOIN run_metrics  m ON m.run_id = r.run_id
        WHERE COALESCE(o.is_confirmed, 0) = 1
          AND COALESCE(m.won, 0) = 1
          AND COALESCE(m.prestige, 0) >= 20
        GROUP BY hero
        ORDER BY perfect_runs DESC, hero COLLATE NOCASE ASC
        """
    )
    return [{"hero": r["hero"], "perfect_runs": int(r["perfect_runs"])} for r in cur.fetchall()]


def rank_series(cur: sqlite3.Cursor) -> list[dict[str, int]]:
    cur.execute(
        """
        SELECT
          r.run_id,
          COALESCE(o.rank_override, r.rank) AS rank_eff
        FROM runs r
        LEFT JOIN run_overrides o ON o.run_id = r.run_id
        WHERE COALESCE(o.is_confirmed, 0) = 1
          AND COALESCE(o.rank_override, r.rank) IS NOT NULL
        ORDER BY r.run_id ASC
        """
    )
    return [{"run_id": int(r["run_id"]), "rank": int(r["rank_eff"])} for r in cur.fetchall()]
