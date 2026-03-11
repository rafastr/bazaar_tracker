from __future__ import annotations

import json
import sqlite3
from typing import Any


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == column for r in cur.fetchall())


def get_hero_list(templates_db_path: str, conn: sqlite3.Connection | None = None) -> list[str]:
    owns = conn is None
    if conn is None:
        conn = sqlite3.connect(templates_db_path)
        conn.row_factory = sqlite3.Row

    heroes: set[str] = set()
    try:
        cur = conn.cursor()
        has_ignored = _has_column(conn, "templates", "ignored")

        if has_ignored:
            cur.execute(
                """
                SELECT heroes_json
                FROM templates
                WHERE COALESCE(ignored, 0) = 0
                  AND heroes_json IS NOT NULL
                  AND TRIM(heroes_json) <> ''
                """
            )
        else:
            cur.execute(
                """
                SELECT heroes_json
                FROM templates
                WHERE heroes_json IS NOT NULL
                  AND TRIM(heroes_json) <> ''
                """
            )

        for row in cur.fetchall():
            s = row["heroes_json"]
            if not s:
                continue
            try:
                data = json.loads(s)
            except Exception:
                continue

            if isinstance(data, list):
                values = data
            elif isinstance(data, dict):
                values = data.get("heroes", [])
            else:
                values = [data]

            for v in values:
                if isinstance(v, str):
                    name = v.strip()
                    if name:
                        heroes.add(name)

        return sorted(heroes, key=lambda x: x.lower())
    finally:
        if owns:
            conn.close()


def get_item_checklist(
    templates_db_path: str,
    run_history_db_path: str,
    *,
    tconn: sqlite3.Connection | None = None,
    hconn: sqlite3.Connection | None = None,
) -> list[dict]:
    def parse_origin_heroes(heroes_json: str) -> set[str]:
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
            if not isinstance(v, str):
                continue
            name = v.strip()
            if not name:
                continue
            out.add(name)
        return out

    # 1) all templates
    t_owns = tconn is None
    if tconn is None:
        tconn = sqlite3.connect(templates_db_path)
        tconn.row_factory = sqlite3.Row

    try:
        tcur = tconn.cursor()
        has_ignored = _has_column(tconn, "templates", "ignored")

        if has_ignored:
            tcur.execute(
                """
                SELECT template_id, name, heroes_json, size
                FROM templates
                WHERE template_id IS NOT NULL
                  AND COALESCE(ignored, 0) = 0
                """
            )
        else:
            tcur.execute(
                """
                SELECT template_id, name, heroes_json, size
                FROM templates
                WHERE template_id IS NOT NULL
                """
            )

        templates = [dict(r) for r in tcur.fetchall()]
    finally:
        if t_owns:
            tconn.close()

    # 2) all wins from run history (per template_id, hero)
    h_owns = hconn is None
    if hconn is None:
        hconn = sqlite3.connect(run_history_db_path)
        hconn.row_factory = sqlite3.Row

    try:
        hcur = hconn.cursor()
        hcur.execute(
            """
            SELECT template_id, hero
            FROM item_hero_wins
            WHERE win_count > 0
            """
        )
        rows = hcur.fetchall()

        hcur.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='imported_item_completion'
            """
        )
        has_imported_completion = hcur.fetchone() is not None

        imported_by_item: dict[str, dict[str, bool]] = {}
        if has_imported_completion:
            hcur.execute(
                """
                SELECT template_id, win_this, win_other, ten_wins
                FROM imported_item_completion
                """
            )
            for r in hcur.fetchall():
                tid = (r["template_id"] or "").strip()
                if not tid:
                    continue
                imported_by_item[tid] = {
                    "win_this": bool(r["win_this"]),
                    "win_other": bool(r["win_other"]),
                    "ten_wins": bool(r["ten_wins"]),
                }

        hcur.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='item_firsts'
            """
        )
        has_item_firsts = hcur.fetchone() is not None

        firsts_by_item: dict[str, dict[str, bool]] = {}
        if has_item_firsts:
            hcur.execute(
                """
                SELECT template_id, first_win_run_id, first_cross_win_run_id
                FROM item_firsts
                """
            )
            for r in hcur.fetchall():
                tid = (r["template_id"] or "").strip()
                if not tid:
                    continue
                firsts_by_item[tid] = {
                    "won_this": r["first_win_run_id"] is not None,
                    "won_other": r["first_cross_win_run_id"] is not None,
                }

    finally:
        if h_owns:
            hconn.close()

    # Build: template_id -> set(heroes_that_won_with_it)
    winners_by_item: dict[str, set[str]] = {}
    for r in rows:
        tid = (r["template_id"] or "").strip()
        hero = (r["hero"] or "").strip()
        if not tid or not hero:
            continue
        winners_by_item.setdefault(tid, set()).add(hero)

    # 3) merge
    out: list[dict] = []
    for t in templates:
        tid = (t.get("template_id") or "").strip()
        if not tid:
            continue

        name = t.get("name") or ""
        origin_heroes = parse_origin_heroes(t.get("heroes_json") or "")

        winners = winners_by_item.get(tid, set())

        imported = imported_by_item.get(tid, {})
        firsts = firsts_by_item.get(tid, {})

        real_won_any = bool(winners) or bool(firsts.get("won_this"))

        origin_no_common = sorted(
            [h for h in origin_heroes if h.strip().lower() != "common"],
            key=str.lower,
        )
        origin_display = ", ".join(origin_no_common)

        is_common = any(h.lower() == "common" for h in origin_heroes) or (not origin_heroes)
        if is_common or not origin_no_common:
            group = "Common"
        elif len(origin_no_common) == 1:
            group = origin_no_common[0]
        else:
            group = " / ".join(origin_no_common)

        if bool(firsts.get("won_other")):
            real_won_other = True
        elif not real_won_any:
            real_won_other = False
        elif is_common:
            real_won_other = True
        else:
            real_won_other = any(h not in origin_heroes for h in winners)

        won_any = real_won_any or bool(imported.get("win_this"))
        won_other = real_won_other or bool(imported.get("win_other"))

        ten_wins = bool(imported.get("ten_wins"))

        out.append(
            {
                "template_id": tid,
                "name": name,
                "size": (t.get("size") or "").strip().lower(),
                "origin": origin_display,
                "group": group,
                "won_this": won_any,
                "won_other": won_other,
                "ten_wins": ten_wins,
            }
        )

    def group_key(g: str) -> tuple[int, str]:
        if (g or "").strip().lower() == "common":
            return (1, "common")
        return (0, (g or "").strip().lower())

    out.sort(key=lambda r: (group_key(r["group"]), r["name"].lower()))
    return out
