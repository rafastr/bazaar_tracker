from __future__ import annotations

import json
import sqlite3
from typing import Any


def get_hero_list(templates_db_path: str, conn: sqlite3.Connection | None = None) -> list[str]:
    import json

    owns = conn is None
    if conn is None:
        conn = sqlite3.connect(templates_db_path)
        conn.row_factory = sqlite3.Row

    heroes: set[str] = set()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT heroes_json FROM templates WHERE heroes_json IS NOT NULL AND TRIM(heroes_json) <> ''"
        )
        for row in cur.fetchall():
            s = row["heroes_json"]
            if not s:
                continue
            try:
                data = json.loads(s)
            except Exception:
                continue

            # Support a few shapes safely:
            # - ["Vanessa","Dooley"]
            # - {"heroes":["Vanessa","Dooley"]}
            # - "Vanessa"  (unlikely, but harmless)
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

        out = sorted(heroes, key=lambda x: x.lower())
        return out
    finally:
        if owns:
            conn.close()


def get_item_checklist(templates_db_path: str, run_history_db_path: str, *, tconn: sqlite3.Connection | None = None, hconn: sqlite3.Connection | None = None) -> list[dict]:
    import sqlite3, json

    def parse_origin_heroes(heroes_json: str) -> set[str]:
        """
        Returns a set of origin heroes from heroes_json.
        Keeps "common" if present (special meaning).
        """
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
        tcur.execute("SELECT template_id, name, heroes_json, size FROM templates WHERE template_id IS NOT NULL")
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

        # ✅ win = used in any won+verified run (any hero)
        won_any = bool(winners)

        # display origin (exclude Common from display)
        origin_no_common = sorted(
            [h for h in origin_heroes if h.strip().lower() != "common"],
            key=str.lower,
        )
        origin_display = ", ".join(origin_no_common)

        # group (for sections)
        is_common = any(h.lower() == "common" for h in origin_heroes) or (not origin_heroes)
        if is_common or not origin_no_common:
            group = "Common"
        elif len(origin_no_common) == 1:
            group = origin_no_common[0]
        else:
            # future-proof: multi-hero items
            group = " / ".join(origin_no_common)

        # ✅ win_other:
        # - if item is Common => any win counts as "win with another hero"
        # - else => true if there exists a winner hero NOT in origin heroes
        if not won_any:
            won_other = False
        elif is_common:
            won_other = True
        else:
            won_other = any(h not in origin_heroes for h in winners)

        out.append(
            {
                "template_id": tid,
                "name": name,
                "size": (t.get("size") or "").strip().lower(),
                "origin": origin_display,
                "group": group,
                "won_this": won_any,
                "won_other": won_other,
            }
        )

    def group_key(g: str) -> tuple[int, str]:
        # Put Common at the end
        if (g or "").strip().lower() == "common":
            return (1, "common")
        return (0, (g or "").strip().lower())

    # sort: group, then name
    out.sort(key=lambda r: (group_key(r["group"]), r["name"].lower()))
    return out
