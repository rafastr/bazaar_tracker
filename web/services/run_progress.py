from __future__ import annotations

import json
import sqlite3
from typing import Any


def _parse_origin_set(heroes_json: str) -> set[str]:
    s = (heroes_json or "").strip()
    if not s:
        return set()
    try:
        data = json.loads(s)
    except Exception:
        return set()

    # Support a few shapes safely:
    # - ["Vanessa","Dooley"]
    # - {"heroes":["Vanessa","Dooley"]}
    # - "Vanessa"
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
        if name:
            out.add(name)
    return out


def get_run_item_progress_table(
    templates_db_path: str,
    run_history_db_path: str,
    run_id: int,
    *,
    hconn: sqlite3.Connection | None = None,
    tconn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    """
    Returns:
      {
        "hero_eff": str,
        "won": bool,
        "confirmed": bool,
        "rows": [
          {
            "template_id": str,
            "name": str,
            "size": str,
            "won_this": bool,
            "won_other": bool,
            "new_won_this": bool,
            "new_won_other": bool,
          }, ...
        ]
      }
    """

    # --- load run hero/flags ---
    h_owns = hconn is None
    if hconn is None:
        hconn = sqlite3.connect(run_history_db_path)
        hconn.row_factory = sqlite3.Row

    try:
        cur = hconn.cursor()
        cur.execute(
            """
            SELECT
              r.hero AS hero_base,
              o.hero_override,
              COALESCE(o.is_confirmed, 0) AS is_confirmed,
              COALESCE(m.won, 0) AS won
            FROM runs r
            LEFT JOIN run_overrides o ON o.run_id = r.run_id
            LEFT JOIN run_metrics  m ON m.run_id = r.run_id
            WHERE r.run_id=?
            """,
            (int(run_id),),
        )
        rr = cur.fetchone()
        if not rr:
            return {"hero_eff": "(unknown)", "won": False, "confirmed": False, "rows": []}

        hero_eff = (rr["hero_override"] or rr["hero_base"] or "(unknown)").strip() or "(unknown)"
        confirmed = int(rr["is_confirmed"] or 0) == 1
        won = int(rr["won"] or 0) == 1

        # --- effective items for this run (tid + size) ---
        cur.execute(
            "SELECT socket_number, template_id, size FROM run_items WHERE run_id=?",
            (int(run_id),),
        )
        base = {int(r["socket_number"]): (r["template_id"] or "").strip() for r in cur.fetchall()}

        cur.execute(
            "SELECT socket_number, template_id_override FROM run_item_overrides WHERE run_id=?",
            (int(run_id),),
        )
        ov = {int(r["socket_number"]): r["template_id_override"] for r in cur.fetchall()}

        tids: list[str] = []
        for sock, tid in base.items():
            if sock in ov and ov[sock] is not None:
                tid_eff = (ov[sock] or "").strip()
            else:
                tid_eff = tid
            if tid_eff:
                tids.append(tid_eff)

        # de-dup for checklist display
        tids = sorted(set(tids))
        if not tids:
            return {"hero_eff": hero_eff, "won": won, "confirmed": confirmed, "rows": []}

        # --- load template names + origins for just these tids ---
        t_owns = tconn is None
        if tconn is None:
            tconn = sqlite3.connect(templates_db_path)
            tconn.row_factory = sqlite3.Row

        try:
            tcur = tconn.cursor()
            qmarks = ",".join("?" for _ in tids)

            tcur.execute(
                f"""
                SELECT template_id, name, heroes_json, size
                FROM templates
                WHERE template_id IN ({qmarks})
                  AND COALESCE(ignored, 0) = 0
                """,
                tuple(tids),
            )
            trows = {r["template_id"]: dict(r) for r in tcur.fetchall()}
        finally:
            if t_owns:
                tconn.close()

        origin_by_tid: dict[str, set[str]] = {}
        is_common_tid: dict[str, bool] = {}
        for tid, tr in trows.items():
            origins = _parse_origin_set(tr.get("heroes_json") or "")
            origin_by_tid[tid] = origins
            is_common_tid[tid] = any(h.lower() == "common" for h in origins) or (not origins)

        # --- compute which items were "first unlocked" by this run ---
        # We compare current progress rows vs progress excluding this run.
        # This is done via item_firsts table if you have it, otherwise use item_hero_wins logic.
        # Your current app.py implementation already had a stable logic here; keep it consistent.

        # Current: item_hero_wins is rebuildable. We'll treat "first unlock" as:
        # - new_won_this: there was no win_count>0 for this template_id before this run across any hero
        # - new_won_other: there was no cross-hero win_count>0 before this run (rule applied)
        #
        # To avoid re-implementing complex "before/after" here, we use item_firsts if present.
        # If item_firsts doesn't exist, we fall back to conservative false flags.
        rows_out: list[dict[str, Any]] = []


        # quick schema check
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='item_firsts'")
        has_item_firsts = cur.fetchone() is not None
        
        firsts_by_tid: dict[str, dict[str, Any]] = {}
        if has_item_firsts:
            cur.execute(
                """
                SELECT template_id, first_win_run_id, first_cross_win_run_id
                FROM item_firsts
                WHERE template_id IN (%s)
                """ % (",".join("?" for _ in tids)),
                tuple(tids),
            )
            for r in cur.fetchall():
                firsts_by_tid[r["template_id"]] = dict(r)


        # imported checklist completion acts like pre-existing progress baseline
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='imported_item_completion'")
        has_imported_completion = cur.fetchone() is not None

        imported_by_tid: dict[str, dict[str, bool]] = {}
        if has_imported_completion:
            cur.execute(
                """
                SELECT template_id, win_this, win_other, ten_wins
                FROM imported_item_completion
                WHERE template_id IN (%s)
                """ % (",".join("?" for _ in tids)),
                tuple(tids),
            )
            for r in cur.fetchall():
                tid = (r["template_id"] or "").strip()
                if not tid:
                    continue
                imported_by_tid[tid] = {
                    "win_this": bool(r["win_this"]),
                    "win_other": bool(r["win_other"]),
                    "ten_wins": bool(r["ten_wins"]),
                }

        # existing progress (after rebuild) from item_hero_wins
        # won_this: any hero has win_count>0 for this tid
        cur.execute(
            """
            SELECT template_id, SUM(win_count) AS total
            FROM item_hero_wins
            WHERE template_id IN (%s)
            GROUP BY template_id
            """ % (",".join("?" for _ in tids)),
            tuple(tids),
        )
        won_any = {r["template_id"]: int(r["total"] or 0) > 0 for r in cur.fetchall()}

        # won_other: any hero has win_count>0 where hero != origin (or common rule)
        # We approximate using stored hero wins vs origins.
        cur.execute(
            """
            SELECT template_id, hero, win_count
            FROM item_hero_wins
            WHERE template_id IN (%s) AND win_count > 0
            """ % (",".join("?" for _ in tids)),
            tuple(tids),
        )
        won_other_map: dict[str, bool] = {tid: False for tid in tids}
        for r in cur.fetchall():
            tid = r["template_id"]
            hero = (r["hero"] or "").strip()
            if not tid:
                continue
            if is_common_tid.get(tid, False):
                won_other_map[tid] = True
                continue
            origins = origin_by_tid.get(tid, set())
            if hero and origins and (hero not in origins):
                won_other_map[tid] = True

        for tid in tids:
            tr = trows.get(tid)
            if not tr:
                continue

            name = tr.get("name") or tid
            size = tr.get("size") or ""

            imported = imported_by_tid.get(tid, {})

            real_won_this = bool(won_any.get(tid, False))
            real_won_other = bool(won_other_map.get(tid, False))

            won_this = real_won_this or bool(imported.get("win_this"))
            won_other = real_won_other or bool(imported.get("win_other"))

            fi = firsts_by_tid.get(tid, {})

            imported_win_this = bool(imported.get("win_this"))
            imported_win_other = bool(imported.get("win_other"))

            new_won_this = (
                bool(fi.get("first_win_run_id") == int(run_id)) if fi else False
            ) and not imported_win_this

            new_won_other = (
                bool(fi.get("first_cross_win_run_id") == int(run_id)) if fi else False
            ) and not imported_win_other

            rows_out.append(
                {
                    "template_id": tid,
                    "name": name,
                    "size": size,
                    "won_this": won_this,
                    "won_other": won_other,
                    "new_won_this": new_won_this,
                    "new_won_other": new_won_other,
                }
            )

        # show only “new” rows if the run is confirmed + won (matches your UI rule)
        if confirmed and won:
            rows_out = [r for r in rows_out if r["new_won_this"] or r["new_won_other"]]
        else:
            rows_out = []

        return {"hero_eff": hero_eff, "won": won, "confirmed": confirmed, "rows": rows_out}
    finally:
        if h_owns:
            hconn.close()
