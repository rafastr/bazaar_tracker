from __future__ import annotations

import json
import sqlite3
from typing import Any

from core.board_layout import visible_board_items


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

        # --- effective visible items for this run (tid + size) ---
        cur.execute(
            "SELECT socket_number, template_id, size FROM run_items WHERE run_id=?",
            (int(run_id),),
        )
        base = {
            int(r["socket_number"]): {
                "template_id": (r["template_id"] or "").strip(),
                "size": (r["size"] or "").strip().lower() or "small",
            }
            for r in cur.fetchall()
        }

        cur.execute(
            """
            SELECT socket_number, template_id_override, size_override
            FROM run_item_overrides
            WHERE run_id=?
            """,
            (int(run_id),),
        )
        ov = {
            int(r["socket_number"]): {
                "template_id": r["template_id_override"],
                "size": r["size_override"],
            }
            for r in cur.fetchall()
        }

        effective_items: list[dict[str, Any]] = []
        all_sockets = sorted(set(base.keys()) | set(ov.keys()))

        for sock in all_sockets:
            b = base.get(sock, {"template_id": "", "size": "small"})
            tid = b["template_id"]
            size = b["size"]

            if sock in ov:
                if ov[sock]["template_id"] is not None:
                    tid = (ov[sock]["template_id"] or "").strip()
                if ov[sock]["size"] is not None:
                    size = (ov[sock]["size"] or "").strip().lower() or "small"

            if tid:
                effective_items.append(
                    {
                        "socket_number": sock,
                        "template_id": tid,
                        "size": size,
                    }
                )

        effective_items = visible_board_items(effective_items)

        # de-dup for checklist display, but preserve size from visible item
        by_tid: dict[str, dict[str, Any]] = {}
        for it in effective_items:
            tid = (it.get("template_id") or "").strip()
            if not tid or tid in by_tid:
                continue
            by_tid[tid] = it

        tids = sorted(by_tid.keys())
        if not tids:
            return {"hero_eff": hero_eff, "won": won, "confirmed": confirmed, "rows": []}

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

        rows_out: list[dict[str, Any]] = []

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
            size = by_tid.get(tid, {}).get("size") or tr.get("size") or ""

            imported = imported_by_tid.get(tid, {})

            real_won_this = bool(won_any.get(tid, False))
            real_won_other = bool(won_other_map.get(tid, False))

            fi = firsts_by_tid.get(tid, {})

            imported_win_this = bool(imported.get("win_this"))
            imported_win_other = bool(imported.get("win_other"))

            new_won_this = (
                bool(fi.get("first_win_run_id") == int(run_id)) if fi else False
            ) and not imported_win_this

            new_won_other = (
                bool(fi.get("first_cross_win_run_id") == int(run_id)) if fi else False
            ) and not imported_win_other

            # Treat “new unlock on this run” as checked too.
            won_this = real_won_this or bool(imported.get("win_this")) or new_won_this
            won_other = real_won_other or bool(imported.get("win_other")) or new_won_other

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

        if confirmed and won:
            rows_out = [r for r in rows_out if r["new_won_this"] or r["new_won_other"]]
        else:
            rows_out = []

        return {"hero_eff": hero_eff, "won": won, "confirmed": confirmed, "rows": rows_out}
    finally:
        if h_owns:
            hconn.close()
