from __future__ import annotations

from typing import Any, Callable
import sqlite3

from web.services.stats import perfect_runs_count, perfect_runs_by_hero, rank_series
from core.config import Settings
from core.run_viewer import list_runs


def build_index_context(
    *,
    settings: Settings,
    get_db: Callable[[], Any],  # RunHistoryDb
    get_templates_conn: Callable[[], sqlite3.Connection],
    hero_colors_map: Callable[[], dict[str, str]],
    get_item_checklist: Callable[..., list[dict[str, Any]]],
    get_hero_list: Callable[..., list[str]],
) -> dict[str, Any]:
    """Build all data needed by the dashboard (index) template."""

    # --- item checklist stats ---
    items = get_item_checklist(
        settings.templates_db_path,
        settings.run_history_db_path,
        tconn=get_templates_conn(),
        hconn=get_db().conn,
    )

    total = len(items)
    win_done = sum(1 for x in items if x.get("won_this"))
    other_done = sum(1 for x in items if x.get("won_other"))

    win_pct = (win_done * 100 / total) if total else 0.0
    other_pct = (other_done * 100 / total) if total else 0.0

    by_group: dict[str, dict[str, Any]] = {}
    for x in items:
        g = x.get("group") or "Common"
        s = by_group.setdefault(g, {"group": g, "total": 0, "win": 0, "other": 0})
        s["total"] += 1
        if x.get("won_this"):
            s["win"] += 1
        if x.get("won_other"):
            s["other"] += 1

    def gkey(name: str) -> tuple[int, str]:
        if name.strip().lower() == "common":
            return (1, "common")
        return (0, name.strip().lower())

    group_stats: list[dict[str, Any]] = []
    for g, s in sorted(by_group.items(), key=lambda kv: gkey(kv[0])):
        total_g = int(s.get("total") or 0)
        win_g = int(s.get("win") or 0)
        other_g = int(s.get("other") or 0)
        group_stats.append(
            {
                "group": g,
                "total": total_g,
                "win": win_g,
                "other": other_g,
                "win_pct": (win_g * 100 / total_g) if total_g else 0.0,
                "other_pct": (other_g * 100 / total_g) if total_g else 0.0,
            }
        )

    overall = {
        "total": total,
        "win": win_done,
        "other": other_done,
        "win_pct": win_pct,
        "other_pct": other_pct,
    }

    # --- hero pie chart + last-10 W/L + streaks ---
    runs = list_runs(settings.run_history_db_path, limit=200)

    hero_counts: dict[str, int] = {}
    for r in runs:
        hero = (r.get("hero_effective") or "(unknown)").strip() or "(unknown)"
        hero_counts[hero] = hero_counts.get(hero, 0) + 1

    hero_colors = hero_colors_map()
    hero_pie = [
        {"hero": h, "count": c, "color": hero_colors.get(h)}
        for h, c in sorted(hero_counts.items(), key=lambda kv: (-kv[1], kv[0].lower()))
    ]

    def outcome(r: dict) -> str:
        if r.get("won") is True:
            return "W"
        if r.get("wins") is not None:
            return "L"
        return "?"

    last10 = runs[:10]
    last10_list = [{"ch": outcome(r)} for r in last10]
    last10_str = "".join(x["ch"] for x in last10_list)

    cur_type: str | None = None
    cur_len = 0
    for r in runs:
        ch = outcome(r)
        if ch == "?":
            break
        if cur_type is None:
            cur_type = ch
            cur_len = 1
        elif ch == cur_type:
            cur_len += 1
        else:
            break

    best_win = 0
    w_run = 0
    for r in runs:
        ch = outcome(r)
        if ch == "W":
            w_run += 1
            best_win = max(best_win, w_run)
        elif ch == "L" or ch == "?":
            w_run = 0

    streaks = {"current_type": cur_type, "current_len": cur_len, "best_win": best_win}

    # --- hero stats table (FULL hero list, even with 0 runs) ---
    heroes = set(get_hero_list(settings.templates_db_path, conn=get_templates_conn()))
    heroes.update(hero_colors_map().keys())
    heroes.add("(unknown)")
    hero_list = sorted(heroes, key=lambda x: (x == "(unknown)", x.lower()))

    db = get_db()
    cur = db.conn.cursor()
    cur.execute(
        """
        SELECT
          hero,
          SUM(cnt) AS runs,
          SUM(wins) AS wins,
          SUM(losses) AS losses,
          SUM(unknowns) AS unknowns,
          AVG(avg_wins) AS avg_wins
        FROM (
          SELECT
            COALESCE(o.hero_override, r.hero, '(unknown)') AS hero,
            1 AS cnt,
            CASE WHEN COALESCE(m.won, 0) = 1 THEN 1 ELSE 0 END AS wins,
            CASE WHEN COALESCE(m.won, 0) = 0 AND m.wins IS NOT NULL THEN 1 ELSE 0 END AS losses,
            CASE WHEN m.wins IS NULL THEN 1 ELSE 0 END AS unknowns,
            CASE WHEN m.won = 1 AND m.wins IS NOT NULL THEN m.wins ELSE NULL END AS avg_wins
          FROM runs r
          LEFT JOIN run_overrides o ON o.run_id = r.run_id
          LEFT JOIN run_metrics  m ON m.run_id = r.run_id
          WHERE COALESCE(o.is_confirmed, 0) = 1
        )
        GROUP BY hero
        """
    )
    rows = [dict(r) for r in cur.fetchall()]

    perfect_runs = perfect_runs_count(cur)
    rank_series_data = rank_series(cur)
    perfect_runs_hero = perfect_runs_by_hero(cur)  # optional to display later

    stats_by_hero: dict[str, dict[str, Any]] = {}
    for r in rows:
        hero = (r.get("hero") or "(unknown)").strip() or "(unknown)"
        w = int(r.get("wins") or 0)
        l = int(r.get("losses") or 0)
        u = int(r.get("unknowns") or 0)
        runs_n = int(r.get("runs") or 0)
        denom = w + l

        stats_by_hero[hero] = {
            "hero": hero,
            "runs": runs_n,
            "wins": w,
            "losses": l,
            "unknowns": u,
            "winrate": (w * 100 / denom) if denom else 0.0,
            "avg_wins": float(r.get("avg_wins") or 0.0),
        }

    hero_stats = [
        stats_by_hero.get(
            hero,
            {"hero": hero, "runs": 0, "wins": 0, "losses": 0, "unknowns": 0, "winrate": 0.0, "avg_wins": 0.0},
        )
        for hero in hero_list
    ]

    # --- achievements list for dashboard ---
    cur.execute(
        """
        SELECT
          a.key,
          a.title,
          a.description,
          u.unlocked_at_unix,
          u.run_id
        FROM achievements a
        LEFT JOIN achievement_unlocks u ON u.key = a.key
        ORDER BY
          CASE WHEN u.unlocked_at_unix IS NOT NULL THEN 0 ELSE 1 END,
          u.unlocked_at_unix DESC,
          a.title COLLATE NOCASE ASC
        """
    )
    ach_rows = [dict(r) for r in cur.fetchall()]
    ach_unlocked = sum(1 for r in ach_rows if r.get("unlocked_at_unix"))
    ach_total = len(ach_rows)


    return {
        "overall": overall,
        "group_stats": group_stats,
        "hero_pie": hero_pie,
        "last10_list": last10_list,
        "last10_str": last10_str,
        "streaks": streaks,
        "hero_stats": hero_stats,
        "achievements": ach_rows,
        "ach_unlocked": ach_unlocked,
        "ach_total": ach_total,
    
        # from stats.py
        "perfect_runs": perfect_runs,
        "rank_series": rank_series_data,
        "perfect_runs_hero": perfect_runs_hero,
    }
