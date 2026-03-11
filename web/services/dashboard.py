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
    season_filter: str = "",
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

    season_options = sorted(
        {r.get("season_id") for r in runs if r.get("season_id") is not None},
        reverse=True,
    )
    
    if season_filter == "":
        season_selected = ""
        runs_filtered = runs
    elif season_filter == "__NONE__":
        season_selected = "__NONE__"
        runs_filtered = [r for r in runs if r.get("season_id") is None]
    else:
        try:
            season_value = int(season_filter)
            season_selected = str(season_value)
            runs_filtered = [r for r in runs if r.get("season_id") == season_value]
        except ValueError:
            season_selected = ""
            runs_filtered = runs
    
    
    hero_counts: dict[str, int] = {}
    for r in runs_filtered:
        hero = (r.get("hero_effective") or "(unknown)").strip() or "(unknown)"
        hero_counts[hero] = hero_counts.get(hero, 0) + 1

    hero_colors = hero_colors_map()
    hero_pie = [
        {"hero": h, "count": c, "color": hero_colors.get(h)}
        for h, c in sorted(hero_counts.items(), key=lambda kv: (-kv[1], kv[0].lower()))
    ]

    def recent_result_token(r: dict) -> dict:
        wins = r.get("wins")
        won = r.get("won")
        run_id = r.get("run_id")
    
        if wins is not None:
            try:
                w = int(wins)
                if w >= 10:
                    return {"ch": "W", "cls": "r-w", "run_id": run_id}
                if w >= 7:
                    return {"ch": str(w), "cls": "r-hi", "run_id": run_id}
                if w >= 4:
                    return {"ch": str(w), "cls": "r-mid", "run_id": run_id}
                return {"ch": str(w), "cls": "r-low", "run_id": run_id}
            except (TypeError, ValueError):
                pass
    
        if won in (True, 1, "1"):
            return {"ch": "W", "cls": "r-w", "run_id": run_id}
        if won in (False, 0, "0"):
            return {"ch": "L", "cls": "r-low", "run_id": run_id}
    
        return {"ch": "?", "cls": "r-u", "run_id": run_id}

    def outcome(r: dict) -> str:
        wins = r.get("wins")
        won = r.get("won")

        if wins is not None:
            try:
                return "W" if int(wins) >= 10 else "L"
            except (TypeError, ValueError):
                pass

        if won in (True, 1, "1"):
            return "W"
        if won in (False, 0, "0"):
            return "L"

        return "?"

    last10 = runs_filtered[:10]
    last10_list = [recent_result_token(r) for r in last10]
    last10_str = "".join(x["ch"] for x in last10_list)

    cur_type: str | None = None
    cur_len = 0
    for r in runs_filtered:
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
    for r in runs_filtered:
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

    hero_stats_sql = """
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
          {season_where}
        )
        GROUP BY hero
    """
    
    hero_stats_params: tuple[Any, ...] = ()
    
    if season_selected == "__NONE__":
        season_where = "AND r.season_id IS NULL"
    elif season_selected != "":
        season_where = "AND r.season_id = ?"
        hero_stats_params = (int(season_selected),)
    else:
        season_where = ""
    
    cur.execute(hero_stats_sql.format(season_where=season_where), hero_stats_params)
    rows = [dict(r) for r in cur.fetchall()]

    if season_selected == "__NONE__":
        cur.execute(
            """
            SELECT COUNT(*) AS n
            FROM runs r
            LEFT JOIN run_overrides o ON o.run_id = r.run_id
            LEFT JOIN run_metrics  m ON m.run_id = r.run_id
            WHERE COALESCE(o.is_confirmed, 0) = 1
              AND COALESCE(m.won, 0) = 1
              AND COALESCE(m.prestige, 0) >= 20
              AND r.season_id IS NULL
            """
        )
    elif season_selected != "":
        cur.execute(
            """
            SELECT COUNT(*) AS n
            FROM runs r
            LEFT JOIN run_overrides o ON o.run_id = r.run_id
            LEFT JOIN run_metrics  m ON m.run_id = r.run_id
            WHERE COALESCE(o.is_confirmed, 0) = 1
              AND COALESCE(m.won, 0) = 1
              AND COALESCE(m.prestige, 0) >= 20
              AND r.season_id = ?
            """,
            (int(season_selected),),
        )
    else:
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
    perfect_runs = int(row["n"]) if row else 0

    # --- season summary ---
    verified_runs = [r for r in runs_filtered if r.get("is_confirmed")]

    wins_vals = [int(r["wins"]) for r in verified_runs if r.get("wins") is not None]
    avg_wins = (sum(wins_vals) / len(wins_vals)) if wins_vals else 0.0

    season_wins = sum(1 for r in verified_runs if outcome(r) == "W")
    season_losses = sum(1 for r in verified_runs if outcome(r) == "L")
    season_unknown = sum(1 for r in verified_runs if outcome(r) == "?")
    season_verified_count = len(verified_runs)
    season_run_count = len(runs_filtered)
    season_winrate = (season_wins * 100 / season_verified_count) if season_verified_count else 0.0

    season_best_win = 0
    w_run = 0
    for r in verified_runs:
        ch = outcome(r)
        if ch == "W":
            w_run += 1
            season_best_win = max(season_best_win, w_run)
        elif ch in ("L", "?"):
            w_run = 0

    season_label = (
            "All"
            if season_selected == ""
            else ("No season" if season_selected == "__NONE__" else f"Season {season_selected}")
            )

    season_summary = {
            "label": season_label,
            "runs": season_run_count,
            "wins": season_wins,
            "losses": season_losses,
            "winrate": season_winrate,
            "avg_wins": avg_wins,
            "perfect_runs": perfect_runs,
            "best_win_streak": season_best_win,
            }

    rank_series_data = rank_series(cur)
    
    if season_selected == "__NONE__":
        run_ids_with_no_season = {
            r["run_id"] for r in runs_filtered if r.get("season_id") is None
        }
        rank_series_data = [
            x for x in rank_series_data
            if x["run_id"] in run_ids_with_no_season
        ]
    elif season_selected != "":
        run_ids_for_season = {
            r["run_id"] for r in runs_filtered
        }
        rank_series_data = [
            x for x in rank_series_data
            if x["run_id"] in run_ids_for_season
        ]
    
    rank_series_data = rank_series_data[-150:]
    
    current_rank = rank_series_data[-1]["rank"] if rank_series_data else None

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
        stats_by_hero[hero]
        for hero in hero_list
        if hero in stats_by_hero and int(stats_by_hero[hero].get("runs", 0)) > 0
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
    
        "achievements_unlocked_count": ach_unlocked,
        "achievements_total": ach_total,
    
        "current_rank": current_rank,
    
        "season_options": season_options,
        "season_selected": season_selected,
        "season_summary": season_summary,
    
        "perfect_runs": perfect_runs,
        "rank_series": rank_series_data,
        "perfect_runs_hero": perfect_runs_hero,
    }
