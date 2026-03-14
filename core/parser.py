from __future__ import annotations

import re
from typing import List, Optional, Dict, Any

from .events import Event


class LogParser:
    RUN_START_MARKER = "[StartRunAppState] Run initialization finalized."
    RUN_END_MARKER = "Starting card reveal sequence"

    # GUID matcher (canonical 8-4-4-4-12)
    _GUID = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"

    HERO_LINE_RE = re.compile(
        r"\[SocketBehavior\] Initializing Socket Connection:.*\| Hero: \[(?P<hero>[^\]]+)\]"
    )
    HERO_CHANGE_RE = re.compile(
        r"Changing EHero to (?P<hero>[A-Za-z][A-Za-z0-9 _'-]*)"
    )

    HERO_VO_RE = re.compile(
        r"LoadedBank result for VO_(?P<hero>[A-Za-z][A-Za-z0-9_]*)"
    )

    RANK_LINE_RE = re.compile(r"Changing leaderboard position from \d+ to (?P<rank>\d+)")

    SEASON_ID_QS_RE = re.compile(r"[?&]seasonId=(?P<season>\d+)")

    # Save instance it of items on purchase
    _re_item_purchase = re.compile(
        rf"Card Purchased:\s*InstanceId:\s*(?P<iid>itm_[A-Za-z0-9_]+)\s*-\s*TemplateId(?P<tid>{_GUID})\b",
        re.IGNORECASE,
    )

    # Game battle snapshot line marker
    _re_gamesim_cards_spawned = re.compile(
        r"\[GameSimHandler\].*Cards Spawned:",
        re.IGNORECASE,
    )

    # Find each item entry inside the battle snapshot
    _re_snapshot_item = re.compile(
        r"\[(?P<iid>itm_[A-Za-z0-9_]+)\s*"
        r"\[(?P<owner>Player|Opponent)\]\s*"
        r"\[(?P<zone>Hand|Stash)\]\s*"
        r"\[Socket_(?P<socket>[0-9])\]\s*"
        r"\[(?P<size>Small|Medium|Large)\]",
        re.IGNORECASE,
    )

    def __init__(self):
        self._last_seen_season = None

    def parse_line(self, line: str) -> Optional[Event]:
        raw = line

        if self.RUN_START_MARKER in line:
            return Event(type="RunStart", raw=raw)

        m = self.HERO_LINE_RE.search(line)
        if m:
            hero = m.group("hero").strip()
            return Event(type="HeroDetected", raw=raw, hero=hero)

        m = self.HERO_CHANGE_RE.search(line)
        if m:
            hero = m.group("hero").strip()
            return Event(type="HeroDetected", raw=raw, hero=hero)

        m = self.HERO_VO_RE.search(line)
        if m:
            hero = m.group("hero").strip()
            return Event(type="HeroDetected", raw=raw, hero=hero)

        # Season parser
        m = self.SEASON_ID_QS_RE.search(line)
        if m:
            sid = int(m.group("season"))
            if sid != self._last_seen_season:
                self._last_seen_season = sid
                return Event(type="SeasonDetected", raw=raw, season_id=sid)

        m = self.RANK_LINE_RE.search(line)
        if m:
            return Event(type="RankUpdated", raw=raw, rank=int(m.group("rank")))
        
        if self.RUN_END_MARKER in line:
            return Event(type="RunEnd", raw=raw)

        # Item purchases only (itm_*)
        m = self._re_item_purchase.search(line)
        if m:
            return Event(
                type="ItemPurchased",
                raw=raw,
                instance_id=m.group("iid"),
                template_id=m.group("tid"),
            )

        # Fight snapshots: parse board state from GameSimHandler Cards Spawned lines
        if self._re_gamesim_cards_spawned.search(line):
            items: List[Dict[str, Any]] = []
            for m2 in self._re_snapshot_item.finditer(line):
                owner = (m2.group("owner") or "").lower()
                zone = (m2.group("zone") or "").lower()

                if owner != "player":
                    continue

                items.append(
                    {
                        "instance_id": m2.group("iid"),
                        "socket_number": int(m2.group("socket")), # 0-9
                        "size": (m2.group("size") or "").lower(),  # small/medium/large
                        "zone": zone,  # for debug purposes
                    }
                )

            # Only emit if found player items
            if items:
                return Event(
                    type="BoardState",
                    raw=raw,
                    board_items=items,
                )

        return None
