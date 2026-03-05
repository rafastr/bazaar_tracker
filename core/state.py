from __future__ import annotations

from typing import Dict, Iterable, List, Any, Optional

from .events import Event
from .instance_store import InstanceStore
from .run_meta_store import RunMetaStore


class RunState:
    """
    Active-run state.
    - Instance_map is persisted to JSON so a run can survive game/tracker restarts.
    - Clear the cache ONLY when the run ends (RunEnd).
    - Not keep completed-run data.
    """

    def __init__(self, store: InstanceStore, meta_store: RunMetaStore) -> None:
        self.store = store
        self.meta_store = meta_store


        # Persisted across sessions for an ongoing run
        self.instance_map: Dict[str, str] = self.store.load()
        self.current_hero: Optional[str] = self.meta_store.get_hero()
        self.current_season_id: Optional[int] = None

        self.in_run: bool = False
        self.last_player_board: Optional[List[Dict[str, Any]]] = None

        self.last_screenshot_path: Optional[str] = None

    def _clear_active_run_cache(self) -> None:
        self.instance_map.clear()
        self.store.save(self.instance_map)
        self.current_hero = None
        self.current_season_id = None
        self.meta_store.clear()


    def handle(self, ev: Event) -> Iterable[Event]:
        # always pass through
        yield ev

        if ev.type == "RunStart":
            # Don't clear instance_map here: run may be resuming and log may have reset.
            self.in_run = True
            self.last_player_board = None
            self.last_screenshot_path = None
            return

        if ev.type == "HeroDetected" and ev.hero:
            self.current_hero = ev.hero
            self.meta_store.set_hero(ev.hero)
            return

        if ev.type == "SeasonDetected" and ev.season_id is not None:
            self.current_season_id = ev.season_id
            return

        # Auto-enter run if tracker started mid-run
        if not self.in_run and ev.type in ("ItemPurchased", "BoardState"):
            self.in_run = True

        if not self.in_run:
            return

        # Persist mapping immediately when we see it
        if ev.type == "ItemPurchased" and ev.instance_id and ev.template_id:
            self.instance_map[ev.instance_id] = ev.template_id
            self.store.save(self.instance_map)
            return

        if ev.type == "BoardState" and ev.board_items:
            # Keep the most recent snapshot (we only care about final fight)
            self.last_player_board = ev.board_items
            return

        if ev.type == "ScreenshotSaved" and ev.screenshot_path:
            self.last_screenshot_path = ev.screenshot_path
            return

        if ev.type == "RunEnd":
            if self.last_player_board:
                enriched = []
                for item in self.last_player_board:
                    iid = item.get("instance_id")
                    tid = self.instance_map.get(iid) if isinstance(iid, str) else None

                    enriched_item = dict(item)
                    enriched_item["template_id"] = tid
                    enriched_item["template_known"] = tid is not None
                    enriched.append(enriched_item)

                sorted_items = sorted(enriched, key=lambda x: x.get("socket_number", 999))

                yield Event(
                    type="FinalBoardSnapshot",
                    raw=ev.raw,
                    board_items=sorted_items,
                    screenshot_path=self.last_screenshot_path,
                    hero=self.current_hero,
                    season_id=self.current_season_id,
                    method="last_seen_gamesimhandler_snapshot + instance_map_join",
                    confidence=1.0,
                )

            # Run is finished: clear active-run cache (you don't want past runs)
            self.in_run = False
            self.last_player_board = None
            self._clear_active_run_cache()
            self.last_screenshot_path = None
            return
