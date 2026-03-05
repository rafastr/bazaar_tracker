from typing import List, Optional

from .events import Event
from .run_history_db import RunHistoryDb


class RunHistorySink:
    """
    Writes FinalBoardSnapshot to sqlite run history.
    """

    def __init__(self, db: RunHistoryDb) -> None:
        self.db = db
        self._last_run_id: Optional[int] = None


    def handle(self, ev: Event) -> List[Event]:
        if ev.type == "FinalBoardSnapshot":
            if not ev.board_items:
                return []

            # board_items already sorted by socket
            run_id = self.db.insert_run(
                ev.board_items,
                screenshot_path=ev.screenshot_path,
                hero=ev.hero,
                season_id=ev.season_id,
            )
            self._last_run_id = run_id
            print({"type": "RunStored", "run_id": run_id, "items": len(ev.board_items)})
            return []

        if ev.type == "RankUpdated" and ev.rank is not None:
            # rank line appears after the run ends; apply it to the last stored run
            if self._last_run_id is not None:
                self.db.update_run_rank(self._last_run_id, int(ev.rank))
                print({"type": "RunRankStored", "run_id": self._last_run_id, "rank": int(ev.rank)})
            return []

        return []
