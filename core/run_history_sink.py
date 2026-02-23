from typing import Optional

from .events import Event
from .run_history_db import RunHistoryDb


class RunHistorySink:
    """
    Writes FinalBoardSnapshot to sqlite run history.
    """

    def __init__(self, db: RunHistoryDb) -> None:
        self.db = db

    def handle(self, ev: Event) -> None:
        if ev.type != "FinalBoardSnapshot":
            return
        if not ev.board_items:
            return

        # board_items already sorted by socket
        run_id = self.db.insert_run(ev.board_items)

        # print a log
        print({"type": "RunStored", "run_id": run_id, "items": len(ev.board_items)})
