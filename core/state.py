from __future__ import annotations

from collections import deque
from typing import Deque, Dict, Iterable

from .events import Event


class RunState:
    """
    Minimal run-scoped state machine + instance/template inference.

    Inference v0:
      - Queue purchases (TemplateId).
      - When a CardSpawned arrives and we have pending purchases, map FIFO.
    """

    def __init__(self) -> None:
        self.in_run: bool = False

        # Run-local correlation buffers
        self._pending_purchases: Deque[int] = deque()

        # Persistent in-memory map (later: load/store via sqlite)
        self.instance_map: Dict[str, int] = {}

    def handle(self, ev: Event) -> Iterable[Event]:
        # Always pass through original event
        yield ev

        if ev.type == "RunStart":
            self.in_run = True
            self._pending_purchases.clear()
            return

        if ev.type == "RunEnd":
            self.in_run = False
            self._pending_purchases.clear()
            return

        # Ignore correlation outside of a run
        if not self.in_run:
            return

        if ev.type == "Purchase" and ev.template_id is not None:
            self._pending_purchases.append(ev.template_id)
            return

        if ev.type == "CardSpawned" and ev.instance_id:
            iid = ev.instance_id

            # If we already know this instance, we can emit a "KnownInstance" later if useful
            if iid in self.instance_map:
                return

            if self._pending_purchases:
                tid = self._pending_purchases.popleft()
                self.instance_map[iid] = tid

                yield Event(
                    type="MappingInferred",
                    raw=ev.raw,
                    instance_id=iid,
                    template_id=tid,
                    method="purchase->spawn_fifo",
                    confidence=0.60,
                )
            return
