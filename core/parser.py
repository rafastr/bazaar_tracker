from __future__ import annotations

import re
from typing import Optional

from .events import Event


class LogParser:
    # Known markers from your notes
    RUN_START_MARKER = "[StartRunAppState] Run initialization finalized."
    RUN_END_MARKER = "Starting card reveal sequence"

    # Your current trigger string
    REVEAL_TRIGGER = "Starting card reveal sequence"

    # TemplateId appears in purchase events (exact format can vary)
    _re_template_id = re.compile(r"TemplateId\s*[:=]\s*(?P<tid>\d+)", re.IGNORECASE)

    # Cards Spawned lines: we *know* instance IDs are like itm_...
    # The rest (zone/socket/size) is best-effort until we see exact wording.
    _re_spawn = re.compile(
        r"Cards\s+Spawned.*?(?P<iid>itm_[A-Za-z0-9_]+)"
        r"(?:.*?\b(?:zone|Zone)\b\s*[:=]\s*(?P<zone>[A-Za-z0-9_]+))?"
        r"(?:.*?\b(?:socket|Socket)\b\s*[:=]\s*(?P<socket>[A-Za-z0-9_]+))?"
        r"(?:.*?\b(?:size|Size)\b\s*[:=]\s*(?P<size>[A-Za-z0-9_]+))?",
        re.IGNORECASE,
    )

    def parse_line(self, line: str) -> Optional[Event]:
        raw = line

        # Run boundaries
        if self.RUN_START_MARKER in line:
            return Event(type="RunStart", raw=raw)

        if self.RUN_END_MARKER in line:
            return Event(type="RunEnd", raw=raw)

        # Existing end-of-run-ish trigger you used for screenshots
        if self.REVEAL_TRIGGER in line:
            return Event(type="CardRevealSequenceStart", raw=raw)

        # Purchase-ish: any line containing TemplateId
        m = self._re_template_id.search(line)
        if m:
            return Event(type="Purchase", raw=raw, template_id=int(m.group("tid")))

        # Spawn lines
        m = self._re_spawn.search(line)
        if m:
            return Event(
                type="CardSpawned",
                raw=raw,
                instance_id=m.group("iid"),
                zone=m.group("zone"),
                socket=m.group("socket"),
                size=m.group("size"),
            )

        return None
