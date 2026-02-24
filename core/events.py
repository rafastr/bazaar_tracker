from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional


@dataclass
class Event:
    type: str
    raw: str

    # Purchase mapping
    template_id: Optional[str] = None
    instance_id: Optional[str] = None

    # Board snapshot
    board_items: Optional[List[Dict[str, Any]]] = None

    screenshot_path: Optional[str] = None

    # misc
    method: Optional[str] = None
    confidence: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None}
