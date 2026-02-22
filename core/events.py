from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional


@dataclass
class Event:
    type: str
    raw: str

    # Common optional fields (only set when relevant)
    template_id: Optional[int] = None
    instance_id: Optional[str] = None
    zone: Optional[str] = None
    socket: Optional[str] = None
    size: Optional[str] = None

    # metadata / derived event info
    # method: Optional[str] = None
    # confidence: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Remove nulls for cleaner JSON
        return {k: v for k, v in d.items() if v is not None}
