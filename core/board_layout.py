from __future__ import annotations

from typing import Any



def size_to_span(size: Optional[str]) -> int:
    if not size:
        return 1
    s = size.lower()
    if s == "medium":
        return 2
    if s == "large":
        return 3
    return 1


def build_board_grid(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Build a 10-slot visual grid.

    - Items span 1/2/3 sockets based on size.
    - If an item has template_id missing but size is present, it still spans (shows empty placeholder).
    - Any remaining sockets become explicit empty 1-span blocks, so the UI always shows all sockets.
    Returns a list of blocks: {start, span, name, size, template_id, socket_number}
    """
    items_sorted = sorted(items, key=lambda x: int(x.get("socket_number", 999)))

    occupied = [False] * 10
    blocks: List[Dict[str, Any]] = []

    def occupy(start: int, span: int) -> None:
        for s in range(start, start + span):
            if 0 <= s < 10:
                occupied[s] = True

    # 1) Place known item blocks
    for it in items_sorted:
        start = int(it.get("socket_number", 999))
        if start < 0 or start > 9:
            continue
        if occupied[start]:
            continue

        span = size_to_span(it.get("size"))
        span = max(1, min(span, 10 - start))

        # If any of the target cells already occupied, shrink span to fit the first free stretch
        # (shouldn't happen normally, but avoids visual overlap)
        while span > 1 and any(occupied[s] for s in range(start, start + span)):
            span -= 1
        if any(occupied[s] for s in range(start, start + span)):
            continue

        occupy(start, span)

        blocks.append(
            {
                "start": start,
                "span": span,
                "name": it.get("name") or "(unknown item)",
                "size": it.get("size") or "small",
                "template_id": it.get("template_id"),  # may be None
                "socket_number": start,  # edit/clear uses the first socket
            }
        )

    # 2) Fill remaining sockets with explicit empties (span=1)
    for s in range(10):
        if not occupied[s]:
            blocks.append(
                {
                    "start": s,
                    "span": 1,
                    "name": "(empty)",
                    "size": "small",
                    "template_id": None,
                    "socket_number": s,
                }
            )

    # Return blocks sorted by socket
    return sorted(blocks, key=lambda b: b["start"])
