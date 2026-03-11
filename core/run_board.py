from __future__ import annotations

import sqlite3
from typing import Any

from core.board_layout import visible_board_items


def get_effective_board_items(
    conn: sqlite3.Connection,
    run_id: int,
) -> list[dict[str, Any]]:
    """
    Return the final visible board items for a run after applying:
    - base run_items
    - run_item_overrides
    - size overrides
    - blanked sockets
    - overlap resolution

    Output shape:
    [
      {
        "socket_number": int,
        "template_id": str,
        "size": str,
      },
      ...
    ]
    """
    cur = conn.cursor()

    cur.execute(
        """
        SELECT socket_number, template_id, size
        FROM run_items
        WHERE run_id=?
        """,
        (int(run_id),),
    )
    base = {
        int(r["socket_number"]): {
            "template_id": (r["template_id"] or "").strip(),
            "size": (r["size"] or "").strip().lower() or "small",
        }
        for r in cur.fetchall()
    }

    cur.execute(
        """
        SELECT socket_number, template_id_override, size_override
        FROM run_item_overrides
        WHERE run_id=?
        """,
        (int(run_id),),
    )
    ov = {
        int(r["socket_number"]): {
            "template_id": r["template_id_override"],
            "size": r["size_override"],
        }
        for r in cur.fetchall()
    }

    merged: list[dict[str, Any]] = []
    all_sockets = sorted(set(base.keys()) | set(ov.keys()))

    for sock in all_sockets:
        b = base.get(sock, {"template_id": "", "size": "small"})
        tid = b["template_id"]
        size = b["size"]

        if sock in ov:
            if ov[sock]["template_id"] is not None:
                tid = (ov[sock]["template_id"] or "").strip()
            if ov[sock]["size"] is not None:
                size = (ov[sock]["size"] or "").strip().lower() or "small"

        if tid:
            merged.append(
                {
                    "socket_number": sock,
                    "template_id": tid,
                    "size": size,
                }
            )

    return visible_board_items(merged)


def get_effective_board_items_with_meta(
    conn: sqlite3.Connection,
    templates_conn: sqlite3.Connection,
    run_id: int,
) -> list[dict[str, Any]]:
    """
    Same as get_effective_board_items(), but resolves item metadata from templates.

    Output shape:
    [
      {
        "socket_number": int,
        "template_id": str,
        "size": str,
        "name": str | None,
        "art_key": str | None,
      },
      ...
    ]
    """
    items = get_effective_board_items(conn, run_id)
    if not items:
        return []

    tcur = templates_conn.cursor()
    out: list[dict[str, Any]] = []

    for it in items:
        tid = (it.get("template_id") or "").strip()
        name = None
        art_key = None

        if tid:
            tcur.execute(
                """
                SELECT name, art_key
                FROM templates
                WHERE template_id = ?
                  AND COALESCE(ignored, 0) = 0
                """,
                (tid,),
            )
            row = tcur.fetchone()
            if row:
                name = row["name"]
                art_key = row["art_key"]

        out.append(
            {
                "socket_number": int(it["socket_number"]),
                "template_id": tid,
                "size": (it.get("size") or "small"),
                "name": name,
                "art_key": art_key,
            }
        )

    return out



def get_effective_socket_state(
    conn: sqlite3.Connection,
    run_id: int,
) -> dict[int, dict[str, Any]]:
    """
    Returns effective editor state for all 10 sockets, including empty ones.

    {
      0: {"template_id": "...", "size": "small"},
      1: {"template_id": "",    "size": "medium"},
      ...
    }
    """
    cur = conn.cursor()

    cur.execute(
        """
        SELECT socket_number, template_id, size
        FROM run_items
        WHERE run_id=?
        """,
        (int(run_id),),
    )
    base = {
        int(r["socket_number"]): {
            "template_id": (r["template_id"] or "").strip(),
            "size": (r["size"] or "").strip().lower() or "small",
        }
        for r in cur.fetchall()
    }

    cur.execute(
        """
        SELECT socket_number, template_id_override, size_override
        FROM run_item_overrides
        WHERE run_id=?
        """,
        (int(run_id),),
    )
    ov = {
        int(r["socket_number"]): {
            "template_id": r["template_id_override"],
            "size": r["size_override"],
        }
        for r in cur.fetchall()
    }

    out: dict[int, dict[str, Any]] = {}

    for sock in range(10):
        b = base.get(sock, {"template_id": "", "size": "small"})
        tid = b["template_id"]
        size = b["size"]

        if sock in ov:
            if ov[sock]["template_id"] is not None:
                tid = (ov[sock]["template_id"] or "").strip()
            if ov[sock]["size"] is not None:
                size = (ov[sock]["size"] or "").strip().lower() or "small"

        out[sock] = {
            "template_id": tid,
            "size": size,
        }

    return out


def build_editor_board_blocks(
    socket_state: dict[int, dict[str, Any]],
    meta_by_socket: dict[int, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """
    Build board blocks for view/edit mode, including empty medium/large placeholders.
    If meta_by_socket is provided, item blocks also carry name/art_key metadata.
    """
    blocks: list[dict[str, Any]] = []
    covered: set[int] = set()
    meta_by_socket = meta_by_socket or {}

    for sock in range(10):
        if sock in covered:
            continue

        st = socket_state.get(sock, {"template_id": "", "size": "small"})
        tid = (st.get("template_id") or "").strip()
        size = (st.get("size") or "small").strip().lower()

        span = 1
        if size == "medium":
            span = 2
        elif size == "large":
            span = 3

        meta = meta_by_socket.get(sock, {})

        blocks.append(
            {
                "socket_number": sock,
                "template_id": tid,
                "size": size,
                "span": span,
                "name": meta.get("name") or ("(empty)" if not tid else ""),
                "art_key": meta.get("art_key"),
                "is_empty": not bool(tid),
            }
        )

        for s in range(sock + 1, min(sock + span, 10)):
            covered.add(s)

    return blocks
