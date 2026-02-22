from __future__ import annotations

import os
import time
from typing import Iterator


def follow_file_lines(
    path: str,
    poll_interval_seconds: float = 0.5,
    encoding: str = "utf-8",
    errors: str = "ignore",
) -> Iterator[str]:
    """
    Incrementally follow a file and yield complete lines as they are appended.

    Handles:
      - file not existing yet
      - truncation/rotation (size < last_pos)
      - partial line writes (keeps a carry buffer)
    """
    last_pos = 0
    carry = ""

    while True:
        try:
            if not os.path.exists(path):
                time.sleep(poll_interval_seconds)
                continue

            size = os.path.getsize(path)
            if size < last_pos:
                # truncated/recreated
                last_pos = 0
                carry = ""

            with open(path, "r", encoding=encoding, errors=errors) as f:
                f.seek(last_pos)
                chunk = f.read()
                last_pos = f.tell()

            if not chunk:
                time.sleep(poll_interval_seconds)
                continue

            carry += chunk

            # Normalize newlines handling:
            # splitlines() handles \n, \r\n, \r
            lines = carry.splitlines(keepends=False)

            # If carry does not end with a newline, last piece may be partial
            ends_with_newline = carry.endswith("\n") or carry.endswith("\r")
            if not ends_with_newline and lines:
                carry = lines.pop()  # keep partial tail
            else:
                carry = ""

            for line in lines:
                # yield non-empty lines too; Unity logs can have blanks but they're useful sometimes
                yield line

        except Exception:
            # Best effort: do not die on transient read errors
            time.sleep(poll_interval_seconds)
            continue


def replay_file_lines(path: str, encoding: str = "utf-8", errors: str = "ignore"):
    """
    Read a file once from start to end, yielding lines.
    Useful for deterministic parser testing on Linux without polling.
    """
    with open(path, "r", encoding=encoding, errors=errors) as f:
        for line in f:
            yield line.rstrip("\n").rstrip("\r")
