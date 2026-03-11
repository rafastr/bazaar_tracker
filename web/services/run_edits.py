from __future__ import annotations

import os
import shutil
import time
from pathlib import Path
from typing import Optional

from core.config import settings
from core.run_history_db import RunHistoryDb
from core.ocr_metrics import extract_run_metrics
from core.ocr_rois import ROIS


def _rebuild_after_edit(db: RunHistoryDb) -> None:
    """
    Rebuild all derived state that depends on effective run data.
    """
    db.rebuild_item_hero_wins()
    db.rebuild_item_firsts(settings.templates_db_path)
    db.rebuild_achievements(settings.templates_db_path)


def confirm_run(run_id: int, confirmed: bool = True) -> None:
    db = RunHistoryDb(settings.run_history_db_path)
    try:
        db.confirm_run(int(run_id), confirmed=bool(confirmed), templates_db_path=settings.templates_db_path)
        _rebuild_after_edit(db)
    finally:
        db.close()


def set_hero_override(run_id: int, hero: Optional[str]) -> None:
    db = RunHistoryDb(settings.run_history_db_path)
    try:
        hero_clean = (hero or "").strip()
        if hero_clean:
            db.set_run_hero_override(int(run_id), hero_clean)
        else:
            db.clear_run_hero_override(int(run_id))
        _rebuild_after_edit(db)
    finally:
        db.close()


def set_rank_override(run_id: int, rank: Optional[int]) -> None:
    db = RunHistoryDb(settings.run_history_db_path)
    try:
        if rank is None:
            db.clear_run_rank_override(int(run_id))
        else:
            db.set_run_rank_override(int(run_id), int(rank))
        _rebuild_after_edit(db)
    finally:
        db.close()


def set_run_notes(run_id: int, notes: Optional[str]) -> None:
    db = RunHistoryDb(settings.run_history_db_path)
    try:
        notes_clean = (notes or "").strip()
        db.set_run_notes(int(run_id), notes_clean)
        # notes do not affect derived stats
    finally:
        db.close()


def set_item_override(
    run_id: int,
    socket_number: int,
    template_id: Optional[str],
    size: Optional[str] = None,
    note: Optional[str] = None,
) -> None:
    db = RunHistoryDb(settings.run_history_db_path)
    try:
        cur = db.conn.cursor()

        template_id_clean = (template_id or "").strip() or None
        size_clean = (size or "").strip().lower() or None
        note_clean = (note or "").strip() or None

        # Preserve / recover the effective template if this is a size-only edit.
        if template_id_clean is None:
            cur.execute(
                """
                SELECT template_id_override
                FROM run_item_overrides
                WHERE run_id = ? AND socket_number = ?
                """,
                (int(run_id), int(socket_number)),
            )
            row = cur.fetchone()

            if row and row["template_id_override"] is not None:
                existing_override_tid = (row["template_id_override"] or "").strip()
                if existing_override_tid:
                    # Real override already present.
                    template_id_clean = existing_override_tid
                else:
                    # Explicit blank override exists.
                    # Recover from base run_items instead of keeping it blank.
                    cur.execute(
                        """
                        SELECT template_id
                        FROM run_items
                        WHERE run_id = ? AND socket_number = ?
                        """,
                        (int(run_id), int(socket_number)),
                    )
                    base_row = cur.fetchone()
                    if base_row and base_row["template_id"]:
                        template_id_clean = (base_row["template_id"] or "").strip() or None
            else:
                cur.execute(
                    """
                    SELECT template_id
                    FROM run_items
                    WHERE run_id = ? AND socket_number = ?
                    """,
                    (int(run_id), int(socket_number)),
                )
                row = cur.fetchone()
                if row and row["template_id"]:
                    template_id_clean = (row["template_id"] or "").strip() or None

        db.upsert_item_override(
            int(run_id),
            int(socket_number),
            template_id_override=template_id_clean if template_id is not None else None,
            size_override=size_clean,
            note=note_clean,
        )

        effective_size = size_clean
        if not effective_size:
            cur.execute(
                """
                SELECT size_override
                FROM run_item_overrides
                WHERE run_id = ? AND socket_number = ?
                """,
                (int(run_id), int(socket_number)),
            )
            row = cur.fetchone()
            if row and row["size_override"]:
                effective_size = row["size_override"]
            else:
                cur.execute(
                    """
                    SELECT size
                    FROM run_items
                    WHERE run_id = ? AND socket_number = ?
                    """,
                    (int(run_id), int(socket_number)),
                )
                row = cur.fetchone()
                effective_size = row["size"] if row and row["size"] else "small"

        _clear_covered_sockets(cur, run_id, socket_number, effective_size)

        db.conn.commit()
        _rebuild_after_edit(db)

    finally:
        db.close()


def clear_item_override(run_id: int, socket_number: int) -> None:
    db = RunHistoryDb(settings.run_history_db_path)
    try:
        db.clear_item_override(int(run_id), int(socket_number))
        _rebuild_after_edit(db)
    finally:
        db.close()


def update_run_metrics(
    run_id: int,
    *,
    season_id: Optional[int],
    rank: Optional[int],
    wins: Optional[int],
    max_health: Optional[int],
    prestige: Optional[int],
    level: Optional[int],
    income: Optional[int],
    gold: Optional[int],
) -> None:
    db = RunHistoryDb(settings.run_history_db_path)
    try:
        cur = db.conn.cursor()
        now = int(time.time())

        cur.execute(
            """
            UPDATE runs
            SET season_id = ?
            WHERE run_id = ?
            """,
            (season_id, int(run_id)),
        )

        cur.execute(
            """
            INSERT OR IGNORE INTO run_overrides (
                run_id,
                hero_override,
                rank_override,
                notes,
                is_confirmed,
                updated_at_unix
            )
            VALUES (?, NULL, NULL, NULL, 0, ?)
            """,
            (int(run_id), now),
        )

        cur.execute(
            """
            UPDATE run_overrides
            SET rank_override = ?,
                updated_at_unix = ?
            WHERE run_id = ?
            """,
            (
                rank,
                now,
                int(run_id),
            ),
        )

        if wins is None:
            won = None
        else:
            won = 1 if wins >= 10 else 0

        cur.execute(
            """
            INSERT OR IGNORE INTO run_metrics (
                run_id,
                wins,
                max_health,
                prestige,
                level,
                income,
                gold,
                won,
                ocr_json,
                ocr_version,
                updated_at_unix
            )
            VALUES (?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?)
            """,
            (int(run_id), now),
        )

        cur.execute(
            """
            UPDATE run_metrics
            SET wins = ?,
                max_health = ?,
                prestige = ?,
                level = ?,
                income = ?,
                gold = ?,
                won = ?,
                updated_at_unix = ?
            WHERE run_id = ?
            """,
            (
                wins,
                max_health,
                prestige,
                level,
                income,
                gold,
                won,
                now,
                int(run_id),
            ),
        )

        db.conn.commit()
        _rebuild_after_edit(db)
    finally:
        db.close()


def _copy_run_screenshot(run_id: int, source_path: str) -> str:
    src = Path(source_path).expanduser().resolve()
    if not src.exists():
        raise FileNotFoundError(f"Screenshot not found: {src}")

    screenshots_dir = Path(settings.screenshot_dir)
    screenshots_dir.mkdir(parents=True, exist_ok=True)

    ext = src.suffix.lower() or ".png"
    now = int(time.time())
    dst = screenshots_dir / f"run_{int(run_id)}_{now}{ext}"

    shutil.copy2(src, dst)
    return str(dst)


def set_run_screenshot(
    run_id: int,
    *,
    source_path: str,
    reread_metrics: bool = False,
) -> str:
    db = RunHistoryDb(settings.run_history_db_path)
    try:
        copied_path = _copy_run_screenshot(int(run_id), source_path)

        cur = db.conn.cursor()
        cur.execute(
            """
            UPDATE runs
            SET screenshot_path = ?
            WHERE run_id = ?
            """,
            (copied_path, int(run_id)),
        )

        if reread_metrics:
            metrics = extract_run_metrics(copied_path, ROIS, ocr_version="v1")
            db.upsert_run_metrics(
                int(run_id),
                wins=metrics.get("wins"),
                max_health=metrics.get("max_health"),
                prestige=metrics.get("prestige"),
                level=metrics.get("level"),
                income=metrics.get("income"),
                gold=metrics.get("gold"),
                won=metrics.get("won"),
                ocr_json=metrics.get("ocr_json"),
                ocr_version=metrics.get("ocr_version"),
            )
            db.conn.commit()
            _rebuild_after_edit(db)
        else:
            db.conn.commit()

        return copied_path
    finally:
        db.close()


def reread_run_metrics_from_screenshot(run_id: int) -> None:
    db = RunHistoryDb(settings.run_history_db_path)
    try:
        cur = db.conn.cursor()
        cur.execute(
            "SELECT screenshot_path FROM runs WHERE run_id = ?",
            (int(run_id),),
        )
        row = cur.fetchone()
        shot = row["screenshot_path"] if row else None
        if not shot:
            raise ValueError("Run has no screenshot")

        metrics = extract_run_metrics(shot, ROIS, ocr_version="v1")
        db.upsert_run_metrics(
            int(run_id),
            wins=metrics.get("wins"),
            max_health=metrics.get("max_health"),
            prestige=metrics.get("prestige"),
            level=metrics.get("level"),
            income=metrics.get("income"),
            gold=metrics.get("gold"),
            won=metrics.get("won"),
            ocr_json=metrics.get("ocr_json"),
            ocr_version=metrics.get("ocr_version"),
        )
        db.conn.commit()
        _rebuild_after_edit(db)
    finally:
        db.close()


def create_manual_run(
    *,
    hero: str | None,
    season_id: Optional[int],
    wins: Optional[int],
    max_health: Optional[int],
    prestige: Optional[int],
    level: Optional[int],
    income: Optional[int],
    gold: Optional[int],
    notes: str | None,
    confirmed: bool,
) -> int:
    import time

    db = RunHistoryDb(settings.run_history_db_path)
    try:
        cur = db.conn.cursor()
        now = int(time.time())

        hero = (hero or "").strip() or None
        notes = (notes or "").strip()

        won = None if wins is None else (1 if wins >= 10 else 0)

        cur.execute(
            """
            INSERT INTO runs (
                ended_at_unix,
                hero,
                is_confirmed,
                notes,
                season_id
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                now,
                hero,
                1 if confirmed else 0,
                notes or None,
                season_id,
            ),
        )
        run_id = int(cur.lastrowid)

        cur.execute(
            """
            INSERT INTO run_metrics (
                run_id,
                wins,
                max_health,
                prestige,
                level,
                income,
                gold,
                won,
                ocr_json,
                ocr_version,
                updated_at_unix
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
            """,
            (
                run_id,
                wins,
                max_health,
                prestige,
                level,
                income,
                gold,
                won,
                now,
            ),
        )

        cur.execute(
            """
            INSERT INTO run_overrides (
                run_id,
                hero_override,
                notes,
                is_confirmed,
                updated_at_unix
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                run_id,
                hero,
                notes or None,
                1 if confirmed else 0,
                now,
            ),
        )

        db.conn.commit()
        _rebuild_after_edit(db)
        return run_id
    finally:
        db.close()


def _size_to_span(size: str | None) -> int:
    s = (size or "small").strip().lower()
    if s == "medium":
        return 2
    if s == "large":
        return 3
    return 1


def _blank_socket_override(cur, run_id: int, socket: int) -> None:
    now = int(time.time())

    cur.execute(
        """
        INSERT INTO run_item_overrides (
            run_id, socket_number, template_id_override, size_override, note, updated_at_unix
        )
        VALUES (?, ?, '', NULL, NULL, ?)
        ON CONFLICT(run_id, socket_number) DO UPDATE SET
            template_id_override = excluded.template_id_override,
            size_override = NULL,
            note = NULL,
            updated_at_unix = excluded.updated_at_unix
        """,
        (int(run_id), int(socket), now),
    )


def _clear_covered_sockets(cur, run_id: int, socket: int, size: str | None) -> None:
    span = _size_to_span(size)
    end = min(10, int(socket) + span)

    for covered in range(int(socket) + 1, end):
        _blank_socket_override(cur, run_id, covered)


def delete_run(run_id: int) -> None:
    db = RunHistoryDb(settings.run_history_db_path)
    try:
        cur = db.conn.cursor()

        # delete dependent rows first
        cur.execute("DELETE FROM run_item_overrides WHERE run_id = ?", (int(run_id),))
        cur.execute("DELETE FROM run_items WHERE run_id = ?", (int(run_id),))
        cur.execute("DELETE FROM run_metrics WHERE run_id = ?", (int(run_id),))
        cur.execute("DELETE FROM run_overrides WHERE run_id = ?", (int(run_id),))

        # then delete the run itself
        cur.execute("DELETE FROM runs WHERE run_id = ?", (int(run_id),))

        db.conn.commit()
        _rebuild_after_edit(db)
    finally:
        db.close()


