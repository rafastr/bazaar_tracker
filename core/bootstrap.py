import json
import shutil
import sqlite3
from pathlib import Path

from core.config import settings


def _read_resource_version(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return str(data.get("resource_version") or "").strip() or None
    except Exception:
        return None


def _copy_images(src_dir: Path, dst_dir: Path) -> None:
    dst_dir.mkdir(parents=True, exist_ok=True)

    for src_path in src_dir.rglob("*"):
        if not src_path.is_file():
            continue

        rel = src_path.relative_to(src_dir)
        dst_path = dst_dir / rel
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dst_path)


def _repair_template_image_paths() -> None:
    """
    Rewrite templates.image_path to match the current user data directory.
    Uses the same naming rule as cache_item_images.py:
      <item_images_dir>/<template_id>.webp
    """
    db_path = Path(settings.templates_db_path)
    items_dir = Path(settings.item_images_dir)

    if not db_path.exists() or not items_dir.exists():
        return

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()

        cur.execute("PRAGMA table_info(templates)")
        cols = {row[1] for row in cur.fetchall()}
        if "image_path" not in cols:
            return

        cur.execute(
            """
            SELECT template_id
            FROM templates
            WHERE template_id IS NOT NULL
            """
        )
        rows = cur.fetchall()

        updated = 0
        cleared = 0

        for (template_id,) in rows:
            tid = str(template_id).strip()
            if not tid:
                continue

            disk_path = items_dir / f"{tid}.webp"

            if disk_path.exists():
                cur.execute(
                    """
                    UPDATE templates
                    SET image_path = ?
                    WHERE template_id = ?
                    """,
                    (str(disk_path), tid),
                )
                updated += 1
            else:
                cur.execute(
                    """
                    UPDATE templates
                    SET image_path = NULL
                    WHERE template_id = ?
                    """,
                    (tid,),
                )
                cleared += 1

        conn.commit()
        print(f"[BOOTSTRAP] repaired image paths: updated={updated}, cleared={cleared}")

    finally:
        conn.close()


def ensure_resources(bundled_resources: Path) -> None:
    """
    Copy bundled resources into the user data directory if missing or outdated.
    """

    bundled_manifest = bundled_resources / "manifest.json"
    user_manifest = Path(settings.data_dir) / "resource_manifest.json"

    bundled_version = _read_resource_version(bundled_manifest)
    user_version = _read_resource_version(user_manifest)

    needs_update = (
        user_version is None
        or bundled_version is None
        or user_version != bundled_version
    )

    # ---- Templates DB ----
    src_templates = bundled_resources / "templates.sqlite3"
    dst_templates = Path(settings.templates_db_path)
    dst_templates.parent.mkdir(parents=True, exist_ok=True)

    if src_templates.exists() and (needs_update or not dst_templates.exists()):
        shutil.copy2(src_templates, dst_templates)
        print("[BOOTSTRAP] copied templates.sqlite3")

    # ---- Item images ----
    src_images = bundled_resources / "assets" / "images" / "items"
    dst_images = Path(settings.item_images_dir)

    should_copy_images = False
    if src_images.exists():
        if not dst_images.exists():
            should_copy_images = True
        elif needs_update:
            should_copy_images = True
        else:
            try:
                should_copy_images = not any(dst_images.iterdir())
            except Exception:
                should_copy_images = True

    if should_copy_images:
        _copy_images(src_images, dst_images)
        print("[BOOTSTRAP] copied item images")

    # ---- Manifest ----
    if bundled_manifest.exists():
        shutil.copy2(bundled_manifest, user_manifest)

    # ---- Repair DB image paths after copy/update ----
    _repair_template_image_paths()
