from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any

from core.config import settings


def _add_file_if_exists(zf: zipfile.ZipFile, real_path: str | os.PathLike[str], arcname: str) -> bool:
    p = Path(real_path)
    if not p.exists() or not p.is_file():
        return False
    zf.write(p, arcname=arcname)
    return True


def _add_tree_if_exists(
    zf: zipfile.ZipFile,
    root_path: str | os.PathLike[str],
    arc_prefix: str,
) -> int:
    root = Path(root_path)
    if not root.exists() or not root.is_dir():
        return 0

    count = 0
    for path in root.rglob("*"):
        if path.is_file():
            rel = path.relative_to(root).as_posix()
            zf.write(path, arcname=f"{arc_prefix}/{rel}")
            count += 1
    return count


def export_everything_to_zip(out_zip: str) -> dict[str, Any]:
    out_path = Path(out_zip)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    included: dict[str, Any] = {
        "files": [],
        "dirs": {},
    }

    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        manifest = {
            "export_version": 1,
            "app": "bazaar_chronicles",
            "exported_at_unix": int(time.time()),
            "data_dir": str(settings.data_dir),
            "contents": {
                "run_history_db": "run_history.sqlite3",
                "templates_db": "templates.sqlite3",
                "instance_map": "instance_map.json",
                "run_meta": "run_meta.json",
                "screenshots": "screenshots/",
                "item_images": "assets/images/items/",
            },
        }
        zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))

        if _add_file_if_exists(zf, settings.run_history_db_path, "run_history.sqlite3"):
            included["files"].append("run_history.sqlite3")

        if _add_file_if_exists(zf, settings.templates_db_path, "templates.sqlite3"):
            included["files"].append("templates.sqlite3")

        if _add_file_if_exists(zf, settings.instance_map_path, "instance_map.json"):
            included["files"].append("instance_map.json")

        if _add_file_if_exists(zf, settings.run_meta_path, "run_meta.json"):
            included["files"].append("run_meta.json")

        included["dirs"]["screenshots"] = _add_tree_if_exists(
            zf,
            settings.screenshot_dir,
            "screenshots",
        )

        included["dirs"]["item_images"] = _add_tree_if_exists(
            zf,
            settings.item_images_dir,
            "assets/images/items",
        )

    return {
        "ok": True,
        "message": "Full backup created",
        "out_path": str(out_path),
        "included": included,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export full BazaarChronicles backup to a zip file")
    p.add_argument("out_zip", help="Output .zip path")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    result = export_everything_to_zip(args.out_zip)
    print(result)


if __name__ == "__main__":
    main()
