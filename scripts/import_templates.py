from __future__ import annotations

import os
import platform
import argparse
import json
from typing import Any, Dict, List, Optional

from core.templates_db import TemplatesDb


def default_cards_path() -> Optional[str]:
    """
    Default to Windows install path.
    On non-Windows systems, return None.
    """
    if platform.system() == "Windows":
        return r"C:\Program Files (x86)\Steam\steamapps\common\The Bazaar\TheBazaar_Data\StreamingAssets\cards.json"
    return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import The Bazaar templates from cards.json")

    p.add_argument(
        "cards_json",
        nargs="?",
        default=default_cards_path(),
        help="Path to cards.json (defaults to Windows install path)",
    )

    p.add_argument(
        "--db",
        dest="db_path",
        default="db/templates.sqlite3",
        help="Output sqlite DB path for templates",
    )

    return p.parse_args()


def should_import_item(name: str) -> bool:
    """Filter obvious non-game items from cards.json."""
    name = name.strip()

    if not name:
        return False

    if "[" in name:
        return False

    if "TEMPLATE" in name.upper():
        return False

    if "DEBUG" in name.upper():
        return False

    return True


def _safe_get_title_text(card: Dict[str, Any]) -> Optional[str]:
    loc = card.get("Localization")
    if isinstance(loc, dict):
        title = loc.get("Title")
        if isinstance(title, dict):
            txt = title.get("Text")
            if isinstance(txt, str) and txt.strip():
                return txt.strip()
    return None


# ------------------------------------------------------------
# Core callable function
# ------------------------------------------------------------

def import_templates_from_cards(
    cards_json: str,
    db_path: str,
) -> dict[str, Any]:

    with open(cards_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) or not data:
        raise RuntimeError("Unexpected JSON structure: expected object with version key(s)")

    all_rows: List[Dict[str, Any]] = []
    total_cards = 0
    total_items = 0
    skipped_templates = 0

    for version_key, cards in data.items():
        if not isinstance(cards, list):
            continue

        for card in cards:

            total_cards += 1

            if not isinstance(card, dict):
                continue

            if card.get("Type") != "Item":
                continue

            template_id = card.get("Id")
            if not isinstance(template_id, str) or not template_id:
                continue

            total_items += 1

            name = _safe_get_title_text(card) or card.get("InternalName") or template_id
            if not isinstance(name, str):
                name = str(name)

            if not should_import_item(name):
                skipped_templates += 1
                continue

            size = card.get("Size")
            if isinstance(size, str):
                size = size.lower()
            else:
                size = None

            heroes = card.get("Heroes")
            if not isinstance(heroes, list):
                heroes = []
            heroes = [h.strip() for h in heroes if isinstance(h, str)]

            tags = card.get("Tags")
            if not isinstance(tags, list):
                tags = []
            tags = [t for t in tags if isinstance(t, str)]

            art_key = card.get("ArtKey")
            if not isinstance(art_key, str):
                art_key = None

            internal_name = card.get("InternalName")
            if not isinstance(internal_name, str):
                internal_name = None

            version = card.get("Version")
            if not isinstance(version, str):
                version = str(version_key)

            all_rows.append(
                {
                    "template_id": template_id,
                    "name": name,
                    "size": size,
                    "heroes_json": json.dumps(heroes, ensure_ascii=False),
                    "tags_json": json.dumps(tags, ensure_ascii=False),
                    "art_key": art_key,
                    "internal_name": internal_name,
                    "version": version,
                }
            )

    db = TemplatesDb(db_path)

    try:
        chunk_size = 1000

        for i in range(0, len(all_rows), chunk_size):
            db.upsert_templates(all_rows[i : i + chunk_size])

    finally:
        db.close()

    return {
        "ok": True,
        "message": "Templates imported",
        "source": cards_json,
        "db": db_path,
        "cards_seen": total_cards,
        "items_imported": len(all_rows),
        "templates_skipped": skipped_templates,
    }


# ------------------------------------------------------------
# CLI wrapper
# ------------------------------------------------------------

def main() -> None:
    args = parse_args()

    result = import_templates_from_cards(
        cards_json=args.cards_json,
        db_path=args.db_path,
    )

    print(
        {
            "type": "TemplatesImported",
            "source": result["source"],
            "db": result["db"],
            "cards_seen": result["cards_seen"],
            "items_imported": result["items_imported"],
            "templates_skipped": result["templates_skipped"],
        }
    )


if __name__ == "__main__":
    main()
