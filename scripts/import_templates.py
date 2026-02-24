import argparse
import json
from typing import Any, Dict, List, Optional

from core.templates_db import TemplatesDb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import The Bazaar templates from cards.json")
    p.add_argument(
        "cards_json",
        help="Path to cards.json (formatted or minified)",
    )
    p.add_argument(
        "--db",
        dest="db_path",
        default="db/templates.sqlite3",
        help="Output sqlite DB path for templates",
    )
    return p.parse_args()


def _safe_get_title_text(card: Dict[str, Any]) -> Optional[str]:
    loc = card.get("Localization")
    if isinstance(loc, dict):
        title = loc.get("Title")
        if isinstance(title, dict):
            txt = title.get("Text")
            if isinstance(txt, str) and txt.strip():
                return txt.strip()
    return None


def main() -> None:
    args = parse_args()

    with open(args.cards_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Root looks like { "5.0.0": [ ... ] }
    if not isinstance(data, dict) or not data:
        raise RuntimeError("Unexpected JSON structure: expected object with version key(s)")

    all_rows: List[Dict[str, Any]] = []
    total_cards = 0
    total_items = 0

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

            size = card.get("Size")
            if not isinstance(size, str):
                size = None

            heroes = card.get("Heroes")
            if not isinstance(heroes, list):
                heroes = []
            heroes = [h for h in heroes if isinstance(h, str)]

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

    db = TemplatesDb(args.db_path)
    try:
        chunk_size = 1000
        for i in range(0, len(all_rows), chunk_size):
            db.upsert_templates(all_rows[i : i + chunk_size])
    finally:
        db.close()

    print(
        {
            "type": "TemplatesImported",
            "source": args.cards_json,
            "db": args.db_path,
            "cards_seen": total_cards,
            "items_imported": total_items,
        }
    )


if __name__ == "__main__":
    main()
