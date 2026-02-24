import json
import os
from typing import Any, Dict, Optional


class RunMetaStore:
    """
    Small JSON cache for active-run metadata that may not reappear in logs
    when resuming a run (e.g., hero).
    Cleared on RunEnd.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)

    def load(self) -> Dict[str, Any]:
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def save(self, data: Dict[str, Any]) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, self.path)

    def set_hero(self, hero: str) -> None:
        data = self.load()
        data["hero"] = hero
        self.save(data)

    def get_hero(self) -> Optional[str]:
        data = self.load()
        hero = data.get("hero")
        return hero if isinstance(hero, str) and hero else None

    def clear(self) -> None:
        self.save({})
