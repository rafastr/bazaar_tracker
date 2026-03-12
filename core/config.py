from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


APP_NAME = "Bazaar Chronicles"
APP_VERSION = "0.8.0"


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _default_data_dir() -> Path:
    override = os.environ.get("BAZAAR_TRACKER_DATA_DIR")
    if override:
        return Path(override).expanduser().resolve()

    appdata = os.environ.get("APPDATA")
    if appdata:
        return Path(appdata) / APP_NAME

    # Fallback for non-Windows dev environments if no override is set.
    return Path.home() / f".{APP_NAME.lower()}"


def _default_log_path() -> str:
    userprofile = os.environ.get("USERPROFILE")
    if not userprofile:
        return r"Player.log"
    return os.path.join(
        userprofile,
        r"AppData\LocalLow\Tempo Storm\The Bazaar\Player.log",
    )


@dataclass(frozen=True)
class Settings:
    # Base user data directory
    data_dir: Path

    # App files under data dir
    exports_dir: Path
    logs_dir: Path
    assets_dir: Path
    item_images_dir: Path
    screenshot_dir: str

    # Log file and db
    log_path: str
    instance_map_path: str
    run_meta_path: str
    run_history_db_path: str
    templates_db_path: str

    poll_interval_seconds: float = 0.5
    log_encoding: str = "utf-8"
    log_encoding_errors: str = "ignore"

    # Output
    pretty_json: bool = False

    # Optional extra sleep in main loop (usually 0)
    loop_sleep_seconds: float = 0.0

    # Screenshots
    enable_screenshots: bool = True
    screenshot_monitor_index: int = 1  # mss monitors[1] = primary on Windows typically
    screenshot_delay_seconds: float = 3.0
    screenshot_cooldown_seconds: float = 10.0

    # Which event types should cause a screenshot
    screenshot_trigger_event_types: Optional[List[str]] = None

    # Path of tesseract, needed on Windows
    tesseract_cmd: str | None = None


def build_settings() -> Settings:
    data_dir = _ensure_dir(_default_data_dir())
    exports_dir = _ensure_dir(data_dir / "exports")
    logs_dir = _ensure_dir(data_dir / "logs")
    assets_dir = _ensure_dir(data_dir / "assets")
    item_images_dir = _ensure_dir(assets_dir / "images" / "items")
    screenshots_dir = _ensure_dir(data_dir / "screenshots")

    return Settings(
        data_dir=data_dir,
        exports_dir=exports_dir,
        logs_dir=logs_dir,
        assets_dir=assets_dir,
        item_images_dir=item_images_dir,
        screenshot_dir=str(screenshots_dir),
        log_path=_default_log_path(),
        instance_map_path=str(data_dir / "instance_map.json"),
        run_meta_path=str(data_dir / "run_meta.json"),
        run_history_db_path=str(data_dir / "run_history.sqlite3"),
        templates_db_path=str(data_dir / "templates.sqlite3"),
        screenshot_trigger_event_types=["RunEnd"],
    )


settings = build_settings()
