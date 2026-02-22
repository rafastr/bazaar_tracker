import os
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Settings:
    # Log file
    log_path: str
    poll_interval_seconds: float = 0.5
    log_encoding: str = "utf-8"
    log_encoding_errors: str = "ignore"

    # Output
    pretty_json: bool = False

    # Optional extra sleep in main loop (usually 0)
    loop_sleep_seconds: float = 0.0

    # Screenshots
    enable_screenshots: bool = True
    screenshot_dir: str = "screenshots"
    screenshot_monitor_index: int = 1  # mss monitors[1] = primary on Windows typically
    screenshot_delay_seconds: float = 2.0
    screenshot_cooldown_seconds: float = 10.0

    # Which event types should cause a screenshot
    screenshot_trigger_event_types: List[str] = None  # set in __post_init__ style below


def _default_log_path() -> str:
    userprofile = os.environ.get("USERPROFILE")
    if not userprofile:
        # Best effort fallback; user can override by editing settings/log_path
        return r"Player.log"
    return os.path.join(
        userprofile,
        r"AppData\LocalLow\Tempo Storm\The Bazaar\Player.log",
    )


# Instantiate settings (edit here if you want)
settings = Settings(
    log_path=_default_log_path(),
    screenshot_trigger_event_types=[
        # your current reliable trigger
        "CardRevealSequenceStart",
    ],
)
