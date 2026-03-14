from __future__ import annotations

import json
import os
import time
from typing import List, Optional, Set

from .events import Event
from core.config import settings


_toaster = None

def _notify_screenshot_taken() -> None:
    global _toaster
    try:
        if _toaster is None:
            from win10toast import ToastNotifier
            _toaster = ToastNotifier()

        _toaster.show_toast(
            "Bazaar Chronicle",
            "Final board screenshot captured.",
            duration=3,
            threaded=True,
        )
    except Exception:
        pass


class Sink:
    def handle(self, ev: Event) -> List[Event]:
        return []


class StdoutSink(Sink):
    def __init__(self, pretty: bool = False) -> None:
        self.pretty = pretty

    def handle(self, ev: Event) -> List[Event]:
        d = ev.to_dict()
        if self.pretty:
            print(json.dumps(d, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(d, ensure_ascii=False))
        return []


class ScreenshotSink(Sink):
    def __init__(
        self,
        enabled: bool,
        out_dir: str,
        monitor_index: int = 1,
        delay_seconds: float = 2.5,
        cooldown_seconds: float = 5.0,
        trigger_event_types: Optional[Set[str]] = None,
    ) -> None:
        self.enabled = enabled
        self.out_dir = out_dir
        self.monitor_index = monitor_index
        self.delay_seconds = delay_seconds
        self.cooldown_seconds = cooldown_seconds
        self.trigger_event_types = trigger_event_types or set()

        self._last_shot_ts: float = 0.0

        if self.enabled:
            os.makedirs(self.out_dir, exist_ok=True)

    def handle(self, ev: Event) -> List[Event]:
        if not self.enabled:
            return []

        if ev.type not in self.trigger_event_types:
            return []

        now = time.time()
        if now - self._last_shot_ts < self.cooldown_seconds:
            return []

        self._last_shot_ts = now
        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)

        path = self._take_screenshot(prefix=ev.type)
        if not path:
            return []

        # Emit event so state/db can attach it to the run sql
        return [Event(type="ScreenshotSaved", raw=ev.raw, screenshot_path=path)]


    def _take_screenshot(self, prefix: str = "shot") -> Optional[str]:
        try:
            from mss import mss
            from PIL import Image
        except Exception as e:
            print(json.dumps(
                {"type": "ScreenshotError", "error": repr(e), "hint": "Install mss and pillow"},
                ensure_ascii=False
            ))
            return

        with mss() as sct:
            monitors = sct.monitors
            idx = self.monitor_index

            # clamp index
            if idx < 1 or idx >= len(monitors):
                idx = 1

            monitor = monitors[idx]
            shot = sct.grab(monitor)
            img = Image.frombytes("RGB", shot.size, shot.rgb)

            ts = int(time.time())
            filename = f"{prefix}_{ts}.png"
            path = os.path.join(self.out_dir, filename)
            img.save(path)
            
            print(json.dumps({"type": "ScreenshotSaved", "path": path}, ensure_ascii=False))
            
            _notify_screenshot_taken()
            
            return path
