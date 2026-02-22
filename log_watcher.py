import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from mss import mss
from PIL import Image

LOG_PATH = r"C:\Users\YOUR_USER\AppData\LocalLow\Tempo Storm\The Bazaar\Player.log"
TRIGGER_STRING = "Starting card reveal sequence"
SCREENSHOT_DIR = "screenshots"

# Tuning knobs
TRIGGER_DELAY_SECONDS = 1.5     # wait after trigger before screenshot
TRIGGER_COOLDOWN_SECONDS = 10.0 # ignore repeated triggers within this time window

os.makedirs(SCREENSHOT_DIR, exist_ok=True)


def take_screenshot():
    with mss() as sct:
        monitor = sct.monitors[1]  # primary monitor
        shot = sct.grab(monitor)
        img = Image.frombytes("RGB", shot.size, shot.rgb)
        timestamp = int(time.time())
        path = os.path.join(SCREENSHOT_DIR, f"run_{timestamp}.png")
        img.save(path)
        print(f"Screenshot saved: {path}")


class LogHandler(FileSystemEventHandler):
    def __init__(self, log_path: str):
        self.log_path = os.path.normpath(log_path)
        self.last_position = 0
        self.last_inode = None
        self.last_trigger_time = 0.0

    def _safe_stat(self):
        try:
            return os.stat(self.log_path)
        except FileNotFoundError:
            return None

    def _read_new(self) -> str:
        st = self._safe_stat()
        if st is None:
            return ""

        inode = getattr(st, "st_ino", None)
        if self.last_inode is None:
            self.last_inode = inode
        elif inode is not None and inode != self.last_inode:
            # file replaced at same path
            self.last_inode = inode
            self.last_position = 0

        size = st.st_size
        if size < self.last_position:
            # file truncated (common on restart)
            self.last_position = 0

        with open(self.log_path, "r", encoding="utf-8", errors="ignore") as f:
            f.seek(self.last_position)
            new_data = f.read()
            self.last_position = f.tell()

        return new_data

    def _cooldown_ok(self) -> bool:
        now = time.time()
        return (now - self.last_trigger_time) >= TRIGGER_COOLDOWN_SECONDS

    def _maybe_trigger(self, new_data: str):
        if TRIGGER_STRING not in new_data:
            return

        if not self._cooldown_ok():
            print("Trigger detected but ignored (cooldown).")
            return

        self.last_trigger_time = time.time()
        print("Run ended detected! Waiting before screenshot...")
        time.sleep(TRIGGER_DELAY_SECONDS)
        take_screenshot()

    def on_modified(self, event):
        if os.path.normpath(event.src_path) != self.log_path:
            return
        new_data = self._read_new()
        if new_data:
            self._maybe_trigger(new_data)

    def on_created(self, event):
        if os.path.normpath(event.src_path) != self.log_path:
            return
        self.last_position = 0
        new_data = self._read_new()
        if new_data:
            self._maybe_trigger(new_data)

    def on_moved(self, event):
        # sometimes log rotation/recreation triggers a move event
        dest = getattr(event, "dest_path", None)
        if dest and os.path.normpath(dest) == self.log_path:
            self.last_position = 0
            new_data = self._read_new()
            if new_data:
                self._maybe_trigger(new_data)


if __name__ == "__main__":
    log_dir = os.path.dirname(LOG_PATH)
    handler = LogHandler(LOG_PATH)

    observer = Observer()
    observer.schedule(handler, path=log_dir, recursive=False)
    observer.start()

    print("Watching log file...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
