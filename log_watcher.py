import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from mss import mss
from PIL import Image

LOG_PATH = r"C:\Users\rafa\AppData\LocalLow\Tempo Storm\The Bazaar\Player.log"
TRIGGER_STRING = "[EndOfRunScreenController]"
SCREENSHOT_DIR = "screenshots"

os.makedirs(SCREENSHOT_DIR, exist_ok=True)


class LogHandler(FileSystemEventHandler):
    def __init__(self):
        self.last_position = 0

    def on_modified(self, event):
        if event.src_path != LOG_PATH:
            return

        with open(LOG_PATH, "r", encoding="utf-8", errors="ignore") as f:
            f.seek(self.last_position)
            new_data = f.read()
            self.last_position = f.tell()

        if TRIGGER_STRING in new_data:
            print("Run ended detected!")
            time.sleep(3)
            take_screenshot()


def take_screenshot():
    with mss() as sct:
        monitor = sct.monitors[1]  # main monitor
        screenshot = sct.grab(monitor)
        img = Image.frombytes("RGB", screenshot.size, screenshot.rgb)

        timestamp = int(time.time())
        path = os.path.join(SCREENSHOT_DIR, f"run_{timestamp}.png")
        img.save(path)

        print(f"Screenshot saved: {path}")


if __name__ == "__main__":
    event_handler = LogHandler()
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(LOG_PATH), recursive=False)
    observer.start()

    print("Watching log file...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
