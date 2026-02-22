import os
import time
from mss import mss
from PIL import Image

LOG_PATH = os.path.join(
    os.environ["USERPROFILE"],
    r"AppData\LocalLow\Tempo Storm\The Bazaar\Player.log"
)

# consts
TRIGGER_STRING = "Starting card reveal sequence"
SCREENSHOT_DIR = "screenshots"
TRIGGER_DELAY_SECONDS = 1.5
TRIGGER_COOLDOWN_SECONDS = 10.0
POLL_INTERVAL_SECONDS = 0.5

os.makedirs(SCREENSHOT_DIR, exist_ok=True)

def take_screenshot():
    with mss() as sct:
        monitor = sct.monitors[1]
        shot = sct.grab(monitor)
        img = Image.frombytes("RGB", shot.size, shot.rgb)
        path = os.path.join(SCREENSHOT_DIR, f"run_{int(time.time())}.png")
        img.save(path)
        print(f"Screenshot saved: {path}")

def main():
    last_pos = 0
    last_trigger = 0.0

    print("Watching:", LOG_PATH)

    while True:
        try:
            if not os.path.exists(LOG_PATH):
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            size = os.path.getsize(LOG_PATH)
            if size < last_pos:
                # log truncated (game restart)
                last_pos = 0

            with open(LOG_PATH, "r", encoding="utf-8", errors="ignore") as f:
                f.seek(last_pos)
                new_data = f.read()
                last_pos = f.tell()

            if new_data and (TRIGGER_STRING in new_data):
                now = time.time()
                if now - last_trigger >= TRIGGER_COOLDOWN_SECONDS:
                    last_trigger = now
                    print("Trigger detected! Waiting...")
                    time.sleep(TRIGGER_DELAY_SECONDS)
                    take_screenshot()

        except Exception as e:
            print("Error:", repr(e))

        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()

