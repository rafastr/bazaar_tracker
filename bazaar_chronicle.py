import threading
import time
import webbrowser
import urllib.request

from main import run_tracker_watch_mode
from web.app import run_web_app

APP_URL = "http://127.0.0.1:5000"


def _open_browser() -> None:
    try:
        webbrowser.open(APP_URL)
    except Exception:
        pass


def wait_for_server_and_open_browser():
    for _ in range(40):  # wait up to ~20 seconds
        try:
            urllib.request.urlopen(APP_URL)
            webbrowser.open(APP_URL)
            return
        except Exception:
            time.sleep(0.5)

    # fallback if server never responded
    webbrowser.open(APP_URL)


def _start_tracker_thread() -> threading.Thread:
    t = threading.Thread(
        target=run_tracker_watch_mode,
        kwargs={
            "pretty": False,
            "screenshots_enabled": True,
        },
        daemon=True,
    )
    t.start()
    return t


if __name__ == "__main__":
    _start_tracker_thread()

    threading.Thread(
        target=wait_for_server_and_open_browser,
        daemon=True
    ).start()

    run_web_app()
