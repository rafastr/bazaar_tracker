from __future__ import annotations

try:
    from win10toast import ToastNotifier
    _toaster = ToastNotifier()
except Exception:
    _toaster = None


def notify_screenshot_taken() -> None:
    if _toaster is None:
        return

    try:
        _toaster.show_toast(
            "Bazaar Chronicle",
            "Screenshot of final board taken.",
            duration=4,
            threaded=True,
        )
    except Exception:
        pass
