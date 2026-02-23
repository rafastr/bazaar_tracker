import argparse
import time

from core.config import settings
from core.tailer import follow_file_lines, replay_file_lines
from core.parser import LogParser
from core.state import RunState
from core.sinks import StdoutSink, ScreenshotSink
from core.instance_store import InstanceStore
from core.run_history_db import RunHistoryDb
from core.run_history_sink import RunHistorySink


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bazaar Tracker (log-based)")
    p.add_argument(
        "--log",
        dest="log_path",
        default=settings.log_path,
        help="Path to the Unity Player.log (or a local test log)",
    )
    p.add_argument(
        "--replay",
        action="store_true",
        help="Replay the entire log file from start to end (no polling). Useful for testing.",
    )
    p.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON events",
    )
    p.add_argument(
        "--no-screenshots",
        action="store_true",
        help="Disable screenshots (recommended on Linux)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    log_path = args.log_path
    pretty = args.pretty or settings.pretty_json
    screenshots_enabled = settings.enable_screenshots and (not args.no_screenshots)

    print("Bazaar Tracker")
    print("Watching:", log_path)
    print("Mode:", "replay" if args.replay else "follow")
    print("Instance cache:", settings.instance_map_path)

    # JSON cache (instance_id -> template_id)
    store = InstanceStore(settings.instance_map_path)

    run_db = RunHistoryDb(settings.run_history_db_path)

    parser = LogParser()
    state = RunState(store=store)

    sinks = [
        StdoutSink(pretty=pretty),
        ScreenshotSink(
            enabled=screenshots_enabled,
            out_dir=settings.screenshot_dir,
            monitor_index=settings.screenshot_monitor_index,
            delay_seconds=settings.screenshot_delay_seconds,
            cooldown_seconds=settings.screenshot_cooldown_seconds,
            trigger_event_types=set(settings.screenshot_trigger_event_types or []),
        ),
        RunHistorySink(run_db),
    ]

    try:
        line_source = (
            replay_file_lines(log_path, encoding=settings.log_encoding, errors=settings.log_encoding_errors)
            if args.replay
            else follow_file_lines(
                log_path,
                poll_interval_seconds=settings.poll_interval_seconds,
                encoding=settings.log_encoding,
                errors=settings.log_encoding_errors,
            )
        )
    
        for line in line_source:
            ev = parser.parse_line(line)
            if ev is None:
                continue
    
            for out_ev in state.handle(ev):
                for sink in sinks:
                    sink.handle(out_ev)
    
            if settings.loop_sleep_seconds > 0 and not args.replay:
                time.sleep(settings.loop_sleep_seconds)

    finally:
        run_db.close()

if __name__ == "__main__":
    main()
