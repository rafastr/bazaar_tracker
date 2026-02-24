import argparse
import time
import datetime

from core.config import settings
from core.tailer import follow_file_lines, replay_file_lines
from core.parser import LogParser
from core.state import RunState
from core.sinks import StdoutSink, ScreenshotSink
from core.instance_store import InstanceStore
from core.run_history_db import RunHistoryDb
from core.run_history_sink import RunHistorySink
from core.run_meta_store import RunMetaStore
from core.run_viewer import list_runs, get_run_board, get_last_run_id


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
    p.add_argument(
        "--list-runs",
        action="store_true",
        help="List recent stored runs"
    )
    p.add_argument(
        "--show-run",
        type=int,
        help="Show a stored run by run_id (resolved with templates DB)"
    )
    p.add_argument(
        "--last-run",
        action="store_true",
        help="Show the most recent stored run",
    )
    return p.parse_args()


def print_run(run: dict) -> None:
    ts = datetime.datetime.fromtimestamp(run["ended_at_unix"])
    print(f'Run {run["run_id"]} ended_at={ts}')

    hero = run.get("hero") or "(unknown)"
    rank = run.get("rank")
    rank_s = str(rank) if rank is not None else "(unknown)"

    print(f"Hero: {hero}")
    print(f"Rank: {rank_s}")
    print(f'Screenshot: {run["screenshot_path"]}')
    print("Board:")

    for it in run["items"]:
        sock = it["socket_number"]
        size = it["size"]
        name = it["name"] or "(unknown template)"
        tid = it["template_id"] or "NULL"
        print(f"  Socket {sock}: {name} | {size} | {tid}")


def main() -> None:
    args = parse_args()

    if args.list_runs:
        rows = list_runs(settings.run_history_db_path, limit=50)
        for r in rows:
            ts = datetime.datetime.fromtimestamp(r["ended_at_unix"])
            print(f'run_id={r["run_id"]} ended_at={ts} screenshot={r["screenshot_path"]}')
        return
    
    if args.show_run is not None:
        run = get_run_board(settings.run_history_db_path, settings.templates_db_path, args.show_run)
        print_run(run)
        return

    if args.last_run:
        run_id = get_last_run_id(settings.run_history_db_path)
        if run_id is None:
            print("No runs stored yet.")
            return
    
        run = get_run_board(
            settings.run_history_db_path,
            settings.templates_db_path,
            run_id,
        )
        print_run(run)
        return

    # Normal watch mode
    log_path = args.log_path
    pretty = args.pretty or settings.pretty_json
    screenshots_enabled = settings.enable_screenshots and (not args.no_screenshots)

    print("Bazaar Tracker")
    print("Watching:", log_path)
    print("Mode:", "replay" if args.replay else "follow")
    print("Instance cache:", settings.instance_map_path)
    print("Run history DB:", settings.run_history_db_path)

    # JSON cache (instance_id -> template_id) and hero being played
    store = InstanceStore(settings.instance_map_path)
    meta_store = RunMetaStore(settings.run_meta_path)
    run_db = RunHistoryDb(settings.run_history_db_path)

    parser = LogParser()
    state = RunState(store=store, meta_store=meta_store)

    screenshot_sink = ScreenshotSink(
        enabled=screenshots_enabled,
        out_dir=settings.screenshot_dir,
        monitor_index=settings.screenshot_monitor_index,
        delay_seconds=settings.screenshot_delay_seconds,
        cooldown_seconds=settings.screenshot_cooldown_seconds,
        trigger_event_types=set(settings.screenshot_trigger_event_types or []),
    )

    sinks = [
        StdoutSink(pretty=pretty),
        screenshot_sink,
        RunHistorySink(run_db),
    ]

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
    
    try:
        for line in line_source:
            ev = parser.parse_line(line)
            if ev is None:
                continue

            # Special ordering for RunEnd:
            # take screenshot first -> feed ScreenshotSaved into state -> finalize run
            if ev.type == "RunEnd":
                emitted = screenshot_sink.handle(ev)
                for e2 in emitted:
                    for out2 in state.handle(e2):
                        for sink in sinks:
                            sink.handle(out2)

            # Normal pipeline of events
            for out_ev in state.handle(ev):
                for sink in sinks:
                    sink.handle(out_ev)
    
            if settings.loop_sleep_seconds > 0 and not args.replay:
                time.sleep(settings.loop_sleep_seconds)

    finally:
        run_db.close()

if __name__ == "__main__":
    main()
