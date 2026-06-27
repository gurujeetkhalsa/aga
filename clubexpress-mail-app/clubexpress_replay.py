import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, TextIO


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string
from clubexpress_staged_processor import (
    DEFAULT_TOP,
    DownstreamCall,
    ReplayResult,
    StagedClubExpressEvent,
    execute_downstream_calls,
    event_from_row,
    json_safe_value,
    list_events,
    load_event,
    load_recent_attempts,
    parse_downstream_calls,
    process_pending_events,
    replay_event,
    stored_procedure_statement,
)


NEW_MEMBERSHIP_EVENT_TYPE = "new_membership"
RENEWAL_EVENT_TYPE = "renewal"


def print_event_list(events: list[StagedClubExpressEvent], output: TextIO) -> None:
    if not events:
        print("(no staged ClubExpress events)", file=output)
        return
    columns = [
        ("parsed_event_id", "ID"),
        ("event_key", "Event Key"),
        ("event_type", "Type"),
        ("status", "Status"),
        ("received_at", "Received"),
        ("agaid", "AGAID"),
        ("chapter_id", "ChapterID"),
        ("parsed_item_count", "Items"),
        ("attempt_count", "Attempts"),
    ]
    rows = [event.as_summary_dict() for event in events]
    print(_format_table(rows, columns), file=output)


def print_event_preview(
    event: StagedClubExpressEvent,
    output: TextIO,
    *,
    attempts: list[dict[str, Any]] | None = None,
    include_payloads: bool = False,
) -> None:
    print("ClubExpress Parsed Event", file=output)
    print(f"  ID: {event.parsed_event_id}", file=output)
    print(f"  Event key: {event.event_key}", file=output)
    print(f"  Message ID: {event.message_id}", file=output)
    print(f"  Type: {event.event_type} ({event.message_type})", file=output)
    print(f"  Status: {event.status}; attempts: {event.attempt_count}", file=output)
    print(f"  Received: {_format_value(event.received_at)}", file=output)
    print(f"  Event date: {_format_value(event.event_date)}", file=output)
    if event.agaid is not None:
        print(f"  AGAID: {event.agaid}", file=output)
    if event.chapter_id is not None:
        print(f"  ChapterID: {event.chapter_id}", file=output)
    print(f"  Parsed item count: {event.parsed_item_count}", file=output)
    print("  Downstream calls:", file=output)
    if not event.downstream_calls:
        print("    (none)", file=output)
    for call in event.downstream_calls:
        marker = "returns rows" if call.returns_rows else "no row result"
        print(f"    - {call.name} ({marker}, {len(call.params)} params)", file=output)
    if attempts:
        print("  Recent attempts:", file=output)
        for attempt in attempts:
            print(
                f"    - {attempt.get('attempt_id')}: {attempt.get('status')} at {_format_value(attempt.get('recorded_at'))}",
                file=output,
            )
    if include_payloads:
        print("  Parsed payload:", file=output)
        print(_indent(json.dumps(json_safe_value(event.parsed_payload), indent=2, sort_keys=True), "    "), file=output)


def print_replay_result(result: ReplayResult, output: TextIO) -> None:
    label = "Replay executed" if result.executed else "Replay preview"
    print(label, file=output)
    print(f"  Event key: {result.event_key}", file=output)
    print(f"  Status: {result.status_before} -> {result.status_after}", file=output)
    print("  Procedures:", file=output)
    for row in result.procedure_results:
        row_count = row.get("row_count")
        suffix = f", rows={row_count}" if row.get("returns_rows") else ""
        print(f"    - {row.get('name')} ({row.get('parameter_count')} params{suffix})", file=output)


def print_batch_result(result, output: TextIO) -> None:
    label = "Staged event processing" if result.executed else "Staged event processing preview"
    print(label, file=output)
    print(f"  Event type: {result.event_type}", file=output)
    print(f"  Selected: {result.selected_count}", file=output)
    print(f"  Processed: {result.processed_count}", file=output)
    print(f"  Errors: {result.error_count}", file=output)
    for item in result.results:
        print(f"    - {item.get('event_key')}: {item.get('status_before')} -> {item.get('status_after')}", file=output)


def main(argv: list[str] | None = None, output: TextIO = sys.stdout) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    adapter = SqlAdapter(conn_str)
    try:
        if args.command == "list":
            events = list_events(adapter, top=args.top, status=args.status, event_type=args.event_type)
            if args.json:
                print(json.dumps([event.as_summary_dict() for event in events], indent=2, sort_keys=True), file=output)
            else:
                print_event_list(events, output)
            return 0

        if args.command == "process-new-members":
            result = process_pending_events(
                adapter,
                event_type=NEW_MEMBERSHIP_EVENT_TYPE,
                top=args.top,
                execute=args.execute,
                confirm_replay=args.confirm_replay,
                processor_name="manual_new_member_processor",
            )
            if args.json:
                print(json.dumps(result.as_dict(), indent=2, sort_keys=True), file=output)
            else:
                print_batch_result(result, output)
            return 0

        if args.command == "process-renewals":
            result = process_pending_events(
                adapter,
                event_type=RENEWAL_EVENT_TYPE,
                top=args.top,
                execute=args.execute,
                confirm_replay=args.confirm_replay,
                processor_name="manual_renewal_processor",
            )
            if args.json:
                print(json.dumps(result.as_dict(), indent=2, sort_keys=True), file=output)
            else:
                print_batch_result(result, output)
            return 0

        if args.command == "process-nightly-memchap":
            from function_app import _process_pending_memchap_events

            result = _process_pending_memchap_events(
                conn_str,
                top=args.top,
                execute=args.execute,
                confirm_replay=args.confirm_replay,
                processor_name="manual_memchap_processor",
            )
            if args.json:
                print(json.dumps(result.as_dict(), indent=2, sort_keys=True), file=output)
            else:
                print_batch_result(result, output)
            return 0

        if args.command == "process-chapterx":
            from function_app import _process_pending_chapter_events

            result = _process_pending_chapter_events(
                conn_str,
                top=args.top,
                execute=args.execute,
                confirm_replay=args.confirm_replay,
                processor_name="manual_chapter_processor",
            )
            if args.json:
                print(json.dumps(result.as_dict(), indent=2, sort_keys=True), file=output)
            else:
                print_batch_result(result, output)
            return 0

        event = load_event(adapter, event_key=args.event_key, parsed_event_id=args.id)
        if args.command == "preview":
            attempts = load_recent_attempts(adapter, event.event_key, top=args.attempts)
            if args.json:
                payload = event.as_summary_dict()
                payload["attempts"] = attempts
                if args.include_payloads:
                    payload["parsed_payload"] = json_safe_value(event.parsed_payload)
                print(json.dumps(payload, indent=2, sort_keys=True), file=output)
            else:
                print_event_preview(event, output, attempts=attempts, include_payloads=args.include_payloads)
            return 0

        result = replay_event(
            adapter,
            event,
            execute=args.execute,
            confirm_replay=args.confirm_replay,
            allow_processed=args.allow_processed,
            force_processing=args.force_processing,
        )
        if args.json:
            print(json.dumps(result.as_dict(), indent=2, sort_keys=True), file=output)
        else:
            print_replay_result(result, output)
        return 0
    except Exception as exc:
        print(f"ClubExpress staged-event tool failed: {exc}", file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preview or replay staged ClubExpress parsed events.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List recent staged ClubExpress parsed events.")
    list_parser.add_argument("--top", type=_positive_int, default=DEFAULT_TOP, help="Maximum number of rows to list.")
    list_parser.add_argument("--status", choices=["staged", "processing", "processed", "error", "skipped"], help="Filter by staged-event status.")
    list_parser.add_argument("--event-type", help="Filter by Event_Type, for example renewal or chapter_renewal_notice.")
    list_parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")

    preview_parser = subparsers.add_parser("preview", help="Preview one staged event and its downstream replay plan.")
    _add_event_selector_args(preview_parser)
    preview_parser.add_argument("--attempts", type=_positive_int, default=5, help="Number of recent attempt rows to include.")
    preview_parser.add_argument("--include-payloads", action="store_true", help="Include parsed payload JSON in the output.")
    preview_parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")

    replay_parser = subparsers.add_parser("replay", help="Replay one staged event's downstream procedures.")
    _add_event_selector_args(replay_parser)
    replay_parser.add_argument("--execute", action="store_true", help="Execute the replay. Omit to preview the replay plan.")
    replay_parser.add_argument("--confirm-replay", action="store_true", help="Required with --execute.")
    replay_parser.add_argument("--allow-processed", action="store_true", help="Allow replaying an event already marked processed.")
    replay_parser.add_argument("--force-processing", action="store_true", help="Allow replaying an event currently marked processing.")
    replay_parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")

    process_parser = subparsers.add_parser("process-new-members", help="Process pending staged new-member events.")
    process_parser.add_argument("--top", type=_positive_int, default=DEFAULT_TOP, help="Maximum staged events to process.")
    process_parser.add_argument("--execute", action="store_true", help="Execute processing. Omit to preview selected rows.")
    process_parser.add_argument("--confirm-replay", action="store_true", help="Required with --execute.")
    process_parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")

    renewal_process_parser = subparsers.add_parser("process-renewals", help="Process pending staged renewal events.")
    renewal_process_parser.add_argument("--top", type=_positive_int, default=DEFAULT_TOP, help="Maximum staged events to process.")
    renewal_process_parser.add_argument("--execute", action="store_true", help="Execute processing. Omit to preview selected rows.")
    renewal_process_parser.add_argument("--confirm-replay", action="store_true", help="Required with --execute.")
    renewal_process_parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")

    memchap_process_parser = subparsers.add_parser("process-nightly-memchap", help="Process pending staged nightly MemChap CSV events.")
    memchap_process_parser.add_argument("--top", type=_positive_int, default=5, help="Maximum staged events to process.")
    memchap_process_parser.add_argument("--execute", action="store_true", help="Execute processing. Omit to preview selected rows.")
    memchap_process_parser.add_argument("--confirm-replay", action="store_true", help="Required with --execute.")
    memchap_process_parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")

    chapter_process_parser = subparsers.add_parser("process-chapterx", help="Process pending staged ChapterX CSV events.")
    chapter_process_parser.add_argument("--top", type=_positive_int, default=5, help="Maximum staged events to process.")
    chapter_process_parser.add_argument("--execute", action="store_true", help="Execute processing. Omit to preview selected rows.")
    chapter_process_parser.add_argument("--confirm-replay", action="store_true", help="Required with --execute.")
    chapter_process_parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    return parser


def _add_event_selector_args(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--event-key", help="membership.clubexpress_parsed_events.Event_Key value.")
    group.add_argument("--id", type=int, help="membership.clubexpress_parsed_events.Parsed_Event_ID value.")


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _format_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    if not rows:
        return "(no rows)"
    rendered = [[_format_value(row.get(key)) for key, _ in columns] for row in rows]
    widths = [
        max(len(heading), *(len(row[index]) for row in rendered))
        for index, (_, heading) in enumerate(columns)
    ]
    lines = [
        "  ".join(heading.ljust(widths[index]) for index, (_, heading) in enumerate(columns)).rstrip(),
        "  ".join("-" * width for width in widths).rstrip(),
    ]
    for row in rendered:
        lines.append("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)).rstrip())
    return "\n".join(lines)


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _indent(text: str, prefix: str) -> str:
    return "\n".join(prefix + line if line else prefix.rstrip() for line in text.splitlines())


if __name__ == "__main__":
    raise SystemExit(main())
