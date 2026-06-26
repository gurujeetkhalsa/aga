import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Protocol, TextIO


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string


DEFAULT_TOP = 25
ROW_RETURNING_PROCEDURES = {
    "rewards.sp_process_chapter_renewal_notices",
    "rewards.sp_record_chapter_renewal_confirmation",
}
ALLOWED_DOWNSTREAM_PROCEDURES = {
    "membership.sp_process_new_member_email",
    "membership.sp_process_membership_renewal",
    "rewards.sp_record_membership_event",
    "rewards.sp_process_chapter_renewal_notices",
    "rewards.sp_record_chapter_renewal_confirmation",
}


class ClubExpressReplayAdapter(Protocol):
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        ...

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        ...


@dataclass(frozen=True)
class DownstreamCall:
    name: str
    params: dict[str, Any]

    @property
    def returns_rows(self) -> bool:
        return self.name in ROW_RETURNING_PROCEDURES


@dataclass(frozen=True)
class StagedClubExpressEvent:
    parsed_event_id: int
    message_id: str
    event_key: str
    message_type: str
    event_type: str
    received_at: Any
    event_date: Any
    agaid: int | None
    chapter_id: int | None
    parsed_item_count: int
    sender: str | None
    subject: str | None
    blob_path: str | None
    status: str
    attempt_count: int
    last_processed_at: Any
    last_error_message: str | None
    created_at: Any
    updated_at: Any
    parsed_payload: dict[str, Any] | list[Any] | None
    downstream_calls: list[DownstreamCall]

    def as_summary_dict(self) -> dict[str, Any]:
        return {
            "parsed_event_id": self.parsed_event_id,
            "message_id": self.message_id,
            "event_key": self.event_key,
            "message_type": self.message_type,
            "event_type": self.event_type,
            "received_at": _json_safe_value(self.received_at),
            "event_date": _json_safe_value(self.event_date),
            "agaid": self.agaid,
            "chapter_id": self.chapter_id,
            "parsed_item_count": self.parsed_item_count,
            "status": self.status,
            "attempt_count": self.attempt_count,
            "last_processed_at": _json_safe_value(self.last_processed_at),
            "last_error_message": self.last_error_message,
            "downstream_procedures": [call.name for call in self.downstream_calls],
        }


@dataclass(frozen=True)
class ReplayResult:
    event_key: str
    executed: bool
    status_before: str
    status_after: str
    procedure_results: list[dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_key": self.event_key,
            "executed": self.executed,
            "status_before": self.status_before,
            "status_after": self.status_after,
            "procedure_results": self.procedure_results,
        }


LIST_EVENTS_SQL = """
SELECT TOP (?)
    [Parsed_Event_ID],
    [Message_ID],
    [Event_Key],
    [Message_Type],
    [Event_Type],
    [Received_At],
    [Event_Date],
    [AGAID],
    [ChapterID],
    [Parsed_Item_Count],
    [Sender],
    [Subject],
    [Blob_Path],
    [Parsed_Payload_Json],
    [Downstream_Payload_Json],
    [Status],
    [Attempt_Count],
    [Last_Processed_At],
    [Last_Error_Message],
    [Created_At],
    [Updated_At]
FROM [membership].[clubexpress_parsed_events]
WHERE (? IS NULL OR [Status] = ?)
  AND (? IS NULL OR [Event_Type] = ?)
ORDER BY [Received_At] DESC, [Parsed_Event_ID] DESC
"""


GET_EVENT_BY_KEY_SQL = """
SELECT TOP (1)
    [Parsed_Event_ID],
    [Message_ID],
    [Event_Key],
    [Message_Type],
    [Event_Type],
    [Received_At],
    [Event_Date],
    [AGAID],
    [ChapterID],
    [Parsed_Item_Count],
    [Sender],
    [Subject],
    [Blob_Path],
    [Parsed_Payload_Json],
    [Downstream_Payload_Json],
    [Status],
    [Attempt_Count],
    [Last_Processed_At],
    [Last_Error_Message],
    [Created_At],
    [Updated_At]
FROM [membership].[clubexpress_parsed_events]
WHERE [Event_Key] = ?
"""


GET_EVENT_BY_ID_SQL = GET_EVENT_BY_KEY_SQL.replace("WHERE [Event_Key] = ?", "WHERE [Parsed_Event_ID] = ?")


RECENT_ATTEMPTS_SQL = """
SELECT TOP (?)
    [Attempt_ID],
    [Event_Key],
    [Status],
    [Error_Message],
    [Result_Payload_Json],
    [Recorded_At]
FROM [membership].[clubexpress_parsed_event_attempts]
WHERE [Event_Key] = ?
ORDER BY [Attempt_ID] DESC
"""


UPDATE_STATUS_SQL = """
EXEC [membership].[sp_update_clubexpress_parsed_event_status]
    @EventKey = ?,
    @Status = ?,
    @ErrorMessage = ?,
    @ResultPayloadJson = ?
"""


def list_events(
    adapter: ClubExpressReplayAdapter,
    *,
    top: int = DEFAULT_TOP,
    status: str | None = None,
    event_type: str | None = None,
) -> list[StagedClubExpressEvent]:
    rows = adapter.query_rows(LIST_EVENTS_SQL, (top, status, status, event_type, event_type))
    return [event_from_row(row) for row in rows]


def load_event(
    adapter: ClubExpressReplayAdapter,
    *,
    event_key: str | None = None,
    parsed_event_id: int | None = None,
) -> StagedClubExpressEvent:
    if bool(event_key) == bool(parsed_event_id):
        raise ValueError("Pass exactly one of event_key or parsed_event_id.")
    if event_key:
        rows = adapter.query_rows(GET_EVENT_BY_KEY_SQL, (event_key,))
    else:
        rows = adapter.query_rows(GET_EVENT_BY_ID_SQL, (parsed_event_id,))
    if not rows:
        label = event_key if event_key else parsed_event_id
        raise ValueError(f"ClubExpress parsed event was not found: {label}")
    return event_from_row(rows[0])


def load_recent_attempts(adapter: ClubExpressReplayAdapter, event_key: str, *, top: int = 5) -> list[dict[str, Any]]:
    rows = adapter.query_rows(RECENT_ATTEMPTS_SQL, (top, event_key))
    attempts = []
    for row in rows:
        attempts.append(
            {
                "attempt_id": _coerce_int(row.get("Attempt_ID")),
                "event_key": row.get("Event_Key"),
                "status": row.get("Status"),
                "error_message": row.get("Error_Message"),
                "result_payload": _parse_optional_json(row.get("Result_Payload_Json")),
                "recorded_at": _json_safe_value(row.get("Recorded_At")),
            }
        )
    return attempts


def event_from_row(row: dict[str, Any]) -> StagedClubExpressEvent:
    downstream_payload = _parse_optional_json(row.get("Downstream_Payload_Json")) or {}
    return StagedClubExpressEvent(
        parsed_event_id=_coerce_int(row.get("Parsed_Event_ID")),
        message_id=str(row.get("Message_ID") or ""),
        event_key=str(row.get("Event_Key") or ""),
        message_type=str(row.get("Message_Type") or ""),
        event_type=str(row.get("Event_Type") or ""),
        received_at=row.get("Received_At"),
        event_date=row.get("Event_Date"),
        agaid=_coerce_optional_int(row.get("AGAID")),
        chapter_id=_coerce_optional_int(row.get("ChapterID")),
        parsed_item_count=_coerce_int(row.get("Parsed_Item_Count")),
        sender=row.get("Sender"),
        subject=row.get("Subject"),
        blob_path=row.get("Blob_Path"),
        status=str(row.get("Status") or ""),
        attempt_count=_coerce_int(row.get("Attempt_Count")),
        last_processed_at=row.get("Last_Processed_At"),
        last_error_message=row.get("Last_Error_Message"),
        created_at=row.get("Created_At"),
        updated_at=row.get("Updated_At"),
        parsed_payload=_parse_optional_json(row.get("Parsed_Payload_Json")),
        downstream_calls=parse_downstream_calls(downstream_payload),
    )


def parse_downstream_calls(payload: dict[str, Any]) -> list[DownstreamCall]:
    calls = []
    for item in payload.get("procedures") or []:
        if not isinstance(item, dict):
            raise ValueError("Downstream procedure entry must be an object.")
        name = str(item.get("name") or "").strip()
        if name not in ALLOWED_DOWNSTREAM_PROCEDURES:
            raise ValueError(f"Unsupported downstream procedure in staged event: {name!r}")
        params = item.get("params") or {}
        if not isinstance(params, dict):
            raise ValueError(f"Downstream params for {name} must be an object.")
        calls.append(DownstreamCall(name=name, params=params))
    return calls


def replay_event(
    adapter: ClubExpressReplayAdapter,
    event: StagedClubExpressEvent,
    *,
    execute: bool = False,
    confirm_replay: bool = False,
    allow_processed: bool = False,
    force_processing: bool = False,
) -> ReplayResult:
    if not event.downstream_calls:
        raise ValueError(f"Parsed event {event.event_key} has no downstream calls to replay.")
    if not execute:
        return ReplayResult(
            event_key=event.event_key,
            executed=False,
            status_before=event.status,
            status_after=event.status,
            procedure_results=[_procedure_preview(call) for call in event.downstream_calls],
        )
    if not confirm_replay:
        raise ValueError("Executing a staged-event replay requires confirm_replay=True.")
    if event.status == "processed" and not allow_processed:
        raise ValueError("Refusing to replay a processed event without allow_processed=True.")
    if event.status == "processing" and not force_processing:
        raise ValueError("Refusing to replay an event marked processing without force_processing=True.")

    mark_event_status(adapter, event.event_key, "processing")
    try:
        procedure_results = execute_downstream_calls(adapter, event.downstream_calls)
        result_payload = {
            "replay": True,
            "procedure_results": procedure_results,
        }
        mark_event_status(adapter, event.event_key, "processed", result_payload=result_payload)
        return ReplayResult(
            event_key=event.event_key,
            executed=True,
            status_before=event.status,
            status_after="processed",
            procedure_results=procedure_results,
        )
    except Exception as exc:
        mark_event_status(adapter, event.event_key, "error", error_message=str(exc))
        raise


def execute_downstream_calls(
    adapter: ClubExpressReplayAdapter,
    calls: list[DownstreamCall],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    pending_statements: list[SqlStatement] = []

    def flush_pending() -> None:
        if not pending_statements:
            return
        adapter.execute_statements(pending_statements)
        for query, params in pending_statements:
            results.append(
                {
                    "name": _procedure_name_from_exec(query),
                    "returns_rows": False,
                    "parameter_count": len(params),
                    "row_count": 0,
                }
            )
        pending_statements.clear()

    for call in calls:
        statement = stored_procedure_statement(call)
        if call.returns_rows:
            flush_pending()
            rows = adapter.query_rows(statement[0], statement[1])
            results.append(
                {
                    "name": call.name,
                    "returns_rows": True,
                    "parameter_count": len(statement[1]),
                    "row_count": len(rows),
                    "rows": _json_safe_value(rows),
                }
            )
        else:
            pending_statements.append(statement)

    flush_pending()
    return results


def stored_procedure_statement(call: DownstreamCall) -> SqlStatement:
    if call.name not in ALLOWED_DOWNSTREAM_PROCEDURES:
        raise ValueError(f"Unsupported downstream procedure: {call.name!r}")
    ordered_items = list(call.params.items())
    proc_sql = _stored_procedure_sql_name(call.name)
    if ordered_items:
        sql = f"EXEC {proc_sql} " + ", ".join(f"@{name} = ?" for name, _ in ordered_items)
    else:
        sql = f"EXEC {proc_sql}"
    return sql, tuple(value for _, value in ordered_items)


def mark_event_status(
    adapter: ClubExpressReplayAdapter,
    event_key: str,
    status: str,
    *,
    error_message: str | None = None,
    result_payload: dict[str, Any] | None = None,
) -> None:
    adapter.execute_statements(
        [
            (
                UPDATE_STATUS_SQL,
                (
                    event_key,
                    status,
                    _truncate_error(error_message),
                    json.dumps(_json_safe_value(result_payload), sort_keys=True) if result_payload is not None else None,
                ),
            )
        ]
    )


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
        print(_indent(json.dumps(_json_safe_value(event.parsed_payload), indent=2, sort_keys=True), "    "), file=output)


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

        event = load_event(adapter, event_key=args.event_key, parsed_event_id=args.id)
        if args.command == "preview":
            attempts = load_recent_attempts(adapter, event.event_key, top=args.attempts)
            if args.json:
                payload = event.as_summary_dict()
                payload["attempts"] = attempts
                if args.include_payloads:
                    payload["parsed_payload"] = _json_safe_value(event.parsed_payload)
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
    return parser


def _add_event_selector_args(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--event-key", help="membership.clubexpress_parsed_events.Event_Key value.")
    group.add_argument("--id", type=int, help="membership.clubexpress_parsed_events.Parsed_Event_ID value.")


def _stored_procedure_sql_name(name: str) -> str:
    schema, proc = name.split(".", 1)
    return f"[{schema}].[{proc}]"


def _procedure_preview(call: DownstreamCall) -> dict[str, Any]:
    return {
        "name": call.name,
        "returns_rows": call.returns_rows,
        "parameter_count": len(call.params),
        "row_count": None,
    }


def _procedure_name_from_exec(query: str) -> str:
    prefix = "EXEC "
    text = query.strip()
    if not text.upper().startswith(prefix):
        return text
    name = text[len(prefix):].split(None, 1)[0]
    return name.replace("[", "").replace("]", "")


def _parse_optional_json(value: Any) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return value
    return json.loads(str(value))


def _coerce_int(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _truncate_error(error_message: str | None) -> str | None:
    if error_message is None:
        return None
    return str(error_message)[:4000]


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


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_value(item) for item in value]
    return value


def _indent(text: str, prefix: str) -> str:
    return "\n".join(prefix + line if line else prefix.rstrip() for line in text.splitlines())


if __name__ == "__main__":
    raise SystemExit(main())
