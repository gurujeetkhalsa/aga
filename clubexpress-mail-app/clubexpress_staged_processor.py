import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Protocol


DEFAULT_TOP = 25
ROW_RETURNING_PROCEDURES = {
    "rewards.sp_process_chapter_renewal_notices",
    "rewards.sp_record_chapter_renewal_confirmation",
}
ALLOWED_DOWNSTREAM_PROCEDURES = {
    "membership.sp_process_new_member_email",
    "membership.sp_process_membership_renewal",
    "membership.sp_process_journal_news_email",
    "rewards.sp_record_membership_event",
    "rewards.sp_process_chapter_renewal_notices",
    "rewards.sp_record_chapter_renewal_confirmation",
}


class ClubExpressStagedEventAdapter(Protocol):
    """Represent club express staged event adapter."""
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        """Query rows."""
        ...

    def execute_statements(self, statements: Iterable[tuple[str, tuple[Any, ...]]]) -> None:
        """Execute statements."""
        ...


@dataclass(frozen=True)
class DownstreamCall:
    """Represent downstream call."""
    name: str
    params: dict[str, Any]

    @property
    def returns_rows(self) -> bool:
        """Execute the returns rows routine."""
        return self.name in ROW_RETURNING_PROCEDURES


@dataclass(frozen=True)
class StagedClubExpressEvent:
    """Represent staged club express event."""
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
        """Execute the as summary dict routine."""
        return {
            "parsed_event_id": self.parsed_event_id,
            "message_id": self.message_id,
            "event_key": self.event_key,
            "message_type": self.message_type,
            "event_type": self.event_type,
            "received_at": json_safe_value(self.received_at),
            "event_date": json_safe_value(self.event_date),
            "agaid": self.agaid,
            "chapter_id": self.chapter_id,
            "parsed_item_count": self.parsed_item_count,
            "status": self.status,
            "attempt_count": self.attempt_count,
            "last_processed_at": json_safe_value(self.last_processed_at),
            "last_error_message": self.last_error_message,
            "downstream_procedures": [call.name for call in self.downstream_calls],
        }


@dataclass(frozen=True)
class ReplayResult:
    """Represent replay result data."""
    event_key: str
    executed: bool
    status_before: str
    status_after: str
    procedure_results: list[dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        """Execute the as dict routine."""
        return {
            "event_key": self.event_key,
            "executed": self.executed,
            "status_before": self.status_before,
            "status_after": self.status_after,
            "procedure_results": self.procedure_results,
        }


@dataclass(frozen=True)
class BatchProcessResult:
    """Represent batch process result data."""
    event_type: str
    executed: bool
    selected_count: int
    processed_count: int
    error_count: int
    results: list[dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        """Execute the as dict routine."""
        return {
            "event_type": self.event_type,
            "executed": self.executed,
            "selected_count": self.selected_count,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "results": self.results,
        }


EVENT_COLUMNS_SQL = """
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
"""


LIST_EVENTS_SQL = f"""
SELECT TOP (?)
{EVENT_COLUMNS_SQL}
FROM [membership].[clubexpress_parsed_events]
WHERE (? IS NULL OR [Status] = ?)
  AND (? IS NULL OR [Event_Type] = ?)
ORDER BY [Received_At] DESC, [Parsed_Event_ID] DESC
"""


LIST_PENDING_EVENTS_SQL = f"""
SELECT TOP (?)
{EVENT_COLUMNS_SQL}
FROM [membership].[clubexpress_parsed_events]
WHERE [Status] = N'staged'
  AND [Event_Type] = ?
ORDER BY [Received_At], [Parsed_Event_ID]
"""


GET_EVENT_BY_KEY_SQL = f"""
SELECT TOP (1)
{EVENT_COLUMNS_SQL}
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
    adapter: ClubExpressStagedEventAdapter,
    *,
    top: int = DEFAULT_TOP,
    status: str | None = None,
    event_type: str | None = None,
) -> list[StagedClubExpressEvent]:
    """List events."""
    rows = adapter.query_rows(LIST_EVENTS_SQL, (top, status, status, event_type, event_type))
    return [event_from_row(row) for row in rows]


def list_pending_events(
    adapter: ClubExpressStagedEventAdapter,
    *,
    event_type: str,
    top: int = DEFAULT_TOP,
) -> list[StagedClubExpressEvent]:
    """List pending events."""
    rows = adapter.query_rows(LIST_PENDING_EVENTS_SQL, (top, event_type))
    return [event_from_row(row) for row in rows]


def load_event(
    adapter: ClubExpressStagedEventAdapter,
    *,
    event_key: str | None = None,
    parsed_event_id: int | None = None,
) -> StagedClubExpressEvent:
    """Load event."""
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


def load_recent_attempts(
    adapter: ClubExpressStagedEventAdapter,
    event_key: str,
    *,
    top: int = 5,
) -> list[dict[str, Any]]:
    """Load recent attempts."""
    rows = adapter.query_rows(RECENT_ATTEMPTS_SQL, (top, event_key))
    attempts = []
    for row in rows:
        attempts.append(
            {
                "attempt_id": coerce_int(row.get("Attempt_ID")),
                "event_key": row.get("Event_Key"),
                "status": row.get("Status"),
                "error_message": row.get("Error_Message"),
                "result_payload": parse_optional_json(row.get("Result_Payload_Json")),
                "recorded_at": json_safe_value(row.get("Recorded_At")),
            }
        )
    return attempts


def event_from_row(row: dict[str, Any]) -> StagedClubExpressEvent:
    """Execute the event from row routine."""
    downstream_payload = parse_optional_json(row.get("Downstream_Payload_Json")) or {}
    return StagedClubExpressEvent(
        parsed_event_id=coerce_int(row.get("Parsed_Event_ID")),
        message_id=str(row.get("Message_ID") or ""),
        event_key=str(row.get("Event_Key") or ""),
        message_type=str(row.get("Message_Type") or ""),
        event_type=str(row.get("Event_Type") or ""),
        received_at=row.get("Received_At"),
        event_date=row.get("Event_Date"),
        agaid=coerce_optional_int(row.get("AGAID")),
        chapter_id=coerce_optional_int(row.get("ChapterID")),
        parsed_item_count=coerce_int(row.get("Parsed_Item_Count")),
        sender=row.get("Sender"),
        subject=row.get("Subject"),
        blob_path=row.get("Blob_Path"),
        status=str(row.get("Status") or ""),
        attempt_count=coerce_int(row.get("Attempt_Count")),
        last_processed_at=row.get("Last_Processed_At"),
        last_error_message=row.get("Last_Error_Message"),
        created_at=row.get("Created_At"),
        updated_at=row.get("Updated_At"),
        parsed_payload=parse_optional_json(row.get("Parsed_Payload_Json")),
        downstream_calls=parse_downstream_calls(downstream_payload),
    )


def parse_downstream_calls(payload: dict[str, Any]) -> list[DownstreamCall]:
    """Parse downstream calls."""
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
    adapter: ClubExpressStagedEventAdapter,
    event: StagedClubExpressEvent,
    *,
    execute: bool = False,
    confirm_replay: bool = False,
    allow_processed: bool = False,
    force_processing: bool = False,
) -> ReplayResult:
    """Execute the replay event routine."""
    if not event.downstream_calls:
        raise ValueError(f"Parsed event {event.event_key} has no downstream calls to replay.")
    if not execute:
        return ReplayResult(
            event_key=event.event_key,
            executed=False,
            status_before=event.status,
            status_after=event.status,
            procedure_results=[procedure_preview(call) for call in event.downstream_calls],
        )
    if not confirm_replay:
        raise ValueError("Executing a staged-event replay requires confirm_replay=True.")
    if event.status == "processed" and not allow_processed:
        raise ValueError("Refusing to replay a processed event without allow_processed=True.")
    if event.status == "processing" and not force_processing:
        raise ValueError("Refusing to replay an event marked processing without force_processing=True.")
    return process_staged_event(
        adapter,
        event,
        processor_name="manual_replay",
        executed_via_replay=True,
    )


def process_pending_events(
    adapter: ClubExpressStagedEventAdapter,
    *,
    event_type: str,
    top: int = DEFAULT_TOP,
    execute: bool = False,
    confirm_replay: bool = False,
    processor_name: str = "staged_event_processor",
) -> BatchProcessResult:
    """Process pending events."""
    events = list_pending_events(adapter, event_type=event_type, top=top)
    results: list[dict[str, Any]] = []
    processed_count = 0
    error_count = 0

    for event in events:
        if not execute:
            preview = replay_event(adapter, event)
            results.append(preview.as_dict())
            continue
        if not confirm_replay:
            raise ValueError("Executing staged-event processing requires confirm_replay=True.")
        try:
            result = process_staged_event(adapter, event, processor_name=processor_name)
            processed_count += 1
            results.append(result.as_dict())
        except Exception as exc:
            error_count += 1
            results.append(
                {
                    "event_key": event.event_key,
                    "executed": True,
                    "status_before": event.status,
                    "status_after": "error",
                    "error": str(exc),
                }
            )

    return BatchProcessResult(
        event_type=event_type,
        executed=execute,
        selected_count=len(events),
        processed_count=processed_count,
        error_count=error_count,
        results=results,
    )


def process_staged_event(
    adapter: ClubExpressStagedEventAdapter,
    event: StagedClubExpressEvent,
    *,
    processor_name: str,
    executed_via_replay: bool = False,
) -> ReplayResult:
    """Process staged event."""
    if event.status != "staged":
        raise ValueError(f"Refusing to process event {event.event_key} with status {event.status!r}.")
    mark_event_status(adapter, event.event_key, "processing")
    try:
        procedure_results = execute_downstream_calls(adapter, event.downstream_calls)
        result_payload = {
            "processor": processor_name,
            "replay": executed_via_replay,
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
    adapter: ClubExpressStagedEventAdapter,
    calls: list[DownstreamCall],
) -> list[dict[str, Any]]:
    """Execute downstream calls."""
    results: list[dict[str, Any]] = []
    pending_statements: list[tuple[str, tuple[Any, ...]]] = []

    def flush_pending() -> None:
        """Execute the flush pending routine."""
        if not pending_statements:
            return
        adapter.execute_statements(pending_statements)
        for query, params in pending_statements:
            results.append(
                {
                    "name": procedure_name_from_exec(query),
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
                    "rows": json_safe_value(rows),
                }
            )
        else:
            pending_statements.append(statement)

    flush_pending()
    return results


def stored_procedure_statement(call: DownstreamCall) -> tuple[str, tuple[Any, ...]]:
    """Execute the stored procedure statement routine."""
    if call.name not in ALLOWED_DOWNSTREAM_PROCEDURES:
        raise ValueError(f"Unsupported downstream procedure: {call.name!r}")
    ordered_items = list(call.params.items())
    proc_sql = stored_procedure_sql_name(call.name)
    if ordered_items:
        sql = f"EXEC {proc_sql} " + ", ".join(f"@{name} = ?" for name, _ in ordered_items)
    else:
        sql = f"EXEC {proc_sql}"
    return sql, tuple(value for _, value in ordered_items)


def mark_event_status(
    adapter: ClubExpressStagedEventAdapter,
    event_key: str,
    status: str,
    *,
    error_message: str | None = None,
    result_payload: dict[str, Any] | None = None,
) -> None:
    """Execute the mark event status routine."""
    adapter.execute_statements(
        [
            (
                UPDATE_STATUS_SQL,
                (
                    event_key,
                    status,
                    truncate_error(error_message),
                    json.dumps(json_safe_value(result_payload), sort_keys=True) if result_payload is not None else None,
                ),
            )
        ]
    )


def stored_procedure_sql_name(name: str) -> str:
    """Execute the stored procedure sql name routine."""
    schema, proc = name.split(".", 1)
    return f"[{schema}].[{proc}]"


def procedure_preview(call: DownstreamCall) -> dict[str, Any]:
    """Execute the procedure preview routine."""
    return {
        "name": call.name,
        "returns_rows": call.returns_rows,
        "parameter_count": len(call.params),
        "row_count": None,
    }


def procedure_name_from_exec(query: str) -> str:
    """Execute the procedure name from exec routine."""
    prefix = "EXEC "
    text = query.strip()
    if not text.upper().startswith(prefix):
        return text
    name = text[len(prefix):].split(None, 1)[0]
    return name.replace("[", "").replace("]", "")


def parse_optional_json(value: Any) -> Any:
    """Parse optional json."""
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return value
    return json.loads(str(value))


def coerce_int(value: Any) -> int:
    """Coerce int."""
    if value is None:
        return 0
    return int(value)


def coerce_optional_int(value: Any) -> int | None:
    """Coerce optional int."""
    if value is None:
        return None
    return int(value)


def truncate_error(error_message: str | None) -> str | None:
    """Execute the truncate error routine."""
    if error_message is None:
        return None
    return str(error_message)[:4000]


def json_safe_value(value: Any) -> Any:
    """Execute the json safe value routine."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe_value(item) for item in value]
    return value
