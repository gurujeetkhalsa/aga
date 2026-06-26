import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass(frozen=True)
class DownstreamProcedure:
    name: str
    params: dict


@dataclass(frozen=True)
class ClubExpressParsedEvent:
    message_id: str
    event_key: str
    message_type: str
    event_type: str
    received_at: datetime
    event_date: Optional[date]
    agaid: Optional[int]
    chapter_id: Optional[int]
    parsed_item_count: int
    sender: Optional[str]
    subject: Optional[str]
    blob_path: Optional[str]
    parsed_payload_json: str
    downstream_payload_json: Optional[str]

    def record_params(self) -> dict:
        return {
            "MessageId": self.message_id,
            "EventKey": self.event_key,
            "MessageType": self.message_type,
            "EventType": self.event_type,
            "ReceivedAt": self.received_at,
            "EventDate": self.event_date,
            "AGAID": self.agaid,
            "ChapterID": self.chapter_id,
            "ParsedItemCount": self.parsed_item_count,
            "Sender": self.sender,
            "Subject": self.subject,
            "BlobPath": self.blob_path,
            "ParsedPayloadJson": self.parsed_payload_json,
            "DownstreamPayloadJson": self.downstream_payload_json,
        }


def parsed_event_key(message_id: str, event_type: str, entity_id: Optional[int] = None) -> str:
    parts = [str(message_id), str(event_type)]
    if entity_id is not None:
        parts.append(str(entity_id))
    return ":".join(parts)


def build_membership_parsed_event(
    *,
    message_id: str,
    message_type: str,
    event_type: str,
    received_at: datetime,
    event_date: date,
    parsed: dict,
    downstream_procedures: list[DownstreamProcedure],
    sender: Optional[str] = None,
    subject: Optional[str] = None,
    blob_path: Optional[str] = None,
) -> ClubExpressParsedEvent:
    agaid = int(parsed["AGAID"])
    parsed_payload = _source_payload(
        message_id=message_id,
        sender=sender,
        subject=subject,
        blob_path=blob_path,
        parsed=parsed,
    )
    return ClubExpressParsedEvent(
        message_id=message_id,
        event_key=parsed_event_key(message_id, event_type, agaid),
        message_type=message_type,
        event_type=event_type,
        received_at=received_at,
        event_date=event_date,
        agaid=agaid,
        chapter_id=agaid if parsed.get("IsChapterMember") else None,
        parsed_item_count=1,
        sender=sender,
        subject=subject,
        blob_path=blob_path,
        parsed_payload_json=_json_dumps(parsed_payload),
        downstream_payload_json=_downstream_payload_json(downstream_procedures),
    )


def build_chapter_renewal_notice_parsed_event(
    *,
    message_id: str,
    message_type: str,
    event_type: str,
    received_at: datetime,
    notice_date: date,
    parsed_rows: list[dict],
    downstream_procedures: list[DownstreamProcedure],
    sender: Optional[str] = None,
    subject: Optional[str] = None,
    blob_path: Optional[str] = None,
) -> ClubExpressParsedEvent:
    parsed_payload = _source_payload(
        message_id=message_id,
        sender=sender,
        subject=subject,
        blob_path=blob_path,
        parsed={"rows": parsed_rows, "row_count": len(parsed_rows)},
    )
    return ClubExpressParsedEvent(
        message_id=message_id,
        event_key=parsed_event_key(message_id, event_type),
        message_type=message_type,
        event_type=event_type,
        received_at=received_at,
        event_date=notice_date,
        agaid=None,
        chapter_id=None,
        parsed_item_count=len(parsed_rows),
        sender=sender,
        subject=subject,
        blob_path=blob_path,
        parsed_payload_json=_json_dumps(parsed_payload),
        downstream_payload_json=_downstream_payload_json(downstream_procedures),
    )


def status_params(
    event_key: str,
    status: str,
    *,
    error_message: Optional[str] = None,
    result_payload: Optional[dict] = None,
) -> dict:
    return {
        "EventKey": event_key,
        "Status": status,
        "ErrorMessage": _truncate_error(error_message),
        "ResultPayloadJson": _json_dumps(result_payload) if result_payload is not None else None,
    }


def result_payload_for_procedures(
    procedures: list[DownstreamProcedure],
    *,
    extra: Optional[dict] = None,
) -> dict:
    payload = {
        "procedures": [{"name": procedure.name} for procedure in procedures],
    }
    if extra:
        payload.update(extra)
    return payload


def _source_payload(
    *,
    message_id: str,
    sender: Optional[str],
    subject: Optional[str],
    blob_path: Optional[str],
    parsed: dict,
) -> dict:
    return {
        "message_id": message_id,
        "sender": sender,
        "subject": subject,
        "blob_path": blob_path,
        "parsed": _json_safe_value(parsed),
    }


def _downstream_payload_json(procedures: list[DownstreamProcedure]) -> Optional[str]:
    if not procedures:
        return None
    return _json_dumps(
        {
            "procedures": [
                {
                    "name": procedure.name,
                    "params": _json_safe_value(procedure.params),
                }
                for procedure in procedures
            ]
        }
    )


def _json_dumps(value: object) -> str:
    return json.dumps(_json_safe_value(value), sort_keys=True, separators=(",", ":"))


def _json_safe_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _truncate_error(error_message: Optional[str]) -> Optional[str]:
    if error_message is None:
        return None
    return str(error_message)[:4000]
