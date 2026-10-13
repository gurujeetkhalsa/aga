# Copyright 2026, American Go Association, All rights reserved

"""Framework-independent validation and availability rules for AGA Events."""

from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
ALLOWED_SELECTION_MODES = {"single", "multiple"}


class EventError(Exception):
    """Base class for errors safe to translate into an HTTP response."""

    status_code = 400
    code = "event_error"

    def __init__(self, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.details = details or {}


class EventNotFound(EventError):
    status_code = 404
    code = "not_found"


class EventConflict(EventError):
    status_code = 409
    code = "conflict"


class EventForbidden(EventError):
    status_code = 403
    code = "forbidden"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_datetime(value: Any, field_name: str) -> datetime:
    """Parse a timezone-aware ISO 8601 timestamp."""
    if not isinstance(value, str) or not value.strip():
        raise EventError(f"{field_name} is required.")
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise EventError(f"{field_name} must be a valid ISO 8601 timestamp.") from exc
    if parsed.tzinfo is None:
        raise EventError(f"{field_name} must include a UTC offset or Z.")
    return parsed.astimezone(timezone.utc)


def require_text(value: Any, field_name: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EventError(f"{field_name} is required.")
    cleaned = " ".join(value.strip().split())
    if len(cleaned) > maximum:
        raise EventError(f"{field_name} must be {maximum} characters or fewer.")
    return cleaned


def normalize_contact(payload: dict[str, Any]) -> dict[str, str | None]:
    """Validate the minimum contact contract for a free registration."""
    if not isinstance(payload, dict):
        raise EventError('Contact must be an object.')
    first_name = require_text(payload.get("first_name"), "first_name", 100)
    last_name = require_text(payload.get("last_name"), "last_name", 100)
    email = require_text(payload.get("email"), "email", 254).lower()
    if not EMAIL_RE.fullmatch(email):
        raise EventError("email must be a valid email address.")
    return {
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "city": optional_text(payload.get("city"), "city", 120),
        "state_province": optional_text(payload.get("state_province"), "state_province", 120),
        "country": optional_text(payload.get("country"), "country", 100),
        "postal_code": optional_text(payload.get("postal_code"), "postal_code", 32),
    }


def optional_text(value: Any, field_name: str, maximum: int) -> str | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise EventError(f"{field_name} must be text.")
    cleaned = " ".join(value.strip().split())
    if not cleaned:
        return None
    if len(cleaned) > maximum:
        raise EventError(f"{field_name} must be {maximum} characters or fewer.")
    return cleaned


def normalize_occurrence_ids(values: Any) -> list[int]:
    if not isinstance(values, list) or not values:
        raise EventError("Select at least one upcoming event.")
    normalized: list[int] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value,(int,str)) or (isinstance(value,str) and not value.isdecimal()):
            raise EventError("occurrence_ids must contain integer identifiers.")
        try:
            occurrence_id = int(value)
        except (TypeError, ValueError) as exc:
            raise EventError("occurrence_ids must contain integer identifiers.") from exc
        if occurrence_id < 1:
            raise EventError("occurrence_ids must contain positive identifiers.")
        if occurrence_id not in normalized:
            normalized.append(occurrence_id)
    return sorted(normalized)


def validate_selection_policy(
    occurrence_ids: list[int], selection_mode: str, max_occurrence_selections: int | None
) -> None:
    if selection_mode not in ALLOWED_SELECTION_MODES:
        raise EventError("The event has an unsupported selection policy.")
    if selection_mode == "single" and len(occurrence_ids) != 1:
        raise EventError("This event allows selecting exactly one date.")
    if max_occurrence_selections is not None and len(occurrence_ids) > max_occurrence_selections:
        raise EventError(
            f"Select no more than {max_occurrence_selections} dates.",
            details={"maximum": max_occurrence_selections},
        )


def occurrence_registration_status(
    *, capacity: int | None, confirmed_count: int, waitlist_enabled: bool
) -> str:
    """Return the status assigned while the caller holds an occurrence lock."""
    if capacity is None or confirmed_count < capacity:
        return "confirmed"
    if waitlist_enabled:
        return "waitlisted"
    raise EventConflict("One of the selected events filled before registration completed.")


def overall_registration_status(statuses: Iterable[str]) -> str:
    unique = set(statuses)
    if unique == {"confirmed"}:
        return "confirmed"
    if unique == {"waitlisted"}:
        return "waitlisted"
    if unique <= {"confirmed", "waitlisted"}:
        return "mixed"
    raise EventError("Registration contains an unsupported occurrence status.")


def workshop_registration_available(row: dict[str, Any], now: datetime) -> bool:
    """Dates remain available through exactly 24 hours before their start."""
    return row['lifecycle'] == 'published' and row['starts_at_utc'] >= now + timedelta(hours=24)


def public_occurrence_payload(row: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    """Add stable, presentation-ready capacity and selection fields."""
    now = now or utc_now()
    capacity = row.get("capacity")
    confirmed = int(row.get("confirmed_count") or 0)
    remaining = None if capacity is None else max(int(capacity) - confirmed, 0)
    lifecycle = row.get("lifecycle")
    selectable = lifecycle == "published"
    reason = None
    if not workshop_registration_available(row, now):
        selectable, reason = False, "Choose a workshop at least 24 hours away."
    elif remaining == 0 and not row.get("waitlist_enabled"):
        selectable, reason = False, "Full"

    if remaining == 0 and row.get("waitlist_enabled"):
        availability = "waitlist"
    elif remaining is None:
        availability = "available"
    elif remaining > 0:
        availability = "available"
    else:
        availability = "full"

    return {
        **row,
        "confirmed_count": confirmed,
        "remaining_capacity": remaining,
        "availability": availability,
        "selectable": selectable,
        "selection_disabled_reason": reason,
    }


def _event_datetime(payload: dict[str, Any], field_name: str, time_zone_name: str) -> datetime:
    aware_value = payload.get(field_name)
    if aware_value:
        return parse_iso_datetime(aware_value, field_name)
    local_field = f"{field_name}_local"
    local_value = payload.get(local_field)
    if not isinstance(local_value, str) or not local_value.strip():
        raise EventError(f"{field_name} is required.")
    try:
        parsed = datetime.fromisoformat(local_value.strip())
    except ValueError as exc:
        raise EventError(f"{local_field} must be a valid local date and time.") from exc
    if parsed.tzinfo is not None:
        raise EventError(f"{local_field} must not include a UTC offset.")
    try:
        zone = ZoneInfo(time_zone_name)
    except ZoneInfoNotFoundError as exc:
        raise EventError("time_zone must be a valid IANA time zone.") from exc
    aware = parsed.replace(tzinfo=zone)
    round_trip = aware.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None)
    if round_trip != parsed:
        raise EventError(f"{local_field} falls in a daylight-saving time gap.")
    return aware.astimezone(timezone.utc)


def _optional_event_datetime(
    payload: dict[str, Any], field_name: str, time_zone_name: str
) -> datetime | None:
    if payload.get(field_name) or payload.get(f"{field_name}_local"):
        return _event_datetime(payload, field_name, time_zone_name)
    return None


def normalize_occurrence_input(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate organizer input when appending a future occurrence."""
    occurrence_key = require_text(payload.get("occurrence_key"), "occurrence_key", 64).lower()
    if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", occurrence_key):
        raise EventError("occurrence_key may contain lowercase letters, numbers, and hyphens.")
    title = require_text(payload.get("title"), "title", 200)
    time_zone_name = require_text(payload.get("time_zone", "America/New_York"), "time_zone", 64)
    try:
        ZoneInfo(time_zone_name)
    except ZoneInfoNotFoundError as exc:
        raise EventError('time_zone must be a valid IANA time zone.') from exc
    starts_at = _event_datetime(payload, "starts_at", time_zone_name)
    ends_at = _optional_event_datetime(payload, "ends_at", time_zone_name) or starts_at + timedelta(hours=1)
    if ends_at <= starts_at:
        raise EventError("ends_at must be later than starts_at.")
    capacity_value = payload.get("capacity")
    capacity = None
    if capacity_value not in (None, ""):
        if isinstance(capacity_value,bool) or not isinstance(capacity_value,(int,str)) or (isinstance(capacity_value,str) and not capacity_value.isdecimal()):
            raise EventError('capacity must be a positive integer or null.')
        try:
            capacity = int(capacity_value)
        except (TypeError, ValueError) as exc:
            raise EventError("capacity must be a positive integer or null.") from exc
        if capacity < 1:
            raise EventError("capacity must be a positive integer or null.")
    location_name = payload.get("location_name")
    if location_name not in (None, ""):
        location_name = require_text(location_name, "location_name", 200)
    opens = _optional_event_datetime(payload,'registration_opens_at',time_zone_name)
    closes = _optional_event_datetime(payload,'registration_closes_at',time_zone_name) or starts_at
    if opens and opens>=closes:
        raise EventError('Registration must open before it closes.')
    if closes>starts_at:
        raise EventError('Registration must close by the workshop start.')
    if type(payload.get('waitlist_enabled',True)) is not bool:
        raise EventError('waitlist_enabled must be true or false.')
    return {
        "occurrence_key": occurrence_key,
        "title": title,
        "starts_at_utc": starts_at,
        "ends_at_utc": ends_at,
        "registration_opens_at_utc": opens,
        "registration_closes_at_utc": closes,
        "capacity": capacity,
        "waitlist_enabled": bool(payload.get("waitlist_enabled", True)),
        "location_name": location_name,
        "time_zone": time_zone_name,
    }
