# Copyright 2026, American Go Association, All rights reserved

"""Azure SQL persistence for the first AGA Events vertical slice."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4
from workshop_repository import WorkshopRepositoryMixin

try:
    import pyodbc
except ImportError:  # pragma: no cover - exercised only in dependency-light tooling
    pyodbc = None

from event_domain import (
    EventError,
    EventConflict,
    EventForbidden,
    EventNotFound,
    occurrence_registration_status,
    overall_registration_status,
    public_occurrence_payload,
    workshop_registration_available,
    validate_selection_policy,
)


def get_sql_connection_string() -> str | None:
    value = os.environ.get("SQL_CONNECTION_STRING") or os.environ.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
    if value:
        return value
    settings_path = Path(__file__).resolve().parent / "local.settings.json"
    if settings_path.exists():
        values = json.loads(settings_path.read_text(encoding="utf-8")).get("Values", {})
        return values.get("SQL_CONNECTION_STRING") or values.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
    return None


def _row_dict(cursor, row) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for index, value in enumerate(row):
        if isinstance(value, datetime) and value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        values[cursor.description[index][0]] = value
    return values


def _sql_datetime(value: datetime | None) -> datetime | None:
    """Convert aware application datetimes to UTC datetime2 values."""
    if value is None or value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _iso(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, UUID):
        return str(value)
    return value


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return _iso(value)


class SqlEventRepository(WorkshopRepositoryMixin):
    def __init__(self, connection_string: str):
        if pyodbc is None:
            raise RuntimeError("pyodbc is required for Azure SQL access.")
        self.connection_string = connection_string

    def _connect(self):
        return pyodbc.connect(self.connection_string, autocommit=False)

    def list_referral_chapters(self):
        """Public chapter identity only; no contacts or Rewards balances."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute("""SELECT ChapterID AS chapter_id, ChapterCode AS chapter_code,
                ChapterName AS chapter_name FROM membership.chapters
                WHERE UPPER(LTRIM(RTRIM(Status))) = 'ACTIVE'
                ORDER BY ChapterName, ChapterCode, ChapterID""")
            return [_row_dict(cursor, row) for row in cursor.fetchall()]
        finally:
            conn.close()

    @staticmethod
    def _referral_chapter(cursor, value):
        # Invalid or unknown links must not prevent an attendee from registering.
        # Existing printed codes retain attribution if a chapter later expires.
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            return None
        text = str(value).strip()
        if not text.isascii() or not text.isdigit() or len(text) > 10:
            return None
        chapter_id = int(text)
        if not 0 < chapter_id <= 2147483647:
            return None
        cursor.execute('''SELECT ChapterID AS chapter_id, ChapterCode AS chapter_code,
            ChapterName AS chapter_name FROM membership.chapters WHERE ChapterID = ?''', chapter_id)
        row = cursor.fetchone()
        return _row_dict(cursor, row) if row else None

    def get_referral_chapter(self, value):
        conn = self._connect()
        try:
            return self._referral_chapter(conn.cursor(), value)
        finally:
            conn.close()

    @staticmethod
    def _managed_event(cursor, event_id: int, principal_id: str) -> dict[str, Any]:
        return WorkshopRepositoryMixin._authorize_workshop(cursor, event_id, principal_id, 'can_manage_events')

    def get_public_event(self, slug: str, now: datetime) -> dict[str, Any]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT event_id, slug, name, description, time_zone, selection_mode,
                       max_occurrence_selections, public_occurrence_limit, lifecycle
                FROM events.events
                WHERE slug = ? AND lifecycle IN ('published', 'registration_open', 'registration_closed')
                """,
                slug,
            )
            event_row = cursor.fetchone()
            if not event_row:
                raise EventNotFound("Event not found.")
            event = _row_dict(cursor, event_row)
            cursor.execute(
                """
                SELECT o.occurrence_id, o.occurrence_key, o.title, o.starts_at_utc,
                       o.ends_at_utc, o.registration_opens_at_utc,
                       o.registration_closes_at_utc, o.capacity, o.waitlist_enabled,
                       o.location_name, o.lifecycle,
                       SUM(CASE WHEN ro.status = 'confirmed' THEN 1 ELSE 0 END) AS confirmed_count,
                       SUM(CASE WHEN ro.status = 'waitlisted' THEN 1 ELSE 0 END) AS waitlisted_count
                FROM events.event_occurrences o
                LEFT JOIN events.registration_occurrences ro ON ro.occurrence_id = o.occurrence_id
                WHERE o.event_id = ? AND o.ends_at_utc >= ? AND o.lifecycle = 'published'
                GROUP BY o.occurrence_id, o.occurrence_key, o.title, o.starts_at_utc,
                         o.ends_at_utc, o.registration_opens_at_utc,
                         o.registration_closes_at_utc, o.capacity, o.waitlist_enabled,
                         o.location_name, o.lifecycle
                ORDER BY o.starts_at_utc, o.occurrence_id
                """,
                event["event_id"],
                _sql_datetime(now),
            )
            rows = [_row_dict(cursor, row) for row in cursor.fetchall()]
            from workshop_providers import email_delivery_config
            test_mode = email_delivery_config()['mode'] == 'test'
            occurrences = []
            for row in rows:
                clock = self._rehearsal_clock(cursor, row['occurrence_id']) if test_mode else None
                effective_now = max(now, clock['simulated_at_utc']) if clock else now
                if workshop_registration_available(row, effective_now):
                    occurrences.append(public_occurrence_payload(row, effective_now))
            # The configured limit sizes the page's scroll viewport, not its results.
            if event["lifecycle"] == "registration_closed":
                for occurrence in occurrences:
                    occurrence["selectable"] = False
                    occurrence["selection_disabled_reason"] = "Registration is closed."
            return _json_ready({**event, "occurrences": occurrences})
        finally:
            conn.close()

    def register_free(
        self,
        *,
        slug: str,
        occurrence_ids: list[int],
        contact: dict[str, str],
        idempotency_key: str,
        now: datetime,
        principal_id: str | None = None,
        managed_event_id: int | None = None,
        send_confirmation_email: bool = True,
        referral_chapter_id: int | str | None = None,
    ) -> dict[str, Any]:
        """Assign capacity atomically, producing confirmed or waitlisted selections."""
        if not isinstance(send_confirmation_email, bool):
            raise EventError('Send confirmation email must be true or false.')
        if not send_confirmation_email and not principal_id:
            raise EventForbidden('Only an authorized organizer can skip a confirmation email.')
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT event_id, current_event_version_id, selection_mode, max_occurrence_selections
                FROM events.events WITH (UPDLOCK, HOLDLOCK)
                WHERE slug = ? AND lifecycle IN ('published', 'registration_open')
                """,
                slug,
            )
            event_row = cursor.fetchone()
            if not event_row:
                raise EventNotFound("Event is not open for registration.")
            event = _row_dict(cursor, event_row)
            if principal_id:
                if event['event_id'] != managed_event_id:
                    raise EventForbidden('This registration is outside your event scope.')
                self._authorize_workshop(cursor, event['event_id'], principal_id, 'can_manage_registrations')
            validate_selection_policy(
                occurrence_ids,
                event["selection_mode"],
                event["max_occurrence_selections"],
            )

            cursor.execute(
                """
                SELECT registration_id FROM events.registrations
                WHERE event_id = ? AND idempotency_key = ?
                """,
                event["event_id"],
                idempotency_key,
            )
            existing = cursor.fetchone()
            if existing:
                cursor.execute('SELECT first_name, last_name, email, city, state_province, country, postal_code FROM events.registration_contacts WHERE registration_id = ?', existing[0])
                saved_contact = _row_dict(cursor, cursor.fetchone())
                cursor.execute('SELECT occurrence_id FROM events.registration_occurrences WHERE registration_id = ? ORDER BY occurrence_id', existing[0])
                saved_ids = [r[0] for r in cursor.fetchall()]
                if saved_contact != contact or saved_ids != occurrence_ids:
                    raise EventConflict('This submission key was already used for a different registration.')
                result = self._registration_result(cursor, existing[0])
                conn.commit()
                return result

            placeholders = ",".join("?" for _ in occurrence_ids)
            cursor.execute(
                f"""
                SELECT occurrence_id, title, starts_at_utc, ends_at_utc, capacity, waitlist_enabled,
                       registration_opens_at_utc, registration_closes_at_utc, lifecycle
                FROM events.event_occurrences WITH (UPDLOCK, HOLDLOCK)
                WHERE event_id = ? AND occurrence_id IN ({placeholders})
                ORDER BY occurrence_id
                """,
                event["event_id"],
                *occurrence_ids,
            )
            occurrences = [_row_dict(cursor, row) for row in cursor.fetchall()]
            if len(occurrences) != len(occurrence_ids):
                raise EventConflict("One or more selected events are no longer available.")

            assigned: list[tuple[dict[str, Any], str]] = []
            for occurrence in occurrences:
                clock=self._rehearsal_clock(cursor,occurrence['occurrence_id'])
                if clock:
                    from workshop_providers import email_delivery_config
                    if email_delivery_config()['mode']=='test' and not workshop_registration_available(occurrence, max(now, clock['simulated_at_utc'])):
                        raise EventConflict('Registration is closed at this rehearsal step. Use a new rehearsal date for more signups.')
                if occurrence["lifecycle"] != "published":
                    raise EventConflict(f"{occurrence['title']} is not available.")
                if not workshop_registration_available(occurrence, now):
                    raise EventConflict('Choose a workshop at least 24 hours away. Refresh the available dates.')
                cursor.execute('''SELECT COUNT(*) FROM events.registration_occurrences ro
                    JOIN events.registration_contacts c ON c.registration_id = ro.registration_id
                    WHERE ro.occurrence_id = ? AND c.email = ? AND ro.status IN ('confirmed','waitlisted')''',
                    occurrence['occurrence_id'], contact['email'])
                if cursor.fetchone()[0]:
                    raise EventConflict('This email already has a signup for the selected workshop.')
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM events.registration_occurrences
                    WHERE occurrence_id = ? AND status = 'confirmed'
                    """,
                    occurrence["occurrence_id"],
                )
                confirmed_count = int(cursor.fetchone()[0])
                status = occurrence_registration_status(
                    capacity=occurrence["capacity"],
                    confirmed_count=confirmed_count,
                    waitlist_enabled=bool(occurrence["waitlist_enabled"]),
                )
                assigned.append((occurrence, status))

            registration_id = uuid4()
            chapter = self._referral_chapter(cursor, referral_chapter_id)
            referral_id = chapter['chapter_id'] if chapter else None
            overall_status = overall_registration_status(status for _, status in assigned)
            cursor.execute(
                """
                INSERT INTO events.registrations
                    (registration_id, event_id, event_version_id, status, payment_status,
                     idempotency_key, submitted_at_utc, created_at_utc, updated_at_utc, referral_chapter_id)
                VALUES (?, ?, ?, ?, 'not_required', ?, ?, ?, ?, ?)
                """,
                registration_id,
                event["event_id"],
                event["current_event_version_id"],
                overall_status,
                idempotency_key,
                _sql_datetime(now),
                _sql_datetime(now),
                _sql_datetime(now),
                referral_id,
            )
            cursor.execute(
                """
                INSERT INTO events.registration_contacts
                    (registration_id, first_name, last_name, email, city,
                     state_province, country, postal_code)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                registration_id,
                contact["first_name"],
                contact["last_name"],
                contact["email"],
                contact["city"],
                contact["state_province"],
                contact["country"],
                contact["postal_code"],
            )
            for occurrence, status in assigned:
                cursor.execute(
                    """
                    INSERT INTO events.registration_occurrences
                        (registration_id, occurrence_id, status, assigned_at_utc)
                    VALUES (?, ?, ?, ?)
                    """,
                    registration_id,
                    occurrence["occurrence_id"],
                    status,
                    _sql_datetime(now),
                )
                self._queue_registration_emails(cursor, event['event_id'], registration_id, occurrence, status, now)
                if status == 'confirmed' and not send_confirmation_email:
                    # Suppress in the registration transaction, before any worker can claim it.
                    # Keeping the dedupe row also prevents settings changes from recreating it.
                    cursor.execute("""UPDATE events.email_outbox SET status = 'suppressed'
                        WHERE registration_id = ? AND occurrence_id = ?
                          AND kind = 'confirmed' AND status = 'pending'""",
                        registration_id, occurrence['occurrence_id'])
            cursor.execute(
                """
                INSERT INTO events.audit_log
                    (event_id, registration_id, action, actor_type, detail_json, created_at_utc)
                VALUES (?, ?, 'registration.submitted', 'public', ?, ?)
                """,
                event["event_id"],
                registration_id,
                json.dumps({"occurrence_ids": occurrence_ids, "status": overall_status}),
                _sql_datetime(now),
            )
            if principal_id:
                self._audit_workshop(cursor, event['event_id'], principal_id, 'registration.added_by_admin',
                                     {'occurrence_ids': occurrence_ids, 'send_confirmation_email': send_confirmation_email}, now, registration_id)
            conn.commit()
            return self._registration_result(cursor, registration_id)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _registration_result(self, cursor, registration_id) -> dict[str, Any]:
        cursor.execute(
            """
            SELECT r.registration_id, r.status, r.payment_status, r.submitted_at_utc,
                   o.occurrence_id, o.title, o.starts_at_utc, ro.status AS occurrence_status
            FROM events.registrations r
            JOIN events.registration_occurrences ro ON ro.registration_id = r.registration_id
            JOIN events.event_occurrences o ON o.occurrence_id = ro.occurrence_id
            WHERE r.registration_id = ?
            ORDER BY o.starts_at_utc, o.occurrence_id
            """,
            registration_id,
        )
        rows = [_row_dict(cursor, row) for row in cursor.fetchall()]
        if not rows:
            raise EventNotFound("Registration not found.")
        first = rows[0]
        return _json_ready(
            {
                "registration_id": first["registration_id"],
                "status": first["status"],
                "payment_status": first["payment_status"],
                "submitted_at": first["submitted_at_utc"],
                "occurrences": [
                    {
                        "occurrence_id": row["occurrence_id"],
                        "title": row["title"],
                        "starts_at": row["starts_at_utc"],
                        "status": row["occurrence_status"],
                    }
                    for row in rows
                ],
            }
        )

    def add_occurrence(
        self,
        *,
        event_id: int,
        principal_id: str,
        occurrence: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            event = self._managed_event(cursor, event_id, principal_id)
            if occurrence["time_zone"] != event["time_zone"]:
                raise EventConflict("The submitted time zone does not match the event time zone.")
            if occurrence['starts_at_utc']<=now:
                raise EventConflict('New workshops must start in the future.')
            rehearsal=self._settings(cursor,event_id)['is_test']
            if rehearsal and occurrence['starts_at_utc']<=now+timedelta(hours=24):
                raise EventConflict('Choose a rehearsal date more than 24 hours in the future.')
            version_id = event["current_event_version_id"]
            cursor.execute(
                """
                INSERT INTO events.event_occurrences
                    (event_id, event_version_id, occurrence_key, title, starts_at_utc,
                     ends_at_utc, registration_opens_at_utc, registration_closes_at_utc,
                     capacity, waitlist_enabled, location_name, lifecycle,
                     created_at_utc, updated_at_utc)
                OUTPUT inserted.occurrence_id
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'published', ?, ?)
                """,
                event_id,
                version_id,
                occurrence["occurrence_key"],
                occurrence["title"],
                _sql_datetime(occurrence["starts_at_utc"]),
                _sql_datetime(occurrence["ends_at_utc"]),
                _sql_datetime(occurrence["registration_opens_at_utc"]),
                _sql_datetime(occurrence["registration_closes_at_utc"]),
                occurrence["capacity"],
                occurrence["waitlist_enabled"],
                occurrence["location_name"],
                _sql_datetime(now),
                _sql_datetime(now),
            )
            occurrence_id = int(cursor.fetchone()[0])
            if rehearsal:
                cursor.execute('INSERT INTO events.rehearsal_clocks(occurrence_id,stage,simulated_at_utc) VALUES (?,0,?)',occurrence_id,_sql_datetime(now))
            cursor.execute(
                """
                INSERT INTO events.audit_log
                    (event_id, action, actor_type, actor_principal_id, detail_json, created_at_utc)
                VALUES (?, 'occurrence.created', 'organizer', ?, ?, ?)
                """,
                event_id,
                principal_id,
                json.dumps({"occurrence_id": occurrence_id, "occurrence_key": occurrence["occurrence_key"]}),
                _sql_datetime(now),
            )
            conn.commit()
            return _json_ready({"occurrence_id": occurrence_id, **occurrence})
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_managed_occurrences(
        self, *, event_id: int, principal_id: str, now: datetime
    ) -> dict[str, Any]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            event = self._managed_event(cursor, event_id, principal_id)
            cursor.execute(
                """
                SELECT o.occurrence_id, o.occurrence_key, o.title, o.starts_at_utc,
                       o.ends_at_utc, o.registration_opens_at_utc,
                       o.registration_closes_at_utc, o.capacity, o.waitlist_enabled,
                       o.location_name, o.lifecycle,
                       SUM(CASE WHEN ro.status = 'confirmed' THEN 1 ELSE 0 END) AS confirmed_count,
                       SUM(CASE WHEN ro.status = 'waitlisted' THEN 1 ELSE 0 END) AS waitlisted_count
                FROM events.event_occurrences o
                LEFT JOIN events.registration_occurrences ro ON ro.occurrence_id = o.occurrence_id
                WHERE o.event_id = ?
                  AND o.lifecycle IN ('draft','published','cancelled','completed')
                GROUP BY o.occurrence_id, o.occurrence_key, o.title, o.starts_at_utc,
                         o.ends_at_utc, o.registration_opens_at_utc,
                         o.registration_closes_at_utc, o.capacity, o.waitlist_enabled,
                         o.location_name, o.lifecycle
                ORDER BY o.starts_at_utc, o.occurrence_id
                """,
                event_id,
            )
            occurrences = [_row_dict(cursor, row) for row in cursor.fetchall()]
            return _json_ready({"event": event, "occurrences": occurrences})
        finally:
            conn.close()

    def update_occurrence(
        self,
        *,
        event_id: int,
        occurrence_id: int,
        principal_id: str,
        occurrence: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            event = self._managed_event(cursor, event_id, principal_id)
            if occurrence["time_zone"] != event["time_zone"]:
                raise EventConflict("The submitted time zone does not match the event time zone.")
            cursor.execute(
                """
                SELECT occurrence_id, occurrence_key, lifecycle, starts_at_utc
                FROM events.event_occurrences WITH (UPDLOCK, HOLDLOCK)
                WHERE event_id = ? AND occurrence_id = ?
                """,
                event_id,
                occurrence_id,
            )
            current = cursor.fetchone()
            if not current:
                raise EventNotFound("Workshop occurrence not found.")
            if current[2] != 'published':
                raise EventConflict('Only published workshops can be edited.')
            if current[3].replace(tzinfo=timezone.utc)<=now or occurrence['starts_at_utc']<=now:
                raise EventConflict('Workshop dates can only be edited before they start.')
            self._check_meeting_editable(cursor, occurrence_id)
            clock=self._rehearsal_clock(cursor,occurrence_id)
            if clock and clock['stage']>0:
                raise EventConflict('This rehearsal has advanced. Use a new rehearsal date to test another schedule.')
            if clock and occurrence['starts_at_utc']<=now+timedelta(hours=24):
                raise EventConflict('Choose a rehearsal date more than 24 hours in the future.')
            if current[1] != occurrence["occurrence_key"]:
                raise EventConflict("The occurrence key cannot be changed after creation.")
            cursor.execute(
                """
                SELECT COUNT(*) FROM events.registration_occurrences
                WHERE occurrence_id = ? AND status = 'confirmed'
                """,
                occurrence_id,
            )
            confirmed_count = int(cursor.fetchone()[0])
            if occurrence["capacity"] is not None and occurrence["capacity"] < confirmed_count:
                raise EventConflict(
                    f"Capacity cannot be lower than the {confirmed_count} confirmed registrations."
                )
            cursor.execute(
                """
                UPDATE events.event_occurrences
                SET title = ?, starts_at_utc = ?, ends_at_utc = ?,
                    registration_opens_at_utc = ?, registration_closes_at_utc = ?,
                    capacity = ?, waitlist_enabled = ?, location_name = ?,
                    updated_at_utc = ?
                WHERE event_id = ? AND occurrence_id = ?
                """,
                occurrence["title"],
                _sql_datetime(occurrence["starts_at_utc"]),
                _sql_datetime(occurrence["ends_at_utc"]),
                _sql_datetime(occurrence["registration_opens_at_utc"]),
                _sql_datetime(occurrence["registration_closes_at_utc"]),
                occurrence["capacity"],
                occurrence["waitlist_enabled"],
                occurrence["location_name"],
                _sql_datetime(now),
                event_id,
                occurrence_id,
            )
            cursor.execute(
                """
                INSERT INTO events.audit_log
                    (event_id, action, actor_type, actor_principal_id, detail_json, created_at_utc)
                VALUES (?, 'occurrence.updated', 'organizer', ?, ?, ?)
                """,
                event_id,
                principal_id,
                json.dumps({"occurrence_id": occurrence_id, "confirmed_count": confirmed_count}),
                _sql_datetime(now),
            )
            cursor.execute("UPDATE events.workshop_meetings SET sync_status = 'pending', updated_at_utc = ? WHERE occurrence_id = ? AND source = 'zoom'", _sql_datetime(now), occurrence_id)
            cursor.execute("UPDATE events.email_outbox SET status = 'suppressed' WHERE occurrence_id = ? AND status = 'pending' AND kind NOT IN ('cancelled','workshop_cancelled')", occurrence_id)
            cursor.execute("SELECT registration_id, status FROM events.registration_occurrences WHERE occurrence_id = ? AND status <> 'cancelled'", occurrence_id)
            for selected in cursor.fetchall():
                self._queue_registration_emails(cursor, event_id, selected[0],
                    dict(occurrence_id=occurrence_id, **occurrence), selected[1], now, str(uuid4()))
            self._promote_waitlist(cursor, event_id, dict(occurrence_id=occurrence_id, lifecycle='published', **occurrence), now)
            conn.commit()
            return _json_ready(
                {
                    "occurrence_id": occurrence_id,
                    "confirmed_count": confirmed_count,
                    **occurrence,
                }
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
