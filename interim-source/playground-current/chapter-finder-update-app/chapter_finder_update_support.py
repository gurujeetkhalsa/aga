# Copyright 2026, American Go Association, All rights reserved

"""Business logic for the Chapter Finder update and review portal."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import quote


CLUB_ID = 454497
EDITABLE_FIELDS = (
    "chapter_name",
    "address_text",
    "postal_text",
    "city",
    "state_province",
    "postal_code",
    "contact_name",
    "contact_email",
    "contact_phone",
    "website_url",
    "website_label",
    "description_text",
)
SQL_TO_JSON_FIELD = {
    "Chapter_Name": "chapter_name",
    "Address_Text": "address_text",
    "Postal_Text": "postal_text",
    "City": "city",
    "State_Province": "state_province",
    "Postal_Code": "postal_code",
    "Contact_Name": "contact_name",
    "Contact_Email": "contact_email",
    "Contact_Phone": "contact_phone",
    "Website_Url": "website_url",
    "Website_Label": "website_label",
    "Description_Text": "description_text",
}
FIELD_LIMITS = {
    "chapter_name": 50,
    "address_text": 101,
    "postal_text": 512,
    "city": 30,
    "state_province": 64,
    "postal_code": 10,
    "contact_name": 256,
    "contact_email": 100,
    "contact_phone": 24,
    "website_url": 100,
    "website_label": 512,
    "description_text": 500,
}


class PortalError(ValueError):
    """Represent a user-facing portal validation or authorization error."""

    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def token_hash(raw_token: str) -> str:
    """Hash a raw chapter access token for lookup and storage."""
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def create_access_link(
    adapter: Any,
    *,
    base_url: str,
    chapter_finder_id: int | None = None,
    clubexpress_chapter_id: int | None = None,
    expires_days: int | None = 365,
    created_by: str | None = None,
    label: str | None = None,
) -> dict[str, Any]:
    """Create one high-entropy chapter link while storing only its hash."""
    current = load_current_entry(adapter, chapter_finder_id, clubexpress_chapter_id)
    raw_token = secrets.token_urlsafe(32)
    expires_at = None
    if expires_days is not None:
        if expires_days < 1 or expires_days > 3650:
            raise PortalError("Expiration must be between 1 and 3650 days.")
        expires_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=expires_days)
    rows = adapter.query_rows(
        """
INSERT INTO [clubexpress].[chapter_finder_update_access_links]
(
    [Chapter_Finder_Entry_ID], [Club_ID], [Chapter_Finder_ID], [Token_Hash_Sha256],
    [Link_Label], [Created_By], [Expires_At]
)
OUTPUT inserted.[Chapter_Finder_Update_Access_Link_ID], inserted.[Created_At]
VALUES (?, ?, ?, ?, ?, ?, ?);
""",
        (
            current["Chapter_Finder_Entry_ID"],
            CLUB_ID,
            current["Chapter_Finder_ID"],
            token_hash(raw_token),
            normalize_text(label),
            normalize_text(created_by),
            expires_at,
        ),
    )
    clean_base = base_url.strip().rstrip("#")
    separator = "&" if "#" in clean_base else "#"
    return {
        "access_link_id": int(rows[0]["Chapter_Finder_Update_Access_Link_ID"]),
        "chapter_finder_id": int(current["Chapter_Finder_ID"]),
        "clubexpress_chapter_id": nullable_int(current.get("ClubExpress_Chapter_ID")),
        "chapter_name": current["Chapter_Name"],
        "expires_at": expires_at,
        "url": f"{clean_base}{separator}token={quote(raw_token, safe='')}",
    }


def revoke_access_link(adapter: Any, access_link_id: int) -> None:
    """Disable an existing chapter form link."""
    adapter.execute_statements(
        [
            (
                """
UPDATE [clubexpress].[chapter_finder_update_access_links]
SET [Is_Active] = 0, [Updated_At] = SYSUTCDATETIME()
WHERE [Chapter_Finder_Update_Access_Link_ID] = ?;
""",
                (access_link_id,),
            )
        ]
    )


def load_access_context(adapter: Any, raw_token: str, *, record_use: bool = False) -> dict[str, Any]:
    """Load the current chapter and private administrators for a valid token."""
    if not raw_token or len(raw_token) > 256:
        raise PortalError("This chapter link is invalid.", 401)
    rows = adapter.query_rows(
        """
SELECT TOP (1)
    link.[Chapter_Finder_Update_Access_Link_ID],
    link.[Expires_At],
    link.[Is_Active],
    entry.*
FROM [clubexpress].[chapter_finder_update_access_links] AS link
INNER JOIN [clubexpress].[chapter_finder_entries] AS entry
    ON entry.[Chapter_Finder_Entry_ID] = link.[Chapter_Finder_Entry_ID]
WHERE link.[Club_ID] = ?
  AND link.[Token_Hash_Sha256] = ?
  AND entry.[Is_Current] = 1;
""",
        (CLUB_ID, token_hash(raw_token)),
    )
    if not rows:
        raise PortalError("This chapter link is invalid.", 401)
    row = rows[0]
    if not bool(row["Is_Active"]):
        raise PortalError("This chapter link has been disabled.", 403)
    expires_at = row.get("Expires_At")
    if expires_at is not None and expires_at <= datetime.now(timezone.utc).replace(tzinfo=None):
        raise PortalError("This chapter link has expired.", 403)
    administrators = load_current_administrators(adapter, int(row["Chapter_Finder_Entry_ID"]))
    base_admins = canonical_administrators(administrators)
    if record_use:
        adapter.execute_statements(
            [
                (
                    """
UPDATE [clubexpress].[chapter_finder_update_access_links]
SET [Last_Used_At] = SYSUTCDATETIME(),
    [Use_Count] = [Use_Count] + 1,
    [Updated_At] = SYSUTCDATETIME()
WHERE [Chapter_Finder_Update_Access_Link_ID] = ?;
""",
                    (row["Chapter_Finder_Update_Access_Link_ID"],),
                )
            ]
        )
    return {
        "access_link_id": int(row["Chapter_Finder_Update_Access_Link_ID"]),
        "entry": row,
        "administrators": administrators,
        "base_administrator_json": base_admins,
        "base_administrator_sha256": sha256_json(base_admins),
    }


def context_payload(context: dict[str, Any]) -> dict[str, Any]:
    """Return the browser-safe form context."""
    entry = context["entry"]
    return {
        "chapter": {
            "chapter_finder_id": int(entry["Chapter_Finder_ID"]),
            "clubexpress_chapter_id": nullable_int(entry.get("ClubExpress_Chapter_ID")),
            "organization_parent": entry.get("Organization_Parent"),
        },
        "fields": current_values_from_row(entry),
        "administrators": [
            {
                "assignment_id": int(item["Chapter_Finder_Administrator_Assignment_ID"]),
                "display_name": item["Administrator_Display_Name"],
                "membership_agaid": nullable_int(item.get("Membership_AGAID")),
                "match_status": item["Member_Match_Status"],
            }
            for item in context["administrators"]
        ],
        "base_content_sha256": entry["Content_Sha256"],
        "base_administrator_sha256": context["base_administrator_sha256"],
        "expires_at": context["entry"].get("Expires_At"),
    }


def lookup_member_for_access_token(adapter: Any, raw_token: str, agaid: int) -> dict[str, Any]:
    """Resolve one active AGA member for the add-administrator control."""
    load_access_context(adapter, raw_token)
    member = load_member(adapter, agaid)
    if member is None:
        raise PortalError("No AGA member was found with that ID.", 404)
    status = normalize_text(member.get("Status"))
    if status and status.casefold() != "active":
        raise PortalError("That AGA member is not currently active.", 409)
    return {
        "agaid": int(member["AGAID"]),
        "display_name": member_display_name(member),
        "status": status,
    }


def submit_update(adapter: Any, raw_token: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Validate and create one submitted update request."""
    context = load_access_context(adapter, raw_token)
    entry = context["entry"]
    submitted_base_hash = str(payload.get("base_content_sha256") or "")
    if not hmac.compare_digest(submitted_base_hash.lower(), str(entry["Content_Sha256"]).lower()):
        raise PortalError("The chapter listing changed after this form was loaded. Reload before submitting.", 409)
    submitted_admin_hash = str(payload.get("base_administrator_sha256") or "")
    if not hmac.compare_digest(submitted_admin_hash.lower(), context["base_administrator_sha256"].lower()):
        raise PortalError("The chapter administrator list changed after this form was loaded. Reload before submitting.", 409)

    raw_fields = payload.get("fields")
    if not isinstance(raw_fields, dict):
        raise PortalError("Form fields are required.")
    current_fields = current_values_from_row(entry)
    proposed_fields = build_normalized_proposed(current_fields, raw_fields)
    validate_public_fields(proposed_fields)
    field_diffs = build_field_diffs(current_fields, proposed_fields)

    retained_ids = parse_positive_int_list(payload.get("retained_assignment_ids"), "retained administrator")
    added_agaids = parse_positive_int_list(payload.get("added_administrator_agaids"), "added AGA ID")
    proposed_admins = resolve_proposed_administrators(
        adapter,
        context["administrators"],
        retained_ids,
        added_agaids,
    )
    proposed_admin_hash = sha256_json(proposed_admins)
    if proposed_admin_hash != context["base_administrator_sha256"]:
        field_diffs.append(
            {
                "field_name": "administrators",
                "current_value": format_administrator_list(context["base_administrator_json"]),
                "proposed_value": format_administrator_list(proposed_admins),
                "change_type": "changed",
                "display_order": len(EDITABLE_FIELDS) + 1,
            }
        )
    if not field_diffs:
        raise PortalError("No changes were made.")

    submitter_name = require_text(payload.get("submitted_by_name"), "Your name", 256)
    submitter_email = require_email(payload.get("submitted_by_email"))
    submitted_by_role = limited_text(payload.get("submitted_by_role"), 128)
    submitter_notes = limited_text(payload.get("submitter_notes"), 4000)

    normalized_payload = {**proposed_fields, "administrators": proposed_admins}
    proposed_public_hash = sha256_json(build_public_content_payload(entry, proposed_fields))
    base_admin_json = json.dumps(context["base_administrator_json"], ensure_ascii=False, sort_keys=True)
    proposed_admin_json = json.dumps(proposed_admins, ensure_ascii=False, sort_keys=True)
    proposed_json = json.dumps(
        {
            "fields": {key: raw_fields.get(key) for key in EDITABLE_FIELDS},
            "retained_assignment_ids": retained_ids,
            "added_administrator_agaids": added_agaids,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    normalized_json = json.dumps(normalized_payload, ensure_ascii=False, sort_keys=True)
    diff_json = json.dumps(field_diffs, ensure_ascii=False, sort_keys=True)
    rows = adapter.query_rows(
        """
INSERT INTO [clubexpress].[chapter_finder_update_requests]
(
    [Chapter_Finder_Entry_ID], [Club_ID], [Chapter_Finder_ID], [ClubExpress_Chapter_ID],
    [Chapter_Name_At_Submission], [Request_Source], [Status],
    [Submitted_By_Name], [Submitted_By_Email], [Submitted_By_Role], [Submitter_Notes],
    [Base_Content_Sha256], [Proposed_Content_Sha256],
    [Proposed_Json], [Normalized_Proposed_Json], [Field_Diff_Json], [Changed_Field_Count],
    [Chapter_Finder_Update_Access_Link_ID], [Base_Administrator_Json],
    [Proposed_Administrator_Json], [Base_Administrator_Sha256], [Proposed_Administrator_Sha256]
)
OUTPUT inserted.[Chapter_Finder_Update_Request_ID], inserted.[Request_Key], inserted.[Created_At]
VALUES (?, ?, ?, ?, ?, N'chapter_update_portal', N'submitted', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
""",
        (
            entry["Chapter_Finder_Entry_ID"],
            CLUB_ID,
            entry["Chapter_Finder_ID"],
            entry.get("ClubExpress_Chapter_ID"),
            entry["Chapter_Name"],
            submitter_name,
            submitter_email,
            submitted_by_role,
            submitter_notes,
            entry["Content_Sha256"],
            proposed_public_hash,
            proposed_json,
            normalized_json,
            diff_json,
            len(field_diffs),
            context["access_link_id"],
            base_admin_json,
            proposed_admin_json,
            context["base_administrator_sha256"],
            proposed_admin_hash,
        ),
    )
    request_id = int(rows[0]["Chapter_Finder_Update_Request_ID"])
    statements: list[tuple[str, tuple[Any, ...]]] = []
    for diff in field_diffs:
        statements.append(
            (
                """
INSERT INTO [clubexpress].[chapter_finder_update_request_fields]
(
    [Chapter_Finder_Update_Request_ID], [Field_Name], [Current_Value],
    [Proposed_Value], [Change_Type], [Display_Order]
)
VALUES (?, ?, ?, ?, ?, ?);
""",
                (
                    request_id,
                    diff["field_name"],
                    diff["current_value"],
                    diff["proposed_value"],
                    diff["change_type"],
                    diff["display_order"],
                ),
            )
        )
    statements.append(
        (
            """
UPDATE [clubexpress].[chapter_finder_update_access_links]
SET [Last_Submitted_At] = SYSUTCDATETIME(), [Updated_At] = SYSUTCDATETIME()
WHERE [Chapter_Finder_Update_Access_Link_ID] = ?;
""",
            (context["access_link_id"],),
        )
    )
    adapter.execute_statements(statements)
    return {
        "request_id": request_id,
        "request_key": str(rows[0]["Request_Key"]),
        "chapter_name": entry["Chapter_Name"],
        "changed_field_count": len(field_diffs),
        "status": "submitted",
        "created_at": rows[0]["Created_At"],
    }


def list_review_requests(adapter: Any, limit: int = 100) -> list[dict[str, Any]]:
    """List recent requests for the authenticated review surface."""
    bounded_limit = min(max(int(limit), 1), 500)
    rows = adapter.query_rows(
        """
SELECT TOP (?)
    [Chapter_Finder_Update_Request_ID], [Request_Key], [Status], [Push_Status],
    [Chapter_Finder_ID], [ClubExpress_Chapter_ID], [Chapter_Name_At_Submission],
    [Submitted_By_Name], [Submitted_By_Email], [Changed_Field_Count],
    [Reviewed_By], [Approved_By], [Rejected_By], [Pushed_At], [Created_At], [Updated_At]
FROM [clubexpress].[v_chapter_finder_update_portal_review]
ORDER BY
    CASE [Status]
        WHEN N'submitted' THEN 0
        WHEN N'needs_review' THEN 1
        WHEN N'pending_push' THEN 2
        WHEN N'failed' THEN 3
        ELSE 4
    END,
    [Created_At] DESC;
""",
        (bounded_limit,),
    )
    return rows


def load_review_request(adapter: Any, request_id: int) -> dict[str, Any]:
    """Load one request and its field-level changes."""
    rows = adapter.query_rows(
        """
SELECT *
FROM [clubexpress].[v_chapter_finder_update_portal_review]
WHERE [Chapter_Finder_Update_Request_ID] = ?;
""",
        (request_id,),
    )
    if not rows:
        raise PortalError("Update request was not found.", 404)
    request_row = rows[0]
    fields = adapter.query_rows(
        """
SELECT [Field_Name], [Current_Value], [Proposed_Value], [Change_Type], [Display_Order]
FROM [clubexpress].[chapter_finder_update_request_fields]
WHERE [Chapter_Finder_Update_Request_ID] = ?
ORDER BY [Display_Order], [Field_Name];
""",
        (request_id,),
    )
    return {"request": request_row, "fields": fields}


def approve_request(adapter: Any, request_id: int, reviewer: str, notes: str | None) -> dict[str, Any]:
    """Approve a submitted request and place it on the push queue."""
    rows = adapter.query_rows(
        """
EXEC [clubexpress].[sp_approve_chapter_finder_update_request]
    @ChapterFinderUpdateRequestID = ?,
    @ApprovedBy = ?,
    @ReviewNotes = ?;
""",
        (request_id, reviewer, limited_text(notes, 4000)),
    )
    if not rows:
        raise PortalError("The request could not be approved.", 409)
    return rows[0]


def reject_request(adapter: Any, request_id: int, reviewer: str, notes: str | None) -> None:
    """Reject a submitted or pending request."""
    adapter.execute_statements(
        [
            (
                """
EXEC [clubexpress].[sp_reject_chapter_finder_update_request]
    @ChapterFinderUpdateRequestID = ?,
    @RejectedBy = ?,
    @ReviewNotes = ?;
""",
                (request_id, reviewer, limited_text(notes, 4000)),
            )
        ]
    )


def load_current_entry(
    adapter: Any,
    chapter_finder_id: int | None,
    clubexpress_chapter_id: int | None,
) -> dict[str, Any]:
    """Load one current public chapter entry."""
    if chapter_finder_id is None and clubexpress_chapter_id is None:
        raise PortalError("A chapter identifier is required.")
    where = "[Chapter_Finder_ID] = ?"
    value = chapter_finder_id
    if clubexpress_chapter_id is not None:
        where = "[ClubExpress_Chapter_ID] = ?"
        value = clubexpress_chapter_id
    rows = adapter.query_rows(
        f"""
SELECT TOP (1) *
FROM [clubexpress].[chapter_finder_entries]
WHERE [Club_ID] = ? AND [Is_Current] = 1 AND {where};
""",
        (CLUB_ID, value),
    )
    if not rows:
        raise PortalError("Current chapter entry was not found.", 404)
    return rows[0]


def load_current_administrators(adapter: Any, entry_id: int) -> list[dict[str, Any]]:
    """Load current private administrator assignments for one chapter."""
    return adapter.query_rows(
        """
SELECT
    [Chapter_Finder_Administrator_Assignment_ID], [Administrator_Display_Name],
    [Membership_AGAID], [Member_Match_Status]
FROM [clubexpress].[chapter_finder_administrator_assignments]
WHERE [Chapter_Finder_Entry_ID] = ? AND [Is_Current] = 1
ORDER BY [Administrator_Display_Name], [Chapter_Finder_Administrator_Assignment_ID];
""",
        (entry_id,),
    )


def load_member(adapter: Any, agaid: int) -> dict[str, Any] | None:
    """Load one member by AGA ID."""
    rows = adapter.query_rows(
        """
SELECT TOP (1) [AGAID], [FirstName], [MiddleInitial], [LastName], [Status]
FROM [membership].[members]
WHERE [AGAID] = ?;
""",
        (agaid,),
    )
    return rows[0] if rows else None


def resolve_proposed_administrators(
    adapter: Any,
    current_rows: list[dict[str, Any]],
    retained_assignment_ids: list[int],
    added_agaids: list[int],
) -> list[dict[str, Any]]:
    """Build the desired administrator list without accepting client-supplied names."""
    current_by_id = {
        int(row["Chapter_Finder_Administrator_Assignment_ID"]): row
        for row in current_rows
    }
    unknown_retained = sorted(set(retained_assignment_ids) - set(current_by_id))
    if unknown_retained:
        raise PortalError("The administrator list is stale. Reload before submitting.", 409)
    result: list[dict[str, Any]] = []
    seen_agaids: set[int] = set()
    for assignment_id in retained_assignment_ids:
        row = current_by_id[assignment_id]
        agaid = nullable_int(row.get("Membership_AGAID"))
        if agaid is not None and agaid in seen_agaids:
            continue
        if agaid is not None:
            seen_agaids.add(agaid)
        result.append(
            {
                "membership_agaid": agaid,
                "display_name": str(row["Administrator_Display_Name"]),
            }
        )
    for agaid in added_agaids:
        if agaid in seen_agaids:
            continue
        member = load_member(adapter, agaid)
        if member is None:
            raise PortalError(f"AGA ID {agaid} was not found.")
        status = normalize_text(member.get("Status"))
        if status and status.casefold() != "active":
            raise PortalError(f"AGA ID {agaid} is not currently active.")
        seen_agaids.add(agaid)
        result.append(
            {
                "membership_agaid": agaid,
                "display_name": member_display_name(member),
            }
        )
    return canonical_administrators(result)


def canonical_administrators(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize and sort an administrator list for hashing and comparison."""
    canonical = [
        {
            "membership_agaid": nullable_int(item.get("Membership_AGAID", item.get("membership_agaid"))),
            "display_name": normalize_text(
                item.get("Administrator_Display_Name", item.get("display_name"))
            ) or "",
        }
        for item in values
    ]
    return sorted(
        canonical,
        key=lambda item: (
            item["display_name"].casefold(),
            item["membership_agaid"] if item["membership_agaid"] is not None else 2_147_483_647,
        ),
    )


def current_values_from_row(row: dict[str, Any]) -> dict[str, str | None]:
    """Return current editable public fields."""
    return {
        json_name: normalize_text(row.get(sql_name))
        for sql_name, json_name in SQL_TO_JSON_FIELD.items()
    }


def build_normalized_proposed(
    current: dict[str, str | None],
    proposed_input: dict[str, Any],
) -> dict[str, str | None]:
    """Build a complete normalized public proposal."""
    unknown = sorted(set(proposed_input) - set(EDITABLE_FIELDS))
    if unknown:
        raise PortalError(f"Unknown form fields: {', '.join(unknown)}")
    values = dict(current)
    for field in EDITABLE_FIELDS:
        if field in proposed_input and field not in {"postal_text", "website_label"}:
            values[field] = normalize_text(proposed_input[field])
    values["postal_text"] = format_postal_line(
        values.get("city"),
        values.get("state_province"),
        values.get("postal_code"),
    )
    values["website_label"] = current.get("website_label")
    return values


def validate_public_fields(values: dict[str, str | None]) -> None:
    """Validate normalized public chapter fields."""
    if not values.get("chapter_name"):
        raise PortalError("Chapter name is required.")
    for field, maximum in FIELD_LIMITS.items():
        value = values.get(field)
        if value and len(value) > maximum:
            raise PortalError(f"{field.replace('_', ' ').title()} is too long.")
    address = values.get("address_text")
    if address:
        address_lines = [line.strip() for line in address.splitlines() if line.strip()]
        if len(address_lines) > 2 or any(len(line) > 50 for line in address_lines):
            raise PortalError("Street or meeting address must use at most two lines of 50 characters each.")
    email = values.get("contact_email")
    if email and not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email):
        raise PortalError("Contact email is not valid.")
    website = values.get("website_url")
    if website and not re.match(r"^https?://", website, flags=re.IGNORECASE):
        raise PortalError("Website URL must start with http:// or https://.")


def format_postal_line(city: Any, state_province: Any, postal_code: Any) -> str | None:
    """Build the public location line from ClubExpress address controls."""
    city_text = normalize_text(city)
    state_text = normalize_text(state_province)
    postal_text = normalize_text(postal_code)
    if not any((city_text, state_text, postal_text)):
        return None
    locality = city_text or ""
    if state_text:
        locality = f"{locality}, {state_text}" if locality else state_text
    if postal_text:
        locality = f"{locality} {postal_text}" if locality else postal_text
    return locality


def build_field_diffs(
    current: dict[str, str | None],
    proposed: dict[str, str | None],
) -> list[dict[str, Any]]:
    """Build reviewable public field differences."""
    diffs: list[dict[str, Any]] = []
    for index, field in enumerate(EDITABLE_FIELDS, start=1):
        current_value = current.get(field)
        proposed_value = proposed.get(field)
        if current_value == proposed_value:
            continue
        change_type = "changed"
        if current_value is None:
            change_type = "added"
        elif proposed_value is None:
            change_type = "removed"
        diffs.append(
            {
                "field_name": field,
                "current_value": current_value,
                "proposed_value": proposed_value,
                "change_type": change_type,
                "display_order": index,
            }
        )
    return diffs


def build_public_content_payload(entry: dict[str, Any], editable: dict[str, Any]) -> dict[str, Any]:
    """Build the normalized public content payload used by the existing sync hash."""
    return {
        "point": {
            "chapter_finder_id": entry["Chapter_Finder_ID"],
            "clubexpress_chapter_id": entry.get("ClubExpress_Chapter_ID"),
            "marker_name": entry.get("Marker_Name"),
            "chapter_name": entry["Chapter_Name"],
            "latitude": str(entry["Latitude"]) if entry.get("Latitude") is not None else None,
            "longitude": str(entry["Longitude"]) if entry.get("Longitude") is not None else None,
            "map_icon": entry.get("Map_Icon"),
            "on_clubexpress": bool(entry.get("On_ClubExpress")),
        },
        "name": editable["chapter_name"],
        "address_text": editable["address_text"],
        "postal_text": editable["postal_text"],
        "city": editable["city"],
        "state_province": editable["state_province"],
        "postal_code": editable["postal_code"],
        "contact_name": editable["contact_name"],
        "contact_email": editable["contact_email"],
        "contact_phone": editable["contact_phone"],
        "website_url": editable["website_url"],
        "website_label": editable["website_label"],
        "description_text": editable["description_text"],
        "organization_parent": entry.get("Organization_Parent"),
        "org_chart_url": entry.get("Org_Chart_Url"),
    }


def parse_positive_int_list(value: Any, label: str) -> list[int]:
    """Validate and deduplicate a short integer list."""
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > 50:
        raise PortalError(f"Invalid {label} list.")
    result: list[int] = []
    for item in value:
        try:
            number = int(item)
        except (TypeError, ValueError) as exc:
            raise PortalError(f"Invalid {label} value.") from exc
        if number <= 0:
            raise PortalError(f"Invalid {label} value.")
        if number not in result:
            result.append(number)
    return result


def member_display_name(member: dict[str, Any]) -> str:
    """Build a member display name from normalized membership columns."""
    parts = [
        normalize_text(member.get("FirstName")),
        normalize_text(member.get("MiddleInitial")),
        normalize_text(member.get("LastName")),
    ]
    display_name = " ".join(part for part in parts if part)
    if not display_name:
        raise PortalError("The selected member does not have a usable name.")
    return display_name


def format_administrator_list(values: list[dict[str, Any]]) -> str:
    """Format an administrator list for field-level review."""
    if not values:
        return "None"
    return ", ".join(
        f"{item['display_name']} (AGA {item['membership_agaid']})"
        if item.get("membership_agaid") is not None
        else item["display_name"]
        for item in values
    )


def require_text(value: Any, label: str, maximum: int) -> str:
    """Require a normalized text value."""
    text = limited_text(value, maximum)
    if not text:
        raise PortalError(f"{label} is required.")
    return text


def require_email(value: Any) -> str:
    """Require a valid submitter email."""
    email = require_text(value, "Your email", 320)
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email):
        raise PortalError("Your email is not valid.")
    return email


def limited_text(value: Any, maximum: int) -> str | None:
    """Normalize text and enforce a maximum length."""
    text = normalize_text(value)
    if text and len(text) > maximum:
        raise PortalError("A submitted value is too long.")
    return text


def normalize_text(value: Any) -> str | None:
    """Normalize line endings and whitespace without destroying paragraphs."""
    if value is None:
        return None
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    lines = [re.sub(r"[ \t\f\v]+", " ", line).strip() for line in text.split("\n")]
    normalized = "\n".join(lines).strip()
    return normalized or None


def sha256_json(value: Any) -> str:
    """Hash stable JSON."""
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=json_safe).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def nullable_int(value: Any) -> int | None:
    """Convert a nullable value to int."""
    return None if value is None else int(value)


def json_safe(value: Any) -> Any:
    """Convert SQL/Python values for JSON responses."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value
