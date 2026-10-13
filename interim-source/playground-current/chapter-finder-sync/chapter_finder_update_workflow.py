# Copyright 2026, American Go Association, All rights reserved

"""Create and review SQL-based Chapter Finder update requests."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bayrate import sql_adapter as bayrate_sql  # noqa: E402
from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string  # noqa: E402
from chapter_finder_clubexpress_push import PushPlanError, build_push_plan  # noqa: E402


DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parent / "sql" / "chapter_finder_update_workflow_schema.sql"

EDITABLE_FIELDS = [
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
]

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

JSON_TO_SQL_FIELD = {value: key for key, value in SQL_TO_JSON_FIELD.items()}


def main(argv: list[str] | None = None) -> int:
    """Run the workflow helper."""
    parser = build_parser()
    args = parser.parse_args(argv)

    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    adapter = SqlAdapter(conn_str)
    if args.command == "apply-schema":
        batch_count = apply_schema(adapter, Path(args.schema_file).resolve())
        print(json.dumps({"applied_schema_batches": batch_count}, indent=2))
        return 0
    if args.command == "propose":
        result = create_update_request(adapter, args)
        print(json.dumps(result, default=json_safe, indent=2))
        return 0
    if args.command == "approve":
        result = approve_request(adapter, args.request_id, args.approved_by, args.review_notes)
        print(json.dumps(result, default=json_safe, indent=2))
        return 0
    if args.command == "reject":
        reject_request(adapter, args.request_id, args.rejected_by, args.review_notes)
        print(json.dumps({"rejected_request_id": args.request_id}, indent=2))
        return 0
    if args.command == "queue":
        rows = adapter.query_rows(
            """
SELECT *
FROM [clubexpress].[v_chapter_finder_update_push_queue]
ORDER BY [Approved_At], [Chapter_Finder_Update_Request_ID];
"""
        )
        print(json.dumps(rows, default=json_safe, indent=2))
        return 0
    if args.command == "show":
        rows = adapter.query_rows(
            """
SELECT *
FROM [clubexpress].[v_chapter_finder_update_review]
WHERE [Chapter_Finder_Update_Request_ID] = ?
ORDER BY [Chapter_Finder_Update_Request_ID];
""",
            (args.request_id,),
        )
        fields = adapter.query_rows(
            """
SELECT [Field_Name], [Current_Value], [Proposed_Value], [Change_Type], [Display_Order]
FROM [clubexpress].[chapter_finder_update_request_fields]
WHERE [Chapter_Finder_Update_Request_ID] = ?
ORDER BY [Display_Order], [Field_Name];
""",
            (args.request_id,),
        )
        print(json.dumps({"request": rows, "fields": fields}, default=json_safe, indent=2))
        return 0
    if args.command == "plan-push":
        result = load_pending_push_plan(adapter, args.request_id)
        print(json.dumps(result, default=json_safe, indent=2))
        return 0
    if args.command == "start-push":
        plan = load_pending_push_plan(adapter, args.request_id)
        rows = start_push_attempt(adapter, args.request_id, args.operator)
        print(json.dumps({"attempt": rows, "clubexpress_push_plan": plan}, default=json_safe, indent=2))
        return 0
    if args.command == "complete-push":
        result_json = None
        if args.result_json:
            result_json = json.dumps(load_json_object(Path(args.result_json)), ensure_ascii=False, sort_keys=True)
        complete_push_attempt(
            adapter,
            args.attempt_id,
            succeeded=args.succeeded,
            result_json=result_json,
            error_message=args.error_message,
        )
        print(json.dumps({"completed_push_attempt_id": args.attempt_id, "succeeded": args.succeeded}, indent=2))
        return 0

    parser.error(f"Unhandled command: {args.command}")
    return 2


def build_parser() -> argparse.ArgumentParser:
    """Build command line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    apply_parser = subparsers.add_parser("apply-schema", help="Apply the SQL update workflow schema.")
    apply_parser.add_argument("--schema-file", default=str(DEFAULT_SCHEMA_PATH), help="SQL schema file to apply.")

    propose_parser = subparsers.add_parser("propose", help="Create a submitted SQL update request.")
    selector = propose_parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--chapter-finder-id", type=int, help="Chapter Finder marker/detail id.")
    selector.add_argument("--clubexpress-chapter-id", type=int, help="ClubExpress chapter id.")
    propose_parser.add_argument("--proposed-json", required=True, help="Path to a JSON object containing proposed editable fields.")
    propose_parser.add_argument("--submitted-by-name", help="Submitter name.")
    propose_parser.add_argument("--submitted-by-email", help="Submitter email.")
    propose_parser.add_argument("--submitted-by-role", help="Submitter role.")
    propose_parser.add_argument("--submitter-notes", help="Notes from the submitter.")
    propose_parser.add_argument("--request-source", default="chapter_update_form", help="Source label for this request.")
    propose_parser.add_argument("--dry-run", action="store_true", help="Validate and show the diff without inserting SQL rows.")

    approve_parser = subparsers.add_parser("approve", help="Approve a request and put it on the push queue.")
    approve_parser.add_argument("request_id", type=int)
    approve_parser.add_argument("--approved-by", required=True)
    approve_parser.add_argument("--review-notes")

    reject_parser = subparsers.add_parser("reject", help="Reject a request.")
    reject_parser.add_argument("request_id", type=int)
    reject_parser.add_argument("--rejected-by", required=True)
    reject_parser.add_argument("--review-notes")

    subparsers.add_parser("queue", help="Show approved requests pending ClubExpress push.")

    show_parser = subparsers.add_parser("show", help="Show a request and its field diffs.")
    show_parser.add_argument("request_id", type=int)

    plan_push_parser = subparsers.add_parser("plan-push", help="Validate and show browser edits without claiming the request.")
    plan_push_parser.add_argument("request_id", type=int)

    start_push_parser = subparsers.add_parser("start-push", help="Claim a pending request immediately before ClubExpress editing.")
    start_push_parser.add_argument("request_id", type=int)
    start_push_parser.add_argument("--operator")

    complete_push_parser = subparsers.add_parser("complete-push", help="Complete a claimed ClubExpress push attempt.")
    complete_push_parser.add_argument("attempt_id", type=int)
    outcome = complete_push_parser.add_mutually_exclusive_group(required=True)
    outcome.add_argument("--succeeded", action="store_true")
    outcome.add_argument("--failed", action="store_true")
    complete_push_parser.add_argument("--result-json", help="Path to a JSON result object.")
    complete_push_parser.add_argument("--error-message")

    return parser


def create_update_request(adapter: SqlAdapter, args: argparse.Namespace) -> dict[str, Any]:
    """Create one update request from proposed JSON."""
    current = load_current_entry(adapter, args.chapter_finder_id, args.clubexpress_chapter_id)
    proposed_input = load_json_object(Path(args.proposed_json))
    proposed = build_normalized_proposed(current, proposed_input)
    current_payload = build_public_content_payload(current, current_values_from_row(current))
    proposed_payload = build_public_content_payload(current, proposed)
    field_diffs = build_field_diffs(current_values_from_row(current), proposed)

    if not field_diffs:
        raise ValueError("The proposed JSON does not change any editable fields.")
    try:
        build_push_plan(
            chapter_finder_id=int(current["Chapter_Finder_ID"]),
            normalized_payload=proposed,
            field_diffs=field_diffs,
        )
    except PushPlanError as exc:
        raise ValueError(str(exc)) from exc

    base_hash = str(current["Content_Sha256"])
    computed_base_hash = sha256_json(current_payload)
    if computed_base_hash.lower() != base_hash.lower():
        # Keep the stored public hash as authority, but expose the mismatch for later investigation.
        base_hash = str(current["Content_Sha256"])

    proposed_hash = sha256_json(proposed_payload)
    if args.dry_run:
        return {
            "dry_run": True,
            "chapter_finder_id": current["Chapter_Finder_ID"],
            "clubexpress_chapter_id": current["ClubExpress_Chapter_ID"],
            "chapter_name": current["Chapter_Name"],
            "base_content_sha256": base_hash,
            "proposed_content_sha256": proposed_hash,
            "changed_field_count": len(field_diffs),
            "field_diffs": field_diffs,
            "normalized_proposed": proposed,
        }

    proposed_json = json.dumps(proposed_input, ensure_ascii=False, sort_keys=True)
    normalized_json = json.dumps(proposed, ensure_ascii=False, sort_keys=True)
    diff_json = json.dumps(field_diffs, ensure_ascii=False, sort_keys=True)

    rows = adapter.query_rows(
        """
INSERT INTO [clubexpress].[chapter_finder_update_requests]
(
    [Chapter_Finder_Entry_ID],
    [Club_ID],
    [Chapter_Finder_ID],
    [ClubExpress_Chapter_ID],
    [Chapter_Name_At_Submission],
    [Request_Source],
    [Status],
    [Submitted_By_Name],
    [Submitted_By_Email],
    [Submitted_By_Role],
    [Submitter_Notes],
    [Base_Content_Sha256],
    [Proposed_Content_Sha256],
    [Proposed_Json],
    [Normalized_Proposed_Json],
    [Field_Diff_Json],
    [Changed_Field_Count]
)
OUTPUT inserted.[Chapter_Finder_Update_Request_ID], inserted.[Request_Key]
VALUES (?, ?, ?, ?, ?, ?, N'submitted', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
""",
        (
            current["Chapter_Finder_Entry_ID"],
            current["Club_ID"],
            current["Chapter_Finder_ID"],
            current["ClubExpress_Chapter_ID"],
            current["Chapter_Name"],
            args.request_source,
            args.submitted_by_name,
            args.submitted_by_email,
            args.submitted_by_role,
            args.submitter_notes,
            base_hash,
            proposed_hash,
            proposed_json,
            normalized_json,
            diff_json,
            len(field_diffs),
        ),
    )
    request_id = int(rows[0]["Chapter_Finder_Update_Request_ID"])
    request_key = rows[0]["Request_Key"]
    insert_field_diffs(adapter, request_id, field_diffs)
    return {
        "request_id": request_id,
        "request_key": str(request_key),
        "chapter_finder_id": current["Chapter_Finder_ID"],
        "clubexpress_chapter_id": current["ClubExpress_Chapter_ID"],
        "chapter_name": current["Chapter_Name"],
        "changed_field_count": len(field_diffs),
        "field_diffs": field_diffs,
        "status": "submitted",
    }


def load_current_entry(adapter: SqlAdapter, chapter_finder_id: int | None, clubexpress_chapter_id: int | None) -> dict[str, Any]:
    """Load one current Chapter Finder entry."""
    where_clause = "[Chapter_Finder_ID] = ?"
    params: tuple[Any, ...] = (chapter_finder_id,)
    if clubexpress_chapter_id is not None:
        where_clause = "[ClubExpress_Chapter_ID] = ?"
        params = (clubexpress_chapter_id,)
    rows = adapter.query_rows(
        f"""
SELECT TOP 1 *
FROM [clubexpress].[chapter_finder_entries]
WHERE [Is_Current] = 1
  AND {where_clause}
ORDER BY [Chapter_Finder_Entry_ID];
""",
        params,
    )
    if not rows:
        raise ValueError("Current Chapter Finder entry was not found.")
    return rows[0]


def current_values_from_row(row: dict[str, Any]) -> dict[str, Any]:
    """Extract editable values from a current SQL row."""
    return {json_name: normalize_value(row.get(sql_name)) for sql_name, json_name in SQL_TO_JSON_FIELD.items()}


def build_normalized_proposed(current: dict[str, Any], proposed_input: dict[str, Any]) -> dict[str, Any]:
    """Build full normalized proposed payload from partial input."""
    unknown = sorted(set(proposed_input) - set(EDITABLE_FIELDS))
    if unknown:
        raise ValueError(f"Unknown proposed field(s): {', '.join(unknown)}")
    values = current_values_from_row(current)
    for field in EDITABLE_FIELDS:
        if field in proposed_input and field not in {"postal_text", "website_label"}:
            values[field] = normalize_value(proposed_input[field])
    values["postal_text"] = format_postal_line(
        values.get("city"),
        values.get("state_province"),
        values.get("postal_code"),
    )
    values["website_label"] = current_values_from_row(current).get("website_label")
    return values


def build_public_content_payload(current: dict[str, Any], editable: dict[str, Any]) -> dict[str, Any]:
    """Build the normalized payload used for public-content hashing."""
    return {
        "point": {
            "chapter_finder_id": current["Chapter_Finder_ID"],
            "clubexpress_chapter_id": current["ClubExpress_Chapter_ID"],
            "marker_name": current["Marker_Name"],
            "chapter_name": current["Chapter_Name"],
            "latitude": str(current["Latitude"]) if current["Latitude"] is not None else None,
            "longitude": str(current["Longitude"]) if current["Longitude"] is not None else None,
            "map_icon": current["Map_Icon"],
            "on_clubexpress": bool(current["On_ClubExpress"]),
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
        "organization_parent": current["Organization_Parent"],
        "org_chart_url": current["Org_Chart_Url"],
    }


def build_field_diffs(current: dict[str, Any], proposed: dict[str, Any]) -> list[dict[str, Any]]:
    """Build field-level diff rows."""
    diffs: list[dict[str, Any]] = []
    for index, field in enumerate(EDITABLE_FIELDS, start=1):
        current_value = normalize_value(current.get(field))
        proposed_value = normalize_value(proposed.get(field))
        if current_value == proposed_value:
            continue
        if current_value is None:
            change_type = "added"
        elif proposed_value is None:
            change_type = "removed"
        else:
            change_type = "changed"
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


def insert_field_diffs(adapter: SqlAdapter, request_id: int, field_diffs: list[dict[str, Any]]) -> None:
    """Insert field diff rows."""
    statements = []
    for diff in field_diffs:
        statements.append(
            (
                """
INSERT INTO [clubexpress].[chapter_finder_update_request_fields]
(
    [Chapter_Finder_Update_Request_ID],
    [Field_Name],
    [Current_Value],
    [Proposed_Value],
    [Change_Type],
    [Display_Order]
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
    adapter.execute_statements(statements)


def approve_request(adapter: SqlAdapter, request_id: int, approved_by: str, review_notes: str | None) -> list[dict[str, Any]]:
    """Approve a request and return its push-queue row."""
    return adapter.query_rows(
        """
EXEC [clubexpress].[sp_approve_chapter_finder_update_request]
    @ChapterFinderUpdateRequestID = ?,
    @ApprovedBy = ?,
    @ReviewNotes = ?;
""",
        (request_id, approved_by, review_notes),
    )


def reject_request(adapter: SqlAdapter, request_id: int, rejected_by: str, review_notes: str | None) -> None:
    """Reject a request."""
    adapter.execute_statements(
        [
            (
                """
EXEC [clubexpress].[sp_reject_chapter_finder_update_request]
    @ChapterFinderUpdateRequestID = ?,
    @RejectedBy = ?,
    @ReviewNotes = ?;
""",
                (request_id, rejected_by, review_notes),
            )
        ]
    )


def start_push_attempt(adapter: SqlAdapter, request_id: int, operator: str | None) -> list[dict[str, Any]]:
    """Claim one approved request and return its immutable push payload."""
    return adapter.query_rows(
        """
EXEC [clubexpress].[sp_start_chapter_finder_update_push_attempt]
    @ChapterFinderUpdateRequestID = ?,
    @Operator = ?;
""",
        (request_id, operator),
    )


def load_pending_push_plan(adapter: SqlAdapter, request_id: int) -> dict[str, Any]:
    """Validate current SQL state and build a non-mutating ClubExpress push plan."""
    rows = adapter.query_rows(
        """
SELECT
    queue.*,
    request.[Base_Administrator_Sha256],
    request.[Proposed_Administrator_Sha256]
FROM [clubexpress].[v_chapter_finder_update_push_queue] AS queue
INNER JOIN [clubexpress].[chapter_finder_update_requests] AS request
    ON request.[Chapter_Finder_Update_Request_ID] = queue.[Chapter_Finder_Update_Request_ID]
WHERE queue.[Chapter_Finder_Update_Request_ID] = ?;
""",
        (request_id,),
    )
    if not rows:
        raise ValueError("Chapter Finder update request is not pending push.")
    row = rows[0]
    current_hash = str(row.get("Current_Public_Content_Sha256") or "").lower()
    base_hash = str(row.get("Base_Content_Sha256") or "").lower()
    if current_hash and base_hash and current_hash != base_hash:
        raise ValueError("The public Chapter Finder listing changed after submission; re-review the request before pushing.")

    base_admin_hash = str(row.get("Base_Administrator_Sha256") or "").lower()
    if base_admin_hash:
        current_admin_hash = load_current_administrator_hash(adapter, int(row["Chapter_Finder_Entry_ID"]))
        if current_admin_hash != base_admin_hash:
            raise ValueError("The Chapter Finder administrator list changed after submission; re-review the request before pushing.")

    normalized_payload = parse_json_value(row.get("Normalized_Proposed_Json"), "normalized proposed payload", dict)
    field_diffs = parse_json_value(row.get("Field_Diff_Json"), "field diff payload", list)
    try:
        return build_push_plan(
            request_id=request_id,
            chapter_finder_id=int(row["Chapter_Finder_ID"]),
            normalized_payload=normalized_payload,
            field_diffs=field_diffs,
        )
    except PushPlanError as exc:
        raise ValueError(str(exc)) from exc


def load_current_administrator_hash(adapter: SqlAdapter, entry_id: int) -> str:
    """Hash the current private administrator list using the portal's canonical shape."""
    rows = adapter.query_rows(
        """
SELECT [Administrator_Display_Name], [Membership_AGAID]
FROM [clubexpress].[chapter_finder_administrator_assignments]
WHERE [Chapter_Finder_Entry_ID] = ?
  AND [Is_Current] = 1;
""",
        (entry_id,),
    )
    values = []
    for row in rows:
        display_name = " ".join(str(row.get("Administrator_Display_Name") or "").split())
        if not display_name:
            continue
        agaid = row.get("Membership_AGAID")
        values.append({"display_name": display_name, "membership_agaid": int(agaid) if agaid is not None else None})
    values.sort(
        key=lambda item: (
            item["display_name"].casefold(),
            item["membership_agaid"] if item["membership_agaid"] is not None else 2_147_483_647,
        )
    )
    return sha256_json(values)


def parse_json_value(value: Any, label: str, expected_type: type) -> Any:
    """Decode a SQL JSON column and enforce its expected top-level type."""
    decoded = json.loads(value) if isinstance(value, str) else value
    if not isinstance(decoded, expected_type):
        raise ValueError(f"The {label} is invalid.")
    return decoded


def complete_push_attempt(
    adapter: SqlAdapter,
    attempt_id: int,
    *,
    succeeded: bool,
    result_json: str | None,
    error_message: str | None,
) -> None:
    """Record the outcome of one authorized ClubExpress browser push."""
    adapter.execute_statements(
        [
            (
                """
EXEC [clubexpress].[sp_complete_chapter_finder_update_push_attempt]
    @ChapterFinderUpdatePushAttemptID = ?,
    @Succeeded = ?,
    @ResultJson = ?,
    @ErrorMessage = ?;
""",
                (attempt_id, succeeded, result_json, error_message),
            )
        ]
    )


def load_json_object(path: Path) -> dict[str, Any]:
    """Load a JSON object from disk."""
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError("Proposed JSON must be an object.")
    return value


def normalize_value(value: Any) -> str | None:
    """Normalize form values for comparison/storage."""
    if value is None:
        return None
    text = str(value).replace("\r\n", "\n").replace("\r", "\n")
    lines = [re.sub(r"[ \t\f\v]+", " ", line).strip() for line in text.split("\n")]
    normalized = "\n".join(line for line in lines if line)
    return normalized or None


def parse_postal_line(postal_text: str | None) -> tuple[str | None, str | None, str | None]:
    """Parse common US postal line text."""
    if not postal_text:
        return None, None, None
    line = postal_text.splitlines()[0].strip().strip(",")
    match = re.match(
        r"^(?P<city>.*?),\s*(?P<state>[A-Z]{2})(?:\s+(?P<postal>[A-Za-z0-9][A-Za-z0-9 -]*))?$",
        line,
    )
    if not match:
        return None, None, None
    postal_code = (match.group("postal") or "").strip().strip(",")
    return match.group("city").strip(), match.group("state").strip(), postal_code or None


def format_postal_line(city: Any, state_province: Any, postal_code: Any) -> str | None:
    """Build the public location line from ClubExpress address controls."""
    city_text = normalize_value(city)
    state_text = normalize_value(state_province)
    postal_text = normalize_value(postal_code)
    if not any((city_text, state_text, postal_text)):
        return None
    locality = city_text or ""
    if state_text:
        locality = f"{locality}, {state_text}" if locality else state_text
    if postal_text:
        locality = f"{locality} {postal_text}" if locality else postal_text
    return locality


def sha256_json(payload: Any) -> str:
    """Hash a normalized JSON payload."""
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=json_safe).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def apply_schema(adapter: SqlAdapter, schema_path: Path) -> int:
    """Apply a SQL Server schema file that uses GO batch separators."""
    text = schema_path.read_text(encoding="utf-8-sig")
    batches = [
        batch.strip()
        for batch in re.split(r"(?im)^\s*GO\s*;?\s*$", text)
        if batch.strip()
    ]
    for batch in batches:
        if requires_autocommit_schema_batch(batch):
            execute_autocommit_statement(adapter.connection_string, batch)
        else:
            adapter.execute_statements([(batch, ())])
    return len(batches)


def requires_autocommit_schema_batch(batch: str) -> bool:
    """Return whether a schema batch must run outside a user transaction."""
    return "CREATE FULLTEXT" in batch.upper()


def execute_autocommit_statement(connection_string: str, query: str) -> None:
    """Execute one SQL statement in autocommit mode."""
    if bayrate_sql.pyodbc is not None:
        try:
            conn = bayrate_sql.pyodbc.connect(connection_string, autocommit=True)
            try:
                cursor = conn.cursor()
                try:
                    cursor.execute(query)
                finally:
                    cursor.close()
            finally:
                conn.close()
            return
        except Exception:
            if bayrate_sql.pytds is None:
                raise
    if bayrate_sql.pytds is None:
        raise RuntimeError("Neither pyodbc nor python-tds is available for SQL access.")
    sql = bayrate_sql._parse_sql_connection_string(connection_string)
    kwargs = {
        "server": sql["server"],
        "port": sql["port"],
        "database": sql["database"],
        "user": sql["user"],
        "password": sql["password"],
        "validate_host": True,
        "enc_login_only": False,
        "autocommit": True,
        "timeout": 60,
        "as_dict": True,
    }
    if bayrate_sql.certifi is not None:
        kwargs["cafile"] = bayrate_sql.certifi.where()
    conn = bayrate_sql.pytds.connect(**kwargs)
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
        finally:
            cursor.close()
    finally:
        conn.close()


def json_safe(value: Any) -> Any:
    """Return JSON-safe values."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


if __name__ == "__main__":
    raise SystemExit(main())
