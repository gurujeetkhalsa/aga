import base64
import csv
import html
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from email import message_from_bytes, policy
from email.parser import BytesParser
from io import StringIO
from typing import Iterable, Optional
from urllib import error, parse, request

import azure.functions as func
import pyodbc

app = func.FunctionApp()

GMAIL_API_BASE_URL = "https://gmail.googleapis.com/gmail/v1"
GMAIL_TOKEN_URL = "https://oauth2.googleapis.com/token"
DEFAULT_MAILBOX_POLL_SCHEDULE = "0 */5 * * * *"
DEFAULT_MYSQL_SYNC_SCHEDULE = "0 30 2 * * *"
NIGHTLY_MESSAGE_TYPE = "nightly_memchap_csv"
NIGHTLY_CATEGORY_MESSAGE_TYPE = "nightly_member_categories_csv"
NEW_MEMBER_MESSAGE_TYPE = "new_member_signup"
RENEWAL_MESSAGE_TYPE = "member_renewal"
IGNORE_MESSAGE_TYPE = "ignore"
MAX_MEMBER_AGAID = 50000
MYSQL_SYNC_RUN_TABLE_SQL = """
IF OBJECT_ID(N'integration.mysql_sync_runs', N'U') IS NOT NULL
BEGIN
    INSERT INTO integration.mysql_sync_runs
    (
        JobName,
        SourceTable,
        TargetTable,
        StartedAt,
        CompletedAt,
        Status,
        RowCount,
        ErrorMessage
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
END
"""
TDLIST_PLACEHOLDER_RATING = 0.0
TDLIST_PLACEHOLDER_SIGMA = 0.0
TDLIST_QUERY = """
SELECT
    m.[AGAID],
    m.[FirstName],
    m.[LastName],
    m.[MemberType],
    m.[ExpirationDate],
    m.[JoinDate],
    m.[State],
    c.[ChapterCode],
    c.[ChapterName]
FROM [membership].[members] AS m
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
WHERE m.[AGAID] < ?
ORDER BY m.[LastName], m.[FirstName], m.[AGAID]
"""

STAGING_COLUMNS = [
    "AGAID",
    "MemberType",
    "FirstName",
    "MiddleInitial",
    "LastName",
    "Nickname",
    "Pronouns",
    "LoginName",
    "Status",
    "LastLogin",
    "EmailAddress",
    "CellPhone",
    "PhoneNumber",
    "Address1",
    "Address2",
    "City",
    "State",
    "ZipCode",
    "Country",
    "DateOfBirth",
    "WorkTitle",
    "Gender",
    "JoinDate",
    "ExpirationDate",
    "LastRenewalDate",
    "ChapterID",
    "EmergencyContactName",
    "EmergencyContactRelationship",
    "EmergencyContactPhone",
    "EmergencyContactEmail",
]

INT_COLUMNS = {"AGAID", "ChapterID"}
DATE_COLUMNS = {"DateOfBirth", "JoinDate", "ExpirationDate", "LastRenewalDate"}
DATETIME_COLUMNS = {"LastLogin"}
OPTIONAL_SOURCE_COLUMNS = {"LastLogin"}
IGNORED_SOURCE_COLUMNS = {"memberdatecreated"}
STRING_COLUMNS = set(STAGING_COLUMNS) - INT_COLUMNS - DATE_COLUMNS - DATETIME_COLUMNS
EXPECTED_HEADER_LOOKUP = {re.sub(r"[^a-z0-9]+", "", column.lower()): column for column in STAGING_COLUMNS}
CATEGORY_COLUMNS = ["AGAID", "Category"]
CATEGORY_HEADER_LOOKUP = {"agaid": "AGAID", "category": "Category"}
MYSQL_SYNC_JOBS = {
    "tournaments": {
        "source_table": "tournaments",
        "target_table": "ratingsync.tournaments",
        "columns": (
            "Tournament_Code",
            "Tournament_Descr",
            "Tournament_Date",
            "City",
            "State_Code",
            "Country_Code",
            "Rounds",
            "Total_Players",
            "Wallist",
            "Elab_Date",
            "status",
        ),
    },
    "ratings": {
        "source_table": "ratings",
        "target_table": "ratingsync.ratings",
        "columns": (
            "Pin_Player",
            "Rating",
            "Sigma",
            "Elab_Date",
            "Tournament_Code",
            "id",
        ),
    },
    "games": {
        "source_table": "games",
        "target_table": "ratingsync.games",
        "columns": (
            "Game_ID",
            "Tournament_Code",
            "Game_Date",
            "Round",
            "Pin_Player_1",
            "Color_1",
            "Rank_1",
            "Pin_Player_2",
            "Color_2",
            "Rank_2",
            "Handicap",
            "Komi",
            "Result",
            "Sgf_Code",
            "Online",
            "Exclude",
            "Rated",
            "Elab_Date",
        ),
    },
}


class CsvValidationError(ValueError):
    pass


class EmailProcessingError(ValueError):
    pass


class GmailApiError(RuntimeError):
    pass


@app.route(route="import_memchap", methods=["POST"])
def import_memchap(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("MemChap import triggered")

    conn_str = _get_sql_connection_string()
    if not conn_str:
        return func.HttpResponse(
            "Missing SQL_CONNECTION_STRING application setting.",
            status_code=500,
        )

    try:
        csv_bytes = _extract_csv_bytes(req)
        rows_staged = _import_memchap_bytes(conn_str, csv_bytes)
        return func.HttpResponse(
            json.dumps({"status": "success", "rows_staged": rows_staged}),
            mimetype="application/json",
            status_code=200,
        )
    except CsvValidationError as exc:
        logging.warning("MemChap CSV validation failed: %s", exc)
        return func.HttpResponse(str(exc), status_code=400)
    except pyodbc.Error as exc:
        logging.exception("MemChap SQL execution failed")
        return func.HttpResponse(f"Import failed: {exc}", status_code=500)
    except Exception as exc:
        logging.exception("MemChap import failed")
        return func.HttpResponse(f"Import failed: {exc}", status_code=500)


@app.function_name(name="GenerateTDListA")
@app.route(route="GenerateTDListA", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def generate_tdlist_a(req: func.HttpRequest) -> func.HttpResponse:
    return _generate_tdlist_response("A")


@app.function_name(name="GenerateTDListB")
@app.route(route="GenerateTDListB", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def generate_tdlist_b(req: func.HttpRequest) -> func.HttpResponse:
    return _generate_tdlist_response("B")


@app.function_name(name="GenerateTDListN")
@app.route(route="GenerateTDListN", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def generate_tdlist_n(req: func.HttpRequest) -> func.HttpResponse:
    return _generate_tdlist_response("N")


@app.timer_trigger(schedule=DEFAULT_MAILBOX_POLL_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def poll_clubexpress_mailbox(timer: func.TimerRequest) -> None:
    if not _is_truthy(os.environ.get("CLUBEXPRESS_MAILBOX_ENABLED", "false")):
        logging.info("ClubExpress mailbox polling is disabled.")
        return

    try:
        access_token = _get_gmail_access_token()
        messages = _list_gmail_messages(access_token)
        logging.info("Fetched %s candidate Gmail messages", len(messages))

        for item in messages:
            message_id = item.get("id")
            if not message_id:
                continue
            try:
                message = _get_gmail_message(access_token, message_id)
                _process_mailbox_message(access_token, message)
            except Exception:
                logging.exception("Failed processing Gmail message %s", message_id)
    except Exception:
        logging.exception("ClubExpress Gmail poll failed")


@app.timer_trigger(schedule=DEFAULT_MYSQL_SYNC_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def sync_mysql_reference_data(timer: func.TimerRequest) -> None:
    if not _is_truthy(os.environ.get("MYSQL_SYNC_ENABLED", "false")):
        logging.info("MySQL sync is disabled.")
        return

    target_conn_str = _get_mysql_sync_sql_connection_string()
    if not target_conn_str:
        logging.error("MYSQL_SYNC_SQL_CONNECTION_STRING or AZURE_SQL_GAMES_CONNECTION_STRING is required.")
        return

    source_config = _get_mysql_source_config()
    selected_jobs = _get_selected_mysql_sync_jobs()
    logging.info("Starting MySQL sync for jobs=%s", ", ".join(selected_jobs))

    for job_name in selected_jobs:
        job = MYSQL_SYNC_JOBS[job_name]
        started_at = datetime.now(timezone.utc)
        completed_at = None
        row_count = None
        error_message = None
        status = "processed"

        try:
            rows = _fetch_mysql_rows(source_config, job["source_table"], job["columns"])
            _replace_sql_table(target_conn_str, job["target_table"], job["columns"], rows)
            row_count = len(rows)
            completed_at = datetime.now(timezone.utc)
            logging.info(
                "MySQL sync job %s completed. source_table=%s target_table=%s rows=%s",
                job_name,
                job["source_table"],
                job["target_table"],
                row_count,
            )
        except Exception as exc:
            status = "error"
            error_message = str(exc)
            completed_at = datetime.now(timezone.utc)
            logging.exception("MySQL sync job %s failed.", job_name)
            _record_mysql_sync_run(
                target_conn_str,
                job_name,
                job["source_table"],
                job["target_table"],
                started_at,
                completed_at,
                status,
                row_count,
                error_message,
            )
            raise

        _record_mysql_sync_run(
            target_conn_str,
            job_name,
            job["source_table"],
            job["target_table"],
            started_at,
            completed_at,
            status,
            row_count,
            error_message,
        )


def _process_mailbox_message(access_token: str, message: dict) -> None:
    sender = _get_header_value(message, "From")
    subject = _get_header_value(message, "Subject")
    attachments = _extract_gmail_attachments(access_token, message)
    message_type = _classify_message(sender, subject, attachments)
    received_at = _message_received_at(message)
    received_date = received_at.date()

    if message_type == IGNORE_MESSAGE_TYPE:
        logging.info("Ignoring Gmail message subject=%r sender=%r", subject, sender)
        _mark_gmail_message_processed(access_token, message)
        return

    archive_path = _archive_message_artifacts(message_type, message, attachments)
    conn_str = _get_sql_connection_string()
    if not conn_str:
        raise RuntimeError("Missing SQL_CONNECTION_STRING application setting.")

    if message_type == NIGHTLY_MESSAGE_TYPE:
        message_id = _message_identifier(message)
        rows_staged = None
        try:
            _execute_stored_procedure(
                conn_str,
                "membership.sp_log_clubexpress_email",
                {
                    "MessageId": message_id,
                    "MessageType": message_type,
                    "ReceivedAt": received_at,
                    "Sender": sender or None,
                    "Subject": subject or None,
                    "BlobPath": archive_path,
                    "Status": "received",
                    "ErrorMessage": None,
                },
            )
            rows_staged = _handle_memchap_email(conn_str, attachments)
            _execute_stored_procedure(
                conn_str,
                "membership.sp_log_clubexpress_email",
                {
                    "MessageId": message_id,
                    "MessageType": message_type,
                    "ReceivedAt": received_at,
                    "Sender": sender or None,
                    "Subject": subject or None,
                    "BlobPath": archive_path,
                    "Status": "processed",
                    "ErrorMessage": None,
                },
            )
        except Exception as exc:
            _execute_stored_procedure(
                conn_str,
                "membership.sp_log_clubexpress_email",
                {
                    "MessageId": message_id,
                    "MessageType": message_type,
                    "ReceivedAt": received_at,
                    "Sender": sender or None,
                    "Subject": subject or None,
                    "BlobPath": archive_path,
                    "Status": "error",
                    "ErrorMessage": str(exc),
                },
            )
            raise
        logging.info("Nightly MemChap message processed. rows_staged=%s archive_path=%s", rows_staged, archive_path)
    elif message_type == NIGHTLY_CATEGORY_MESSAGE_TYPE:
        message_id = _message_identifier(message)
        rows_staged = None
        try:
            _execute_stored_procedure(
                conn_str,
                "membership.sp_log_clubexpress_email",
                {
                    "MessageId": message_id,
                    "MessageType": message_type,
                    "ReceivedAt": received_at,
                    "Sender": sender or None,
                    "Subject": subject or None,
                    "BlobPath": archive_path,
                    "Status": "received",
                    "ErrorMessage": None,
                },
            )
            rows_staged = _handle_member_categories_email(conn_str, attachments)
            _execute_stored_procedure(
                conn_str,
                "membership.sp_log_clubexpress_email",
                {
                    "MessageId": message_id,
                    "MessageType": message_type,
                    "ReceivedAt": received_at,
                    "Sender": sender or None,
                    "Subject": subject or None,
                    "BlobPath": archive_path,
                    "Status": "processed",
                    "ErrorMessage": None,
                },
            )
        except Exception as exc:
            _execute_stored_procedure(
                conn_str,
                "membership.sp_log_clubexpress_email",
                {
                    "MessageId": message_id,
                    "MessageType": message_type,
                    "ReceivedAt": received_at,
                    "Sender": sender or None,
                    "Subject": subject or None,
                    "BlobPath": archive_path,
                    "Status": "error",
                    "ErrorMessage": str(exc),
                },
            )
            raise
        logging.info("Nightly category message processed. rows_staged=%s archive_path=%s", rows_staged, archive_path)
    elif message_type == NEW_MEMBER_MESSAGE_TYPE:
        parsed = _parse_new_member_email(_message_body_to_text(message))
        _execute_stored_procedure(
            conn_str,
            "membership.sp_process_new_member_email",
            {
                "MessageId": _message_identifier(message),
                "ReceivedAt": received_at,
                "AGAID": parsed["AGAID"],
                "MemberType": parsed["MemberType"],
                "FirstName": parsed["FirstName"],
                "LastName": parsed["LastName"],
                "EmailAddress": parsed.get("EmailAddress"),
                "JoinDate": received_date,
                "ExpirationDate": received_date + timedelta(days=365),
                "Sender": sender or None,
                "Subject": subject or None,
                "BlobPath": archive_path,
            },
        )
        logging.info("New member email processed for AGAID=%s archive_path=%s", parsed["AGAID"], archive_path)
    elif message_type == RENEWAL_MESSAGE_TYPE:
        parsed = _parse_renewal_email(_message_body_to_text(message))
        _execute_stored_procedure(
            conn_str,
            "membership.sp_process_membership_renewal",
            {
                "MessageId": _message_identifier(message),
                "ReceivedAt": received_at,
                "AGAID": parsed["AGAID"],
                "ExpirationDate": received_date + timedelta(days=365),
                "PhoneNumber": parsed.get("PhoneNumber"),
                "EmailAddress": parsed.get("EmailAddress"),
                "LoginName": parsed.get("LoginName"),
                "MemberType": parsed.get("MemberType"),
                "IsChapterMember": 1 if parsed["IsChapterMember"] else 0,
                "Sender": sender or None,
                "Subject": subject or None,
                "BlobPath": archive_path,
            },
        )
        logging.info("Renewal email processed for AGAID=%s archive_path=%s", parsed["AGAID"], archive_path)
    else:
        raise RuntimeError(f"Unsupported mailbox message type: {message_type}")

    _mark_gmail_message_processed(access_token, message)


def _generate_tdlist_response(list_type: str) -> func.HttpResponse:
    conn_str = _get_sql_connection_string()
    if not conn_str:
        return func.HttpResponse(
            "Missing SQL_CONNECTION_STRING application setting.",
            status_code=500,
        )

    try:
        rows = _fetch_tdlist_rows(conn_str)
        if list_type == "A":
            body = _render_tdlist_tab(rows, chapter_field="ChapterCode")
            filename = "TDListA.txt"
        elif list_type == "B":
            body = _render_tdlist_tab(rows, chapter_field="ChapterName")
            filename = "TDListB.txt"
        elif list_type == "N":
            body = _render_tdlist_fixed_width(rows)
            filename = "TDListN.txt"
        else:
            raise ValueError(f"Unsupported TDList type: {list_type}")

        return func.HttpResponse(
            body,
            status_code=200,
            mimetype="text/plain; charset=utf-8",
            headers={"Content-Disposition": f'inline; filename="{filename}"'},
        )
    except pyodbc.Error as exc:
        logging.exception("TDList %s SQL execution failed", list_type)
        return func.HttpResponse(f"TDList generation failed: {exc}", status_code=500)
    except Exception as exc:
        logging.exception("TDList %s generation failed", list_type)
        return func.HttpResponse(f"TDList generation failed: {exc}", status_code=500)


def _fetch_tdlist_rows(conn_str: str) -> list[dict[str, object]]:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(TDLIST_QUERY, MAX_MEMBER_AGAID)
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, record)) for record in cursor.fetchall()]
        cursor.close()
        return rows
    finally:
        conn.close()


def _render_tdlist_tab(rows: list[dict[str, object]], *, chapter_field: str) -> str:
    rendered_rows = []
    for row in rows:
        rendered_rows.append(
            "\t".join(
                [
                    _tdlist_name(row),
                    str(row["AGAID"] or ""),
                    _tdlist_text(row.get("MemberType")),
                    _format_tdlist_decimal(TDLIST_PLACEHOLDER_RATING, digits=5),
                    _format_tdlist_date(row.get("ExpirationDate")),
                    _tdlist_text(row.get(chapter_field)),
                    _tdlist_text(row.get("State")),
                    _format_tdlist_decimal(TDLIST_PLACEHOLDER_SIGMA, digits=5),
                    _format_tdlist_date(row.get("JoinDate")),
                ]
            )
        )
    return "\n".join(rendered_rows) + ("\n" if rendered_rows else "")


def _render_tdlist_fixed_width(rows: list[dict[str, object]]) -> str:
    rendered_rows = []
    for row in rows:
        chapter_code = _tdlist_text(row.get("ChapterCode")) or "none"
        rendered_rows.append(
            f"{_tdlist_name(row):<28}"
            f"{str(row['AGAID'] or ''):>6} "
            f"{_tdlist_text(row.get('MemberType')):<7} "
            f"{_format_tdlist_decimal(TDLIST_PLACEHOLDER_RATING, digits=1):>6} "
            f"{_format_tdlist_date(row.get('ExpirationDate')):>10} "
            f"{chapter_code:<4} "
            f"{_tdlist_text(row.get('State')):<2}"
        )
    return "\n".join(rendered_rows) + ("\n" if rendered_rows else "")


def _tdlist_name(row: dict[str, object]) -> str:
    last_name = _tdlist_text(row.get("LastName"))
    first_name = _tdlist_text(row.get("FirstName"))
    if last_name and first_name:
        return f"{last_name}, {first_name}"
    return last_name or first_name


def _tdlist_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _format_tdlist_decimal(value: float, *, digits: int) -> str:
    return f"{value:.{digits}f}"


def _format_tdlist_date(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return f"{value.month}/{value.day}/{value.year}"
    return str(value)


def _handle_memchap_email(conn_str: str, attachments: list[dict]) -> int:
    for attachment in attachments:
        content_bytes = attachment.get("contentBytes")
        if not content_bytes:
            continue
        if _detect_attachment_report_type(attachment.get("name", ""), content_bytes) != NIGHTLY_MESSAGE_TYPE:
            continue
        return _import_memchap_bytes(conn_str, content_bytes)

    raise EmailProcessingError("No MemChap attachment was found on the nightly ClubExpress email.")


def _handle_member_categories_email(conn_str: str, attachments: list[dict]) -> int:
    for attachment in attachments:
        content_bytes = attachment.get("contentBytes")
        if not content_bytes:
            continue
        if _detect_attachment_report_type(attachment.get("name", ""), content_bytes) != NIGHTLY_CATEGORY_MESSAGE_TYPE:
            continue
        return _import_member_categories_bytes(conn_str, content_bytes)

    raise EmailProcessingError("No member category attachment was found on the nightly ClubExpress email.")


def _import_memchap_bytes(conn_str: str, csv_bytes: bytes) -> int:
    rows = _parse_csv_rows(csv_bytes)
    _stage_and_import(conn_str, rows)
    return len(rows)


def _import_member_categories_bytes(conn_str: str, csv_bytes: bytes) -> int:
    rows = _parse_member_category_rows(csv_bytes)
    _stage_and_import_member_categories(conn_str, rows)
    return len(rows)


def _is_memchap_attachment_name(name: str) -> bool:
    normalized_name = (name or "").strip().lower()
    return normalized_name.endswith('.csv') and 'memchap' in normalized_name


def _classify_message(sender: str, subject: str, attachments: list[dict]) -> str:
    normalized_sender = (sender or "").strip().lower()
    normalized_subject = (subject or "").strip()

    detected_report_type = _detect_message_report_type(attachments)
    if detected_report_type:
        return detected_report_type
    if "New Member Signup - Payment" in normalized_subject:
        return NEW_MEMBER_MESSAGE_TYPE
    if "American Go Association - Member Renewal" in normalized_subject:
        return RENEWAL_MESSAGE_TYPE
    if "scheduler@mail2.clubexpress.com" in normalized_sender and attachments:
        return IGNORE_MESSAGE_TYPE
    return IGNORE_MESSAGE_TYPE


def _parse_new_member_email(text: str) -> dict:
    segment = _slice_between_markers(text, "membership in American Go Association.", "Club Url")
    agaid = _extract_required_int(segment, r"Member Number:\s*(\d+)", "AGAID")
    member_type = _extract_member_type(segment)
    if member_type.strip().lower() == "chapter":
        raise EmailProcessingError("Chapter member signup emails should be ignored before parsing.")

    email_address = _extract_optional_text(segment, r"Email:\s*(.*?)\s*Login")
    name_line = _extract_name_line(segment)
    if not name_line:
        raise EmailProcessingError("Could not determine member name from new member email.")

    name_parts = [part for part in name_line.split() if part]
    if len(name_parts) < 2:
        raise EmailProcessingError(f"Full name line is not parseable: {name_line!r}")

    return {
        "AGAID": agaid,
        "MemberType": member_type,
        "FirstName": name_parts[0],
        "LastName": name_parts[-1],
        "EmailAddress": email_address,
    }


def _parse_renewal_email(text: str) -> dict:
    segment = _slice_between_markers(text, "A membership renewal has been processed for American Go Association.", "Club Url")
    member_type = _extract_member_type(segment)
    return {
        "AGAID": _extract_required_int(segment, r"Member Number:\s*(\d+)", "AGAID"),
        "PhoneNumber": _extract_optional_text(segment, r"Phone:\s*(.*?)\s*Email"),
        "EmailAddress": _extract_optional_text(segment, r"Email:\s*(.*?)\s*Login Name"),
        "LoginName": _extract_optional_text(segment, r"Login Name:\s*(.*?)\s*(?:Member\s+)?Type"),
        "MemberType": member_type,
        "IsChapterMember": member_type.strip().lower().startswith("chapter"),
    }


def _slice_between_markers(text: str, start_marker: str, end_marker: str) -> str:
    if start_marker in text:
        text = text.split(start_marker, 1)[1]
    if end_marker in text:
        text = text.split(end_marker, 1)[0]
    return text.strip()


def _extract_name_line(text: str) -> Optional[str]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in lines[:8]:
        if ":" in line:
            continue
        if len(line.split()) >= 2:
            return line
    return None


def _extract_member_type(text: str) -> str:
    return _extract_required_text(text, r"(?:Member\s+)?Type:\s*(.*?)\s*Total", "Member Type")


def _extract_required_text(text: str, pattern: str, label: str) -> str:
    value = _extract_optional_text(text, pattern)
    if value is None or value == "":
        raise EmailProcessingError(f"Could not extract required field {label}.")
    return value


def _extract_optional_text(text: str, pattern: str) -> Optional[str]:
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return re.sub(r"\s+", " ", match.group(1)).strip()


def _extract_required_int(text: str, pattern: str, label: str) -> int:
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        raise EmailProcessingError(f"Could not extract required integer field {label}.")
    return int(match.group(1))


def _detect_message_report_type(attachments: list[dict]) -> Optional[str]:
    for attachment in attachments:
        content_bytes = attachment.get("contentBytes")
        if not content_bytes:
            continue
        report_type = _detect_attachment_report_type(attachment.get("name", ""), content_bytes)
        if report_type:
            return report_type
    return None


def _detect_attachment_report_type(name: str, content_bytes: bytes) -> Optional[str]:
    if _is_memchap_attachment_name(name):
        return NIGHTLY_MESSAGE_TYPE

    canonical_headers = _read_csv_header_canonical(content_bytes)
    if not canonical_headers:
        return None
    if _is_memchap_header(canonical_headers):
        return NIGHTLY_MESSAGE_TYPE
    if _is_member_category_header(canonical_headers):
        return NIGHTLY_CATEGORY_MESSAGE_TYPE
    return None


def _read_csv_header_canonical(csv_bytes: bytes) -> list[str]:
    rows = _read_csv_matrix(csv_bytes, raise_on_error=False)
    if not rows:
        return []

    for row in rows[:2]:
        canonical = [_canonicalize_header(value) for value in row if value is not None]
        if _is_memchap_header(canonical) or _is_member_category_header(canonical):
            return canonical

    return [_canonicalize_header(value) for value in rows[0] if value is not None]


def _is_memchap_header(headers: list[str]) -> bool:
    required = {_canonicalize_header("AGAID"), _canonicalize_header("MemberType"), _canonicalize_header("FirstName"), _canonicalize_header("LastName")}
    return required.issubset(set(headers))


def _is_member_category_header(headers: list[str]) -> bool:
    return {_canonicalize_header("AGAID"), _canonicalize_header("Category")}.issubset(set(headers))


def _message_body_to_text(message: dict) -> str:
    payload = message.get("payload") or {}
    text = _extract_message_part_text(payload, "text/plain")
    if text is not None:
        return text
    html_body = _extract_message_part_text(payload, "text/html")
    if html_body is not None:
        return _html_to_text(html_body)
    snippet = message.get("snippet") or ""
    return html.unescape(snippet)


def _extract_message_part_text(part: dict, mime_type: str) -> Optional[str]:
    if (part.get("mimeType") or "").lower() == mime_type:
        data = ((part.get("body") or {}).get("data"))
        if data:
            return _decode_base64url_to_text(data)

    for child in part.get("parts") or []:
        value = _extract_message_part_text(child, mime_type)
        if value is not None:
            return value
    return None


def _html_to_text(value: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", value, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return html.unescape(text)


def _archive_message_artifacts(message_type: str, message: dict, attachments: list[dict]) -> Optional[str]:
    container_name = os.environ.get("CLUBEXPRESS_ARCHIVE_CONTAINER")
    if not container_name:
        return None

    try:
        from azure.identity import ManagedIdentityCredential
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        logging.warning("Archive container is configured, but azure-storage-blob or azure-identity is not installed.")
        return None

    blob_service_uri = os.environ.get("AzureWebJobsStorage__blobServiceUri")
    connection_string = os.environ.get("AzureWebJobsStorage")
    if connection_string:
        client = BlobServiceClient.from_connection_string(connection_string)
    elif blob_service_uri:
        client_id = os.environ.get("AzureWebJobsStorage__clientId")
        credential = ManagedIdentityCredential(client_id=client_id) if client_id else ManagedIdentityCredential()
        client = BlobServiceClient(account_url=blob_service_uri, credential=credential)
    else:
        logging.warning("Archive container is configured, but no Blob service configuration is available.")
        return None

    container = client.get_container_client(container_name)
    try:
        container.create_container()
    except Exception:
        pass

    received_at = _message_received_at(message)
    safe_id = _safe_blob_name(_message_identifier(message))
    prefix = f"{message_type}/{received_at:%Y/%m/%d}/{safe_id}"

    metadata_payload = {
        "id": message.get("id"),
        "threadId": message.get("threadId"),
        "internalDate": message.get("internalDate"),
        "labelIds": message.get("labelIds"),
        "snippet": message.get("snippet"),
        "payload_headers": (message.get("payload") or {}).get("headers"),
    }
    container.upload_blob(f"{prefix}/message.json", json.dumps(metadata_payload, indent=2).encode("utf-8"), overwrite=True)

    for attachment in attachments:
        content_bytes = attachment.get("contentBytes")
        if content_bytes is None:
            continue
        attachment_name = _safe_blob_name(attachment.get("name") or "attachment.bin")
        container.upload_blob(f"{prefix}/attachments/{attachment_name}", content_bytes, overwrite=True)

    return prefix


def _safe_blob_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", value)


def _message_identifier(message: dict) -> str:
    return message.get("id") or f"message-{datetime.now(timezone.utc).timestamp()}"


def _message_received_at(message: dict) -> datetime:
    internal_date = message.get("internalDate")
    if internal_date:
        return datetime.fromtimestamp(int(internal_date) / 1000, tz=timezone.utc)
    return datetime.now(timezone.utc)


def _execute_stored_procedure(conn_str: str, proc_name: str, params: dict) -> None:
    ordered_items = [(key, value) for key, value in params.items()]
    sql = f"EXEC {proc_name} " + ", ".join(f"@{name} = ?" for name, _ in ordered_items)

    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(sql, [value for _, value in ordered_items])
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _get_sql_connection_string() -> Optional[str]:
    return os.environ.get("SQL_CONNECTION_STRING")


def _get_mysql_sync_sql_connection_string() -> Optional[str]:
    return os.environ.get("MYSQL_SYNC_SQL_CONNECTION_STRING") or os.environ.get("AZURE_SQL_GAMES_CONNECTION_STRING")


def _get_selected_mysql_sync_jobs() -> list[str]:
    configured = os.environ.get("MYSQL_SYNC_TABLES")
    if not configured:
        return list(MYSQL_SYNC_JOBS.keys())

    selected = []
    unknown = []
    for raw_name in configured.split(","):
        name = raw_name.strip().lower()
        if not name:
            continue
        if name not in MYSQL_SYNC_JOBS:
            unknown.append(name)
            continue
        if name not in selected:
            selected.append(name)

    if unknown:
        raise RuntimeError(f"Unsupported MYSQL_SYNC_TABLES values: {', '.join(unknown)}")
    if not selected:
        raise RuntimeError("MYSQL_SYNC_TABLES did not include any recognized jobs.")
    return selected


def _parse_connection_string(value: str) -> dict[str, str]:
    parts = {}
    for item in value.split(";"):
        if "=" not in item:
            continue
        key, part_value = item.split("=", 1)
        parts[key.strip().lower()] = part_value.strip().strip("{}")
    return parts


def _get_mysql_source_config() -> dict[str, object]:
    conn_str = os.environ.get("MYSQL_SOURCE_CONNECTION_STRING", "")
    parts = _parse_connection_string(conn_str) if conn_str else {}

    host = parts.get("server") or parts.get("host") or os.environ.get("MYSQL_HOST")
    database = parts.get("database") or os.environ.get("MYSQL_DATABASE")
    user = parts.get("user") or parts.get("uid") or os.environ.get("MYSQL_USER")
    password = (
        parts.get("password")
        or parts.get("pwd")
        or os.environ.get("MYSQL_SOURCE_PASSWORD")
        or os.environ.get("MYSQL_PASSWORD")
    )
    port_raw = parts.get("port") or os.environ.get("MYSQL_PORT") or "3306"
    ssl_mode = parts.get("sslmode") or os.environ.get("MYSQL_SSL_MODE") or ""

    if not host or not database or not user or not password:
        raise RuntimeError(
            "MySQL source configuration is incomplete. Set MYSQL_SOURCE_CONNECTION_STRING or MYSQL_HOST, "
            "MYSQL_DATABASE, MYSQL_USER, and MYSQL_SOURCE_PASSWORD."
        )

    return {
        "host": host,
        "database": database,
        "user": user,
        "password": password,
        "port": int(port_raw),
        "ssl_mode": ssl_mode,
    }


def _fetch_mysql_rows(source_config: dict[str, object], table_name: str, columns: tuple[str, ...]) -> list[tuple]:
    try:
        import pymysql
    except ImportError as exc:
        raise RuntimeError("PyMySQL is required for MySQL sync. Add it to requirements and deployment.") from exc

    ssl_mode = str(source_config.get("ssl_mode", "")).strip().lower()
    ssl = {} if ssl_mode in {"1", "true", "required", "require", "verify-ca", "verify_identity"} else None
    select_sql = f"SELECT {', '.join(columns)} FROM {table_name}"

    conn = pymysql.connect(
        host=str(source_config["host"]),
        port=int(source_config["port"]),
        user=str(source_config["user"]),
        password=str(source_config["password"]),
        database=str(source_config["database"]),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        ssl=ssl,
        read_timeout=120,
        write_timeout=120,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(select_sql)
            records = cursor.fetchall()
            return [tuple(record.get(column) for column in columns) for record in records]
    finally:
        conn.close()


def _replace_sql_table(conn_str: str, table_name: str, columns: tuple[str, ...], rows: list[tuple]) -> None:
    insert_sql = (
        f"INSERT INTO {table_name} ("
        + ", ".join(f"[{column}]" for column in columns)
        + ") VALUES ("
        + ", ".join("?" for _ in columns)
        + ")"
    )

    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        if rows:
            cursor.fast_executemany = True
            cursor.executemany(insert_sql, rows)
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _record_mysql_sync_run(
    conn_str: str,
    job_name: str,
    source_table: str,
    target_table: str,
    started_at: datetime,
    completed_at: Optional[datetime],
    status: str,
    row_count: Optional[int],
    error_message: Optional[str],
) -> None:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(
            MYSQL_SYNC_RUN_TABLE_SQL,
            job_name,
            source_table,
            target_table,
            started_at,
            completed_at,
            status,
            row_count,
            error_message,
        )
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        logging.exception("Failed to record MySQL sync run for job %s.", job_name)
    finally:
        conn.close()


def _get_gmail_access_token() -> str:
    client_id = _require_env("GOOGLE_WORKSPACE_CLIENT_ID")
    client_secret = _require_env("GOOGLE_WORKSPACE_CLIENT_SECRET")
    refresh_token = _require_env("GOOGLE_WORKSPACE_REFRESH_TOKEN")

    payload = parse.urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")

    req = request.Request(GMAIL_TOKEN_URL, data=payload, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with request.urlopen(req, timeout=30) as response:
            body = json.loads(response.read().decode("utf-8"))
            return body["access_token"]
    except error.HTTPError as exc:
        raise GmailApiError(f"Gmail token request failed: {exc.read().decode('utf-8', errors='ignore')}") from exc


def _list_gmail_messages(access_token: str) -> list[dict]:
    mailbox_user = _require_env("GOOGLE_WORKSPACE_MAILBOX")
    max_results = int(os.environ.get("CLUBEXPRESS_MAILBOX_BATCH_SIZE", "10"))
    query_text = os.environ.get(
        "GOOGLE_WORKSPACE_QUERY",
        'in:inbox -label:ProcessedByFunction (subject:"New Member Signup - Payment" OR subject:"American Go Association - Member Renewal" OR has:attachment)',
    )
    response = _gmail_json_request(
        access_token,
        f"/users/{parse.quote(mailbox_user)}/messages",
        query={"q": query_text, "maxResults": str(max_results)},
    )
    return response.get("messages", [])


def _get_gmail_message(access_token: str, message_id: str) -> dict:
    mailbox_user = _require_env("GOOGLE_WORKSPACE_MAILBOX")
    return _gmail_json_request(
        access_token,
        f"/users/{parse.quote(mailbox_user)}/messages/{parse.quote(message_id)}",
        query={"format": "full"},
    )


def _mark_gmail_message_processed(access_token: str, message: dict) -> None:
    mailbox_user = _require_env("GOOGLE_WORKSPACE_MAILBOX")
    processed_label = os.environ.get("CLUBEXPRESS_PROCESSED_CATEGORY", "ProcessedByFunction")
    label_id = _ensure_gmail_label(access_token, processed_label)
    body = {"removeLabelIds": ["UNREAD"], "addLabelIds": [label_id]}
    _gmail_json_request(
        access_token,
        f"/users/{parse.quote(mailbox_user)}/messages/{parse.quote(message['id'])}/modify",
        method="POST",
        body=body,
    )


def _ensure_gmail_label(access_token: str, label_name: str) -> str:
    mailbox_user = _require_env("GOOGLE_WORKSPACE_MAILBOX")
    labels = _gmail_json_request(access_token, f"/users/{parse.quote(mailbox_user)}/labels").get("labels", [])
    for label in labels:
        if label.get("name") == label_name:
            return label["id"]

    created = _gmail_json_request(
        access_token,
        f"/users/{parse.quote(mailbox_user)}/labels",
        method="POST",
        body={
            "name": label_name,
            "labelListVisibility": "labelShow",
            "messageListVisibility": "show",
        },
    )
    return created["id"]


def _gmail_json_request(
    access_token: str,
    path: str,
    *,
    method: str = "GET",
    query: Optional[dict[str, str]] = None,
    body: Optional[dict] = None,
) -> dict:
    url = GMAIL_API_BASE_URL + path
    if query:
        url += "?" + parse.urlencode(query)

    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")

    req = request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {access_token}")
    req.add_header("Accept", "application/json")
    if body is not None:
        req.add_header("Content-Type", "application/json")

    try:
        with request.urlopen(req, timeout=30) as response:
            raw = response.read()
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise GmailApiError(f"Gmail request failed for {path}: {exc.code} {detail}") from exc


def _extract_gmail_attachments(access_token: str, message: dict) -> list[dict]:
    attachments = []
    payload = message.get("payload") or {}
    _collect_gmail_attachments(access_token, message, payload, attachments)
    return attachments


def _collect_gmail_attachments(access_token: str, message: dict, part: dict, attachments: list[dict]) -> None:
    filename = part.get("filename") or ""
    body = part.get("body") or {}
    data = body.get("data")
    attachment_id = body.get("attachmentId")
    mime_type = (part.get("mimeType") or "").lower()

    if filename and (data or attachment_id or mime_type == "text/csv"):
        content_bytes = _decode_base64url(data) if data else None
        if content_bytes is None and attachment_id:
            content_bytes = _get_gmail_attachment_bytes(access_token, message, attachment_id)
        attachments.append(
            {
                "name": filename,
                "mimeType": part.get("mimeType"),
                "attachmentId": attachment_id,
                "contentBytes": content_bytes,
            }
        )

    for child in part.get("parts") or []:
        _collect_gmail_attachments(access_token, message, child, attachments)


def _get_gmail_attachment_bytes(access_token: str, message: dict, attachment_id: str) -> bytes:
    mailbox_user = _require_env("GOOGLE_WORKSPACE_MAILBOX")
    message_id = _message_identifier(message)
    response = _gmail_json_request(
        access_token,
        f"/users/{parse.quote(mailbox_user)}/messages/{parse.quote(message_id)}/attachments/{parse.quote(attachment_id)}",
    )
    data = response.get("data")
    if not data:
        raise EmailProcessingError(f"Gmail attachment {attachment_id!r} did not include data.")
    return _decode_base64url(data)


def _get_header_value(message: dict, header_name: str) -> str:
    headers = ((message.get("payload") or {}).get("headers") or [])
    for header in headers:
        if (header.get("name") or "").lower() == header_name.lower():
            return header.get("value") or ""
    return ""


def _decode_base64url(value: str) -> bytes:
    padding = '=' * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _decode_base64url_to_text(value: str) -> str:
    return _decode_base64url(value).decode("utf-8", errors="replace")


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required application setting {name}.")
    return value


def _is_truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _extract_csv_bytes(req: func.HttpRequest) -> bytes:
    body = req.get_body() or b""
    content_type = req.headers.get("content-type", "")

    if content_type.lower().startswith("multipart/form-data"):
        message = BytesParser(policy=policy.default).parsebytes(
            f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("utf-8") + body
        )

        for part in message.iter_attachments():
            if part.get_content_disposition() != "form-data":
                continue
            filename = part.get_filename()
            field_name = part.get_param("name", header="content-disposition")
            if filename or field_name in {"file", "csv"}:
                payload = part.get_payload(decode=True) or b""
                if payload:
                    return payload

        raise CsvValidationError("Multipart request did not include a CSV file part.")

    if not body:
        raise CsvValidationError("Request body is empty. Send the CSV as the request body or multipart file upload.")

    return body


def _parse_date(value: str) -> date:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise CsvValidationError(f"Invalid date value '{value}'.")


def _parse_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M %p",
    ):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    try:
        return datetime.combine(_parse_date(normalized), datetime.min.time())
    except CsvValidationError as exc:
        raise CsvValidationError(f"Invalid datetime value '{value}'.") from exc


def _convert_value(column: str, raw_value: str):
    value = raw_value.strip()
    if value == "":
        return None
    if column in INT_COLUMNS:
        try:
            return int(value)
        except ValueError as exc:
            raise CsvValidationError(f"Column {column} requires an integer. Received '{raw_value}'.") from exc
    if column in DATE_COLUMNS:
        return _parse_date(value)
    if column in DATETIME_COLUMNS:
        return _parse_datetime(value)
    if column in STRING_COLUMNS:
        return value
    raise CsvValidationError(f"Unsupported column mapping for {column}.")


def _normalize_header(fieldnames: Iterable[Optional[str]]) -> list[str]:
    normalized = []
    for field in fieldnames:
        if field is None:
            normalized.append("")
            continue
        normalized.append(field.strip())
    return normalized


def _canonicalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.strip().lower())


def _parse_csv_rows(csv_bytes: bytes) -> list[tuple]:
    csv_text = _decode_csv_text(csv_bytes)

    reader = csv.DictReader(StringIO(csv_text))
    if not reader.fieldnames:
        raise CsvValidationError("CSV is missing a header row.")

    original_header = list(reader.fieldnames)
    incoming_header = _normalize_header(original_header)
    source_columns_by_target = {}
    unknown_columns = []
    duplicate_columns = []

    for original_name, normalized_name in zip(original_header, incoming_header):
        canonical_name = _canonicalize_header(normalized_name)
        mapped = EXPECTED_HEADER_LOOKUP.get(canonical_name)
        if not mapped:
            if canonical_name in IGNORED_SOURCE_COLUMNS:
                continue
            unknown_columns.append(normalized_name)
            continue
        if mapped in source_columns_by_target:
            duplicate_columns.append(mapped)
            continue
        source_columns_by_target[mapped] = original_name

    if unknown_columns:
        raise CsvValidationError(f"CSV contains unsupported columns: {', '.join(unknown_columns)}")
    if duplicate_columns:
        raise CsvValidationError(f"CSV contains duplicate columns: {', '.join(duplicate_columns)}")

    missing_columns = [
        column for column in STAGING_COLUMNS
        if column not in source_columns_by_target and column not in OPTIONAL_SOURCE_COLUMNS
    ]
    if missing_columns:
        raise CsvValidationError(f"CSV is missing required columns: {', '.join(missing_columns)}")

    rows = []
    skipped_non_member_rows = 0
    for row_number, row in enumerate(reader, start=2):
        converted_row = []
        for column in STAGING_COLUMNS:
            source_key = source_columns_by_target.get(column)
            raw_value = row.get(source_key, "") if source_key else ""
            try:
                converted_row.append(_convert_value(column, raw_value or ""))
            except CsvValidationError as exc:
                raise CsvValidationError(f"Row {row_number}: {exc}") from exc
        if not _is_member_agaid(converted_row[0]):
            skipped_non_member_rows += 1
            continue
        rows.append(tuple(converted_row))

    if skipped_non_member_rows:
        logging.info("Skipped %s MemChap rows with non-member AGAIDs.", skipped_non_member_rows)
    if not rows:
        raise CsvValidationError("CSV did not contain any data rows.")
    return rows


def _parse_member_category_rows(csv_bytes: bytes) -> list[tuple[int, str]]:
    csv_rows = _read_csv_matrix(csv_bytes)
    if not csv_rows:
        raise CsvValidationError("CSV is missing a header row.")

    header_index = None
    original_header = None
    for idx, row in enumerate(csv_rows[:2]):
        canonical = [_canonicalize_header(value) for value in row]
        if _is_member_category_header(canonical):
            header_index = idx
            original_header = row
            break

    if header_index is None or original_header is None:
        raise CsvValidationError("CSV is missing required columns: AGAID, Category")

    incoming_header = _normalize_header(original_header)
    source_columns_by_target = {}
    unknown_columns = []
    duplicate_columns = []

    for original_name, normalized_name in zip(original_header, incoming_header):
        canonical_name = _canonicalize_header(normalized_name)
        mapped = CATEGORY_HEADER_LOOKUP.get(canonical_name)
        if not mapped:
            if canonical_name:
                unknown_columns.append(normalized_name)
            continue
        if mapped in source_columns_by_target:
            duplicate_columns.append(mapped)
            continue
        source_columns_by_target[mapped] = original_name

    if unknown_columns:
        raise CsvValidationError(f"CSV contains unsupported columns: {', '.join(unknown_columns)}")
    if duplicate_columns:
        raise CsvValidationError(f"CSV contains duplicate columns: {', '.join(duplicate_columns)}")

    missing_columns = [column for column in CATEGORY_COLUMNS if column not in source_columns_by_target]
    if missing_columns:
        raise CsvValidationError(f"CSV is missing required columns: {', '.join(missing_columns)}")

    column_indexes = {name: original_header.index(source_columns_by_target[name]) for name in CATEGORY_COLUMNS}

    rows = []
    skipped_non_member_rows = 0
    seen_pairs = set()
    for row_number, row in enumerate(csv_rows[header_index + 1 :], start=header_index + 2):
        padded = list(row) + [""] * (len(original_header) - len(row))
        agaid_raw = padded[column_indexes["AGAID"]] or ""
        category_raw = padded[column_indexes["Category"]] or ""
        try:
            agaid = int(agaid_raw.strip())
        except ValueError as exc:
            raise CsvValidationError(f"Row {row_number}: Column AGAID requires an integer. Received '{agaid_raw}'.") from exc

        category = category_raw.strip()
        if not category:
            raise CsvValidationError(f"Row {row_number}: Column Category is required.")
        if not _is_member_agaid(agaid):
            skipped_non_member_rows += 1
            continue

        pair = (agaid, category)
        if pair in seen_pairs:
            raise CsvValidationError(f"Row {row_number}: Duplicate AGAID/category pair {agaid}/{category}.")
        seen_pairs.add(pair)
        rows.append(pair)

    if skipped_non_member_rows:
        logging.info("Skipped %s category rows with non-member AGAIDs.", skipped_non_member_rows)
    return rows


def _read_csv_matrix(csv_bytes: bytes, *, raise_on_error: bool = True) -> list[list[str]]:
    try:
        csv_text = csv_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        if raise_on_error:
            raise CsvValidationError("CSV must be UTF-8 encoded.") from exc
        return []
    return list(csv.reader(StringIO(csv_text)))


def _decode_csv_text(csv_bytes: bytes) -> str:
    try:
        return csv_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvValidationError("CSV must be UTF-8 encoded.") from exc


def _is_member_agaid(agaid: Optional[int]) -> bool:
    return agaid is not None and agaid < MAX_MEMBER_AGAID


def _stage_and_import(conn_str: str, rows: list[tuple]) -> None:
    insert_sql = (
        "INSERT INTO staging.memchap ("
        + ", ".join(f"[{column}]" for column in STAGING_COLUMNS)
        + ") VALUES ("
        + ", ".join("?" for _ in STAGING_COLUMNS)
        + ")"
    )

    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE staging.memchap")
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, rows)
        cursor.execute("EXEC membership.sp_import_memchap")
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _stage_and_import_member_categories(conn_str: str, rows: list[tuple[int, str]]) -> None:
    insert_sql = "INSERT INTO staging.member_categories ([AGAID], [Category]) VALUES (?, ?)"

    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE staging.member_categories")
        if rows:
            cursor.fast_executemany = True
            cursor.executemany(insert_sql, rows)
        cursor.execute("EXEC membership.sp_import_member_categories")
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()





