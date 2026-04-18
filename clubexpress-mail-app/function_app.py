import base64
import csv
import html
import json
import logging
import os
import re
import threading
from datetime import date, datetime, timedelta, timezone
from email import message_from_bytes, policy
from email.parser import BytesParser
from html.parser import HTMLParser
from io import StringIO
from pathlib import Path
from typing import Iterable, Optional
from urllib import error, parse, request

import azure.functions as func
import certifi
try:
    import pyodbc
except Exception:
    class _MissingPyodbc:
        Error = Exception

        @staticmethod
        def connect(*args, **kwargs):
            raise RuntimeError("pyodbc is unavailable in this environment")

    pyodbc = _MissingPyodbc()
try:
    import pytds
except Exception:
    pytds = None

app = func.FunctionApp()
# Migration note: this app keeps the mailbox ingestion trigger and related helpers.

GMAIL_API_BASE_URL = "https://gmail.googleapis.com/gmail/v1"
GMAIL_TOKEN_URL = "https://oauth2.googleapis.com/token"
DEFAULT_MAILBOX_POLL_SCHEDULE = "0 */5 * * * *"
NIGHTLY_MESSAGE_TYPE = "nightly_memchap_csv"
NIGHTLY_CATEGORY_MESSAGE_TYPE = "nightly_member_categories_csv"
NEW_MEMBER_MESSAGE_TYPE = "new_member_signup"
RENEWAL_MESSAGE_TYPE = "member_renewal"
JOURNAL_MESSAGE_TYPE = "american_go_e_journal"
JOURNAL_SUBJECT_PREFIX = "American Go E - Journal"
JOURNAL_EXCLUDED_MATCH_NAMES = {"chris garlock"}
DEFAULT_JOURNAL_NAME_PREFIXES = (
    "AGA President",
    "AGA Vice President",
    "AGA Board Chair",
    "AGA Board Member",
    "Congress Director",
    "Chapter President",
    "Chapter Vice President",
    "Chapter Secretary",
    "Chapter Treasurer",
    "Executive Director",
    "Membership Director",
    "Tournament Director",
)
IGNORE_MESSAGE_TYPE = "ignore"
TDLIST_REDIRECT_URLS = {
    "A": os.environ.get("TDLIST_REDIRECT_URL_A", ""),
    "B": os.environ.get("TDLIST_REDIRECT_URL_B", ""),
    "N": os.environ.get("TDLIST_REDIRECT_URL_N", ""),
}
MAX_MEMBER_AGAID = 50000
TDLIST_QUERY = """
WITH current_ratings AS
(
    SELECT
        ranked.[AGAID],
        ranked.[Rating],
        ranked.[Sigma]
    FROM
    (
        SELECT
            r.[Pin_Player] AS [AGAID],
            r.[Rating],
            r.[Sigma],
            ROW_NUMBER() OVER
            (
                PARTITION BY r.[Pin_Player]
                ORDER BY r.[Elab_Date] DESC, r.[id] DESC
            ) AS rn
        FROM [ratings].[ratings] AS r
        WHERE r.[Pin_Player] IS NOT NULL
    ) AS ranked
    WHERE ranked.rn = 1
)
SELECT
    m.[AGAID],
    m.[FirstName],
    m.[LastName],
    m.[MemberType],
    m.[ExpirationDate],
    m.[JoinDate],
    m.[State],
    cr.[Rating],
    cr.[Sigma],
    c.[ChapterCode],
    c.[ChapterName]
FROM [membership].[members] AS m
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
LEFT JOIN current_ratings AS cr
    ON cr.[AGAID] = m.[AGAID]
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
JOURNAL_NLP_MODEL = "en_core_web_sm"
JOURNAL_NLP_EXCLUDE = ["tagger", "parser", "lemmatizer", "attribute_ruler"]
_journal_name_nlp = None
_journal_name_nlp_attempted = False
_journal_name_nlp_lock = threading.Lock()


class CsvValidationError(ValueError):
    pass


class EmailProcessingError(ValueError):
    pass


class GmailApiError(RuntimeError):
    pass


class _JournalHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.blocks: list[dict[str, object]] = []
        self._text_parts: list[str] = []
        self._links: list[str] = []
        self._heading_level: Optional[int] = None
        self._link_href: Optional[str] = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        normalized_tag = tag.lower()
        if normalized_tag in {"p", "div", "li", "tr", "table", "section", "article"}:
            self._flush_block()
        if normalized_tag == "br":
            self._text_parts.append("\n")
        if normalized_tag.startswith("h") and len(normalized_tag) == 2 and normalized_tag[1].isdigit():
            self._flush_block()
            self._heading_level = int(normalized_tag[1])
        if normalized_tag == "a":
            self._link_href = dict(attrs).get("href")

    def handle_endtag(self, tag: str) -> None:
        normalized_tag = tag.lower()
        if normalized_tag == "a":
            self._link_href = None
        if normalized_tag.startswith("h") and len(normalized_tag) == 2 and normalized_tag[1].isdigit():
            self._flush_block()
            self._heading_level = None
        if normalized_tag in {"p", "div", "li", "tr", "table", "section", "article"}:
            self._flush_block()

    def handle_data(self, data: str) -> None:
        if not data:
            return
        self._text_parts.append(data)
        if self._link_href:
            self._links.append(self._link_href)

    def close(self) -> None:
        super().close()
        self._flush_block()

    def _flush_block(self) -> None:
        text = re.sub(r"\s+", " ", "".join(self._text_parts)).strip()
        links = []
        seen_links = set()
        for href in self._links:
            normalized = (href or "").strip()
            if not normalized or normalized in seen_links:
                continue
            seen_links.add(normalized)
            links.append(normalized)
        if text or links:
            self.blocks.append(
                {
                    "text": text,
                    "links": links,
                    "heading_level": self._heading_level,
                }
            )
        self._text_parts = []
        self._links = []


class _JournalVisibleTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._skip_depth = 0
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        normalized_tag = tag.lower()
        if normalized_tag in {"script", "style", "svg", "noscript"}:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if normalized_tag in {"br", "p", "div", "li", "tr", "table", "section", "article", "h1", "h2", "h3", "h4", "h5", "h6", "td"}:
            self._parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        normalized_tag = tag.lower()
        if normalized_tag in {"script", "style", "svg", "noscript"}:
            if self._skip_depth:
                self._skip_depth -= 1
            return
        if self._skip_depth:
            return
        if normalized_tag in {"p", "div", "li", "tr", "table", "section", "article", "h1", "h2", "h3", "h4", "h5", "h6", "td"}:
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        if data:
            self._parts.append(data)

    def get_lines(self) -> list[str]:
        lines = []
        for raw_line in "".join(self._parts).splitlines():
            normalized = re.sub(r"\s+", " ", raw_line).strip()
            if normalized:
                lines.append(normalized)
        return lines


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
    elif message_type == JOURNAL_MESSAGE_TYPE:
        parsed = _parse_journal_email(conn_str, message)
        _execute_stored_procedure(
            conn_str,
            "membership.sp_process_journal_news_email",
            {
                "MessageId": _message_identifier(message),
                "ReceivedAt": received_at,
                "JournalDate": parsed["JournalDate"],
                "MatchesJson": json.dumps(parsed["Matches"]),
                "ReviewMatchesJson": json.dumps(parsed["ReviewMatches"]),
                "Sender": sender or None,
                "Subject": subject or None,
                "BlobPath": archive_path,
            },
        )
        logging.info(
            "Journal email processed for %s with %s articles, %s news matches, and %s review matches archive_path=%s",
            parsed["JournalDate"],
            len(parsed["Articles"]),
            len(parsed["Matches"]),
            len(parsed["ReviewMatches"]),
            archive_path,
        )
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


def _redirect_tdlist(list_type: str) -> func.HttpResponse:
    target_url = TDLIST_REDIRECT_URLS.get(list_type)
    if not target_url:
        return func.HttpResponse(f"Unsupported TDList type: {list_type}", status_code=500)
    return func.HttpResponse(status_code=302, headers={"Location": target_url})


def _fetch_tdlist_rows(conn_str: str) -> list[dict[str, object]]:
    try:
        conn = pyodbc.connect(conn_str)
    except Exception:
        return _fetch_tdlist_rows_via_tds(conn_str)

    try:
        cursor = conn.cursor()
        cursor.execute(TDLIST_QUERY, MAX_MEMBER_AGAID)
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, record)) for record in cursor.fetchall()]
        cursor.close()
        return rows
    finally:
        conn.close()


def _fetch_tdlist_rows_via_tds(conn_str: str) -> list[dict[str, object]]:
    conn = _tds_connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(TDLIST_QUERY.replace("?", str(MAX_MEMBER_AGAID)))
        return list(cursor.fetchall())
    finally:
        conn.close()


def _parse_sql_connection_string(connection_string: str) -> dict[str, object]:
    parts: dict[str, str] = {}
    for item in connection_string.split(";"):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        parts[key.strip().lower()] = value.strip().strip("{}")
    server = parts["server"].replace("tcp:", "")
    host, port_text = (server.split(",", 1) + ["1433"])[:2]
    return {
        "server": host,
        "port": int(port_text),
        "database": parts["database"],
        "user": parts["uid"],
        "password": parts["pwd"],
    }


def _tds_connect(conn_str: str):
    if pytds is None:
        raise RuntimeError("python-tds is unavailable in this environment")
    sql = _parse_sql_connection_string(conn_str)
    return pytds.connect(
        server=sql["server"],
        port=sql["port"],
        database=sql["database"],
        user=sql["user"],
        password=sql["password"],
        cafile=certifi.where(),
        validate_host=True,
        enc_login_only=False,
        autocommit=True,
        timeout=60,
        as_dict=True,
    )


def _render_tdlist_tab(rows: list[dict[str, object]], *, chapter_field: str) -> str:
    rendered_rows = []
    for row in rows:
        rendered_rows.append(
            "\t".join(
                [
                    _tdlist_name(row),
                    str(row["AGAID"] or ""),
                    _tdlist_text(row.get("MemberType")),
                    _format_tdlist_decimal(row.get("Rating"), digits=5),
                    _format_tdlist_date(row.get("ExpirationDate")),
                    _tdlist_text(row.get(chapter_field)),
                    _tdlist_text(row.get("State")),
                    _format_tdlist_decimal(row.get("Sigma"), digits=5),
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
            f"{_format_tdlist_decimal(row.get('Rating'), digits=1):>6} "
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


def _format_tdlist_decimal(value: object, *, digits: int) -> str:
    if value is None:
        return ""
    return f"{float(value):.{digits}f}"


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
    if normalized_subject.startswith(JOURNAL_SUBJECT_PREFIX):
        return JOURNAL_MESSAGE_TYPE
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


def _parse_journal_email(conn_str: str, message: dict) -> dict:
    subject = _get_header_value(message, "Subject")
    journal_date = _parse_journal_subject_date(subject)
    html_body = _message_body_to_html(message)
    if html_body:
        articles = _extract_journal_articles_from_html(html_body)
        review_blog_entries = _extract_journal_review_blog_entries_from_html(html_body)
    else:
        articles = _extract_journal_articles_from_text(_message_body_to_text(message))
        review_blog_entries = _extract_journal_review_blog_entries_from_text(_message_body_to_text(message))

    member_lookup = _load_member_name_lookup(conn_str)
    matches = []
    for article in articles:
        article_text = " ".join(part for part in [article["title"], article.get("analysisText")] if part)
        for agaid, matched_name in _match_member_rows_in_article(article_text, member_lookup):
            matches.append(
                {
                    "AGAID": agaid,
                    "MatchedName": matched_name,
                    "ArticleTitle": article["title"],
                    "ArticleLink": article["link"],
                }
            )

    review_matches = []
    for blog_entry in review_blog_entries:
        for review in _extract_review_matches_from_blog_entry(blog_entry, member_lookup):
            review_matches.append(review)

    return {
        "JournalDate": journal_date,
        "Articles": articles,
        "Matches": matches,
        "ReviewMatches": review_matches,
    }


def _parse_journal_subject_date(subject: str) -> date:
    raw_value = (subject or "").strip()
    if not raw_value.startswith(JOURNAL_SUBJECT_PREFIX):
        raise EmailProcessingError(f"Unsupported journal subject: {subject!r}")

    suffix = raw_value[len(JOURNAL_SUBJECT_PREFIX):].strip(" :-")
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(suffix, fmt).date()
        except ValueError:
            continue
    raise EmailProcessingError(f"Could not parse journal date from subject {subject!r}.")


def _extract_journal_articles_from_html(html_body: str) -> list[dict[str, str]]:
    lines = _extract_visible_lines_from_html(html_body)
    articles = _extract_journal_articles_from_lines(lines, html_body=html_body)
    if articles:
        return articles

    parser = _JournalHtmlParser()
    parser.feed(html_body)
    parser.close()
    return _build_journal_articles_from_blocks(parser.blocks)


def _extract_journal_articles_from_text(text: str) -> list[dict[str, str]]:
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines() if line.strip()]
    return _extract_journal_articles_from_lines(lines)


def _extract_journal_review_blog_entries_from_html(html_body: str) -> list[dict[str, str]]:
    lines = _extract_visible_lines_from_html(html_body)
    return _extract_journal_review_blog_entries_from_lines(lines, html_body=html_body)


def _extract_journal_review_blog_entries_from_text(text: str) -> list[dict[str, str]]:
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines() if line.strip()]
    return _extract_journal_review_blog_entries_from_lines(lines)


def _extract_visible_lines_from_html(html_body: str) -> list[str]:
    parser = _JournalVisibleTextParser()
    parser.feed(html_body)
    parser.close()
    return parser.get_lines()


def _extract_journal_articles_from_lines(lines: list[str], html_body: Optional[str] = None) -> list[dict[str, str]]:
    news_lines = _slice_journal_news_lines(lines)
    if not news_lines:
        return []

    title_links = _extract_journal_title_links(html_body, news_lines) if html_body else []
    if title_links:
        articles = _build_articles_from_title_links(news_lines, title_links)
        if articles:
            return articles

    return _build_article_from_news_lines(news_lines)


def _slice_journal_news_lines(lines: list[str]) -> list[str]:
    return _slice_journal_section_lines(lines, "news")


def _slice_journal_blog_lines(lines: list[str]) -> list[str]:
    return _slice_journal_section_lines(lines, "blogs")


def _slice_journal_section_lines(lines: list[str], section_name: str) -> list[str]:
    start_index = None
    for idx, line in enumerate(lines):
        if line.lower() == section_name:
            start_index = idx + 1
            break
    if start_index is None:
        return []

    section_lines = []
    for line in lines[start_index:]:
        if _looks_like_terminal_journal_section(line):
            break
        section_lines.append(line)
    return section_lines


def _looks_like_terminal_journal_section(line: str) -> bool:
    normalized = (line or "").strip().lower()
    return normalized in {
        "upcoming events",
        "events",
        "blogs",
        "blog",
        "classifieds",
        "not an aga member? you can join here and support",
    }


def _extract_journal_title_links(html_body: str, news_lines: list[str]) -> list[tuple[str, str]]:
    titles_in_news = {line for line in news_lines if _looks_like_article_title(line)}
    if not titles_in_news:
        return []

    candidates = []
    seen = set()
    pattern = re.compile(r"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL)
    for href, inner_html in pattern.findall(html_body):
        link_text = re.sub(r"\s+", " ", _html_to_text(inner_html)).strip()
        clean_href = html.unescape(href).strip()
        if not link_text or not clean_href.startswith(("http://", "https://")):
            continue
        if link_text not in titles_in_news:
            continue
        if link_text.lower() in {"view in your browser", "download", "unsubscribe"}:
            continue
        key = (link_text, clean_href)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(key)
    return candidates


def _extract_journal_review_blog_entries_from_lines(lines: list[str], html_body: Optional[str] = None) -> list[dict[str, str]]:
    blog_lines = _slice_journal_blog_lines(lines)
    if not blog_lines:
        return []

    title_links = _extract_journal_title_links(html_body, blog_lines) if html_body else []
    link_lookup: dict[str, str] = {}
    for title, href in title_links:
        link_lookup.setdefault(title, href)

    entries = []
    for idx, line in enumerate(blog_lines):
        if not line.lower().startswith("naol reviews"):
            continue
        for candidate in blog_lines[idx + 1:idx + 4]:
            href = link_lookup.get(candidate)
            if href:
                entries.append(
                    {
                        "label": line[:500],
                        "title": candidate[:500],
                        "link": href[:1000],
                    }
                )
                break
    return entries


def _build_articles_from_title_links(news_lines: list[str], title_links: list[tuple[str, str]]) -> list[dict[str, str]]:
    articles = []
    title_indices = _resolve_article_title_indices(news_lines, [title for title, _ in title_links])
    if not title_indices:
        return articles

    for idx, ((title, href), title_index) in enumerate(zip(title_links, title_indices)):
        next_title_index = title_indices[idx + 1] if idx + 1 < len(title_indices) else len(news_lines)
        body_lines = [line for line in news_lines[title_index:next_title_index] if line]
        analysis_lines = [line for line in body_lines if line != title]
        articles.append(
            {
                "title": title[:500],
                "link": href[:1000],
                "analysisText": " ".join(analysis_lines).strip(),
            }
        )
    return articles



def _resolve_article_title_indices(news_lines: list[str], ordered_titles: list[str]) -> list[int]:
    if not ordered_titles:
        return []

    sequences: list[list[int]] = []
    search_start = 0
    while True:
        sequence: list[int] = []
        position = search_start
        for title in ordered_titles:
            idx = _find_line_index(news_lines, title, position)
            if idx is None:
                sequence = []
                break
            sequence.append(idx)
            position = idx + 1
        if not sequence:
            break
        sequences.append(sequence)
        search_start = sequence[0] + 1

    if not sequences:
        return []
    return sequences[-1]


def _build_article_from_news_lines(news_lines: list[str]) -> list[dict[str, str]]:
    current = None
    articles = []
    for line in news_lines:
        url_match = re.search(r"https?://\S+", line)
        if url_match:
            inline_title = line[:url_match.start()].strip()
            link = url_match.group(0).rstrip(").,")[:1000]
            if current and current.get("title") and not current.get("link") and not inline_title:
                current["link"] = link
                continue
            title = inline_title or url_match.group(0)
            if current and current.get("title") and current.get("link"):
                articles.append(current)
            current = {
                "title": title[:500],
                "link": link,
                "analysisText": "",
            }
            continue
        if current is None and _looks_like_article_title(line):
            current = {"title": line[:500], "link": "", "analysisText": ""}
            continue
        if current is not None:
            current["analysisText"] = (current.get("analysisText", "") + " " + line).strip()
    if current and current.get("title"):
        articles.append(current)
    return [article for article in articles if article.get("title") and article.get("link")]


def _find_line_index(lines: list[str], target: str, start: int) -> Optional[int]:
    for idx in range(start, len(lines)):
        if lines[idx] == target:
            return idx
    return None


def _build_journal_articles_from_blocks(blocks: list[dict[str, object]]) -> list[dict[str, str]]:
    news_index = None
    for idx, block in enumerate(blocks):
        block_text = str(block.get("text") or "").strip().lower()
        if block_text == "news":
            news_index = idx + 1
            break
    if news_index is None:
        return []

    articles = []
    current = None
    found_article = False
    for block in blocks[news_index:]:
        text = str(block.get("text") or "").strip()
        if not text:
            continue

        links = [link for link in (block.get("links") or []) if isinstance(link, str) and link.startswith(("http://", "https://"))]
        heading_level = block.get("heading_level")
        if found_article and heading_level is not None and not links and _looks_like_section_heading(text):
            break

        if links and _looks_like_article_title(text):
            if current and current.get("title") and current.get("link"):
                articles.append(current)
            current = {
                "title": text[:500],
                "link": links[0][:1000],
                "analysisText": "",
            }
            found_article = True
            continue

        if current is not None:
            current["analysisText"] = (current.get("analysisText", "") + " " + text).strip()

    if current and current.get("title") and current.get("link"):
        articles.append(current)
    return articles


def _looks_like_article_title(text: str) -> bool:
    collapsed = re.sub(r"\s+", " ", text).strip()
    word_count = len(collapsed.split())
    return 2 <= word_count <= 20 and len(collapsed) <= 180


def _looks_like_section_heading(text: str) -> bool:
    collapsed = re.sub(r"\s+", " ", text).strip()
    if len(collapsed) > 60:
        return False
    if ":" in collapsed:
        return True
    return collapsed.lower() in {"events", "classifieds", "calendar", "tournaments", "resources", "about", "membership"}


def _load_member_name_lookup(conn_str: str) -> dict[str, list[tuple[int, str]]]:
    lookup: dict[str, list[tuple[int, str]]] = {}
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT [AGAID], [FirstName], [LastName]
            FROM [membership].[members]
            WHERE [AGAID] < ?
              AND [FirstName] IS NOT NULL
              AND [LastName] IS NOT NULL
            """,
            MAX_MEMBER_AGAID,
        )
        for agaid, first_name, last_name in cursor.fetchall():
            matched_name = f"{first_name} {last_name}".strip()
            key = _normalize_person_name(matched_name)
            if not key:
                continue
            entry = (int(agaid), matched_name)
            if entry not in lookup.setdefault(key, []):
                lookup[key].append(entry)
        cursor.close()
    finally:
        conn.close()
    return lookup


def _match_member_rows_in_article(text: str, member_lookup: dict[str, list[tuple[int, str]]]) -> list[tuple[int, str]]:
    matched_rows = set()
    for candidate in _extract_candidate_person_names(text):
        key = _normalize_person_name(candidate)
        if not key:
            continue
        if key in JOURNAL_EXCLUDED_MATCH_NAMES:
            continue
        matched_rows.update(member_lookup.get(key, []))
    return sorted(matched_rows, key=lambda row: (row[0], row[1].lower()))


def _extract_candidate_person_names(text: str) -> set[str]:
    candidates = set()
    nlp = _get_journal_name_nlp()
    if nlp is not None:
        try:
            doc = nlp(text or "")
            for entity in doc.ents:
                if entity.label_ != "PERSON":
                    continue
                candidates.update(_expand_candidate_person_names(entity.text))
        except Exception:
            logging.warning("spaCy person extraction failed; falling back to regex candidate matching.", exc_info=True)

    pattern = re.compile(r"\b[A-Z][a-z]+(?:[-'][A-Z][a-z]+)?(?:\s+[A-Z][a-z]+(?:[-'][A-Z][a-z]+)?){1,3}\b")
    for match in pattern.finditer(text or ""):
        candidates.update(_expand_candidate_person_names(match.group(0)))
    return candidates


def _get_journal_name_nlp():
    global _journal_name_nlp, _journal_name_nlp_attempted
    if _journal_name_nlp_attempted:
        return _journal_name_nlp
    with _journal_name_nlp_lock:
        if _journal_name_nlp_attempted:
            return _journal_name_nlp
        _journal_name_nlp_attempted = True
        try:
            import spacy
            _journal_name_nlp = spacy.load(JOURNAL_NLP_MODEL, exclude=JOURNAL_NLP_EXCLUDE)
            logging.info("Loaded spaCy journal PERSON extractor using model %s.", JOURNAL_NLP_MODEL)
        except Exception:
            logging.warning(
                "Unable to load spaCy model %s; journal person extraction will use regex fallback only.",
                JOURNAL_NLP_MODEL,
                exc_info=True,
            )
            _journal_name_nlp = None
        return _journal_name_nlp


def _expand_candidate_person_names(value: str) -> set[str]:
    tokens = re.findall(r"[A-Za-z]+(?:[-'][A-Za-z]+)?", value or "")
    if len(tokens) < 2:
        return set()
    candidates = {" ".join(tokens)}
    max_window = min(len(tokens), 4)
    for window_size in range(2, max_window + 1):
        candidates.add(" ".join(tokens[-window_size:]))
    stripped = _strip_journal_name_prefix(tokens)
    if stripped:
        candidates.add(" ".join(stripped))
    return {candidate.strip() for candidate in candidates if candidate.strip()}


def _strip_journal_name_prefix(tokens: list[str]) -> list[str]:
    lowered_tokens = [token.lower() for token in tokens]
    for prefix_tokens in _journal_name_prefix_token_lists():
        prefix_length = len(prefix_tokens)
        if len(lowered_tokens) <= prefix_length:
            continue
        if lowered_tokens[:prefix_length] == prefix_tokens:
            return tokens[prefix_length:]
    return []


def _journal_name_prefix_token_lists() -> list[list[str]]:
    configured = list(DEFAULT_JOURNAL_NAME_PREFIXES)
    extra_prefixes = os.environ.get("JOURNAL_NAME_PREFIXES", "")
    configured.extend(prefix.strip() for prefix in extra_prefixes.split(";") if prefix.strip())
    token_lists = []
    seen = set()
    for prefix in configured:
        tokens = [token.lower() for token in re.findall(r"[A-Za-z]+(?:[-'][A-Za-z]+)?", prefix)]
        if len(tokens) < 1:
            continue
        key = tuple(tokens)
        if key in seen:
            continue
        seen.add(key)
        token_lists.append(tokens)
    return sorted(token_lists, key=len, reverse=True)


def _extract_review_matches_from_blog_entry(
    blog_entry: dict[str, str],
    member_lookup: dict[str, list[tuple[int, str]]],
) -> list[dict[str, str | int]]:
    blog_html = _fetch_external_html(blog_entry.get("link", ""))
    if not blog_html:
        return []

    review_post = _parse_naol_review_blog_html(blog_html)
    matches: list[dict[str, str | int]] = []
    for section in review_post["sections"]:
        reviewer_name = str(section.get("reviewer_name") or "").strip()
        reviewer_rank = str(section.get("reviewer_rank") or "").strip()
        video_link = str(section.get("video_link") or "").strip()
        if not reviewer_name or not reviewer_rank or not video_link:
            continue
        section_games = [game for game in section.get("games", []) if isinstance(game, dict)]
        review_count = len(section_games)
        for game_order, game in enumerate(section_games, start=1):
            if not isinstance(game, dict):
                continue
            players = [
                {
                    "name": str(game.get("player_one_name") or "").strip(),
                    "rank": str(game.get("player_one_rank") or "").strip(),
                    "opponent_name": str(game.get("player_two_name") or "").strip(),
                    "opponent_rank": str(game.get("player_two_rank") or "").strip(),
                },
                {
                    "name": str(game.get("player_two_name") or "").strip(),
                    "rank": str(game.get("player_two_rank") or "").strip(),
                    "opponent_name": str(game.get("player_one_name") or "").strip(),
                    "opponent_rank": str(game.get("player_one_rank") or "").strip(),
                },
            ]
            for player in players:
                for agaid, matched_name in _match_member_rows_in_article(player["name"], member_lookup):
                    matches.append(
                        {
                            "AGAID": agaid,
                            "MatchedName": matched_name,
                            "ReviewTitle": str(review_post.get("title") or blog_entry.get("title") or "")[:500],
                            "BlogLink": str(blog_entry.get("link") or "")[:1000],
                            "ReviewerName": reviewer_name[:200],
                            "ReviewerRank": reviewer_rank[:40],
                            "ReviewedPlayerName": player["name"][:200],
                            "ReviewedPlayerRank": player["rank"][:40],
                            "OpponentName": player["opponent_name"][:200],
                            "OpponentRank": player["opponent_rank"][:40],
                            "GameLink": str(game.get("game_link") or "")[:1000],
                            "VideoLink": video_link[:1000],
                            "VideoReviewCount": review_count,
                            "ReviewGameOrder": game_order,
                        }
                    )
    return matches


def _fetch_external_html(url: str) -> Optional[str]:
    clean_url = (url or "").strip()
    if not clean_url.startswith(("http://", "https://")):
        return None

    req = request.Request(
        clean_url,
        headers={
            "User-Agent": "AGA Membership Functions/1.0",
            "Accept": "text/html,application/xhtml+xml",
        },
        method="GET",
    )
    try:
        with request.urlopen(req, timeout=30) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
    except (error.URLError, TimeoutError, ValueError):
        logging.warning("Failed to fetch external HTML from %s", clean_url, exc_info=True)
        return None


def _parse_naol_review_blog_html(html_body: str) -> dict[str, object]:
    lines = _extract_visible_lines_from_html(html_body)
    title = _extract_naol_blog_title(lines)
    iframe_links = _extract_iframe_video_links(html_body)
    sections = _extract_naol_review_sections(lines, iframe_links)
    return {
        "title": title,
        "sections": sections,
    }


def _extract_naol_blog_title(lines: list[str]) -> str:
    for idx, line in enumerate(lines):
        if line.strip().lower() == "naol reviews" and idx + 1 < len(lines):
            return lines[idx + 1][:500]
    for line in lines[:12]:
        if "naol review" in line.lower():
            return line[:500]
    return "NAOL Reviews"


def _extract_iframe_video_links(html_body: str) -> list[str]:
    links = []
    seen = set()
    pattern = re.compile(r"<iframe\b[^>]*src=[\"']([^\"']+)[\"']", re.IGNORECASE)
    for href in pattern.findall(html_body or ""):
        clean_href = html.unescape(href).strip()
        if not clean_href:
            continue
        if clean_href.startswith("//"):
            clean_href = f"https:{clean_href}"
        if clean_href.startswith("/"):
            clean_href = parse.urljoin("https://www.usgo.org", clean_href)
        clean_href = _normalize_video_link(clean_href)
        if clean_href in seen:
            continue
        seen.add(clean_href)
        links.append(clean_href[:1000])
    return links


def _normalize_video_link(url: str) -> str:
    clean_url = (url or "").strip()
    if not clean_url.startswith(("http://", "https://")):
        return clean_url

    parsed = parse.urlparse(clean_url)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    if host in {"www.youtube.com", "youtube.com", "www.youtube-nocookie.com", "youtube-nocookie.com"}:
        embed_match = re.match(r"^/(?:embed|live_embed)/([^/?#]+)", path)
        if embed_match:
            video_id = embed_match.group(1).strip()
            if video_id:
                return f"https://www.youtube.com/watch?v={video_id}"
    return clean_url


def _extract_naol_review_sections(lines: list[str], iframe_links: list[str]) -> list[dict[str, object]]:
    sections: list[dict[str, object]] = []
    current: Optional[dict[str, object]] = None
    iframe_index = 0
    in_post_body = False

    for raw_line in lines:
        line = re.sub(r"\s+", " ", raw_line).strip().lstrip("\ufeff")
        if not line:
            continue
        lower_line = line.lower()
        if not in_post_body:
            if _looks_like_naol_reviewer_header(line):
                in_post_body = True
            else:
                continue

        if lower_line == "return to previous page":
            break

        reviewer_match = _parse_naol_reviewer_header(line)
        if reviewer_match:
            if current and current.get("video_link"):
                sections.append(current)
            current = {
                "reviewer_name": reviewer_match["reviewer_name"],
                "reviewer_rank": reviewer_match["reviewer_rank"],
                "games": [],
                "video_link": "",
            }
            continue

        if current is None:
            continue

        game_match = _parse_naol_game_line(line)
        if game_match:
            current["games"].append(game_match)
            continue

        if "review video" in lower_line:
            if iframe_index < len(iframe_links):
                current["video_link"] = iframe_links[iframe_index]
                iframe_index += 1
            if current.get("games") and current.get("video_link"):
                sections.append(current)
                current = None

    if current and current.get("games") and current.get("video_link"):
        sections.append(current)
    return sections


def _looks_like_naol_reviewer_header(line: str) -> bool:
    return _parse_naol_reviewer_header(line) is not None


def _parse_naol_reviewer_header(line: str) -> Optional[dict[str, str]]:
    match = re.match(r"^\s*(?P<name>.+?)\s*\((?P<rank>\d{1,2}[kKdDpP])\)", line)
    if not match:
        return None
    return {
        "reviewer_name": match.group("name").strip(),
        "reviewer_rank": match.group("rank").strip(),
    }


def _parse_naol_game_line(line: str) -> Optional[dict[str, str]]:
    match = re.match(
        r"^\s*(?P<player_one>.+?)\s+(?P<rank_one>\d{1,2}[kKdDpP])\s+"
        r"(?P<player_two>.+?)\s+(?P<rank_two>\d{1,2}[kKdDpP])\s*[-–—]\s*"
        r"(?P<game_link>https?://\S+)\.?\s*$",
        line,
    )
    if not match:
        return None
    return {
        "player_one_name": match.group("player_one").strip(),
        "player_one_rank": match.group("rank_one").strip(),
        "player_two_name": match.group("player_two").strip(),
        "player_two_rank": match.group("rank_two").strip(),
        "game_link": match.group("game_link").rstrip(").,").strip(),
    }


def _normalize_person_name(value: str) -> Optional[str]:
    tokens = re.findall(r"[A-Za-z]+(?:[-'][A-Za-z]+)?", value or "")
    if len(tokens) < 2:
        return None
    return f"{tokens[0].lower()} {tokens[-1].lower()}"


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


def _message_body_to_html(message: dict) -> Optional[str]:
    payload = message.get("payload") or {}
    return _extract_message_part_text(payload, "text/html")


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



def _lookup_members(
    conn_str: str,
    agaid: Optional[int],
    last_name_prefix: Optional[str],
    first_name_prefix: Optional[str],
    limit: int,
    offset: int,
) -> list[dict[str, object]]:
    effective_limit = min(max(limit, 1), 100)
    effective_offset = max(offset, 0)

    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "EXEC [api].[sp_lookup_members] @AGAID = ?, @LastNamePrefix = ?, @FirstNamePrefix = ?, @MaxRows = ?, @OffsetRows = ?",
            agaid,
            last_name_prefix,
            first_name_prefix,
            effective_limit + 1,
            effective_offset,
        )
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, record)) for record in cursor.fetchall()]
        cursor.close()
        return rows
    finally:
        conn.close()

def _json_safe_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value
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


def _repo_root() -> Path:
    return Path(__file__).resolve().parent



def _get_sql_connection_string() -> Optional[str]:
    conn = os.environ.get("SQL_CONNECTION_STRING")
    if conn:
        return conn
    settings_path = _repo_root() / "local.settings.json"
    if settings_path.exists():
        try:
            values = json.loads(settings_path.read_text(encoding="utf-8")).get("Values", {})
        except (OSError, json.JSONDecodeError):
            return None
        conn = values.get("SQL_CONNECTION_STRING") or values.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
        if conn:
            return conn
    return None



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

















