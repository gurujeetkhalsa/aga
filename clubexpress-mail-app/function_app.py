import base64
import html
import json
import logging
import os
import re
import threading
from datetime import date, datetime, timedelta, timezone
from email import message_from_bytes, policy
from email.message import EmailMessage
from email.parser import BytesParser
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable, Optional
from urllib import error, parse, request

import azure.functions as func
import certifi

from clubexpress_csv_parsers import (
    CHAPTER_COLUMNS,
    STAGING_COLUMNS,
    CsvValidationError,
    canonicalize_header as csv_canonicalize_header,
    decode_csv_text as csv_decode_csv_text,
    detect_attachment_report_type as detect_csv_attachment_report_type,
    is_chapter_header as csv_is_chapter_header,
    is_chapter_attachment_name as csv_is_chapter_attachment_name,
    is_member_agaid as csv_is_member_agaid,
    is_member_category_header as csv_is_member_category_header,
    is_memchap_header as csv_is_memchap_header,
    is_memchap_attachment_name as csv_is_memchap_attachment_name,
    normalize_header as csv_normalize_header,
    parse_chapter_rows as parse_csv_chapter_rows,
    parse_date as csv_parse_date,
    parse_datetime as csv_parse_datetime,
    parse_memchap_rows,
    parse_member_category_rows as parse_csv_member_category_rows,
    read_csv_header_canonical as csv_read_csv_header_canonical,
    read_csv_matrix as csv_read_csv_matrix,
)
from clubexpress_parsers import (
    EmailProcessingError,
    extract_chapter_renewal_notice_rows_from_html,
    extract_chapter_renewal_notice_rows_from_text,
    parse_chapter_renewal_notice,
    parse_new_member_email,
    parse_renewal_email,
)
from clubexpress_staging import (
    ClubExpressParsedEvent,
    DownstreamProcedure,
    build_chapter_renewal_notice_parsed_event,
    build_csv_attachment_parsed_event,
    build_journal_parsed_event,
    build_membership_parsed_event,
    result_payload_for_procedures,
    status_params as parsed_event_status_params,
)
from clubexpress_staged_processor import (
    BatchProcessResult,
    execute_downstream_calls,
    json_safe_value,
    list_pending_events,
    mark_event_status,
    process_pending_events,
    process_staged_event,
    replay_event,
)

try:
    import pyodbc
except Exception:
    class _MissingPyodbc:
        """Represent missing pyodbc."""
        Error = Exception

        @staticmethod
        def connect(*args, **kwargs):
            """Execute the connect routine."""
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
DEFAULT_REWARDS_SNAPSHOT_SCHEDULE = os.environ.get("REWARDS_SNAPSHOT_SCHEDULE", "0 10 5 * * *")
DEFAULT_REWARDS_MEMBERSHIP_AWARDS_SCHEDULE = os.environ.get("REWARDS_MEMBERSHIP_AWARDS_SCHEDULE", "0 20 5 * * *")
DEFAULT_REWARDS_RATED_GAME_AWARDS_SCHEDULE = os.environ.get("REWARDS_RATED_GAME_AWARDS_SCHEDULE", "0 30 5 * * *")
DEFAULT_REWARDS_TOURNAMENT_AWARDS_SCHEDULE = os.environ.get("REWARDS_TOURNAMENT_AWARDS_SCHEDULE", "0 35 5 * * *")
DEFAULT_REWARDS_EXPIRATIONS_SCHEDULE = os.environ.get("REWARDS_EXPIRATIONS_SCHEDULE", "0 40 5 * * *")
DEFAULT_PENDING_CHAPTER_RENEWALS_EMAIL_SCHEDULE = os.environ.get("PENDING_CHAPTER_RENEWALS_EMAIL_SCHEDULE", "0 50 5 * * *")
DEFAULT_STAGED_NEW_MEMBER_PROCESSOR_SCHEDULE = os.environ.get("CLUBEXPRESS_STAGED_NEW_MEMBER_PROCESSOR_SCHEDULE", "0 */5 * * * *")
DEFAULT_STAGED_RENEWAL_PROCESSOR_SCHEDULE = os.environ.get("CLUBEXPRESS_STAGED_RENEWAL_PROCESSOR_SCHEDULE", "0 */5 * * * *")
DEFAULT_STAGED_CHAPTER_RENEWAL_NOTICE_PROCESSOR_SCHEDULE = os.environ.get("CLUBEXPRESS_STAGED_CHAPTER_RENEWAL_NOTICE_PROCESSOR_SCHEDULE", "0 */5 * * * *")
DEFAULT_STAGED_MEMCHAP_PROCESSOR_SCHEDULE = os.environ.get("CLUBEXPRESS_STAGED_MEMCHAP_PROCESSOR_SCHEDULE", "0 */5 * * * *")
DEFAULT_STAGED_CHAPTER_PROCESSOR_SCHEDULE = os.environ.get("CLUBEXPRESS_STAGED_CHAPTER_PROCESSOR_SCHEDULE", "0 */5 * * * *")
DEFAULT_STAGED_MEMBER_CATEGORIES_PROCESSOR_SCHEDULE = os.environ.get("CLUBEXPRESS_STAGED_MEMBER_CATEGORIES_PROCESSOR_SCHEDULE", "0 */5 * * * *")
DEFAULT_STAGED_JOURNAL_PROCESSOR_SCHEDULE = os.environ.get("CLUBEXPRESS_STAGED_JOURNAL_PROCESSOR_SCHEDULE", "0 */5 * * * *")
DEFAULT_REWARDS_LEDGER_START_DATE = "2026-05-02"
NIGHTLY_MESSAGE_TYPE = "nightly_memchap_csv"
NIGHTLY_CATEGORY_MESSAGE_TYPE = "nightly_member_categories_csv"
CHAPTER_MESSAGE_TYPE = "chapter_csv"
CHAPTER_RENEWAL_NOTICE_MESSAGE_TYPE = "chapter_renewal_notice"
NEW_MEMBER_MESSAGE_TYPE = "new_member_signup"
RENEWAL_MESSAGE_TYPE = "member_renewal"
JOURNAL_MESSAGE_TYPE = "american_go_e_journal"
REWARDS_MEMBERSHIP_EVENT_PROC = "rewards.sp_record_membership_event"
REWARDS_DAILY_SNAPSHOT_PROC = "rewards.sp_create_daily_snapshot"
REWARDS_MEMBERSHIP_AWARDS_PROC = "rewards.sp_process_membership_awards"
REWARDS_RATED_GAME_AWARDS_PROC = "rewards.sp_process_rated_game_awards"
REWARDS_TOURNAMENT_AWARDS_PROC = "rewards.sp_process_tournament_awards"
REWARDS_EXPIRATIONS_PROC = "rewards.sp_process_point_expirations"
REWARDS_CHAPTER_RENEWAL_NOTICES_PROC = "rewards.sp_process_chapter_renewal_notices"
REWARDS_CHAPTER_RENEWAL_CONFIRMATION_PROC = "rewards.sp_record_chapter_renewal_confirmation"
REWARDS_PENDING_CHAPTER_RENEWALS_PROC = "rewards.sp_get_pending_chapter_renewals"
CLUBEXPRESS_PARSED_EVENT_STAGING_PROC = "membership.sp_record_clubexpress_parsed_event"
CLUBEXPRESS_PARSED_EVENT_STATUS_PROC = "membership.sp_update_clubexpress_parsed_event_status"
REWARDS_NEW_MEMBERSHIP_EVENT_TYPE = "new_membership"
REWARDS_RENEWAL_EVENT_TYPE = "renewal"
MEMCHAP_IMPORT_ACTION = "membership.import_memchap_csv"
CHAPTER_IMPORT_ACTION = "membership.import_chapter_csv"
MEMBER_CATEGORIES_IMPORT_ACTION = "membership.import_member_categories_csv"
JOURNAL_NEWS_PROC = "membership.sp_process_journal_news_email"
CHAPTER_RENEWAL_NOTICE_SUBJECT = "Membership Renewal Emails"
CHAPTER_RENEWAL_POINTS = 35000
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
JOURNAL_FIRST_NAME_ALIAS_GROUPS = (
    ("abigail", "abby", "abbie", "gail"),
    ("alexander", "alex", "xander"),
    ("alexandra", "alex", "lexi", "sandra"),
    ("andrew", "andy", "drew"),
    ("anthony", "tony"),
    ("barbara", "barb", "barbie"),
    ("benjamin", "ben", "benny"),
    ("catherine", "cathy", "kathy", "kate", "katie"),
    ("charles", "charlie", "chuck"),
    ("christopher", "chris"),
    ("daniel", "dan", "danny"),
    ("david", "dave", "davey"),
    ("deborah", "deb", "debbie"),
    ("donald", "don", "donny"),
    ("dorothy", "dot", "dottie"),
    ("edward", "ed", "eddie", "ted", "teddy", "ned"),
    ("edwin", "ed", "eddie", "ned"),
    ("elizabeth", "beth", "betsy", "betty", "eliza", "liz", "lizzie"),
    ("frances", "fran", "frannie"),
    ("frederick", "fred", "freddie"),
    ("george", "geo"),
    ("gerald", "gerry", "jerry"),
    ("gregory", "greg"),
    ("james", "jim", "jimmy"),
    ("janet", "jan"),
    ("jennifer", "jen", "jenny"),
    ("john", "jack", "johnny"),
    ("jonathan", "jon", "johnny"),
    ("joseph", "joe", "joey"),
    ("katherine", "kathy", "kate", "katie"),
    ("kenneth", "ken", "kenny"),
    ("kimberly", "kim"),
    ("lawrence", "larry"),
    ("margaret", "maggie", "meg", "peggy"),
    ("matthew", "matt"),
    ("michael", "mike", "mikey"),
    ("nicholas", "nick", "nicky"),
    ("patricia", "pat", "patty", "tricia"),
    ("patrick", "pat"),
    ("rebecca", "becky", "becca"),
    ("richard", "rick", "ricky", "dick"),
    ("robert", "rob", "robbie", "bob", "bobby"),
    ("ronald", "ron", "ronnie"),
    ("samuel", "sam", "sammy"),
    ("stephen", "steve", "stevie"),
    ("steven", "steve", "stevie"),
    ("susan", "sue", "susie", "suzie"),
    ("theodore", "ted", "teddy", "theo"),
    ("thomas", "tom", "tommy"),
    ("timothy", "tim", "timmy"),
    ("victoria", "vicki", "vicky", "tori"),
    ("william", "bill", "billy", "will", "willy", "liam"),
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
        ranked.[Sigma],
        ranked.[LastUpdate]
    FROM
    (
        SELECT
            r.[Pin_Player] AS [AGAID],
            r.[Rating],
            r.[Sigma],
            r.[Elab_Date] AS [LastUpdate],
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
    m.[State],
    cr.[Rating],
    cr.[Sigma],
    cr.[LastUpdate],
    c.[ChapterCode],
    c.[ChapterName]
FROM [membership].[members] AS m
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
LEFT JOIN current_ratings AS cr
    ON cr.[AGAID] = m.[AGAID]
WHERE m.[AGAID] < ?
  AND (m.[Status] IS NULL OR UPPER(LTRIM(RTRIM(m.[Status]))) <> N'DROPPED')
ORDER BY m.[LastName], m.[FirstName], m.[AGAID]
"""

JOURNAL_NLP_MODEL = "en_core_web_sm"
JOURNAL_NLP_EXCLUDE = ["tagger", "parser", "lemmatizer", "attribute_ruler"]
_journal_name_nlp = None
_journal_name_nlp_attempted = False
_journal_name_nlp_lock = threading.Lock()


class GmailApiError(RuntimeError):
    """Represent gmail api error failures."""
    pass


class _JournalHtmlParser(HTMLParser):
    """Represent journal html parser."""
    def __init__(self) -> None:
        """Initialize the journal html parser instance."""
        super().__init__(convert_charrefs=True)
        self.blocks: list[dict[str, object]] = []
        self._text_parts: list[str] = []
        self._links: list[str] = []
        self._heading_level: Optional[int] = None
        self._link_href: Optional[str] = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        """Handle starttag."""
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
        """Handle endtag."""
        normalized_tag = tag.lower()
        if normalized_tag == "a":
            self._link_href = None
        if normalized_tag.startswith("h") and len(normalized_tag) == 2 and normalized_tag[1].isdigit():
            self._flush_block()
            self._heading_level = None
        if normalized_tag in {"p", "div", "li", "tr", "table", "section", "article"}:
            self._flush_block()

    def handle_data(self, data: str) -> None:
        """Handle data."""
        if not data:
            return
        self._text_parts.append(data)
        if self._link_href:
            self._links.append(self._link_href)

    def close(self) -> None:
        """Execute the close routine."""
        super().close()
        self._flush_block()

    def _flush_block(self) -> None:
        """Execute the flush block routine."""
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
    """Represent journal visible text parser."""
    def __init__(self) -> None:
        """Initialize the journal visible text parser instance."""
        super().__init__(convert_charrefs=True)
        self._skip_depth = 0
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        """Handle starttag."""
        normalized_tag = tag.lower()
        if normalized_tag in {"script", "style", "svg", "noscript"}:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if normalized_tag in {"br", "p", "div", "li", "tr", "table", "section", "article", "h1", "h2", "h3", "h4", "h5", "h6", "td"}:
            self._parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        """Handle endtag."""
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
        """Handle data."""
        if self._skip_depth:
            return
        if data:
            self._parts.append(data)

    def get_lines(self) -> list[str]:
        """Return lines."""
        lines = []
        for raw_line in "".join(self._parts).splitlines():
            normalized = re.sub(r"\s+", " ", raw_line).strip()
            if normalized:
                lines.append(normalized)
        return lines


class _NaolReviewHtmlParser(HTMLParser):
    """Represent naol review html parser."""
    _BLOCK_TAGS = {"p", "div", "li", "tr", "table", "section", "article", "h1", "h2", "h3", "h4", "h5", "h6", "td"}
    _SKIP_TAGS = {"script", "style", "svg", "noscript"}

    def __init__(self) -> None:
        """Initialize the naol review html parser instance."""
        super().__init__(convert_charrefs=True)
        self._skip_depth = 0
        self._parts: list[str] = []
        self._seen_video_links: set[str] = set()
        self.tokens: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        """Handle starttag."""
        normalized_tag = tag.lower()
        if normalized_tag in self._SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if normalized_tag in self._BLOCK_TAGS or normalized_tag == "br":
            self._flush_text()
        if normalized_tag == "iframe":
            self._flush_text()
            attr_map = {name.lower(): value for name, value in attrs if name and value}
            video_link = _normalize_video_link(attr_map.get("src", ""))
            if not video_link:
                return
            if video_link in self._seen_video_links:
                return
            self._seen_video_links.add(video_link)
            self.tokens.append({"video_link": video_link[:1000]})

    def handle_endtag(self, tag: str) -> None:
        """Handle endtag."""
        normalized_tag = tag.lower()
        if normalized_tag in self._SKIP_TAGS:
            if self._skip_depth:
                self._skip_depth -= 1
            return
        if self._skip_depth:
            return
        if normalized_tag in self._BLOCK_TAGS:
            self._flush_text()

    def handle_data(self, data: str) -> None:
        """Handle data."""
        if self._skip_depth:
            return
        if data:
            self._parts.append(data)

    def _flush_text(self) -> None:
        """Execute the flush text routine."""
        text = re.sub(r"\s+", " ", "".join(self._parts)).strip()
        if text:
            self.tokens.append({"text": text})
        self._parts = []

    def get_tokens(self) -> list[dict[str, str]]:
        """Return tokens."""
        self._flush_text()
        return self.tokens


@app.timer_trigger(schedule=DEFAULT_MAILBOX_POLL_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def poll_clubexpress_mailbox(timer: func.TimerRequest) -> None:
    """Execute the poll clubexpress mailbox routine."""
    if not _is_truthy(os.environ.get("CLUBEXPRESS_MAILBOX_ENABLED", "false")):
        logging.info("ClubExpress mailbox polling is disabled.")
        return

    try:
        access_token = _get_gmail_access_token()
        messages = _list_gmail_messages(access_token)
        logging.info("Fetched %s candidate Gmail messages", len(messages))

        for message in _fetch_gmail_messages_for_processing(access_token, messages):
            message_id = _message_identifier(message)
            try:
                _process_mailbox_message(access_token, message)
            except Exception:
                logging.exception("Failed processing Gmail message %s", message_id)
    except Exception:
        logging.exception("ClubExpress Gmail poll failed")


def _fetch_gmail_messages_for_processing(access_token: str, items: list[dict]) -> list[dict]:
    """Fetch gmail messages for processing."""
    messages: list[dict] = []
    for item in items:
        message_id = item.get("id")
        if not message_id:
            continue
        try:
            messages.append(_get_gmail_message(access_token, message_id))
        except Exception:
            logging.exception("Failed fetching Gmail message %s", message_id)

    messages.sort(key=lambda message: (_message_received_at(message), _message_identifier(message)))
    if messages:
        logging.info(
            "Processing %s Gmail messages oldest-to-newest from %s to %s",
            len(messages),
            _message_received_at(messages[0]).isoformat(),
            _message_received_at(messages[-1]).isoformat(),
        )
    return messages


@app.timer_trigger(schedule=DEFAULT_STAGED_NEW_MEMBER_PROCESSOR_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def process_staged_new_member_events(timer: func.TimerRequest) -> None:
    """Process staged new member events."""
    if not _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_NEW_MEMBER_PROCESSOR_ENABLED", "false")):
        logging.info("ClubExpress staged new-member processor is disabled.")
        return

    conn_str = _get_sql_connection_string()
    if not conn_str:
        logging.error("Missing SQL_CONNECTION_STRING application setting.")
        return

    batch_size = _env_positive_int("CLUBEXPRESS_STAGED_NEW_MEMBER_PROCESSOR_BATCH_SIZE", 25)
    try:
        result = process_pending_events(
            _ClubExpressStagedEventSqlAdapter(conn_str),
            event_type=REWARDS_NEW_MEMBERSHIP_EVENT_TYPE,
            top=batch_size,
            execute=True,
            confirm_replay=True,
            processor_name="staged_new_member_processor",
        )
        logging.info(
            "ClubExpress staged new-member processing complete. selected=%s processed=%s errors=%s",
            result.selected_count,
            result.processed_count,
            result.error_count,
        )
    except Exception:
        logging.exception("ClubExpress staged new-member processing failed.")


@app.timer_trigger(schedule=DEFAULT_STAGED_RENEWAL_PROCESSOR_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def process_staged_renewal_events(timer: func.TimerRequest) -> None:
    """Process staged renewal events."""
    if not _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_RENEWAL_PROCESSOR_ENABLED", "false")):
        logging.info("ClubExpress staged renewal processor is disabled.")
        return

    conn_str = _get_sql_connection_string()
    if not conn_str:
        logging.error("Missing SQL_CONNECTION_STRING application setting.")
        return

    batch_size = _env_positive_int("CLUBEXPRESS_STAGED_RENEWAL_PROCESSOR_BATCH_SIZE", 25)
    try:
        result = process_pending_events(
            _ClubExpressStagedEventSqlAdapter(conn_str),
            event_type=REWARDS_RENEWAL_EVENT_TYPE,
            top=batch_size,
            execute=True,
            confirm_replay=True,
            processor_name="staged_renewal_processor",
        )
        logging.info(
            "ClubExpress staged renewal processing complete. selected=%s processed=%s errors=%s",
            result.selected_count,
            result.processed_count,
            result.error_count,
        )
    except Exception:
        logging.exception("ClubExpress staged renewal processing failed.")


@app.timer_trigger(schedule=DEFAULT_STAGED_CHAPTER_RENEWAL_NOTICE_PROCESSOR_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def process_staged_chapter_renewal_notice_events(timer: func.TimerRequest) -> None:
    """Process staged chapter renewal notice events."""
    if not _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_CHAPTER_RENEWAL_NOTICE_PROCESSOR_ENABLED", "false")):
        logging.info("ClubExpress staged chapter-renewal notice processor is disabled.")
        return

    conn_str = _get_sql_connection_string()
    if not conn_str:
        logging.error("Missing SQL_CONNECTION_STRING application setting.")
        return

    batch_size = _env_positive_int("CLUBEXPRESS_STAGED_CHAPTER_RENEWAL_NOTICE_PROCESSOR_BATCH_SIZE", 10)
    try:
        result = _process_pending_chapter_renewal_notice_events(
            conn_str,
            top=batch_size,
            execute=True,
            confirm_replay=True,
            processor_name="staged_chapter_renewal_notice_processor",
        )
        logging.info(
            "ClubExpress staged chapter-renewal notice processing complete. selected=%s processed=%s errors=%s",
            result.selected_count,
            result.processed_count,
            result.error_count,
        )
    except Exception:
        logging.exception("ClubExpress staged chapter-renewal notice processing failed.")


@app.timer_trigger(schedule=DEFAULT_STAGED_MEMCHAP_PROCESSOR_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def process_staged_memchap_events(timer: func.TimerRequest) -> None:
    """Process staged memchap events."""
    if not _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_MEMCHAP_PROCESSOR_ENABLED", "false")):
        logging.info("ClubExpress staged MemChap processor is disabled.")
        return

    conn_str = _get_sql_connection_string()
    if not conn_str:
        logging.error("Missing SQL_CONNECTION_STRING application setting.")
        return

    batch_size = _env_positive_int("CLUBEXPRESS_STAGED_MEMCHAP_PROCESSOR_BATCH_SIZE", 5)
    try:
        result = _process_pending_memchap_events(
            conn_str,
            top=batch_size,
            execute=True,
            confirm_replay=True,
            processor_name="staged_memchap_processor",
        )
        logging.info(
            "ClubExpress staged MemChap processing complete. selected=%s processed=%s errors=%s",
            result.selected_count,
            result.processed_count,
            result.error_count,
        )
    except Exception:
        logging.exception("ClubExpress staged MemChap processing failed.")


@app.timer_trigger(schedule=DEFAULT_STAGED_CHAPTER_PROCESSOR_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def process_staged_chapter_events(timer: func.TimerRequest) -> None:
    """Process staged chapter events."""
    if not _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_CHAPTER_PROCESSOR_ENABLED", "false")):
        logging.info("ClubExpress staged ChapterX processor is disabled.")
        return

    conn_str = _get_sql_connection_string()
    if not conn_str:
        logging.error("Missing SQL_CONNECTION_STRING application setting.")
        return

    batch_size = _env_positive_int("CLUBEXPRESS_STAGED_CHAPTER_PROCESSOR_BATCH_SIZE", 5)
    try:
        result = _process_pending_chapter_events(
            conn_str,
            top=batch_size,
            execute=True,
            confirm_replay=True,
            processor_name="staged_chapter_processor",
        )
        logging.info(
            "ClubExpress staged ChapterX processing complete. selected=%s processed=%s errors=%s",
            result.selected_count,
            result.processed_count,
            result.error_count,
        )
    except Exception:
        logging.exception("ClubExpress staged ChapterX processing failed.")


@app.timer_trigger(schedule=DEFAULT_STAGED_MEMBER_CATEGORIES_PROCESSOR_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def process_staged_member_category_events(timer: func.TimerRequest) -> None:
    """Process staged member category events."""
    if not _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_MEMBER_CATEGORIES_PROCESSOR_ENABLED", "false")):
        logging.info("ClubExpress staged member-category processor is disabled.")
        return

    conn_str = _get_sql_connection_string()
    if not conn_str:
        logging.error("Missing SQL_CONNECTION_STRING application setting.")
        return

    batch_size = _env_positive_int("CLUBEXPRESS_STAGED_MEMBER_CATEGORIES_PROCESSOR_BATCH_SIZE", 5)
    try:
        result = _process_pending_member_category_events(
            conn_str,
            top=batch_size,
            execute=True,
            confirm_replay=True,
            processor_name="staged_member_category_processor",
        )
        logging.info(
            "ClubExpress staged member-category processing complete. selected=%s processed=%s errors=%s",
            result.selected_count,
            result.processed_count,
            result.error_count,
        )
    except Exception:
        logging.exception("ClubExpress staged member-category processing failed.")


@app.timer_trigger(schedule=DEFAULT_STAGED_JOURNAL_PROCESSOR_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def process_staged_journal_events(timer: func.TimerRequest) -> None:
    """Process staged journal events."""
    if not _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_JOURNAL_PROCESSOR_ENABLED", "false")):
        logging.info("ClubExpress staged E-Journal processor is disabled.")
        return

    conn_str = _get_sql_connection_string()
    if not conn_str:
        logging.error("Missing SQL_CONNECTION_STRING application setting.")
        return

    batch_size = _env_positive_int("CLUBEXPRESS_STAGED_JOURNAL_PROCESSOR_BATCH_SIZE", 10)
    try:
        result = _process_pending_journal_events(
            conn_str,
            top=batch_size,
            execute=True,
            confirm_replay=True,
            processor_name="staged_journal_processor",
        )
        logging.info(
            "ClubExpress staged E-Journal processing complete. selected=%s processed=%s errors=%s",
            result.selected_count,
            result.processed_count,
            result.error_count,
        )
    except Exception:
        logging.exception("ClubExpress staged E-Journal processing failed.")


@app.timer_trigger(schedule=DEFAULT_REWARDS_SNAPSHOT_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def create_rewards_daily_snapshot(timer: func.TimerRequest) -> None:
    """Create rewards daily snapshot."""
    if not _is_truthy(os.environ.get("REWARDS_SNAPSHOT_ENABLED", "true")):
        logging.info("Chapter Rewards daily snapshot is disabled.")
        return

    conn_str = _get_sql_connection_string()
    if not conn_str:
        logging.error("Missing SQL_CONNECTION_STRING application setting.")
        return

    snapshot_date = _rewards_snapshot_date()
    try:
        rows = _execute_stored_procedure_rows(
            conn_str,
            REWARDS_DAILY_SNAPSHOT_PROC,
            _rewards_snapshot_params(snapshot_date),
        )
        row = rows[0] if rows else {}
        logging.info(
            "Chapter Rewards snapshot complete. date=%s run_id=%s already_existed=%s members=%s chapters=%s current_chapters=%s multipliers=1x:%s 2x:%s 3x:%s",
            snapshot_date.isoformat(),
            row.get("RunID"),
            row.get("AlreadyExisted"),
            row.get("MemberSnapshotCount"),
            row.get("ChapterSnapshotCount"),
            row.get("CurrentChapterCount"),
            row.get("Multiplier1ChapterCount"),
            row.get("Multiplier2ChapterCount"),
            row.get("Multiplier3ChapterCount"),
        )
    except Exception:
        logging.exception("Chapter Rewards daily snapshot failed for %s", snapshot_date.isoformat())


@app.timer_trigger(schedule=DEFAULT_REWARDS_MEMBERSHIP_AWARDS_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def process_rewards_membership_awards(timer: func.TimerRequest) -> None:
    """Process rewards membership awards."""
    if not _is_truthy(os.environ.get("REWARDS_MEMBERSHIP_AWARDS_ENABLED", "true")):
        logging.info("Chapter Rewards membership awards are disabled.")
        return

    conn_str = _get_sql_connection_string()
    if not conn_str:
        logging.error("Missing SQL_CONNECTION_STRING application setting.")
        return

    as_of_date = _rewards_snapshot_date()
    try:
        rows = _execute_stored_procedure_rows(
            conn_str,
            REWARDS_MEMBERSHIP_AWARDS_PROC,
            _rewards_membership_awards_params(as_of_date),
        )
        row = rows[0] if rows else {}
        logging.info(
            "Chapter Rewards membership awards complete. as_of=%s run_id=%s pending=%s eligible=%s already_awarded=%s new_awards=%s points=%s expiring_no_chapter=%s waiting_for_chapter=%s missing_snapshot_coverage=%s ineligible=%s",
            as_of_date.isoformat(),
            row.get("RunID"),
            row.get("PendingEventCount"),
            row.get("EligibleEventCount"),
            row.get("AlreadyAwardedCount"),
            row.get("NewAwardCount"),
            row.get("PointTotal"),
            row.get("ExpiringNoChapterCount"),
            row.get("WaitingForChapterCount"),
            row.get("MissingSnapshotCoverageCount"),
            row.get("IneligibleCount"),
        )
    except Exception:
        logging.exception("Chapter Rewards membership awards failed for %s", as_of_date.isoformat())


@app.timer_trigger(schedule=DEFAULT_REWARDS_RATED_GAME_AWARDS_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def process_rewards_rated_game_awards(timer: func.TimerRequest) -> None:
    """Process rewards rated game awards."""
    if not _is_truthy(os.environ.get("REWARDS_RATED_GAME_AWARDS_ENABLED", "true")):
        logging.info("Chapter Rewards rated-game awards are disabled.")
        return

    conn_str = _get_sql_connection_string()
    if not conn_str:
        logging.error("Missing SQL_CONNECTION_STRING application setting.")
        return

    game_date = _rewards_snapshot_date()
    params = _rewards_rated_game_awards_params(game_date)
    try:
        rows = _execute_stored_procedure_rows(
            conn_str,
            REWARDS_RATED_GAME_AWARDS_PROC,
            params,
        )
        row = rows[0] if rows else {}
        logging.info(
            "Chapter Rewards rated-game awards complete. date_from=%s date_to=%s run_id=%s participants=%s eligible=%s already_awarded=%s new_awards=%s points=%s missing_member_snapshot=%s missing_chapter_snapshot=%s inactive_player=%s no_chapter=%s chapter_not_current=%s",
            params["GameDateFrom"].isoformat(),
            game_date.isoformat(),
            row.get("RunID"),
            row.get("ParticipantCount"),
            row.get("EligibleAwardCount"),
            row.get("AlreadyAwardedCount"),
            row.get("NewAwardCount"),
            row.get("PointTotal"),
            row.get("MissingMemberSnapshotCount"),
            row.get("MissingChapterSnapshotCount"),
            row.get("InactivePlayerCount"),
            row.get("NoChapterCount"),
            row.get("ChapterNotCurrentCount"),
        )
    except Exception:
        logging.exception("Chapter Rewards rated-game awards failed for %s", game_date.isoformat())


@app.timer_trigger(schedule=DEFAULT_REWARDS_TOURNAMENT_AWARDS_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def process_rewards_tournament_awards(timer: func.TimerRequest) -> None:
    """Process rewards tournament awards."""
    if not _is_truthy(os.environ.get("REWARDS_TOURNAMENT_AWARDS_ENABLED", "true")):
        logging.info("Chapter Rewards tournament awards are disabled.")
        return

    conn_str = _get_sql_connection_string()
    if not conn_str:
        logging.error("Missing SQL_CONNECTION_STRING application setting.")
        return

    tournament_date_to = _rewards_snapshot_date()
    try:
        rows = _execute_stored_procedure_rows(
            conn_str,
            REWARDS_TOURNAMENT_AWARDS_PROC,
            _rewards_tournament_awards_params(tournament_date_to),
        )
        row = rows[0] if rows else {}
        logging.info(
            "Chapter Rewards tournament awards complete. date_to=%s run_id=%s groups=%s sections=%s rated_games=%s host_new_awards=%s host_points=%s state_new_awards=%s state_points=%s new_awards=%s points=%s missing_host=%s missing_reward_event_key=%s",
            tournament_date_to.isoformat(),
            row.get("RunID"),
            row.get("EventGroupCount"),
            row.get("TournamentSectionCount"),
            row.get("RatedGameCount"),
            row.get("HostNewAwardCount"),
            row.get("HostPointTotal"),
            row.get("StateNewAwardCount"),
            row.get("StateChampionshipPointTotal"),
            row.get("NewAwardCount"),
            row.get("PointTotal"),
            row.get("MissingHostChapterCount"),
            row.get("MissingRewardEventKeyCount"),
        )
    except Exception:
        logging.exception("Chapter Rewards tournament awards failed through %s", tournament_date_to.isoformat())


@app.timer_trigger(schedule=DEFAULT_REWARDS_EXPIRATIONS_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def process_rewards_point_expirations(timer: func.TimerRequest) -> None:
    """Process rewards point expirations."""
    if not _is_truthy(os.environ.get("REWARDS_EXPIRATIONS_ENABLED", "true")):
        logging.info("Chapter Rewards point expirations are disabled.")
        return

    conn_str = _get_sql_connection_string()
    if not conn_str:
        logging.error("Missing SQL_CONNECTION_STRING application setting.")
        return

    as_of_date = _rewards_snapshot_date()
    try:
        rows = _execute_stored_procedure_rows(
            conn_str,
            REWARDS_EXPIRATIONS_PROC,
            _rewards_point_expirations_params(as_of_date),
        )
        row = rows[0] if rows else {}
        logging.info(
            "Chapter Rewards point expirations complete. as_of=%s run_id=%s expiring_lots=%s already_expired=%s new_expirations=%s points=%s chapters=%s",
            as_of_date.isoformat(),
            row.get("RunID"),
            row.get("ExpiringLotCount"),
            row.get("AlreadyExpiredCount"),
            row.get("NewExpirationCount"),
            row.get("ExpiredPointTotal"),
            row.get("ChapterCount"),
        )
    except Exception:
        logging.exception("Chapter Rewards point expirations failed for %s", as_of_date.isoformat())


@app.timer_trigger(schedule=DEFAULT_PENDING_CHAPTER_RENEWALS_EMAIL_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def send_pending_chapter_renewals_email(timer: func.TimerRequest) -> None:
    """Send pending chapter renewals email."""
    if not _is_truthy(os.environ.get("PENDING_CHAPTER_RENEWALS_EMAIL_ENABLED", "true")):
        logging.info("Pending chapter renewal email is disabled.")
        return

    conn_str = _get_sql_connection_string()
    if not conn_str:
        logging.error("Missing SQL_CONNECTION_STRING application setting.")
        return

    as_of_date = _rewards_snapshot_date()
    try:
        rows = _execute_stored_procedure_rows(
            conn_str,
            REWARDS_PENDING_CHAPTER_RENEWALS_PROC,
            {"AsOfDate": as_of_date},
        )
        access_token = _get_gmail_access_token()
        sent = _send_pending_chapter_renewals_email_if_configured(access_token, rows, as_of_date)
        logging.info(
            "Pending chapter renewal email complete. as_of=%s pending=%s sent=%s",
            as_of_date.isoformat(),
            len(rows),
            sent,
        )
    except Exception:
        logging.exception("Pending chapter renewal email failed for %s", as_of_date.isoformat())


def _process_mailbox_message(access_token: str, message: dict) -> None:
    """Process mailbox message."""
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
            if _clubexpress_staged_memchap_consumption_enabled():
                parsed_event = _build_memchap_parsed_event(
                    message_id=message_id,
                    message_type=message_type,
                    received_at=received_at,
                    event_date=received_date,
                    attachments=attachments,
                    sender=sender,
                    subject=subject,
                    blob_path=archive_path,
                )
                rows_staged = parsed_event.parsed_item_count
                _record_clubexpress_parsed_event(conn_str, parsed_event)
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
                        "Status": "staged",
                        "ErrorMessage": None,
                    },
                )
            else:
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
        if _clubexpress_staged_memchap_consumption_enabled():
            logging.info("Nightly MemChap message staged. rows=%s archive_path=%s", rows_staged, archive_path)
        else:
            logging.info("Nightly MemChap message processed. rows_staged=%s archive_path=%s", rows_staged, archive_path)
    elif message_type == CHAPTER_MESSAGE_TYPE:
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
            if _clubexpress_staged_chapter_consumption_enabled():
                parsed_event = _build_chapter_parsed_event(
                    message_id=message_id,
                    message_type=message_type,
                    received_at=received_at,
                    event_date=received_date,
                    attachments=attachments,
                    sender=sender,
                    subject=subject,
                    blob_path=archive_path,
                )
                rows_staged = parsed_event.parsed_item_count
                _record_clubexpress_parsed_event(conn_str, parsed_event)
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
                        "Status": "staged",
                        "ErrorMessage": None,
                    },
                )
            else:
                rows_staged = _handle_chapter_email(conn_str, attachments)
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
        if _clubexpress_staged_chapter_consumption_enabled():
            logging.info("Chapter CSV message staged. rows=%s archive_path=%s", rows_staged, archive_path)
        else:
            logging.info("Chapter CSV message processed. rows_staged=%s archive_path=%s", rows_staged, archive_path)
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
            if _clubexpress_staged_member_categories_consumption_enabled():
                parsed_event = _build_member_categories_parsed_event(
                    message_id=message_id,
                    message_type=message_type,
                    received_at=received_at,
                    event_date=received_date,
                    attachments=attachments,
                    sender=sender,
                    subject=subject,
                    blob_path=archive_path,
                )
                rows_staged = parsed_event.parsed_item_count
                _record_clubexpress_parsed_event(conn_str, parsed_event)
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
                        "Status": "staged",
                        "ErrorMessage": None,
                    },
                )
            else:
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
        if _clubexpress_staged_member_categories_consumption_enabled():
            logging.info("Nightly category message staged. rows=%s archive_path=%s", rows_staged, archive_path)
        else:
            logging.info("Nightly category message processed. rows_staged=%s archive_path=%s", rows_staged, archive_path)
    elif message_type == NEW_MEMBER_MESSAGE_TYPE:
        message_id = _message_identifier(message)
        parsed = _parse_new_member_email(_message_body_to_text(message))
        membership_params = {
            "MessageId": message_id,
            "ReceivedAt": received_at,
            "AGAID": parsed["AGAID"],
            "MemberType": parsed["MemberType"],
            "FirstName": parsed["FirstName"],
            "LastName": parsed["LastName"],
            "EmailAddress": parsed.get("EmailAddress"),
            "JoinDate": received_date,
            "ExpirationDate": parsed.get("ExpirationDate") or _default_membership_expiration_date(received_date, parsed.get("MemberType")),
            "Sender": sender or None,
            "Subject": subject or None,
            "BlobPath": archive_path,
        }
        rewards_params = _membership_reward_event_params(
            message,
            received_at,
            REWARDS_NEW_MEMBERSHIP_EVENT_TYPE,
            received_date,
            parsed,
            sender=sender,
            subject=subject,
            blob_path=archive_path,
        )
        downstream_procedures = [
            DownstreamProcedure("membership.sp_process_new_member_email", membership_params),
            DownstreamProcedure(REWARDS_MEMBERSHIP_EVENT_PROC, rewards_params),
        ]
        parsed_event = build_membership_parsed_event(
            message_id=message_id,
            message_type=message_type,
            event_type=REWARDS_NEW_MEMBERSHIP_EVENT_TYPE,
            received_at=received_at,
            event_date=received_date,
            parsed=parsed,
            downstream_procedures=downstream_procedures,
            sender=sender or None,
            subject=subject or None,
            blob_path=archive_path,
        )
        _record_clubexpress_parsed_event(conn_str, parsed_event)
        if _clubexpress_staged_new_member_consumption_enabled():
            logging.info(
                "New member email staged for async processing. AGAID=%s event_key=%s archive_path=%s",
                parsed["AGAID"],
                parsed_event.event_key,
                archive_path,
            )
        else:
            try:
                _execute_stored_procedures(conn_str, _downstream_procedure_calls(downstream_procedures))
                _mark_clubexpress_parsed_event_processed(
                    conn_str,
                    parsed_event,
                    result_payload_for_procedures(downstream_procedures),
                )
            except Exception as exc:
                _mark_clubexpress_parsed_event_error(conn_str, parsed_event, exc)
                raise
            logging.info("New member email processed for AGAID=%s archive_path=%s", parsed["AGAID"], archive_path)
    elif message_type == RENEWAL_MESSAGE_TYPE:
        message_id = _message_identifier(message)
        parsed = _parse_renewal_email(_message_body_to_text(message))
        membership_params = {
            "MessageId": message_id,
            "ReceivedAt": received_at,
            "AGAID": parsed["AGAID"],
            "ExpirationDate": parsed.get("ExpirationDate") or _default_membership_expiration_date(received_date, parsed.get("MemberType")),
            "PhoneNumber": parsed.get("PhoneNumber"),
            "EmailAddress": parsed.get("EmailAddress"),
            "LoginName": parsed.get("LoginName"),
            "MemberType": parsed.get("MemberType"),
            "IsChapterMember": 1 if parsed["IsChapterMember"] else 0,
            "Sender": sender or None,
            "Subject": subject or None,
            "BlobPath": archive_path,
        }
        rewards_params = _membership_reward_event_params(
            message,
            received_at,
            REWARDS_RENEWAL_EVENT_TYPE,
            received_date,
            parsed,
            sender=sender,
            subject=subject,
            blob_path=archive_path,
        )
        downstream_procedures = [
            DownstreamProcedure("membership.sp_process_membership_renewal", membership_params),
            DownstreamProcedure(REWARDS_MEMBERSHIP_EVENT_PROC, rewards_params),
        ]
        confirmation_params = None
        if parsed["IsChapterMember"]:
            confirmation_params = _chapter_renewal_confirmation_params(
                message,
                received_at,
                parsed,
                sender=sender,
                subject=subject,
                blob_path=archive_path,
            )
            downstream_procedures.append(
                DownstreamProcedure(REWARDS_CHAPTER_RENEWAL_CONFIRMATION_PROC, confirmation_params)
            )
        parsed_event = build_membership_parsed_event(
            message_id=message_id,
            message_type=message_type,
            event_type=REWARDS_RENEWAL_EVENT_TYPE,
            received_at=received_at,
            event_date=received_date,
            parsed=parsed,
            downstream_procedures=downstream_procedures,
            sender=sender or None,
            subject=subject or None,
            blob_path=archive_path,
        )
        _record_clubexpress_parsed_event(conn_str, parsed_event)
        if _clubexpress_staged_renewal_consumption_enabled():
            logging.info(
                "Renewal email staged for async processing. AGAID=%s event_key=%s archive_path=%s",
                parsed["AGAID"],
                parsed_event.event_key,
                archive_path,
            )
        else:
            try:
                _execute_stored_procedures(conn_str, _downstream_procedure_calls(downstream_procedures[:2]))
                confirmation = None
                if confirmation_params:
                    confirmation_rows = _execute_stored_procedure_rows(
                        conn_str,
                        REWARDS_CHAPTER_RENEWAL_CONFIRMATION_PROC,
                        confirmation_params,
                    )
                    confirmation = confirmation_rows[0] if confirmation_rows else {}
                    logging.info(
                        "Chapter renewal confirmation processed for ChapterID=%s recorded=%s reason=%s notice_id=%s",
                        parsed["AGAID"],
                        confirmation.get("Recorded"),
                        confirmation.get("Reason"),
                        confirmation.get("NoticeID"),
                    )
                result_payload = result_payload_for_procedures(downstream_procedures)
                if confirmation is not None:
                    result_payload["chapter_renewal_confirmation"] = confirmation
                _mark_clubexpress_parsed_event_processed(conn_str, parsed_event, result_payload)
            except Exception as exc:
                _mark_clubexpress_parsed_event_error(conn_str, parsed_event, exc)
                raise
            logging.info("Renewal email processed for AGAID=%s archive_path=%s", parsed["AGAID"], archive_path)
    elif message_type == CHAPTER_RENEWAL_NOTICE_MESSAGE_TYPE:
        message_id = _message_identifier(message)
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
            parsed_rows = _parse_chapter_renewal_notice_email(message)
            notice_params = _chapter_renewal_notice_params(
                message,
                received_at,
                received_date,
                parsed_rows,
                sender=sender,
                subject=subject,
                blob_path=archive_path,
            )
            downstream_procedures = [
                DownstreamProcedure(REWARDS_CHAPTER_RENEWAL_NOTICES_PROC, notice_params),
            ]
            parsed_event = build_chapter_renewal_notice_parsed_event(
                message_id=message_id,
                message_type=message_type,
                event_type=CHAPTER_RENEWAL_NOTICE_MESSAGE_TYPE,
                received_at=received_at,
                notice_date=received_date,
                parsed_rows=parsed_rows,
                downstream_procedures=downstream_procedures,
                sender=sender or None,
                subject=subject or None,
                blob_path=archive_path,
            )
            _record_clubexpress_parsed_event(conn_str, parsed_event)
            if _clubexpress_staged_chapter_renewal_notice_consumption_enabled():
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
                        "Status": "staged",
                        "ErrorMessage": None,
                    },
                )
                logging.info(
                    "Chapter renewal notice staged for async processing. event_key=%s chapter_rows=%s archive_path=%s",
                    parsed_event.event_key,
                    len(parsed_rows),
                    archive_path,
                )
            else:
                try:
                    result_rows = _execute_stored_procedure_rows(
                        conn_str,
                        REWARDS_CHAPTER_RENEWAL_NOTICES_PROC,
                        notice_params,
                    )
                    summary_email_sent = _send_chapter_renewal_notice_summary_if_configured(access_token, result_rows, subject, received_at)
                    decision_counts = _chapter_renewal_notice_decision_counts(result_rows)
                    result_payload = result_payload_for_procedures(
                        downstream_procedures,
                        extra={
                            "decision_counts": decision_counts,
                            "result_count": len(result_rows),
                            "summary_email_sent": summary_email_sent,
                        },
                    )
                    _mark_clubexpress_parsed_event_processed(conn_str, parsed_event, result_payload)
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
                    _mark_clubexpress_parsed_event_error(conn_str, parsed_event, exc)
                    raise
                posted_count = decision_counts.get("posted", 0) + decision_counts.get("already_posted", 0)
                insufficient_count = decision_counts.get("insufficient_points", 0)
                logging.info(
                    "Chapter renewal notice processed. chapter_rows=%s posted_or_existing=%s insufficient=%s archive_path=%s",
                    len(parsed_rows),
                    posted_count,
                    insufficient_count,
                    archive_path,
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
    elif message_type == JOURNAL_MESSAGE_TYPE:
        message_id = _message_identifier(message)
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
            parsed = _parse_journal_email(conn_str, message)
            journal_params = _journal_news_email_params(
                message,
                received_at,
                parsed,
                sender=sender,
                subject=subject,
                blob_path=archive_path,
            )
            downstream_procedures = [
                DownstreamProcedure(JOURNAL_NEWS_PROC, journal_params),
            ]
            parsed_event = build_journal_parsed_event(
                message_id=message_id,
                message_type=message_type,
                event_type=JOURNAL_MESSAGE_TYPE,
                received_at=received_at,
                journal_date=parsed["JournalDate"],
                parsed=parsed,
                downstream_procedures=downstream_procedures,
                sender=sender or None,
                subject=subject or None,
                blob_path=archive_path,
            )
            _record_clubexpress_parsed_event(conn_str, parsed_event)
            if _clubexpress_staged_journal_consumption_enabled():
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
                        "Status": "staged",
                        "ErrorMessage": None,
                    },
                )
                logging.info(
                    "Journal email staged for async processing. event_key=%s articles=%s news_matches=%s review_matches=%s archive_path=%s",
                    parsed_event.event_key,
                    len(parsed["Articles"]),
                    len(parsed["Matches"]),
                    len(parsed["ReviewMatches"]),
                    archive_path,
                )
            else:
                try:
                    _execute_stored_procedures(conn_str, _downstream_procedure_calls(downstream_procedures))
                    _mark_clubexpress_parsed_event_processed(
                        conn_str,
                        parsed_event,
                        result_payload_for_procedures(downstream_procedures),
                    )
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
                    _mark_clubexpress_parsed_event_error(conn_str, parsed_event, exc)
                    raise
                logging.info(
                    "Journal email processed for %s with %s articles, %s news matches, and %s review matches archive_path=%s",
                    parsed["JournalDate"],
                    len(parsed["Articles"]),
                    len(parsed["Matches"]),
                    len(parsed["ReviewMatches"]),
                    archive_path,
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
    else:
        raise RuntimeError(f"Unsupported mailbox message type: {message_type}")

    _mark_gmail_message_processed(access_token, message)


def _generate_tdlist_response(list_type: str) -> func.HttpResponse:
    """Execute the generate tdlist response routine."""
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
    """Execute the redirect tdlist routine."""
    target_url = TDLIST_REDIRECT_URLS.get(list_type)
    if not target_url:
        return func.HttpResponse(f"Unsupported TDList type: {list_type}", status_code=500)
    return func.HttpResponse(status_code=302, headers={"Location": target_url})


def _fetch_tdlist_rows(conn_str: str) -> list[dict[str, object]]:
    """Fetch tdlist rows."""
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
    """Fetch tdlist rows via tds."""
    conn = _tds_connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(TDLIST_QUERY.replace("?", str(MAX_MEMBER_AGAID)))
        return list(cursor.fetchall())
    finally:
        conn.close()


def _parse_sql_connection_string(connection_string: str) -> dict[str, object]:
    """Parse sql connection string."""
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
    """Execute the tds connect routine."""
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
    """Render tdlist tab."""
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
                    _format_tdlist_date(row.get("LastUpdate")),
                ]
            )
        )
    return "\n".join(rendered_rows) + ("\n" if rendered_rows else "")


def _render_tdlist_fixed_width(rows: list[dict[str, object]]) -> str:
    """Render tdlist fixed width."""
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
    """Execute the tdlist name routine."""
    last_name = _tdlist_text(row.get("LastName"))
    first_name = _tdlist_text(row.get("FirstName"))
    if last_name and first_name:
        return f"{last_name}, {first_name}"
    return last_name or first_name


def _tdlist_text(value: object) -> str:
    """Execute the tdlist text routine."""
    if value is None:
        return ""
    return str(value).strip()


def _format_tdlist_decimal(value: object, *, digits: int) -> str:
    """Format tdlist decimal."""
    if value is None:
        return ""
    return f"{float(value):.{digits}f}"


def _format_tdlist_date(value: object) -> str:
    """Format tdlist date."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return f"{value.month}/{value.day}/{value.year}"
    return str(value)


def _build_memchap_parsed_event(
    *,
    message_id: str,
    message_type: str,
    received_at: datetime,
    event_date: date,
    attachments: list[dict],
    sender: Optional[str],
    subject: Optional[str],
    blob_path: Optional[str],
) -> ClubExpressParsedEvent:
    """Build memchap parsed event."""
    if not blob_path:
        raise RuntimeError("Staged MemChap processing requires CLUBEXPRESS_ARCHIVE_CONTAINER and Blob storage configuration.")

    attachment = _find_report_attachment(attachments, NIGHTLY_MESSAGE_TYPE, "MemChap")
    content_bytes = attachment.get("contentBytes")
    if content_bytes is None:
        raise EmailProcessingError("MemChap attachment did not include content bytes.")

    rows = _parse_csv_rows(content_bytes)
    attachment_name = attachment.get("name") or "attachment.csv"
    return build_csv_attachment_parsed_event(
        message_id=message_id,
        message_type=message_type,
        event_type=NIGHTLY_MESSAGE_TYPE,
        received_at=received_at,
        event_date=event_date,
        attachment_name=attachment_name,
        attachment_blob_name=_safe_blob_name(attachment_name),
        row_count=len(rows),
        action_name=MEMCHAP_IMPORT_ACTION,
        sender=sender or None,
        subject=subject or None,
        blob_path=blob_path,
    )


def _build_chapter_parsed_event(
    *,
    message_id: str,
    message_type: str,
    received_at: datetime,
    event_date: date,
    attachments: list[dict],
    sender: Optional[str],
    subject: Optional[str],
    blob_path: Optional[str],
) -> ClubExpressParsedEvent:
    """Build chapter parsed event."""
    if not blob_path:
        raise RuntimeError("Staged ChapterX processing requires CLUBEXPRESS_ARCHIVE_CONTAINER and Blob storage configuration.")

    attachment = _find_report_attachment(attachments, CHAPTER_MESSAGE_TYPE, "ChapterX")
    content_bytes = attachment.get("contentBytes")
    if content_bytes is None:
        raise EmailProcessingError("ChapterX attachment did not include content bytes.")

    rows = _parse_chapter_rows(content_bytes)
    attachment_name = attachment.get("name") or "attachment.csv"
    return build_csv_attachment_parsed_event(
        message_id=message_id,
        message_type=message_type,
        event_type=CHAPTER_MESSAGE_TYPE,
        received_at=received_at,
        event_date=event_date,
        attachment_name=attachment_name,
        attachment_blob_name=_safe_blob_name(attachment_name),
        row_count=len(rows),
        action_name=CHAPTER_IMPORT_ACTION,
        sender=sender or None,
        subject=subject or None,
        blob_path=blob_path,
    )


def _build_member_categories_parsed_event(
    *,
    message_id: str,
    message_type: str,
    received_at: datetime,
    event_date: date,
    attachments: list[dict],
    sender: Optional[str],
    subject: Optional[str],
    blob_path: Optional[str],
) -> ClubExpressParsedEvent:
    """Build member categories parsed event."""
    if not blob_path:
        raise RuntimeError("Staged member-category processing requires CLUBEXPRESS_ARCHIVE_CONTAINER and Blob storage configuration.")

    attachment = _find_report_attachment(attachments, NIGHTLY_CATEGORY_MESSAGE_TYPE, "member category")
    content_bytes = attachment.get("contentBytes")
    if content_bytes is None:
        raise EmailProcessingError("Member-category attachment did not include content bytes.")

    rows = _parse_member_category_rows(content_bytes)
    attachment_name = attachment.get("name") or "attachment.csv"
    return build_csv_attachment_parsed_event(
        message_id=message_id,
        message_type=message_type,
        event_type=NIGHTLY_CATEGORY_MESSAGE_TYPE,
        received_at=received_at,
        event_date=event_date,
        attachment_name=attachment_name,
        attachment_blob_name=_safe_blob_name(attachment_name),
        row_count=len(rows),
        action_name=MEMBER_CATEGORIES_IMPORT_ACTION,
        sender=sender or None,
        subject=subject or None,
        blob_path=blob_path,
    )


def _find_report_attachment(attachments: list[dict], report_type: str, label: str) -> dict:
    """Find report attachment."""
    for attachment in attachments:
        content_bytes = attachment.get("contentBytes")
        if not content_bytes:
            continue
        if _detect_attachment_report_type(attachment.get("name", ""), content_bytes) == report_type:
            return attachment

    raise EmailProcessingError(f"No {label} attachment was found on the ClubExpress email.")


def _handle_memchap_email(conn_str: str, attachments: list[dict]) -> int:
    """Handle memchap email."""
    for attachment in attachments:
        content_bytes = attachment.get("contentBytes")
        if not content_bytes:
            continue
        if _detect_attachment_report_type(attachment.get("name", ""), content_bytes) != NIGHTLY_MESSAGE_TYPE:
            continue
        return _import_memchap_bytes(conn_str, content_bytes)

    raise EmailProcessingError("No MemChap attachment was found on the nightly ClubExpress email.")


def _handle_chapter_email(conn_str: str, attachments: list[dict]) -> int:
    """Handle chapter email."""
    for attachment in attachments:
        content_bytes = attachment.get("contentBytes")
        if not content_bytes:
            continue
        if _detect_attachment_report_type(attachment.get("name", ""), content_bytes) != CHAPTER_MESSAGE_TYPE:
            continue
        return _import_chapter_bytes(conn_str, content_bytes)

    raise EmailProcessingError("No Chapter attachment was found on the ClubExpress email.")


def _handle_member_categories_email(conn_str: str, attachments: list[dict]) -> int:
    """Handle member categories email."""
    for attachment in attachments:
        content_bytes = attachment.get("contentBytes")
        if not content_bytes:
            continue
        if _detect_attachment_report_type(attachment.get("name", ""), content_bytes) != NIGHTLY_CATEGORY_MESSAGE_TYPE:
            continue
        return _import_member_categories_bytes(conn_str, content_bytes)

    raise EmailProcessingError("No member category attachment was found on the nightly ClubExpress email.")


def _import_memchap_bytes(conn_str: str, csv_bytes: bytes) -> int:
    """Import memchap bytes."""
    rows = _parse_csv_rows(csv_bytes)
    _stage_and_import(conn_str, rows)
    return len(rows)


def _process_pending_memchap_events(
    conn_str: str,
    *,
    top: int = 5,
    execute: bool = False,
    confirm_replay: bool = False,
    processor_name: str = "staged_memchap_processor",
    adapter: Optional[object] = None,
) -> BatchProcessResult:
    """Process pending memchap events."""
    return _process_pending_csv_attachment_events(
        conn_str,
        event_type=NIGHTLY_MESSAGE_TYPE,
        action_name=MEMCHAP_IMPORT_ACTION,
        import_bytes=_import_memchap_bytes,
        top=top,
        execute=execute,
        confirm_replay=confirm_replay,
        processor_name=processor_name,
        adapter=adapter,
    )


def _process_pending_chapter_events(
    conn_str: str,
    *,
    top: int = 5,
    execute: bool = False,
    confirm_replay: bool = False,
    processor_name: str = "staged_chapter_processor",
    adapter: Optional[object] = None,
) -> BatchProcessResult:
    """Process pending chapter events."""
    return _process_pending_csv_attachment_events(
        conn_str,
        event_type=CHAPTER_MESSAGE_TYPE,
        action_name=CHAPTER_IMPORT_ACTION,
        import_bytes=_import_chapter_bytes,
        top=top,
        execute=execute,
        confirm_replay=confirm_replay,
        processor_name=processor_name,
        adapter=adapter,
    )


def _process_pending_member_category_events(
    conn_str: str,
    *,
    top: int = 5,
    execute: bool = False,
    confirm_replay: bool = False,
    processor_name: str = "staged_member_category_processor",
    adapter: Optional[object] = None,
) -> BatchProcessResult:
    """Process pending member category events."""
    return _process_pending_csv_attachment_events(
        conn_str,
        event_type=NIGHTLY_CATEGORY_MESSAGE_TYPE,
        action_name=MEMBER_CATEGORIES_IMPORT_ACTION,
        import_bytes=_import_member_categories_bytes,
        top=top,
        execute=execute,
        confirm_replay=confirm_replay,
        processor_name=processor_name,
        adapter=adapter,
    )


def _process_pending_chapter_renewal_notice_events(
    conn_str: str,
    *,
    top: int = 10,
    execute: bool = False,
    confirm_replay: bool = False,
    processor_name: str = "staged_chapter_renewal_notice_processor",
    adapter: Optional[object] = None,
    access_token: Optional[str] = None,
) -> BatchProcessResult:
    """Process pending chapter renewal notice events."""
    if execute and not confirm_replay:
        raise ValueError("Executing staged chapter-renewal notice processing requires confirm_replay=True.")

    staged_adapter = adapter or _ClubExpressStagedEventSqlAdapter(conn_str)
    events = list_pending_events(staged_adapter, event_type=CHAPTER_RENEWAL_NOTICE_MESSAGE_TYPE, top=top)
    results = []
    processed_count = 0
    error_count = 0
    gmail_access_token = access_token

    for event in events:
        if not execute:
            preview = replay_event(staged_adapter, event)
            results.append(preview.as_dict())
            continue

        try:
            mark_event_status(staged_adapter, event.event_key, "processing")
            procedure_results = execute_downstream_calls(staged_adapter, event.downstream_calls)
            result_rows = _chapter_renewal_notice_result_rows(procedure_results)
            summary_email_sent = False
            if _configured_email_recipients("CHAPTER_RENEWAL_NOTICE_EMAIL_TO"):
                try:
                    if not gmail_access_token:
                        gmail_access_token = _get_gmail_access_token()
                    summary_email_sent = _send_chapter_renewal_notice_summary_if_configured(
                        gmail_access_token,
                        result_rows,
                        event.subject or CHAPTER_RENEWAL_NOTICE_SUBJECT,
                        _coerce_summary_received_at(event.received_at),
                    )
                except Exception:
                    logging.exception("Failed preparing Chapter Rewards renewal notice summary email.")
                    summary_email_sent = False

            decision_counts = _chapter_renewal_notice_decision_counts(result_rows)
            result_payload = {
                "processor": processor_name,
                "replay": False,
                "procedure_results": procedure_results,
                "decision_counts": decision_counts,
                "result_count": len(result_rows),
                "summary_email_sent": summary_email_sent,
            }
            mark_event_status(staged_adapter, event.event_key, "processed", result_payload=json_safe_value(result_payload))
            _update_clubexpress_email_log_for_staged_event(conn_str, event, "processed")
            processed_count += 1
            results.append(
                {
                    "event_key": event.event_key,
                    "executed": True,
                    "status_before": event.status,
                    "status_after": "processed",
                    "procedure_results": procedure_results,
                    "decision_counts": decision_counts,
                    "result_count": len(result_rows),
                    "summary_email_sent": summary_email_sent,
                }
            )
        except Exception as exc:
            error_count += 1
            try:
                mark_event_status(staged_adapter, event.event_key, "error", error_message=str(exc))
            except Exception:
                logging.exception("Failed marking staged chapter-renewal notice event %s as error.", event.event_key)
            _update_clubexpress_email_log_for_staged_event(conn_str, event, "error", error_message=str(exc))
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
        event_type=CHAPTER_RENEWAL_NOTICE_MESSAGE_TYPE,
        executed=execute,
        selected_count=len(events),
        processed_count=processed_count,
        error_count=error_count,
        results=results,
    )


def _chapter_renewal_notice_result_rows(procedure_results: list[dict]) -> list[dict]:
    """Execute the chapter renewal notice result rows routine."""
    for result in procedure_results:
        if result.get("name") != REWARDS_CHAPTER_RENEWAL_NOTICES_PROC:
            continue
        rows = result.get("rows")
        return rows if isinstance(rows, list) else []
    return []


def _process_pending_journal_events(
    conn_str: str,
    *,
    top: int = 10,
    execute: bool = False,
    confirm_replay: bool = False,
    processor_name: str = "staged_journal_processor",
    adapter: Optional[object] = None,
) -> BatchProcessResult:
    """Process pending journal events."""
    if execute and not confirm_replay:
        raise ValueError("Executing staged E-Journal processing requires confirm_replay=True.")

    staged_adapter = adapter or _ClubExpressStagedEventSqlAdapter(conn_str)
    events = list_pending_events(staged_adapter, event_type=JOURNAL_MESSAGE_TYPE, top=top)
    results = []
    processed_count = 0
    error_count = 0

    for event in events:
        if not execute:
            preview = replay_event(staged_adapter, event)
            results.append(preview.as_dict())
            continue

        try:
            result = process_staged_event(staged_adapter, event, processor_name=processor_name)
            _update_clubexpress_email_log_for_staged_event(conn_str, event, "processed")
            processed_count += 1
            results.append(result.as_dict())
        except Exception as exc:
            error_count += 1
            _update_clubexpress_email_log_for_staged_event(conn_str, event, "error", error_message=str(exc))
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
        event_type=JOURNAL_MESSAGE_TYPE,
        executed=execute,
        selected_count=len(events),
        processed_count=processed_count,
        error_count=error_count,
        results=results,
    )


def _process_pending_csv_attachment_events(
    conn_str: str,
    *,
    event_type: str,
    action_name: str,
    import_bytes,
    top: int,
    execute: bool,
    confirm_replay: bool,
    processor_name: str,
    adapter: Optional[object] = None,
) -> BatchProcessResult:
    """Process pending csv attachment events."""
    if execute and not confirm_replay:
        raise ValueError(f"Executing staged {event_type} processing requires confirm_replay=True.")

    staged_adapter = adapter or _ClubExpressStagedEventSqlAdapter(conn_str)
    events = list_pending_events(staged_adapter, event_type=event_type, top=top)
    results = []
    processed_count = 0
    error_count = 0

    for event in events:
        if not execute:
            results.append(
                {
                    "event_key": event.event_key,
                    "executed": False,
                    "status_before": event.status,
                    "status_after": event.status,
                    "procedure_results": [_csv_attachment_action_preview(event, action_name)],
                }
            )
            continue

        try:
            mark_event_status(staged_adapter, event.event_key, "processing")
            action_result = _process_csv_attachment_staged_event(conn_str, event, action_name, import_bytes)
            result_payload = {
                "processor": processor_name,
                "replay": False,
                "action_results": [action_result],
            }
            mark_event_status(staged_adapter, event.event_key, "processed", result_payload=json_safe_value(result_payload))
            _update_clubexpress_email_log_for_staged_event(conn_str, event, "processed")
            processed_count += 1
            results.append(
                {
                    "event_key": event.event_key,
                    "executed": True,
                    "status_before": event.status,
                    "status_after": "processed",
                    "procedure_results": [action_result],
                }
            )
        except Exception as exc:
            error_count += 1
            try:
                mark_event_status(staged_adapter, event.event_key, "error", error_message=str(exc))
            except Exception:
                logging.exception("Failed marking staged MemChap event %s as error.", event.event_key)
            _update_clubexpress_email_log_for_staged_event(conn_str, event, "error", error_message=str(exc))
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


def _update_clubexpress_email_log_for_staged_event(
    conn_str: str,
    event: object,
    status: str,
    *,
    error_message: Optional[str] = None,
) -> None:
    """Update clubexpress email log for staged event."""
    try:
        _execute_stored_procedure(
            conn_str,
            "membership.sp_log_clubexpress_email",
            {
                "MessageId": event.message_id,
                "MessageType": event.message_type,
                "ReceivedAt": event.received_at,
                "Sender": event.sender,
                "Subject": event.subject,
                "BlobPath": event.blob_path,
                "Status": status,
                "ErrorMessage": error_message,
            },
        )
    except Exception:
        logging.exception(
            "Failed updating legacy ClubExpress email log for staged event %s to %s.",
            event.event_key,
            status,
        )


def _process_csv_attachment_staged_event(conn_str: str, event: object, action_name: str, import_bytes) -> dict:
    """Process csv attachment staged event."""
    parsed_payload = event.parsed_payload if isinstance(event.parsed_payload, dict) else {}
    parsed = parsed_payload.get("parsed") if isinstance(parsed_payload.get("parsed"), dict) else {}
    blob_path = event.blob_path or parsed_payload.get("blob_path")
    attachment_blob_name = parsed.get("attachment_blob_name")
    if not blob_path:
        raise ValueError(f"Staged CSV event {event.event_key} is missing Blob_Path.")
    if not attachment_blob_name:
        raise ValueError(f"Staged CSV event {event.event_key} is missing attachment_blob_name.")

    content_bytes = _download_archived_attachment_bytes(str(blob_path), str(attachment_blob_name))
    rows_imported = import_bytes(conn_str, content_bytes)
    return {
        "name": action_name,
        "returns_rows": False,
        "parameter_count": 2,
        "row_count": rows_imported,
        "attachment_name": parsed.get("attachment_name"),
        "attachment_blob_name": attachment_blob_name,
    }


def _csv_attachment_action_preview(event: object, action_name: str) -> dict:
    """Execute the csv attachment action preview routine."""
    return {
        "name": action_name,
        "returns_rows": False,
        "parameter_count": 2,
        "row_count": event.parsed_item_count,
    }


def _import_chapter_bytes(conn_str: str, csv_bytes: bytes) -> int:
    """Import chapter bytes."""
    rows = _parse_chapter_rows(csv_bytes)
    _stage_and_import_chapters(conn_str, rows)
    return len(rows)


def _import_member_categories_bytes(conn_str: str, csv_bytes: bytes) -> int:
    """Import member categories bytes."""
    rows = _parse_member_category_rows(csv_bytes)
    _stage_and_import_member_categories(conn_str, rows)
    return len(rows)


def _is_memchap_attachment_name(name: str) -> bool:
    """Return whether memchap attachment name."""
    return csv_is_memchap_attachment_name(name)


def _is_chapter_attachment_name(name: str) -> bool:
    """Return whether chapter attachment name."""
    return csv_is_chapter_attachment_name(name)


def _classify_message(sender: str, subject: str, attachments: list[dict]) -> str:
    """Execute the classify message routine."""
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
    if normalized_subject == CHAPTER_RENEWAL_NOTICE_SUBJECT:
        return CHAPTER_RENEWAL_NOTICE_MESSAGE_TYPE
    if "scheduler@mail2.clubexpress.com" in normalized_sender and attachments:
        return IGNORE_MESSAGE_TYPE
    return IGNORE_MESSAGE_TYPE


def _parse_new_member_email(text: str) -> dict:
    """Parse new member email."""
    return parse_new_member_email(text)


def _parse_renewal_email(text: str) -> dict:
    """Parse renewal email."""
    return parse_renewal_email(text)


def _parse_chapter_renewal_notice_email(message: dict) -> list[dict]:
    """Parse chapter renewal notice email."""
    return parse_chapter_renewal_notice(_message_body_to_html(message) or "", _message_body_to_text(message))


def _extract_chapter_renewal_notice_rows_from_html(html_body: str) -> list[dict]:
    """Extract chapter renewal notice rows from html."""
    return extract_chapter_renewal_notice_rows_from_html(html_body)


def _extract_chapter_renewal_notice_rows_from_text(text: str) -> list[dict]:
    """Extract chapter renewal notice rows from text."""
    return extract_chapter_renewal_notice_rows_from_text(text)


def _parse_journal_email(conn_str: str, message: dict) -> dict:
    """Parse journal email."""
    subject = _get_header_value(message, "Subject")
    journal_date = _parse_journal_subject_date(subject)
    html_body = _message_body_to_html(message)
    if html_body:
        articles = _extract_journal_articles_from_html(html_body)
        review_blog_entries = _extract_journal_review_blog_entries_from_html(html_body)
    else:
        articles = _extract_journal_articles_from_text(_message_body_to_text(message))
        review_blog_entries = _extract_journal_review_blog_entries_from_text(_message_body_to_text(message))
    articles = _normalize_journal_article_titles(articles)

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
    """Parse journal subject date."""
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
    """Extract journal articles from html."""
    parser = _JournalHtmlParser()
    parser.feed(html_body)
    parser.close()
    articles = _build_journal_articles_from_blocks(parser.blocks)
    if articles:
        return articles

    lines = _extract_visible_lines_from_html(html_body)
    return _extract_journal_articles_from_lines(lines, html_body=html_body)


def _extract_journal_articles_from_text(text: str) -> list[dict[str, str]]:
    """Extract journal articles from text."""
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines() if line.strip()]
    return _extract_journal_articles_from_lines(lines)


def _normalize_journal_article_titles(articles: list[dict[str, str]]) -> list[dict[str, str]]:
    """Normalize journal article titles."""
    normalized_articles = []
    for article in articles:
        title = str(article.get("title") or "").strip()
        link = str(article.get("link") or "").strip()
        if _looks_like_url(title) and link:
            resolved_title = _fetch_article_page_title(link)
            if resolved_title:
                article = dict(article)
                article["title"] = resolved_title[:500]
        normalized_articles.append(article)
    return normalized_articles


def _extract_journal_review_blog_entries_from_html(html_body: str) -> list[dict[str, str]]:
    """Extract journal review blog entries from html."""
    lines = _extract_visible_lines_from_html(html_body)
    return _extract_journal_review_blog_entries_from_lines(lines, html_body=html_body)


def _extract_journal_review_blog_entries_from_text(text: str) -> list[dict[str, str]]:
    """Extract journal review blog entries from text."""
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines() if line.strip()]
    return _extract_journal_review_blog_entries_from_lines(lines)


def _extract_visible_lines_from_html(html_body: str) -> list[str]:
    """Extract visible lines from html."""
    parser = _JournalVisibleTextParser()
    parser.feed(html_body)
    parser.close()
    return parser.get_lines()


def _extract_journal_articles_from_lines(lines: list[str], html_body: Optional[str] = None) -> list[dict[str, str]]:
    """Extract journal articles from lines."""
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
    """Execute the slice journal news lines routine."""
    return _slice_journal_section_lines(lines, "news")


def _slice_journal_blog_lines(lines: list[str]) -> list[str]:
    """Execute the slice journal blog lines routine."""
    return _slice_journal_section_lines(lines, "blogs")


def _slice_journal_section_lines(lines: list[str], section_name: str) -> list[str]:
    """Execute the slice journal section lines routine."""
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
    """Execute the looks like terminal journal section routine."""
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
    """Extract journal title links."""
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
    """Extract journal review blog entries from lines."""
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
    """Build articles from title links."""
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
    """Execute the resolve article title indices routine."""
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
    """Build article from news lines."""
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
    """Find line index."""
    for idx in range(start, len(lines)):
        if lines[idx] == target:
            return idx
    return None


def _journal_block_http_links(block: dict[str, object]) -> list[str]:
    """Execute the journal block http links routine."""
    return [
        link[:1000]
        for link in (block.get("links") or [])
        if isinstance(link, str) and link.startswith(("http://", "https://"))
    ]


def _build_journal_articles_from_blocks(blocks: list[dict[str, object]]) -> list[dict[str, str]]:
    """Build journal articles from blocks."""
    news_index = None
    for idx, block in enumerate(blocks):
        block_text = str(block.get("text") or "").strip().lower()
        if block_text == "news":
            news_index = idx + 1
            break
    if news_index is None:
        return []

    news_blocks: list[dict[str, object]] = []
    for block in blocks[news_index:]:
        text = str(block.get("text") or "").strip()
        if not text:
            continue
        if block.get("heading_level") is not None and _looks_like_terminal_journal_section(text):
            break
        news_blocks.append(block)

    link_lookup: dict[str, str] = {}
    for block in news_blocks:
        text = str(block.get("text") or "").strip()
        links = _journal_block_http_links(block)
        if links and _looks_like_article_title(text):
            link_lookup.setdefault(text, links[0])

    heading_indices = [
        idx
        for idx, block in enumerate(news_blocks)
        if block.get("heading_level") is not None
        and _looks_like_article_title(str(block.get("text") or "").strip())
        and (
            str(block.get("text") or "").strip() in link_lookup
            or bool(_journal_block_http_links(block))
        )
    ]
    if heading_indices:
        articles = []
        for position, heading_index in enumerate(heading_indices):
            heading_block = news_blocks[heading_index]
            title = str(heading_block.get("text") or "").strip()
            next_heading_index = heading_indices[position + 1] if position + 1 < len(heading_indices) else len(news_blocks)
            heading_links = _journal_block_http_links(heading_block)
            link = (heading_links[0] if heading_links else link_lookup.get(title, ""))
            if not link:
                continue
            analysis_lines = []
            for body_block in news_blocks[heading_index + 1:next_heading_index]:
                body_text = str(body_block.get("text") or "").strip()
                if body_text:
                    analysis_lines.append(body_text)
            articles.append(
                {
                    "title": title[:500],
                    "link": link,
                    "analysisText": " ".join(analysis_lines).strip(),
                }
            )
        if articles:
            return articles

    articles = []
    current = None
    found_article = False
    for block in news_blocks:
        text = str(block.get("text") or "").strip()
        if not text:
            continue

        links = _journal_block_http_links(block)
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
    """Execute the looks like article title routine."""
    collapsed = re.sub(r"\s+", " ", text).strip()
    word_count = len(collapsed.split())
    return 2 <= word_count <= 20 and len(collapsed) <= 180


def _looks_like_url(text: str) -> bool:
    """Execute the looks like url routine."""
    return bool(re.match(r"^https?://\S+$", (text or "").strip(), re.IGNORECASE))


def _fetch_article_page_title(url: str) -> str:
    """Fetch article page title."""
    html_body = _fetch_external_html(url)
    if not html_body:
        return ""

    title_match = re.search(r"<title\b[^>]*>(.*?)</title>", html_body, re.IGNORECASE | re.DOTALL)
    if title_match:
        title = re.sub(r"\s+", " ", _html_to_text(title_match.group(1))).strip()
        title = re.sub(r"\s+-\s+American Go Association\s*$", "", title).strip()
        if title and not _looks_like_url(title):
            return title

    heading_match = re.search(r"<h[12]\b[^>]*>(.*?)</h[12]>", html_body, re.IGNORECASE | re.DOTALL)
    if heading_match:
        title = re.sub(r"\s+", " ", _html_to_text(heading_match.group(1))).strip()
        if title and not _looks_like_url(title):
            return title
    return ""


def _looks_like_section_heading(text: str) -> bool:
    """Execute the looks like section heading routine."""
    collapsed = re.sub(r"\s+", " ", text).strip()
    if len(collapsed) > 60:
        return False
    if ":" in collapsed:
        return True
    return collapsed.lower() in {"events", "classifieds", "calendar", "tournaments", "resources", "about", "membership"}


def _load_member_name_lookup(conn_str: str) -> dict[str, list[tuple[int, str]]]:
    """Load member name lookup."""
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
    """Match member rows in article."""
    matched_rows = set()
    for candidate in _extract_candidate_person_names(text):
        for key in _journal_person_lookup_keys(candidate):
            if key in JOURNAL_EXCLUDED_MATCH_NAMES:
                continue
            matched_rows.update(member_lookup.get(key, []))
    return sorted(matched_rows, key=lambda row: (row[0], row[1].lower()))


def _journal_person_lookup_keys(value: str) -> set[str]:
    """Execute the journal person lookup keys routine."""
    key = _normalize_person_name(value)
    if not key:
        return set()

    first_name, last_name = key.split(" ", 1)
    return {f"{first_variant} {last_name}" for first_variant in _journal_first_name_variants(first_name)}


def _journal_first_name_variants(first_name: str) -> set[str]:
    """Execute the journal first name variants routine."""
    normalized = (first_name or "").strip().lower()
    if not normalized:
        return set()

    variants = {normalized}
    for group in _journal_first_name_alias_groups():
        if normalized in group:
            variants.update(group)
    return variants


def _journal_first_name_alias_groups() -> list[set[str]]:
    """Execute the journal first name alias groups routine."""
    groups = [set(group) for group in JOURNAL_FIRST_NAME_ALIAS_GROUPS]
    extra_groups = os.environ.get("JOURNAL_FIRST_NAME_ALIAS_GROUPS", "")
    for raw_group in extra_groups.split(";"):
        names = {name.lower() for name in re.findall(r"[A-Za-z]+(?:[-'][A-Za-z]+)?", raw_group)}
        if len(names) >= 2:
            groups.append(names)
    return groups


def _extract_candidate_person_names(text: str) -> set[str]:
    """Extract candidate person names."""
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
    """Return journal name nlp."""
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
    """Execute the expand candidate person names routine."""
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
    """Execute the strip journal name prefix routine."""
    lowered_tokens = [token.lower() for token in tokens]
    for prefix_tokens in _journal_name_prefix_token_lists():
        prefix_length = len(prefix_tokens)
        if len(lowered_tokens) <= prefix_length:
            continue
        if lowered_tokens[:prefix_length] == prefix_tokens:
            return tokens[prefix_length:]
    return []


def _journal_name_prefix_token_lists() -> list[list[str]]:
    """Execute the journal name prefix token lists routine."""
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
    """Extract review matches from blog entry."""
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
    """Fetch external html."""
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
    """Parse naol review blog html."""
    lines = _extract_visible_lines_from_html(html_body)
    title = _extract_naol_blog_title(lines, html_body)
    tokens = _extract_naol_review_tokens_from_html(html_body)
    sections = _extract_naol_review_sections(tokens)
    return {
        "title": title,
        "sections": sections,
    }


def _extract_naol_blog_title(lines: list[str], html_body: str = "") -> str:
    """Extract naol blog title."""
    title_match = re.search(r"<title\b[^>]*>(.*?)</title>", html_body or "", re.IGNORECASE | re.DOTALL)
    if title_match:
        title = re.sub(r"\s+", " ", _html_to_text(title_match.group(1))).strip()
        title = re.sub(r"\s+-\s+American Go Association\s*$", "", title).strip()
        if title and "naol" in title.lower():
            return title[:500]

    for idx, line in enumerate(lines):
        if line.strip().lower() == "naol reviews" and idx + 1 < len(lines):
            return lines[idx + 1][:500]
    for line in lines[:12]:
        if "naol review" in line.lower():
            return line[:500]
    return "NAOL Reviews"


def _extract_iframe_video_links(html_body: str) -> list[str]:
    """Extract iframe video links."""
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


def _extract_naol_review_tokens_from_html(html_body: str) -> list[dict[str, str]]:
    """Extract naol review tokens from html."""
    parser = _NaolReviewHtmlParser()
    parser.feed(html_body or "")
    parser.close()
    return parser.get_tokens()


def _normalize_video_link(url: str) -> str:
    """Normalize video link."""
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


def _extract_naol_review_sections(tokens: list[dict[str, str]]) -> list[dict[str, object]]:
    """Extract naol review sections."""
    sections: list[dict[str, object]] = []
    current: Optional[dict[str, object]] = None
    in_post_body = False

    for token in tokens:
        video_link = (token.get("video_link") or "").strip()
        if video_link:
            if current is not None and current.get("_awaiting_video"):
                current["video_link"] = video_link
                _append_naol_review_section(sections, current)
                current = None
            continue

        raw_line = token.get("text", "")
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
            _append_naol_review_section(sections, current)
            current = {
                "reviewer_name": reviewer_match["reviewer_name"],
                "reviewer_rank": reviewer_match["reviewer_rank"],
                "games": [],
                "video_link": "",
                "_awaiting_video": False,
            }
            continue

        if current is None:
            continue

        game_match = _parse_naol_game_line(line)
        if game_match:
            current["games"].append(game_match)
            continue

        if "review video" in lower_line:
            current["_awaiting_video"] = True

    _append_naol_review_section(sections, current)
    return sections


def _append_naol_review_section(sections: list[dict[str, object]], section: Optional[dict[str, object]]) -> None:
    """Execute the append naol review section routine."""
    if not section or not section.get("games") or not section.get("video_link"):
        return
    sections.append({key: value for key, value in section.items() if not key.startswith("_")})


def _looks_like_naol_reviewer_header(line: str) -> bool:
    """Execute the looks like naol reviewer header routine."""
    return _parse_naol_reviewer_header(line) is not None


def _parse_naol_reviewer_header(line: str) -> Optional[dict[str, str]]:
    """Parse naol reviewer header."""
    match = re.match(r"^\s*(?P<name>.+?)\s*\((?P<rank>\d{1,2}[kKdDpP])\)", line)
    if not match:
        return None
    return {
        "reviewer_name": match.group("name").strip(),
        "reviewer_rank": match.group("rank").strip(),
    }


def _parse_naol_game_line(line: str) -> Optional[dict[str, str]]:
    """Parse naol game line."""
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
    """Normalize person name."""
    tokens = re.findall(r"[A-Za-z]+(?:[-'][A-Za-z]+)?", value or "")
    if len(tokens) < 2:
        return None
    return f"{tokens[0].lower()} {tokens[-1].lower()}"


def _default_membership_expiration_date(received_date: date, member_type: Optional[str]) -> date:
    """Execute the default membership expiration date routine."""
    normalized_type = (member_type or "").strip().lower()
    if normalized_type == "tournament pass":
        return received_date + timedelta(days=29)
    return received_date + timedelta(days=365)


def _detect_message_report_type(attachments: list[dict]) -> Optional[str]:
    """Execute the detect message report type routine."""
    for attachment in attachments:
        content_bytes = attachment.get("contentBytes")
        if not content_bytes:
            continue
        report_type = _detect_attachment_report_type(attachment.get("name", ""), content_bytes)
        if report_type:
            return report_type
    return None


def _detect_attachment_report_type(name: str, content_bytes: bytes) -> Optional[str]:
    """Execute the detect attachment report type routine."""
    return detect_csv_attachment_report_type(name, content_bytes)


def _read_csv_header_canonical(csv_bytes: bytes) -> list[str]:
    """Read csv header canonical."""
    return csv_read_csv_header_canonical(csv_bytes)


def _is_memchap_header(headers: list[str]) -> bool:
    """Return whether memchap header."""
    return csv_is_memchap_header(headers)


def _is_member_category_header(headers: list[str]) -> bool:
    """Return whether member category header."""
    return csv_is_member_category_header(headers)


def _is_chapter_header(headers: list[str]) -> bool:
    """Return whether chapter header."""
    return csv_is_chapter_header(headers)


def _message_body_to_text(message: dict) -> str:
    """Execute the message body to text routine."""
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
    """Execute the message body to html routine."""
    payload = message.get("payload") or {}
    return _extract_message_part_text(payload, "text/html")


def _extract_message_part_text(part: dict, mime_type: str) -> Optional[str]:
    """Extract message part text."""
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
    """Execute the html to text routine."""
    text = re.sub(r"<br\s*/?>", "\n", value, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return html.unescape(text)


def _archive_message_artifacts(message_type: str, message: dict, attachments: list[dict]) -> Optional[str]:
    """Archive message artifacts."""
    container = _archive_container_client()
    if container is None:
        return None
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


def _download_archived_attachment_bytes(blob_path: str, attachment_blob_name: str) -> bytes:
    """Execute the download archived attachment bytes routine."""
    container = _archive_container_client()
    if container is None:
        raise RuntimeError("ClubExpress archive Blob container is not configured.")
    blob_name = f"{blob_path}/attachments/{attachment_blob_name}"
    return container.download_blob(blob_name).readall()


def _archive_container_client() -> Optional[object]:
    """Archive container client."""
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

    return client.get_container_client(container_name)


def _safe_blob_name(value: str) -> str:
    """Execute the safe blob name routine."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", value)


def _message_identifier(message: dict) -> str:
    """Execute the message identifier routine."""
    return message.get("id") or f"message-{datetime.now(timezone.utc).timestamp()}"


def _message_received_at(message: dict) -> datetime:
    """Execute the message received at routine."""
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
    """Execute the lookup members routine."""
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
    """Execute the json safe value routine."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _membership_reward_event_params(
    message: dict,
    received_at: datetime,
    event_type: str,
    event_date: date,
    parsed: dict,
    *,
    sender: Optional[str] = None,
    subject: Optional[str] = None,
    blob_path: Optional[str] = None,
) -> dict:
    """Execute the membership reward event params routine."""
    source_payload = {
        "message_id": _message_identifier(message),
        "sender": sender,
        "subject": subject,
        "blob_path": blob_path,
        "parsed": {key: _json_safe_value(value) for key, value in parsed.items()},
    }
    return {
        "MessageId": _message_identifier(message),
        "ReceivedAt": received_at,
        "AGAID": parsed["AGAID"],
        "EventType": event_type,
        "EventDate": event_date,
        "MemberType": parsed.get("MemberType"),
        "SourcePayloadJson": json.dumps(source_payload, sort_keys=True),
    }


def _journal_news_email_params(
    message: dict,
    received_at: datetime,
    parsed: dict,
    *,
    sender: Optional[str] = None,
    subject: Optional[str] = None,
    blob_path: Optional[str] = None,
) -> dict:
    """Execute the journal news email params routine."""
    return {
        "MessageId": _message_identifier(message),
        "ReceivedAt": received_at,
        "JournalDate": parsed["JournalDate"],
        "MatchesJson": json.dumps(parsed["Matches"]),
        "ReviewMatchesJson": json.dumps(parsed["ReviewMatches"]),
        "Sender": sender or None,
        "Subject": subject or None,
        "BlobPath": blob_path,
    }


def _chapter_renewal_confirmation_params(
    message: dict,
    received_at: datetime,
    parsed: dict,
    *,
    sender: Optional[str] = None,
    subject: Optional[str] = None,
    blob_path: Optional[str] = None,
) -> dict:
    """Execute the chapter renewal confirmation params routine."""
    source_payload = {
        "message_id": _message_identifier(message),
        "sender": sender,
        "subject": subject,
        "blob_path": blob_path,
        "parsed": {key: _json_safe_value(value) for key, value in parsed.items()},
    }
    return {
        "MessageId": _message_identifier(message),
        "ReceivedAt": received_at,
        "ChapterID": parsed["AGAID"],
        "MemberType": parsed.get("MemberType"),
        "SourcePayloadJson": json.dumps(source_payload, sort_keys=True),
    }


def _rewards_snapshot_date(today: Optional[date] = None) -> date:
    """Execute the rewards snapshot date routine."""
    return today or date.today()


def _rewards_snapshot_params(snapshot_date: date) -> dict:
    """Execute the rewards snapshot params routine."""
    return {
        "SnapshotDate": snapshot_date,
        "RunType": "daily",
        "ReplaceExisting": 0,
    }


def _rewards_membership_awards_params(as_of_date: date) -> dict:
    """Execute the rewards membership awards params routine."""
    return {
        "AsOfDate": as_of_date,
        "RunType": "daily",
        "DryRun": 0,
    }


def _rewards_rated_game_awards_params(game_date: date) -> dict:
    """Execute the rewards rated game awards params routine."""
    return {
        "GameDateFrom": _rewards_rated_game_awards_start_date(game_date),
        "GameDateTo": game_date,
        "RunType": "daily",
        "DryRun": 0,
    }


def _rewards_rated_game_awards_start_date(game_date: date) -> date:
    """Execute the rewards rated game awards start date routine."""
    configured = (
        os.environ.get("REWARDS_RATED_GAME_AWARDS_DATE_FROM")
        or os.environ.get("REWARDS_LEDGER_START_DATE")
        or DEFAULT_REWARDS_LEDGER_START_DATE
    )
    try:
        start_date = date.fromisoformat(configured)
    except ValueError:
        logging.warning(
            "Invalid rewards rated-game award start date %r; using %s.",
            configured,
            DEFAULT_REWARDS_LEDGER_START_DATE,
        )
        start_date = date.fromisoformat(DEFAULT_REWARDS_LEDGER_START_DATE)
    return min(start_date, game_date)


def _rewards_tournament_awards_params(tournament_date_to: date) -> dict:
    """Execute the rewards tournament awards params routine."""
    return {
        "TournamentDateFrom": None,
        "TournamentDateTo": tournament_date_to,
        "RunType": "daily",
        "DryRun": 0,
    }


def _rewards_point_expirations_params(as_of_date: date) -> dict:
    """Execute the rewards point expirations params routine."""
    return {
        "AsOfDate": as_of_date,
        "RunType": "daily",
        "DryRun": 0,
    }


def _chapter_renewal_notice_params(
    message: dict,
    received_at: datetime,
    notice_date: date,
    parsed_rows: list[dict],
    *,
    sender: Optional[str] = None,
    subject: Optional[str] = None,
    blob_path: Optional[str] = None,
) -> dict:
    """Execute the chapter renewal notice params routine."""
    notices = []
    for row in parsed_rows:
        payload = {
            "message_id": _message_identifier(message),
            "sender": sender,
            "subject": subject,
            "blob_path": blob_path,
            "parsed": row,
        }
        notices.append(
            {
                "source_row_number": row["source_row_number"],
                "chapter_id": row["chapter_id"],
                "member_raw": row["member_raw"],
                "member_type": row["member_type"],
                "row_payload": row.get("row_payload") or {},
                "source_payload": payload,
            }
        )

    return {
        "MessageId": _message_identifier(message),
        "ReceivedAt": received_at,
        "NoticeDate": notice_date,
        "NoticesJson": json.dumps(notices, sort_keys=True, default=str),
        "PointsPerRenewal": CHAPTER_RENEWAL_POINTS,
        "DryRun": 0,
        "RunType": "daily",
    }


def _send_chapter_renewal_notice_summary_if_configured(
    access_token: str,
    result_rows: list[dict],
    source_subject: str,
    received_at: datetime,
) -> bool:
    """Send chapter renewal notice summary if configured."""
    recipients = _configured_email_recipients("CHAPTER_RENEWAL_NOTICE_EMAIL_TO")
    if not recipients:
        return False

    subject = f"Chapter renewal points processing - {received_at.date().isoformat()}"
    body = _chapter_renewal_notice_summary_body(result_rows, source_subject, received_at)
    try:
        _send_gmail_plain_text(access_token, recipients, subject, body)
        return True
    except Exception:
        logging.exception("Failed to send Chapter Rewards renewal notice summary email.")
        return False


def _coerce_summary_received_at(value: object) -> datetime:
    """Coerce summary received at."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    try:
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except Exception:
        logging.warning("Could not parse staged-event received_at %r for summary email; using current time.", value)
        return datetime.now(timezone.utc)


def _send_pending_chapter_renewals_email_if_configured(access_token: str, rows: list[dict], as_of_date: date) -> bool:
    """Send pending chapter renewals email if configured."""
    recipients = _configured_email_recipients("CHAPTER_RENEWAL_PENDING_EMAIL_TO")
    if not recipients:
        recipients = _configured_email_recipients("CHAPTER_RENEWAL_NOTICE_EMAIL_TO")
    if not recipients:
        return False

    subject = f"Pending chapter renewals after rewards debit - {as_of_date.isoformat()}"
    body = _pending_chapter_renewals_email_body(rows, as_of_date)
    try:
        _send_gmail_plain_text(access_token, recipients, subject, body)
        return True
    except Exception:
        logging.exception("Failed to send pending Chapter Rewards renewal email.")
        return False


def _pending_chapter_renewals_email_body(rows: list[dict], as_of_date: date) -> str:
    """Execute the pending chapter renewals email body routine."""
    lines = [
        "Chapter Rewards pending ClubExpress renewal follow-up.",
        "",
        f"As of: {as_of_date.isoformat()}",
        f"Pending debited chapters: {len(rows)}",
        "",
    ]

    if not rows:
        lines.append("No chapters are currently debited for renewal and waiting on a ClubExpress renewal confirmation email.")
        return "\n".join(lines).rstrip() + "\n"

    lines.extend(
        [
            "These chapters had Chapter Rewards points debited for ClubExpress chapter renewal, but no matching ClubExpress chapter renewal email has been received yet.",
            "",
        ]
    )
    for row in rows:
        chapter_id = row.get("ChapterID")
        code = row.get("Chapter_Code") or ""
        name = row.get("Chapter_Name") or ""
        notice_date = _format_email_value(row.get("Notice_Date"))
        pending_days = row.get("Pending_Days")
        points = row.get("Points_Required")
        transaction_id = row.get("TransactionID")
        lines.append(
            f"- {code} {name} ({chapter_id}): {points} points debited on {notice_date}; "
            f"pending {pending_days} day(s); txn {transaction_id}"
        )

    lines.extend(
        [
            "",
            "A chapter drops from this list after the mailbox processes its ClubExpress renewal email with Member Type = Chapter.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _chapter_renewal_notice_summary_body(result_rows: list[dict], source_subject: str, received_at: datetime) -> str:
    """Execute the chapter renewal notice summary body routine."""
    posted = [row for row in result_rows if str(row.get("Decision") or "").lower() == "posted"]
    already_posted = [row for row in result_rows if str(row.get("Decision") or "").lower() == "already_posted"]
    insufficient = [row for row in result_rows if str(row.get("Decision") or "").lower() == "insufficient_points"]
    not_found = [row for row in result_rows if str(row.get("Decision") or "").lower() == "chapter_not_found"]

    lines = [
        "Chapter Rewards automatic chapter renewal processing is complete.",
        "",
        f"Source email: {source_subject or CHAPTER_RENEWAL_NOTICE_SUBJECT}",
        f"Received: {received_at.isoformat()}",
        f"Renewal points required per chapter: {CHAPTER_RENEWAL_POINTS}",
        "",
        f"Debited: {len(posted)}",
        f"Already debited: {len(already_posted)}",
        f"Insufficient points: {len(insufficient)}",
        f"Chapter not found: {len(not_found)}",
        "",
    ]

    lines.extend(_chapter_renewal_notice_summary_section("Debited", posted))
    lines.extend(_chapter_renewal_notice_summary_section("Already Debited", already_posted))
    lines.extend(_chapter_renewal_notice_summary_section("Insufficient Points", insufficient))
    lines.extend(_chapter_renewal_notice_summary_section("Chapter Not Found", not_found))
    return "\n".join(lines).rstrip() + "\n"


def _chapter_renewal_notice_summary_section(title: str, rows: list[dict]) -> list[str]:
    """Execute the chapter renewal notice summary section routine."""
    if not rows:
        return [f"{title}: none", ""]

    lines = [f"{title}:"]
    for row in rows:
        chapter_id = row.get("ChapterID")
        code = row.get("Chapter_Code") or ""
        name = row.get("Chapter_Name") or ""
        available = row.get("Available_Points")
        required = row.get("Points_Required")
        transaction_id = row.get("TransactionID")
        suffix = f", txn {transaction_id}" if transaction_id else ""
        lines.append(f"- {chapter_id} {code} {name}: available {available}, required {required}{suffix}")
    lines.append("")
    return lines


def _format_email_value(value: object) -> str:
    """Format email value."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _configured_email_recipients(setting_name: str) -> list[str]:
    """Execute the configured email recipients routine."""
    raw_value = os.environ.get(setting_name, "")
    return [part.strip() for part in re.split(r"[;,]", raw_value) if part.strip()]


def _env_positive_int(setting_name: str, default_value: int) -> int:
    """Execute the env positive int routine."""
    raw_value = os.environ.get(setting_name, "")
    if not raw_value:
        return default_value
    try:
        value = int(raw_value)
    except ValueError:
        logging.warning("Invalid positive integer setting %s=%r; using %s.", setting_name, raw_value, default_value)
        return default_value
    if value <= 0:
        logging.warning("Invalid positive integer setting %s=%r; using %s.", setting_name, raw_value, default_value)
        return default_value
    return value


def _chapter_renewal_notice_decision_counts(result_rows: list[dict]) -> dict[str, int]:
    """Execute the chapter renewal notice decision counts routine."""
    counts: dict[str, int] = {}
    for row in result_rows:
        decision = str(row.get("Decision") or "").strip().lower()
        if not decision:
            decision = "unknown"
        counts[decision] = counts.get(decision, 0) + 1
    return counts


def _downstream_procedure_calls(procedures: list[DownstreamProcedure]) -> list[tuple[str, dict]]:
    """Execute the downstream procedure calls routine."""
    return [(procedure.name, procedure.params) for procedure in procedures]


def _clubexpress_parsed_event_staging_enabled() -> bool:
    """Execute the clubexpress parsed event staging enabled routine."""
    return _is_truthy(os.environ.get("CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED", "false"))


def _clubexpress_staged_new_member_consumption_enabled() -> bool:
    """Execute the clubexpress staged new member consumption enabled routine."""
    return (
        _clubexpress_parsed_event_staging_enabled()
        and _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_NEW_MEMBER_CONSUMPTION_ENABLED", "false"))
    )


def _clubexpress_staged_renewal_consumption_enabled() -> bool:
    """Execute the clubexpress staged renewal consumption enabled routine."""
    return (
        _clubexpress_parsed_event_staging_enabled()
        and _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_RENEWAL_CONSUMPTION_ENABLED", "false"))
    )


def _clubexpress_staged_chapter_renewal_notice_consumption_enabled() -> bool:
    """Execute the clubexpress staged chapter renewal notice consumption enabled routine."""
    return (
        _clubexpress_parsed_event_staging_enabled()
        and _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_CHAPTER_RENEWAL_NOTICE_CONSUMPTION_ENABLED", "false"))
    )


def _clubexpress_staged_memchap_consumption_enabled() -> bool:
    """Execute the clubexpress staged memchap consumption enabled routine."""
    return (
        _clubexpress_parsed_event_staging_enabled()
        and _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_MEMCHAP_CONSUMPTION_ENABLED", "false"))
    )


def _clubexpress_staged_chapter_consumption_enabled() -> bool:
    """Execute the clubexpress staged chapter consumption enabled routine."""
    return (
        _clubexpress_parsed_event_staging_enabled()
        and _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_CHAPTER_CONSUMPTION_ENABLED", "false"))
    )


def _clubexpress_staged_member_categories_consumption_enabled() -> bool:
    """Execute the clubexpress staged member categories consumption enabled routine."""
    return (
        _clubexpress_parsed_event_staging_enabled()
        and _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_MEMBER_CATEGORIES_CONSUMPTION_ENABLED", "false"))
    )


def _clubexpress_staged_journal_consumption_enabled() -> bool:
    """Execute the clubexpress staged journal consumption enabled routine."""
    return (
        _clubexpress_parsed_event_staging_enabled()
        and _is_truthy(os.environ.get("CLUBEXPRESS_STAGED_JOURNAL_CONSUMPTION_ENABLED", "false"))
    )


def _record_clubexpress_parsed_event(conn_str: str, parsed_event: ClubExpressParsedEvent) -> None:
    """Record clubexpress parsed event."""
    if not _clubexpress_parsed_event_staging_enabled():
        return
    _execute_stored_procedure(
        conn_str,
        CLUBEXPRESS_PARSED_EVENT_STAGING_PROC,
        parsed_event.record_params(),
    )


def _mark_clubexpress_parsed_event_processed(
    conn_str: str,
    parsed_event: ClubExpressParsedEvent,
    result_payload: dict,
) -> None:
    """Execute the mark clubexpress parsed event processed routine."""
    if not _clubexpress_parsed_event_staging_enabled():
        return
    _execute_stored_procedure(
        conn_str,
        CLUBEXPRESS_PARSED_EVENT_STATUS_PROC,
        parsed_event_status_params(
            parsed_event.event_key,
            "processed",
            result_payload=result_payload,
        ),
    )


def _mark_clubexpress_parsed_event_error(
    conn_str: str,
    parsed_event: ClubExpressParsedEvent,
    exc: Exception,
) -> None:
    """Execute the mark clubexpress parsed event error routine."""
    if not _clubexpress_parsed_event_staging_enabled():
        return
    try:
        _execute_stored_procedure(
            conn_str,
            CLUBEXPRESS_PARSED_EVENT_STATUS_PROC,
            parsed_event_status_params(
                parsed_event.event_key,
                "error",
                error_message=str(exc),
            ),
        )
    except Exception:
        logging.exception("Failed updating ClubExpress parsed-event status for %s", parsed_event.event_key)


def _stored_procedure_call(proc_name: str, params: dict) -> tuple[str, list]:
    """Execute the stored procedure call routine."""
    ordered_items = [(key, value) for key, value in params.items()]
    sql = f"EXEC {proc_name} " + ", ".join(f"@{name} = ?" for name, _ in ordered_items)
    return sql, [value for _, value in ordered_items]


def _execute_stored_procedure(conn_str: str, proc_name: str, params: dict) -> None:
    """Execute stored procedure."""
    _execute_stored_procedures(conn_str, [(proc_name, params)])


def _execute_stored_procedure_rows(conn_str: str, proc_name: str, params: dict) -> list[dict]:
    """Execute stored procedure rows."""
    sql, values = _stored_procedure_call(proc_name, params)

    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(sql, values)
        rows = []
        if cursor.description:
            columns = [column[0] for column in cursor.description]
            rows = [dict(zip(columns, record)) for record in cursor.fetchall()]
        conn.commit()
        cursor.close()
        return rows
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


class _ClubExpressStagedEventSqlAdapter:
    """Represent club express staged event sql adapter."""
    def __init__(self, conn_str: str) -> None:
        """Initialize the club express staged event sql adapter instance."""
        self.conn_str = conn_str

    def query_rows(self, query: str, params: Iterable[object] = ()) -> list[dict[str, object]]:
        """Query rows."""
        conn = pyodbc.connect(self.conn_str)
        try:
            cursor = conn.cursor()
            cursor.execute(query, *tuple(params))
            rows = []
            if cursor.description:
                columns = [column[0] for column in cursor.description]
                rows = [dict(zip(columns, record)) for record in cursor.fetchall()]
            cursor.close()
            return rows
        finally:
            conn.close()

    def execute_statements(self, statements: Iterable[tuple[str, tuple[object, ...]]]) -> None:
        """Execute statements."""
        conn = pyodbc.connect(self.conn_str)
        try:
            cursor = conn.cursor()
            for query, params in statements:
                cursor.execute(query, *tuple(params))
            conn.commit()
            cursor.close()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _execute_stored_procedures(conn_str: str, procedures: Iterable[tuple[str, dict]]) -> None:
    """Execute stored procedures."""
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        for proc_name, params in procedures:
            sql, values = _stored_procedure_call(proc_name, params)
            cursor.execute(sql, values)
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _repo_root() -> Path:
    """Execute the repo root routine."""
    return Path(__file__).resolve().parent



def _get_sql_connection_string() -> Optional[str]:
    """Return sql connection string."""
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
    """Return gmail access token."""
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
    """List gmail messages."""
    mailbox_user = _require_env("GOOGLE_WORKSPACE_MAILBOX")
    max_results = int(os.environ.get("CLUBEXPRESS_MAILBOX_BATCH_SIZE", "10"))
    query_text = os.environ.get(
        "GOOGLE_WORKSPACE_QUERY",
        'in:inbox -label:ProcessedByFunction (subject:"New Member Signup - Payment" OR subject:"American Go Association - Member Renewal" OR subject:"Membership Renewal Emails" OR subject:"American Go E - Journal" OR subject:"Weekly American Go E - Journal" OR has:attachment)',
    )
    response = _gmail_json_request(
        access_token,
        f"/users/{parse.quote(mailbox_user)}/messages",
        query={"q": query_text, "maxResults": str(max_results)},
    )
    return response.get("messages", [])


def _get_gmail_message(access_token: str, message_id: str) -> dict:
    """Return gmail message."""
    mailbox_user = _require_env("GOOGLE_WORKSPACE_MAILBOX")
    return _gmail_json_request(
        access_token,
        f"/users/{parse.quote(mailbox_user)}/messages/{parse.quote(message_id)}",
        query={"format": "full"},
    )


def _mark_gmail_message_processed(access_token: str, message: dict) -> None:
    """Execute the mark gmail message processed routine."""
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
    """Ensure gmail label."""
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


def _send_gmail_plain_text(access_token: str, recipients: list[str], subject: str, body: str) -> None:
    """Send gmail plain text."""
    mailbox_user = _require_env("GOOGLE_WORKSPACE_MAILBOX")
    sender = os.environ.get("CHAPTER_RENEWAL_NOTICE_EMAIL_FROM", mailbox_user)

    message = EmailMessage()
    message["To"] = ", ".join(recipients)
    message["From"] = sender
    message["Subject"] = subject
    message.set_content(body)

    raw = base64.urlsafe_b64encode(message.as_bytes()).decode("ascii").rstrip("=")
    _gmail_json_request(
        access_token,
        f"/users/{parse.quote(mailbox_user)}/messages/send",
        method="POST",
        body={"raw": raw},
    )


def _gmail_json_request(
    access_token: str,
    path: str,
    *,
    method: str = "GET",
    query: Optional[dict[str, str]] = None,
    body: Optional[dict] = None,
) -> dict:
    """Execute the gmail json request routine."""
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
    """Extract gmail attachments."""
    attachments = []
    payload = message.get("payload") or {}
    _collect_gmail_attachments(access_token, message, payload, attachments)
    return attachments


def _collect_gmail_attachments(access_token: str, message: dict, part: dict, attachments: list[dict]) -> None:
    """Execute the collect gmail attachments routine."""
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
    """Return gmail attachment bytes."""
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
    """Return header value."""
    headers = ((message.get("payload") or {}).get("headers") or [])
    for header in headers:
        if (header.get("name") or "").lower() == header_name.lower():
            return header.get("value") or ""
    return ""


def _decode_base64url(value: str) -> bytes:
    """Decode base64url."""
    padding = '=' * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _decode_base64url_to_text(value: str) -> str:
    """Decode base64url to text."""
    return _decode_base64url(value).decode("utf-8", errors="replace")


def _require_env(name: str) -> str:
    """Execute the require env routine."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required application setting {name}.")
    return value


def _is_truthy(value: str) -> bool:
    """Return whether truthy."""
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _extract_csv_bytes(req: func.HttpRequest) -> bytes:
    """Extract csv bytes."""
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
    """Parse date."""
    return csv_parse_date(value)


def _parse_datetime(value: str) -> datetime:
    """Parse datetime."""
    return csv_parse_datetime(value)


def _normalize_header(fieldnames: Iterable[Optional[str]]) -> list[str]:
    """Normalize header."""
    return csv_normalize_header(fieldnames)


def _canonicalize_header(value: str) -> str:
    """Execute the canonicalize header routine."""
    return csv_canonicalize_header(value)


def _parse_csv_rows(csv_bytes: bytes) -> list[tuple]:
    """Parse csv rows."""
    return parse_memchap_rows(csv_bytes)


def _parse_member_category_rows(csv_bytes: bytes) -> list[tuple[int, str]]:
    """Parse member category rows."""
    return parse_csv_member_category_rows(csv_bytes)


def _parse_chapter_rows(csv_bytes: bytes) -> list[tuple]:
    """Parse chapter rows."""
    return parse_csv_chapter_rows(csv_bytes)


def _read_csv_matrix(csv_bytes: bytes, *, raise_on_error: bool = True) -> list[list[str]]:
    """Read csv matrix."""
    return csv_read_csv_matrix(csv_bytes, raise_on_error=raise_on_error)


def _decode_csv_text(csv_bytes: bytes) -> str:
    """Decode csv text."""
    return csv_decode_csv_text(csv_bytes)


def _is_member_agaid(agaid: Optional[int]) -> bool:
    """Return whether member agaid."""
    return csv_is_member_agaid(agaid)


def _stage_and_import(conn_str: str, rows: list[tuple]) -> None:
    """Stage and import."""
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


def _stage_and_import_chapters(conn_str: str, rows: list[tuple]) -> None:
    """Stage and import chapters."""
    insert_sql = (
        "INSERT INTO staging.chapters ("
        + ", ".join(f"[{column}]" for column in CHAPTER_COLUMNS)
        + ") VALUES ("
        + ", ".join("?" for _ in CHAPTER_COLUMNS)
        + ")"
    )

    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE staging.chapters")
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, rows)
        cursor.execute("EXEC membership.sp_import_chapters")
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _stage_and_import_member_categories(conn_str: str, rows: list[tuple[int, str]]) -> None:
    """Stage and import member categories."""
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

















