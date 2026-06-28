import base64
import json
import logging
import os
import re
from datetime import date, datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable, Optional
from urllib import error, parse, request

import azure.functions as func

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


app = func.FunctionApp()

GMAIL_API_BASE_URL = "https://gmail.googleapis.com/gmail/v1"
GMAIL_TOKEN_URL = "https://oauth2.googleapis.com/token"

DEFAULT_REWARDS_SNAPSHOT_SCHEDULE = os.environ.get("REWARDS_SNAPSHOT_SCHEDULE", "0 10 5 * * *")
DEFAULT_REWARDS_MEMBERSHIP_AWARDS_SCHEDULE = os.environ.get("REWARDS_MEMBERSHIP_AWARDS_SCHEDULE", "0 20 5 * * *")
DEFAULT_REWARDS_RATED_GAME_AWARDS_SCHEDULE = os.environ.get("REWARDS_RATED_GAME_AWARDS_SCHEDULE", "0 30 5 * * *")
DEFAULT_REWARDS_TOURNAMENT_AWARDS_SCHEDULE = os.environ.get("REWARDS_TOURNAMENT_AWARDS_SCHEDULE", "0 35 5 * * *")
DEFAULT_REWARDS_EXPIRATIONS_SCHEDULE = os.environ.get("REWARDS_EXPIRATIONS_SCHEDULE", "0 40 5 * * *")
DEFAULT_PENDING_CHAPTER_RENEWALS_EMAIL_SCHEDULE = os.environ.get("PENDING_CHAPTER_RENEWALS_EMAIL_SCHEDULE", "0 50 5 * * *")
DEFAULT_REWARDS_LEDGER_START_DATE = "2026-05-02"

REWARDS_DAILY_SNAPSHOT_PROC = "rewards.sp_create_daily_snapshot"
REWARDS_MEMBERSHIP_AWARDS_PROC = "rewards.sp_process_membership_awards"
REWARDS_RATED_GAME_AWARDS_PROC = "rewards.sp_process_rated_game_awards"
REWARDS_TOURNAMENT_AWARDS_PROC = "rewards.sp_process_tournament_awards"
REWARDS_EXPIRATIONS_PROC = "rewards.sp_process_point_expirations"
REWARDS_PENDING_CHAPTER_RENEWALS_PROC = "rewards.sp_get_pending_chapter_renewals"


class GmailApiError(RuntimeError):
    """Represent gmail api error failures."""
    pass


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


def _stored_procedure_call(proc_name: str, params: dict) -> tuple[str, list]:
    """Execute the stored procedure call routine."""
    ordered_items = [(key, value) for key, value in params.items()]
    sql = f"EXEC {proc_name} " + ", ".join(f"@{name} = ?" for name, _ in ordered_items)
    return sql, [value for _, value in ordered_items]


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
        detail = exc.read().decode("utf-8", errors="ignore")
        raise GmailApiError(f"Gmail token request failed: {detail}") from exc


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


def _require_env(name: str) -> str:
    """Execute the require env routine."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required application setting {name}.")
    return value


def _is_truthy(value: str) -> bool:
    """Return whether truthy."""
    return value.strip().lower() in {"1", "true", "yes", "on"}
