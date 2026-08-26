# SPDX-FileCopyrightText: 2010 Philip Waldron
# SPDX-FileCopyrightText: 2026 American Go Association
# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

import azure.functions as func


APP_ROOT = Path(__file__).resolve().parent
REPO_ROOT = APP_ROOT.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

try:
    from bayrate.auth import authorize_bayrate_admin
    from bayrate.commit_staged_run import build_commit_plan, commit_staged_run, printable_commit_plan
    from bayrate.replay_staged_run import run_staged_replay
    from bayrate.sql_adapter import SqlAdapter
    from bayrate.stage_reports import (
        apply_tournament_review_decision,
        build_insert_statements,
        build_staging_payload,
        ensure_payload_run_id,
        explain_staged_run_review,
        load_host_chapter_options,
        load_staged_run,
        printable_payload,
        update_staged_run_review,
    )

    BAYRATE_IMPORT_ERROR = None
except Exception as exc:
    authorize_bayrate_admin = None
    build_commit_plan = None
    commit_staged_run = None
    printable_commit_plan = None
    run_staged_replay = None
    SqlAdapter = None
    apply_tournament_review_decision = None
    build_insert_statements = None
    build_staging_payload = None
    ensure_payload_run_id = None
    explain_staged_run_review = None
    load_host_chapter_options = None
    load_staged_run = None
    printable_payload = None
    update_staged_run_review = None
    BAYRATE_IMPORT_ERROR = str(exc)


app = func.FunctionApp()


def get_sql_connection_string() -> str | None:
    """Return sql connection string."""
    conn = os.environ.get("SQL_CONNECTION_STRING") or os.environ.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
    if conn:
        return conn
    for settings_path in (APP_ROOT / "local.settings.json", REPO_ROOT / "local.settings.json"):
        if not settings_path.exists():
            continue
        try:
            values = json.loads(settings_path.read_text(encoding="utf-8")).get("Values", {})
        except (OSError, json.JSONDecodeError):
            continue
        conn = values.get("SQL_CONNECTION_STRING") or values.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
        if conn:
            return conn
    return None


SQL_CONNECTION_STRING = get_sql_connection_string()
if not SQL_CONNECTION_STRING:
    raise RuntimeError(
        "Missing SQL connection string. Set SQL_CONNECTION_STRING or MYSQL_SYNC_SQL_CONNECTION_STRING "
        "in Function App settings or local.settings.json."
    )


def response_headers(content_type: str) -> dict[str, str]:
    """Execute the response headers routine."""
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, X-Functions-Key, x-functions-key",
        "Cache-Control": "no-store",
        "Content-Type": content_type,
    }


def json_safe_value(value: Any) -> Any:
    """Execute the json safe value routine."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def load_html(template_name: str, api_base: str = "") -> str:
    """Load html."""
    markup = (APP_ROOT / template_name).read_text(encoding="utf-8")
    return (
        markup.replace('"__BAYRATE_API_BASE__"', json.dumps(api_base))
        .replace('"__RATINGS_EXPLORER_API_BASE__"', json.dumps(api_base))
    )


def _bayrate_json_response(payload: dict, status_code: int = 200) -> func.HttpResponse:
    """Execute the bayrate json response routine."""
    return func.HttpResponse(
        json.dumps(payload, default=json_safe_value),
        status_code=status_code,
        headers=response_headers("application/json; charset=utf-8"),
    )


def _bayrate_preview_error(message: str, status_code: int = 400) -> func.HttpResponse:
    """Execute the bayrate preview error routine."""
    payload = {"ok": False, "error": message}
    if BAYRATE_IMPORT_ERROR:
        payload["import_error"] = BAYRATE_IMPORT_ERROR
    return _bayrate_json_response(payload, status_code=status_code)


def _bayrate_modules_available(*names: str) -> bool:
    """Execute the bayrate modules available routine."""
    modules = {
        "stage": build_staging_payload is not None and printable_payload is not None,
        "load": load_staged_run is not None and printable_payload is not None,
        "write": build_insert_statements is not None and ensure_payload_run_id is not None,
        "review": (
            load_staged_run is not None
            and apply_tournament_review_decision is not None
            and update_staged_run_review is not None
        ),
        "replay": run_staged_replay is not None,
        "commit": build_commit_plan is not None and commit_staged_run is not None and printable_commit_plan is not None,
    }
    return all(modules.get(name, False) for name in names)


def _bayrate_adapter_or_error() -> tuple[object | None, func.HttpResponse | None]:
    """Execute the bayrate adapter or error routine."""
    if SqlAdapter is None:
        return None, _bayrate_preview_error("BayRate SQL adapter is not available in this deployment.", status_code=500)
    return SqlAdapter(SQL_CONNECTION_STRING), None


def _bayrate_login_redirect(req: func.HttpRequest) -> str:
    """Execute the bayrate login redirect routine."""
    raw_url = getattr(req, "url", "") or "/api/bayrate"
    parsed = urlsplit(raw_url)
    redirect_path = parsed.path or "/api/bayrate"
    if parsed.query:
        redirect_path = f"{redirect_path}?{parsed.query}"
    return f"/.auth/login/aad?post_login_redirect_uri={quote(redirect_path, safe='')}"


def _bayrate_authorization_response(
    req: func.HttpRequest,
    adapter: object,
    *,
    html: bool = False,
) -> tuple[dict | None, func.HttpResponse | None]:
    """Execute the bayrate authorization response routine."""
    if authorize_bayrate_admin is None:
        message = "BayRate authorization modules are not available in this deployment."
        if html:
            return None, func.HttpResponse(message, status_code=500, headers=response_headers("text/plain; charset=utf-8"))
        return None, _bayrate_preview_error(message, status_code=500)

    result = authorize_bayrate_admin(req.headers, adapter)
    if result.ok:
        return {
            "principal_name": result.principal.principal_name if result.principal else None,
            "principal_id": result.principal.principal_id if result.principal else None,
            "identity_provider": result.principal.identity_provider if result.principal else None,
        }, None

    if html and result.status_code == 401:
        headers = response_headers("text/plain; charset=utf-8")
        headers["Location"] = _bayrate_login_redirect(req)
        return None, func.HttpResponse("", status_code=302, headers=headers)

    message = result.error or "BayRate authorization failed."
    if html:
        return None, func.HttpResponse(message, status_code=result.status_code, headers=response_headers("text/plain; charset=utf-8"))
    return None, _bayrate_json_response(
        {
            "ok": False,
            "error": message,
            "authorization": {
                "status_code": result.status_code,
                "principal_name": result.principal.principal_name if result.principal else None,
            },
        },
        status_code=result.status_code,
    )


def _bayrate_request_json(req: func.HttpRequest) -> tuple[dict | None, func.HttpResponse | None]:
    """Execute the bayrate request json routine."""
    try:
        body = req.get_json()
    except ValueError:
        return None, _bayrate_preview_error("Request body must be JSON.")
    if not isinstance(body, dict):
        return None, _bayrate_preview_error("Request body must be a JSON object.")
    return body, None


def _bayrate_report_inputs_from_body(
    body: dict,
    adapter: object | None = None,
) -> tuple[list[tuple[str, str]] | None, list[dict] | None, func.HttpResponse | None]:
    """Execute the bayrate report inputs from body routine."""
    reports = body.get("reports")
    if not isinstance(reports, list) or not reports:
        return None, None, _bayrate_preview_error("At least one report is required.")
    if len(reports) > 20:
        return None, None, _bayrate_preview_error("At most 20 reports can be previewed at once.")

    report_inputs = []
    report_metadata = []
    host_options_by_id = None
    for index, item in enumerate(reports, start=1):
        if not isinstance(item, dict):
            return None, None, _bayrate_preview_error(f"Report {index} must be an object.")
        source_name = str(item.get("source_name") or f"pasted-report-{index}.txt").strip()
        content = str(item.get("content") or "")
        if not content.strip():
            return None, None, _bayrate_preview_error(f"Report {index} is empty.")
        if len(content) > 500_000:
            return None, None, _bayrate_preview_error(f"Report {index} is too large.")
        metadata, metadata_error, host_options_by_id = _bayrate_report_metadata_from_item(
            item,
            adapter=adapter,
            host_options_by_id=host_options_by_id,
        )
        if metadata_error:
            return None, None, _bayrate_preview_error(f"Report {index}: {metadata_error}")
        report_inputs.append((source_name, content))
        report_metadata.append(metadata)
    return report_inputs, report_metadata, None


def _bayrate_report_metadata_from_item(
    item: dict,
    *,
    adapter: object | None,
    host_options_by_id: dict | None,
) -> tuple[dict, str | None, dict | None]:
    """Execute the bayrate report metadata from item routine."""
    raw_metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    metadata = {}
    text_fields = {
        "tournament_descr": 255,
        "city": 100,
        "state_code": 20,
        "country_code": 20,
        "reward_event_key": 128,
        "reward_event_name": 255,
    }
    for key, max_length in text_fields.items():
        if key not in raw_metadata:
            continue
        value = str(raw_metadata.get(key) or "").strip()
        if value and len(value) > max_length:
            return {}, f"{key} must be {max_length} characters or fewer.", host_options_by_id
        metadata[key] = value or None

    if "reward_is_state_championship" in raw_metadata:
        metadata["reward_is_state_championship"] = bool(raw_metadata.get("reward_is_state_championship"))

    raw_host_id = raw_metadata.get("host_chapter_id")
    if raw_host_id in (None, ""):
        return metadata, None, host_options_by_id
    try:
        host_chapter_id = int(str(raw_host_id).strip())
    except (TypeError, ValueError):
        return {}, "host_chapter_id must be an integer.", host_options_by_id
    if host_chapter_id == 0 or host_chapter_id < -1:
        return {}, "host_chapter_id must be a positive chapter ID or -1 for AGA.", host_options_by_id
    if adapter is not None and load_host_chapter_options is not None:
        if host_options_by_id is None:
            host_options_by_id = {
                int(option.get("chapter_id")): option
                for option in load_host_chapter_options(adapter)
                if option.get("chapter_id") is not None
            }
        option = host_options_by_id.get(host_chapter_id)
        if option is None:
            return {}, f"Host chapter {host_chapter_id} was not found.", host_options_by_id
        metadata["host_chapter_id"] = host_chapter_id
        metadata["host_chapter_code"] = option.get("chapter_code")
        metadata["host_chapter_name"] = option.get("chapter_name")
    else:
        metadata["host_chapter_id"] = host_chapter_id
        metadata["host_chapter_code"] = str(raw_metadata.get("host_chapter_code") or "").strip() or None
        metadata["host_chapter_name"] = str(raw_metadata.get("host_chapter_name") or "").strip() or None
    return metadata, None, host_options_by_id


def _bayrate_payload_response(payload: dict, *, adapter: object | None = None, written: bool = False) -> dict:
    """Execute the bayrate payload response routine."""
    explanations = explain_staged_run_review(adapter, payload) if (adapter and explain_staged_run_review is not None) else None
    summary = printable_payload(payload, include_games=False)
    summary["written"] = written
    response = {
        "ok": True,
        "written": written,
        "summary": summary,
        "same_date_groups": _bayrate_same_date_groups(payload),
        "review_explanation": explanations,
    }
    if adapter and load_host_chapter_options is not None:
        response["host_chapter_options"] = load_host_chapter_options(adapter)
    if adapter and written and summary.get("run_id") is not None:
        response["commit_state"] = _bayrate_commit_state(adapter, summary.get("run_id"))
    return response


def _bayrate_host_chapter_from_body(adapter: object, body: dict) -> tuple[dict | None, func.HttpResponse | None]:
    """Execute the bayrate host chapter from body routine."""
    if "host_chapter_id" not in body and "hostChapterId" not in body:
        return None, None
    raw_id = body.get("host_chapter_id", body.get("hostChapterId"))
    try:
        host_chapter_id = int(str(raw_id).strip())
    except (TypeError, ValueError):
        return None, _bayrate_preview_error("host_chapter_id must be an integer.")
    if host_chapter_id == 0 or host_chapter_id < -1:
        return None, _bayrate_preview_error("host_chapter_id must be a positive chapter ID or -1 for AGA.")
    if load_host_chapter_options is None:
        return None, _bayrate_preview_error("BayRate host chapter lookup is not available.", status_code=500)
    for option in load_host_chapter_options(adapter):
        if int(option.get("chapter_id") or 0) == host_chapter_id:
            return option, None
    return None, _bayrate_preview_error(f"Host chapter {host_chapter_id} was not found.", status_code=404)


def _bayrate_optional_bool_from_body(body: dict, *names: str) -> bool | None:
    """Execute the bayrate optional bool from body routine."""
    for name in names:
        if name not in body:
            continue
        value = body.get(name)
        if isinstance(value, bool):
            return value
        text = str(value or "").strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off", ""}:
            return False
        return bool(value)
    return None


def _bayrate_commit_state(adapter: object, run_id: int | str) -> dict:
    """Execute the bayrate commit state routine."""
    rows = adapter.query_rows(
        """
SELECT
    (SELECT JSON_VALUE(
        CASE WHEN ISJSON([SummaryJson]) = 1 THEN [SummaryJson] ELSE N'{}' END,
        N'$.commit_status'
    ) FROM [ratings].[bayrate_runs] WHERE [RunID] = ?) AS [AuditCommitStatus],
    (SELECT JSON_VALUE(
        CASE WHEN ISJSON([SummaryJson]) = 1 THEN [SummaryJson] ELSE N'{}' END,
        N'$.superseded_by_run_id'
    ) FROM [ratings].[bayrate_runs] WHERE [RunID] = ?) AS [SupersededByRunID],
    (SELECT COUNT(*) FROM [ratings].[bayrate_staged_ratings] WHERE [RunID] = ?) AS [StagedRatingCount],
    (SELECT COUNT(*) FROM [ratings].[bayrate_staged_ratings] WHERE [RunID] = ? AND [Planned_Rating_Row_ID] IS NOT NULL) AS [PlannedRatingCount],
    (SELECT COUNT(*) FROM [ratings].[bayrate_staged_games] WHERE [RunID] = ?) AS [StagedGameCount],
    (SELECT COUNT(*) FROM [ratings].[bayrate_staged_games] WHERE [RunID] = ? AND [Game_ID] IS NOT NULL) AS [PlannedGameCount]
""",
        (run_id, run_id, run_id, run_id, run_id, run_id),
    )
    row = rows[0] if rows else {}
    audit_commit_status = str(row.get("AuditCommitStatus") or "").strip()
    superseded_by_run_id = row.get("SupersededByRunID")
    staged_rating_count = int(row.get("StagedRatingCount") or 0)
    planned_rating_count = int(row.get("PlannedRatingCount") or 0)
    staged_game_count = int(row.get("StagedGameCount") or 0)
    planned_game_count = int(row.get("PlannedGameCount") or 0)
    superseded = audit_commit_status == "superseded"
    partial_marker = (
        0 < planned_rating_count < staged_rating_count
        or 0 < planned_game_count < staged_game_count
        or (planned_rating_count > 0 and staged_rating_count == 0)
        or (planned_game_count > 0 and staged_game_count == 0)
    )
    committed = (
        staged_rating_count > 0
        and planned_rating_count == staged_rating_count
        and planned_game_count == staged_game_count
        and not partial_marker
    )
    if superseded:
        state = "superseded"
        committed = True
    elif partial_marker:
        state = "partial_commit_marker"
    elif committed:
        state = "committed"
    elif staged_rating_count:
        state = "replayed_uncommitted"
    else:
        state = "needs_replay"
    return {
        "state": state,
        "committed": committed,
        "audit_commit_status": audit_commit_status or None,
        "superseded_by_run_id": superseded_by_run_id,
        "has_staged_ratings": staged_rating_count > 0,
        "staged_rating_count": staged_rating_count,
        "planned_rating_count": planned_rating_count,
        "staged_game_count": staged_game_count,
        "planned_game_count": planned_game_count,
    }


def _bayrate_replay_response(artifact: dict) -> dict:
    """Execute the bayrate replay response routine."""
    plan = artifact.get("plan") or {}
    result = artifact.get("bayrate_result") or {}
    staged_rating_summary = artifact.get("staged_rating_summary") or {}
    return {
        "ok": True,
        "read_only": True,
        "run_id": plan.get("run_id"),
        "output_path": artifact.get("output_path"),
        "plan": plan,
        "staged_rating_summary": staged_rating_summary,
        "result_summary": {
            "event_count": result.get("event_count"),
            "player_count": result.get("player_count"),
            "pre_event_metrics": result.get("pre_event_metrics"),
            "post_event_fit_metrics": result.get("post_event_fit_metrics"),
            "rating_result_count": staged_rating_summary.get("rating_count", 0),
            "staged_rating_count": plan.get("staged_rating_count", 0),
        },
    }


def _bayrate_same_date_groups(payload: dict) -> list[dict]:
    """Execute the bayrate same date groups routine."""
    groups: dict[str, list[dict]] = {}
    for entry in payload.get("staged_tournaments") or []:
        row = entry.get("tournament_row") or {}
        tournament_date = row.get("Tournament_Date")
        key = tournament_date.isoformat() if hasattr(tournament_date, "isoformat") else str(tournament_date or "")
        groups.setdefault(key, []).append(
            {
                "source_report_ordinal": entry.get("source_report_ordinal"),
                "source_report_name": entry.get("source_report_name"),
                "tournament_code": row.get("Tournament_Code"),
                "title": row.get("Tournament_Descr"),
                "tournament_date": tournament_date,
                "status": entry.get("status"),
                "duplicate_candidate": entry.get("duplicate_candidate"),
                "game_count": sum(
                    1
                    for game in payload.get("staged_games") or []
                    if game.get("source_report_ordinal") == entry.get("source_report_ordinal")
                ),
            }
        )
    return [
        {
            "tournament_date": tournament_date,
            "events": events,
        }
        for tournament_date, events in sorted(groups.items())
        if tournament_date and len(events) > 1
    ]


@app.function_name(name="BayRateStagingPage")
@app.route(route="bayrate", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_page(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingPage Azure Function endpoint."""
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter, html=True)
    if auth_error:
        return auth_error
    return func.HttpResponse(
        load_html("bayrate_staging.html"),
        status_code=200,
        headers=response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="BayRateStagingPreview")
@app.route(route="bayrate/preview", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_preview(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingPreview Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("stage"):
        return _bayrate_preview_error("BayRate staging modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    report_inputs, report_metadata, error = _bayrate_report_inputs_from_body(body, adapter)
    if error:
        return error

    duplicate_check = bool(body.get("duplicate_check", True))
    try:
        payload = build_staging_payload(
            report_inputs,
            adapter=adapter,
            duplicate_check=duplicate_check,
            report_metadata=report_metadata,
        )
    except Exception as exc:
        return _bayrate_preview_error(str(exc))

    response_adapter = adapter if duplicate_check else None
    return _bayrate_json_response(
        {
            **_bayrate_payload_response(payload, adapter=response_adapter, written=False),
            "duplicate_check": duplicate_check and bool(adapter),
        }
    )


@app.function_name(name="BayRateMetadataOptions")
@app.route(route="bayrate/metadata-options", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_metadata_options(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateMetadataOptions Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=response_headers("application/json; charset=utf-8"))
    if load_host_chapter_options is None:
        return _bayrate_preview_error("BayRate metadata option lookup is not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    try:
        host_chapter_options = load_host_chapter_options(adapter)
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)
    return _bayrate_json_response({"ok": True, "host_chapter_options": host_chapter_options})


@app.function_name(name="BayRateStagingWrite")
@app.route(route="bayrate/stage", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_write(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingWrite Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("stage", "write"):
        return _bayrate_preview_error("BayRate staging write modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    if body.get("confirm_stage") is not True:
        return _bayrate_preview_error("confirm_stage=true is required.")
    report_inputs, report_metadata, error = _bayrate_report_inputs_from_body(body, adapter)
    if error:
        return error
    try:
        payload = build_staging_payload(
            report_inputs,
            adapter=adapter,
            duplicate_check=True,
            report_metadata=report_metadata,
        )
        ensure_payload_run_id(payload, adapter)
        adapter.execute_statements(build_insert_statements(payload))
        payload["written"] = True
        payload["dry_run"] = False
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)
    return _bayrate_json_response(_bayrate_payload_response(payload, adapter=adapter, written=True))


@app.function_name(name="BayRateStagingReview")
@app.route(route="bayrate/review", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_review(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingReview Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("stage", "review"):
        return _bayrate_preview_error("BayRate review modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    if body.get("confirm_review") is not True:
        return _bayrate_preview_error("confirm_review=true is required.")
    run_id = str(body.get("run_id") or "").strip()
    if not run_id:
        return _bayrate_preview_error("run_id is required.")
    source_report_ordinal = body.get("source_report_ordinal")
    if not isinstance(source_report_ordinal, int):
        return _bayrate_preview_error("source_report_ordinal must be an integer.")
    host_chapter, error = _bayrate_host_chapter_from_body(adapter, body)
    if error:
        return error
    try:
        payload = load_staged_run(adapter, run_id)
        apply_tournament_review_decision(
            payload,
            source_report_ordinal,
            use_duplicate_code=bool(body.get("use_duplicate_code", False)),
            mark_ready=bool(body.get("mark_ready", False)),
            operator_note=str(body.get("operator_note") or "").strip() or None,
            host_chapter_id=host_chapter.get("chapter_id") if host_chapter else None,
            host_chapter_code=host_chapter.get("chapter_code") if host_chapter else None,
            host_chapter_name=host_chapter.get("chapter_name") if host_chapter else None,
            reward_event_key=str(body.get("reward_event_key") or "").strip() or None
            if "reward_event_key" in body
            else None,
            reward_event_name=str(body.get("reward_event_name") or "").strip() or None
            if "reward_event_name" in body
            else None,
            reward_is_state_championship=_bayrate_optional_bool_from_body(
                body,
                "reward_is_state_championship",
                "rewardIsStateChampionship",
            ),
        )
        update_staged_run_review(adapter, payload)
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)
    return _bayrate_json_response(_bayrate_payload_response(payload, adapter=adapter, written=True))


@app.function_name(name="BayRateStagingRun")
@app.route(route="bayrate/run", methods=["GET", "POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_run(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingRun Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("load", "review"):
        return _bayrate_preview_error("BayRate staged-run modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error

    if req.method == "GET":
        run_id = str(req.params.get("run_id") or "").strip()
    else:
        body, error = _bayrate_request_json(req)
        if error:
            return error
        run_id = str(body.get("run_id") or "").strip()
    if not run_id:
        return _bayrate_preview_error("run_id is required.")

    try:
        payload = load_staged_run(adapter, run_id)
    except Exception as exc:
        status_code = 404 if "was not found" in str(exc) else 500
        return _bayrate_preview_error(str(exc), status_code=status_code)
    return _bayrate_json_response(_bayrate_payload_response(payload, adapter=adapter, written=True))


@app.function_name(name="BayRateStagingReplay")
@app.route(route="bayrate/replay", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_replay(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingReplay Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("replay"):
        return _bayrate_preview_error("BayRate replay modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    run_id = str(body.get("run_id") or "").strip()
    if not run_id:
        return _bayrate_preview_error("run_id is required.")
    try:
        artifact = run_staged_replay(
            adapter,
            run_id=run_id,
            allow_needs_review=bool(body.get("allow_needs_review", True)),
            write_artifact=False,
            persist_staged_ratings=bool(body.get("persist_staged_ratings", True)),
        )
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)
    response = _bayrate_replay_response(artifact)
    response["commit_state"] = _bayrate_commit_state(adapter, run_id)
    return _bayrate_json_response(response)


@app.function_name(name="BayRateStagingCommitPreview")
@app.route(route="bayrate/commit-preview", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_commit_preview(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingCommitPreview Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("commit"):
        return _bayrate_preview_error("BayRate commit modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    run_id = str(body.get("run_id") or "").strip()
    if not run_id:
        return _bayrate_preview_error("run_id is required.")
    try:
        plan = build_commit_plan(adapter, run_id)
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)
    return _bayrate_json_response(
        {
            "ok": True,
            "commit_plan": printable_commit_plan(plan),
            "commit_state": _bayrate_commit_state(adapter, run_id),
        }
    )


@app.function_name(name="BayRateStagingCommit")
@app.route(route="bayrate/commit", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_commit(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingCommit Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("commit"):
        return _bayrate_preview_error("BayRate commit modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    authorization, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    run_id = str(body.get("run_id") or "").strip()
    if not run_id:
        return _bayrate_preview_error("run_id is required.")
    if body.get("confirm_production_commit") is not True:
        return _bayrate_preview_error("confirm_production_commit=true is required.")
    expected_confirmation = f"COMMIT RUN {run_id}"
    if str(body.get("confirmation_text") or "").strip() != expected_confirmation:
        return _bayrate_preview_error(f'confirmation_text must be "{expected_confirmation}".')
    commit_plan_hash = str(body.get("commit_plan_hash") or "").strip()
    if not commit_plan_hash:
        return _bayrate_preview_error("commit_plan_hash is required. Preview the production commit before committing.")

    try:
        plan = commit_staged_run(
            adapter,
            run_id,
            confirm_production_commit=True,
            expected_plan_hash=commit_plan_hash,
            confirm_sgf_replacement=body.get("confirm_sgf_replacement") is True,
            operator_principal_name=(authorization or {}).get("principal_name"),
            operator_principal_id=(authorization or {}).get("principal_id"),
        )
    except ValueError as exc:
        status_code = 409 if "changed since preview" in str(exc) else 400
        return _bayrate_preview_error(str(exc), status_code=status_code)
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)

    return _bayrate_json_response(
        {
            "ok": True,
            "commit_plan": printable_commit_plan(plan),
            "commit_state": _bayrate_commit_state(adapter, run_id),
        }
    )
