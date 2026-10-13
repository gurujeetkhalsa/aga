# Copyright 2026, American Go Association, All rights reserved

import base64
import csv
import hashlib
import json
import mimetypes
import os
import re
import sys
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from io import StringIO
from pathlib import Path
from time import perf_counter
from urllib.parse import quote, urlsplit

import azure.functions as func

import ratings_explorer_support as explorer
from bayrate_redirect import standalone_bayrate_url

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

try:
    from bayrate.auth import BAYRATE_RUN_PERMISSION, REWARDS_REDEMPTIONS_PERMISSION, SGF_IMPORT_PERMISSION, authorize_admin_permission, authorize_bayrate_admin
    from bayrate.commit_staged_run import build_commit_plan, commit_staged_run, printable_commit_plan
    from bayrate.merge_member_ratings import build_member_merge_plan, merge_member_ratings, printable_member_merge_plan
    from bayrate.sql_adapter import SqlAdapter
    from bayrate.replay_staged_run import run_staged_replay
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
except Exception:
    BAYRATE_RUN_PERMISSION = "bayrate_run"
    REWARDS_REDEMPTIONS_PERMISSION = "rewards_redemptions"
    SGF_IMPORT_PERMISSION = "sgf_import"
    authorize_admin_permission = None
    authorize_bayrate_admin = None
    build_commit_plan = None
    commit_staged_run = None
    printable_commit_plan = None
    build_member_merge_plan = None
    merge_member_ratings = None
    printable_member_merge_plan = None
    SqlAdapter = None
    run_staged_replay = None
    apply_tournament_review_decision = None
    build_insert_statements = None
    build_staging_payload = None
    ensure_payload_run_id = None
    explain_staged_run_review = None
    load_host_chapter_options = None
    load_staged_run = None
    printable_payload = None
    update_staged_run_review = None

try:
    from leago_sgf_import.workflow import apply_leago_import, preview_leago_import
except Exception:
    apply_leago_import = None
    preview_leago_import = None

try:
    from leago_sgf_import.archive_workflow import apply_archive_import, preview_archive_import
except Exception:
    apply_archive_import = None
    preview_archive_import = None

app = func.FunctionApp()
ALLOWED_SEARCH_LIMITS = {10, 25, 50, 100, 250}
DEFAULT_SEARCH_LIMIT = 25
ALLOWED_ACTIVITY_YEARS = {1, 3, 5, 10}
DEFAULT_RECENT_ACTIVITY_YEARS = 3
ALLOWED_RATING_BANDS = {item["value"] for item in explorer.RATING_FILTER_OPTIONS}
ALLOWED_PLAYER_STATUS_FILTERS = {"all", "active", "expired"}
SQL_CONNECTION_STRING = explorer.get_sql_connection_string()
if not SQL_CONNECTION_STRING:
    raise RuntimeError(
        "Missing SQL connection string. Set SQL_CONNECTION_STRING or MYSQL_SYNC_SQL_CONNECTION_STRING in Function App settings or local.settings.json."
    )


def _json_response(payload: dict) -> func.HttpResponse:
    """Execute the json response routine."""
    return func.HttpResponse(
        json.dumps(payload),
        status_code=200,
        headers=explorer.response_headers("application/json; charset=utf-8"),
    )


def _with_debug(payload: dict, **debug_fields) -> dict:
    """Execute the with debug routine."""
    enriched = dict(payload)
    enriched["_debug"] = {key: value for key, value in debug_fields.items() if value is not None}
    return enriched


def _public_json_default(value):
    """Execute the public json default routine."""
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return explorer.json_safe_value(value)


def _public_json_response(payload: dict, status_code: int = 200) -> func.HttpResponse:
    """Execute the public json response routine."""
    return func.HttpResponse(
        json.dumps(payload, default=_public_json_default),
        status_code=status_code,
        headers=explorer.response_headers("application/json; charset=utf-8"),
    )


def _utc_now_text() -> str:
    """Execute the utc now text routine."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _bayrate_json_response(payload: dict, status_code: int = 200) -> func.HttpResponse:
    """Execute the bayrate json response routine."""
    return func.HttpResponse(
        json.dumps(payload, default=explorer.json_safe_value),
        status_code=status_code,
        headers=explorer.response_headers("application/json; charset=utf-8"),
    )


def _bayrate_preview_error(message: str, status_code: int = 400) -> func.HttpResponse:
    """Execute the bayrate preview error routine."""
    return _bayrate_json_response({"ok": False, "error": message}, status_code=status_code)


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
        "member_merge": (
            build_member_merge_plan is not None
            and merge_member_ratings is not None
            and printable_member_merge_plan is not None
        ),
    }
    return all(modules.get(name, False) for name in names)


def _bayrate_adapter_or_error() -> tuple[object | None, func.HttpResponse | None]:
    """Execute the bayrate adapter or error routine."""
    if SqlAdapter is None:
        return None, _bayrate_preview_error("BayRate SQL adapter is not available in this deployment.", status_code=500)
    return SqlAdapter(SQL_CONNECTION_STRING), None


def _bayrate_login_redirect(req: func.HttpRequest) -> str:
    """Execute the bayrate login redirect routine."""
    raw_url = getattr(req, "url", "") or "/api/ratings-explorer/bayrate"
    parsed = urlsplit(raw_url)
    redirect_path = parsed.path or "/api/ratings-explorer/bayrate"
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
            return None, func.HttpResponse(message, status_code=500, headers=explorer.response_headers("text/plain; charset=utf-8"))
        return None, _bayrate_preview_error(message, status_code=500)

    result = authorize_bayrate_admin(req.headers, adapter)
    if result.ok:
        return {
            "principal_name": result.principal.principal_name if result.principal else None,
            "principal_id": result.principal.principal_id if result.principal else None,
            "identity_provider": result.principal.identity_provider if result.principal else None,
        }, None

    if html and result.status_code == 401:
        headers = explorer.response_headers("text/plain; charset=utf-8")
        headers["Location"] = _bayrate_login_redirect(req)
        return None, func.HttpResponse("", status_code=302, headers=headers)

    message = result.error or "BayRate authorization failed."
    if html:
        return None, func.HttpResponse(message, status_code=result.status_code, headers=explorer.response_headers("text/plain; charset=utf-8"))
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
    (SELECT COUNT(*) FROM [ratings].[bayrate_staged_ratings] WHERE [RunID] = ?) AS [StagedRatingCount],
    (SELECT COUNT(*) FROM [ratings].[bayrate_staged_ratings] WHERE [RunID] = ? AND [Planned_Rating_Row_ID] IS NOT NULL) AS [PlannedRatingCount],
    (SELECT COUNT(*) FROM [ratings].[bayrate_staged_games] WHERE [RunID] = ?) AS [StagedGameCount],
    (SELECT COUNT(*) FROM [ratings].[bayrate_staged_games] WHERE [RunID] = ? AND [Game_ID] IS NOT NULL) AS [PlannedGameCount]
""",
        (run_id, run_id, run_id, run_id),
    )
    row = rows[0] if rows else {}
    staged_rating_count = int(row.get("StagedRatingCount") or 0)
    planned_rating_count = int(row.get("PlannedRatingCount") or 0)
    staged_game_count = int(row.get("StagedGameCount") or 0)
    planned_game_count = int(row.get("PlannedGameCount") or 0)
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
    if partial_marker:
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


def _bayrate_member_merge_ids_from_body(body: dict) -> tuple[list[int] | None, int | None, func.HttpResponse | None]:
    """Execute the bayrate member merge ids from body routine."""
    raw_sources = body.get("source_agaids", body.get("sourceAgaids"))
    if raw_sources is None:
        raw_sources = body.get("source_agaid", body.get("sourceAgaid"))
    if raw_sources in (None, ""):
        return None, None, _bayrate_preview_error("source_agaid or source_agaids is required.")
    if isinstance(raw_sources, list):
        raw_source_values = raw_sources
    else:
        raw_source_values = [raw_sources]

    source_ids = []
    seen_sources = set()
    for value in raw_source_values:
        try:
            source_id = int(str(value).strip())
        except (TypeError, ValueError):
            return None, None, _bayrate_preview_error("source_agaids must contain only integers.")
        if source_id <= 0:
            return None, None, _bayrate_preview_error("source_agaids must be positive integers.")
        if source_id in seen_sources:
            continue
        seen_sources.add(source_id)
        source_ids.append(source_id)
    if not source_ids:
        return None, None, _bayrate_preview_error("At least one source AGAID is required.")

    raw_target = body.get("target_agaid", body.get("targetAgaid"))
    try:
        target_id = int(str(raw_target).strip())
    except (TypeError, ValueError):
        return None, None, _bayrate_preview_error("target_agaid is required and must be an integer.")
    if target_id <= 0:
        return None, None, _bayrate_preview_error("target_agaid must be a positive integer.")
    if target_id in seen_sources:
        return None, None, _bayrate_preview_error("target_agaid must be different from every source AGAID.")
    return source_ids, target_id, None


def _bayrate_member_merge_confirmation_text(source_agaids: list[int], target_agaid: int) -> str:
    """Execute the bayrate member merge confirmation text routine."""
    return f"MERGE {','.join(str(value) for value in source_agaids)} INTO {target_agaid}"


def _bayrate_member_merge_page_html() -> str:
    """Execute the bayrate member merge page html routine."""
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>BayRate Member Merge</title>
  <style>
    :root {
      --bg: #f6f7f9;
      --panel: #ffffff;
      --ink: #17202c;
      --muted: #667386;
      --line: #d8dee7;
      --accent: #256f70;
      --accent-strong: #174e50;
      --accent-soft: #e5f1ef;
      --danger: #a33a3a;
      --danger-soft: #feecec;
      --ready: #1f7a4d;
      --ready-soft: #e7f5ed;
      --warn: #9b5b16;
      --warn-soft: #fff4df;
      --shadow: 0 12px 28px rgba(24, 33, 47, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: Arial, Helvetica, sans-serif;
      color: var(--ink);
      background: var(--bg);
    }
    button, input {
      font: inherit;
    }
    button {
      min-height: 38px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 0 12px;
      background: var(--panel);
      color: var(--ink);
      font-weight: 800;
      cursor: pointer;
    }
    button.primary {
      border-color: var(--accent);
      background: var(--accent);
      color: #fff;
    }
    button.danger {
      border-color: var(--danger);
      background: var(--danger);
      color: #fff;
    }
    button:disabled {
      opacity: 0.55;
      cursor: not-allowed;
    }
    input {
      width: 100%;
      min-height: 38px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 0 10px;
      color: var(--ink);
      background: #fff;
      font-weight: 700;
    }
    .page {
      width: min(980px, 100%);
      margin: 0 auto;
      padding: 18px;
      display: grid;
      gap: 14px;
    }
    .topbar,
    .panel {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      box-shadow: var(--shadow);
    }
    .topbar {
      padding: 16px 18px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
    }
    .panel {
      padding: 14px;
      display: grid;
      gap: 12px;
    }
    h1, h2, p { margin: 0; }
    h1 { font-size: clamp(1.45rem, 2.4vw, 2rem); letter-spacing: 0; }
    h2 { font-size: 1.05rem; letter-spacing: 0; }
    a { color: var(--accent-strong); font-weight: 800; }
    .muted { color: var(--muted); }
    .grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(220px, 1fr));
      gap: 12px;
    }
    label {
      display: grid;
      gap: 5px;
      color: var(--muted);
      font-size: 0.88rem;
      font-weight: 800;
    }
    .actions,
    .row {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(120px, 1fr));
      gap: 8px;
    }
    .metric,
    .message {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fbfcfd;
      padding: 10px;
    }
    .metric-label {
      color: var(--muted);
      font-size: 0.78rem;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    .metric-value {
      margin-top: 4px;
      font-size: 1.25rem;
      font-weight: 800;
    }
    .message {
      line-height: 1.35;
      overflow-wrap: anywhere;
    }
    .message.ready {
      border-color: #a7d7bb;
      background: var(--ready-soft);
      color: #155637;
    }
    .message.warn {
      border-color: #e0a05a;
      background: var(--warn-soft);
      color: #5f370b;
    }
    .message.danger {
      border-color: #efb1b1;
      background: var(--danger-soft);
      color: var(--danger);
      font-weight: 800;
    }
    .list {
      display: grid;
      gap: 8px;
    }
    .hidden { display: none; }
    @media (max-width: 720px) {
      .grid,
      .metrics {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <main class="page">
    <header class="topbar">
      <div>
        <h1>BayRate Member Merge</h1>
        <p class="muted">Production rating identity maintenance</p>
      </div>
      <a href="/api/ratings-explorer/bayrate">BayRate Staging</a>
    </header>

    <section class="panel">
      <h2>Merge IDs</h2>
      <div class="grid">
        <label>
          Source AGAID, drop
          <input id="sourceInput" inputmode="numeric" autocomplete="off" />
        </label>
        <label>
          Target AGAID, keep
          <input id="targetInput" inputmode="numeric" autocomplete="off" />
        </label>
      </div>
      <div class="actions">
        <button id="previewBtn" class="primary" type="button">Preview Merge</button>
        <span id="statusText" class="muted"></span>
      </div>
    </section>

    <section id="previewPanel" class="panel hidden">
      <div class="row">
        <h2>Preview</h2>
        <span id="readyText" class="muted"></span>
      </div>
      <div id="metrics" class="metrics"></div>
      <div id="details" class="list"></div>
    </section>

    <section id="executePanel" class="panel hidden">
      <h2>Execute</h2>
      <div id="confirmationMessage" class="message warn"></div>
      <label>
        Confirmation text
        <input id="confirmationInput" autocomplete="off" />
      </label>
      <div class="actions">
        <button id="executeBtn" class="danger" type="button" disabled>Execute Merge</button>
      </div>
    </section>
  </main>

  <script>
    const mergeUrl = window.location.pathname.replace(/\\/$/, "");
    const previewUrl = mergeUrl.replace(/\\/member-merge$/, "/member-merge-preview");
    const state = { plan: null, confirmationText: "" };
    const els = {
      sourceInput: document.getElementById("sourceInput"),
      targetInput: document.getElementById("targetInput"),
      previewBtn: document.getElementById("previewBtn"),
      executeBtn: document.getElementById("executeBtn"),
      confirmationInput: document.getElementById("confirmationInput"),
      statusText: document.getElementById("statusText"),
      readyText: document.getElementById("readyText"),
      previewPanel: document.getElementById("previewPanel"),
      executePanel: document.getElementById("executePanel"),
      confirmationMessage: document.getElementById("confirmationMessage"),
      metrics: document.getElementById("metrics"),
      details: document.getElementById("details"),
    };

    function parseIds() {
      const sourceIds = String(els.sourceInput.value || "")
        .split(/[\\s,]+/)
        .map((value) => value.trim())
        .filter(Boolean)
        .map((value) => Number(value));
      const targetId = Number(String(els.targetInput.value || "").trim());
      if (!sourceIds.length || sourceIds.some((value) => !Number.isInteger(value) || value <= 0)) {
        throw new Error("Enter a source AGAID.");
      }
      if (!Number.isInteger(targetId) || targetId <= 0) {
        throw new Error("Enter a target AGAID.");
      }
      if (sourceIds.includes(targetId)) {
        throw new Error("Source and target must be different.");
      }
      return { source_agaids: Array.from(new Set(sourceIds)), target_agaid: targetId };
    }

    async function postJson(url, body) {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "same-origin",
        body: JSON.stringify(body),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.error || `Request failed: ${response.status}`);
      }
      return payload;
    }

    async function previewMerge() {
      setBusy(true, "Previewing...");
      clearExecution();
      try {
        const body = parseIds();
        const payload = await postJson(previewUrl, body);
        state.plan = payload.member_merge_plan;
        state.confirmationText = payload.confirmation_text || "";
        renderPreview();
        els.statusText.textContent = "Preview complete";
      } catch (error) {
        showError(error.message || String(error));
      } finally {
        setBusy(false);
      }
    }

    async function executeMerge() {
      if (!state.plan || els.confirmationInput.value.trim() !== state.confirmationText) return;
      setBusy(true, "Executing...");
      try {
        const body = {
          ...parseIds(),
          confirm_member_rating_merge: true,
          confirmation_text: els.confirmationInput.value.trim(),
          member_merge_plan_hash: state.plan.plan_hash,
        };
        const payload = await postJson(mergeUrl, body);
        state.plan = payload.member_merge_plan;
        renderPreview();
        els.executePanel.classList.add("hidden");
        els.statusText.textContent = "Merge executed";
        els.details.insertAdjacentHTML("afterbegin", `<div class="message ready">Production rows were updated.</div>`);
      } catch (error) {
        showError(error.message || String(error));
      } finally {
        setBusy(false);
      }
    }

    function renderPreview() {
      const plan = state.plan || {};
      els.previewPanel.classList.remove("hidden");
      els.readyText.textContent = plan.ready_to_execute ? "Ready" : "Blocked";
      els.metrics.innerHTML = [
        metric("Rating Rows", plan.rating_update_count || 0),
        metric("Player 1 Games", plan.game_player_1_update_count || 0),
        metric("Player 2 Games", plan.game_player_2_update_count || 0),
        metric("Updates", plan.production_write_count || 0),
      ].join("");

      const messages = [];
      messages.push(message("ready", `Source ${escapeHtml((plan.source_agaids || []).join(", "))} will merge into target ${escapeHtml(plan.target_agaid)}.`));
      messages.push(message("", `Source rating row IDs: ${escapeHtml(formatRange(plan.source_rating_id_range))}`));
      messages.push(message("", `Source game IDs: ${escapeHtml(formatRange(plan.source_game_id_range))}`));
      (plan.latest_source_ratings || []).forEach((rating) => {
        messages.push(message("", `Latest source ${escapeHtml(rating.agaid)}: ${escapeHtml(formatRating(rating))}`));
      });
      messages.push(message("", `Latest target: ${escapeHtml(formatRating(plan.latest_target_rating))}`));
      (plan.rating_conflicts || []).forEach((conflict) => {
        messages.push(message("danger", `Merge blocker ${escapeHtml(conflict.tournament_code || "<blank>")}: rating rows would combine AGAIDs ${escapeHtml((conflict.agaids || []).join(", "))}. ${escapeHtml(formatRatingRows(conflict.rows || []))}`));
      });
      (plan.preexisting_rating_duplicates || []).forEach((duplicate) => {
        messages.push(message("warn", `Pre-existing duplicate for AGAID ${escapeHtml(duplicate.pin_player)} at ${escapeHtml(duplicate.tournament_code || "<blank>")}: ${escapeHtml(formatRatingRows(duplicate.rows || []))}`));
      });
      (plan.self_play_game_rows || []).forEach((game) => {
        messages.push(message("danger", `Self-play blocker Game_ID ${escapeHtml(game.game_id)}: ${escapeHtml(game.pin_player_1)}-${escapeHtml(game.pin_player_2)}`));
      });
      els.details.innerHTML = messages.join("");

      if (plan.ready_to_execute) {
        els.executePanel.classList.remove("hidden");
        els.confirmationMessage.textContent = `Type ${state.confirmationText} to execute.`;
        els.confirmationInput.value = "";
        updateExecuteButton();
      } else {
        els.executePanel.classList.add("hidden");
      }
    }

    function clearExecution() {
      state.plan = null;
      state.confirmationText = "";
      els.previewPanel.classList.add("hidden");
      els.executePanel.classList.add("hidden");
      els.details.innerHTML = "";
      els.metrics.innerHTML = "";
    }

    function updateExecuteButton() {
      els.executeBtn.disabled = els.confirmationInput.value.trim() !== state.confirmationText || !state.plan;
    }

    function setBusy(busy, text) {
      els.previewBtn.disabled = busy;
      els.executeBtn.disabled = busy || els.confirmationInput.value.trim() !== state.confirmationText || !state.plan;
      if (text) els.statusText.textContent = text;
    }

    function showError(text) {
      els.previewPanel.classList.remove("hidden");
      els.readyText.textContent = "Error";
      els.metrics.innerHTML = "";
      els.details.innerHTML = message("danger", escapeHtml(text));
      els.executePanel.classList.add("hidden");
      els.statusText.textContent = "";
    }

    function metric(label, value) {
      return `<div class="metric"><div class="metric-label">${escapeHtml(label)}</div><div class="metric-value">${escapeHtml(value)}</div></div>`;
    }

    function message(kind, text) {
      return `<div class="message ${kind}">${text}</div>`;
    }

    function formatRange(range) {
      if (!range || range.first === null || range.first === undefined) return "none";
      return range.first === range.last ? String(range.first) : `${range.first}..${range.last}`;
    }

    function formatRating(rating) {
      if (!rating || rating.rating_row_id === null || rating.rating_row_id === undefined) return "no rating rows";
      return `${rating.rating} +/- ${rating.sigma} on ${rating.elab_date} (${rating.tournament_code}, row ${rating.rating_row_id})`;
    }

    function formatRatingRows(rows) {
      if (!rows.length) return "No rows listed.";
      return rows
        .map((row) => `row ${row.rating_row_id} AGAID ${row.pin_player}`)
        .join("; ");
    }

    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
    }

    els.previewBtn.addEventListener("click", previewMerge);
    els.executeBtn.addEventListener("click", executeMerge);
    els.confirmationInput.addEventListener("input", updateExecuteButton);
    [els.sourceInput, els.targetInput].forEach((input) => {
      input.addEventListener("input", () => {
        clearExecution();
        els.statusText.textContent = "";
      });
      input.addEventListener("keydown", (event) => {
        if (event.key !== "Enter") return;
        event.preventDefault();
        previewMerge();
      });
    });
  </script>
</body>
</html>"""


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


REWARDS_PUBLIC_BALANCES_SQL = """
SELECT
    balance.[Chapter_Code],
    balance.[Chapter_Name],
    balance.[As_Of_Date],
    balance.[Latest_Snapshot_Date],
    balance.[Is_Current],
    balance.[Active_Member_Count],
    balance.[Multiplier],
    balance.[Available_Points],
    balance.[Total_Remaining_Points],
    balance.[Expired_Unallocated_Points],
    balance.[Original_Points],
    balance.[Consumed_Points],
    balance.[Ledger_Balance],
    balance.[Balance_Reconciliation_Delta],
    balance.[Total_Credits],
    balance.[Total_Debits],
    COALESCE(recent.[Earned_30_Days], 0) AS [Earned_30_Days],
    COALESCE(recent.[Used_30_Days], 0) AS [Used_30_Days],
    balance.[Lot_Count],
    balance.[Transaction_Count],
    balance.[Expiring_30_Days],
    balance.[Expiring_60_Days],
    balance.[Expiring_90_Days],
    balance.[Next_Expiration_Date],
    balance.[Last_Transaction_Posted_At]
FROM [rewards].[v_chapter_balances] AS balance
LEFT JOIN [membership].[chapters] AS chapter
    ON chapter.[ChapterID] = balance.[ChapterID]
OUTER APPLY
(
    SELECT
        SUM(CASE WHEN tx.[Points_Delta] > 0 THEN tx.[Points_Delta] ELSE 0 END) AS [Earned_30_Days],
        SUM(CASE WHEN tx.[Points_Delta] < 0 THEN -tx.[Points_Delta] ELSE 0 END) AS [Used_30_Days]
    FROM [rewards].[transactions] AS tx
    WHERE tx.[ChapterID] = balance.[ChapterID]
      AND tx.[Posted_At] >= DATEADD(day, -30, SYSUTCDATETIME())
) AS recent
WHERE (
       COALESCE(balance.[Available_Points], 0) <> 0
    OR COALESCE(balance.[Ledger_Balance], 0) <> 0
    OR COALESCE(balance.[Total_Credits], 0) <> 0
    OR COALESCE(balance.[Total_Debits], 0) <> 0
)
  AND COALESCE(NULLIF(UPPER(LTRIM(RTRIM(chapter.[Status]))), N''), N'ACTIVE') <> N'DROPPED'
ORDER BY balance.[Available_Points] DESC, balance.[Chapter_Code]
"""


REWARDS_PUBLIC_CHAPTER_BALANCE_SQL = """
SELECT TOP 1
    balance.[Chapter_Code],
    balance.[Chapter_Name],
    balance.[As_Of_Date],
    balance.[Latest_Snapshot_Date],
    balance.[Is_Current],
    balance.[Active_Member_Count],
    balance.[Multiplier],
    balance.[Available_Points],
    balance.[Total_Remaining_Points],
    balance.[Expired_Unallocated_Points],
    balance.[Original_Points],
    balance.[Consumed_Points],
    balance.[Ledger_Balance],
    balance.[Balance_Reconciliation_Delta],
    balance.[Total_Credits],
    balance.[Total_Debits],
    COALESCE(recent.[Earned_30_Days], 0) AS [Earned_30_Days],
    COALESCE(recent.[Used_30_Days], 0) AS [Used_30_Days],
    balance.[Lot_Count],
    balance.[Transaction_Count],
    balance.[Expiring_30_Days],
    balance.[Expiring_60_Days],
    balance.[Expiring_90_Days],
    balance.[Next_Expiration_Date],
    balance.[Last_Transaction_Posted_At]
FROM [rewards].[v_chapter_balances] AS balance
LEFT JOIN [membership].[chapters] AS chapter
    ON chapter.[ChapterID] = balance.[ChapterID]
OUTER APPLY
(
    SELECT
        SUM(CASE WHEN tx.[Points_Delta] > 0 THEN tx.[Points_Delta] ELSE 0 END) AS [Earned_30_Days],
        SUM(CASE WHEN tx.[Points_Delta] < 0 THEN -tx.[Points_Delta] ELSE 0 END) AS [Used_30_Days]
    FROM [rewards].[transactions] AS tx
    WHERE tx.[ChapterID] = balance.[ChapterID]
      AND tx.[Posted_At] >= DATEADD(day, -30, SYSUTCDATETIME())
) AS recent
WHERE UPPER(balance.[Chapter_Code]) = UPPER(?)
  AND COALESCE(NULLIF(UPPER(LTRIM(RTRIM(chapter.[Status]))), N''), N'ACTIVE') <> N'DROPPED'
"""


REWARDS_PUBLIC_TRANSACTIONS_SQL = """
WITH [chapter_tx] AS
(
    SELECT
        [TransactionID],
        [Transaction_Type],
        [Points_Delta],
        [Base_Points],
        [Multiplier],
        [Effective_Date],
        [Earned_Date],
        [Posted_At],
        [RunID],
        [Run_Type],
        [Run_Snapshot_Date],
        [Run_Status],
        [Run_Processor],
        [Source_Type],
        [Rule_Version],
        [LotID],
        [Lot_Remaining_Points],
        [Lot_Expires_On],
        [Allocated_From_Lots],
        [Allocated_Lot_Count],
        [MetadataJson]
    FROM [rewards].[v_chapter_transaction_history]
    WHERE UPPER([Chapter_Code]) = UPPER(?)
),
[public_tx] AS
(
    SELECT
        tx.*,
        CASE
            WHEN tx.[Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN N'membership'
            WHEN tx.[Source_Type] = N'rated_game_participation' THEN N'rated_games'
            WHEN tx.[Source_Type] = N'tournament_host' THEN N'tournament_host'
            WHEN tx.[Source_Type] = N'state_championship' THEN N'state_championship'
            WHEN tx.[Source_Type] = N'redemption' THEN N'redemption'
            WHEN tx.[Source_Type] = N'opening_balance' THEN N'opening_balance'
            WHEN tx.[Source_Type] = N'point_expiration' THEN N'expiration'
            WHEN tx.[Source_Type] = N'legacy_dues_credit_adjustment' THEN N'adjustment'
            ELSE N'other'
        END AS [Source_Category],
        CASE
            WHEN tx.[Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN N'Membership awards'
            WHEN tx.[Source_Type] = N'rated_game_participation' THEN N'Rated games'
            WHEN tx.[Source_Type] = N'tournament_host' THEN N'Tournament host'
            WHEN tx.[Source_Type] = N'state_championship' THEN N'State Championship'
            WHEN tx.[Source_Type] = N'redemption' THEN N'Redemption'
            WHEN tx.[Source_Type] = N'opening_balance' THEN N'Opening balance'
            WHEN tx.[Source_Type] = N'point_expiration' THEN N'Expiration'
            WHEN tx.[Source_Type] = N'legacy_dues_credit_adjustment' THEN N'Dues credit adjustment'
            ELSE N'Other'
        END AS [Source_Label],
        CASE
        WHEN tx.[Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN
            CONCAT(
                CASE JSON_VALUE(tx.[MetadataJson], '$.event_type')
                    WHEN N'new_membership' THEN N'New membership'
                    WHEN N'renewal' THEN N'Renewal'
                    WHEN N'lifetime' THEN N'Lifetime membership'
                    ELSE N'Membership award'
                END,
                CASE
                    WHEN NULLIF(JSON_VALUE(tx.[MetadataJson], '$.member_count'), N'') IS NOT NULL
                        THEN CONCAT(N' x ', JSON_VALUE(tx.[MetadataJson], '$.member_count'))
                    ELSE N''
                END,
                CASE
                    WHEN COALESCE(NULLIF(JSON_VALUE(tx.[MetadataJson], '$.member_type'), N''), NULLIF(JSON_VALUE(tx.[MetadataJson], '$.event_member_type'), N'')) IS NOT NULL
                        THEN CONCAT(N' - ', COALESCE(JSON_VALUE(tx.[MetadataJson], '$.member_type'), JSON_VALUE(tx.[MetadataJson], '$.event_member_type')))
                    ELSE N''
                END
            )
        WHEN tx.[Source_Type] = N'rated_game_participation' THEN
            CONCAT(
                N'Rated game participation',
                CASE
                    WHEN NULLIF(JSON_VALUE(tx.[MetadataJson], '$.tournament_code'), N'') IS NOT NULL
                        THEN CONCAT(N' - ', JSON_VALUE(tx.[MetadataJson], '$.tournament_code'))
                    ELSE N''
                END
            )
        WHEN tx.[Source_Type] = N'tournament_host' THEN
            CONCAT(
                COALESCE(NULLIF(JSON_VALUE(tx.[MetadataJson], '$.reward_event_name'), N''), N'Tournament host award'),
                CASE
                    WHEN NULLIF(JSON_VALUE(tx.[MetadataJson], '$.rated_game_count'), N'') IS NOT NULL
                        THEN CONCAT(N' - ', JSON_VALUE(tx.[MetadataJson], '$.rated_game_count'), N' rated games')
                    ELSE N''
                END
            )
        WHEN tx.[Source_Type] = N'state_championship' THEN
            CONCAT(COALESCE(NULLIF(JSON_VALUE(tx.[MetadataJson], '$.reward_event_name'), N''), N'State Championship'), N' - State Championship award')
        WHEN tx.[Source_Type] = N'redemption' THEN
            CONCAT(
                N'Redemption - ',
                REPLACE(COALESCE(NULLIF(JSON_VALUE(tx.[MetadataJson], '$.redemption_category'), N''), N'other'), N'_', N' '),
                N', ',
                REPLACE(COALESCE(NULLIF(JSON_VALUE(tx.[MetadataJson], '$.payment_mode'), N''), N'other'), N'_', N' '),
                CASE
                    WHEN NULLIF(JSON_VALUE(tx.[MetadataJson], '$.description'), N'') IS NOT NULL
                        THEN CONCAT(N' - ', JSON_VALUE(tx.[MetadataJson], '$.description'))
                    ELSE N''
                END
            )
        WHEN tx.[Source_Type] = N'opening_balance' THEN N'Opening balance'
        WHEN tx.[Source_Type] = N'point_expiration' THEN N'Expired unused points'
        WHEN tx.[Source_Type] = N'legacy_dues_credit_adjustment' THEN N'Dues credit adjustment'
        ELSE REPLACE(tx.[Source_Type], N'_', N' ')
        END AS [Public_Detail]
    FROM [chapter_tx] AS tx
),
[public_entries] AS
(
    SELECT
        COUNT(*) AS [Public_Entry_Count],
        MAX([TransactionID]) AS [Sort_TransactionID],
        [Transaction_Type],
        SUM([Points_Delta]) AS [Points_Delta],
        CASE WHEN MIN([Base_Points]) = MAX([Base_Points]) THEN MAX([Base_Points]) ELSE NULL END AS [Base_Points],
        CASE WHEN MIN([Multiplier]) = MAX([Multiplier]) THEN MAX([Multiplier]) ELSE NULL END AS [Multiplier],
        [Effective_Date],
        [Earned_Date],
        MAX([Posted_At]) AS [Posted_At],
        [RunID],
        [Run_Type],
        [Run_Snapshot_Date],
        [Run_Status],
        [Run_Processor],
        [Rule_Version],
        SUM([Allocated_From_Lots]) AS [Allocated_From_Lots],
        SUM([Allocated_Lot_Count]) AS [Allocated_Lot_Count],
        [Source_Category],
        [Source_Label],
        [Public_Detail]
    FROM [public_tx]
    GROUP BY
        [Transaction_Type],
        [Effective_Date],
        [Earned_Date],
        [RunID],
        [Run_Type],
        [Run_Snapshot_Date],
        [Run_Status],
        [Run_Processor],
        [Rule_Version],
        [Source_Category],
        [Source_Label],
        [Public_Detail]
),
[scored] AS
(
    SELECT
        entries.*,
        SUM(entries.[Points_Delta]) OVER
        (
            ORDER BY entries.[Effective_Date], entries.[Posted_At], entries.[Sort_TransactionID]
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS [Running_Balance]
    FROM [public_entries] AS entries
),
[selected] AS
(
    SELECT TOP (?)
        [Public_Entry_Count],
        [Sort_TransactionID],
        [Transaction_Type],
        [Points_Delta],
        [Base_Points],
        [Multiplier],
        [Effective_Date],
        [Earned_Date],
        [Posted_At],
        [RunID],
        [Run_Type],
        [Run_Snapshot_Date],
        [Run_Status],
        [Run_Processor],
        [Rule_Version],
        [Allocated_From_Lots],
        [Allocated_Lot_Count],
        [Running_Balance],
        [Source_Category],
        [Source_Label],
        [Public_Detail]
    FROM [scored]
    ORDER BY [Effective_Date] DESC, [Posted_At] DESC, [Sort_TransactionID] DESC
)
SELECT
    [Public_Entry_Count],
    [Transaction_Type],
    [Points_Delta],
    [Base_Points],
    [Multiplier],
    [Effective_Date],
    [Earned_Date],
    [Posted_At],
    [RunID],
    [Run_Type],
    [Run_Snapshot_Date],
    [Run_Status],
    [Run_Processor],
    [Rule_Version],
    [Allocated_From_Lots],
    [Allocated_Lot_Count],
    [Running_Balance],
    [Source_Category],
    [Source_Label],
    [Public_Detail]
FROM [selected]
ORDER BY [Effective_Date] DESC, [Posted_At] DESC, [Sort_TransactionID] DESC
"""


REWARDS_PUBLIC_LOTS_SQL = """
WITH [public_lots] AS
(
    SELECT
        [Original_Points],
        [Remaining_Points],
        [Allocated_Points],
        [Allocation_Count],
        [Earned_Date],
        [Expires_On],
        [Days_Until_Expiration],
        [Aging_Status],
        [RunID],
        CASE
            WHEN [Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN N'membership'
            WHEN [Source_Type] = N'rated_game_participation' THEN N'rated_games'
            WHEN [Source_Type] = N'tournament_host' THEN N'tournament_host'
            WHEN [Source_Type] = N'state_championship' THEN N'state_championship'
            WHEN [Source_Type] = N'opening_balance' THEN N'opening_balance'
            WHEN [Source_Type] = N'legacy_dues_credit_adjustment' THEN N'adjustment'
            ELSE N'other'
        END AS [Source_Category],
        CASE
            WHEN [Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN N'Membership awards'
            WHEN [Source_Type] = N'rated_game_participation' THEN N'Rated games'
            WHEN [Source_Type] = N'tournament_host' THEN N'Tournament host'
            WHEN [Source_Type] = N'state_championship' THEN N'State Championship'
            WHEN [Source_Type] = N'opening_balance' THEN N'Opening balance'
            WHEN [Source_Type] = N'legacy_dues_credit_adjustment' THEN N'Dues credit adjustment'
            ELSE N'Other'
        END AS [Source_Label]
    FROM [rewards].[v_point_lot_aging]
    WHERE UPPER([Chapter_Code]) = UPPER(?)
)
SELECT TOP (?)
    COUNT(*) AS [Lot_Count],
    SUM([Original_Points]) AS [Original_Points],
    SUM([Remaining_Points]) AS [Remaining_Points],
    SUM([Allocated_Points]) AS [Allocated_Points],
    SUM([Allocation_Count]) AS [Allocation_Count],
    [Earned_Date],
    [Expires_On],
    MIN([Days_Until_Expiration]) AS [Days_Until_Expiration],
    [Aging_Status],
    [RunID],
    [Source_Category],
    [Source_Label]
FROM [public_lots]
GROUP BY
    [Earned_Date],
    [Expires_On],
    [Aging_Status],
    [RunID],
    [Source_Category],
    [Source_Label]
ORDER BY
    CASE WHEN SUM([Remaining_Points]) > 0 THEN 0 ELSE 1 END,
    [Expires_On],
    [Earned_Date],
    [Source_Label]
"""


REWARDS_PUBLIC_BREAKDOWN_SQL = """
WITH [public_tx] AS
(
    SELECT
        CASE
            WHEN [Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN N'membership'
            WHEN [Source_Type] = N'rated_game_participation' THEN N'rated_games'
            WHEN [Source_Type] = N'tournament_host' THEN N'tournament_host'
            WHEN [Source_Type] = N'state_championship' THEN N'state_championship'
            WHEN [Source_Type] = N'redemption' THEN N'redemption'
            WHEN [Source_Type] = N'opening_balance' THEN N'opening_balance'
            WHEN [Source_Type] = N'point_expiration' THEN N'expiration'
            WHEN [Source_Type] = N'legacy_dues_credit_adjustment' THEN N'adjustment'
            ELSE N'other'
        END AS [Source_Category],
        CASE
            WHEN [Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN N'Membership awards'
            WHEN [Source_Type] = N'rated_game_participation' THEN N'Rated games'
            WHEN [Source_Type] = N'tournament_host' THEN N'Tournament host'
            WHEN [Source_Type] = N'state_championship' THEN N'State Championship'
            WHEN [Source_Type] = N'redemption' THEN N'Redemption'
            WHEN [Source_Type] = N'opening_balance' THEN N'Opening balance'
            WHEN [Source_Type] = N'point_expiration' THEN N'Expiration'
            WHEN [Source_Type] = N'legacy_dues_credit_adjustment' THEN N'Dues credit adjustment'
            ELSE N'Other'
        END AS [Source_Label],
        [Points_Delta]
    FROM [rewards].[v_chapter_transaction_history]
    WHERE UPPER([Chapter_Code]) = UPPER(?)
)
SELECT
    [Source_Category],
    [Source_Label],
    COUNT(*) AS [Transaction_Count],
    SUM(CASE WHEN [Points_Delta] > 0 THEN [Points_Delta] ELSE 0 END) AS [Credit_Points],
    SUM(CASE WHEN [Points_Delta] < 0 THEN -[Points_Delta] ELSE 0 END) AS [Debit_Points],
    SUM([Points_Delta]) AS [Net_Points]
FROM [public_tx]
GROUP BY [Source_Category], [Source_Label]
ORDER BY [Net_Points] DESC, [Source_Label]
"""


REWARDS_PUBLIC_REDEMPTIONS_SQL = """
SELECT TOP (?)
    request.[RedemptionID],
    request.[Posted_TransactionID],
    request.[Request_Date],
    request.[Points],
    request.[Amount_USD],
    request.[Redemption_Category],
    request.[Payment_Mode],
    request.[Description],
    request.[Receipt_Reference],
    request.[Status],
    request.[Posted_At],
    COALESCE(receipts.[Active_Receipt_Count], 0) AS [Active_Receipt_Count]
FROM [rewards].[redemption_requests] AS request
OUTER APPLY
(
    SELECT COUNT(*) AS [Active_Receipt_Count]
    FROM [rewards].[redemption_receipts] AS receipt
    WHERE receipt.[RedemptionID] = request.[RedemptionID]
      AND receipt.[Is_Deleted] = 0
) AS receipts
WHERE UPPER(request.[Chapter_Code]) = UPPER(?)
  AND request.[Status] = N'posted'
ORDER BY request.[Request_Date] DESC, request.[Posted_At] DESC
"""


REWARDS_ADMIN_CHAPTER_OPTIONS_SQL = """
SELECT
    balance.[ChapterID],
    balance.[Chapter_Code],
    balance.[Chapter_Name],
    balance.[Available_Points],
    balance.[Latest_Snapshot_Date],
    balance.[Active_Member_Count],
    balance.[Multiplier]
FROM [rewards].[v_chapter_balances] AS balance
LEFT JOIN [membership].[chapters] AS chapter
    ON chapter.[ChapterID] = balance.[ChapterID]
WHERE balance.[Chapter_Code] IS NOT NULL
  AND COALESCE(NULLIF(UPPER(LTRIM(RTRIM(chapter.[Status]))), N''), N'ACTIVE') <> N'DROPPED'
ORDER BY balance.[Chapter_Code]
"""


CHAPTER_DIRECTORY_SNAPSHOT_SQL = """
SELECT
    MAX(CASE WHEN [Snapshot_Date] <= ? THEN [Snapshot_Date] END) AS [Snapshot_Date],
    MIN([Snapshot_Date]) AS [Min_Snapshot_Date],
    MAX([Snapshot_Date]) AS [Max_Snapshot_Date],
    COUNT(DISTINCT [Snapshot_Date]) AS [Snapshot_Date_Count]
FROM [rewards].[chapter_daily_snapshot]
"""


CHAPTER_DIRECTORY_REPORT_SQL = """
SELECT
    snapshot.[Snapshot_Date],
    snapshot.[ChapterID],
    COALESCE(NULLIF(LTRIM(RTRIM(snapshot.[Chapter_Name])), N''), chapter.[ChapterName]) AS [Chapter_Name],
    COALESCE(NULLIF(LTRIM(RTRIM(snapshot.[Chapter_Code])), N''), chapter.[ChapterCode]) AS [Chapter_Code],
    snapshot.[Active_Member_Count],
    CASE
        WHEN snapshot.[Active_Member_Count] BETWEEN 2 AND 4 THEN 1
        WHEN snapshot.[Active_Member_Count] BETWEEN 5 AND 9 THEN 2
        ELSE CAST(2 + FLOOR(CAST(snapshot.[Active_Member_Count] AS decimal(10, 2)) / 10) AS int)
    END AS [Votes],
    chapter.[Status] AS [Chapter_Status],
    chapter.[ChapterRepID] AS [Leader_AGAID],
    CASE
        WHEN leader.[AGAID] IS NULL THEN NULL
        WHEN NULLIF(LTRIM(RTRIM(leader.[FirstName])), N'') IS NOT NULL
         AND NULLIF(LTRIM(RTRIM(leader.[LastName])), N'') IS NOT NULL
            THEN LTRIM(RTRIM(leader.[FirstName])) + N' ' + LTRIM(RTRIM(leader.[LastName]))
        ELSE COALESCE(NULLIF(LTRIM(RTRIM(leader.[FirstName])), N''), NULLIF(LTRIM(RTRIM(leader.[LastName])), N''))
    END AS [Leader_Name],
    leader.[EmailAddress] AS [Leader_Email],
    leader.[Status] AS [Leader_Status]
FROM [rewards].[chapter_daily_snapshot] AS snapshot
LEFT JOIN [membership].[chapters] AS chapter
    ON chapter.[ChapterID] = snapshot.[ChapterID]
LEFT JOIN [membership].[members] AS leader
    ON leader.[AGAID] = chapter.[ChapterRepID]
WHERE snapshot.[Snapshot_Date] = ?
  AND snapshot.[Active_Member_Count] >= 2
  AND COALESCE(NULLIF(UPPER(LTRIM(RTRIM(chapter.[Status]))), N''), N'ACTIVE') <> N'DROPPED'
ORDER BY snapshot.[ChapterID]
"""


REWARDS_MANUAL_DEBIT_SQL = """
EXEC [rewards].[sp_post_manual_redemption]
    @ChapterID = ?,
    @ChapterCode = ?,
    @RequestDate = ?,
    @Points = ?,
    @RedemptionCategory = ?,
    @PaymentMode = ?,
    @Description = ?,
    @ReceiptReference = ?,
    @ExternalRequestID = ?,
    @Notes = ?,
    @SourcePayloadJson = ?,
    @DryRun = ?,
    @RunType = N'manual',
    @PostedByPrincipalName = ?,
    @PostedByPrincipalId = ?
"""


REWARDS_MANUAL_DEBIT_LOOKUP_SQL = """
SELECT TOP 1
    request.[RedemptionID],
    request.[External_Request_ID],
    request.[ChapterID],
    request.[Chapter_Code],
    request.[Chapter_Name],
    request.[Request_Date],
    request.[Points],
    request.[Amount_USD],
    request.[Redemption_Category],
    request.[Payment_Mode],
    request.[Description],
    request.[Receipt_Reference],
    request.[Status],
    request.[Posted_TransactionID],
    request.[Posted_At],
    request.[Posted_By_Principal_Name],
    balance.[Available_Points],
    balance.[Latest_Snapshot_Date]
FROM [rewards].[redemption_requests] AS request
LEFT JOIN [rewards].[v_chapter_balances] AS balance
    ON balance.[ChapterID] = request.[ChapterID]
WHERE request.[External_Request_ID] = ?
ORDER BY request.[RedemptionID] DESC
"""


REWARDS_REDEMPTION_CATEGORIES = {"chapter_renewal", "go_promotion", "other"}
REWARDS_PAYMENT_MODES = {"dues_credit", "reimbursement", "other"}
REWARDS_MAX_MANUAL_DEBIT_POINTS = 100_000_000
REWARDS_RECEIPT_CONTAINER = (os.environ.get("REWARDS_RECEIPT_CONTAINER") or "").strip() or "chapter-rewards-receipts"
REWARDS_MAX_RECEIPT_BYTES = int(os.environ.get("REWARDS_MAX_RECEIPT_BYTES") or str(8 * 1024 * 1024))
REWARDS_MAX_RECEIPT_FILES = int(os.environ.get("REWARDS_MAX_RECEIPT_FILES") or "5")
REWARDS_ALLOWED_RECEIPT_CONTENT_TYPES = {
    "image/jpeg",
    "image/jpg",
    "image/png",
    "image/gif",
    "image/webp",
    "application/pdf",
}


REWARDS_RECEIPTS_SQL = """
SELECT
    receipt.[ReceiptID],
    receipt.[RedemptionID],
    receipt.[Posted_TransactionID],
    receipt.[ChapterID],
    receipt.[Chapter_Code],
    receipt.[Blob_Container],
    receipt.[Blob_Name],
    receipt.[Original_File_Name],
    receipt.[Content_Type],
    receipt.[Content_Length],
    receipt.[Uploaded_At],
    receipt.[Uploaded_By_Principal_Name],
    receipt.[Is_Deleted]
FROM [rewards].[redemption_receipts] AS receipt
WHERE receipt.[RedemptionID] = ?
  AND receipt.[Is_Deleted] = 0
ORDER BY receipt.[Uploaded_At] DESC, receipt.[ReceiptID] DESC
"""


REWARDS_RECEIPT_LOOKUP_SQL = """
SELECT TOP 1
    receipt.[ReceiptID],
    receipt.[RedemptionID],
    receipt.[Posted_TransactionID],
    receipt.[ChapterID],
    receipt.[Chapter_Code],
    receipt.[Blob_Container],
    receipt.[Blob_Name],
    receipt.[Original_File_Name],
    receipt.[Content_Type],
    receipt.[Content_Length],
    receipt.[Uploaded_At],
    receipt.[Uploaded_By_Principal_Name],
    receipt.[Is_Deleted]
FROM [rewards].[redemption_receipts] AS receipt
WHERE receipt.[ReceiptID] = ?
  AND receipt.[Is_Deleted] = 0
"""


REWARDS_ADMIN_REDEMPTION_SQL = """
SELECT TOP 1
    request.[RedemptionID],
    request.[External_Request_ID],
    request.[ChapterID],
    request.[Chapter_Code],
    request.[Chapter_Name],
    request.[Request_Date],
    request.[Points],
    request.[Amount_USD],
    request.[Redemption_Category],
    request.[Payment_Mode],
    request.[Description],
    request.[Receipt_Reference],
    request.[Status],
    request.[Posted_TransactionID],
    request.[Posted_At],
    request.[Posted_By_Principal_Name],
    request.[Notes],
    request.[Notes_Updated_At],
    request.[Notes_Updated_By_Principal_Name]
FROM [rewards].[redemption_requests] AS request
WHERE request.[RedemptionID] = ?
"""


REWARDS_ADD_RECEIPT_SQL = """
EXEC [rewards].[sp_add_redemption_receipt]
    @RedemptionID = ?,
    @BlobContainer = ?,
    @BlobName = ?,
    @OriginalFileName = ?,
    @ContentType = ?,
    @ContentLength = ?,
    @Sha256Hex = ?,
    @UploadedByPrincipalName = ?,
    @UploadedByPrincipalId = ?
"""


REWARDS_DELETE_RECEIPT_SQL = """
EXEC [rewards].[sp_delete_redemption_receipt]
    @ReceiptID = ?,
    @DeletedByPrincipalName = ?,
    @DeletedByPrincipalId = ?
"""


REWARDS_UPDATE_NOTES_SQL = """
EXEC [rewards].[sp_update_redemption_notes]
    @RedemptionID = ?,
    @Notes = ?,
    @UpdatedByPrincipalName = ?,
    @UpdatedByPrincipalId = ?
"""


def _rewards_int(value) -> int:
    """Execute the rewards int routine."""
    if value is None:
        return 0
    return int(value)


def _rewards_optional_int(value) -> int | None:
    """Execute the rewards optional int routine."""
    if value is None:
        return None
    return int(value)


def _rewards_text(value) -> str | None:
    """Execute the rewards text routine."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_chapter_directory_as_of_date(req: func.HttpRequest) -> tuple[date | None, func.HttpResponse | None]:
    """Parse chapter directory as of date."""
    raw_text = (
        req.params.get("as_of")
        or req.params.get("asOf")
        or req.params.get("date")
        or req.params.get("snapshot_date")
        or ""
    ).strip()
    if not raw_text:
        return date.today(), None
    for parse_func in (
        lambda value: date.fromisoformat(value),
        lambda value: datetime.strptime(value, "%m/%d/%Y").date(),
        lambda value: datetime.strptime(value, "%m-%d-%Y").date(),
    ):
        try:
            return parse_func(raw_text), None
        except ValueError:
            continue
    return None, func.HttpResponse("Query parameter 'as_of' must be a date like YYYY-MM-DD or M/D/YYYY.", status_code=400)


def _chapter_directory_row_payload(row: dict) -> dict:
    """Execute the chapter directory row payload routine."""
    return {
        "snapshot_date": explorer.json_safe_value(row.get("Snapshot_Date")),
        "chapter_id": _rewards_optional_int(row.get("ChapterID")),
        "chapter_name": _rewards_text(row.get("Chapter_Name")),
        "chapter_code": _rewards_text(row.get("Chapter_Code")),
        "member_count": _rewards_int(row.get("Active_Member_Count")),
        "votes": _rewards_int(row.get("Votes")),
        "chapter_status": _rewards_text(row.get("Chapter_Status")),
        "leader_agaid": _rewards_optional_int(row.get("Leader_AGAID")),
        "leader_name": _rewards_text(row.get("Leader_Name")),
        "leader_email": _rewards_text(row.get("Leader_Email")),
        "leader_status": _rewards_text(row.get("Leader_Status")),
    }


def _chapter_directory_payload(
    *,
    requested_as_of_date: date,
    snapshot_metadata: dict,
    rows: list[dict],
    authorization: dict | None = None,
) -> dict:
    """Execute the chapter directory payload routine."""
    chapters = [_chapter_directory_row_payload(row) for row in rows]
    summary = {
        "chapter_count": len(chapters),
        "total_member_count": sum(chapter.get("member_count") or 0 for chapter in chapters),
        "total_votes": sum(chapter.get("votes") or 0 for chapter in chapters),
        "missing_leader_count": sum(1 for chapter in chapters if not chapter.get("leader_agaid")),
        "missing_leader_email_count": sum(1 for chapter in chapters if chapter.get("leader_agaid") and not chapter.get("leader_email")),
    }
    return {
        "ok": True,
        "generated_at": _utc_now_text(),
        "requested_as_of_date": explorer.json_safe_value(requested_as_of_date),
        "snapshot_date": explorer.json_safe_value(snapshot_metadata.get("Snapshot_Date")),
        "snapshot_range": {
            "min_snapshot_date": explorer.json_safe_value(snapshot_metadata.get("Min_Snapshot_Date")),
            "max_snapshot_date": explorer.json_safe_value(snapshot_metadata.get("Max_Snapshot_Date")),
            "snapshot_date_count": _rewards_int(snapshot_metadata.get("Snapshot_Date_Count")),
        },
        "sources": {
            "member_count": "rewards.chapter_daily_snapshot.Active_Member_Count for snapshot_date",
            "chapter_identity": "rewards.chapter_daily_snapshot for snapshot_date, falling back to current membership.chapters",
            "chapter_status": "current membership.chapters.Status",
            "leader": "current membership.chapters.ChapterRepID joined to current membership.members",
        },
        "summary": summary,
        "authorization": authorization,
        "chapters": chapters,
    }


def _chapter_directory_csv_response(payload: dict) -> func.HttpResponse:
    """Execute the chapter directory csv response routine."""
    columns = [
        ("chapter_id", "ChapterID"),
        ("chapter_name", "ChapterName"),
        ("chapter_code", "ChapterCode"),
        ("member_count", "MemberCount"),
        ("votes", "Votes"),
        ("chapter_status", "ChapterStatus"),
        ("leader_agaid", "LeaderAGAID"),
        ("leader_name", "LeaderName"),
        ("leader_email", "LeaderEmail"),
        ("leader_status", "LeaderStatus"),
        ("snapshot_date", "SnapshotDate"),
    ]
    output = StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([heading for _, heading in columns])
    for chapter in payload.get("chapters") or []:
        writer.writerow([chapter.get(key) or "" for key, _ in columns])
    snapshot_date = str(payload.get("snapshot_date") or payload.get("requested_as_of_date") or "chapters")
    headers = explorer.response_headers("text/csv; charset=utf-8")
    headers["Content-Disposition"] = f'attachment; filename="chapter-directory-{snapshot_date}.csv"'
    return func.HttpResponse(output.getvalue(), status_code=200, headers=headers)


def _rewards_balance_payload(row: dict) -> dict:
    """Execute the rewards balance payload routine."""
    available_points = _rewards_int(row.get("Available_Points"))
    return {
        "chapter_code": _rewards_text(row.get("Chapter_Code")),
        "chapter_name": _rewards_text(row.get("Chapter_Name")),
        "as_of_date": explorer.json_safe_value(row.get("As_Of_Date")),
        "latest_snapshot_date": explorer.json_safe_value(row.get("Latest_Snapshot_Date")),
        "is_current": bool(row.get("Is_Current")) if row.get("Is_Current") is not None else None,
        "active_member_count": _rewards_int(row.get("Active_Member_Count")),
        "multiplier": _rewards_int(row.get("Multiplier")),
        "available_points": available_points,
        "available_value_usd": round(available_points / 1000, 3),
        "total_remaining_points": _rewards_int(row.get("Total_Remaining_Points")),
        "expired_unallocated_points": _rewards_int(row.get("Expired_Unallocated_Points")),
        "original_points": _rewards_int(row.get("Original_Points")),
        "consumed_points": _rewards_int(row.get("Consumed_Points")),
        "ledger_balance": _rewards_int(row.get("Ledger_Balance")),
        "balance_reconciliation_delta": _rewards_int(row.get("Balance_Reconciliation_Delta")),
        "total_credits": _rewards_int(row.get("Total_Credits")),
        "total_debits": _rewards_int(row.get("Total_Debits")),
        "earned_30_days": _rewards_int(row.get("Earned_30_Days")),
        "used_30_days": _rewards_int(row.get("Used_30_Days")),
        "lot_count": _rewards_int(row.get("Lot_Count")),
        "transaction_count": _rewards_int(row.get("Transaction_Count")),
        "expiring_30_days": _rewards_int(row.get("Expiring_30_Days")),
        "expiring_60_days": _rewards_int(row.get("Expiring_60_Days")),
        "expiring_90_days": _rewards_int(row.get("Expiring_90_Days")),
        "next_expiration_date": explorer.json_safe_value(row.get("Next_Expiration_Date")),
        "last_transaction_posted_at": explorer.json_safe_value(row.get("Last_Transaction_Posted_At")),
    }


def _rewards_transaction_payload(row: dict) -> dict:
    """Execute the rewards transaction payload routine."""
    points = _rewards_int(row.get("Points_Delta"))
    return {
        "entry_count": _rewards_int(row.get("Public_Entry_Count")) or 1,
        "transaction_type": _rewards_text(row.get("Transaction_Type")),
        "points_delta": points,
        "value_delta_usd": round(points / 1000, 3),
        "base_points": _rewards_optional_int(row.get("Base_Points")),
        "multiplier": _rewards_optional_int(row.get("Multiplier")),
        "effective_date": explorer.json_safe_value(row.get("Effective_Date")),
        "earned_date": explorer.json_safe_value(row.get("Earned_Date")),
        "posted_at": explorer.json_safe_value(row.get("Posted_At")),
        "run_id": _rewards_optional_int(row.get("RunID")),
        "run_type": _rewards_text(row.get("Run_Type")),
        "run_snapshot_date": explorer.json_safe_value(row.get("Run_Snapshot_Date")),
        "run_status": _rewards_text(row.get("Run_Status")),
        "run_processor": _rewards_text(row.get("Run_Processor")),
        "rule_version": _rewards_text(row.get("Rule_Version")),
        "allocated_from_lots": _rewards_int(row.get("Allocated_From_Lots")),
        "allocated_lot_count": _rewards_int(row.get("Allocated_Lot_Count")),
        "running_balance": _rewards_int(row.get("Running_Balance")),
        "source_category": _rewards_text(row.get("Source_Category")) or "other",
        "source_label": _rewards_text(row.get("Source_Label")) or "Other",
        "public_detail": _rewards_text(row.get("Public_Detail")) or "Rewards activity",
    }


def _rewards_lot_payload(row: dict) -> dict:
    """Execute the rewards lot payload routine."""
    return {
        "lot_count": _rewards_int(row.get("Lot_Count")) or 1,
        "original_points": _rewards_int(row.get("Original_Points")),
        "remaining_points": _rewards_int(row.get("Remaining_Points")),
        "allocated_points": _rewards_int(row.get("Allocated_Points")),
        "allocation_count": _rewards_int(row.get("Allocation_Count")),
        "earned_date": explorer.json_safe_value(row.get("Earned_Date")),
        "expires_on": explorer.json_safe_value(row.get("Expires_On")),
        "days_until_expiration": _rewards_optional_int(row.get("Days_Until_Expiration")),
        "aging_status": _rewards_text(row.get("Aging_Status")),
        "run_id": _rewards_optional_int(row.get("RunID")),
        "source_category": _rewards_text(row.get("Source_Category")) or "other",
        "source_label": _rewards_text(row.get("Source_Label")) or "Other",
    }


def _rewards_breakdown_payload(row: dict) -> dict:
    """Execute the rewards breakdown payload routine."""
    return {
        "source_category": _rewards_text(row.get("Source_Category")) or "other",
        "source_label": _rewards_text(row.get("Source_Label")) or "Other",
        "transaction_count": _rewards_int(row.get("Transaction_Count")),
        "credit_points": _rewards_int(row.get("Credit_Points")),
        "debit_points": _rewards_int(row.get("Debit_Points")),
        "net_points": _rewards_int(row.get("Net_Points")),
    }


def _rewards_redemption_payload(row: dict) -> dict:
    """Execute the rewards redemption payload routine."""
    points = _rewards_int(row.get("Points"))
    amount = row.get("Amount_USD")
    return {
        "redemption_id": _rewards_optional_int(row.get("RedemptionID")),
        "transaction_id": _rewards_optional_int(row.get("Posted_TransactionID")),
        "request_date": explorer.json_safe_value(row.get("Request_Date")),
        "points": points,
        "amount_usd": float(amount) if amount is not None else round(points / 1000, 3),
        "redemption_category": _rewards_text(row.get("Redemption_Category")),
        "payment_mode": _rewards_text(row.get("Payment_Mode")),
        "description": _rewards_text(row.get("Description")),
        "receipt_reference": _rewards_text(row.get("Receipt_Reference")),
        "active_receipt_count": _rewards_int(row.get("Active_Receipt_Count")),
        "status": _rewards_text(row.get("Status")),
        "posted_at": explorer.json_safe_value(row.get("Posted_At")),
    }


def _rewards_receipt_payload(row: dict) -> dict:
    """Execute the rewards receipt payload routine."""
    receipt_id = _rewards_optional_int(row.get("ReceiptID"))
    return {
        "receipt_id": receipt_id,
        "redemption_id": _rewards_optional_int(row.get("RedemptionID")),
        "transaction_id": _rewards_optional_int(row.get("Posted_TransactionID")),
        "chapter_id": _rewards_optional_int(row.get("ChapterID")),
        "chapter_code": _rewards_text(row.get("Chapter_Code")),
        "original_file_name": _rewards_text(row.get("Original_File_Name")),
        "content_type": _rewards_text(row.get("Content_Type")),
        "content_length": _rewards_int(row.get("Content_Length")),
        "uploaded_at": explorer.json_safe_value(row.get("Uploaded_At")),
        "uploaded_by_principal_name": _rewards_text(row.get("Uploaded_By_Principal_Name")),
        "is_deleted": bool(row.get("Is_Deleted")),
        "url": f"{api_route_root()}/rewards/admin/receipts/{receipt_id}" if receipt_id else None,
    }


def api_route_root() -> str:
    """Execute the api route root routine."""
    return "/api/ratings-explorer"


def _rewards_admin_redemption_payload(row: dict, receipts: list[dict] | None = None) -> dict:
    """Execute the rewards admin redemption payload routine."""
    payload = _rewards_redemption_payload(row)
    payload.update(
        {
            "external_request_id": _rewards_text(row.get("External_Request_ID")),
            "chapter_id": _rewards_optional_int(row.get("ChapterID")),
            "chapter_code": _rewards_text(row.get("Chapter_Code")),
            "chapter_name": _rewards_text(row.get("Chapter_Name")),
            "posted_by_principal_name": _rewards_text(row.get("Posted_By_Principal_Name")),
            "notes": _rewards_text(row.get("Notes")),
            "notes_updated_at": explorer.json_safe_value(row.get("Notes_Updated_At")),
            "notes_updated_by_principal_name": _rewards_text(row.get("Notes_Updated_By_Principal_Name")),
            "receipts": receipts or [],
        }
    )
    return payload


def _rewards_admin_chapter_payload(row: dict) -> dict:
    """Execute the rewards admin chapter payload routine."""
    return {
        "chapter_id": _rewards_optional_int(row.get("ChapterID")),
        "chapter_code": _rewards_text(row.get("Chapter_Code")),
        "chapter_name": _rewards_text(row.get("Chapter_Name")),
        "available_points": _rewards_int(row.get("Available_Points")),
        "latest_snapshot_date": explorer.json_safe_value(row.get("Latest_Snapshot_Date")),
        "active_member_count": _rewards_int(row.get("Active_Member_Count")),
        "multiplier": _rewards_int(row.get("Multiplier")),
    }


def _rewards_manual_debit_payload(row: dict) -> dict:
    """Execute the rewards manual debit payload routine."""
    points = _rewards_int(row.get("Points"))
    available = _rewards_int(row.get("Available_Points"))
    amount = row.get("Amount_USD")
    return {
        "run_id": _rewards_optional_int(row.get("RunID")),
        "dry_run": bool(row.get("DryRun")),
        "redemption_id": _rewards_optional_int(row.get("RedemptionID") or row.get("Existing_RedemptionID")),
        "transaction_id": _rewards_optional_int(row.get("Posted_TransactionID") or row.get("Existing_TransactionID")),
        "external_request_id": _rewards_text(row.get("External_Request_ID")),
        "chapter_id": _rewards_optional_int(row.get("ChapterID")),
        "chapter_code": _rewards_text(row.get("Chapter_Code")),
        "chapter_name": _rewards_text(row.get("Chapter_Name")),
        "request_date": explorer.json_safe_value(row.get("Request_Date")),
        "points": points,
        "amount_usd": float(amount) if amount is not None else round(points / 1000, 3),
        "redemption_category": _rewards_text(row.get("Redemption_Category")),
        "payment_mode": _rewards_text(row.get("Payment_Mode")),
        "description": _rewards_text(row.get("Description")),
        "receipt_reference": _rewards_text(row.get("Receipt_Reference")),
        "status": _rewards_text(row.get("Status")),
        "posted_at": explorer.json_safe_value(row.get("Posted_At")),
        "posted_by_principal_name": _rewards_text(row.get("Posted_By_Principal_Name")),
        "available_points": available,
        "available_after_points": _rewards_int(row.get("Available_After_Points")),
        "available_lot_count": _rewards_int(row.get("Available_Lot_Count")),
        "insufficient_balance_count": _rewards_int(row.get("InsufficientBalanceCount")),
        "shortfall_points": _rewards_int(row.get("Shortfall_Points")),
        "already_posted_count": _rewards_int(row.get("AlreadyPostedCount")),
        "new_post_count": _rewards_int(row.get("NewPostCount")),
        "latest_snapshot_date": explorer.json_safe_value(row.get("Latest_Snapshot_Date")),
    }


def _rewards_summary_payload(chapters: list[dict]) -> dict:
    """Execute the rewards summary payload routine."""
    available_points = sum(chapter["available_points"] for chapter in chapters)
    ledger_balance = sum(chapter["ledger_balance"] for chapter in chapters)
    total_credits = sum(chapter["total_credits"] for chapter in chapters)
    total_debits = sum(chapter["total_debits"] for chapter in chapters)
    return {
        "chapter_count": len(chapters),
        "available_points": available_points,
        "available_value_usd": round(available_points / 1000, 3),
        "ledger_balance": ledger_balance,
        "total_credits": total_credits,
        "total_debits": total_debits,
        "expiring_30_days": sum(chapter["expiring_30_days"] for chapter in chapters),
        "expiring_60_days": sum(chapter["expiring_60_days"] for chapter in chapters),
        "expiring_90_days": sum(chapter["expiring_90_days"] for chapter in chapters),
        "reconciliation_delta": sum(chapter["balance_reconciliation_delta"] for chapter in chapters),
    }


def _parse_rewards_limit(req: func.HttpRequest, default: int = 150, maximum: int = 500) -> tuple[int | None, func.HttpResponse | None]:
    """Parse rewards limit."""
    raw_limit = (req.params.get("limit") or "").strip()
    if not raw_limit:
        return default, None
    if not raw_limit.isdigit():
        return None, func.HttpResponse("Query parameter 'limit' must be a positive integer.", status_code=400)
    limit = int(raw_limit)
    if limit < 1 or limit > maximum:
        return None, func.HttpResponse(f"Query parameter 'limit' must be between 1 and {maximum}.", status_code=400)
    return limit, None


def _parse_rewards_chapter_code(req: func.HttpRequest) -> tuple[str | None, func.HttpResponse | None]:
    """Parse rewards chapter code."""
    chapter_code = (req.params.get("chapter_code") or req.params.get("chapter") or "").strip()
    if not chapter_code:
        return None, func.HttpResponse("Query parameter 'chapter_code' is required.", status_code=400)
    if len(chapter_code) > 64:
        return None, func.HttpResponse("Query parameter 'chapter_code' is too long.", status_code=400)
    return chapter_code, None


def _rewards_admin_authorization_response(
    req: func.HttpRequest,
    adapter: object,
    *,
    html: bool = False,
) -> tuple[dict | None, func.HttpResponse | None]:
    """Execute the rewards admin authorization response routine."""
    if authorize_admin_permission is None:
        message = "Rewards authorization modules are not available in this deployment."
        if html:
            return None, func.HttpResponse(message, status_code=500, headers=explorer.response_headers("text/plain; charset=utf-8"))
        return None, _bayrate_preview_error(message, status_code=500)

    result = authorize_admin_permission(
        req.headers,
        adapter,
        required_permission=REWARDS_REDEMPTIONS_PERMISSION,
        feature_label="Chapter Rewards",
    )
    if result.ok:
        return {
            "principal_name": result.principal.principal_name if result.principal else None,
            "principal_id": result.principal.principal_id if result.principal else None,
            "identity_provider": result.principal.identity_provider if result.principal else None,
            "permission_code": result.admin_row.get("Permission_Code") if result.admin_row else None,
        }, None

    if html and result.status_code == 401:
        headers = explorer.response_headers("text/plain; charset=utf-8")
        headers["Location"] = _bayrate_login_redirect(req)
        return None, func.HttpResponse("", status_code=302, headers=headers)

    message = result.error or "Rewards authorization failed."
    if html:
        return None, func.HttpResponse(message, status_code=result.status_code, headers=explorer.response_headers("text/plain; charset=utf-8"))
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


def _chapter_report_authorization_response(
    req: func.HttpRequest,
    adapter: object,
    *,
    html: bool = False,
) -> tuple[dict | None, func.HttpResponse | None]:
    """Execute the chapter report authorization response routine."""
    if authorize_admin_permission is None:
        message = "Chapter report authorization modules are not available in this deployment."
        if html:
            return None, func.HttpResponse(message, status_code=500, headers=explorer.response_headers("text/plain; charset=utf-8"))
        return None, _bayrate_preview_error(message, status_code=500)

    result = authorize_admin_permission(
        req.headers,
        adapter,
        required_permission=BAYRATE_RUN_PERMISSION,
        feature_label="Chapter Reports",
    )
    if result.ok:
        return {
            "principal_name": result.principal.principal_name if result.principal else None,
            "principal_id": result.principal.principal_id if result.principal else None,
            "identity_provider": result.principal.identity_provider if result.principal else None,
            "permission_code": result.admin_row.get("Permission_Code") if result.admin_row else None,
        }, None

    if html and result.status_code == 401:
        headers = explorer.response_headers("text/plain; charset=utf-8")
        headers["Location"] = _bayrate_login_redirect(req)
        return None, func.HttpResponse("", status_code=302, headers=headers)

    message = result.error or "Chapter report authorization failed."
    if html:
        return None, func.HttpResponse(message, status_code=result.status_code, headers=explorer.response_headers("text/plain; charset=utf-8"))
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


def _rewards_manual_debit_error(message: str, status_code: int = 400) -> func.HttpResponse:
    """Execute the rewards manual debit error routine."""
    return _public_json_response({"ok": False, "error": message}, status_code=status_code)


def _sgf_import_authorization_response(
    req: func.HttpRequest,
    adapter: object,
    *,
    html: bool = False,
) -> tuple[dict | None, func.HttpResponse | None]:
    """Execute the sgf import authorization response routine."""
    if authorize_admin_permission is None:
        message = "SGF import authorization modules are not available in this deployment."
        if html:
            return None, func.HttpResponse(message, status_code=500, headers=explorer.response_headers("text/plain; charset=utf-8"))
        return None, _bayrate_preview_error(message, status_code=500)

    result = authorize_admin_permission(
        req.headers,
        adapter,
        required_permission=SGF_IMPORT_PERMISSION,
        feature_label="SGF Import",
    )
    if result.ok:
        return {
            "principal_name": result.principal.principal_name if result.principal else None,
            "principal_id": result.principal.principal_id if result.principal else None,
            "identity_provider": result.principal.identity_provider if result.principal else None,
            "permission_code": result.admin_row.get("Permission_Code") if result.admin_row else None,
        }, None

    if html and result.status_code == 401:
        headers = explorer.response_headers("text/plain; charset=utf-8")
        headers["Location"] = _bayrate_login_redirect(req)
        return None, func.HttpResponse("", status_code=302, headers=headers)

    message = result.error or "SGF import authorization failed."
    if html:
        return None, func.HttpResponse(message, status_code=result.status_code, headers=explorer.response_headers("text/plain; charset=utf-8"))
    return None, _public_json_response(
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


def _sgf_import_error(message: str, status_code: int = 400) -> func.HttpResponse:
    """Execute the sgf import error routine."""
    return _public_json_response({"ok": False, "error": message}, status_code=status_code)


def _sgf_import_request_from_body(body: dict) -> tuple[dict | None, func.HttpResponse | None]:
    """Execute the sgf import request from body routine."""
    references = body.get("references", body.get("reference", body.get("leago_reference", body.get("leago_url"))))
    token = str(body.get("bearer_token") or body.get("leago_bearer_token") or "").strip()
    source_type = str(body.get("source_type") or body.get("default_kind") or "tournament").strip().lower()
    if source_type not in {"archive", "event", "tournament"}:
        return None, _sgf_import_error("source_type must be archive, event, or tournament.")
    replace_existing = body.get("replace_existing") is True
    if source_type == "archive":
        archive_base64 = str(body.get("archive_base64") or "").strip()
        archive_name = str(body.get("archive_name") or "sgf-archive.zip").strip()
        if not archive_base64:
            return None, _sgf_import_error("Archive ZIP data is required.")
        return {
            "source_type": source_type,
            "archive_base64": archive_base64,
            "archive_name": archive_name,
            "replace_existing": replace_existing,
        }, None
    if not token:
        return None, _sgf_import_error("Leago bearer token is required.")
    allow_aga_keys = body.get("allow_aga_keys") or body.get("allow_aga_key") or []
    if isinstance(allow_aga_keys, str):
        allow_aga_keys = [item.strip() for item in re.split(r"[\s,]+", allow_aga_keys) if item.strip()]
    if not isinstance(allow_aga_keys, list):
        return None, _sgf_import_error("allow_aga_keys must be a list or text value.")
    return {
        "references": references,
        "bearer_token": token,
        "source_type": source_type,
        "replace_existing": replace_existing,
        "allow_aga_keys": [str(item).strip() for item in allow_aga_keys if str(item).strip()],
    }, None


def _clean_rewards_body_text(body: dict, key: str, *, max_length: int, required: bool = False) -> tuple[str | None, str | None]:
    """Clean rewards body text."""
    text = str(body.get(key) or "").strip()
    if not text:
        if required:
            return None, f"{key} is required."
        return None, None
    if len(text) > max_length:
        return None, f"{key} must be {max_length} characters or fewer."
    return text, None


def _rewards_manual_debit_request_from_body(
    body: dict,
    authorization: dict,
    *,
    generate_request_id: bool,
) -> tuple[dict | None, func.HttpResponse | None]:
    """Execute the rewards manual debit request from body routine."""
    raw_chapter_id = body.get("chapter_id", body.get("chapterId"))
    chapter_id = None
    if raw_chapter_id not in (None, ""):
        try:
            chapter_id = int(str(raw_chapter_id).strip())
        except ValueError:
            return None, _rewards_manual_debit_error("chapter_id must be an integer.")
        if chapter_id <= 0:
            return None, _rewards_manual_debit_error("chapter_id must be positive.")

    chapter_code, error_text = _clean_rewards_body_text(body, "chapter_code", max_length=64)
    if error_text:
        return None, _rewards_manual_debit_error(error_text)
    if chapter_id is None and not chapter_code:
        return None, _rewards_manual_debit_error("Select a chapter before previewing or posting a debit.")

    raw_request_date = str(body.get("request_date") or body.get("requestDate") or date.today().isoformat()).strip()
    try:
        request_date = date.fromisoformat(raw_request_date[:10])
    except ValueError:
        return None, _rewards_manual_debit_error("request_date must use YYYY-MM-DD format.")

    try:
        points = int(str(body.get("points") or "").replace(",", "").strip())
    except ValueError:
        return None, _rewards_manual_debit_error("points must be a positive integer.")
    if points <= 0:
        return None, _rewards_manual_debit_error("points must be a positive integer.")
    if points > REWARDS_MAX_MANUAL_DEBIT_POINTS:
        return None, _rewards_manual_debit_error(f"points must be {REWARDS_MAX_MANUAL_DEBIT_POINTS:,} or less.")

    category = str(body.get("redemption_category") or body.get("category") or "").strip().lower()
    if category not in REWARDS_REDEMPTION_CATEGORIES:
        return None, _rewards_manual_debit_error("Choose a supported redemption category.")

    payment_mode = str(body.get("payment_mode") or body.get("paymentMode") or "").strip().lower()
    if payment_mode not in REWARDS_PAYMENT_MODES:
        return None, _rewards_manual_debit_error("Choose a supported payment mode.")

    description, error_text = _clean_rewards_body_text(body, "description", max_length=512, required=True)
    if error_text:
        return None, _rewards_manual_debit_error(error_text)

    receipt_reference, error_text = _clean_rewards_body_text(body, "receipt_reference", max_length=256)
    if error_text:
        return None, _rewards_manual_debit_error(error_text)

    notes, error_text = _clean_rewards_body_text(body, "notes", max_length=4000)
    if error_text:
        return None, _rewards_manual_debit_error(error_text)

    external_request_id, error_text = _clean_rewards_body_text(body, "external_request_id", max_length=64)
    if error_text:
        return None, _rewards_manual_debit_error(error_text)
    if external_request_id is None and generate_request_id:
        external_request_id = f"manual-{datetime.now(timezone.utc):%Y%m%d}-{uuid.uuid4().hex[:12]}"

    source_payload = {
        "entry_source": "rewards_admin_form",
        "chapter_id": chapter_id,
        "chapter_code": chapter_code,
        "request_date": request_date.isoformat(),
        "points": points,
        "redemption_category": category,
        "payment_mode": payment_mode,
        "description": description,
        "receipt_reference": receipt_reference,
        "notes": notes,
        "entered_by_principal_name": authorization.get("principal_name"),
        "entered_by_principal_id": authorization.get("principal_id"),
    }

    return {
        "chapter_id": chapter_id,
        "chapter_code": chapter_code,
        "request_date": request_date,
        "points": points,
        "redemption_category": category,
        "payment_mode": payment_mode,
        "description": description,
        "receipt_reference": receipt_reference,
        "external_request_id": external_request_id,
        "notes": notes,
        "source_payload_json": json.dumps(source_payload, sort_keys=True),
        "principal_name": authorization.get("principal_name"),
        "principal_id": authorization.get("principal_id"),
    }, None


def _rewards_manual_debit_params(request: dict, *, dry_run: bool) -> tuple:
    """Execute the rewards manual debit params routine."""
    return (
        request["chapter_id"],
        request["chapter_code"],
        request["request_date"],
        request["points"],
        request["redemption_category"],
        request["payment_mode"],
        request["description"],
        request["receipt_reference"],
        request["external_request_id"],
        request["notes"],
        request["source_payload_json"],
        bool(dry_run),
        request["principal_name"],
        request["principal_id"],
    )


def _safe_rewards_blob_component(value: str, fallback: str = "receipt") -> str:
    """Execute the safe rewards blob component routine."""
    text = str(value or "").strip()
    if not text:
        text = fallback
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("._-")
    return text[:180] or fallback


def _rewards_receipt_container_client():
    """Execute the rewards receipt container client routine."""
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:
        raise RuntimeError("azure-storage-blob is not installed for receipt uploads.") from exc

    connection_string = os.environ.get("AzureWebJobsStorage")
    blob_service_uri = os.environ.get("AzureWebJobsStorage__blobServiceUri")
    if connection_string:
        client = BlobServiceClient.from_connection_string(connection_string)
    elif blob_service_uri:
        try:
            from azure.identity import ManagedIdentityCredential
        except ImportError as exc:
            raise RuntimeError("azure-identity is required for managed identity receipt uploads.") from exc
        client_id = os.environ.get("AzureWebJobsStorage__clientId")
        credential = ManagedIdentityCredential(client_id=client_id) if client_id else ManagedIdentityCredential()
        client = BlobServiceClient(account_url=blob_service_uri, credential=credential)
    else:
        raise RuntimeError("No Azure Blob Storage configuration is available for receipt uploads.")

    container = client.get_container_client(REWARDS_RECEIPT_CONTAINER)
    try:
        container.create_container()
    except Exception:
        pass
    return container


def _decode_receipt_upload(item: dict) -> tuple[str, str, bytes]:
    """Decode receipt upload."""
    filename = _safe_rewards_blob_component(str(item.get("name") or "receipt"), fallback="receipt")
    content_type = str(item.get("content_type") or item.get("type") or "").strip().lower()
    if not content_type or content_type == "application/octet-stream":
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    if content_type in {"image/jpg", "image/pjpeg"}:
        content_type = "image/jpeg"
    if content_type not in REWARDS_ALLOWED_RECEIPT_CONTENT_TYPES:
        raise ValueError(f"{filename} has unsupported file type {content_type}.")

    data_url = str(item.get("data_url") or item.get("dataUrl") or "").strip()
    if "," in data_url and data_url.lower().startswith("data:"):
        encoded = data_url.split(",", 1)[1]
    else:
        encoded = str(item.get("base64") or "").strip()
    if not encoded:
        raise ValueError(f"{filename} is missing file content.")
    try:
        content = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise ValueError(f"{filename} is not valid base64 content.") from exc
    if not content:
        raise ValueError(f"{filename} is empty.")
    if len(content) > REWARDS_MAX_RECEIPT_BYTES:
        raise ValueError(f"{filename} is larger than the {REWARDS_MAX_RECEIPT_BYTES // (1024 * 1024)} MB receipt limit.")
    return filename, content_type, content


def _rewards_receipt_blob_name(redemption_id: int, filename: str) -> str:
    """Execute the rewards receipt blob name routine."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"redemptions/{redemption_id}/{stamp}-{uuid.uuid4().hex[:12]}-{filename}"


def _positive_body_int(body: dict, key: str) -> tuple[int | None, func.HttpResponse | None]:
    """Execute the positive body int routine."""
    try:
        value = int(str(body.get(key) or "").strip())
    except ValueError:
        return None, _rewards_manual_debit_error(f"{key} must be a positive integer.")
    if value <= 0:
        return None, _rewards_manual_debit_error(f"{key} must be a positive integer.")
    return value, None


def _load_snapshot_or_error() -> tuple[dict | None, func.HttpResponse | None]:
    """Load snapshot or error."""
    if (os.environ.get("RATINGS_EXPLORER_DISABLE_SNAPSHOT") or "").strip().lower() in {"1", "true", "yes", "on"}:
        return None, None
    snapshot = explorer.load_snapshot()
    if snapshot:
        return snapshot, None
    return None, None


def _get_conn_str_or_error() -> tuple[str | None, func.HttpResponse | None]:
    """Return conn str or error."""
    return SQL_CONNECTION_STRING, None


def _player_detail_has_recent_game_handicap(payload: dict | None) -> bool:
    """Execute the player detail has recent game handicap routine."""
    if not payload:
        return False
    recent_games = payload.get("recent_games") or []
    if not recent_games:
        return True
    return all("handicap" in game for game in recent_games)


def _player_detail_has_recent_game_sgf_metadata(payload: dict | None) -> bool:
    """Execute the player detail has recent game sgf metadata routine."""
    if not payload:
        return False
    recent_games = payload.get("recent_games") or []
    if not recent_games:
        return True
    return all("game_id" in game and "has_sgf" in game for game in recent_games)


def _player_detail_has_recent_game_rank_metadata(payload: dict | None) -> bool:
    """Execute the player detail has recent game rank metadata routine."""
    if not payload:
        return False
    recent_games = payload.get("recent_games") or []
    if not recent_games:
        return True
    sgf_games = [game for game in recent_games if game.get("has_sgf")]
    if not sgf_games:
        return True
    return all("player_rank" in game and "opponent_rank" in game for game in sgf_games)


def _tournament_detail_has_game_sgf_metadata(payload: dict | None) -> bool:
    """Execute the tournament detail has game sgf metadata routine."""
    if not payload:
        return False
    games = payload.get("games") or []
    if not games:
        return True
    return all("has_sgf" in game for game in games)


def _history_payload_from_points(history: list[tuple[datetime, float, float]]) -> list[dict]:
    """Execute the history payload from points routine."""
    return explorer.serialize_rating_history(history)


def _parse_search_limit(req: func.HttpRequest) -> tuple[int | None, func.HttpResponse | None]:
    """Parse search limit."""
    limit_text = (req.params.get("limit") or "").strip()
    if not limit_text:
        return DEFAULT_SEARCH_LIMIT, None
    if not limit_text.isdigit():
        return None, func.HttpResponse("Query parameter 'limit' must be one of 10, 25, 50, 100, or 250.", status_code=400)
    limit = int(limit_text)
    if limit not in ALLOWED_SEARCH_LIMITS:
        return None, func.HttpResponse("Query parameter 'limit' must be one of 10, 25, 50, 100, or 250.", status_code=400)
    return limit, None


def _parse_nonnegative_int_param(req: func.HttpRequest, name: str, default: int = 0) -> tuple[int | None, func.HttpResponse | None]:
    """Parse nonnegative int param."""
    raw_text = (req.params.get(name) or "").strip()
    if not raw_text:
        return default, None
    if not raw_text.isdigit():
        return None, func.HttpResponse(f"Query parameter '{name}' must be a non-negative integer.", status_code=400)
    return int(raw_text), None


def _years_ago_iso(years: int, today: date) -> str:
    """Execute the years ago iso routine."""
    try:
        return today.replace(year=today.year - years).isoformat()
    except ValueError:
        return today.replace(month=2, day=28, year=today.year - years).isoformat()


def _parse_recent_activity_cutoff(req: func.HttpRequest) -> tuple[str | None, func.HttpResponse | None]:
    """Parse recent activity cutoff."""
    raw_years_text = req.params.get("recent_activity_years")
    years_text = (raw_years_text or "").strip()
    if years_text.lower() in {"none", "all", "no_limit", "nolimit"}:
        return None, None
    if not years_text:
        return _years_ago_iso(DEFAULT_RECENT_ACTIVITY_YEARS, datetime.now(timezone.utc).date()), None
    if not years_text.isdigit():
        return None, func.HttpResponse(
            "Query parameter 'recent_activity_years' must be one of 1, 3, 5, or 10.",
            status_code=400,
        )
    years = int(years_text)
    if years not in ALLOWED_ACTIVITY_YEARS:
        return None, func.HttpResponse(
            "Query parameter 'recent_activity_years' must be one of 1, 3, 5, or 10.",
            status_code=400,
        )
    return _years_ago_iso(years, datetime.now(timezone.utc).date()), None


def _parse_rating_bands(req: func.HttpRequest) -> tuple[list[str] | None, func.HttpResponse | None]:
    """Parse rating bands."""
    rating_bands_text = (req.params.get("rating_bands") or req.params.get("rating_band") or "").strip()
    if not rating_bands_text:
        return None, None
    rating_bands = [value.strip() for value in rating_bands_text.split(",") if value.strip()]
    invalid = [value for value in rating_bands if value not in ALLOWED_RATING_BANDS]
    if invalid:
        return None, func.HttpResponse(
            "Query parameter 'rating_bands' is invalid.",
            status_code=400,
        )
    return rating_bands, None


def _parse_player_status(req: func.HttpRequest) -> tuple[str, func.HttpResponse | None]:
    """Parse player status."""
    status_filter = (req.params.get("status") or "").strip().lower() or "all"
    if status_filter not in ALLOWED_PLAYER_STATUS_FILTERS:
        return "all", func.HttpResponse(
            "Query parameter 'status' must be one of All, Active, or Expired.",
            status_code=400,
        )
    return status_filter, None


def _parse_csv_values(req: func.HttpRequest, key: str, legacy_key: str | None = None) -> list[str] | None:
    """Parse csv values."""
    raw = (req.params.get(key) or req.params.get(legacy_key or "") or "").strip()
    if not raw:
        return None
    values = [value.strip() for value in raw.split(",") if value.strip()]
    return values or None


def _is_default_player_startup_search(
    agaid: int | None,
    first_name: str | None,
    last_name: str | None,
    chapters: list[str] | None,
    states: list[str] | None,
    member_types: list[str] | None,
    status_filter: str | None,
    recent_activity_cutoff: str | None,
    rating_bands: list[str] | None,
) -> bool:
    """Return whether default player startup search."""
    return (
        agaid is None
        and not (first_name or "").strip()
        and not (last_name or "").strip()
        and not chapters
        and not states
        and not member_types
        and (status_filter or "all").strip().lower() == "all"
        and recent_activity_cutoff is None
        and not rating_bands
    )


@app.function_name(name="RewardsPublicReportPage")
@app.route(route="ratings-explorer/rewards", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_public_report_page(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RewardsPublicReportPage Azure Function endpoint."""
    return func.HttpResponse(
        explorer.load_ratings_explorer_html("", "rewards_report.html"),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="RewardsAdminReportPage")
@app.route(route="ratings-explorer/rewards/admin", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_admin_report_page(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RewardsAdminReportPage Azure Function endpoint."""
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _rewards_admin_authorization_response(req, adapter, html=True)
    if auth_error:
        return auth_error
    return func.HttpResponse(
        explorer.load_ratings_explorer_html("", "rewards_report.html"),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="RewardsAdminMe")
@app.route(route="ratings-explorer/rewards/admin/me", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_admin_me(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RewardsAdminMe Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    authorization, auth_error = _rewards_admin_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    return _public_json_response({"ok": True, "authorization": authorization})


@app.function_name(name="RatingsExplorerSgfImportPage")
@app.route(route="ratings-explorer/sgf-import", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def sgf_import_page(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerSgfImportPage Azure Function endpoint."""
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _sgf_import_authorization_response(req, adapter, html=True)
    if auth_error:
        return auth_error
    return func.HttpResponse(
        explorer.load_ratings_explorer_html("", "leago_sgf_import.html"),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="RatingsExplorerSgfImportPreview")
@app.route(route="ratings-explorer/sgf-import/preview", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def sgf_import_preview(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerSgfImportPreview Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    started = perf_counter()
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    authorization, auth_error = _sgf_import_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, body_error = _bayrate_request_json(req)
    if body_error:
        return body_error
    import_request, request_error = _sgf_import_request_from_body(body)
    if request_error:
        return request_error
    try:
        if import_request["source_type"] == "archive":
            if preview_archive_import is None:
                return _sgf_import_error("Archive SGF import modules are not available in this deployment.", status_code=500)
            payload = preview_archive_import(
                SQL_CONNECTION_STRING,
                import_request["archive_base64"],
                archive_name=import_request["archive_name"],
                replace_existing=import_request["replace_existing"],
            )
            data_source = "sgf_archive_sql_live"
        else:
            if preview_leago_import is None:
                return _sgf_import_error("Leago SGF import modules are not available in this deployment.", status_code=500)
            payload = preview_leago_import(
                SQL_CONNECTION_STRING,
                import_request["references"],
                import_request["bearer_token"],
                default_kind=import_request["source_type"],
                replace_existing=import_request["replace_existing"],
                allow_aga_keys=import_request["allow_aga_keys"],
            )
            data_source = "leago_api_sql_live"
        payload["authorization"] = authorization
        return _public_json_response(
            _with_debug(
                payload,
                data_source=data_source,
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except ValueError as exc:
        return _sgf_import_error(str(exc))
    except Exception as exc:
        return _sgf_import_error(f"SGF preview failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerSgfImportApply")
@app.route(route="ratings-explorer/sgf-import/apply", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def sgf_import_apply(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerSgfImportApply Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    started = perf_counter()
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    authorization, auth_error = _sgf_import_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, body_error = _bayrate_request_json(req)
    if body_error:
        return body_error
    import_request, request_error = _sgf_import_request_from_body(body)
    if request_error:
        return request_error
    if body.get("confirm_apply") is not True:
        return _sgf_import_error("confirm_apply=true is required before uploading SGFs.")
    plan_hash = str(body.get("plan_hash") or "").strip()
    if not plan_hash:
        return _sgf_import_error("plan_hash is required. Run preview before applying.")
    try:
        if import_request["source_type"] == "archive":
            if apply_archive_import is None:
                return _sgf_import_error("Archive SGF import modules are not available in this deployment.", status_code=500)
            payload = apply_archive_import(
                SQL_CONNECTION_STRING,
                import_request["archive_base64"],
                archive_name=import_request["archive_name"],
                expected_plan_hash=plan_hash,
                replace_existing=import_request["replace_existing"],
            )
            data_source = "sgf_archive_sql_live"
            refresh_detail = "Snapshot refresh requested after archive SGF import."
        else:
            if apply_leago_import is None:
                return _sgf_import_error("Leago SGF import modules are not available in this deployment.", status_code=500)
            payload = apply_leago_import(
                SQL_CONNECTION_STRING,
                import_request["references"],
                import_request["bearer_token"],
                expected_plan_hash=plan_hash,
                default_kind=import_request["source_type"],
                replace_existing=import_request["replace_existing"],
                allow_aga_keys=import_request["allow_aga_keys"],
            )
            data_source = "leago_api_sql_live"
            refresh_detail = "Snapshot refresh requested after Leago SGF import."
        payload["authorization"] = authorization
        if int((payload.get("summary") or {}).get("applied") or 0) > 0:
            requested_at = datetime.now(timezone.utc).isoformat()
            try:
                explorer.request_snapshot_refresh("sgf_import", requested_at)
                explorer.update_snapshot_status(
                    "queued",
                    source="sgf_import",
                    detail=refresh_detail,
                    error=None,
                )
                payload["snapshot_refresh"] = {"queued": True, "requested_at": requested_at}
            except Exception as exc:
                payload["snapshot_refresh"] = {"queued": False, "error": str(exc)}
        return _public_json_response(
            _with_debug(
                payload,
                data_source=data_source,
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except ValueError as exc:
        return _sgf_import_error(str(exc))
    except Exception as exc:
        return _sgf_import_error(f"SGF apply failed: {exc}", status_code=500)


@app.function_name(name="RewardsAdminOptions")
@app.route(route="ratings-explorer/rewards/admin/options", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_admin_options(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RewardsAdminOptions Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    started = perf_counter()
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    authorization, auth_error = _rewards_admin_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    try:
        rows = adapter.query_rows(REWARDS_ADMIN_CHAPTER_OPTIONS_SQL)
        return _public_json_response(
            _with_debug(
                {
                    "ok": True,
                    "authorization": authorization,
                    "chapters": [_rewards_admin_chapter_payload(row) for row in rows],
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return _rewards_manual_debit_error(f"Rewards admin options failed: {exc}", status_code=500)


@app.function_name(name="RewardsAdminDebitPreview")
@app.route(route="ratings-explorer/rewards/admin/debit-preview", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_admin_debit_preview(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RewardsAdminDebitPreview Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    started = perf_counter()
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    authorization, auth_error = _rewards_admin_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, body_error = _bayrate_request_json(req)
    if body_error:
        return body_error
    request, request_error = _rewards_manual_debit_request_from_body(body, authorization, generate_request_id=False)
    if request_error:
        return request_error
    try:
        rows = adapter.query_rows(REWARDS_MANUAL_DEBIT_SQL, _rewards_manual_debit_params(request, dry_run=True))
        if not rows:
            return _rewards_manual_debit_error("Manual debit preview did not return a result.", status_code=500)
        preview = _rewards_manual_debit_payload(rows[0])
        return _public_json_response(
            _with_debug(
                {"ok": True, "authorization": authorization, "preview": preview},
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return _rewards_manual_debit_error(f"Manual debit preview failed: {exc}", status_code=500)


@app.function_name(name="RewardsAdminDebitPost")
@app.route(route="ratings-explorer/rewards/admin/debit", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_admin_debit_post(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RewardsAdminDebitPost Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    started = perf_counter()
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    authorization, auth_error = _rewards_admin_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, body_error = _bayrate_request_json(req)
    if body_error:
        return body_error
    if body.get("confirm_debit") is not True:
        return _rewards_manual_debit_error("confirm_debit=true is required before posting a manual debit.")
    request, request_error = _rewards_manual_debit_request_from_body(body, authorization, generate_request_id=True)
    if request_error:
        return request_error
    try:
        adapter.execute_statements([(REWARDS_MANUAL_DEBIT_SQL, _rewards_manual_debit_params(request, dry_run=False))])
        rows = adapter.query_rows(REWARDS_MANUAL_DEBIT_LOOKUP_SQL, (request["external_request_id"],))
        if not rows:
            return _rewards_manual_debit_error("Manual debit posted but could not be reloaded.", status_code=500)
        debit = _rewards_manual_debit_payload(rows[0])
        return _public_json_response(
            _with_debug(
                {"ok": True, "authorization": authorization, "debit": debit},
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return _rewards_manual_debit_error(f"Manual debit post failed: {exc}", status_code=500)


@app.function_name(name="RewardsAdminRedemption")
@app.route(route="ratings-explorer/rewards/admin/redemption", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_admin_redemption(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RewardsAdminRedemption Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    authorization, auth_error = _rewards_admin_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    try:
        redemption_id = int(str(req.params.get("redemption_id") or "").strip())
    except ValueError:
        return _rewards_manual_debit_error("redemption_id must be a positive integer.")
    if redemption_id <= 0:
        return _rewards_manual_debit_error("redemption_id must be a positive integer.")
    try:
        rows = adapter.query_rows(REWARDS_ADMIN_REDEMPTION_SQL, (redemption_id,))
        if not rows:
            return _rewards_manual_debit_error("Redemption was not found.", status_code=404)
        receipt_rows = adapter.query_rows(REWARDS_RECEIPTS_SQL, (redemption_id,))
        receipts = [_rewards_receipt_payload(row) for row in receipt_rows]
        return _public_json_response(
            {
                "ok": True,
                "authorization": authorization,
                "redemption": _rewards_admin_redemption_payload(rows[0], receipts),
            }
        )
    except Exception as exc:
        return _rewards_manual_debit_error(f"Redemption detail failed: {exc}", status_code=500)


@app.function_name(name="RewardsAdminRedemptionNotes")
@app.route(route="ratings-explorer/rewards/admin/redemption-notes", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_admin_redemption_notes(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RewardsAdminRedemptionNotes Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    authorization, auth_error = _rewards_admin_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, body_error = _bayrate_request_json(req)
    if body_error:
        return body_error
    redemption_id, id_error = _positive_body_int(body, "redemption_id")
    if id_error:
        return id_error
    notes = str(body.get("notes") or "").strip()
    if len(notes) > 4000:
        return _rewards_manual_debit_error("notes must be 4000 characters or fewer.")
    try:
        adapter.execute_statements(
            [
                (
                    REWARDS_UPDATE_NOTES_SQL,
                    (redemption_id, notes or None, authorization.get("principal_name"), authorization.get("principal_id")),
                )
            ]
        )
        rows = adapter.query_rows(REWARDS_ADMIN_REDEMPTION_SQL, (redemption_id,))
        receipt_rows = adapter.query_rows(REWARDS_RECEIPTS_SQL, (redemption_id,))
        return _public_json_response(
            {
                "ok": True,
                "authorization": authorization,
                "redemption": _rewards_admin_redemption_payload(rows[0], [_rewards_receipt_payload(row) for row in receipt_rows]),
            }
        )
    except Exception as exc:
        return _rewards_manual_debit_error(f"Notes update failed: {exc}", status_code=500)


@app.function_name(name="RewardsAdminReceiptUpload")
@app.route(route="ratings-explorer/rewards/admin/receipt-upload", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_admin_receipt_upload(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RewardsAdminReceiptUpload Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    authorization, auth_error = _rewards_admin_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, body_error = _bayrate_request_json(req)
    if body_error:
        return body_error
    redemption_id, id_error = _positive_body_int(body, "redemption_id")
    if id_error:
        return id_error
    files = body.get("files")
    if not isinstance(files, list) or not files:
        return _rewards_manual_debit_error("At least one receipt file is required.")
    if len(files) > REWARDS_MAX_RECEIPT_FILES:
        return _rewards_manual_debit_error(f"Upload at most {REWARDS_MAX_RECEIPT_FILES} receipt files at a time.")

    try:
        container = _rewards_receipt_container_client()
        for item in files:
            if not isinstance(item, dict):
                return _rewards_manual_debit_error("Each receipt file must be an object.")
            filename, content_type, content = _decode_receipt_upload(item)
            blob_name = _rewards_receipt_blob_name(redemption_id, filename)
            blob = container.get_blob_client(blob_name)
            blob.upload_blob(content, overwrite=False)
            try:
                adapter.execute_statements(
                    [
                        (
                            REWARDS_ADD_RECEIPT_SQL,
                            (
                                redemption_id,
                                REWARDS_RECEIPT_CONTAINER,
                                blob_name,
                                filename,
                                content_type,
                                len(content),
                                hashlib.sha256(content).hexdigest(),
                                authorization.get("principal_name"),
                                authorization.get("principal_id"),
                            ),
                        )
                    ]
                )
            except Exception:
                try:
                    blob.delete_blob(delete_snapshots="include")
                except Exception:
                    pass
                raise
        receipt_rows = adapter.query_rows(REWARDS_RECEIPTS_SQL, (redemption_id,))
        receipt_payloads = [_rewards_receipt_payload(row) for row in receipt_rows]
        return _public_json_response(
            {
                "ok": True,
                "authorization": authorization,
                "uploaded": receipt_payloads,
                "receipts": receipt_payloads,
            }
        )
    except ValueError as exc:
        return _rewards_manual_debit_error(str(exc))
    except Exception as exc:
        return _rewards_manual_debit_error(f"Receipt upload failed: {exc}", status_code=500)


@app.function_name(name="RewardsAdminReceiptDelete")
@app.route(route="ratings-explorer/rewards/admin/receipt-delete", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_admin_receipt_delete(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RewardsAdminReceiptDelete Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    authorization, auth_error = _rewards_admin_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, body_error = _bayrate_request_json(req)
    if body_error:
        return body_error
    receipt_id, id_error = _positive_body_int(body, "receipt_id")
    if id_error:
        return id_error
    try:
        adapter.execute_statements(
            [
                (
                    REWARDS_DELETE_RECEIPT_SQL,
                    (receipt_id, authorization.get("principal_name"), authorization.get("principal_id")),
                )
            ]
        )
        return _public_json_response(
            {
                "ok": True,
                "authorization": authorization,
                "receipt": {"receipt_id": receipt_id, "is_deleted": True},
            }
        )
    except Exception as exc:
        return _rewards_manual_debit_error(f"Receipt removal failed: {exc}", status_code=500)


@app.function_name(name="RewardsAdminReceiptFile")
@app.route(route="ratings-explorer/rewards/admin/receipts/{receipt_id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_admin_receipt_file(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RewardsAdminReceiptFile Azure Function endpoint."""
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _rewards_admin_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    try:
        receipt_id = int(str(req.route_params.get("receipt_id") or "").strip())
    except ValueError:
        return func.HttpResponse("receipt_id must be a positive integer.", status_code=400)
    try:
        rows = adapter.query_rows(REWARDS_RECEIPT_LOOKUP_SQL, (receipt_id,))
        if not rows:
            return func.HttpResponse("Receipt was not found.", status_code=404)
        receipt = rows[0]
        container = _rewards_receipt_container_client()
        blob = container.get_blob_client(receipt["Blob_Name"])
        content = blob.download_blob().readall()
        content_type = _rewards_text(receipt.get("Content_Type")) or "application/octet-stream"
        filename = _safe_rewards_blob_component(_rewards_text(receipt.get("Original_File_Name")) or f"receipt-{receipt_id}")
        headers = explorer.response_headers(content_type)
        headers["Content-Disposition"] = f'inline; filename="{filename}"'
        return func.HttpResponse(content, status_code=200, headers=headers)
    except Exception as exc:
        return func.HttpResponse(f"Receipt download failed: {exc}", status_code=500)


@app.function_name(name="ChapterDirectoryReportPage")
@app.route(route="ratings-explorer/reports/chapters", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_directory_report_page(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterDirectoryReportPage Azure Function endpoint."""
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return func.HttpResponse("SQL adapter is not available.", status_code=500, headers=explorer.response_headers("text/plain; charset=utf-8"))
    _, auth_error = _chapter_report_authorization_response(req, adapter, html=True)
    if auth_error:
        return auth_error
    return func.HttpResponse(
        explorer.load_ratings_explorer_html("", "chapter_directory_report.html"),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="ChapterDirectoryReportData")
@app.route(route="ratings-explorer/reports/chapters/data", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_directory_report_data(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterDirectoryReportData Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    started = perf_counter()
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    authorization, auth_error = _chapter_report_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    requested_as_of_date, date_error = _parse_chapter_directory_as_of_date(req)
    if date_error:
        return date_error
    try:
        snapshot_rows = explorer.query_rows(SQL_CONNECTION_STRING, CHAPTER_DIRECTORY_SNAPSHOT_SQL, [requested_as_of_date])
        snapshot_metadata = snapshot_rows[0] if snapshot_rows else {}
        snapshot_date = snapshot_metadata.get("Snapshot_Date")
        if snapshot_date is None:
            return _public_json_response(
                {
                    "ok": False,
                    "error": f"No chapter snapshot was found on or before {requested_as_of_date.isoformat()}.",
                    "requested_as_of_date": requested_as_of_date.isoformat(),
                    "snapshot_range": {
                        "min_snapshot_date": explorer.json_safe_value(snapshot_metadata.get("Min_Snapshot_Date")),
                        "max_snapshot_date": explorer.json_safe_value(snapshot_metadata.get("Max_Snapshot_Date")),
                        "snapshot_date_count": _rewards_int(snapshot_metadata.get("Snapshot_Date_Count")),
                    },
                },
                status_code=404,
            )
        rows = explorer.query_rows(SQL_CONNECTION_STRING, CHAPTER_DIRECTORY_REPORT_SQL, [snapshot_date])
        payload = _chapter_directory_payload(
            requested_as_of_date=requested_as_of_date,
            snapshot_metadata=snapshot_metadata,
            rows=rows,
            authorization=authorization,
        )
        output_format = (req.params.get("format") or "").strip().lower()
        if output_format == "csv" or (req.params.get("download") or "").strip().lower() in {"1", "true", "yes"}:
            return _chapter_directory_csv_response(payload)
        return _public_json_response(
            _with_debug(
                payload,
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return func.HttpResponse(f"Chapter directory report failed: {exc}", status_code=500)


@app.function_name(name="GoServicesDirectoryPrototypePage")
@app.route(route="ratings-explorer/go-services", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def go_services_directory_prototype_page(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the GoServicesDirectoryPrototypePage Azure Function endpoint."""
    return func.HttpResponse(
        explorer.load_ratings_explorer_html("", "go_services_directory_prototype.html"),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="RewardsPublicBalances")
@app.route(route="ratings-explorer/rewards/balances", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_public_balances(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RewardsPublicBalances Azure Function endpoint."""
    started = perf_counter()
    try:
        rows = explorer.query_rows(SQL_CONNECTION_STRING, REWARDS_PUBLIC_BALANCES_SQL, [])
        chapters = [_rewards_balance_payload(row) for row in rows]
        return _public_json_response(
            _with_debug(
                {
                    "ok": True,
                    "generated_at": _utc_now_text(),
                    "summary": _rewards_summary_payload(chapters),
                    "chapters": chapters,
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return func.HttpResponse(f"Rewards balance report failed: {exc}", status_code=500)


@app.function_name(name="RewardsPublicChapter")
@app.route(route="ratings-explorer/rewards/chapter", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_public_chapter(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RewardsPublicChapter Azure Function endpoint."""
    started = perf_counter()
    chapter_code, error = _parse_rewards_chapter_code(req)
    if error:
        return error
    limit, error = _parse_rewards_limit(req)
    if error:
        return error
    try:
        balance_rows = explorer.query_rows(SQL_CONNECTION_STRING, REWARDS_PUBLIC_CHAPTER_BALANCE_SQL, [chapter_code])
        if not balance_rows:
            return func.HttpResponse(f"No rewards balance found for chapter '{chapter_code}'.", status_code=404)
        transactions = [
            _rewards_transaction_payload(row)
            for row in explorer.query_rows(SQL_CONNECTION_STRING, REWARDS_PUBLIC_TRANSACTIONS_SQL, [chapter_code, limit])
        ]
        lots = [
            _rewards_lot_payload(row)
            for row in explorer.query_rows(SQL_CONNECTION_STRING, REWARDS_PUBLIC_LOTS_SQL, [chapter_code, limit])
        ]
        breakdown = [
            _rewards_breakdown_payload(row)
            for row in explorer.query_rows(SQL_CONNECTION_STRING, REWARDS_PUBLIC_BREAKDOWN_SQL, [chapter_code])
        ]
        redemptions = [
            _rewards_redemption_payload(row)
            for row in explorer.query_rows(SQL_CONNECTION_STRING, REWARDS_PUBLIC_REDEMPTIONS_SQL, [limit, chapter_code])
        ]
        return _public_json_response(
            _with_debug(
                {
                    "ok": True,
                    "generated_at": _utc_now_text(),
                    "chapter": _rewards_balance_payload(balance_rows[0]),
                    "transactions": transactions,
                    "lots": lots,
                    "breakdown": breakdown,
                    "redemptions": redemptions,
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return func.HttpResponse(f"Rewards chapter report failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerPage")
@app.route(route="ratings-explorer", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_page(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerPage Azure Function endpoint."""
    return func.HttpResponse(
        explorer.load_ratings_explorer_html(""),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="RatingsExplorerMobilePage")
@app.route(route="ratings-explorer/mobile", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_mobile_page(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerMobilePage Azure Function endpoint."""
    return func.HttpResponse(
        explorer.load_ratings_explorer_html("", "ratings_explorer_mobile.html"),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="BayRateStagingPage")
@app.route(route="ratings-explorer/bayrate", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_page(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingPage Azure Function endpoint."""
    headers = explorer.response_headers("text/plain; charset=utf-8")
    headers["Location"] = standalone_bayrate_url()
    return func.HttpResponse(
        "BayRate has moved to its standalone application.",
        status_code=302,
        headers=headers,
    )


@app.function_name(name="BayRateStagingPreview")
@app.route(route="ratings-explorer/bayrate/preview", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_preview(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingPreview Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
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
@app.route(route="ratings-explorer/bayrate/metadata-options", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_metadata_options(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateMetadataOptions Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
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
@app.route(route="ratings-explorer/bayrate/stage", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_write(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingWrite Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
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
@app.route(route="ratings-explorer/bayrate/review", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_review(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingReview Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
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
@app.route(route="ratings-explorer/bayrate/run", methods=["GET", "POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_run(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingRun Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
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
@app.route(route="ratings-explorer/bayrate/replay", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_replay(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingReplay Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
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
@app.route(route="ratings-explorer/bayrate/commit-preview", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_commit_preview(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingCommitPreview Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
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
@app.route(route="ratings-explorer/bayrate/commit", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_commit(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateStagingCommit Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
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


@app.function_name(name="BayRateMemberMergePreview")
@app.route(route="ratings-explorer/bayrate/member-merge-preview", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_member_merge_preview(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateMemberMergePreview Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("member_merge"):
        return _bayrate_preview_error("BayRate member-merge modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    source_agaids, target_agaid, error = _bayrate_member_merge_ids_from_body(body)
    if error:
        return error
    try:
        plan = build_member_merge_plan(adapter, source_agaids=source_agaids, target_agaid=target_agaid)
    except ValueError as exc:
        return _bayrate_preview_error(str(exc))
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)
    return _bayrate_json_response(
        {
            "ok": True,
            "read_only": True,
            "member_merge_plan": printable_member_merge_plan(plan),
            "confirmation_text": _bayrate_member_merge_confirmation_text(source_agaids, target_agaid),
        }
    )


@app.function_name(name="BayRateMemberMerge")
@app.route(route="ratings-explorer/bayrate/member-merge", methods=["GET", "POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_member_merge(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the BayRateMemberMerge Azure Function endpoint."""
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("member_merge"):
        return _bayrate_preview_error("BayRate member-merge modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    if req.method == "GET":
        _, auth_error = _bayrate_authorization_response(req, adapter, html=True)
        if auth_error:
            return auth_error
        return func.HttpResponse(
            _bayrate_member_merge_page_html(),
            status_code=200,
            headers=explorer.response_headers("text/html; charset=utf-8"),
        )
    authorization, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    source_agaids, target_agaid, error = _bayrate_member_merge_ids_from_body(body)
    if error:
        return error
    if body.get("confirm_member_rating_merge") is not True:
        return _bayrate_preview_error("confirm_member_rating_merge=true is required.")
    expected_confirmation = _bayrate_member_merge_confirmation_text(source_agaids, target_agaid)
    if str(body.get("confirmation_text") or "").strip() != expected_confirmation:
        return _bayrate_preview_error(f'confirmation_text must be "{expected_confirmation}".')
    plan_hash = str(body.get("member_merge_plan_hash") or body.get("merge_plan_hash") or "").strip()
    if not plan_hash:
        return _bayrate_preview_error("member_merge_plan_hash is required. Preview the member merge before executing.")

    try:
        plan = merge_member_ratings(
            adapter,
            source_agaids=source_agaids,
            target_agaid=target_agaid,
            confirm_member_rating_merge=True,
            expected_plan_hash=plan_hash,
        )
    except ValueError as exc:
        status_code = 409 if "changed since preview" in str(exc) else 400
        return _bayrate_preview_error(str(exc), status_code=status_code)
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)

    return _bayrate_json_response(
        {
            "ok": True,
            "member_merge_plan": printable_member_merge_plan(plan),
            "confirmation_text": expected_confirmation,
            "authorization": authorization,
        }
    )


@app.function_name(name="RatingsExplorerPlayers")
@app.route(route="ratings-explorer/players", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_players(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerPlayers Azure Function endpoint."""
    started = perf_counter()
    snapshot = explorer.load_player_search_snapshot()
    limit, error = _parse_search_limit(req)
    if error:
        return error
    recent_activity_cutoff, error = _parse_recent_activity_cutoff(req)
    if error:
        return error
    rating_bands, error = _parse_rating_bands(req)
    if error:
        return error
    status_filter, error = _parse_player_status(req)
    if error:
        return error
    agaid_text = (req.params.get("agaid") or "").strip()
    chapters = _parse_csv_values(req, "chapters", "chapter")
    states = _parse_csv_values(req, "states", "state")
    member_types = _parse_csv_values(req, "member_types", "member_type")
    first_name = (req.params.get("first_name") or "").strip() or None
    last_name = (req.params.get("last_name") or "").strip() or None
    agaid = None
    if agaid_text:
        if not agaid_text.isdigit():
            return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
        agaid = int(agaid_text)
    try:
        can_use_snapshot = bool(
            snapshot and (
                explorer.snapshot_supports_player_member_type(snapshot)
                or _is_default_player_startup_search(
                    agaid,
                    first_name,
                    last_name,
                    chapters,
                    states,
                    member_types,
                    status_filter,
                    recent_activity_cutoff,
                    rating_bands,
                )
            )
        )
        if can_use_snapshot:
            return _json_response(
                _with_debug(
                    {
                        "results": explorer.search_players_from_snapshot(
                            snapshot,
                            agaid,
                            first_name,
                            last_name,
                            chapters,
                            states,
                            member_types,
                            status_filter,
                            recent_activity_cutoff,
                            rating_bands,
                            limit,
                        )
                    },
                    data_source="player_search_snapshot",
                    elapsed_ms=round((perf_counter() - started) * 1000, 1),
                )
            )
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        return _json_response(
            _with_debug(
                {
                    "results": explorer.search_players(
                        conn_str,
                        agaid,
                        first_name,
                        last_name,
                        chapters,
                        states,
                        member_types,
                        status_filter,
                        recent_activity_cutoff,
                        rating_bands,
                        limit,
                    )
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return func.HttpResponse(f"Player search failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerPlayersStartup")
@app.route(route="ratings-explorer/players-startup", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_players_startup(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerPlayersStartup Azure Function endpoint."""
    started = perf_counter()
    limit, error = _parse_search_limit(req)
    if error:
        return error
    try:
        startup_payload = explorer.load_startup_players()
        if startup_payload:
            results = list(startup_payload.get("results") or [])[:limit]
            return _json_response(
                _with_debug(
                    {
                        "results": results,
                        "meta": startup_payload.get("meta") or {},
                    },
                    data_source="startup_snapshot",
                    elapsed_ms=round((perf_counter() - started) * 1000, 1),
                )
            )
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        return _json_response(
            _with_debug(
                {
                    "results": explorer.search_players(
                        conn_str,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        "all",
                        None,
                        None,
                        limit,
                    )
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return func.HttpResponse(f"Startup player load failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerTournaments")
@app.route(route="ratings-explorer/tournaments", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_tournaments(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerTournaments Azure Function endpoint."""
    started = perf_counter()
    snapshot = explorer.load_tournament_search_snapshot()
    limit, error = _parse_search_limit(req)
    if error:
        return error
    page, error = _parse_nonnegative_int_param(req, "page")
    if error:
        return error
    try:
        tournament_code = (req.params.get("tournament_code") or "").strip() or None
        cities = _parse_csv_values(req, "cities", "city")
        states = _parse_csv_values(req, "states", "state")
        if snapshot:
            search_payload = explorer.search_tournaments_from_snapshot(
                snapshot,
                (req.params.get("description") or "").strip() or None,
                tournament_code,
                cities,
                states,
                (req.params.get("date_from") or "").strip() or None,
                (req.params.get("date_before") or "").strip() or None,
                limit,
                page=page,
            )
            return _json_response(
                _with_debug(
                    search_payload,
                    data_source="tournament_search_snapshot",
                    elapsed_ms=round((perf_counter() - started) * 1000, 1),
                )
            )
        legacy_snapshot, error = _load_snapshot_or_error()
        if error:
            return error
        if legacy_snapshot:
            search_payload = explorer.search_tournaments_from_snapshot(
                legacy_snapshot,
                (req.params.get("description") or "").strip() or None,
                tournament_code,
                cities,
                states,
                (req.params.get("date_from") or "").strip() or None,
                (req.params.get("date_before") or "").strip() or None,
                limit,
                page=page,
            )
            return _json_response(
                _with_debug(
                    search_payload,
                    data_source="main_snapshot_fallback",
                    elapsed_ms=round((perf_counter() - started) * 1000, 1),
                )
            )
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        search_payload = explorer.search_tournaments(
            conn_str,
            (req.params.get("description") or "").strip() or None,
            tournament_code,
            cities,
            states,
            (req.params.get("date_from") or "").strip() or None,
            (req.params.get("date_before") or "").strip() or None,
            limit,
            page=page,
        )
        return _json_response(
            _with_debug(
                search_payload,
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return func.HttpResponse(f"Tournament search failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerFilterOptions")
@app.route(route="ratings-explorer/filter-options", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_filter_options(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerFilterOptions Azure Function endpoint."""
    started = perf_counter()
    try:
        filter_options = explorer.load_filter_options_snapshot()
        if filter_options:
            return _json_response(_with_debug(filter_options, data_source="filter_options_snapshot", elapsed_ms=round((perf_counter() - started) * 1000, 1)))
        snapshot, error = _load_snapshot_or_error()
        if error:
            return error
        if snapshot:
            return _json_response(_with_debug(explorer.build_filter_options_from_snapshot(snapshot), data_source="main_snapshot_fallback", elapsed_ms=round((perf_counter() - started) * 1000, 1)))
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        return _json_response(_with_debug(explorer.build_filter_options(conn_str), data_source="sql_live", elapsed_ms=round((perf_counter() - started) * 1000, 1)))
    except Exception as exc:
        return func.HttpResponse(f"Filter options failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerPlayer")
@app.route(route="ratings-explorer/player", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_player(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerPlayer Azure Function endpoint."""
    started = perf_counter()
    agaid_text = (req.params.get("agaid") or "").strip()
    recent_games_sgf_only = (req.params.get("recent_games_sgf_only") or "").strip().lower() in {"1", "true", "yes", "on"}
    recent_tournaments_page_text = (req.params.get("recent_tournaments_page") or "0").strip()
    recent_games_page_text = (req.params.get("recent_games_page") or "0").strip()
    opponents_page_text = (req.params.get("opponents_page") or "0").strip()
    opponents_sort = (req.params.get("opponents_sort") or "games").strip().lower()
    if not agaid_text.isdigit():
        return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
    if not recent_tournaments_page_text.isdigit() or not recent_games_page_text.isdigit() or not opponents_page_text.isdigit():
        return func.HttpResponse("Paging parameters must be non-negative integers.", status_code=400)
    if opponents_sort not in {"games", "latest"}:
        return func.HttpResponse("Query parameter 'opponents_sort' must be 'games' or 'latest'.", status_code=400)
    try:
        data_source = None
        recent_tournaments_page = int(recent_tournaments_page_text)
        recent_games_page = int(recent_games_page_text)
        opponents_page = int(opponents_page_text)
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        payload = explorer.get_player_detail(
            conn_str,
            int(agaid_text),
            recent_games_sgf_only=recent_games_sgf_only,
            recent_tournaments_page=recent_tournaments_page,
            recent_games_page=recent_games_page,
            opponents_page=opponents_page,
            opponents_sort=opponents_sort,
            include_context=False,
        )
        history_points = explorer.load_sql_rating_history(int(agaid_text))
        if payload:
            data_source = "sql_live"
        if not payload:
            return func.HttpResponse(f"No player found for AGAID {agaid_text}.", status_code=404)
        payload = dict(payload)
        payload["rating_history"] = payload.get("rating_history") or _history_payload_from_points(history_points or [])
        payload["news_articles"] = []
        payload["review_videos"] = []
        payload["data_source"] = data_source or "unknown"
        return _json_response(_with_debug(payload, data_source=data_source or "unknown", elapsed_ms=round((perf_counter() - started) * 1000, 1)))
    except Exception as exc:
        return func.HttpResponse(f"Player detail failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerPlayerContext")
@app.route(route="ratings-explorer/player-context", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_player_context(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerPlayerContext Azure Function endpoint."""
    started = perf_counter()
    agaid_text = (req.params.get("agaid") or "").strip()
    if not agaid_text.isdigit():
        return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
    context_page_text = (req.params.get("context_page") or "0").strip()
    try:
        context_page = max(0, int(context_page_text or "0"))
    except ValueError:
        return func.HttpResponse("Query parameter 'context_page' must be numeric.", status_code=400)
    try:
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        agaid = int(agaid_text)
        context_payload = explorer.load_player_context_items(conn_str, agaid, page=context_page)
        return _json_response(
            _with_debug(
                {
                    "context_items": context_payload.get("items", []),
                    "context_paging": context_payload.get("paging", {}),
                    "news_articles": explorer.load_player_articles(conn_str, agaid),
                    "review_videos": explorer.load_player_review_videos(conn_str, agaid),
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return func.HttpResponse(f"Player context failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerTournament")
@app.route(route="ratings-explorer/tournament", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_tournament(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerTournament Azure Function endpoint."""
    started = perf_counter()
    tournament_code = (req.params.get("tournament_code") or "").strip()
    if not tournament_code:
        return func.HttpResponse("Query parameter 'tournament_code' is required.", status_code=400)
    try:
        data_source = None
        payload = explorer.load_tournament_detail_snapshot(tournament_code)
        if payload and not _tournament_detail_has_game_sgf_metadata(payload):
            payload = None
        if payload:
            data_source = "tournament_detail_snapshot"
        conn_str, error = _get_conn_str_or_error()
        if not payload and not error:
            payload = explorer.get_tournament_detail(conn_str, tournament_code)
            if payload:
                data_source = "sql_live"
        if not payload:
            if error:
                return error
            return func.HttpResponse(f"No tournament found for code '{tournament_code}'.", status_code=404)
        payload = dict(payload)
        payload["data_source"] = data_source or "unknown"
        return _json_response(_with_debug(payload, data_source=data_source or "unknown", elapsed_ms=round((perf_counter() - started) * 1000, 1)))
    except Exception as exc:
        return func.HttpResponse(f"Tournament detail failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerGameSgf")
@app.route(route="ratings-explorer/game-sgf", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_game_sgf(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerGameSgf Azure Function endpoint."""
    game_id_text = (req.params.get("game_id") or "").strip()
    if not game_id_text.isdigit():
        return func.HttpResponse("Query parameter 'game_id' must be numeric.", status_code=400)
    try:
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        sgf_code, sgf_text = explorer.load_game_sgf_text(conn_str, int(game_id_text))
        if not sgf_code:
            return func.HttpResponse(f"No SGF is linked for game {game_id_text}.", status_code=404)
        if sgf_text is None:
            return func.HttpResponse(f"SGF blob '{sgf_code}' was not found.", status_code=404)
        return func.HttpResponse(
            sgf_text,
            status_code=200,
            headers=explorer.response_headers("application/x-go-sgf; charset=utf-8"),
        )
    except Exception as exc:
        return func.HttpResponse(f"SGF lookup failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerGameSgfViewer")
@app.route(route="ratings-explorer/game-sgf-viewer", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_game_sgf_viewer(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerGameSgfViewer Azure Function endpoint."""
    game_id_text = (req.params.get("game_id") or "").strip()
    if not game_id_text.isdigit():
        return func.HttpResponse("Query parameter 'game_id' must be numeric.", status_code=400)
    game_id = int(game_id_text)
    mobile = (req.params.get("mobile") or "").strip().lower() in {"1", "true", "yes", "on"}
    sgf_url = f"/api/ratings-explorer/game-sgf?game_id={game_id}"
    page = explorer.render_game_sgf_viewer_html(game_id, sgf_url, mobile=mobile)
    return func.HttpResponse(
        page,
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="RatingsExplorerAsset")
@app.route(route="ratings-explorer/assets/{*asset_path}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_asset(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerAsset Azure Function endpoint."""
    asset_path = (req.route_params.get("asset_path") or "").strip()
    if not asset_path:
        return func.HttpResponse("Asset path is required.", status_code=400)
    payload, content_type = explorer.get_asset_bytes(asset_path)
    if payload is None or content_type is None:
        return func.HttpResponse(f"Asset '{asset_path}' was not found.", status_code=404)
    return func.HttpResponse(
        body=payload,
        status_code=200,
        headers=explorer.response_headers(content_type),
    )


@app.function_name(name="RatingsExplorerPlayerHistorySvg")
@app.route(route="ratings-explorer/player-history.svg", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_player_history_svg(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerPlayerHistorySvg Azure Function endpoint."""
    agaid_text = (req.params.get("agaid") or "").strip()
    if not agaid_text.isdigit():
        return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
    try:
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        history = explorer.load_sql_rating_history(int(agaid_text))
        member_name = explorer.load_member_name(conn_str, int(agaid_text))
        svg = explorer.render_single_history_svg(
            int(agaid_text),
            history,
            member_name,
        )
        return func.HttpResponse(
            svg,
            status_code=200,
            headers=explorer.response_headers("image/svg+xml; charset=utf-8"),
        )
    except ValueError as exc:
        return func.HttpResponse(str(exc), status_code=404)
    except Exception as exc:
        return func.HttpResponse(f"History chart generation failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerSnapshotStatus")
@app.route(route="ratings-explorer/snapshot-status", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_snapshot_status(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerSnapshotStatus Azure Function endpoint."""
    started = perf_counter()
    status = explorer.load_snapshot_status()
    snapshot_meta = (status or {}).get("snapshot_meta")
    if snapshot_meta:
        return _json_response(
            _with_debug(
                {
                    "snapshot_available": True,
                    "meta": snapshot_meta,
                    "job_status": status,
                },
                data_source="snapshot_status",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    return _json_response(_with_debug({"snapshot_available": False, "meta": None, "job_status": status}, data_source="none", elapsed_ms=round((perf_counter() - started) * 1000, 1)))


@app.function_name(name="RatingsExplorerSnapshotWarm")
@app.route(route="ratings-explorer/snapshot-warm", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_snapshot_warm(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerSnapshotWarm Azure Function endpoint."""
    started = perf_counter()
    startup = explorer.load_startup_players()
    player_search = explorer.load_player_search_snapshot()
    tournament_search = explorer.load_tournament_search_snapshot()
    filter_options = explorer.load_filter_options_snapshot()
    ok = bool(startup and player_search and tournament_search and filter_options)
    return _json_response(
        _with_debug(
            {
                "ok": ok,
                "snapshot_available": ok,
            },
            data_source="small_artifacts" if ok else "none",
            elapsed_ms=round((perf_counter() - started) * 1000, 1),
        )
    )


@app.function_name(name="RatingsExplorerSnapshotRefresh")
@app.route(route="ratings-explorer/snapshot-refresh", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def ratings_explorer_snapshot_refresh(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the RatingsExplorerSnapshotRefresh Azure Function endpoint."""
    requested_at = datetime.now(timezone.utc).isoformat()
    explorer.request_snapshot_refresh("http", requested_at)
    explorer.update_snapshot_status(
        "queued",
        source="http",
        detail="Snapshot refresh requested and waiting for timer pickup.",
        error=None,
    )
    return _json_response({"ok": True, "queued": True, "requested_at": requested_at})


@app.function_name(name="RatingsExplorerNightlySnapshot")
@app.schedule(schedule="0 15 6 * * *", arg_name="timer", run_on_startup=False, use_monitor=True)
def ratings_explorer_nightly_snapshot(timer: func.TimerRequest) -> None:
    """Handle the RatingsExplorerNightlySnapshot Azure Function endpoint."""
    conn_str = SQL_CONNECTION_STRING
    explorer.update_snapshot_status("running", source="timer", detail="Nightly snapshot refresh started.", error=None)
    try:
        snapshot = explorer.refresh_snapshot(conn_str)
        explorer.update_snapshot_status(
            "completed",
            source="timer",
            detail="Nightly snapshot refresh completed.",
            error=None,
            snapshot_meta=snapshot.get("meta", {}),
        )
    except Exception as exc:
        explorer.update_snapshot_status(
            "failed",
            source="timer",
            detail="Nightly snapshot refresh failed.",
            error=str(exc),
        )
        raise


@app.function_name(name="RatingsExplorerPendingSnapshotRefresh")
@app.schedule(schedule="0 */5 * * * *", arg_name="timer", run_on_startup=False, use_monitor=True)
def ratings_explorer_pending_snapshot_refresh(timer: func.TimerRequest) -> None:
    """Handle the RatingsExplorerPendingSnapshotRefresh Azure Function endpoint."""
    request = explorer.load_snapshot_request()
    if not request:
        return
    conn_str = SQL_CONNECTION_STRING
    try:
        explorer.update_snapshot_status(
            "running",
            source="manual-timer",
            detail=f"Pending snapshot refresh started from {request.get('source')}.",
            error=None,
        )
        snapshot = explorer.refresh_snapshot(conn_str)
        explorer.clear_snapshot_request()
        explorer.update_snapshot_status(
            "completed",
            source="manual-timer",
            detail="Pending snapshot refresh completed.",
            error=None,
            snapshot_meta=snapshot.get("meta", {}),
        )
    except Exception as exc:
        explorer.update_snapshot_status(
            "failed",
            source="manual-timer",
            detail="Pending snapshot refresh failed.",
            error=str(exc),
        )
        raise
