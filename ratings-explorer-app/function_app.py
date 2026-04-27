import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from time import perf_counter
from urllib.parse import quote, urlsplit

import azure.functions as func

import ratings_explorer_support as explorer

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

try:
    from bayrate.auth import authorize_bayrate_admin
    from bayrate.commit_staged_run import build_commit_plan, printable_commit_plan
    from bayrate.sql_adapter import SqlAdapter
    from bayrate.replay_staged_run import run_staged_replay
    from bayrate.stage_reports import (
        apply_tournament_review_decision,
        build_insert_statements,
        build_staging_payload,
        ensure_payload_run_id,
        explain_staged_run_review,
        load_staged_run,
        printable_payload,
        update_staged_run_review,
    )
except Exception:
    authorize_bayrate_admin = None
    build_commit_plan = None
    printable_commit_plan = None
    SqlAdapter = None
    run_staged_replay = None
    apply_tournament_review_decision = None
    build_insert_statements = None
    build_staging_payload = None
    ensure_payload_run_id = None
    explain_staged_run_review = None
    load_staged_run = None
    printable_payload = None
    update_staged_run_review = None

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
    return func.HttpResponse(
        json.dumps(payload),
        status_code=200,
        headers=explorer.response_headers("application/json; charset=utf-8"),
    )


def _with_debug(payload: dict, **debug_fields) -> dict:
    enriched = dict(payload)
    enriched["_debug"] = {key: value for key, value in debug_fields.items() if value is not None}
    return enriched


def _bayrate_json_response(payload: dict, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload, default=explorer.json_safe_value),
        status_code=status_code,
        headers=explorer.response_headers("application/json; charset=utf-8"),
    )


def _bayrate_preview_error(message: str, status_code: int = 400) -> func.HttpResponse:
    return _bayrate_json_response({"ok": False, "error": message}, status_code=status_code)


def _bayrate_modules_available(*names: str) -> bool:
    modules = {
        "stage": build_staging_payload is not None and printable_payload is not None,
        "write": build_insert_statements is not None and ensure_payload_run_id is not None,
        "review": (
            load_staged_run is not None
            and apply_tournament_review_decision is not None
            and update_staged_run_review is not None
        ),
        "replay": run_staged_replay is not None,
        "commit": build_commit_plan is not None and printable_commit_plan is not None,
    }
    return all(modules.get(name, False) for name in names)


def _bayrate_adapter_or_error() -> tuple[object | None, func.HttpResponse | None]:
    if SqlAdapter is None:
        return None, _bayrate_preview_error("BayRate SQL adapter is not available in this deployment.", status_code=500)
    return SqlAdapter(SQL_CONNECTION_STRING), None


def _bayrate_login_redirect(req: func.HttpRequest) -> str:
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
    try:
        body = req.get_json()
    except ValueError:
        return None, _bayrate_preview_error("Request body must be JSON.")
    if not isinstance(body, dict):
        return None, _bayrate_preview_error("Request body must be a JSON object.")
    return body, None


def _bayrate_report_inputs_from_body(body: dict) -> tuple[list[tuple[str, str]] | None, func.HttpResponse | None]:
    reports = body.get("reports")
    if not isinstance(reports, list) or not reports:
        return None, _bayrate_preview_error("At least one report is required.")
    if len(reports) > 20:
        return None, _bayrate_preview_error("At most 20 reports can be previewed at once.")

    report_inputs = []
    for index, item in enumerate(reports, start=1):
        if not isinstance(item, dict):
            return None, _bayrate_preview_error(f"Report {index} must be an object.")
        source_name = str(item.get("source_name") or f"pasted-report-{index}.txt").strip()
        content = str(item.get("content") or "")
        if not content.strip():
            return None, _bayrate_preview_error(f"Report {index} is empty.")
        if len(content) > 500_000:
            return None, _bayrate_preview_error(f"Report {index} is too large.")
        report_inputs.append((source_name, content))
    return report_inputs, None


def _bayrate_payload_response(payload: dict, *, adapter: object | None = None, written: bool = False) -> dict:
    explanations = explain_staged_run_review(adapter, payload) if (adapter and explain_staged_run_review is not None) else None
    summary = printable_payload(payload, include_games=False)
    summary["written"] = written
    return {
        "ok": True,
        "written": written,
        "summary": summary,
        "same_date_groups": _bayrate_same_date_groups(payload),
        "review_explanation": explanations,
    }


def _bayrate_replay_response(artifact: dict) -> dict:
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

def _load_snapshot_or_error() -> tuple[dict | None, func.HttpResponse | None]:
    if (os.environ.get("RATINGS_EXPLORER_DISABLE_SNAPSHOT") or "").strip().lower() in {"1", "true", "yes", "on"}:
        return None, None
    snapshot = explorer.load_snapshot()
    if snapshot:
        return snapshot, None
    return None, None


def _get_conn_str_or_error() -> tuple[str | None, func.HttpResponse | None]:
    return SQL_CONNECTION_STRING, None


def _player_detail_has_recent_game_handicap(payload: dict | None) -> bool:
    if not payload:
        return False
    recent_games = payload.get("recent_games") or []
    if not recent_games:
        return True
    return all("handicap" in game for game in recent_games)


def _player_detail_has_recent_game_sgf_metadata(payload: dict | None) -> bool:
    if not payload:
        return False
    recent_games = payload.get("recent_games") or []
    if not recent_games:
        return True
    return all("game_id" in game and "has_sgf" in game for game in recent_games)


def _player_detail_has_recent_game_rank_metadata(payload: dict | None) -> bool:
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
    if not payload:
        return False
    games = payload.get("games") or []
    if not games:
        return True
    return all("has_sgf" in game for game in games)


def _history_payload_from_points(history: list[tuple[datetime, float, float]]) -> list[dict]:
    return explorer.serialize_rating_history(history)


def _parse_search_limit(req: func.HttpRequest) -> tuple[int | None, func.HttpResponse | None]:
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
    raw_text = (req.params.get(name) or "").strip()
    if not raw_text:
        return default, None
    if not raw_text.isdigit():
        return None, func.HttpResponse(f"Query parameter '{name}' must be a non-negative integer.", status_code=400)
    return int(raw_text), None


def _years_ago_iso(years: int, today: date) -> str:
    try:
        return today.replace(year=today.year - years).isoformat()
    except ValueError:
        return today.replace(month=2, day=28, year=today.year - years).isoformat()


def _parse_recent_activity_cutoff(req: func.HttpRequest) -> tuple[str | None, func.HttpResponse | None]:
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
    status_filter = (req.params.get("status") or "").strip().lower() or "all"
    if status_filter not in ALLOWED_PLAYER_STATUS_FILTERS:
        return "all", func.HttpResponse(
            "Query parameter 'status' must be one of All, Active, or Expired.",
            status_code=400,
        )
    return status_filter, None


def _parse_csv_values(req: func.HttpRequest, key: str, legacy_key: str | None = None) -> list[str] | None:
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


@app.function_name(name="RatingsExplorerPage")
@app.route(route="ratings-explorer", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_page(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        explorer.load_ratings_explorer_html(""),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="RatingsExplorerMobilePage")
@app.route(route="ratings-explorer/mobile", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_mobile_page(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        explorer.load_ratings_explorer_html("", "ratings_explorer_mobile.html"),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="BayRateStagingPage")
@app.route(route="ratings-explorer/bayrate", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_page(req: func.HttpRequest) -> func.HttpResponse:
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter, html=True)
    if auth_error:
        return auth_error
    return func.HttpResponse(
        explorer.load_ratings_explorer_html("", "bayrate_staging.html"),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="BayRateStagingPreview")
@app.route(route="ratings-explorer/bayrate/preview", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_preview(req: func.HttpRequest) -> func.HttpResponse:
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
    report_inputs, error = _bayrate_report_inputs_from_body(body)
    if error:
        return error

    duplicate_check = bool(body.get("duplicate_check", True))
    try:
        payload = build_staging_payload(
            report_inputs,
            adapter=adapter,
            duplicate_check=duplicate_check,
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


@app.function_name(name="BayRateStagingWrite")
@app.route(route="ratings-explorer/bayrate/stage", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_write(req: func.HttpRequest) -> func.HttpResponse:
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
    report_inputs, error = _bayrate_report_inputs_from_body(body)
    if error:
        return error
    try:
        payload = build_staging_payload(report_inputs, adapter=adapter, duplicate_check=True)
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
    try:
        payload = load_staged_run(adapter, run_id)
        apply_tournament_review_decision(
            payload,
            source_report_ordinal,
            use_duplicate_code=bool(body.get("use_duplicate_code", False)),
            mark_ready=bool(body.get("mark_ready", False)),
            operator_note=str(body.get("operator_note") or "").strip() or None,
        )
        update_staged_run_review(adapter, payload)
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)
    return _bayrate_json_response(_bayrate_payload_response(payload, adapter=adapter, written=True))


@app.function_name(name="BayRateStagingReplay")
@app.route(route="ratings-explorer/bayrate/replay", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_replay(req: func.HttpRequest) -> func.HttpResponse:
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
    return _bayrate_json_response(_bayrate_replay_response(artifact))


@app.function_name(name="BayRateStagingCommitPreview")
@app.route(route="ratings-explorer/bayrate/commit-preview", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_commit_preview(req: func.HttpRequest) -> func.HttpResponse:
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
    return _bayrate_json_response({"ok": True, "commit_plan": printable_commit_plan(plan)})


@app.function_name(name="RatingsExplorerPlayers")
@app.route(route="ratings-explorer/players", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_players(req: func.HttpRequest) -> func.HttpResponse:
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
    started = perf_counter()
    agaid_text = (req.params.get("agaid") or "").strip()
    if not agaid_text.isdigit():
        return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
    try:
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        agaid = int(agaid_text)
        return _json_response(
            _with_debug(
                {
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
