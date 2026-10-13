import json
from datetime import date, datetime, timezone

import azure.functions as func

import ratings_explorer_support as explorer

app = func.FunctionApp()
ALLOWED_SEARCH_LIMITS = {10, 25, 50, 100, 250}
DEFAULT_SEARCH_LIMIT = 25
ALLOWED_ACTIVITY_YEARS = {1, 3, 5, 10}
DEFAULT_RECENT_ACTIVITY_YEARS = 3
ALLOWED_RATING_BANDS = {item["value"] for item in explorer.RATING_FILTER_OPTIONS}
ALLOWED_PLAYER_STATUS_FILTERS = {"all", "active", "expired"}


def _json_response(payload: dict) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload),
        status_code=200,
        headers=explorer.response_headers("application/json; charset=utf-8"),
    )


def _empty_response(status_code: int = 204) -> func.HttpResponse:
    return func.HttpResponse(status_code=status_code, headers=explorer.response_headers("text/plain; charset=utf-8"))


def _load_snapshot_or_error() -> tuple[dict | None, func.HttpResponse | None]:
    snapshot = explorer.load_snapshot()
    if snapshot:
        return snapshot, None
    return None, None


def _get_conn_str_or_error() -> tuple[str | None, func.HttpResponse | None]:
    conn_str = explorer.get_sql_connection_string()
    if not conn_str:
        return None, func.HttpResponse("Missing SQL connection string.", status_code=500)
    return conn_str, None


def _player_detail_has_recent_game_handicap(payload: dict | None) -> bool:
    if not payload:
        return False
    recent_games = payload.get("recent_games") or []
    if not recent_games:
        return True
    return all("handicap" in game for game in recent_games)


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


def _parse_json_body(req: func.HttpRequest) -> tuple[dict | None, func.HttpResponse | None]:
    try:
        payload = req.get_json()
    except ValueError:
        return None, func.HttpResponse("Request body must be valid JSON.", status_code=400)
    if not isinstance(payload, dict):
        return None, func.HttpResponse("Request body must be a JSON object.", status_code=400)
    return payload, None


def _normalize_sgf_text(raw_value: object) -> str:
    text = str(raw_value or "").replace("\ufeff", "").strip()
    return text


def _validate_sgf_text(sgf_text: str) -> func.HttpResponse | None:
    if not sgf_text:
        return func.HttpResponse("Field 'sgf_text' is required.", status_code=400)
    if "(;" not in sgf_text or ")" not in sgf_text:
        return func.HttpResponse("Field 'sgf_text' does not look like valid SGF.", status_code=400)
    return None


@app.function_name(name="RatingsExplorerPage")
@app.route(route="ratings-explorer", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_page(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        explorer.load_ratings_explorer_html(""),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="RatingsExplorerPlayers")
@app.route(route="ratings-explorer/players", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_players(req: func.HttpRequest) -> func.HttpResponse:
    snapshot, error = _load_snapshot_or_error()
    if error:
        return error
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
    agaid = None
    if agaid_text:
        if not agaid_text.isdigit():
            return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
        agaid = int(agaid_text)
    try:
        if snapshot and explorer.snapshot_supports_player_member_type(snapshot):
            return _json_response(
                {
                    "results": explorer.search_players_from_snapshot(
                        snapshot,
                        agaid,
                        (req.params.get("first_name") or "").strip() or None,
                        (req.params.get("last_name") or "").strip() or None,
                        chapters,
                        states,
                        member_types,
                        status_filter,
                        recent_activity_cutoff,
                        rating_bands,
                        limit,
                    )
                }
            )
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        return _json_response(
            {
                "results": explorer.search_players(
                    conn_str,
                    agaid,
                    (req.params.get("first_name") or "").strip() or None,
                    (req.params.get("last_name") or "").strip() or None,
                    chapters,
                    states,
                    member_types,
                    status_filter,
                    recent_activity_cutoff,
                    rating_bands,
                    limit,
                )
            }
        )
    except Exception as exc:
        return func.HttpResponse(f"Player search failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerTournaments")
@app.route(route="ratings-explorer/tournaments", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_tournaments(req: func.HttpRequest) -> func.HttpResponse:
    snapshot, error = _load_snapshot_or_error()
    if error:
        return error
    limit, error = _parse_search_limit(req)
    if error:
        return error
    try:
        tournament_code = (req.params.get("tournament_code") or "").strip() or None
        cities = _parse_csv_values(req, "cities", "city")
        states = _parse_csv_values(req, "states", "state")
        if snapshot:
            return _json_response(
                {
                    "results": explorer.search_tournaments_from_snapshot(
                        snapshot,
                        (req.params.get("description") or "").strip() or None,
                        tournament_code,
                        cities,
                        states,
                        (req.params.get("date_from") or "").strip() or None,
                        (req.params.get("date_before") or "").strip() or None,
                        limit,
                    )
                }
            )
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        return _json_response(
            {
                "results": explorer.search_tournaments(
                    conn_str,
                    (req.params.get("description") or "").strip() or None,
                    tournament_code,
                    cities,
                    states,
                    (req.params.get("date_from") or "").strip() or None,
                    (req.params.get("date_before") or "").strip() or None,
                    limit,
                )
            }
        )
    except Exception as exc:
        return func.HttpResponse(f"Tournament search failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerFilterOptions")
@app.route(route="ratings-explorer/filter-options", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_filter_options(req: func.HttpRequest) -> func.HttpResponse:
    snapshot, error = _load_snapshot_or_error()
    if error:
        return error
    try:
        if snapshot:
            return _json_response(explorer.build_filter_options_from_snapshot(snapshot))
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        return _json_response(explorer.build_filter_options(conn_str))
    except Exception as exc:
        return func.HttpResponse(f"Filter options failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerPlayer")
@app.route(route="ratings-explorer/player", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_player(req: func.HttpRequest) -> func.HttpResponse:
    snapshot, error = _load_snapshot_or_error()
    if error:
        return error
    agaid_text = (req.params.get("agaid") or "").strip()
    if not agaid_text.isdigit():
        return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
    try:
        history_points = None
        payload = (
            explorer.get_player_detail_from_snapshot(snapshot, int(agaid_text))
            if snapshot and explorer.snapshot_supports_player_member_type(snapshot)
            else None
        )
        payload_from_snapshot = bool(payload)
        if payload and not _player_detail_has_recent_game_handicap(payload):
            payload = None
            payload_from_snapshot = False
        if not payload:
            conn_str, error = _get_conn_str_or_error()
            if error:
                return error
            payload = explorer.get_player_detail(conn_str, int(agaid_text))
            history_points = explorer.load_sql_rating_history(int(agaid_text))
        if not payload:
            return func.HttpResponse(f"No player found for AGAID {agaid_text}.", status_code=404)
        if payload_from_snapshot:
            history_points = explorer.load_rating_history_from_snapshot(snapshot, int(agaid_text))
        payload = dict(payload)
        payload["rating_history"] = payload.get("rating_history") or _history_payload_from_points(history_points or [])
        conn_str, _ = _get_conn_str_or_error()
        payload = explorer.attach_player_articles(conn_str, payload, int(agaid_text))
        return _json_response(payload)
    except Exception as exc:
        return func.HttpResponse(f"Player detail failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerTournament")
@app.route(route="ratings-explorer/tournament", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_tournament(req: func.HttpRequest) -> func.HttpResponse:
    tournament_code = (req.params.get("tournament_code") or "").strip()
    if not tournament_code:
        return func.HttpResponse("Query parameter 'tournament_code' is required.", status_code=400)
    try:
        data_source = None
        payload = explorer.load_tournament_detail_snapshot(tournament_code)
        if payload:
            data_source = "tournament_detail_snapshot"
        conn_str, error = _get_conn_str_or_error()
        if not payload and not error:
            payload = explorer.get_tournament_detail(conn_str, tournament_code)
            if payload:
                data_source = "sql_live"
        if not payload:
            snapshot = explorer.load_snapshot()
            if snapshot:
                payload = explorer.get_tournament_detail_from_snapshot(snapshot, tournament_code)
                if payload:
                    data_source = "main_snapshot_fallback"
        if not payload:
            if error:
                return error
            return func.HttpResponse(f"No tournament found for code '{tournament_code}'.", status_code=404)
        payload = dict(payload)
        payload["data_source"] = data_source or "unknown"
        return _json_response(payload)
    except Exception as exc:
        return func.HttpResponse(f"Tournament detail failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerSgfUpload")
@app.route(route="ratings-explorer/sgf-upload", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.FUNCTION)
def ratings_explorer_sgf_upload(req: func.HttpRequest) -> func.HttpResponse:
    if req.method.upper() == "OPTIONS":
        return _empty_response()
    payload, error = _parse_json_body(req)
    if error:
        return error
    game_id_text = str(payload.get("game_id") or "").strip()
    if not game_id_text.isdigit():
        return func.HttpResponse("Field 'game_id' must be numeric.", status_code=400)
    sgf_text = _normalize_sgf_text(payload.get("sgf_text"))
    error = _validate_sgf_text(sgf_text)
    if error:
        return error
    conn_str, error = _get_conn_str_or_error()
    if error:
        return error
    try:
        game = explorer.load_game_for_sgf_upload(conn_str, int(game_id_text))
        if not game:
            return func.HttpResponse(f"No game found for Game_ID {game_id_text}.", status_code=404)
        explorer.update_game_sgf(conn_str, int(game_id_text), sgf_text)
        updated = explorer.load_game_for_sgf_upload(conn_str, int(game_id_text))
        return _json_response(
            {
                "ok": True,
                "message": f"Uploaded SGF for game {game_id_text}.",
                "game": explorer.sgf_upload_game_payload(updated or game),
            }
        )
    except Exception as exc:
        return func.HttpResponse(f"SGF upload failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerPlayerHistorySvg")
@app.route(route="ratings-explorer/player-history.svg", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_player_history_svg(req: func.HttpRequest) -> func.HttpResponse:
    agaid_text = (req.params.get("agaid") or "").strip()
    if not agaid_text.isdigit():
        return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
    snapshot, error = _load_snapshot_or_error()
    if error:
        return error
    try:
        if snapshot:
            history = explorer.load_rating_history_from_snapshot(snapshot, int(agaid_text))
            member_name = explorer.load_member_name_from_snapshot(snapshot, int(agaid_text))
        else:
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
    snapshot = explorer.load_snapshot()
    status = explorer.load_snapshot_status()
    if snapshot:
        return _json_response(
            {
                "snapshot_available": True,
                "meta": snapshot.get("meta", {}),
                "job_status": status,
            }
        )
    return _json_response({"snapshot_available": False, "meta": None, "job_status": status})


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
    conn_str = explorer.get_sql_connection_string()
    if not conn_str:
        explorer.update_snapshot_status(
            "failed",
            source="timer",
            detail="Nightly snapshot skipped.",
            error="Missing SQL connection string.",
        )
        return
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
    conn_str = explorer.get_sql_connection_string()
    if not conn_str:
        explorer.update_snapshot_status(
            "failed",
            source="manual-timer",
            detail="Pending snapshot refresh skipped.",
            error="Missing SQL connection string.",
        )
        return
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
