import json
import os
from datetime import date, datetime, timezone
from time import perf_counter

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


def _with_debug(payload: dict, **debug_fields) -> dict:
    enriched = dict(payload)
    enriched["_debug"] = {key: value for key, value in debug_fields.items() if value is not None}
    return enriched

def _load_snapshot_or_error() -> tuple[dict | None, func.HttpResponse | None]:
    if (os.environ.get("RATINGS_EXPLORER_DISABLE_SNAPSHOT") or "").strip().lower() in {"1", "true", "yes", "on"}:
        return None, None
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
                    data_source="main_snapshot",
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
        snapshot, error = _load_snapshot_or_error()
        if error:
            return error
        if snapshot:
            startup_payload = explorer.build_startup_players_payload(snapshot, limit=limit)
            return _json_response(
                _with_debug(
                    {
                        "results": startup_payload.get("results") or [],
                        "meta": startup_payload.get("meta") or {},
                    },
                    data_source="main_snapshot",
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
                _with_debug(
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
                    },
                    data_source="main_snapshot",
                    elapsed_ms=round((perf_counter() - started) * 1000, 1),
                )
            )
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        return _json_response(
            _with_debug(
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
                },
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
    snapshot, error = _load_snapshot_or_error()
    if error:
        return error
    try:
        if snapshot:
            return _json_response(_with_debug(explorer.build_filter_options_from_snapshot(snapshot), data_source="main_snapshot", elapsed_ms=round((perf_counter() - started) * 1000, 1)))
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
    snapshot, error = _load_snapshot_or_error()
    if error:
        return error
    agaid_text = (req.params.get("agaid") or "").strip()
    recent_games_sgf_only = (req.params.get("recent_games_sgf_only") or "").strip().lower() in {"1", "true", "yes", "on"}
    recent_tournaments_page_text = (req.params.get("recent_tournaments_page") or "0").strip()
    recent_games_page_text = (req.params.get("recent_games_page") or "0").strip()
    if not agaid_text.isdigit():
        return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
    if not recent_tournaments_page_text.isdigit() or not recent_games_page_text.isdigit():
        return func.HttpResponse("Paging parameters must be non-negative integers.", status_code=400)
    try:
        history_points = None
        data_source = None
        recent_tournaments_page = int(recent_tournaments_page_text)
        recent_games_page = int(recent_games_page_text)
        use_default_player_pages = recent_tournaments_page == 0 and recent_games_page == 0
        payload = (
            explorer.get_player_detail_from_snapshot(snapshot, int(agaid_text))
            if snapshot and explorer.snapshot_supports_player_member_type(snapshot) and not recent_games_sgf_only and use_default_player_pages
            else None
        )
        payload_from_snapshot = bool(payload)
        if payload_from_snapshot:
            data_source = "main_snapshot"
        if payload and (
            not _player_detail_has_recent_game_handicap(payload)
            or not _player_detail_has_recent_game_sgf_metadata(payload)
            or not _player_detail_has_recent_game_rank_metadata(payload)
        ):
            payload = None
            payload_from_snapshot = False
        if not payload:
            conn_str, error = _get_conn_str_or_error()
            if error:
                return error
            payload = explorer.get_player_detail(
                conn_str,
                int(agaid_text),
                recent_games_sgf_only=recent_games_sgf_only,
                recent_tournaments_page=recent_tournaments_page,
                recent_games_page=recent_games_page,
                include_context=False,
            )
            history_points = explorer.load_sql_rating_history(int(agaid_text))
            if payload:
                data_source = "sql_live"
        if not payload:
            return func.HttpResponse(f"No player found for AGAID {agaid_text}.", status_code=404)
        if payload_from_snapshot:
            history_points = explorer.load_rating_history_from_snapshot(snapshot, int(agaid_text))
        payload = dict(payload)
        if payload_from_snapshot:
            player_meta = payload.get("player") or {}
            recent_tournaments = payload.get("recent_tournaments") or []
            recent_games = payload.get("recent_games") or []
            payload["recent_tournaments_paging"] = {
                "page": recent_tournaments_page,
                "page_size": 12,
                "total_count": int(player_meta.get("tournament_count") or 0),
                "has_previous": recent_tournaments_page > 0,
                "has_next": len(recent_tournaments) < int(player_meta.get("tournament_count") or 0),
            }
            payload["recent_games_paging"] = {
                "page": recent_games_page,
                "page_size": 20,
                "total_count": int(player_meta.get("game_count") or 0),
                "has_previous": recent_games_page > 0,
                "has_next": len(recent_games) < int(player_meta.get("game_count") or 0),
            }
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
            snapshot = explorer.load_snapshot()
            if snapshot:
                payload = explorer.get_tournament_detail_from_snapshot(snapshot, tournament_code)
                if payload and not _tournament_detail_has_game_sgf_metadata(payload):
                    payload = None
                if payload:
                    data_source = "main_snapshot_fallback"
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
    sgf_url = f"/api/ratings-explorer/game-sgf?game_id={game_id}"
    page = explorer.render_game_sgf_viewer_html(game_id, sgf_url)
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
    snapshot = explorer.load_snapshot()
    return _json_response(
        _with_debug(
            {
                "ok": bool(snapshot),
                "snapshot_available": bool(snapshot),
            },
            data_source="main_snapshot" if snapshot else "none",
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
