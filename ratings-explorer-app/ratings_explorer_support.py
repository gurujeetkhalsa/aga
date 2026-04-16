import html
import json
import math
import mimetypes
import os
import re
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import certifi
import pytds
try:
    from azure.storage.blob import BlobServiceClient
except Exception:
    BlobServiceClient = None
try:
    import pyodbc
except Exception:
    pyodbc = None
try:
    import wgo_embedded_assets
except Exception:
    wgo_embedded_assets = None

MAX_MEMBER_AGAID = 50000
SNAPSHOT_DIRNAME = "data"
ASSETS_DIRNAME = "assets"
SNAPSHOT_FILENAME = "ratings_explorer_snapshot.json"
SNAPSHOT_STATUS_FILENAME = "ratings_explorer_snapshot_status.json"
SNAPSHOT_REQUEST_FILENAME = "ratings_explorer_snapshot_request.json"
STARTUP_PLAYERS_FILENAME = "ratings_explorer_startup_players.json"
PLAYER_SEARCH_SNAPSHOT_FILENAME = "ratings_explorer_player_search_snapshot.json"
SNAPSHOT_TOURNAMENT_DETAIL_DIRNAME = "ratings_explorer_tournament_details"
SNAPSHOT_CONTAINER = "ratings-explorer"
SGF_CONTAINER = os.environ.get("RATINGS_SGF_CONTAINER") or "ratings-game-sgf"
SGF_VIEWER_REV = "20260415b"
SNAPSHOT_BLOB_NAME = "ratings-explorer-snapshot.json"
SNAPSHOT_STATUS_BLOB_NAME = "ratings-explorer-snapshot-status.json"
SNAPSHOT_REQUEST_BLOB_NAME = "ratings-explorer-snapshot-request.json"
STARTUP_PLAYERS_BLOB_NAME = "ratings-explorer-startup-players.json"
PLAYER_SEARCH_SNAPSHOT_BLOB_NAME = "ratings-explorer-player-search-snapshot.json"
SNAPSHOT_TOURNAMENT_DETAIL_BLOB_PREFIX = "tournament-details"
SNAPSHOT_CACHE_TTL_SECONDS = 15.0
SNAPSHOT_STATUS_CACHE_TTL_SECONDS = 5.0
SNAPSHOT_REQUEST_CACHE_TTL_SECONDS = 5.0
TOURNAMENT_DETAIL_CACHE_TTL_SECONDS = 30.0
STARTUP_PLAYERS_CACHE_TTL_SECONDS = 60.0
PLAYER_CONTEXT_CACHE_TTL_SECONDS = 60.0
PLAYER_RATING_HISTORY_CACHE_TTL_SECONDS = 60.0
GAME_SGF_CACHE_TTL_SECONDS = 60.0
_MEMORY_CACHE: dict[tuple[str, str], tuple[float, Any]] = {}


def _cache_get(namespace: str, key: str) -> Any:
    entry = _MEMORY_CACHE.get((namespace, key))
    if not entry:
        return None, False
    expires_at, value = entry
    if time.monotonic() >= expires_at:
        _MEMORY_CACHE.pop((namespace, key), None)
        return None, False
    return value, True


def _cache_set(namespace: str, key: str, value: Any, ttl_seconds: float) -> Any:
    _MEMORY_CACHE[(namespace, key)] = (time.monotonic() + max(0.0, ttl_seconds), value)
    return value


def _cache_delete(namespace: str, key: str) -> None:
    _MEMORY_CACHE.pop((namespace, key), None)


def _cache_delete_prefix(namespace: str) -> None:
    doomed = [cache_key for cache_key in _MEMORY_CACHE if cache_key[0] == namespace]
    for cache_key in doomed:
        _MEMORY_CACHE.pop(cache_key, None)


US_STATE_LABELS = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "AS": "American Samoa",
    "GU": "Guam",
    "MP": "Northern Mariana Islands",
    "PR": "Puerto Rico",
    "VI": "U.S. Virgin Islands",
}
RATING_FILTER_OPTIONS = [
    {"value": "8plus_dan", "label": "8+ dan", "min": 8.0, "max": None},
    {"value": "7_dan", "label": "7 dan", "min": 7.0, "max": 8.0},
    {"value": "6_dan", "label": "6 dan", "min": 6.0, "max": 7.0},
    {"value": "5_dan", "label": "5 dan", "min": 5.0, "max": 6.0},
    {"value": "4_dan", "label": "4 dan", "min": 4.0, "max": 5.0},
    {"value": "3_dan", "label": "3 dan", "min": 3.0, "max": 4.0},
    {"value": "2_dan", "label": "2 dan", "min": 2.0, "max": 3.0},
    {"value": "1_dan", "label": "1 dan", "min": 1.0, "max": 2.0},
    {"value": "1_kyu", "label": "1 kyu", "min": -2.0, "max": -1.0},
    {"value": "2_kyu", "label": "2 kyu", "min": -3.0, "max": -2.0},
    {"value": "3_kyu", "label": "3 kyu", "min": -4.0, "max": -3.0},
    {"value": "4_kyu", "label": "4 kyu", "min": -5.0, "max": -4.0},
    {"value": "5_kyu", "label": "5 kyu", "min": -6.0, "max": -5.0},
    {"value": "6_kyu", "label": "6 kyu", "min": -7.0, "max": -6.0},
    {"value": "7_kyu", "label": "7 kyu", "min": -8.0, "max": -7.0},
    {"value": "8_kyu", "label": "8 kyu", "min": -9.0, "max": -8.0},
    {"value": "9_kyu", "label": "9 kyu", "min": -10.0, "max": -9.0},
    {"value": "10_kyu", "label": "10 kyu", "min": -11.0, "max": -10.0},
    {"value": "11_kyu", "label": "11 kyu", "min": -12.0, "max": -11.0},
    {"value": "12_kyu", "label": "12 kyu", "min": -13.0, "max": -12.0},
    {"value": "13_kyu", "label": "13 kyu", "min": -14.0, "max": -13.0},
    {"value": "14_kyu", "label": "14 kyu", "min": -15.0, "max": -14.0},
    {"value": "15_kyu", "label": "15 kyu", "min": -16.0, "max": -15.0},
    {"value": "16_kyu", "label": "16 kyu", "min": -17.0, "max": -16.0},
    {"value": "17_kyu", "label": "17 kyu", "min": -18.0, "max": -17.0},
    {"value": "18_kyu", "label": "18 kyu", "min": -19.0, "max": -18.0},
    {"value": "19_kyu", "label": "19 kyu", "min": -20.0, "max": -19.0},
    {"value": "20plus_kyu", "label": "20+ kyu", "min": None, "max": -20.0},
]
RATING_FILTER_BY_VALUE = {item["value"]: item for item in RATING_FILTER_OPTIONS}
BLANK_STATE_VALUE = "__blank_state__"
BLANK_STATE_OPTION = {"value": BLANK_STATE_VALUE, "label": "blank"}
BLANK_CITY_VALUE = "__blank_city__"
BLANK_CITY_OPTION = {"value": BLANK_CITY_VALUE, "label": "blank"}
BLANK_CHAPTER_VALUE = "__blank_chapter__"
BLANK_CHAPTER_OPTION = {"value": BLANK_CHAPTER_VALUE, "label": "blank"}


def _app_root() -> Path:
    return Path(__file__).resolve().parent


def _candidate_app_roots() -> list[Path]:
    candidates: list[Path] = [_app_root()]
    cwd = Path.cwd()
    if cwd not in candidates:
        candidates.append(cwd)
    home = (os.environ.get("HOME") or "").strip()
    if home:
        wwwroot = Path(home) / "site" / "wwwroot"
        if wwwroot not in candidates:
            candidates.append(wwwroot)
    linux_wwwroot = Path("/home/site/wwwroot")
    if linux_wwwroot not in candidates:
        candidates.append(linux_wwwroot)
    parent = _app_root().parent
    if parent not in candidates:
        candidates.append(parent)
    return candidates


def _embedded_asset_text(relative_path: str) -> str | None:
    if wgo_embedded_assets is None:
        return None
    normalized = str(Path(relative_path)).replace("\\", "/").strip().lower()
    lookup = {
        "wgo/wgo.min.js": getattr(wgo_embedded_assets, "WGO_MIN_JS", None),
        "wgo/wgo.player.min.js": getattr(wgo_embedded_assets, "WGO_PLAYER_MIN_JS", None),
        "wgo/wgo.player.css": getattr(wgo_embedded_assets, "WGO_PLAYER_CSS", None),
    }
    value = lookup.get(normalized)
    return value if isinstance(value, str) and value else None


def get_asset_bytes(relative_path: str) -> tuple[bytes | None, str | None]:
    parts = [part for part in Path(relative_path).parts if part not in {"", "."}]
    if not parts or any(part == ".." for part in parts):
        return None, None
    embedded_text = _embedded_asset_text("/".join(parts))
    if embedded_text is not None:
        content_type = mimetypes.guess_type(parts[-1])[0] or "application/octet-stream"
        return embedded_text.encode("utf-8"), content_type
    for app_root in _candidate_app_roots():
        asset_root = app_root / ASSETS_DIRNAME
        path = asset_root.joinpath(*parts)
        try:
            resolved = path.resolve()
            resolved_asset_root = asset_root.resolve()
        except OSError:
            continue
        if resolved_asset_root not in resolved.parents and resolved != resolved_asset_root:
            continue
        if not resolved.is_file():
            continue
        content_type = mimetypes.guess_type(resolved.name)[0] or "application/octet-stream"
        try:
            return resolved.read_bytes(), content_type
        except OSError:
            continue
    return None, None


def get_asset_text(relative_path: str) -> str | None:
    payload, _ = get_asset_bytes(relative_path)
    if payload is None:
        return None
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError:
        return None


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


def get_sql_connection_string() -> str | None:
    conn = os.environ.get("SQL_CONNECTION_STRING")
    if conn:
        return conn
    for settings_path in (_app_root() / "local.settings.json", _app_root().parent / "local.settings.json"):
        if settings_path.exists():
            try:
                values = json.loads(settings_path.read_text(encoding="utf-8")).get("Values", {})
            except (OSError, json.JSONDecodeError):
                continue
            conn = values.get("SQL_CONNECTION_STRING")
            if conn:
                return conn
    return None


def _get_setting(name: str) -> str | None:
    value = os.environ.get(name)
    if value:
        return value
    for settings_path in (_app_root() / "local.settings.json", _app_root().parent / "local.settings.json"):
        if settings_path.exists():
            try:
                values = json.loads(settings_path.read_text(encoding="utf-8")).get("Values", {})
            except (OSError, json.JSONDecodeError):
                continue
            value = values.get(name)
            if value:
                return value
    return None


def response_headers(content_type: str) -> dict[str, str]:
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, X-Functions-Key, x-functions-key",
        "Cache-Control": "no-store",
        "Content-Type": content_type,
    }


def json_safe_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def rounded_rating(value: Any) -> float | None:
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def current_ratings_cte() -> str:
    return """
WITH current_ratings AS
(
    SELECT ranked.[AGAID], ranked.[Rating], ranked.[Sigma], ranked.[Elab_Date], ranked.[Tournament_Code]
    FROM
    (
        SELECT
            r.[Pin_Player] AS [AGAID],
            r.[Rating],
            r.[Sigma],
            r.[Elab_Date],
            r.[Tournament_Code],
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
"""


def query_rows(conn_str: str, query: str, params: list[Any] | tuple[Any, ...]) -> list[dict[str, Any]]:
    if pyodbc is not None:
        try:
            return _query_rows_via_odbc(conn_str, query, params)
        except Exception:
            pass
    return _query_rows_via_tds(conn_str, query, params)


def _query_rows_via_odbc(conn_str: str, query: str, params: list[Any] | tuple[Any, ...]) -> list[dict[str, Any]]:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(query, list(params))
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, record)) for record in cursor.fetchall()]
    finally:
        conn.close()


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (datetime, date)):
        return "'" + value.isoformat() + "'"
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _query_rows_via_tds(conn_str: str, query: str, params: list[Any] | tuple[Any, ...]) -> list[dict[str, Any]]:
    conn = _tds_connect(conn_str)
    try:
        cursor = conn.cursor()
        rendered_query = query
        for value in params:
            rendered_query = rendered_query.replace("?", _sql_literal(value), 1)
        cursor.execute(rendered_query)
        return list(cursor.fetchall())
    finally:
        conn.close()


def query_one(conn_str: str, query: str, params: list[Any] | tuple[Any, ...]) -> dict[str, Any] | None:
    rows = query_rows(conn_str, query, params)
    return rows[0] if rows else None


def execute_non_query(conn_str: str, query: str, params: list[Any] | tuple[Any, ...]) -> None:
    if pyodbc is not None:
        try:
            _execute_non_query_via_odbc(conn_str, query, params)
            return
        except Exception:
            pass
    _execute_non_query_via_tds(conn_str, query, params)


def _execute_non_query_via_odbc(conn_str: str, query: str, params: list[Any] | tuple[Any, ...]) -> None:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(query, list(params))
        conn.commit()
    finally:
        conn.close()


def _execute_non_query_via_tds(conn_str: str, query: str, params: list[Any] | tuple[Any, ...]) -> None:
    conn = _tds_connect(conn_str)
    try:
        cursor = conn.cursor()
        rendered_query = query
        for value in params:
            rendered_query = rendered_query.replace("?", _sql_literal(value), 1)
        cursor.execute(rendered_query)
    finally:
        conn.close()


def load_member_name(conn_str: str, agaid: int) -> str | None:
    row = query_one(
        conn_str,
        """
SELECT TOP (1) [FirstName], [LastName]
FROM [membership].[members]
WHERE [AGAID] = ?
""",
        [agaid],
    )
    if not row:
        return None
    first = (row.get("FirstName") or "").strip()
    last = (row.get("LastName") or "").strip()
    name = f"{first} {last}".strip()
    return name or None


def member_name_from_row(row: dict[str, Any]) -> str:
    first = (row.get("FirstName") or "").strip()
    last = (row.get("LastName") or "").strip()
    full_name = f"{first} {last}".strip()
    return full_name or f"AGAID {row.get('AGAID')}"


def chapter_label(row: dict[str, Any]) -> str | None:
    code = (row.get("ChapterCode") or "").strip()
    name = (row.get("ChapterName") or "").strip()
    if code and name:
        return f"{code} - {name}"
    return code or name or None


def member_type_label(value: Any) -> str | None:
    mapping = {
        "Adult Full": "Adult",
        "Adult Full - Lifetime": "Life",
        "Youth": "Youth",
        "Tournament Pass": "Pass",
        "Complimentary": "Comp",
    }
    text = (str(value).strip() if value is not None else "")
    return mapping.get(text) or None


def normalized_member_type_filter(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip().lower()
    mapping = {
        "adult": "Adult Full",
        "adult full": "Adult Full",
        "life": "Adult Full - Lifetime",
        "adult full - lifetime": "Adult Full - Lifetime",
        "youth": "Youth",
        "pass": "Tournament Pass",
        "tournament pass": "Tournament Pass",
        "comp": "Complimentary",
        "complimentary": "Complimentary",
    }
    return mapping.get(text)


def state_option(code: str | None) -> dict[str, str] | None:
    text = (code or "").strip().upper()
    name = US_STATE_LABELS.get(text)
    if not name:
        return None
    return {"value": text, "label": f"{text} - {name}"}


def state_filter_clause(column_name: str, values: list[str]) -> tuple[str | None, list[str]]:
    normalized = [value for value in values if value]
    if not normalized:
        return None, []
    include_blank = BLANK_STATE_VALUE in normalized
    concrete_values = [value for value in normalized if value != BLANK_STATE_VALUE]
    clauses: list[str] = []
    params: list[str] = []
    if concrete_values:
        placeholders = ", ".join("?" for _ in concrete_values)
        clauses.append(f"{column_name} IN ({placeholders})")
        params.extend(concrete_values)
    if include_blank:
        clauses.append(f"({column_name} IS NULL OR LTRIM(RTRIM({column_name})) = '')")
    if not clauses:
        return None, []
    if len(clauses) == 1:
        return clauses[0], params
    return "(" + " OR ".join(clauses) + ")", params


def city_filter_clause(column_name: str, values: list[str]) -> tuple[str | None, list[str]]:
    normalized = [value for value in values if value]
    if not normalized:
        return None, []
    include_blank = BLANK_CITY_VALUE in normalized
    concrete_values = [value for value in normalized if value != BLANK_CITY_VALUE]
    clauses: list[str] = []
    params: list[str] = []
    if concrete_values:
        placeholders = ", ".join("?" for _ in concrete_values)
        clauses.append(f"{column_name} IN ({placeholders})")
        params.extend(concrete_values)
    if include_blank:
        clauses.append(f"({column_name} IS NULL OR LTRIM(RTRIM({column_name})) = '')")
    if not clauses:
        return None, []
    if len(clauses) == 1:
        return clauses[0], params
    return "(" + " OR ".join(clauses) + ")", params


def chapter_filter_clause(values: list[str]) -> tuple[str | None, list[str]]:
    normalized = [value for value in values if value]
    if not normalized:
        return None, []
    include_blank = BLANK_CHAPTER_VALUE in normalized
    concrete_values = [value for value in normalized if value != BLANK_CHAPTER_VALUE]
    clauses: list[str] = []
    params: list[str] = []
    if concrete_values:
        placeholders = ", ".join("?" for _ in concrete_values)
        clauses.append(f"c.[ChapterCode] IN ({placeholders})")
        params.extend(concrete_values)
    if include_blank:
        clauses.append("c.[ChapterCode] IS NULL")
    if not clauses:
        return None, []
    if len(clauses) == 1:
        return clauses[0], params
    return "(" + " OR ".join(clauses) + ")", params


def _sql_in_clause(column_name: str, values: list[str]) -> tuple[str | None, list[str]]:
    normalized = [value for value in values if value]
    if not normalized:
        return None, []
    placeholders = ", ".join("?" for _ in normalized)
    return f"{column_name} IN ({placeholders})", normalized


def _normalize_rating_number(rating: Any) -> float | None:
    try:
        return float(rating)
    except (TypeError, ValueError):
        return None


def _rating_filter_matches_value(value: float, rating_filter: dict[str, Any]) -> bool:
    min_value = rating_filter.get("min")
    max_value = rating_filter.get("max")
    if min_value is not None and value < float(min_value):
        return False
    if max_value is not None and value >= float(max_value):
        return False
    return True


def rating_matches_band(rating: Any, rating_bands: list[str] | None) -> bool:
    if not rating_bands:
        return True
    value = _normalize_rating_number(rating)
    if value is None:
        return False
    for rating_band in rating_bands:
        rating_filter = RATING_FILTER_BY_VALUE.get(rating_band)
        if rating_filter and _rating_filter_matches_value(value, rating_filter):
            return True
    return False


def rating_band_sql_clause(rating_bands: list[str] | None, column_name: str) -> tuple[str | None, list[float]]:
    if not rating_bands:
        return None, []
    clauses: list[str] = []
    params: list[float] = []
    for rating_band in rating_bands:
        rating_filter = RATING_FILTER_BY_VALUE.get(rating_band)
        if not rating_filter:
            continue
        min_value = rating_filter.get("min")
        max_value = rating_filter.get("max")
        if min_value is None and max_value is not None:
            clauses.append(f"{column_name} < ?")
            params.append(float(max_value))
        elif min_value is not None and max_value is None:
            clauses.append(f"{column_name} >= ?")
            params.append(float(min_value))
        elif min_value is not None and max_value is not None:
            clauses.append(f"({column_name} >= ? AND {column_name} < ?)")
            params.extend([float(min_value), float(max_value)])
    if not clauses:
        return None, []
    return "(" + " OR ".join(clauses) + ")", params


def rating_to_rank_label(rating: Any) -> str | None:
    value = _normalize_rating_number(rating)
    if value is None:
        return None
    if value >= 1:
        return f"{int(math.floor(value))} dan"
    kyu = int(math.floor(-value))
    if kyu < 1:
        kyu = 1
    return f"{kyu} kyu"


def player_summary_payload(row: dict[str, Any]) -> dict[str, Any]:
    rating = rounded_rating(row.get("Rating"))
    sigma = rounded_rating(row.get("Sigma"))
    return {
        "agaid": row.get("AGAID"),
        "display_name": member_name_from_row(row),
        "first_name": row.get("FirstName"),
        "last_name": row.get("LastName"),
        "state": row.get("State"),
        "chapter_code": row.get("ChapterCode"),
        "chapter_name": row.get("ChapterName"),
        "chapter_label": chapter_label(row),
        "rating": rating,
        "sigma": sigma,
        "rank_label": rating_to_rank_label(rating),
        "game_count": row.get("GameCount") or 0,
        "tournament_count": row.get("TournamentCount") or 0,
        "latest_event_date": json_safe_value(row.get("LatestEventDate")),
        "expiration_date": json_safe_value(row.get("ExpirationDate")),
        "member_type": member_type_label(row.get("MemberType")),
    }


def normalize_player_summary(item: dict[str, Any] | None) -> dict[str, Any] | None:
    if not item:
        return item
    normalized = dict(item)
    normalized["rank_label"] = rating_to_rank_label(normalized.get("rating"))
    return normalized


def expiration_status_matches(expiration_date: Any, status_filter: str | None, today: date | None = None) -> bool:
    normalized_filter = (status_filter or "all").strip().lower()
    if normalized_filter in {"", "all"}:
        return True
    expiration_text = json_safe_value(expiration_date)
    if not expiration_text:
        return normalized_filter == "expired"
    try:
        expiration_value = date.fromisoformat(expiration_text)
    except ValueError:
        return normalized_filter == "expired"
    current_date = today or datetime.now(timezone.utc).date()
    if normalized_filter == "active":
        return expiration_value >= current_date
    if normalized_filter == "expired":
        return expiration_value < current_date
    return True


def tournament_summary_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "tournament_code": row.get("Tournament_Code"),
        "description": row.get("Tournament_Descr"),
        "tournament_date": json_safe_value(row.get("Tournament_Date")),
        "city": row.get("City"),
        "state": row.get("State_Code"),
        "country": row.get("Country_Code"),
        "rounds": row.get("Rounds"),
        "total_players": row.get("Total_Players"),
        "participant_count": row.get("ParticipantCount") or 0,
        "game_count": row.get("GameCount") or 0,
        "latest_game_date": json_safe_value(row.get("LatestGameDate")),
        "wallist_preview": (row.get("Wallist") or "")[:280],
    }


def tournament_participant_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "agaid": row.get("AGAID"),
        "first_name": row.get("FirstName"),
        "last_name": row.get("LastName"),
        "display_name": member_name_from_row(row),
        "state": row.get("State"),
        "chapter_code": row.get("ChapterCode"),
        "chapter_label": chapter_label(row),
        "games_played": row.get("GamesPlayed") or 0,
    }


def game_has_sgf(row: dict[str, Any]) -> bool:
    sgf_code = row.get("Sgf_Code")
    return bool(str(sgf_code or "").strip())


def game_sgf_viewer_url(game_id: Any, has_sgf: bool) -> str | None:
    if not has_sgf or game_id is None:
        return None
    return f"/api/ratings-explorer/game-sgf-viewer?game_id={game_id}&rev={SGF_VIEWER_REV}"


def tournament_game_payload(row: dict[str, Any]) -> dict[str, Any]:
    has_sgf = game_has_sgf(row)
    return {
        "game_id": row.get("Game_ID"),
        "game_date": json_safe_value(row.get("Game_Date")),
        "round": row.get("Round"),
        "result_text": row.get("Result"),
        "handicap": row.get("Handicap"),
        "komi": row.get("Komi"),
        "sgf_code": row.get("Sgf_Code"),
        "has_sgf": has_sgf,
        "sgf_viewer_url": game_sgf_viewer_url(row.get("Game_ID"), has_sgf),
        "player_1": {
            "agaid": row.get("Pin_Player_1"),
            "first_name": row.get("Player1FirstName"),
            "last_name": row.get("Player1LastName"),
            "display_name": member_name_from_row(
                {
                    "AGAID": row.get("Pin_Player_1"),
                    "FirstName": row.get("Player1FirstName"),
                    "LastName": row.get("Player1LastName"),
                }
            ),
            "color": row.get("Color_1"),
            "entry_rank": row.get("Rank_1"),
        },
        "player_2": {
            "agaid": row.get("Pin_Player_2"),
            "first_name": row.get("Player2FirstName"),
            "last_name": row.get("Player2LastName"),
            "display_name": member_name_from_row(
                {
                    "AGAID": row.get("Pin_Player_2"),
                    "FirstName": row.get("Player2FirstName"),
                    "LastName": row.get("Player2LastName"),
                }
            ),
            "color": row.get("Color_2"),
            "entry_rank": row.get("Rank_2"),
        },
    }


def search_players(
    conn_str: str,
    agaid: int | None,
    first_name: str | None,
    last_name: str | None,
    chapters: list[str] | None,
    states: list[str] | None,
    member_types: list[str] | None,
    status_filter: str | None,
    recent_activity_cutoff: str | None,
    rating_bands: list[str] | None,
    limit: int,
) -> list[dict[str, Any]]:
    filters: list[str] = [f"m.[AGAID] < {MAX_MEMBER_AGAID}"]
    params: list[Any] = []
    if agaid is not None:
        filters.append("m.[AGAID] = ?")
        params.append(agaid)
    if first_name:
        filters.append("m.[FirstName] LIKE ?")
        params.append(f"{first_name}%")
    if last_name:
        filters.append("m.[LastName] LIKE ?")
        params.append(f"{last_name}%")
    chapter_clause, chapter_params = chapter_filter_clause(chapters or [])
    if chapter_clause:
        filters.append(chapter_clause)
        params.extend(chapter_params)
    state_clause, state_params = state_filter_clause("m.[State]", states or [])
    if state_clause:
        filters.append(state_clause)
        params.extend(state_params)
    normalized_member_types = [
        value for value in (normalized_member_type_filter(item) for item in (member_types or [])) if value
    ]
    member_type_clause, member_type_params = _sql_in_clause("m.[MemberType]", normalized_member_types)
    if member_type_clause:
        filters.append(member_type_clause)
        params.extend(member_type_params)
    normalized_status_filter = (status_filter or "all").strip().lower()
    today_iso = datetime.now(timezone.utc).date().isoformat()
    if normalized_status_filter == "active":
        filters.append("CAST(m.[ExpirationDate] AS date) >= ?")
        params.append(today_iso)
    elif normalized_status_filter == "expired":
        filters.append("(m.[ExpirationDate] IS NULL OR CAST(m.[ExpirationDate] AS date) < ?)")
        params.append(today_iso)
    if recent_activity_cutoff:
        filters.append("stats.[LatestEventDate] >= ?")
        params.append(recent_activity_cutoff)
    rating_clause, rating_params = rating_band_sql_clause(rating_bands, "cr.[Rating]")
    if rating_clause:
        filters.append(rating_clause)
        params.extend(rating_params)

    query = (
        current_ratings_cte()
        + f"""
SELECT TOP {limit}
    m.[AGAID],
    m.[FirstName],
    m.[LastName],
    m.[State],
    m.[MemberType],
    m.[ExpirationDate],
    c.[ChapterCode],
    c.[ChapterName],
    cr.[Rating],
    cr.[Sigma],
    stats.[GameCount],
    stats.[TournamentCount],
    stats.[LatestEventDate]
FROM [membership].[members] AS m
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
LEFT JOIN current_ratings AS cr
    ON cr.[AGAID] = m.[AGAID]
OUTER APPLY
(
    SELECT
        COUNT(*) AS [GameCount],
        COUNT(DISTINCT player_games.[Tournament_Code]) AS [TournamentCount],
        MAX(player_games.[Game_Date]) AS [LatestEventDate]
    FROM
    (
        SELECT g.[Tournament_Code], g.[Game_Date]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_1] = m.[AGAID]
        UNION ALL
        SELECT g.[Tournament_Code], g.[Game_Date]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_2] = m.[AGAID]
    ) AS player_games
) AS stats
WHERE {" AND ".join(filters)}
ORDER BY
    CASE WHEN cr.[Rating] IS NULL THEN 1 ELSE 0 END,
    cr.[Rating] DESC,
    m.[LastName],
    m.[FirstName],
    m.[AGAID]
"""
    )
    return [player_summary_payload(row) for row in query_rows(conn_str, query, params)]


def search_tournaments(
    conn_str: str,
    description: str | None,
    tournament_code: str | None,
    cities: list[str] | None,
    states: list[str] | None,
    date_from: str | None,
    date_before: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    filters: list[str] = ["1 = 1"]
    params: list[Any] = []
    if description:
        filters.append("COALESCE(t.[Tournament_Descr], '') LIKE ?")
        params.append(f"%{description}%")
    if tournament_code:
        filters.append("t.[Tournament_Code] = ?")
        params.append(tournament_code)
    city_clause, city_params = city_filter_clause("t.[City]", cities or [])
    if city_clause:
        filters.append(city_clause)
        params.extend(city_params)
    state_clause, state_params = state_filter_clause("t.[State_Code]", states or [])
    if state_clause:
        filters.append(state_clause)
        params.extend(state_params)
    if date_from:
        filters.append("t.[Tournament_Date] >= ?")
        params.append(date_from)
    if date_before:
        filters.append("t.[Tournament_Date] < ?")
        params.append(date_before)

    query = f"""
SELECT TOP {limit}
    t.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date],
    t.[City],
    t.[State_Code],
    t.[Country_Code],
    t.[Rounds],
    t.[Total_Players],
    t.[Wallist],
    stats.[ParticipantCount],
    stats.[GameCount]
FROM [ratings].[tournaments] AS t
OUTER APPLY
(
    SELECT
        COUNT(DISTINCT g.[Game_ID]) AS [GameCount],
        COUNT(DISTINCT participant.[AGAID]) AS [ParticipantCount]
    FROM [ratings].[games] AS g
    OUTER APPLY
    (
        VALUES (g.[Pin_Player_1]), (g.[Pin_Player_2])
    ) AS participant([AGAID])
    WHERE g.[Tournament_Code] = t.[Tournament_Code]
) AS stats
WHERE {" AND ".join(filters)}
ORDER BY t.[Tournament_Date] DESC, t.[Tournament_Code]
"""
    return [tournament_summary_payload(row) for row in query_rows(conn_str, query, params)]


def build_filter_options(conn_str: str) -> dict[str, Any]:
    chapter_rows = query_rows(
        conn_str,
        f"""
SELECT DISTINCT
    c.[ChapterCode],
    c.[ChapterName]
FROM [membership].[members] AS m
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
WHERE m.[AGAID] < {MAX_MEMBER_AGAID}
  AND c.[ChapterCode] IS NOT NULL
ORDER BY c.[ChapterCode], c.[ChapterName]
""",
        [],
    )
    state_rows = query_rows(
        conn_str,
        f"""
SELECT [State]
FROM
(
    SELECT DISTINCT m.[State] AS [State]
    FROM [membership].[members] AS m
    WHERE m.[AGAID] < {MAX_MEMBER_AGAID}
      AND m.[State] IS NOT NULL
    UNION
    SELECT DISTINCT t.[State_Code] AS [State]
    FROM [ratings].[tournaments] AS t
    WHERE t.[State_Code] IS NOT NULL
) AS states
ORDER BY [State]
""",
        [],
    )
    city_rows = query_rows(
        conn_str,
        """
SELECT DISTINCT t.[City]
FROM [ratings].[tournaments] AS t
WHERE t.[City] IS NOT NULL
  AND LTRIM(RTRIM(t.[City])) <> ''
ORDER BY t.[City]
""",
        [],
    )
    member_type_rows = query_rows(
        conn_str,
        f"""
SELECT DISTINCT m.[MemberType]
FROM [membership].[members] AS m
WHERE m.[AGAID] < {MAX_MEMBER_AGAID}
  AND m.[MemberType] IS NOT NULL
ORDER BY m.[MemberType]
""",
        [],
    )
    tournament_location_rows = query_rows(
        conn_str,
        """
SELECT DISTINCT
    t.[State_Code],
    t.[City]
FROM [ratings].[tournaments] AS t
ORDER BY t.[State_Code], t.[City]
""",
        [],
    )
    tournament_rows = query_rows(
        conn_str,
        """
SELECT
    t.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date]
FROM [ratings].[tournaments] AS t
WHERE t.[Tournament_Code] IS NOT NULL
ORDER BY t.[Tournament_Date] DESC, t.[Tournament_Descr], t.[Tournament_Code]
""",
        [],
    )
    chapters = [dict(BLANK_CHAPTER_OPTION)]
    for row in chapter_rows:
        code = (row.get("ChapterCode") or "").strip()
        label = chapter_label(row)
        if not code or not label:
            continue
        chapters.append({"value": code, "label": label})
    states = [dict(BLANK_STATE_OPTION)]
    seen_states: set[str] = set()
    for row in state_rows:
        option = state_option(row.get("State"))
        if not option or option["value"] in seen_states:
            continue
        seen_states.add(option["value"])
        states.append(option)
    tournaments = []
    for row in tournament_rows:
        code = (row.get("Tournament_Code") or "").strip()
        if not code:
            continue
        label = (row.get("Tournament_Descr") or "").strip() or code
        tournament_date = json_safe_value(row.get("Tournament_Date"))
        if tournament_date:
            label = f"{label} ({tournament_date})"
        tournaments.append({"value": code, "label": label, "tournament_date": tournament_date or ""})
    tournament_states = [dict(BLANK_STATE_OPTION)]
    seen_tournament_states: set[str] = set()
    cities = [dict(BLANK_CITY_OPTION)]
    seen_cities: set[str] = set()
    for row in city_rows:
        city = (row.get("City") or "").strip()
        if not city:
            continue
        city_key = city.lower()
        if city_key in seen_cities:
            continue
        seen_cities.add(city_key)
        cities.append({"value": city, "label": city})
    tournament_locations = []
    seen_locations: set[tuple[str, str]] = set()
    for row in tournament_location_rows:
        raw_state = (row.get("State_Code") or "").strip().upper()
        raw_city = (row.get("City") or "").strip()
        state_value = raw_state if raw_state else BLANK_STATE_VALUE
        city_value = raw_city if raw_city else BLANK_CITY_VALUE
        location_key = (state_value, city_value)
        if location_key in seen_locations:
            continue
        seen_locations.add(location_key)
        tournament_locations.append({"state": state_value, "city": city_value})
        if state_value != BLANK_STATE_VALUE and state_value not in seen_tournament_states:
            option = state_option(state_value)
            if option:
                seen_tournament_states.add(state_value)
                tournament_states.append(option)
    member_types = []
    seen_member_types: set[str] = set()
    for row in member_type_rows:
        label = member_type_label(row.get("MemberType"))
        if not label or label in seen_member_types:
            continue
        seen_member_types.add(label)
        member_types.append({"value": label, "label": label})
    return {
        "chapters": chapters,
        "cities": cities,
        "member_types": member_types,
        "states": states,
        "tournament_cities": cities,
        "tournament_locations": tournament_locations,
        "tournament_states": tournament_states,
        "tournaments": [{"value": item["value"], "label": item["label"]} for item in tournaments],
    }


def get_player_detail(
    conn_str: str,
    agaid: int,
    recent_games_sgf_only: bool = False,
    recent_tournaments_page: int = 0,
    recent_games_page: int = 0,
    include_context: bool = False,
) -> dict[str, Any] | None:
    recent_tournaments_page = max(0, int(recent_tournaments_page or 0))
    recent_games_page = max(0, int(recent_games_page or 0))
    tournaments_page_size = 12
    games_page_size = 20
    tournaments_offset = recent_tournaments_page * tournaments_page_size
    games_offset = recent_games_page * games_page_size
    summary_query = (
        current_ratings_cte()
        + """
SELECT
    m.[AGAID],
    m.[FirstName],
    m.[LastName],
    m.[State],
    m.[MemberType],
    m.[ExpirationDate],
    c.[ChapterCode],
    c.[ChapterName],
    cr.[Rating],
    cr.[Sigma],
    stats.[GameCount],
    stats.[TournamentCount],
    stats.[OpponentCount],
    stats.[Wins],
    stats.[Losses],
    stats.[LatestEventDate]
FROM [membership].[members] AS m
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
LEFT JOIN current_ratings AS cr
    ON cr.[AGAID] = m.[AGAID]
OUTER APPLY
(
    SELECT
        COUNT(*) AS [GameCount],
        COUNT(DISTINCT player_games.[Tournament_Code]) AS [TournamentCount],
        COUNT(DISTINCT player_games.[OpponentAGAID]) AS [OpponentCount],
        SUM(CASE WHEN player_games.[Outcome] = 'W' THEN 1 ELSE 0 END) AS [Wins],
        SUM(CASE WHEN player_games.[Outcome] = 'L' THEN 1 ELSE 0 END) AS [Losses],
        MAX(player_games.[Game_Date]) AS [LatestEventDate]
    FROM
    (
        SELECT
            g.[Tournament_Code],
            g.[Game_Date],
            g.[Pin_Player_2] AS [OpponentAGAID],
            CASE
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'B' THEN 'W' ELSE 'L' END
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'W' THEN 'W' ELSE 'L' END
                ELSE NULL
            END AS [Outcome]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_1] = m.[AGAID]
        UNION ALL
        SELECT
            g.[Tournament_Code],
            g.[Game_Date],
            g.[Pin_Player_1] AS [OpponentAGAID],
            CASE
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'B' THEN 'W' ELSE 'L' END
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'W' THEN 'W' ELSE 'L' END
                ELSE NULL
            END AS [Outcome]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_2] = m.[AGAID]
    ) AS player_games
) AS stats
WHERE m.[AGAID] = ?
"""
    )
    row = query_one(conn_str, summary_query, [agaid])
    if not row:
        return None

    tournaments_query = """
SELECT
    t.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date],
    t.[City],
    t.[State_Code],
    tournament_games.[GamesPlayed],
    tournament_games.[Wins],
    tournament_games.[Losses]
FROM
(
    SELECT
        player_games.[Tournament_Code],
        COUNT(*) AS [GamesPlayed],
        SUM(CASE WHEN player_games.[Outcome] = 'W' THEN 1 ELSE 0 END) AS [Wins],
        SUM(CASE WHEN player_games.[Outcome] = 'L' THEN 1 ELSE 0 END) AS [Losses],
        MAX(player_games.[Game_Date]) AS [LatestDate]
    FROM
    (
        SELECT
            g.[Tournament_Code],
            g.[Game_Date],
            CASE
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'B' THEN 'W' ELSE 'L' END
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'W' THEN 'W' ELSE 'L' END
                ELSE NULL
            END AS [Outcome]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_1] = ?
        UNION ALL
        SELECT
            g.[Tournament_Code],
            g.[Game_Date],
            CASE
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'B' THEN 'W' ELSE 'L' END
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'W' THEN 'W' ELSE 'L' END
                ELSE NULL
            END AS [Outcome]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_2] = ?
    ) AS player_games
    GROUP BY player_games.[Tournament_Code]
) AS tournament_games
LEFT JOIN [ratings].[tournaments] AS t
    ON t.[Tournament_Code] = tournament_games.[Tournament_Code]
ORDER BY tournament_games.[LatestDate] DESC, tournament_games.[Tournament_Code]
OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
"""
    opponents_query = """
SELECT TOP 16
    opponent_stats.[OpponentAGAID] AS [AGAID],
    m.[FirstName],
    m.[LastName],
    m.[State],
    c.[ChapterCode],
    c.[ChapterName],
    opponent_stats.[GamesPlayed],
    opponent_stats.[Wins],
    opponent_stats.[Losses],
    opponent_stats.[LatestDate]
FROM
(
    SELECT
        player_games.[OpponentAGAID],
        COUNT(*) AS [GamesPlayed],
        SUM(CASE WHEN player_games.[Outcome] = 'W' THEN 1 ELSE 0 END) AS [Wins],
        SUM(CASE WHEN player_games.[Outcome] = 'L' THEN 1 ELSE 0 END) AS [Losses],
        MAX(player_games.[Game_Date]) AS [LatestDate]
    FROM
    (
        SELECT
            g.[Pin_Player_2] AS [OpponentAGAID],
            g.[Game_Date],
            CASE
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'B' THEN 'W' ELSE 'L' END
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'W' THEN 'W' ELSE 'L' END
                ELSE NULL
            END AS [Outcome]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_1] = ?
        UNION ALL
        SELECT
            g.[Pin_Player_1] AS [OpponentAGAID],
            g.[Game_Date],
            CASE
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'B' THEN 'W' ELSE 'L' END
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'W' THEN 'W' ELSE 'L' END
                ELSE NULL
            END AS [Outcome]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_2] = ?
    ) AS player_games
    WHERE player_games.[OpponentAGAID] IS NOT NULL
    GROUP BY player_games.[OpponentAGAID]
) AS opponent_stats
LEFT JOIN [membership].[members] AS m
    ON m.[AGAID] = opponent_stats.[OpponentAGAID]
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
ORDER BY opponent_stats.[GamesPlayed] DESC, opponent_stats.[LatestDate] DESC, opponent_stats.[OpponentAGAID]
"""
    sgf_only_filter = "WHERE COALESCE(LTRIM(RTRIM(player_games.[Sgf_Code])), '') <> ''" if recent_games_sgf_only else ""
    recent_games_query = (
        current_ratings_cte()
        + f"""
SELECT
    player_games.[Game_ID],
    player_games.[Game_Date],
    player_games.[Round],
    player_games.[ResultText],
    player_games.[Outcome],
    player_games.[Color],
    player_games.[Handicap],
    player_games.[Sgf_Code],
    player_games.[PlayerRank],
    player_games.[OpponentRank],
    player_games.[OpponentAGAID],
    player_games.[Tournament_Code],
    t.[Tournament_Descr],
    m.[FirstName],
    m.[LastName]
FROM
(
    SELECT
        g.[Game_ID],
        g.[Game_Date],
        g.[Round],
        g.[Result] AS [ResultText],
        g.[Color_1] AS [Color],
        g.[Handicap] AS [Handicap],
        g.[Sgf_Code],
        g.[Rank_1] AS [PlayerRank],
        g.[Rank_2] AS [OpponentRank],
        g.[Pin_Player_2] AS [OpponentAGAID],
        g.[Tournament_Code],
        CASE
            WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'B' THEN 'W' ELSE 'L' END
            WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'W' THEN 'W' ELSE 'L' END
            ELSE NULL
        END AS [Outcome]
    FROM [ratings].[games] AS g
    WHERE g.[Pin_Player_1] = ?
    UNION ALL
    SELECT
        g.[Game_ID],
        g.[Game_Date],
        g.[Round],
        g.[Result] AS [ResultText],
        g.[Color_2] AS [Color],
        g.[Handicap] AS [Handicap],
        g.[Sgf_Code],
        g.[Rank_2] AS [PlayerRank],
        g.[Rank_1] AS [OpponentRank],
        g.[Pin_Player_1] AS [OpponentAGAID],
        g.[Tournament_Code],
        CASE
            WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'B' THEN 'W' ELSE 'L' END
            WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'W' THEN 'W' ELSE 'L' END
            ELSE NULL
        END AS [Outcome]
    FROM [ratings].[games] AS g
    WHERE g.[Pin_Player_2] = ?
) AS player_games
LEFT JOIN [ratings].[tournaments] AS t
    ON t.[Tournament_Code] = player_games.[Tournament_Code]
LEFT JOIN [membership].[members] AS m
    ON m.[AGAID] = player_games.[OpponentAGAID]
{sgf_only_filter}
ORDER BY player_games.[Game_Date] DESC, player_games.[Tournament_Code], player_games.[Round]
OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
"""
    )
    recent_games_count_query = (
        current_ratings_cte()
        + f"""
SELECT COUNT(*) AS [GameCount]
FROM
(
    SELECT
        g.[Game_ID],
        g.[Sgf_Code]
    FROM [ratings].[games] AS g
    WHERE g.[Pin_Player_1] = ?
    UNION ALL
    SELECT
        g.[Game_ID],
        g.[Sgf_Code]
    FROM [ratings].[games] AS g
    WHERE g.[Pin_Player_2] = ?
) AS player_games
{sgf_only_filter}
"""
    )

    tournaments = [
        {
            "tournament_code": tournament.get("Tournament_Code"),
            "description": tournament.get("Tournament_Descr"),
            "tournament_date": json_safe_value(tournament.get("Tournament_Date")),
            "city": tournament.get("City"),
            "state": tournament.get("State_Code"),
            "games_played": tournament.get("GamesPlayed") or 0,
            "wins": tournament.get("Wins") or 0,
            "losses": tournament.get("Losses") or 0,
        }
        for tournament in query_rows(conn_str, tournaments_query, [agaid, agaid, tournaments_offset, tournaments_page_size])
    ]
    opponents = [
        {
            "agaid": opponent.get("AGAID"),
            "display_name": member_name_from_row(opponent),
            "state": opponent.get("State"),
            "chapter_label": chapter_label(opponent),
            "games_played": opponent.get("GamesPlayed") or 0,
            "wins": opponent.get("Wins") or 0,
            "losses": opponent.get("Losses") or 0,
            "latest_game_date": json_safe_value(opponent.get("LatestDate")),
        }
        for opponent in query_rows(conn_str, opponents_query, [agaid, agaid])
    ]
    recent_games = [
        {
            "game_id": game.get("Game_ID"),
            "game_date": json_safe_value(game.get("Game_Date")),
            "round": game.get("Round"),
            "result_text": game.get("ResultText"),
            "outcome": game.get("Outcome"),
            "color": game.get("Color"),
            "handicap": game.get("Handicap"),
            "sgf_code": game.get("Sgf_Code"),
            "has_sgf": game_has_sgf(game),
            "sgf_viewer_url": game_sgf_viewer_url(game.get("Game_ID"), game_has_sgf(game)),
            "player_rank": game.get("PlayerRank"),
            "opponent_agaid": game.get("OpponentAGAID"),
            "opponent_name": member_name_from_row(
                {
                    "AGAID": game.get("OpponentAGAID"),
                    "FirstName": game.get("FirstName"),
                    "LastName": game.get("LastName"),
                }
            ),
            "opponent_rank": game.get("OpponentRank"),
            "tournament_code": game.get("Tournament_Code"),
            "tournament_description": game.get("Tournament_Descr"),
        }
        for game in query_rows(conn_str, recent_games_query, [agaid, agaid, games_offset, games_page_size])
    ]
    history_points = serialize_rating_history(load_sql_rating_history(agaid))
    total_recent_tournaments = int(row.get("TournamentCount") or 0)
    total_recent_games = int(row.get("GameCount") or 0)
    if recent_games_sgf_only:
        total_recent_games = int((query_one(conn_str, recent_games_count_query, [agaid, agaid]) or {}).get("GameCount") or 0)

    summary = player_summary_payload(row)
    summary.update(
        {
            "opponent_count": row.get("OpponentCount") or 0,
            "wins": row.get("Wins") or 0,
            "losses": row.get("Losses") or 0,
            "history_chart_url": f"/api/ratings-explorer/player-history.svg?agaid={agaid}",
        }
    )
    payload = {
        "player": summary,
        "rating_history": history_points,
        "recent_tournaments": tournaments,
        "recent_tournaments_paging": {
            "page": recent_tournaments_page,
            "page_size": tournaments_page_size,
            "total_count": total_recent_tournaments,
            "has_previous": recent_tournaments_page > 0,
            "has_next": (tournaments_offset + len(tournaments)) < total_recent_tournaments,
        },
        "opponents": opponents,
        "recent_games": recent_games,
        "recent_games_paging": {
            "page": recent_games_page,
            "page_size": games_page_size,
            "total_count": total_recent_games,
            "has_previous": recent_games_page > 0,
            "has_next": (games_offset + len(recent_games)) < total_recent_games,
        },
    }
    return attach_player_articles(conn_str, payload, agaid) if include_context else payload



def load_player_articles(conn_str: str, agaid: int, limit: int = 7) -> list[dict[str, Any]]:
    article_limit = max(1, min(int(limit), 7))
    cache_key = f"{agaid}:{article_limit}"
    cached, found = _cache_get("player_articles", cache_key)
    if found:
        return cached
    query = f"""
SELECT TOP {article_limit}
    [JournalDate],
    [ArticleTitle],
    [ArticleLink]
FROM [integration].[journal_article_member_match]
WHERE [AGAID] = ?
  AND NULLIF(LTRIM(RTRIM([ArticleTitle])), '') IS NOT NULL
  AND NULLIF(LTRIM(RTRIM([ArticleLink])), '') IS NOT NULL
ORDER BY [JournalDate] DESC, [JournalArticleMemberMatchID] DESC
"""
    articles = [
        {
            "journal_date": json_safe_value(article.get("JournalDate")),
            "article_title": article.get("ArticleTitle"),
            "article_link": article.get("ArticleLink"),
        }
        for article in query_rows(conn_str, query, [agaid])
    ]
    return _cache_set("player_articles", cache_key, articles, PLAYER_CONTEXT_CACHE_TTL_SECONDS)



def load_player_review_videos(conn_str: str, agaid: int, limit: int = 7) -> list[dict[str, Any]]:
    review_limit = max(1, min(int(limit), 7))
    cache_key = f"{agaid}:{review_limit}"
    cached, found = _cache_get("player_review_videos", cache_key)
    if found:
        return cached
    query = f"""
WITH all_reviews AS
(
    SELECT
        match_rows.[JournalReviewMemberMatchID],
        match_rows.[JournalDate],
        match_rows.[AGAID],
        match_rows.[BlogLink],
        match_rows.[ReviewerName],
        match_rows.[ReviewerRank],
        match_rows.[OpponentName],
        match_rows.[OpponentRank],
        match_rows.[GameLink],
        match_rows.[VideoLink],
        match_rows.[VideoReviewCount]
    FROM [integration].[journal_review_member_match] AS match_rows
    WHERE NULLIF(LTRIM(RTRIM(match_rows.[ReviewerName])), '') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(match_rows.[ReviewerRank])), '') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(match_rows.[OpponentName])), '') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(match_rows.[OpponentRank])), '') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(match_rows.[VideoLink])), '') IS NOT NULL
),
distinct_video_games AS
(
    SELECT
        all_reviews.[JournalDate],
        all_reviews.[BlogLink],
        all_reviews.[ReviewerName],
        all_reviews.[ReviewerRank],
        MAX(all_reviews.[OpponentName]) AS [OpponentName],
        MAX(all_reviews.[OpponentRank]) AS [OpponentRank],
        all_reviews.[GameLink],
        all_reviews.[VideoLink],
        MAX(all_reviews.[VideoReviewCount]) AS [StoredReviewCount],
        MIN(all_reviews.[JournalReviewMemberMatchID]) AS [FirstMatchID]
    FROM all_reviews
    WHERE NULLIF(LTRIM(RTRIM(all_reviews.[GameLink])), '') IS NOT NULL
    GROUP BY
        all_reviews.[JournalDate],
        all_reviews.[BlogLink],
        all_reviews.[ReviewerName],
        all_reviews.[ReviewerRank],
        all_reviews.[GameLink],
        all_reviews.[VideoLink]
),
ordered_video_games AS
(
    SELECT
        distinct_video_games.[JournalDate],
        distinct_video_games.[BlogLink],
        distinct_video_games.[ReviewerName],
        distinct_video_games.[ReviewerRank],
        distinct_video_games.[OpponentName],
        distinct_video_games.[OpponentRank],
        distinct_video_games.[GameLink],
        distinct_video_games.[VideoLink],
        COALESCE(
            NULLIF(distinct_video_games.[StoredReviewCount], 0),
            COUNT(*) OVER (
                PARTITION BY
                    distinct_video_games.[JournalDate],
                    distinct_video_games.[BlogLink],
                    distinct_video_games.[ReviewerName],
                    distinct_video_games.[ReviewerRank],
                    distinct_video_games.[VideoLink]
            )
        ) AS [ReviewCount],
        ROW_NUMBER() OVER (
            PARTITION BY
                distinct_video_games.[JournalDate],
                distinct_video_games.[BlogLink],
                distinct_video_games.[ReviewerName],
                distinct_video_games.[ReviewerRank],
                distinct_video_games.[VideoLink]
            ORDER BY distinct_video_games.[FirstMatchID]
        ) AS [GameOrder]
    FROM distinct_video_games
),
matched_reviews AS
(
    SELECT DISTINCT
        all_reviews.[AGAID],
        ordered_video_games.[JournalDate],
        ordered_video_games.[ReviewerName],
        ordered_video_games.[ReviewerRank],
        ordered_video_games.[OpponentName],
        ordered_video_games.[OpponentRank],
        ordered_video_games.[VideoLink],
        ordered_video_games.[ReviewCount],
        ordered_video_games.[GameOrder],
        ordered_video_games.[GameLink],
        ordered_video_games.[BlogLink],
        MIN(all_reviews.[JournalReviewMemberMatchID]) OVER (
            PARTITION BY
                all_reviews.[AGAID],
                ordered_video_games.[JournalDate],
                ordered_video_games.[BlogLink],
                ordered_video_games.[ReviewerName],
                ordered_video_games.[ReviewerRank],
                ordered_video_games.[VideoLink],
                ordered_video_games.[GameLink]
        ) AS [FirstMatchID]
    FROM all_reviews
    INNER JOIN ordered_video_games
        ON ordered_video_games.[JournalDate] = all_reviews.[JournalDate]
       AND ISNULL(ordered_video_games.[BlogLink], '') = ISNULL(all_reviews.[BlogLink], '')
       AND ordered_video_games.[ReviewerName] = all_reviews.[ReviewerName]
       AND ordered_video_games.[ReviewerRank] = all_reviews.[ReviewerRank]
       AND ordered_video_games.[VideoLink] = all_reviews.[VideoLink]
       AND ISNULL(ordered_video_games.[GameLink], '') = ISNULL(all_reviews.[GameLink], '')
)
SELECT TOP {review_limit}
    matched_reviews.[JournalDate],
    matched_reviews.[ReviewerName],
    matched_reviews.[ReviewerRank],
    matched_reviews.[OpponentName],
    matched_reviews.[OpponentRank],
    matched_reviews.[VideoLink],
    matched_reviews.[ReviewCount],
    matched_reviews.[GameOrder]
FROM matched_reviews
WHERE matched_reviews.[AGAID] = ?
ORDER BY matched_reviews.[JournalDate] DESC, matched_reviews.[FirstMatchID] ASC
"""
    reviews = [
        {
            "journal_date": json_safe_value(review.get("JournalDate")),
            "reviewer_name": review.get("ReviewerName"),
            "reviewer_rank": review.get("ReviewerRank"),
            "opponent_name": review.get("OpponentName"),
            "opponent_rank": review.get("OpponentRank"),
            "video_link": review.get("VideoLink"),
            "review_count": int(review.get("ReviewCount") or 0),
            "game_order": int(review.get("GameOrder") or 0),
            "context_label": (
                f'{review.get("ReviewerName")} ({review.get("ReviewerRank")}) reviews game vs '
                f'{review.get("OpponentName")} ({review.get("OpponentRank")})'
            ),
        }
        for review in query_rows(conn_str, query, [agaid])
    ]
    return _cache_set("player_review_videos", cache_key, reviews, PLAYER_CONTEXT_CACHE_TTL_SECONDS)


def attach_player_articles(conn_str: str | None, payload: dict[str, Any] | None, agaid: int) -> dict[str, Any] | None:
    if not payload:
        return payload
    payload["news_articles"] = load_player_articles(conn_str, agaid) if conn_str else []
    payload["review_videos"] = load_player_review_videos(conn_str, agaid) if conn_str else []
    return payload


def get_tournament_detail(conn_str: str, tournament_code: str) -> dict[str, Any] | None:
    summary_query = """
SELECT
    t.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date],
    t.[City],
    t.[State_Code],
    t.[Country_Code],
    t.[Rounds],
    t.[Total_Players],
    t.[Wallist],
    stats.[GameCount],
    stats.[ParticipantCount],
    stats.[LatestGameDate]
FROM [ratings].[tournaments] AS t
OUTER APPLY
(
    SELECT
        COUNT(DISTINCT g.[Game_ID]) AS [GameCount],
        COUNT(DISTINCT participant.[AGAID]) AS [ParticipantCount],
        MAX(g.[Game_Date]) AS [LatestGameDate]
    FROM [ratings].[games] AS g
    OUTER APPLY
    (
        VALUES (g.[Pin_Player_1]), (g.[Pin_Player_2])
    ) AS participant([AGAID])
    WHERE g.[Tournament_Code] = t.[Tournament_Code]
) AS stats
WHERE t.[Tournament_Code] = ?
"""
    summary = query_one(conn_str, summary_query, [tournament_code])
    if not summary:
        return None

    participants_query = """
SELECT
    participant_rows.[AGAID],
    m.[FirstName],
    m.[LastName],
    m.[State],
    c.[ChapterCode],
    c.[ChapterName],
    participant_rows.[GamesPlayed]
FROM
(
    SELECT
        participant.[AGAID],
        COUNT(*) AS [GamesPlayed]
    FROM [ratings].[games] AS g
    OUTER APPLY
    (
        VALUES (g.[Pin_Player_1]), (g.[Pin_Player_2])
    ) AS participant([AGAID])
    WHERE g.[Tournament_Code] = ?
      AND participant.[AGAID] IS NOT NULL
    GROUP BY participant.[AGAID]
) AS participant_rows
LEFT JOIN [membership].[members] AS m
    ON m.[AGAID] = participant_rows.[AGAID]
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
ORDER BY participant_rows.[GamesPlayed] DESC, m.[LastName], m.[FirstName], participant_rows.[AGAID]
"""
    games_query = """
SELECT
    g.[Game_ID],
    g.[Game_Date],
    g.[Round],
    g.[Result],
    g.[Handicap],
    g.[Komi],
    g.[Color_1],
    g.[Color_2],
    g.[Sgf_Code],
    g.[Rank_1],
    g.[Rank_2],
    g.[Pin_Player_1],
    g.[Pin_Player_2],
    p1.[FirstName] AS [Player1FirstName],
    p1.[LastName] AS [Player1LastName],
    p2.[FirstName] AS [Player2FirstName],
    p2.[LastName] AS [Player2LastName]
FROM [ratings].[games] AS g
LEFT JOIN [membership].[members] AS p1
    ON p1.[AGAID] = g.[Pin_Player_1]
LEFT JOIN [membership].[members] AS p2
    ON p2.[AGAID] = g.[Pin_Player_2]
WHERE g.[Tournament_Code] = ?
ORDER BY g.[Game_Date], TRY_CONVERT(INT, g.[Round]), g.[Round], g.[Game_ID]
"""

    participants = [
        tournament_participant_payload(participant)
        for participant in query_rows(conn_str, participants_query, [tournament_code])
    ]
    games = [
        tournament_game_payload(game)
        for game in query_rows(conn_str, games_query, [tournament_code])
    ]
    recent_games = list(reversed(games[-20:]))

    return {
        "tournament": {
            "tournament_code": summary.get("Tournament_Code"),
            "description": summary.get("Tournament_Descr"),
            "tournament_date": json_safe_value(summary.get("Tournament_Date")),
            "city": summary.get("City"),
            "state": summary.get("State_Code"),
            "country": summary.get("Country_Code"),
            "rounds": summary.get("Rounds"),
            "total_players": summary.get("Total_Players"),
            "participant_count": summary.get("ParticipantCount") or 0,
            "game_count": summary.get("GameCount") or 0,
            "latest_game_date": json_safe_value(summary.get("LatestGameDate")),
            "wallist": summary.get("Wallist"),
        },
        "participants": participants,
        "games": games,
        "recent_games": recent_games,
    }


def load_sql_rating_history(agaid: int) -> list[tuple[datetime, float, float]]:
    cache_key = str(agaid)
    cached, found = _cache_get("player_rating_history", cache_key)
    if found:
        return cached
    conn_str = get_sql_connection_string()
    if not conn_str:
        raise ValueError("SQL connection string not configured.")
    rows = query_rows(
        conn_str,
        """
SELECT [Elab_Date], [Rating], [Sigma]
FROM [ratings].[ratings]
WHERE [Pin_Player] = ?
  AND [Elab_Date] IS NOT NULL
  AND [Rating] IS NOT NULL
ORDER BY [Elab_Date], [id]
""",
        [agaid],
    )
    history: list[tuple[datetime, float, float]] = []
    for row in rows:
        event_date = row.get("Elab_Date")
        rating = row.get("Rating")
        sigma = row.get("Sigma")
        if event_date is None or rating is None:
            continue
        history.append(
            (
                datetime.combine(event_date, datetime.min.time()),
                float(rating),
                float(sigma or 0.0),
            )
        )
    return _cache_set("player_rating_history", cache_key, history, PLAYER_RATING_HISTORY_CACHE_TTL_SECONDS)


def serialize_rating_history(history_points: list[tuple[datetime, float, float]]) -> list[dict[str, Any]]:
    return [
        {
            "date": dt.date().isoformat(),
            "rating": rounded_rating(value),
            "sigma": rounded_rating(sigma) or 0.0,
        }
        for dt, value, sigma in history_points
    ]


def render_single_history_svg(
    agaid: int,
    history_points: list[tuple[datetime, float, float]],
    member_name: str | None = None,
) -> str:
    if not history_points:
        raise ValueError(f"No official rating history found for AGAID {agaid}.")

    min_date = min(point[0] for point in history_points)
    max_date = max(point[0] for point in history_points)
    def chart_rating(value: float) -> float:
        if value <= -1.0:
            return value
        if value >= 1.0:
            return value - 2.0
        return -1.0

    def chart_tick_label(tick: int) -> str:
        if tick == -1:
            return "1/-1"
        return f"{tick + 2:d}" if tick >= 0 else f"{tick:d}"

    center_chart_values = [chart_rating(point[1]) for point in history_points]
    min_chart_rating = min(center - sigma for center, (_, _, sigma) in zip(center_chart_values, history_points))
    max_chart_rating = max(center + sigma for center, (_, _, sigma) in zip(center_chart_values, history_points))
    if min_chart_rating == max_chart_rating:
        min_chart_rating -= 1.0
        max_chart_rating += 1.0
    chart_padding = max((max_chart_rating - min_chart_rating) * 0.05, 0.25)
    min_chart_rating -= chart_padding
    max_chart_rating += chart_padding

    width = 1200
    height = 520
    left = 88
    right = 26
    top = 52
    bottom = 62
    plot_w = width - left - right
    plot_h = height - top - bottom

    def x_pos(dt: datetime) -> float:
        total_days = (max_date - min_date).days or 1
        return left + (((dt - min_date).days) / total_days) * plot_w

    def y_pos(rating: float) -> float:
        return top + ((max_chart_rating - chart_rating(rating)) / (max_chart_rating - min_chart_rating)) * plot_h

    def y_pos_chart(chart_value: float) -> float:
        return top + ((max_chart_rating - chart_value) / (max_chart_rating - min_chart_rating)) * plot_h

    def polyline(points: list[tuple[datetime, float, float]]) -> str:
        return " ".join(f"{x_pos(dt):.2f},{y_pos(value):.2f}" for dt, value, _ in points)

    def sigma_band(points: list[tuple[datetime, float, float]]) -> str:
        upper = [f"{x_pos(dt):.2f},{y_pos_chart(chart_rating(value) + sigma):.2f}" for dt, value, sigma in points]
        lower = [f"{x_pos(dt):.2f},{y_pos_chart(chart_rating(value) - sigma):.2f}" for dt, value, sigma in reversed(points)]
        return " ".join(upper + lower)

    x_ticks = []
    for year in range(min_date.year, max_date.year + 1, 2):
        tick = datetime(year, 1, 1)
        if min_date <= tick <= max_date:
            x_ticks.append(tick)
    if not x_ticks:
        x_ticks = [min_date, max_date]

    y_start = math.floor(min_chart_rating)
    y_end = math.ceil(max_chart_rating)
    y_ticks = list(range(y_start, y_end + 1))
    if y_start <= -1 <= y_end and -1 not in y_ticks:
        y_ticks.append(-1)
    if len(y_ticks) < 2:
        y_ticks = sorted({math.floor(min_chart_rating), math.ceil(max_chart_rating)})
    else:
        y_ticks = sorted(set(y_ticks))

    font_stack = "'Red Hat Text', 'Segoe UI', Helvetica, Arial, sans-serif"
    svg = []
    svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">')
    svg.append(f"<style>text{{font-family:{font_stack};}}</style>")
    svg.append('<rect width="100%" height="100%" fill="#ffffff"/>')
    svg.append('<rect x="0" y="0" width="100%" height="100%" fill="url(#chartBg)"/>')
    svg.append(
        "<defs>"
        '<linearGradient id="chartBg" x1="0%" y1="0%" x2="100%" y2="100%">'
        '<stop offset="0%" stop-color="#ffffff"/>'
        '<stop offset="100%" stop-color="#f4f9fd"/>'
        "</linearGradient>"
        "</defs>"
    )
    title = html.escape(member_name) if member_name else f"AGAID {agaid}"
    svg.append(f'<text x="{width / 2}" y="28" text-anchor="middle" font-size="24" font-weight="700" fill="#163248">{title}</text>')
    svg.append(f'<text x="{width / 2}" y="48" text-anchor="middle" font-size="13" font-weight="500" fill="#5a7082">Official rating snapshots from ratings.ratings</text>')
    for tick in x_ticks:
        x = x_pos(tick)
        svg.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_h}" stroke="#d9e7f2" stroke-width="1"/>')
        svg.append(f'<text x="{x:.2f}" y="{height - 24}" text-anchor="middle" font-size="13" font-weight="700" fill="#4a6477">{tick.year}</text>')
    for tick in y_ticks:
        y = top + ((max_chart_rating - float(tick)) / (max_chart_rating - min_chart_rating)) * plot_h
        svg.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_w}" y2="{y:.2f}" stroke="#d9e7f2" stroke-width="1"/>')
        svg.append(f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" font-size="13" font-weight="700" fill="#4a6477">{chart_tick_label(tick)}</text>')
    svg.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" stroke="#014a7d" stroke-width="1.5"/>')
    svg.append(f'<line x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top + plot_h}" stroke="#014a7d" stroke-width="1.5"/>')
    svg.append(f'<polygon fill="#aed0e8" fill-opacity="0.42" points="{sigma_band(history_points)}"/>')
    svg.append(f'<polyline fill="none" stroke="#3a73b2" stroke-width="3" points="{polyline(history_points)}"/>')
    for dt, value, _ in history_points:
        svg.append(f'<circle cx="{x_pos(dt):.2f}" cy="{y_pos(value):.2f}" r="2.9" fill="#014a7d" stroke="#ffffff" stroke-width="1.2"/>')
    svg.append(f'<text x="{width / 2}" y="{height - 4}" text-anchor="middle" font-size="14" font-weight="700" fill="#163248">Date</text>')
    svg.append(f'<text x="24" y="{height / 2}" transform="rotate(-90 24 {height / 2})" text-anchor="middle" font-size="14" font-weight="700" fill="#163248">AGA numeric rating</text>')
    svg.append("</svg>")
    return "\n".join(svg)


def render_game_sgf_viewer_html(game_id: int, sgf_url: str) -> str:
    wgo_css = get_asset_text("wgo/wgo.player.css") or ""
    wgo_core_js = get_asset_text("wgo/wgo.min.js") or ""
    wgo_player_js = get_asset_text("wgo/wgo.player.min.js") or ""
    escaped_wgo_css = wgo_css.replace("</style>", "<\\/style>")
    escaped_wgo_core_js = wgo_core_js.replace("</script>", "<\\/script>")
    escaped_wgo_player_js = wgo_player_js.replace("</script>", "<\\/script>")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Game {game_id} SGF</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #eef5fa;
      --panel: #ffffff;
      --ink: #163248;
      --muted: #5a7082;
      --line: #bfd4e5;
      --accent: #014a7d;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: Arial, sans-serif; background: radial-gradient(circle at top, #f7fbfe 0%, var(--bg) 55%, #ddeaf4 100%); color: var(--ink); }}
    .shell {{ min-height: 100vh; padding: 8px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 8px; box-shadow: 0 18px 50px rgba(1, 74, 125, 0.08); }}
    .status {{ color: var(--muted); margin: 0 0 8px; font-size: 0.9rem; }}
    .viewer-shell {{ min-height: 520px; }}
    #viewer {{ min-height: 520px; width: 100%; overflow: hidden; }}
    #viewer .wgo-player-main {{ position: relative; }}
    #viewer .wgo-player-control {{
      position: static;
      width: 100%;
      padding: 0 0 6px !important;
      z-index: 1;
      background: transparent;
    }}
    #viewer .wgo-control-wrapper {{ width: 100%; text-align: center; }}
    #viewer .wgo-ctrlgroup-control {{
      float: none !important;
      display: inline-flex;
      align-items: center;
    }}
    #viewer .wgo-ctrlgroup-right {{
      position: absolute;
      top: 20px;
      right: calc(42% - 90px);
      float: none !important;
      display: flex;
      align-items: center;
      z-index: 2;
    }}
    #viewer .wgo-player-control button.wgo-button {{
      width: 34px;
      height: 34px;
      margin: 0 2px;
    }}
    #viewer .wgo-player-top .wgo-player-info,
    #viewer .wgo-player-bottom .wgo-player-info {{
      right: 0;
      width: 42%;
    }}
    #viewer .wgo-player-top .wgo-infobox .wgo-box-title,
    #viewer .wgo-player-bottom .wgo-infobox .wgo-box-title {{
      right: 42%;
      margin-left: 2px;
    }}
    #viewer .wgo-player-top .wgo-player-wrapper,
    #viewer .wgo-player-bottom .wgo-player-wrapper {{
      padding-left: 0;
      width: 50%;
    }}
    #viewer .wgo-player-board {{
      margin-top: 0 !important;
    }}
    #viewer .wgo-board {{
      transform: scale(0.92);
      transform-origin: top center;
    }}
    pre {{ margin: 0; white-space: pre-wrap; overflow-wrap: anywhere; max-height: 480px; overflow: auto; background: #f6fbff; border-radius: 12px; border: 1px solid var(--line); padding: 12px; font-size: 0.82rem; }}
    .hidden {{ display: none; }}
  </style>
  <style>{escaped_wgo_css}</style>
</head>
<body>
  <div class="shell">
    <div class="card">
      <div id="status" class="status hidden">Loading SGF viewer...</div>
      <div id="viewer-shell" class="viewer-shell">
        <div id="viewer" data-wgo="{html.escape(sgf_url, quote=True)}" style="width: 100%;">
          Your browser could not load the interactive SGF viewer. Use the raw SGF link below.
        </div>
      </div>
      <pre id="raw" class="hidden"></pre>
    </div>
  </div>
  <script>{escaped_wgo_core_js}</script>
  <script>
    if (window.WGo) {{
      window.WGo.DIR = "";
      window.WGo.ERROR_REPORT = true;
      if (window.WGo.Board && window.WGo.Board.default) {{
        window.WGo.Board.default.background = "";
      }}
    }}
  </script>
  <script>{escaped_wgo_player_js}</script>
  <script>
    const statusEl = document.getElementById('status');
    const viewerShellEl = document.getElementById('viewer-shell');
    const viewerEl = document.getElementById('viewer');
    const rawEl = document.getElementById('raw');
    const sgfDownloadUrl = {json.dumps(sgf_url)};
    const sgfDownloadName = {json.dumps(f"game-{game_id}.sgf")};
    let viewerTweaksApplied = false;

    if (window.WGo && window.WGo.BasicPlayer && window.WGo.BasicPlayer.component && window.WGo.BasicPlayer.component.Control) {{
      const menuItems = window.WGo.BasicPlayer.component.Control.menu || [];
      const permalinkItem = menuItems.find((item) => item && item.args && item.args.name === 'permalink');
      if (permalinkItem) {{
        window.WGo.i18n = window.WGo.i18n || {{}};
        window.WGo.i18n.en = window.WGo.i18n.en || {{}};
        window.WGo.i18n.en.permalink = 'Download SGF';
        permalinkItem.args.click = async function(player) {{
          try {{
            const response = await fetch(sgfDownloadUrl);
            if (!response.ok) throw new Error(`Download failed with ${{response.status}}.`);
            const blob = await response.blob();
            const objectUrl = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = objectUrl;
            link.download = sgfDownloadName;
            document.body.appendChild(link);
            link.click();
            link.remove();
            window.setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
          }} catch (error) {{
            const message = error && error.message ? error.message : 'Could not download the SGF file.';
            if (player && typeof player.showMessage === 'function') {{
              player.showMessage(`<h1>Download SGF</h1><p>${{message}}</p>`);
            }} else {{
              statusEl.textContent = message;
              statusEl.classList.remove('hidden');
            }}
          }}
        }};
      }}
    }}

    function customizeViewerLayout() {{
      const root = document.querySelector('#viewer .wgo-player-main');
      if (!root) return false;
      const topControl = root.querySelector('.wgo-player-control');
      if (topControl) {{
        const navGroup = topControl.querySelector('.wgo-ctrlgroup-control');
        if (navGroup) {{
          navGroup.style.display = 'inline-flex';
          navGroup.style.float = 'none';
        }}
        const aboutGroup = topControl.querySelector('.wgo-ctrlgroup-right');
        if (aboutGroup) {{
          aboutGroup.style.display = 'flex';
          aboutGroup.style.float = 'none';
          root.appendChild(aboutGroup);
        }}
        viewerTweaksApplied = true;
        return true;
      }}
      return false;
    }}

    function showRawFallback(message, sgf) {{
      statusEl.textContent = message;
      if (sgf) {{
        rawEl.textContent = sgf;
        rawEl.classList.remove('hidden');
      }}
    }}

    async function loadSgf() {{
      let response;
      try {{
        response = await fetch({json.dumps(sgf_url)});
      }} catch (error) {{
        statusEl.textContent = 'Could not reach the SGF endpoint.';
        return;
      }}
      if (!response.ok) {{
        statusEl.textContent = (await response.text()) || `SGF request failed with ${{response.status}}.`;
        return;
      }}
      const sgf = await response.text();
      try {{
        if (window.WGo && typeof window.WGo.BasicPlayer === 'function') {{
          if (!viewerEl._wgo_player) {{
            viewerEl.textContent = '';
            const player = new window.WGo.BasicPlayer(viewerEl, {{ sgf }});
            viewerEl._wgo_player = player;
            window.setTimeout(() => {{ if (customizeViewerLayout()) observer.disconnect(); }}, 0);
          }}
          return;
        }}
      }} catch (error) {{
        const message = error && error.message ? error.message : 'Interactive SGF viewer unavailable here.';
        showRawFallback(`${{message}} Showing raw SGF instead.`, sgf);
        return;
      }}
      showRawFallback('Interactive SGF viewer unavailable here. Showing raw SGF instead.', sgf);
    }}

    const observer = new MutationObserver(() => {{
      if (!viewerTweaksApplied && customizeViewerLayout()) observer.disconnect();
    }});
    observer.observe(viewerShellEl, {{ childList: true, subtree: true }});
    window.addEventListener('load', () => {{
      window.setTimeout(() => {{ if (customizeViewerLayout()) observer.disconnect(); }}, 0);
      window.setTimeout(() => {{ if (customizeViewerLayout()) observer.disconnect(); }}, 250);
      window.setTimeout(() => {{ if (customizeViewerLayout()) observer.disconnect(); }}, 800);
    }});

    loadSgf();
  </script>
</body>
</html>
"""


def load_ratings_explorer_html(api_base: str = "") -> str:
    template_path = _app_root() / "ratings_explorer.html"
    markup = template_path.read_text(encoding="utf-8")
    return markup.replace('"__RATINGS_EXPLORER_API_BASE__"', json.dumps(api_base))


def snapshot_path() -> Path:
    return _app_root() / SNAPSHOT_DIRNAME / SNAPSHOT_FILENAME


def snapshot_status_path() -> Path:
    return _app_root() / SNAPSHOT_DIRNAME / SNAPSHOT_STATUS_FILENAME


def snapshot_request_path() -> Path:
    return _app_root() / SNAPSHOT_DIRNAME / SNAPSHOT_REQUEST_FILENAME


def startup_players_path() -> Path:
    return _app_root() / SNAPSHOT_DIRNAME / STARTUP_PLAYERS_FILENAME


def player_search_snapshot_path() -> Path:
    return _app_root() / SNAPSHOT_DIRNAME / PLAYER_SEARCH_SNAPSHOT_FILENAME


def tournament_detail_dir_path() -> Path:
    return _app_root() / SNAPSHOT_DIRNAME / SNAPSHOT_TOURNAMENT_DETAIL_DIRNAME


def _safe_snapshot_component(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "_"
    return "".join(char if char.isalnum() or char in ("-", "_", ".") else "_" for char in text)


def tournament_detail_path(tournament_code: str) -> Path:
    return tournament_detail_dir_path() / f"{_safe_snapshot_component(tournament_code)}.json"


def snapshot_storage_connection_string() -> str | None:
    return _get_setting("AzureWebJobsStorage")


def _snapshot_container_client():
    conn_str = snapshot_storage_connection_string()
    if not conn_str or BlobServiceClient is None:
        return None
    service = BlobServiceClient.from_connection_string(conn_str)
    container = service.get_container_client(SNAPSHOT_CONTAINER)
    try:
        container.create_container()
    except Exception:
        pass
    return container


def _blob_client():
    container = _snapshot_container_client()
    if container is None:
        return None
    return container.get_blob_client(SNAPSHOT_BLOB_NAME)


def _status_blob_client():
    container = _snapshot_container_client()
    if container is None:
        return None
    return container.get_blob_client(SNAPSHOT_STATUS_BLOB_NAME)


def _startup_players_blob_client():
    container = _snapshot_container_client()
    if container is None:
        return None
    return container.get_blob_client(STARTUP_PLAYERS_BLOB_NAME)


def _player_search_snapshot_blob_client():
    container = _snapshot_container_client()
    if container is None:
        return None
    return container.get_blob_client(PLAYER_SEARCH_SNAPSHOT_BLOB_NAME)


def _request_blob_client():
    container = _snapshot_container_client()
    if container is None:
        return None
    return container.get_blob_client(SNAPSHOT_REQUEST_BLOB_NAME)


def _tournament_detail_blob_client(tournament_code: str):
    container = _snapshot_container_client()
    if container is None:
        return None
    safe_code = _safe_snapshot_component(tournament_code)
    return container.get_blob_client(f"{SNAPSHOT_TOURNAMENT_DETAIL_BLOB_PREFIX}/{safe_code}.json")


def _sgf_container_client():
    conn_str = snapshot_storage_connection_string()
    if not conn_str or BlobServiceClient is None:
        return None
    service = BlobServiceClient.from_connection_string(conn_str)
    container = service.get_container_client(SGF_CONTAINER)
    try:
        container.create_container()
    except Exception:
        pass
    return container


def sgf_blob_client(sgf_code: str):
    container = _sgf_container_client()
    if container is None:
        return None
    safe_code = (sgf_code or "").strip().strip("/\\")
    if not safe_code:
        return None
    return container.get_blob_client(f"games/{safe_code}.sgf")


def _slug_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


def build_uploaded_sgf_code(game_id: int, row: dict[str, Any] | None = None, now: datetime | None = None) -> str:
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%d-%H%M%S")
    if not row:
        return f"upload-{game_id}-{stamp}"
    player_1 = _slug_text(member_name_from_row(
        {
            "AGAID": row.get("Pin_Player_1"),
            "FirstName": row.get("Player1FirstName"),
            "LastName": row.get("Player1LastName"),
        }
    ))
    player_2 = _slug_text(member_name_from_row(
        {
            "AGAID": row.get("Pin_Player_2"),
            "FirstName": row.get("Player2FirstName"),
            "LastName": row.get("Player2LastName"),
        }
    ))
    game_date = str(row.get("Game_Date") or "").strip().replace("-", "")
    prefix = "-".join(part for part in [player_1 or "game", player_2 or "game", game_date or None] if part)
    return f"{prefix}-{game_id}-{stamp}"


def upload_sgf_blob(sgf_code: str, sgf_text: str) -> str:
    blob = sgf_blob_client(sgf_code)
    if blob is None:
        raise RuntimeError("SGF blob container is unavailable.")
    blob.upload_blob(sgf_text.encode("utf-8"), overwrite=True)
    return sgf_code


def load_game_sgf_text(conn_str: str, game_id: int) -> tuple[str | None, str | None]:
    cache_key = str(game_id)
    cached, found = _cache_get("game_sgf", cache_key)
    if found:
        return cached
    row = query_one(
        conn_str,
        """
SELECT TOP 1 [Game_ID], [Sgf_Code]
FROM [ratings].[games]
WHERE [Game_ID] = ?
""",
        [game_id],
    )
    if not row:
        return _cache_set("game_sgf", cache_key, (None, None), GAME_SGF_CACHE_TTL_SECONDS)
    sgf_code = str(row.get("Sgf_Code") or "").strip()
    if not sgf_code:
        return _cache_set("game_sgf", cache_key, (sgf_code, None), GAME_SGF_CACHE_TTL_SECONDS)
    blob = sgf_blob_client(sgf_code)
    if blob is None:
        return _cache_set("game_sgf", cache_key, (sgf_code, None), GAME_SGF_CACHE_TTL_SECONDS)
    try:
        payload = blob.download_blob().readall()
    except Exception:
        return _cache_set("game_sgf", cache_key, (sgf_code, None), GAME_SGF_CACHE_TTL_SECONDS)
    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError:
        decoded = payload.decode("latin-1")
    return _cache_set("game_sgf", cache_key, (sgf_code, decoded), GAME_SGF_CACHE_TTL_SECONDS)


def load_game_for_sgf_upload(conn_str: str, game_id: int) -> dict[str, Any] | None:
    return query_one(
        conn_str,
        """
SELECT TOP 1
    g.[Game_ID],
    g.[Tournament_Code],
    g.[Game_Date],
    g.[Round],
    g.[Result],
    g.[Handicap],
    g.[Komi],
    g.[Sgf_Code],
    g.[Pin_Player_1],
    g.[Pin_Player_2],
    p1.[FirstName] AS [Player1FirstName],
    p1.[LastName] AS [Player1LastName],
    p2.[FirstName] AS [Player2FirstName],
    p2.[LastName] AS [Player2LastName]
FROM [ratings].[games] AS g
LEFT JOIN [membership].[members] AS p1
    ON p1.[AGAID] = g.[Pin_Player_1]
LEFT JOIN [membership].[members] AS p2
    ON p2.[AGAID] = g.[Pin_Player_2]
WHERE g.[Game_ID] = ?
""",
        [game_id],
    )


def update_game_sgf_code(conn_str: str, game_id: int, sgf_code: str) -> None:
    execute_non_query(
        conn_str,
        """
UPDATE [ratings].[games]
SET [Sgf_Code] = ?
WHERE [Game_ID] = ?
""",
        [sgf_code, game_id],
    )
    _cache_delete("game_sgf", str(game_id))


def sgf_upload_game_payload(row: dict[str, Any]) -> dict[str, Any]:
    has_sgf = game_has_sgf(row)
    return {
        "game_id": row.get("Game_ID"),
        "game_date": json_safe_value(row.get("Game_Date")),
        "round": row.get("Round"),
        "result_text": row.get("Result"),
        "handicap": row.get("Handicap"),
        "komi": row.get("Komi"),
        "tournament_code": row.get("Tournament_Code"),
        "sgf_code": row.get("Sgf_Code"),
        "has_sgf": has_sgf,
        "sgf_viewer_url": game_sgf_viewer_url(row.get("Game_ID"), has_sgf),
        "player_1": {
            "agaid": row.get("Pin_Player_1"),
            "display_name": member_name_from_row(
                {
                    "AGAID": row.get("Pin_Player_1"),
                    "FirstName": row.get("Player1FirstName"),
                    "LastName": row.get("Player1LastName"),
                }
            ),
        },
        "player_2": {
            "agaid": row.get("Pin_Player_2"),
            "display_name": member_name_from_row(
                {
                    "AGAID": row.get("Pin_Player_2"),
                    "FirstName": row.get("Player2FirstName"),
                    "LastName": row.get("Player2LastName"),
                }
            ),
        },
    }


def load_snapshot() -> dict[str, Any] | None:
    cached, found = _cache_get("snapshot", "main")
    if found:
        return cached
    blob = _blob_client()
    if blob is not None:
        try:
            payload = blob.download_blob().readall()
            return _cache_set("snapshot", "main", json.loads(payload.decode("utf-8")), SNAPSHOT_CACHE_TTL_SECONDS)
        except Exception:
            pass
    path = snapshot_path()
    if not path.exists():
        return _cache_set("snapshot", "main", None, SNAPSHOT_CACHE_TTL_SECONDS)
    try:
        return _cache_set("snapshot", "main", json.loads(path.read_text(encoding="utf-8")), SNAPSHOT_CACHE_TTL_SECONDS)
    except (OSError, json.JSONDecodeError):
        return _cache_set("snapshot", "main", None, SNAPSHOT_CACHE_TTL_SECONDS)


def load_snapshot_status() -> dict[str, Any] | None:
    cached, found = _cache_get("snapshot_status", "main")
    if found:
        return cached
    blob = _status_blob_client()
    if blob is not None:
        try:
            payload = blob.download_blob().readall()
            return _cache_set("snapshot_status", "main", json.loads(payload.decode("utf-8")), SNAPSHOT_STATUS_CACHE_TTL_SECONDS)
        except Exception:
            pass
    path = snapshot_status_path()
    if not path.exists():
        return _cache_set("snapshot_status", "main", None, SNAPSHOT_STATUS_CACHE_TTL_SECONDS)
    try:
        return _cache_set("snapshot_status", "main", json.loads(path.read_text(encoding="utf-8")), SNAPSHOT_STATUS_CACHE_TTL_SECONDS)
    except (OSError, json.JSONDecodeError):
        return _cache_set("snapshot_status", "main", None, SNAPSHOT_STATUS_CACHE_TTL_SECONDS)


def load_snapshot_request() -> dict[str, Any] | None:
    cached, found = _cache_get("snapshot_request", "main")
    if found:
        return cached
    blob = _request_blob_client()
    if blob is not None:
        try:
            payload = blob.download_blob().readall()
            return _cache_set("snapshot_request", "main", json.loads(payload.decode("utf-8")), SNAPSHOT_REQUEST_CACHE_TTL_SECONDS)
        except Exception:
            pass
    path = snapshot_request_path()
    if not path.exists():
        return _cache_set("snapshot_request", "main", None, SNAPSHOT_REQUEST_CACHE_TTL_SECONDS)
    try:
        return _cache_set("snapshot_request", "main", json.loads(path.read_text(encoding="utf-8")), SNAPSHOT_REQUEST_CACHE_TTL_SECONDS)
    except (OSError, json.JSONDecodeError):
        return _cache_set("snapshot_request", "main", None, SNAPSHOT_REQUEST_CACHE_TTL_SECONDS)


def load_tournament_detail_snapshot(tournament_code: str) -> dict[str, Any] | None:
    cache_key = str(tournament_code or "").strip()
    cached, found = _cache_get("tournament_detail", cache_key)
    if found:
        return cached
    blob = _tournament_detail_blob_client(tournament_code)
    if blob is not None:
        try:
            payload = blob.download_blob().readall()
            return _cache_set("tournament_detail", cache_key, json.loads(payload.decode("utf-8")), TOURNAMENT_DETAIL_CACHE_TTL_SECONDS)
        except Exception:
            pass
    path = tournament_detail_path(tournament_code)
    if not path.exists():
        return _cache_set("tournament_detail", cache_key, None, TOURNAMENT_DETAIL_CACHE_TTL_SECONDS)
    try:
        return _cache_set("tournament_detail", cache_key, json.loads(path.read_text(encoding="utf-8")), TOURNAMENT_DETAIL_CACHE_TTL_SECONDS)
    except (OSError, json.JSONDecodeError):
        return _cache_set("tournament_detail", cache_key, None, TOURNAMENT_DETAIL_CACHE_TTL_SECONDS)


def load_startup_players() -> dict[str, Any] | None:
    cached, found = _cache_get("startup_players", "main")
    if found:
        return cached
    blob = _startup_players_blob_client()
    if blob is not None:
        try:
            payload = blob.download_blob().readall()
            return _cache_set("startup_players", "main", json.loads(payload.decode("utf-8")), STARTUP_PLAYERS_CACHE_TTL_SECONDS)
        except Exception:
            pass
    path = startup_players_path()
    if not path.exists():
        return _cache_set("startup_players", "main", None, STARTUP_PLAYERS_CACHE_TTL_SECONDS)
    try:
        return _cache_set("startup_players", "main", json.loads(path.read_text(encoding="utf-8")), STARTUP_PLAYERS_CACHE_TTL_SECONDS)
    except (OSError, json.JSONDecodeError):
        return _cache_set("startup_players", "main", None, STARTUP_PLAYERS_CACHE_TTL_SECONDS)


def load_player_search_snapshot() -> dict[str, Any] | None:
    cached, found = _cache_get("player_search_snapshot", "main")
    if found:
        return cached
    blob = _player_search_snapshot_blob_client()
    if blob is not None:
        try:
            payload = blob.download_blob().readall()
            return _cache_set("player_search_snapshot", "main", json.loads(payload.decode("utf-8")), STARTUP_PLAYERS_CACHE_TTL_SECONDS)
        except Exception:
            pass
    path = player_search_snapshot_path()
    if not path.exists():
        return _cache_set("player_search_snapshot", "main", None, STARTUP_PLAYERS_CACHE_TTL_SECONDS)
    try:
        return _cache_set("player_search_snapshot", "main", json.loads(path.read_text(encoding="utf-8")), STARTUP_PLAYERS_CACHE_TTL_SECONDS)
    except (OSError, json.JSONDecodeError):
        return _cache_set("player_search_snapshot", "main", None, STARTUP_PLAYERS_CACHE_TTL_SECONDS)


def save_snapshot(snapshot: dict[str, Any]) -> None:
    payload = json.dumps(snapshot, ensure_ascii=True)
    _cache_set("snapshot", "main", snapshot, SNAPSHOT_CACHE_TTL_SECONDS)
    blob = _blob_client()
    if blob is not None:
        try:
            blob.upload_blob(payload.encode("utf-8"), overwrite=True)
        except Exception:
            pass
    path = snapshot_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
    except Exception:
        pass


def save_startup_players(payload_dict: dict[str, Any]) -> None:
    payload = json.dumps(payload_dict, ensure_ascii=True)
    _cache_set("startup_players", "main", payload_dict, STARTUP_PLAYERS_CACHE_TTL_SECONDS)
    blob = _startup_players_blob_client()
    if blob is not None:
        try:
            blob.upload_blob(payload.encode("utf-8"), overwrite=True)
        except Exception:
            pass
    path = startup_players_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
    except Exception:
        pass


def save_player_search_snapshot(payload_dict: dict[str, Any]) -> None:
    payload = json.dumps(payload_dict, ensure_ascii=True)
    _cache_set("player_search_snapshot", "main", payload_dict, STARTUP_PLAYERS_CACHE_TTL_SECONDS)
    blob = _player_search_snapshot_blob_client()
    if blob is not None:
        try:
            blob.upload_blob(payload.encode("utf-8"), overwrite=True)
        except Exception:
            pass
    path = player_search_snapshot_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
    except Exception:
        pass


def save_tournament_detail_snapshots(tournament_details: dict[str, dict[str, Any]]) -> None:
    detail_dir = tournament_detail_dir_path()
    try:
        detail_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    for tournament_code, payload_dict in tournament_details.items():
        payload = json.dumps(payload_dict, ensure_ascii=True)
        _cache_set("tournament_detail", str(tournament_code or "").strip(), payload_dict, TOURNAMENT_DETAIL_CACHE_TTL_SECONDS)
        blob = _tournament_detail_blob_client(tournament_code)
        if blob is not None:
            try:
                blob.upload_blob(payload.encode("utf-8"), overwrite=True)
            except Exception:
                pass
        try:
            tournament_detail_path(tournament_code).write_text(payload, encoding="utf-8")
        except Exception:
            pass


def save_snapshot_status(status: dict[str, Any]) -> None:
    payload = json.dumps(status, ensure_ascii=True)
    _cache_set("snapshot_status", "main", status, SNAPSHOT_STATUS_CACHE_TTL_SECONDS)
    blob = _status_blob_client()
    if blob is not None:
        try:
            blob.upload_blob(payload.encode("utf-8"), overwrite=True)
        except Exception:
            pass
    path = snapshot_status_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
    except Exception:
        pass


def save_snapshot_request(request: dict[str, Any]) -> None:
    payload = json.dumps(request, ensure_ascii=True)
    _cache_set("snapshot_request", "main", request, SNAPSHOT_REQUEST_CACHE_TTL_SECONDS)
    blob = _request_blob_client()
    if blob is not None:
        try:
            blob.upload_blob(payload.encode("utf-8"), overwrite=True)
        except Exception:
            pass
    path = snapshot_request_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
    except Exception:
        pass


def clear_snapshot_request() -> None:
    _cache_delete("snapshot_request", "main")
    blob = _request_blob_client()
    if blob is not None:
        try:
            blob.delete_blob(delete_snapshots="include")
        except Exception:
            pass
    path = snapshot_request_path()
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def update_snapshot_status(
    state: str,
    *,
    source: str | None = None,
    detail: str | None = None,
    error: str | None = None,
    snapshot_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    existing = load_snapshot_status() or {}
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    status = {
        "state": state,
        "updated_at": now,
        "source": source or existing.get("source"),
        "detail": detail if detail is not None else existing.get("detail"),
        "error": error,
        "snapshot_meta": snapshot_meta if snapshot_meta is not None else existing.get("snapshot_meta"),
        "last_started_at": existing.get("last_started_at"),
        "last_completed_at": existing.get("last_completed_at"),
    }
    if state == "running":
        status["last_started_at"] = now
    if state == "completed":
        status["last_completed_at"] = now
    save_snapshot_status(status)
    return status


def request_snapshot_refresh(source: str, requested_at: str) -> dict[str, Any]:
    request = {
        "requested_at": requested_at,
        "source": source,
        "status": "pending",
    }
    save_snapshot_request(request)
    return request


def _snapshot_sort_key_player(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        1 if item.get("rating") is None else 0,
        -(item.get("rating") or float("-inf")),
        (item.get("last_name") or "").lower(),
        (item.get("first_name") or "").lower(),
        item.get("agaid") or 0,
    )


def _snapshot_sort_key_tournament(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        item.get("tournament_date") or "",
        item.get("tournament_code") or "",
    )


def _player_matches_snapshot_filters(
    item: dict[str, Any],
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
    chapter_values = set(chapters or [])
    include_blank_chapter = BLANK_CHAPTER_VALUE in chapter_values
    concrete_chapter_values = {value for value in chapter_values if value != BLANK_CHAPTER_VALUE}
    state_values = set(states or [])
    include_blank_state = BLANK_STATE_VALUE in state_values
    concrete_state_values = {value for value in state_values if value != BLANK_STATE_VALUE}
    member_type_values = {value.lower() for value in (member_types or []) if value}
    if agaid is not None and item.get("agaid") != agaid:
        return False
    if first_name and not (item.get("first_name") or "").lower().startswith(first_name.lower()):
        return False
    if last_name and not (item.get("last_name") or "").lower().startswith(last_name.lower()):
        return False
    if chapter_values:
        item_chapter = (item.get("chapter_code") or "").strip()
        if item_chapter:
            if concrete_chapter_values and item_chapter not in concrete_chapter_values:
                return False
            if not concrete_chapter_values and include_blank_chapter:
                return False
        elif not include_blank_chapter:
            return False
    if state_values:
        item_state = (item.get("state") or "").strip()
        if item_state:
            if concrete_state_values and item_state not in concrete_state_values:
                return False
            if not concrete_state_values and include_blank_state:
                return False
        elif not include_blank_state:
            return False
    if member_type_values and (item.get("member_type") or "").lower() not in member_type_values:
        return False
    if not expiration_status_matches(item.get("expiration_date"), status_filter):
        return False
    latest_event_date = (item.get("latest_event_date") or "").strip()
    if recent_activity_cutoff and (not latest_event_date or latest_event_date < recent_activity_cutoff):
        return False
    if not rating_matches_band(item.get("rating"), rating_bands):
        return False
    return True


def search_players_from_snapshot(
    snapshot: dict[str, Any],
    agaid: int | None,
    first_name: str | None,
    last_name: str | None,
    chapters: list[str] | None,
    states: list[str] | None,
    member_types: list[str] | None,
    status_filter: str | None,
    recent_activity_cutoff: str | None,
    rating_bands: list[str] | None,
    limit: int,
) -> list[dict[str, Any]]:
    players = snapshot.get("players", [])
    matches = [
        normalize_player_summary(item)
        for item in players
        if _player_matches_snapshot_filters(
            item,
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
    ]
    matches.sort(key=_snapshot_sort_key_player)
    return matches[:limit]


def snapshot_supports_player_member_type(snapshot: dict[str, Any]) -> bool:
    players = snapshot.get("players") or []
    if not players:
        return False
    return any("member_type" in item for item in players[:25])


def search_tournaments_from_snapshot(
    snapshot: dict[str, Any],
    description: str | None,
    tournament_code: str | None,
    cities: list[str] | None,
    states: list[str] | None,
    date_from: str | None,
    date_before: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    description_text = (description or "").lower()
    tournament_code_text = (tournament_code or "").strip().lower()
    city_values = {value.lower() for value in (cities or []) if value}
    state_values = {value.lower() for value in (states or []) if value}
    include_blank_city = BLANK_CITY_VALUE.lower() in city_values
    concrete_city_values = {value for value in city_values if value != BLANK_CITY_VALUE.lower()}
    include_blank_state = BLANK_STATE_VALUE.lower() in state_values
    concrete_state_values = {value for value in state_values if value != BLANK_STATE_VALUE.lower()}
    date_from_text = (date_from or "").strip()
    date_before_text = (date_before or "").strip()
    matches = []
    for item in snapshot.get("tournaments", []):
        if tournament_code_text and (item.get("tournament_code") or "").lower() != tournament_code_text:
            continue
        if description_text and description_text not in (item.get("description") or "").lower():
            continue
        if city_values:
            item_city = (item.get("city") or "").strip().lower()
            if item_city:
                if concrete_city_values and item_city not in concrete_city_values:
                    continue
                if not concrete_city_values and include_blank_city:
                    continue
            elif not include_blank_city:
                continue
        if state_values:
            item_state = (item.get("state") or "").strip().lower()
            if item_state:
                if concrete_state_values and item_state not in concrete_state_values:
                    continue
                if not concrete_state_values and include_blank_state:
                    continue
            elif not include_blank_state:
                continue
        tournament_date = (item.get("tournament_date") or "").strip()
        if date_from_text and tournament_date and tournament_date < date_from_text:
            continue
        if date_from_text and not tournament_date:
            continue
        if date_before_text and tournament_date and tournament_date >= date_before_text:
            continue
        if date_before_text and not tournament_date:
            continue
        matches.append(item)
    matches.sort(key=_snapshot_sort_key_tournament, reverse=True)
    return matches[:limit]


def build_filter_options_from_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    chapters_by_code: dict[str, dict[str, str]] = {BLANK_CHAPTER_VALUE: dict(BLANK_CHAPTER_OPTION)}
    states: dict[str, dict[str, str]] = {BLANK_STATE_VALUE: dict(BLANK_STATE_OPTION)}
    cities: dict[str, dict[str, str]] = {BLANK_CITY_VALUE: dict(BLANK_CITY_OPTION)}
    member_types: dict[str, dict[str, str]] = {}
    tournaments: list[dict[str, str]] = []
    seen_tournaments: set[str] = set()
    tournament_locations: list[dict[str, str]] = []
    seen_locations: set[tuple[str, str]] = set()

    for item in snapshot.get("players", []):
        chapter_code = (item.get("chapter_code") or "").strip()
        chapter_name = (item.get("chapter_name") or "").strip()
        chapter_text = (item.get("chapter_label") or "").strip()
        if chapter_code and chapter_code not in chapters_by_code:
            chapters_by_code[chapter_code] = {
                "value": chapter_code,
                "label": chapter_text or chapter_code,
            }
        option = state_option(item.get("state"))
        if option:
            states[option["value"]] = option
        member_type = (item.get("member_type") or "").strip()
        if member_type:
            member_types[member_type] = {"value": member_type, "label": member_type}

    for item in snapshot.get("tournaments", []):
        raw_state = (item.get("state") or "").strip().upper()
        raw_city = (item.get("city") or "").strip()
        state_value = raw_state if raw_state else BLANK_STATE_VALUE
        city_value = raw_city if raw_city else BLANK_CITY_VALUE
        location_key = (state_value, city_value)
        if location_key not in seen_locations:
            seen_locations.add(location_key)
            tournament_locations.append({"state": state_value, "city": city_value})
        option = state_option(item.get("state"))
        if option:
            states[option["value"]] = option
        if raw_city:
            cities[raw_city.lower()] = {"value": raw_city, "label": raw_city}
        code = (item.get("tournament_code") or "").strip()
        if not code or code in seen_tournaments:
            continue
        seen_tournaments.add(code)
        description = (item.get("description") or "").strip()
        tournament_date = (item.get("tournament_date") or "").strip()
        label = description or code
        if tournament_date:
            label = f"{label} ({tournament_date})"
        tournaments.append({"value": code, "label": label, "tournament_date": tournament_date})

    chapter_items = [chapters_by_code[key] for key in sorted(k for k in chapters_by_code if k != BLANK_CHAPTER_VALUE)]
    chapters = [dict(BLANK_CHAPTER_OPTION)] + chapter_items
    state_items = [states[key] for key in sorted(k for k in states if k != BLANK_STATE_VALUE)]
    city_items = [cities[key] for key in sorted(k for k in cities if k != BLANK_CITY_VALUE)]
    tournament_options = sorted(
        tournaments,
        key=lambda item: ((item.get("tournament_date") or ""), item["label"].lower()),
        reverse=True,
    )
    return {
        "chapters": chapters,
        "cities": [dict(BLANK_CITY_OPTION)] + city_items,
        "member_types": [member_types[key] for key in sorted(member_types)],
        "states": [dict(BLANK_STATE_OPTION)] + state_items,
        "tournament_cities": [dict(BLANK_CITY_OPTION)] + city_items,
        "tournament_locations": tournament_locations,
        "tournament_states": [dict(BLANK_STATE_OPTION)] + state_items,
        "tournaments": [{"value": item["value"], "label": item["label"]} for item in tournament_options],
    }


def get_player_detail_from_snapshot(snapshot: dict[str, Any], agaid: int) -> dict[str, Any] | None:
    detail = (snapshot.get("player_details") or {}).get(str(agaid))
    if not detail:
        return None
    normalized_detail = dict(detail)
    normalized_detail["player"] = normalize_player_summary(detail.get("player"))
    return normalized_detail


def get_tournament_detail_from_snapshot(snapshot: dict[str, Any], tournament_code: str) -> dict[str, Any] | None:
    return (snapshot.get("tournament_details") or {}).get(tournament_code)


def load_member_name_from_snapshot(snapshot: dict[str, Any], agaid: int) -> str | None:
    detail = get_player_detail_from_snapshot(snapshot, agaid)
    if detail and detail.get("player"):
        return detail["player"].get("display_name")
    for player in snapshot.get("players", []):
        if player.get("agaid") == agaid:
            return player.get("display_name")
    return None


def load_rating_history_from_snapshot(snapshot: dict[str, Any], agaid: int) -> list[tuple[datetime, float, float]]:
    points = ((snapshot.get("rating_histories") or {}).get(str(agaid))) or []
    history: list[tuple[datetime, float, float]] = []
    for point in points:
        event_date = point.get("date")
        if not event_date:
            continue
        history.append(
            (
                datetime.combine(date.fromisoformat(event_date), datetime.min.time()),
                float(point.get("rating") or 0.0),
                float(point.get("sigma") or 0.0),
            )
        )
    return history


def _build_snapshot_artifacts(conn_str: str) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    player_summary_rows = query_rows(
        conn_str,
        current_ratings_cte()
        + f"""
SELECT
    m.[AGAID],
    m.[FirstName],
    m.[LastName],
    m.[State],
    m.[MemberType],
    m.[ExpirationDate],
    c.[ChapterCode],
    c.[ChapterName],
    cr.[Rating],
    cr.[Sigma],
    stats.[GameCount],
    stats.[TournamentCount],
    stats.[LatestEventDate]
FROM [membership].[members] AS m
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
LEFT JOIN current_ratings AS cr
    ON cr.[AGAID] = m.[AGAID]
OUTER APPLY
(
    SELECT
        COUNT(DISTINCT g.[Game_ID]) AS [GameCount],
        COUNT(DISTINCT player_games.[Tournament_Code]) AS [TournamentCount],
        MAX(player_games.[Game_Date]) AS [LatestEventDate]
    FROM
    (
        SELECT g.[Tournament_Code], g.[Game_Date]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_1] = m.[AGAID]
        UNION ALL
        SELECT g.[Tournament_Code], g.[Game_Date]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_2] = m.[AGAID]
    ) AS player_games
) AS stats
WHERE m.[AGAID] < {MAX_MEMBER_AGAID}
ORDER BY
    CASE WHEN cr.[Rating] IS NULL THEN 1 ELSE 0 END,
    cr.[Rating] DESC,
    m.[LastName],
    m.[FirstName],
    m.[AGAID]
""",
        [],
    )
    tournament_summary_rows = query_rows(
        conn_str,
        """
SELECT
    t.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date],
    t.[City],
    t.[State_Code],
    t.[Country_Code],
    t.[Rounds],
    t.[Total_Players],
    t.[Wallist],
    stats.[ParticipantCount],
    stats.[GameCount],
    stats.[LatestGameDate]
FROM [ratings].[tournaments] AS t
OUTER APPLY
(
    SELECT
        COUNT(DISTINCT g.[Game_ID]) AS [GameCount],
        COUNT(DISTINCT participant.[AGAID]) AS [ParticipantCount],
        MAX(g.[Game_Date]) AS [LatestGameDate]
    FROM [ratings].[games] AS g
    OUTER APPLY
    (
        VALUES (g.[Pin_Player_1]), (g.[Pin_Player_2])
    ) AS participant([AGAID])
    WHERE g.[Tournament_Code] = t.[Tournament_Code]
) AS stats
ORDER BY t.[Tournament_Date] DESC, t.[Tournament_Code]
""",
        [],
    )
    player_detail_rows = query_rows(
        conn_str,
        current_ratings_cte()
        + f"""
SELECT
    m.[AGAID],
    m.[FirstName],
    m.[LastName],
    m.[State],
    m.[ExpirationDate],
    c.[ChapterCode],
    c.[ChapterName],
    cr.[Rating],
    cr.[Sigma],
    stats.[GameCount],
    stats.[TournamentCount],
    stats.[OpponentCount],
    stats.[Wins],
    stats.[Losses],
    stats.[LatestEventDate]
FROM [membership].[members] AS m
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
LEFT JOIN current_ratings AS cr
    ON cr.[AGAID] = m.[AGAID]
OUTER APPLY
(
    SELECT
        COUNT(*) AS [GameCount],
        COUNT(DISTINCT player_games.[Tournament_Code]) AS [TournamentCount],
        COUNT(DISTINCT player_games.[OpponentAGAID]) AS [OpponentCount],
        SUM(CASE WHEN player_games.[Outcome] = 'W' THEN 1 ELSE 0 END) AS [Wins],
        SUM(CASE WHEN player_games.[Outcome] = 'L' THEN 1 ELSE 0 END) AS [Losses],
        MAX(player_games.[Game_Date]) AS [LatestEventDate]
    FROM
    (
        SELECT
            g.[Tournament_Code],
            g.[Game_Date],
            g.[Pin_Player_2] AS [OpponentAGAID],
            CASE
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'B' THEN 'W' ELSE 'L' END
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'W' THEN 'W' ELSE 'L' END
                ELSE NULL
            END AS [Outcome]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_1] = m.[AGAID]
        UNION ALL
        SELECT
            g.[Tournament_Code],
            g.[Game_Date],
            g.[Pin_Player_1] AS [OpponentAGAID],
            CASE
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'B' THEN 'W' ELSE 'L' END
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'W' THEN 'W' ELSE 'L' END
                ELSE NULL
            END AS [Outcome]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_2] = m.[AGAID]
    ) AS player_games
) AS stats
WHERE m.[AGAID] < {MAX_MEMBER_AGAID}
""",
        [],
    )
    player_tournament_rows = query_rows(
        conn_str,
        f"""
SELECT
    tournament_games.[AGAID],
    tournament_games.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date],
    t.[City],
    t.[State_Code],
    tournament_games.[GamesPlayed],
    tournament_games.[Wins],
    tournament_games.[Losses],
    tournament_games.[LatestDate]
FROM
(
    SELECT
        player_games.[AGAID],
        player_games.[Tournament_Code],
        COUNT(*) AS [GamesPlayed],
        SUM(CASE WHEN player_games.[Outcome] = 'W' THEN 1 ELSE 0 END) AS [Wins],
        SUM(CASE WHEN player_games.[Outcome] = 'L' THEN 1 ELSE 0 END) AS [Losses],
        MAX(player_games.[Game_Date]) AS [LatestDate]
    FROM
    (
        SELECT
            g.[Pin_Player_1] AS [AGAID],
            g.[Tournament_Code],
            g.[Game_Date],
            CASE
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'B' THEN 'W' ELSE 'L' END
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'W' THEN 'W' ELSE 'L' END
                ELSE NULL
            END AS [Outcome]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_1] IS NOT NULL AND g.[Pin_Player_1] < {MAX_MEMBER_AGAID}
        UNION ALL
        SELECT
            g.[Pin_Player_2] AS [AGAID],
            g.[Tournament_Code],
            g.[Game_Date],
            CASE
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'B' THEN 'W' ELSE 'L' END
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'W' THEN 'W' ELSE 'L' END
                ELSE NULL
            END AS [Outcome]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_2] IS NOT NULL AND g.[Pin_Player_2] < {MAX_MEMBER_AGAID}
    ) AS player_games
    GROUP BY player_games.[AGAID], player_games.[Tournament_Code]
) AS tournament_games
LEFT JOIN [ratings].[tournaments] AS t
    ON t.[Tournament_Code] = tournament_games.[Tournament_Code]
ORDER BY tournament_games.[AGAID], tournament_games.[LatestDate] DESC, tournament_games.[Tournament_Code]
""",
        [],
    )
    opponent_rows = query_rows(
        conn_str,
        f"""
SELECT
    opponent_stats.[AGAID],
    opponent_stats.[OpponentAGAID] AS [AGAID_Opponent],
    m.[FirstName],
    m.[LastName],
    m.[State],
    c.[ChapterCode],
    c.[ChapterName],
    opponent_stats.[GamesPlayed],
    opponent_stats.[Wins],
    opponent_stats.[Losses],
    opponent_stats.[LatestDate]
FROM
(
    SELECT
        player_games.[AGAID],
        player_games.[OpponentAGAID],
        COUNT(*) AS [GamesPlayed],
        SUM(CASE WHEN player_games.[Outcome] = 'W' THEN 1 ELSE 0 END) AS [Wins],
        SUM(CASE WHEN player_games.[Outcome] = 'L' THEN 1 ELSE 0 END) AS [Losses],
        MAX(player_games.[Game_Date]) AS [LatestDate]
    FROM
    (
        SELECT
            g.[Pin_Player_1] AS [AGAID],
            g.[Pin_Player_2] AS [OpponentAGAID],
            g.[Game_Date],
            CASE
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'B' THEN 'W' ELSE 'L' END
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'W' THEN 'W' ELSE 'L' END
                ELSE NULL
            END AS [Outcome]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_1] IS NOT NULL AND g.[Pin_Player_1] < {MAX_MEMBER_AGAID}
        UNION ALL
        SELECT
            g.[Pin_Player_2] AS [AGAID],
            g.[Pin_Player_1] AS [OpponentAGAID],
            g.[Game_Date],
            CASE
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'B' THEN 'W' ELSE 'L' END
                WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'W' THEN 'W' ELSE 'L' END
                ELSE NULL
            END AS [Outcome]
        FROM [ratings].[games] AS g
        WHERE g.[Pin_Player_2] IS NOT NULL AND g.[Pin_Player_2] < {MAX_MEMBER_AGAID}
    ) AS player_games
    WHERE player_games.[OpponentAGAID] IS NOT NULL
    GROUP BY player_games.[AGAID], player_games.[OpponentAGAID]
) AS opponent_stats
LEFT JOIN [membership].[members] AS m
    ON m.[AGAID] = opponent_stats.[OpponentAGAID]
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
ORDER BY opponent_stats.[AGAID], opponent_stats.[GamesPlayed] DESC, opponent_stats.[LatestDate] DESC, opponent_stats.[OpponentAGAID]
""",
        [],
    )
    player_recent_game_rows = query_rows(
        conn_str,
        current_ratings_cte()
        + f"""
SELECT
    player_games.[AGAID],
    player_games.[Game_ID],
    player_games.[Game_Date],
    player_games.[Round],
    player_games.[ResultText],
    player_games.[Outcome],
    player_games.[Color],
    player_games.[Handicap],
    player_games.[Sgf_Code],
    player_games.[PlayerRank],
    player_games.[OpponentRank],
    player_games.[OpponentAGAID],
    player_games.[Tournament_Code],
    t.[Tournament_Descr],
    m.[FirstName],
    m.[LastName]
FROM
(
    SELECT
        g.[Pin_Player_1] AS [AGAID],
        g.[Game_ID],
        g.[Game_Date],
        g.[Round],
        g.[Result] AS [ResultText],
        g.[Color_1] AS [Color],
        g.[Handicap] AS [Handicap],
        g.[Sgf_Code],
        g.[Rank_1] AS [PlayerRank],
        g.[Rank_2] AS [OpponentRank],
        g.[Pin_Player_2] AS [OpponentAGAID],
        g.[Tournament_Code],
        CASE
            WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'B' THEN 'W' ELSE 'L' END
            WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_1], '')) = 'W' THEN 'W' ELSE 'L' END
            ELSE NULL
        END AS [Outcome]
    FROM [ratings].[games] AS g
    WHERE g.[Pin_Player_1] IS NOT NULL AND g.[Pin_Player_1] < {MAX_MEMBER_AGAID}
    UNION ALL
    SELECT
        g.[Pin_Player_2] AS [AGAID],
        g.[Game_ID],
        g.[Game_Date],
        g.[Round],
        g.[Result] AS [ResultText],
        g.[Color_2] AS [Color],
        g.[Handicap] AS [Handicap],
        g.[Sgf_Code],
        g.[Rank_2] AS [PlayerRank],
        g.[Rank_1] AS [OpponentRank],
        g.[Pin_Player_1] AS [OpponentAGAID],
        g.[Tournament_Code],
        CASE
            WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'B%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'B' THEN 'W' ELSE 'L' END
            WHEN UPPER(COALESCE(g.[Result], '')) LIKE 'W%' THEN CASE WHEN UPPER(COALESCE(g.[Color_2], '')) = 'W' THEN 'W' ELSE 'L' END
            ELSE NULL
        END AS [Outcome]
    FROM [ratings].[games] AS g
    WHERE g.[Pin_Player_2] IS NOT NULL AND g.[Pin_Player_2] < {MAX_MEMBER_AGAID}
) AS player_games
LEFT JOIN [ratings].[tournaments] AS t
    ON t.[Tournament_Code] = player_games.[Tournament_Code]
LEFT JOIN [membership].[members] AS m
    ON m.[AGAID] = player_games.[OpponentAGAID]
ORDER BY player_games.[AGAID], player_games.[Game_Date] DESC, player_games.[Tournament_Code], player_games.[Round]
""",
        [],
    )
    tournament_participant_rows = query_rows(
        conn_str,
        """
SELECT
    participant_rows.[Tournament_Code],
    participant_rows.[AGAID],
    m.[FirstName],
    m.[LastName],
    m.[State],
    c.[ChapterCode],
    c.[ChapterName],
    participant_rows.[GamesPlayed]
FROM
(
    SELECT
        g.[Tournament_Code],
        participant.[AGAID],
        COUNT(*) AS [GamesPlayed]
    FROM [ratings].[games] AS g
    OUTER APPLY
    (
        VALUES (g.[Pin_Player_1]), (g.[Pin_Player_2])
    ) AS participant([AGAID])
    WHERE participant.[AGAID] IS NOT NULL
    GROUP BY g.[Tournament_Code], participant.[AGAID]
) AS participant_rows
LEFT JOIN [membership].[members] AS m
    ON m.[AGAID] = participant_rows.[AGAID]
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
ORDER BY participant_rows.[Tournament_Code], participant_rows.[GamesPlayed] DESC, m.[LastName], m.[FirstName], participant_rows.[AGAID]
""",
        [],
    )
    tournament_game_rows = query_rows(
        conn_str,
        """
SELECT
    g.[Tournament_Code],
    g.[Game_ID],
    g.[Game_Date],
    g.[Round],
    g.[Result],
    g.[Handicap],
    g.[Komi],
    g.[Color_1],
    g.[Color_2],
    g.[Sgf_Code],
    g.[Rank_1],
    g.[Rank_2],
    g.[Pin_Player_1],
    g.[Pin_Player_2],
    p1.[FirstName] AS [Player1FirstName],
    p1.[LastName] AS [Player1LastName],
    p2.[FirstName] AS [Player2FirstName],
    p2.[LastName] AS [Player2LastName]
FROM [ratings].[games] AS g
LEFT JOIN [membership].[members] AS p1
    ON p1.[AGAID] = g.[Pin_Player_1]
LEFT JOIN [membership].[members] AS p2
    ON p2.[AGAID] = g.[Pin_Player_2]
ORDER BY g.[Tournament_Code], g.[Game_Date], TRY_CONVERT(INT, g.[Round]), g.[Round], g.[Game_ID]
""",
        [],
    )
    city_rows = query_rows(
        conn_str,
        """
SELECT DISTINCT t.[City]
FROM [ratings].[tournaments] AS t
WHERE t.[City] IS NOT NULL
  AND LTRIM(RTRIM(t.[City])) <> ''
ORDER BY t.[City]
""",
        [],
    )
    member_type_rows = query_rows(
        conn_str,
        f"""
SELECT DISTINCT m.[MemberType]
FROM [membership].[members] AS m
WHERE m.[AGAID] < {MAX_MEMBER_AGAID}
  AND m.[MemberType] IS NOT NULL
ORDER BY m.[MemberType]
""",
        [],
    )
    history_rows = query_rows(
        conn_str,
        f"""
SELECT [Pin_Player], [Elab_Date], [Rating], [Sigma]
FROM [ratings].[ratings]
WHERE [Pin_Player] IS NOT NULL
  AND [Pin_Player] < {MAX_MEMBER_AGAID}
  AND [Elab_Date] IS NOT NULL
  AND [Rating] IS NOT NULL
ORDER BY [Pin_Player], [Elab_Date], [id]
""",
        [],
    )

    included_agaids = {
        str(row.get("AGAID"))
        for row in player_summary_rows
        if (row.get("GameCount") or 0) > 0 and row.get("AGAID") is not None
    }

    players = [
        player_summary_payload(row)
        for row in player_summary_rows
        if str(row.get("AGAID")) in included_agaids
    ]
    players.sort(key=_snapshot_sort_key_player)
    tournaments = [tournament_summary_payload(row) for row in tournament_summary_rows]
    tournaments.sort(key=_snapshot_sort_key_tournament, reverse=True)

    player_details: dict[str, Any] = {}
    for row in player_detail_rows:
        if str(row.get("AGAID")) not in included_agaids:
            continue
        summary = player_summary_payload(row)
        summary.update(
            {
                "opponent_count": row.get("OpponentCount") or 0,
                "wins": row.get("Wins") or 0,
                "losses": row.get("Losses") or 0,
                "history_chart_url": f"/api/ratings-explorer/player-history.svg?agaid={row.get('AGAID')}",
            }
        )
        player_details[str(row.get("AGAID"))] = {
            "player": summary,
            "rating_history": [],
            "recent_tournaments": [],
            "opponents": [],
            "recent_games": [],
        }

    for row in player_tournament_rows:
        agaid = str(row.get("AGAID"))
        if agaid not in player_details or len(player_details[agaid]["recent_tournaments"]) >= 12:
            continue
        player_details[agaid]["recent_tournaments"].append(
            {
                "tournament_code": row.get("Tournament_Code"),
                "description": row.get("Tournament_Descr"),
                "tournament_date": json_safe_value(row.get("Tournament_Date")),
                "city": row.get("City"),
                "state": row.get("State_Code"),
                "games_played": row.get("GamesPlayed") or 0,
                "wins": row.get("Wins") or 0,
                "losses": row.get("Losses") or 0,
            }
        )

    for row in opponent_rows:
        agaid = str(row.get("AGAID"))
        if agaid not in player_details or len(player_details[agaid]["opponents"]) >= 16:
            continue
        player_details[agaid]["opponents"].append(
            {
                "agaid": row.get("AGAID_Opponent"),
                "display_name": member_name_from_row(
                    {
                        "AGAID": row.get("AGAID_Opponent"),
                        "FirstName": row.get("FirstName"),
                        "LastName": row.get("LastName"),
                    }
                ),
                "state": row.get("State"),
                "chapter_label": chapter_label(row),
                "games_played": row.get("GamesPlayed") or 0,
                "wins": row.get("Wins") or 0,
                "losses": row.get("Losses") or 0,
                "latest_game_date": json_safe_value(row.get("LatestDate")),
            }
        )

    for row in player_recent_game_rows:
        agaid = str(row.get("AGAID"))
        if agaid not in player_details or len(player_details[agaid]["recent_games"]) >= 20:
            continue
        player_details[agaid]["recent_games"].append(
            {
                "game_id": row.get("Game_ID"),
                "game_date": json_safe_value(row.get("Game_Date")),
                "round": row.get("Round"),
                "result_text": row.get("ResultText"),
                "outcome": row.get("Outcome"),
                "color": row.get("Color"),
                "handicap": row.get("Handicap"),
                "sgf_code": row.get("Sgf_Code"),
                "has_sgf": game_has_sgf(row),
                "sgf_viewer_url": game_sgf_viewer_url(row.get("Game_ID"), game_has_sgf(row)),
                "player_rank": row.get("PlayerRank"),
                "opponent_agaid": row.get("OpponentAGAID"),
                "opponent_name": member_name_from_row(
                    {
                        "AGAID": row.get("OpponentAGAID"),
                        "FirstName": row.get("FirstName"),
                        "LastName": row.get("LastName"),
                    }
                ),
                "opponent_rank": row.get("OpponentRank"),
                "tournament_code": row.get("Tournament_Code"),
                "tournament_description": row.get("Tournament_Descr"),
            }
        )

    tournament_details: dict[str, Any] = {}
    tournament_detail_snapshots: dict[str, Any] = {}
    for row in tournament_summary_rows:
        code = row.get("Tournament_Code")
        if not code:
            continue
        tournament_payload = {
            "tournament_code": code,
            "description": row.get("Tournament_Descr"),
            "tournament_date": json_safe_value(row.get("Tournament_Date")),
            "city": row.get("City"),
            "state": row.get("State_Code"),
            "country": row.get("Country_Code"),
            "rounds": row.get("Rounds"),
            "total_players": row.get("Total_Players"),
            "participant_count": row.get("ParticipantCount") or 0,
            "game_count": row.get("GameCount") or 0,
            "latest_game_date": json_safe_value(row.get("LatestGameDate")),
            "wallist": row.get("Wallist"),
        }
        tournament_details[code] = {
            "tournament": dict(tournament_payload),
            "participants": [],
            "recent_games": [],
        }
        tournament_detail_snapshots[code] = {
            "tournament": dict(tournament_payload),
            "participants": [],
            "games": [],
            "recent_games": [],
        }

    for row in tournament_participant_rows:
        code = row.get("Tournament_Code")
        participant_payload = tournament_participant_payload(row)
        if code in tournament_detail_snapshots:
            tournament_detail_snapshots[code]["participants"].append(participant_payload)
        if code not in tournament_details or len(tournament_details[code]["participants"]) >= 24:
            continue
        tournament_details[code]["participants"].append(participant_payload)

    for row in tournament_game_rows:
        code = row.get("Tournament_Code")
        if code not in tournament_detail_snapshots:
            continue
        tournament_detail_snapshots[code]["games"].append(tournament_game_payload(row))

    for code, detail_payload in tournament_detail_snapshots.items():
        recent_games = list(reversed(detail_payload["games"][-20:]))
        detail_payload["recent_games"] = recent_games
        if code in tournament_details:
            tournament_details[code]["recent_games"] = recent_games

    rating_histories: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in history_rows:
        agaid = row.get("Pin_Player")
        if agaid is None or str(agaid) not in included_agaids:
            continue
        rating_histories[str(agaid)].append(
            {
                "date": json_safe_value(row.get("Elab_Date")),
                "rating": rounded_rating(row.get("Rating")),
                "sigma": rounded_rating(row.get("Sigma")) or 0.0,
            }
        )

    for agaid, detail in player_details.items():
        detail["rating_history"] = list(rating_histories.get(agaid, []))

    snapshot = {
        "meta": {
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "player_count": len(players),
            "tournament_count": len(tournaments),
            "tournament_detail_snapshot_count": len(tournament_detail_snapshots),
            "snapshot_version": 5,
        },
        "players": players,
        "tournaments": tournaments,
        "player_details": player_details,
        "tournament_details": tournament_details,
        "rating_histories": dict(rating_histories),
    }
    return snapshot, tournament_detail_snapshots


def build_snapshot(conn_str: str) -> dict[str, Any]:
    snapshot, _ = _build_snapshot_artifacts(conn_str)
    return snapshot


def build_startup_players_payload(snapshot: dict[str, Any], limit: int = 25) -> dict[str, Any]:
    players = [normalize_player_summary(item) for item in (snapshot.get("players") or [])[: max(1, int(limit))]]
    return {
        "meta": dict(snapshot.get("meta") or {}),
        "results": players,
    }


def build_player_search_snapshot_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "meta": dict(snapshot.get("meta") or {}),
        "players": [normalize_player_summary(item) for item in (snapshot.get("players") or [])],
    }


def refresh_snapshot(conn_str: str) -> dict[str, Any]:
    snapshot, tournament_detail_snapshots = _build_snapshot_artifacts(conn_str)
    save_snapshot(snapshot)
    save_startup_players(build_startup_players_payload(snapshot))
    save_player_search_snapshot(build_player_search_snapshot_payload(snapshot))
    save_tournament_detail_snapshots(tournament_detail_snapshots)
    return snapshot
