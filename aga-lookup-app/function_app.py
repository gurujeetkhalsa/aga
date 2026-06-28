import json
import math
import os
import re
from contextlib import suppress
from datetime import date, datetime
from pathlib import Path
from typing import Any

import azure.functions as func

try:
    import certifi
except Exception:
    certifi = None

try:
    import pyodbc
except Exception:
    pyodbc = None

try:
    import pytds
except Exception:
    pytds = None


app = func.FunctionApp()

MAX_MEMBER_AGAID = 50000
AGA_LOOKUP_MEMBER_TYPES = {
    "youth": "YOUTH",
    "adult": "ADULT",
    "life": "LIFE",
    "comp": "COMP",
    "pass": "PASS",
}


def _get_sql_connection_string() -> str | None:
    conn = os.environ.get("SQL_CONNECTION_STRING") or os.environ.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
    if conn and conn.strip():
        return conn

    app_root = Path(__file__).resolve().parent
    for settings_path in (
        app_root / "local.settings.json",
        app_root.parent / "local.settings.json",
        app_root.parent / "membership-data-app" / "local.settings.json",
    ):
        if not settings_path.exists():
            continue
        with suppress(OSError, json.JSONDecodeError):
            values = json.loads(settings_path.read_text(encoding="utf-8")).get("Values", {})
            conn = values.get("SQL_CONNECTION_STRING") or values.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
            if conn and str(conn).strip():
                return str(conn)
    return None


def _json_response(payload: dict[str, Any], status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload, default=_json_safe_value),
        mimetype="application/json",
        status_code=status_code,
    )


def _text_response(message: str, status_code: int) -> func.HttpResponse:
    return func.HttpResponse(message, status_code=status_code)


def _normalized_query_param_name(name: str) -> str:
    return re.sub(r"[-_\s]+", "", str(name)).lower()


def _query_param(req: func.HttpRequest, *names: str, default: str = "") -> str:
    accepted_names = {_normalized_query_param_name(name) for name in names}
    for key, value in (req.params or {}).items():
        if _normalized_query_param_name(key) in accepted_names:
            return "" if value is None else str(value)
    return default


def _parse_optional_rating_filter(raw_text: str, name: str) -> tuple[float | None, func.HttpResponse | None]:
    if not raw_text:
        return None, None
    try:
        value = float(raw_text)
    except ValueError:
        return None, _text_response(f"Query parameter '{name}' must be numeric.", 400)
    if not math.isfinite(value):
        return None, _text_response(f"Query parameter '{name}' must be finite.", 400)
    return value, None


@app.function_name(name="LookupMembers")
@app.route(route="lookup-members", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def lookup_members(req: func.HttpRequest) -> func.HttpResponse:
    conn_str = _get_sql_connection_string()
    if not conn_str:
        return _text_response("Missing SQL_CONNECTION_STRING application setting.", 500)

    agaid_text = (req.params.get("agaid") or req.params.get("id") or "").strip()
    last_name_prefix = (
        req.params.get("last_name")
        or req.params.get("lastName")
        or req.params.get("last_name_prefix")
        or req.params.get("lastNamePrefix")
        or ""
    ).strip()
    first_name_prefix = (
        req.params.get("first_name")
        or req.params.get("firstName")
        or req.params.get("first_name_prefix")
        or req.params.get("firstNamePrefix")
        or ""
    ).strip()
    limit_text = (
        req.params.get("limit")
        or req.params.get("max_rows")
        or req.params.get("maxRows")
        or "25"
    ).strip()
    offset_text = (req.params.get("offset") or "0").strip()

    agaid = None
    if agaid_text:
        if not agaid_text.isdigit():
            return _text_response("Query parameter 'agaid' must be numeric.", 400)
        agaid = int(agaid_text)

    if agaid is None and not last_name_prefix and not first_name_prefix:
        return _text_response("At least one of 'agaid', 'last_name', or 'first_name' is required.", 400)

    try:
        limit = int(limit_text)
    except ValueError:
        return _text_response("Query parameter 'limit' must be an integer.", 400)

    try:
        offset = int(offset_text)
    except ValueError:
        return _text_response("Query parameter 'offset' must be an integer.", 400)

    if limit < 1:
        return _text_response("Query parameter 'limit' must be greater than 0.", 400)
    if offset < 0:
        return _text_response("Query parameter 'offset' must be 0 or greater.", 400)

    try:
        page_rows = _lookup_members(
            conn_str,
            agaid,
            last_name_prefix or None,
            first_name_prefix or None,
            limit,
            offset,
        )
    except Exception as exc:
        return _text_response(f"Lookup failed: {exc}", 500)

    has_more = len(page_rows) > limit
    rows = page_rows[:limit]
    return _json_response(
        {
            "count": len(rows),
            "limit": limit,
            "offset": offset,
            "has_more": has_more,
            "next_offset": (offset + limit) if has_more else None,
            "results": rows,
        }
    )


@app.function_name(name="AGALookup")
@app.route(route="AGALookup", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def aga_lookup(req: func.HttpRequest) -> func.HttpResponse:
    conn_str = _get_sql_connection_string()
    if not conn_str:
        return _text_response("Missing SQL_CONNECTION_STRING application setting.", 500)

    agaid_text = _query_param(req, "agaid", "id").strip()
    last_name_prefix = _query_param(req, "last_name", "last_name_prefix").strip()
    first_name_prefix = _query_param(req, "first_name", "first_name_prefix").strip()
    status_filter = _query_param(req, "status", "member_status").strip()
    min_rating_text = _query_param(req, "min_rating").strip()
    max_rating_text = _query_param(req, "max_rating").strip()
    chapter_code_prefix = _query_param(req, "chapter_code").strip()
    chapter_name_prefix = _query_param(req, "chapter_name").strip()
    member_type_filter = _query_param(req, "member_type").strip()
    state_filter = _query_param(req, "state").strip()
    limit_text = _query_param(req, "limit", "max_rows").strip()
    offset_text = _query_param(req, "offset", default="0").strip()

    agaid = None
    if agaid_text:
        if not agaid_text.isdigit():
            return _text_response("Query parameter 'agaid' must be numeric.", 400)
        agaid = int(agaid_text)

    allowed_statuses = {"active": "ACTIVE", "dropped": "DROPPED", "expired": "EXPIRED"}
    status = None
    if status_filter:
        status = allowed_statuses.get(status_filter.lower())
        if status is None:
            return _text_response("Query parameter 'status' must be one of: Active, Dropped, Expired.", 400)

    min_rating, error = _parse_optional_rating_filter(min_rating_text, "minRating")
    if error:
        return error
    max_rating, error = _parse_optional_rating_filter(max_rating_text, "maxRating")
    if error:
        return error
    if min_rating is not None and max_rating is not None and min_rating > max_rating:
        return _text_response("Query parameter 'minRating' must be less than or equal to 'maxRating'.", 400)

    member_type = None
    if member_type_filter:
        member_type = AGA_LOOKUP_MEMBER_TYPES.get(member_type_filter.lower())
        if member_type is None:
            return _text_response("Query parameter 'memberType' must be one of: Youth, Adult, Life, Comp, Pass.", 400)

    state = None
    if state_filter:
        if not re.fullmatch(r"[A-Za-z]{2}", state_filter):
            return _text_response("Query parameter 'state' must be a 2-character state abbreviation.", 400)
        state = state_filter.upper()

    has_lookup_filter = any(
        (
            agaid is not None,
            bool(last_name_prefix),
            bool(first_name_prefix),
            bool(status),
            min_rating is not None,
            max_rating is not None,
            bool(chapter_code_prefix),
            bool(chapter_name_prefix),
            bool(member_type),
            bool(state),
        )
    )
    if not has_lookup_filter:
        return _text_response(
            "At least one AGALookup filter is required. Use query parameters like 'agaid=754', "
            "'lastName=Smith', 'status=Active', or 'chapterCode=NY'.",
            400,
        )

    limit = None
    if limit_text:
        try:
            limit = int(limit_text)
        except ValueError:
            return _text_response("Query parameter 'limit' must be an integer.", 400)
        if limit < 1:
            return _text_response("Query parameter 'limit' must be greater than 0.", 400)

    try:
        offset = int(offset_text)
    except ValueError:
        return _text_response("Query parameter 'offset' must be an integer.", 400)
    if offset < 0:
        return _text_response("Query parameter 'offset' must be 0 or greater.", 400)

    try:
        page_rows = _aga_lookup_members(
            conn_str,
            agaid,
            last_name_prefix or None,
            first_name_prefix or None,
            status,
            min_rating,
            max_rating,
            chapter_code_prefix or None,
            chapter_name_prefix or None,
            member_type,
            state,
            limit,
            offset,
        )
    except Exception as exc:
        return _text_response(f"Lookup failed: {exc}", 500)

    has_more = bool(limit is not None and len(page_rows) > limit)
    rows = page_rows[:limit] if limit is not None else page_rows
    return _json_response(
        {
            "count": len(rows),
            "limit": limit,
            "offset": offset,
            "has_more": has_more,
            "next_offset": (offset + limit) if has_more and limit is not None else None,
            "results": rows,
        }
    )


def _lookup_members(
    conn_str: str,
    agaid: int | None,
    last_name_prefix: str | None,
    first_name_prefix: str | None,
    limit: int,
    offset: int,
) -> list[dict[str, Any]]:
    effective_limit = min(max(limit, 1), 100)
    effective_offset = max(offset, 0)
    return _query_rows(
        conn_str,
        "EXEC [api].[sp_lookup_members] @AGAID = ?, @LastNamePrefix = ?, @FirstNamePrefix = ?, @MaxRows = ?, @OffsetRows = ?",
        (agaid, last_name_prefix, first_name_prefix, effective_limit + 1, effective_offset),
    )


def _aga_lookup_members(
    conn_str: str,
    agaid: int | None,
    last_name_prefix: str | None,
    first_name_prefix: str | None,
    status: str | None,
    min_rating: float | None,
    max_rating: float | None,
    chapter_code_prefix: str | None,
    chapter_name_prefix: str | None,
    member_type: str | None,
    state: str | None,
    limit: int | None,
    offset: int,
) -> list[dict[str, Any]]:
    where_clauses = ["m.[AGAID] < ?"]
    params: list[Any] = [MAX_MEMBER_AGAID]

    if agaid is not None:
        where_clauses.append("m.[AGAID] = ?")
        params.append(agaid)
    if last_name_prefix:
        where_clauses.append("m.[LastName] LIKE ?")
        params.append(f"{last_name_prefix}%")
    if first_name_prefix:
        where_clauses.append("m.[FirstName] LIKE ?")
        params.append(f"{first_name_prefix}%")
    if status:
        where_clauses.append("UPPER(LTRIM(RTRIM(m.[Status]))) = ?")
        params.append(status)
    if min_rating is not None:
        where_clauses.append("cr.[Rating] >= ?")
        params.append(min_rating)
    if max_rating is not None:
        where_clauses.append("cr.[Rating] <= ?")
        params.append(max_rating)
    if chapter_code_prefix:
        where_clauses.append("UPPER(LTRIM(RTRIM(c.[ChapterCode]))) LIKE ?")
        params.append(f"{chapter_code_prefix.upper()}%")
    if chapter_name_prefix:
        where_clauses.append("UPPER(LTRIM(RTRIM(c.[ChapterName]))) LIKE ?")
        params.append(f"{chapter_name_prefix.upper()}%")
    if member_type:
        where_clauses.append("UPPER(LTRIM(RTRIM(m.[MemberType]))) = ?")
        params.append(member_type)
    if state:
        where_clauses.append("UPPER(LTRIM(RTRIM(m.[State]))) = ?")
        params.append(state)

    fetch_clause = ""
    if offset or limit is not None:
        fetch_clause = "OFFSET ? ROWS"
        params.append(offset)
        if limit is not None:
            fetch_clause += " FETCH NEXT ? ROWS ONLY"
            params.append(limit + 1)

    query = f"""
WITH current_ratings AS
(
    SELECT
        ranked.[AGAID],
        ranked.[Rating],
        ranked.[Sigma],
        ranked.[RatingDate]
    FROM
    (
        SELECT
            r.[Pin_Player] AS [AGAID],
            r.[Rating],
            r.[Sigma],
            r.[Elab_Date] AS [RatingDate],
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
    CASE
        WHEN NULLIF(LTRIM(RTRIM(m.[LastName])), N'') IS NOT NULL
             AND NULLIF(LTRIM(RTRIM(m.[FirstName])), N'') IS NOT NULL
            THEN LTRIM(RTRIM(m.[LastName])) + N', ' + LTRIM(RTRIM(m.[FirstName]))
        ELSE COALESCE(NULLIF(LTRIM(RTRIM(m.[LastName])), N''), NULLIF(LTRIM(RTRIM(m.[FirstName])), N''), N'')
    END AS [DisplayName],
    m.[Status],
    cr.[Rating],
    cr.[Sigma],
    cr.[RatingDate],
    m.[MemberType],
    m.[ExpirationDate],
    c.[ChapterCode],
    c.[ChapterName],
    m.[State]
FROM [membership].[members] AS m
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
LEFT JOIN current_ratings AS cr
    ON cr.[AGAID] = m.[AGAID]
WHERE {" AND ".join(where_clauses)}
ORDER BY m.[LastName], m.[FirstName], m.[AGAID]
{fetch_clause}
"""
    return _query_rows(conn_str, query, tuple(params))


def _query_rows(conn_str: str, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    if pyodbc is not None:
        try:
            return _query_rows_via_odbc(conn_str, query, params)
        except Exception:
            if pytds is None:
                raise
    return _query_rows_via_tds(conn_str, query, params)


def _query_rows_via_odbc(conn_str: str, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    assert pyodbc is not None
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(query, *params)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, record)) for record in cursor.fetchall()]
    finally:
        conn.close()


def _query_rows_via_tds(conn_str: str, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    conn = _tds_connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(_render_query(query, params))
        return list(cursor.fetchall())
    finally:
        conn.close()


def _tds_connect(conn_str: str):
    if pytds is None:
        raise RuntimeError("Neither pyodbc nor python-tds is available for SQL access.")
    sql = _parse_sql_connection_string(conn_str)
    kwargs = {
        "server": sql["server"],
        "port": sql["port"],
        "database": sql["database"],
        "user": sql["user"],
        "password": sql["password"],
        "validate_host": True,
        "enc_login_only": False,
        "autocommit": True,
        "timeout": 60,
        "as_dict": True,
    }
    if certifi is not None:
        kwargs["cafile"] = certifi.where()
    return pytds.connect(**kwargs)


def _parse_sql_connection_string(connection_string: str) -> dict[str, Any]:
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
        "database": parts.get("database") or parts["initial catalog"],
        "user": parts.get("uid") or parts.get("user id"),
        "password": parts.get("pwd") or parts.get("password"),
    }


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (datetime, date)):
        return "'" + value.isoformat() + "'"
    return "N'" + str(value).replace("'", "''") + "'"


def _render_query(query: str, params: tuple[Any, ...]) -> str:
    rendered = query
    for value in params:
        rendered = rendered.replace("?", _sql_literal(value), 1)
    return rendered


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value
