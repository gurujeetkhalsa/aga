"""TD list HTTP functions, redirects, SQL access helpers, and text renderers."""

import json
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
TDLIST_REDIRECT_URLS = {
    "A": os.environ.get("TDLIST_REDIRECT_URL_A", ""),
    "B": os.environ.get("TDLIST_REDIRECT_URL_B", ""),
    "N": os.environ.get("TDLIST_REDIRECT_URL_N", ""),
}

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


def _get_sql_connection_string() -> str | None:
    """Return the SQL connection string from app settings or local development settings."""
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


@app.function_name(name="GenerateTDListA")
@app.route(route="GenerateTDListA", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def generate_tdlist_a(req: func.HttpRequest) -> func.HttpResponse:
    """Generate the tab-delimited TDListA export using chapter codes."""
    return _generate_tdlist_response("A")


@app.function_name(name="GenerateTDListB")
@app.route(route="GenerateTDListB", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def generate_tdlist_b(req: func.HttpRequest) -> func.HttpResponse:
    """Generate the tab-delimited TDListB export using chapter names."""
    return _generate_tdlist_response("B")


@app.function_name(name="GenerateTDListN")
@app.route(route="GenerateTDListN", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def generate_tdlist_n(req: func.HttpRequest) -> func.HttpResponse:
    """Generate the fixed-width TDListN export for tournament-director tooling."""
    return _generate_tdlist_response("N")


@app.function_name(name="TDListShortA")
@app.route(route="tda", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def tdlist_short_a(req: func.HttpRequest) -> func.HttpResponse:
    """Redirect the short TDListA URL to the configured full export URL."""
    return _redirect_tdlist("A")


@app.function_name(name="TDListShortB")
@app.route(route="tdb", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def tdlist_short_b(req: func.HttpRequest) -> func.HttpResponse:
    """Redirect the short TDListB URL to the configured full export URL."""
    return _redirect_tdlist("B")


@app.function_name(name="TDListShortN")
@app.route(route="tdn", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def tdlist_short_n(req: func.HttpRequest) -> func.HttpResponse:
    """Redirect the short TDListN URL to the configured full export URL."""
    return _redirect_tdlist("N")


def _generate_tdlist_response(list_type: str) -> func.HttpResponse:
    """Fetch membership rows, render the requested TD list format, and return it as text."""
    conn_str = _get_sql_connection_string()
    if not conn_str:
        return func.HttpResponse("Missing SQL_CONNECTION_STRING application setting.", status_code=500)

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
    except Exception as exc:
        return func.HttpResponse(f"TDList generation failed: {exc}", status_code=500)


def _redirect_tdlist(list_type: str) -> func.HttpResponse:
    """Return an HTTP redirect for a configured TD list short route."""
    target_url = TDLIST_REDIRECT_URLS.get(list_type)
    if not target_url:
        return func.HttpResponse(f"Unsupported TDList type: {list_type}", status_code=500)
    return func.HttpResponse(status_code=302, headers={"Location": target_url})


def _fetch_tdlist_rows(conn_str: str) -> list[dict[str, Any]]:
    """Fetch current non-dropped members and their latest ratings for TD list rendering."""
    return _query_rows(conn_str, TDLIST_QUERY, (MAX_MEMBER_AGAID,))


def _query_rows(conn_str: str, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    """Run a SQL query using pyodbc when available, otherwise fall back to python-tds."""
    if pyodbc is not None:
        try:
            return _query_rows_via_odbc(conn_str, query, params)
        except Exception:
            if pytds is None:
                raise
    return _query_rows_via_tds(conn_str, query, params)


def _query_rows_via_odbc(conn_str: str, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    """Run a parameterized SQL query through an ODBC connection."""
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
    """Run a SQL query through python-tds using rendered literals."""
    conn = _tds_connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(_render_query(query, params))
        return list(cursor.fetchall())
    finally:
        conn.close()


def _tds_connect(conn_str: str):
    """Open a python-tds connection from an ADO-style SQL connection string."""
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
    """Parse an ADO-style SQL connection string into python-tds connection fields."""
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
    """Render a Python value as a SQL literal for the python-tds fallback path."""
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
    """Replace positional question-mark parameters with SQL literals for python-tds."""
    rendered = query
    for value in params:
        rendered = rendered.replace("?", _sql_literal(value), 1)
    return rendered


def _render_tdlist_tab(rows: list[dict[str, Any]], *, chapter_field: str) -> str:
    """Render TDListA or TDListB as tab-delimited text."""
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


def _render_tdlist_fixed_width(rows: list[dict[str, Any]]) -> str:
    """Render TDListN as fixed-width text with short member-type labels."""
    rendered_rows = []
    for row in rows:
        chapter_code = _tdlist_text(row.get("ChapterCode")) or "none"
        rendered_rows.append(
            f"{_tdlist_name(row):<28}"
            f"{str(row['AGAID'] or ''):>6} "
            f"{_tdlist_member_type_label(row.get('MemberType')):<5} "
            f"{_format_tdlist_decimal(row.get('Rating'), digits=1):>6} "
            f"{_format_tdlist_date(row.get('ExpirationDate')):>10} "
            f"{chapter_code:<4} "
            f"{_tdlist_text(row.get('State')):<2}"
        )
    return "\n".join(rendered_rows) + ("\n" if rendered_rows else "")


def _tdlist_name(row: dict[str, Any]) -> str:
    """Return the display name used in TD list exports."""
    last_name = _tdlist_text(row.get("LastName"))
    first_name = _tdlist_text(row.get("FirstName"))
    if last_name and first_name:
        return f"{last_name}, {first_name}"
    return last_name or first_name


def _tdlist_text(value: Any) -> str:
    """Return a stripped string value, treating nulls as blank text."""
    if value is None:
        return ""
    return str(value).strip()


def _tdlist_member_type_label(value: Any) -> str:
    """Map full membership type names to the short TDListN status labels."""
    text = _tdlist_text(value)
    key = re.sub(r"[^a-z0-9]+", "", text.lower())
    if not key:
        return ""
    if key.startswith("youth"):
        return "Youth"
    if key.startswith("life") or key.startswith("lifetime"):
        return "Life"
    if key.startswith("comp") or key.startswith("complimentary"):
        return "Comp"
    if "pass" in key:
        return "Pass"
    if key.startswith("full") or key.startswith("adult") or key.startswith("regular"):
        return "Full"
    return text[:5]


def _format_tdlist_decimal(value: Any, *, digits: int) -> str:
    """Format a numeric TD list value with the requested decimal places."""
    if value is None:
        return ""
    return f"{float(value):.{digits}f}"


def _format_tdlist_date(value: Any) -> str:
    """Format a date or datetime in the historical TD list month/day/year format."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return f"{value.month}/{value.day}/{value.year}"
    return str(value)
