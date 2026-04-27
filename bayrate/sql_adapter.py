import json
import os
from contextlib import suppress
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

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


SqlStatement = tuple[str, tuple[Any, ...]]


class SqlAdapter:
    def __init__(self, connection_string: str) -> None:
        if not connection_string or not connection_string.strip():
            raise ValueError("SQL connection string is required.")
        self.connection_string = connection_string

    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        return query_rows(self.connection_string, query, tuple(params))

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        execute_statements(self.connection_string, list(statements))


def get_sql_connection_string() -> str | None:
    conn = os.environ.get("SQL_CONNECTION_STRING") or os.environ.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
    if conn and conn.strip():
        return conn

    app_root = Path(__file__).resolve().parent
    candidates = [
        app_root / "local.settings.json",
        app_root.parent / "local.settings.json",
        app_root.parent / "ratings-explorer-app" / "local.settings.json",
    ]
    for settings_path in candidates:
        if not settings_path.exists():
            continue
        with suppress(OSError, json.JSONDecodeError):
            values = json.loads(settings_path.read_text(encoding="utf-8")).get("Values", {})
            conn = values.get("SQL_CONNECTION_STRING") or values.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
            if conn and str(conn).strip():
                return str(conn)
    return None


def query_rows(conn_str: str, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    if pyodbc is not None:
        try:
            return _query_rows_via_odbc(conn_str, query, params)
        except Exception:
            if pytds is None:
                raise
    return _query_rows_via_tds(conn_str, query, params)


def execute_statements(conn_str: str, statements: list[SqlStatement]) -> None:
    if not statements:
        return
    if pyodbc is not None:
        try:
            _execute_statements_via_odbc(conn_str, statements)
            return
        except Exception:
            if pytds is None:
                raise
    _execute_statements_via_tds(conn_str, statements)


def _query_rows_via_odbc(conn_str: str, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(query, *params)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, record)) for record in cursor.fetchall()]
    finally:
        conn.close()


def _execute_statements_via_odbc(conn_str: str, statements: list[SqlStatement]) -> None:
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        for query, params in statements:
            cursor.execute(query, *params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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
        "database": parts.get("database") or parts["initial catalog"],
        "user": parts.get("uid") or parts["user id"],
        "password": parts.get("pwd") or parts["password"],
    }


def _tds_connect(conn_str: str):
    if pytds is None:
        raise RuntimeError("Neither pyodbc nor pytds is available for SQL access.")
    sql = _parse_sql_connection_string(conn_str)
    kwargs = {
        "server": sql["server"],
        "port": sql["port"],
        "database": sql["database"],
        "user": sql["user"],
        "password": sql["password"],
        "validate_host": True,
        "enc_login_only": False,
        "autocommit": False,
        "timeout": 60,
        "as_dict": True,
    }
    if certifi is not None:
        kwargs["cafile"] = certifi.where()
    return pytds.connect(**kwargs)


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


def _render_query(query: str, params: tuple[Any, ...]) -> str:
    rendered = query
    for value in params:
        rendered = rendered.replace("?", _sql_literal(value), 1)
    return rendered


def _query_rows_via_tds(conn_str: str, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    conn = _tds_connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(_render_query(query, params))
        return list(cursor.fetchall())
    finally:
        conn.close()


def _execute_statements_via_tds(conn_str: str, statements: list[SqlStatement]) -> None:
    conn = _tds_connect(conn_str)
    try:
        cursor = conn.cursor()
        for query, params in statements:
            cursor.execute(_render_query(query, params))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
