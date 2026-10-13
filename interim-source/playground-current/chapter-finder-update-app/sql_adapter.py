# Copyright 2026, American Go Association, All rights reserved

"""Small SQL Server adapter shared by the Chapter Finder portal endpoints."""

from __future__ import annotations

import os
from datetime import date, datetime
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
    """Execute parameterized SQL through pyodbc with a python-tds fallback."""

    def __init__(self, connection_string: str) -> None:
        if not connection_string or not connection_string.strip():
            raise ValueError("SQL connection string is required.")
        self.connection_string = connection_string

    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        return query_rows(self.connection_string, query, tuple(params))

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        execute_statements(self.connection_string, list(statements))


def get_sql_connection_string() -> str | None:
    """Return the configured Azure SQL connection string."""
    value = os.environ.get("SQL_CONNECTION_STRING") or os.environ.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
    return value.strip() if value and value.strip() else None


def query_rows(connection_string: str, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    """Execute a query and return dictionaries."""
    if pyodbc is not None:
        try:
            connection = pyodbc.connect(connection_string)
            try:
                cursor = connection.cursor()
                try:
                    cursor.execute(query, *params)
                    columns = [column[0] for column in cursor.description]
                    rows = [dict(zip(columns, record)) for record in cursor.fetchall()]
                    connection.commit()
                    return rows
                finally:
                    cursor.close()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
        except Exception:
            if pytds is None:
                raise
    return _query_rows_tds(connection_string, query, params)


def execute_statements(connection_string: str, statements: list[SqlStatement]) -> None:
    """Execute statements in one transaction."""
    if not statements:
        return
    if pyodbc is not None:
        try:
            connection = pyodbc.connect(connection_string)
            try:
                cursor = connection.cursor()
                try:
                    for query, params in statements:
                        cursor.execute(query, *params)
                    connection.commit()
                    return
                finally:
                    cursor.close()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
        except Exception:
            if pytds is None:
                raise
    _execute_statements_tds(connection_string, statements)


def _parse_connection_string(connection_string: str) -> dict[str, Any]:
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


def _tds_connect(connection_string: str):
    if pytds is None:
        raise RuntimeError("Neither pyodbc nor python-tds is available for SQL access.")
    sql = _parse_connection_string(connection_string)
    kwargs = {
        **sql,
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
    if isinstance(value, (date, datetime)):
        return "'" + value.isoformat() + "'"
    return "'" + str(value).replace("'", "''") + "'"


def _render_query(query: str, params: tuple[Any, ...]) -> str:
    rendered = query
    for value in params:
        rendered = rendered.replace("?", _sql_literal(value), 1)
    return rendered


def _query_rows_tds(connection_string: str, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    connection = _tds_connect(connection_string)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(_render_query(query, params))
            rows = list(cursor.fetchall())
            connection.commit()
            return rows
        finally:
            cursor.close()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _execute_statements_tds(connection_string: str, statements: list[SqlStatement]) -> None:
    connection = _tds_connect(connection_string)
    try:
        cursor = connection.cursor()
        try:
            for query, params in statements:
                cursor.execute(_render_query(query, params))
            connection.commit()
        finally:
            cursor.close()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
