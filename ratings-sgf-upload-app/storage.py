from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any, Iterable

import certifi
import pytds
from azure.storage.blob import BlobServiceClient, ContentSettings

try:
    import pyodbc
except Exception:
    pyodbc = None


def sql_connection_string() -> str:
    value = os.environ.get("SQL_CONNECTION_STRING") or os.environ.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
    if not value or not value.strip():
        raise RuntimeError("SQL_CONNECTION_STRING is not configured.")
    return value


def _parse_connection_string(value: str) -> dict[str, Any]:
    parts: dict[str, str] = {}
    for item in value.split(";"):
        if "=" not in item:
            continue
        key, item_value = item.split("=", 1)
        parts[key.strip().lower()] = item_value.strip().strip("{}")
    server = parts["server"].replace("tcp:", "")
    host, port_text = (server.split(",", 1) + ["1433"])[:2]
    return {
        "server": host,
        "port": int(port_text),
        "database": parts.get("database") or parts["initial catalog"],
        "user": parts.get("uid") or parts["user id"],
        "password": parts.get("pwd") or parts["password"],
    }


def _connect():
    sql = _parse_connection_string(sql_connection_string())
    return pytds.connect(
        server=sql["server"],
        port=sql["port"],
        database=sql["database"],
        user=sql["user"],
        password=sql["password"],
        cafile=certifi.where(),
        validate_host=True,
        enc_login_only=False,
        autocommit=False,
        timeout=60,
        as_dict=True,
    )


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


def _render(query: str, params: Iterable[Any]) -> str:
    rendered = query
    for value in params:
        rendered = rendered.replace("?", _sql_literal(value), 1)
    return rendered


def query_rows(query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
    values = tuple(params)
    if pyodbc is not None:
        try:
            return _query_rows_odbc(query, values)
        except Exception:
            pass
    connection = _connect()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(_render(query, values))
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


def execute(query: str, params: Iterable[Any] = ()) -> None:
    values = tuple(params)
    if pyodbc is not None:
        try:
            _execute_odbc(query, values)
            return
        except Exception:
            pass
    connection = _connect()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(_render(query, values))
            connection.commit()
        finally:
            cursor.close()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _query_rows_odbc(query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    connection = pyodbc.connect(sql_connection_string())
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(query, *params)
            columns = [column[0] for column in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            connection.commit()
            return rows
        finally:
            cursor.close()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _execute_odbc(query: str, params: tuple[Any, ...]) -> None:
    connection = pyodbc.connect(sql_connection_string())
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(query, *params)
            connection.commit()
        finally:
            cursor.close()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def load_games(game_ids: list[int]) -> dict[int, dict[str, Any]]:
    if not game_ids:
        return {}
    placeholders = ",".join("?" for _ in game_ids)
    rows = query_rows(
        f"""
SELECT
    g.[Game_ID], g.[Tournament_Code], g.[Game_Date], g.[Round], g.[Result],
    g.[Handicap], g.[Komi], g.[Sgf_Code], g.[Pin_Player_1], g.[Pin_Player_2]
FROM [ratings].[games] AS g
WHERE g.[Game_ID] IN ({placeholders})
""",
        game_ids,
    )
    return {int(row["Game_ID"]): row for row in rows}


def update_game_sgf_code(game_id: int, sgf_code: str) -> None:
    execute(
        "UPDATE [ratings].[games] SET [Sgf_Code] = ? WHERE [Game_ID] = ?",
        (sgf_code, game_id),
    )


def _blob_service() -> BlobServiceClient:
    connection_string = os.environ.get("AzureWebJobsStorage")
    if connection_string:
        return BlobServiceClient.from_connection_string(connection_string)
    account_url = os.environ.get("AzureWebJobsStorage__blobServiceUri")
    if not account_url:
        raise RuntimeError("AzureWebJobsStorage or AzureWebJobsStorage__blobServiceUri is not configured.")
    from azure.identity import AzureCliCredential, ManagedIdentityCredential

    client_id = os.environ.get("AzureWebJobsStorage__clientId")
    if os.environ.get("IDENTITY_ENDPOINT") or os.environ.get("MSI_ENDPOINT"):
        credential = ManagedIdentityCredential(client_id=client_id) if client_id else ManagedIdentityCredential()
    else:
        credential = AzureCliCredential()
    return BlobServiceClient(account_url=account_url, credential=credential)


def upload_sgf(sgf_code: str, sgf_text: str) -> None:
    container_name = os.environ.get("RATINGS_SGF_CONTAINER", "ratings-game-sgf")
    container = _blob_service().get_container_client(container_name)
    container.get_blob_client(f"games/{sgf_code}.sgf").upload_blob(
        sgf_text.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/x-go-sgf; charset=utf-8"),
    )
