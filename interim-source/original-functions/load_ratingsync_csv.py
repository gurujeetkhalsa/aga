import argparse
import csv
import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import pyodbc


TABLE_SPECS = {
    "games": {
        "table": "ratings.games",
        "columns": (
            "Game_ID",
            "Tournament_Code",
            "Game_Date",
            "Round",
            "Pin_Player_1",
            "Color_1",
            "Rank_1",
            "Pin_Player_2",
            "Color_2",
            "Rank_2",
            "Handicap",
            "Komi",
            "Result",
            "Sgf_Code",
            "Online",
            "Exclude",
            "Rated",
            "Elab_Date",
        ),
        "int_columns": {"Game_ID", "Pin_Player_1", "Pin_Player_2", "Handicap", "Komi", "Online", "Exclude", "Rated"},
        "date_columns": {"Game_Date", "Elab_Date"},
    },
    "tournaments": {
        "table": "ratings.tournaments",
        "columns": (
            "Tournament_Code",
            "Tournament_Descr",
            "Tournament_Date",
            "City",
            "State_Code",
            "Country_Code",
            "Rounds",
            "Total_Players",
            "Wallist",
            "Elab_Date",
            "status",
        ),
        "int_columns": {"Rounds", "Total_Players"},
        "date_columns": {"Tournament_Date", "Elab_Date"},
    },
}


def _load_connection_string() -> str:
    conn = os.environ.get("SQL_CONNECTION_STRING") or os.environ.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
    if conn:
        return conn

    settings_path = Path(__file__).with_name("local.settings.json")
    if settings_path.exists():
        settings = json.loads(settings_path.read_text())
        values = settings.get("Values", {})
        conn = values.get("SQL_CONNECTION_STRING") or values.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
        if conn:
            return conn

    raise RuntimeError("SQL connection string not found in environment or local.settings.json")


def _normalize_headers(fieldnames: Iterable[str]) -> dict[str, str]:
    return {name.strip().lower(): name for name in fieldnames if name and name.strip()}


def _parse_date(value: str) -> date | None:
    text = value.strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass

    raise ValueError(f"Unsupported date value: {value!r}")


def _parse_int(value: str) -> int | None:
    text = value.strip()
    if not text:
        return None
    return int(text)


def _coerce_value(column: str, raw_value: str | None, spec: dict[str, object]) -> object:
    if raw_value is None:
        return None

    text = raw_value.strip()
    if text == "":
        return None

    if column in spec["int_columns"]:
        return _parse_int(text)
    if column in spec["date_columns"]:
        return _parse_date(text)
    return text


def _read_rows(csv_path: Path, dataset: str) -> list[tuple[object, ...]]:
    spec = TABLE_SPECS[dataset]
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise RuntimeError(f"CSV has no header row: {csv_path}")

        header_lookup = _normalize_headers(reader.fieldnames)
        missing = [column for column in spec["columns"] if column.lower() not in header_lookup]
        if missing:
            raise RuntimeError(f"CSV is missing expected columns for {dataset}: {', '.join(missing)}")

        rows = []
        for row_number, row in enumerate(reader, start=2):
            try:
                rows.append(
                    tuple(
                        _coerce_value(column, row.get(header_lookup[column.lower()]), spec)
                        for column in spec["columns"]
                    )
                )
            except Exception as exc:
                raise RuntimeError(f"Failed parsing {csv_path.name} row {row_number}: {exc}") from exc
        return rows


def _get_next_game_id(cursor: pyodbc.Cursor) -> int:
    cursor.execute("SELECT COALESCE(MAX([Game_ID]), 0) + 1 FROM ratings.games")
    row = cursor.fetchone()
    return int(row[0])


def _prepare_game_rows_for_append(cursor: pyodbc.Cursor, rows: list[tuple[object, ...]]) -> list[tuple[object, ...]]:
    if not rows:
        return rows

    game_id_index = TABLE_SPECS["games"]["columns"].index("Game_ID")
    next_game_id = _get_next_game_id(cursor)
    prepared_rows = []
    for offset, row in enumerate(rows):
        mutable_row = list(row)
        mutable_row[game_id_index] = next_game_id + offset
        prepared_rows.append(tuple(mutable_row))
    return prepared_rows


def _append_sql_table(conn_str: str, dataset: str, rows: list[tuple[object, ...]]) -> int:
    spec = TABLE_SPECS[dataset]
    columns = spec["columns"]
    insert_sql = (
        f"INSERT INTO {spec['table']} ("
        + ", ".join(f"[{column}]" for column in columns)
        + ") VALUES ("
        + ", ".join("?" for _ in columns)
        + ")"
    )

    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        rows_to_insert = rows
        if dataset == "games":
            rows_to_insert = _prepare_game_rows_for_append(cursor, rows)
        if rows_to_insert:
            cursor.fast_executemany = True
            cursor.executemany(insert_sql, rows_to_insert)
        conn.commit()
        return len(rows_to_insert)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Append ratingsync landing table rows from CSV extracts.")
    parser.add_argument("--games", type=Path, help="Path to the full games CSV extract")
    parser.add_argument("--tournaments", type=Path, help="Path to the full tournaments CSV extract")
    parser.add_argument("--dry-run", action="store_true", help="Validate CSVs without loading SQL")
    args = parser.parse_args()

    requested = []
    if args.games:
        requested.append(("games", args.games))
    if args.tournaments:
        requested.append(("tournaments", args.tournaments))
    if not requested:
        parser.error("Provide at least one of --games or --tournaments")

    rowsets: dict[str, list[tuple[object, ...]]] = {}
    for dataset, path in requested:
        rowsets[dataset] = _read_rows(path, dataset)
        print(f"{dataset}: parsed {len(rowsets[dataset])} rows from {path}")

    if args.dry_run:
        return

    conn_str = _load_connection_string()
    for dataset, _ in requested:
        inserted = _append_sql_table(conn_str, dataset, rowsets[dataset])
        print(f"{dataset}: appended {inserted} rows into {TABLE_SPECS[dataset]['table']}")


if __name__ == "__main__":
    main()
