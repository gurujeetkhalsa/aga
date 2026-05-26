"""Export a read-only BayRate experiment dataset from SQL.

The exported files are designed for offline algorithm work:

* games.csv contains the historical games inside the benchmark window.
* ratings.csv contains only rating rows before the benchmark window, avoiding
  leakage from production ratings that were calculated during the benchmark.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string
from bayrate.stage_reports import StageSqlAdapter


GAME_COLUMNS = [
    "Game_ID",
    "Tournament_Code",
    "Game_Date",
    "Round",
    "Pin_Player_1",
    "Pin_Player_2",
    "Rank_1",
    "Rank_2",
    "Color_1",
    "Handicap",
    "Komi",
    "Result",
    "Rated",
    "Exclude",
    "Online",
]

RATING_COLUMNS = ["AGAID", "Rating", "Sigma", "Elab_Date", "Tournament_Code", "row_id"]

GAMES_SQL = """
WITH benchmark_events AS (
    SELECT
        COALESCE(g.[Tournament_Code], CONCAT(N'date:', CONVERT(nvarchar(10), g.[Game_Date], 23))) AS [Event_Key],
        MIN(g.[Game_Date]) AS [Event_Date],
        MIN(g.[Game_ID]) AS [First_Game_ID],
        MIN(r.[id]) AS [First_Rating_Row_ID]
    FROM [ratings].[games] AS g
    LEFT JOIN [ratings].[ratings] AS r
        ON r.[Tournament_Code] = g.[Tournament_Code]
    WHERE g.[Game_Date] >= ?
      AND (? IS NULL OR g.[Game_Date] <= ?)
    GROUP BY COALESCE(g.[Tournament_Code], CONCAT(N'date:', CONVERT(nvarchar(10), g.[Game_Date], 23)))
)
SELECT
    g.[Game_ID],
    g.[Tournament_Code],
    g.[Game_Date],
    g.[Round],
    g.[Pin_Player_1],
    g.[Pin_Player_2],
    g.[Rank_1],
    g.[Rank_2],
    g.[Color_1],
    g.[Handicap],
    g.[Komi],
    g.[Result],
    g.[Rated],
    g.[Exclude],
    g.[Online]
FROM [ratings].[games] AS g
INNER JOIN benchmark_events AS e
    ON e.[Event_Key] = COALESCE(g.[Tournament_Code], CONCAT(N'date:', CONVERT(nvarchar(10), g.[Game_Date], 23)))
WHERE e.[Event_Date] >= ?
  AND (? IS NULL OR e.[Event_Date] <= ?)
ORDER BY
    e.[Event_Date],
    CASE WHEN e.[First_Rating_Row_ID] IS NULL THEN 1 ELSE 0 END,
    e.[First_Rating_Row_ID],
    e.[First_Game_ID],
    e.[Event_Key],
    g.[Game_Date],
    g.[Round],
    g.[Game_ID]
"""

RATINGS_SQL = """
WITH benchmark_events AS (
    SELECT
        COALESCE(g.[Tournament_Code], CONCAT(N'date:', CONVERT(nvarchar(10), g.[Game_Date], 23))) AS [Event_Key],
        MIN(g.[Game_Date]) AS [Event_Date]
    FROM [ratings].[games] AS g
    WHERE g.[Game_Date] >= ?
      AND (? IS NULL OR g.[Game_Date] <= ?)
    GROUP BY COALESCE(g.[Tournament_Code], CONCAT(N'date:', CONVERT(nvarchar(10), g.[Game_Date], 23)))
),
benchmark_players AS (
    SELECT DISTINCT g.[Pin_Player_1] AS [Pin_Player]
    FROM [ratings].[games] AS g
    INNER JOIN benchmark_events AS e
        ON e.[Event_Key] = COALESCE(g.[Tournament_Code], CONCAT(N'date:', CONVERT(nvarchar(10), g.[Game_Date], 23)))
    WHERE g.[Pin_Player_1] IS NOT NULL
    UNION
    SELECT DISTINCT g.[Pin_Player_2] AS [Pin_Player]
    FROM [ratings].[games] AS g
    INNER JOIN benchmark_events AS e
        ON e.[Event_Key] = COALESCE(g.[Tournament_Code], CONCAT(N'date:', CONVERT(nvarchar(10), g.[Game_Date], 23)))
    WHERE g.[Pin_Player_2] IS NOT NULL
)
SELECT
    r.[Pin_Player] AS [AGAID],
    r.[Rating],
    r.[Sigma],
    r.[Elab_Date],
    r.[Tournament_Code],
    r.[id] AS [row_id]
FROM [ratings].[ratings] AS r
INNER JOIN benchmark_players AS p
    ON p.[Pin_Player] = r.[Pin_Player]
WHERE r.[Rating] IS NOT NULL
  AND r.[Sigma] IS NOT NULL
  AND r.[Elab_Date] < ?
ORDER BY r.[Pin_Player], r.[Elab_Date], r.[id]
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a local CSV dataset for BayRate sigma experiments.")
    parser.add_argument("--min-game-date", required=True, help="Benchmark window start date, YYYY-MM-DD.")
    parser.add_argument("--max-game-date", help="Optional benchmark window end date, YYYY-MM-DD.")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for games.csv, ratings.csv, metadata.json.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    connection_string = args.connection_string or get_sql_connection_string()
    if not connection_string:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        raise SystemExit(1)
    try:
        metadata = export_experiment_dataset(
            SqlAdapter(connection_string),
            output_dir=args.output_dir,
            min_game_date=date.fromisoformat(args.min_game_date),
            max_game_date=date.fromisoformat(args.max_game_date) if args.max_game_date else None,
        )
    except Exception as exc:
        print(f"Experiment dataset export failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print("", file=sys.stdout)
    print("BayRate Experiment Dataset Export", file=sys.stdout)
    print(f"  Output: {metadata['output_dir']}", file=sys.stdout)
    print(f"  Games: {metadata['game_count']}", file=sys.stdout)
    print(f"  Prior rating rows: {metadata['rating_count']}", file=sys.stdout)
    print(f"  Metadata: {metadata['metadata_path']}", file=sys.stdout)


def export_experiment_dataset(
    adapter: StageSqlAdapter,
    *,
    output_dir: Path,
    min_game_date: date,
    max_game_date: date | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    max_date_param = max_game_date
    game_params = (min_game_date, max_date_param, max_date_param, min_game_date, max_date_param, max_date_param)
    rating_params = (min_game_date, max_date_param, max_date_param, min_game_date)

    game_rows = adapter.query_rows(GAMES_SQL, game_params)
    rating_rows = adapter.query_rows(RATINGS_SQL, rating_params)

    games_path = output_dir / "games.csv"
    ratings_path = output_dir / "ratings.csv"
    metadata_path = output_dir / "metadata.json"
    write_csv(games_path, GAME_COLUMNS, game_rows, include_header=True)
    write_csv(ratings_path, RATING_COLUMNS, rating_rows, include_header=False)

    metadata = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "purpose": "BayRate sigma experiment dataset",
        "min_game_date": min_game_date.isoformat(),
        "max_game_date": max_game_date.isoformat() if max_game_date else None,
        "leakage_guard": "ratings.csv includes only rows with Elab_Date before min_game_date",
        "output_dir": str(output_dir),
        "games_path": str(games_path),
        "ratings_path": str(ratings_path),
        "metadata_path": str(metadata_path),
        "game_count": len(game_rows),
        "rating_count": len(rating_rows),
        "games_columns": GAME_COLUMNS,
        "ratings_columns": RATING_COLUMNS,
        "ratings_has_header": False,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2, default=_json_default) + "\n", encoding="utf-8")
    return metadata


def write_csv(path: Path, columns: list[str], rows: Iterable[dict[str, Any]], *, include_header: bool) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        if include_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_value(row.get(column)) for column in columns})


def _csv_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if value is None:
        return ""
    return value


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


if __name__ == "__main__":
    main()
