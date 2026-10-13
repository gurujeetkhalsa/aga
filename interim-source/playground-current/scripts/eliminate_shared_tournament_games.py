"""Preview or delete shared game rows from one production tournament.

This cleanup removes only source tournament games whose ordered game signatures
already exist in a canonical tournament. It keeps source games that are unique
to the source tournament and does not delete either tournament row.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string  # noqa: E402


TOURNAMENT_SQL_TEMPLATE = """
SELECT
    [Tournament_Code],
    [Tournament_Descr],
    CAST([Tournament_Date] AS date) AS [Tournament_Date],
    [City],
    [State_Code],
    [Country_Code]
FROM [ratings].[tournaments]
WHERE [Tournament_Code] IN ({placeholders})
ORDER BY [Tournament_Code]
"""


GAME_SQL_TEMPLATE = """
SELECT
    [Game_ID],
    [Tournament_Code],
    CAST([Game_Date] AS date) AS [Game_Date],
    [Round],
    [Pin_Player_1],
    [Pin_Player_2],
    [Handicap],
    [Komi],
    [Result],
    [Sgf_Code]
FROM [ratings].[games]
WHERE [Tournament_Code] IN ({placeholders})
ORDER BY [Tournament_Code], [Game_Date], [Round], [Game_ID]
"""


RATING_SQL_TEMPLATE = """
SELECT
    [Tournament_Code],
    COUNT_BIG(*) AS [RatingRowCount],
    COUNT(DISTINCT [Pin_Player]) AS [RatedPlayerCount],
    MIN([id]) AS [FirstRatingRowID],
    MAX([id]) AS [LastRatingRowID],
    MIN(CAST([Elab_Date] AS date)) AS [FirstRatingDate],
    MAX(CAST([Elab_Date] AS date)) AS [LastRatingDate]
FROM [ratings].[ratings]
WHERE [Tournament_Code] IN ({placeholders})
GROUP BY [Tournament_Code]
ORDER BY [Tournament_Code]
"""


DELETE_SHARED_GAMES_GUARD_SQL = """
DECLARE @SourceCode nvarchar(32) = ?;
DECLARE @CanonicalCode nvarchar(32) = ?;
DECLARE @ExpectedDeleteCount int = ?;

IF @SourceCode = @CanonicalCode
BEGIN
    THROW 51070, N'Source and canonical tournament codes must be different.', 1;
END;

IF NOT EXISTS (SELECT 1 FROM [ratings].[tournaments] WHERE [Tournament_Code] = @SourceCode)
BEGIN
    THROW 51071, N'Source tournament was not found.', 1;
END;

IF NOT EXISTS (SELECT 1 FROM [ratings].[tournaments] WHERE [Tournament_Code] = @CanonicalCode)
BEGIN
    THROW 51072, N'Canonical tournament was not found.', 1;
END;

IF EXISTS (SELECT 1 FROM [ratings].[ratings] WHERE [Tournament_Code] = @SourceCode)
BEGIN
    THROW 51073, N'Source tournament has rating rows; this cleanup only handles game-only duplicate cleanup.', 1;
END;

IF EXISTS
(
    SELECT 1
    FROM [ratings].[games]
    WHERE [Tournament_Code] = @SourceCode
      AND NULLIF(LTRIM(RTRIM([Sgf_Code])), N'') IS NOT NULL
)
BEGIN
    THROW 51074, N'Source tournament has SGF-linked games; review manually before deleting game rows.', 1;
END;

DECLARE @RowsToDelete TABLE
(
    [Game_ID] int NOT NULL PRIMARY KEY
);

WITH source_rows AS
(
    SELECT
        [Game_ID],
        [_Signature],
        ROW_NUMBER() OVER (PARTITION BY [_Signature] ORDER BY [Game_ID]) AS [SignatureOrdinal]
    FROM
    (
        SELECT
            [Game_ID],
            CONCAT(
                COALESCE(CONVERT(nvarchar(10), CAST([Game_Date] AS date), 23), N'<null>'), N'|',
                COALESCE(CONVERT(nvarchar(32), [Round]), N'<null>'), N'|',
                COALESCE(CONVERT(nvarchar(32), [Pin_Player_1]), N'<null>'), N'|',
                COALESCE(CONVERT(nvarchar(32), [Pin_Player_2]), N'<null>'), N'|',
                COALESCE(CONVERT(nvarchar(32), [Handicap]), N'<null>'), N'|',
                COALESCE(CONVERT(nvarchar(32), CONVERT(decimal(18, 4), [Komi])), N'<null>'), N'|',
                UPPER(LTRIM(RTRIM(COALESCE([Result], N''))))
            ) AS [_Signature]
        FROM [ratings].[games]
        WHERE [Tournament_Code] = @SourceCode
    ) AS source_base
),
canonical_rows AS
(
    SELECT
        [_Signature],
        ROW_NUMBER() OVER (PARTITION BY [_Signature] ORDER BY [Game_ID]) AS [SignatureOrdinal]
    FROM
    (
        SELECT
            [Game_ID],
            CONCAT(
                COALESCE(CONVERT(nvarchar(10), CAST([Game_Date] AS date), 23), N'<null>'), N'|',
                COALESCE(CONVERT(nvarchar(32), [Round]), N'<null>'), N'|',
                COALESCE(CONVERT(nvarchar(32), [Pin_Player_1]), N'<null>'), N'|',
                COALESCE(CONVERT(nvarchar(32), [Pin_Player_2]), N'<null>'), N'|',
                COALESCE(CONVERT(nvarchar(32), [Handicap]), N'<null>'), N'|',
                COALESCE(CONVERT(nvarchar(32), CONVERT(decimal(18, 4), [Komi])), N'<null>'), N'|',
                UPPER(LTRIM(RTRIM(COALESCE([Result], N''))))
            ) AS [_Signature]
        FROM [ratings].[games]
        WHERE [Tournament_Code] = @CanonicalCode
    ) AS canonical_base
)
INSERT INTO @RowsToDelete ([Game_ID])
SELECT source_rows.[Game_ID]
FROM source_rows
INNER JOIN canonical_rows
    ON canonical_rows.[_Signature] = source_rows.[_Signature]
   AND canonical_rows.[SignatureOrdinal] = source_rows.[SignatureOrdinal];

IF (SELECT COUNT(*) FROM @RowsToDelete) <> @ExpectedDeleteCount
BEGIN
    THROW 51075, N'Shared game cleanup plan changed since preview. Preview again before executing.', 1;
END;

DELETE g
FROM [ratings].[games] AS g
INNER JOIN @RowsToDelete AS d
    ON d.[Game_ID] = g.[Game_ID];
"""


def main() -> None:
    args = parse_args()
    if args.execute and not args.confirm_eliminate_shared_games:
        print("--execute requires --confirm-eliminate-shared-games.", file=sys.stderr)
        raise SystemExit(2)

    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        raise SystemExit(1)

    adapter = SqlAdapter(conn_str)
    try:
        if args.execute:
            plan = eliminate_shared_games(
                adapter,
                source_code=args.source_code,
                canonical_code=args.canonical_code,
                confirm_eliminate_shared_games=True,
            )
        else:
            plan = build_shared_game_cleanup_plan(adapter, source_code=args.source_code, canonical_code=args.canonical_code)
    except Exception as exc:
        print(f"Shared game cleanup failed: {exc}", file=sys.stderr)
        raise SystemExit(1)

    print_shared_game_cleanup_plan(plan, sys.stdout)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-code", required=True, help="Tournament_Code to delete shared games from.")
    parser.add_argument("--canonical-code", required=True, help="Tournament_Code that already contains the games.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    parser.add_argument("--execute", action="store_true", help="Delete shared source games from production.")
    parser.add_argument(
        "--confirm-eliminate-shared-games",
        action="store_true",
        help="Required with --execute to acknowledge production game deletes.",
    )
    return parser.parse_args()


def eliminate_shared_games(
    adapter: Any,
    *,
    source_code: str,
    canonical_code: str,
    confirm_eliminate_shared_games: bool = False,
) -> dict[str, Any]:
    if not confirm_eliminate_shared_games:
        raise ValueError("confirm_eliminate_shared_games=True is required to delete production rows.")
    plan = build_shared_game_cleanup_plan(adapter, source_code=source_code, canonical_code=canonical_code)
    validate_plan_for_execution(plan)
    adapter.execute_statements(build_delete_statements(plan))
    plan["executed"] = True
    plan["post_cleanup"] = build_post_cleanup_summary(adapter, plan["source_code"], plan["canonical_code"])
    return plan


def build_shared_game_cleanup_plan(adapter: Any, *, source_code: str, canonical_code: str) -> dict[str, Any]:
    source = _clean_code(source_code, "source_code")
    canonical = _clean_code(canonical_code, "canonical_code")
    if source == canonical:
        raise ValueError("source_code and canonical_code must be different.")

    codes = [canonical, source]
    tournament_rows = load_tournament_rows(adapter, codes)
    game_rows = load_game_rows(adapter, codes)
    rating_rows = load_rating_summaries(adapter, codes)

    tournaments_by_code = {row.get("Tournament_Code"): row for row in tournament_rows}
    games_by_code = {code: [row for row in game_rows if row.get("Tournament_Code") == code] for code in codes}
    ratings_by_code = {row.get("Tournament_Code"): row for row in rating_rows}

    canonical_games = games_by_code[canonical]
    source_games = games_by_code[source]
    canonical_signatures = Counter(_game_signature(row) for row in canonical_games)
    source_signatures = Counter(_game_signature(row) for row in source_games)
    shared_signatures = source_signatures & canonical_signatures
    source_unique_signatures = source_signatures - canonical_signatures

    shared_game_rows = _sample_games_for_signatures(source_games, shared_signatures, limit=1000000)
    sample_shared_game_rows = shared_game_rows[:12]
    sample_source_unique_games = _sample_games_for_signatures(source_games, source_unique_signatures)

    plan = {
        "source_code": source,
        "canonical_code": canonical,
        "executed": False,
        "source_tournament": tournaments_by_code.get(source),
        "canonical_tournament": tournaments_by_code.get(canonical),
        "source_game_count": len(source_games),
        "canonical_game_count": len(canonical_games),
        "shared_game_count": len(shared_game_rows),
        "source_game_count_after_cleanup": len(source_games) - len(shared_game_rows),
        "source_distinct_game_signature_count": len(source_signatures),
        "canonical_distinct_game_signature_count": len(canonical_signatures),
        "source_rating_summary": ratings_by_code.get(source),
        "canonical_rating_summary": ratings_by_code.get(canonical),
        "source_sgf_game_count": sum(1 for row in source_games if str(row.get("Sgf_Code") or "").strip()),
        "source_game_id_range": _id_range(row.get("Game_ID") for row in source_games),
        "canonical_game_id_range": _id_range(row.get("Game_ID") for row in canonical_games),
        "delete_game_id_range": _id_range(row.get("Game_ID") for row in shared_game_rows),
        "sample_shared_game_rows": sample_shared_game_rows,
        "sample_source_unique_games": sample_source_unique_games,
    }
    plan["production_delete_count"] = plan["shared_game_count"]
    plan["ready_to_execute"] = _is_ready_to_execute(plan)
    plan["plan_hash"] = _stable_hash(_plan_fingerprint(plan))
    return plan


def validate_plan_for_execution(plan: dict[str, Any]) -> None:
    if not plan.get("source_tournament"):
        raise ValueError(f"Source tournament {plan.get('source_code')} was not found.")
    if not plan.get("canonical_tournament"):
        raise ValueError(f"Canonical tournament {plan.get('canonical_code')} was not found.")
    source_rating_count = _int_value((plan.get("source_rating_summary") or {}).get("RatingRowCount"))
    if source_rating_count:
        raise ValueError(f"Source tournament {plan.get('source_code')} has {source_rating_count} rating rows.")
    if int(plan.get("source_sgf_game_count") or 0):
        raise ValueError(f"Source tournament {plan.get('source_code')} has SGF-linked games.")
    if not int(plan.get("shared_game_count") or 0):
        raise ValueError("No shared source games would be deleted.")


def build_delete_statements(plan: dict[str, Any]) -> list[SqlStatement]:
    return [
        (
            DELETE_SHARED_GAMES_GUARD_SQL,
            (plan["source_code"], plan["canonical_code"], int(plan["shared_game_count"])),
        )
    ]


def build_post_cleanup_summary(adapter: Any, source_code: str, canonical_code: str) -> dict[str, Any]:
    plan = build_shared_game_cleanup_plan(adapter, source_code=source_code, canonical_code=canonical_code)
    return {
        "remaining_shared_game_count": plan["shared_game_count"],
        "source_game_count": plan["source_game_count"],
        "canonical_game_count": plan["canonical_game_count"],
        "source_rating_rows": _int_value((plan.get("source_rating_summary") or {}).get("RatingRowCount")),
    }


def load_tournament_rows(adapter: Any, codes: list[str]) -> list[dict[str, Any]]:
    query = TOURNAMENT_SQL_TEMPLATE.format(placeholders=_placeholders(codes))
    return [_json_safe_row(row) for row in adapter.query_rows(query, tuple(codes))]


def load_game_rows(adapter: Any, codes: list[str]) -> list[dict[str, Any]]:
    query = GAME_SQL_TEMPLATE.format(placeholders=_placeholders(codes))
    return [_json_safe_row(row) for row in adapter.query_rows(query, tuple(codes))]


def load_rating_summaries(adapter: Any, codes: list[str]) -> list[dict[str, Any]]:
    query = RATING_SQL_TEMPLATE.format(placeholders=_placeholders(codes))
    return [_json_safe_row(row) for row in adapter.query_rows(query, tuple(codes))]


def print_shared_game_cleanup_plan(plan: dict[str, Any], output: Any) -> None:
    print("Shared Tournament Game Cleanup Plan", file=output)
    print(f"  Source code: {plan['source_code']}", file=output)
    print(f"  Canonical code: {plan['canonical_code']}", file=output)
    print(f"  Source games before cleanup: {plan['source_game_count']}", file=output)
    print(f"  Shared source games to delete: {plan['shared_game_count']}", file=output)
    print(f"  Source games after cleanup: {plan['source_game_count_after_cleanup']}", file=output)
    print(f"  Canonical games retained: {plan['canonical_game_count']}", file=output)
    print(f"  Source rating rows: {_int_value((plan.get('source_rating_summary') or {}).get('RatingRowCount'))}", file=output)
    print(f"  Source SGF-linked games: {plan['source_sgf_game_count']}", file=output)
    print(f"  Source Game_IDs: {_format_id_range(plan['source_game_id_range'])}", file=output)
    print(f"  Delete Game_IDs: {_format_id_range(plan['delete_game_id_range'])}", file=output)
    print(f"  Estimated production deletes: {plan['production_delete_count']}", file=output)
    print(f"  Ready to execute: {'yes' if plan['ready_to_execute'] else 'no'}", file=output)
    print(f"  Plan hash: {plan['plan_hash']}", file=output)
    if plan.get("post_cleanup"):
        post = plan["post_cleanup"]
        print("  Post-cleanup verification:", file=output)
        print(f"    Remaining shared source games: {post['remaining_shared_game_count']}", file=output)
        print(f"    Source game rows: {post['source_game_count']}", file=output)
        print(f"    Canonical game rows: {post['canonical_game_count']}", file=output)
        print(f"    Source rating rows: {post['source_rating_rows']}", file=output)
    if plan.get("executed"):
        print("  Executed: yes", file=output)


def _is_ready_to_execute(plan: dict[str, Any]) -> bool:
    source_rating_count = _int_value((plan.get("source_rating_summary") or {}).get("RatingRowCount"))
    return (
        bool(plan.get("source_tournament"))
        and bool(plan.get("canonical_tournament"))
        and int(plan.get("shared_game_count") or 0) > 0
        and source_rating_count == 0
        and int(plan.get("source_sgf_game_count") or 0) == 0
    )


def _plan_fingerprint(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in plan.items()
        if key
        not in {
            "executed",
            "plan_hash",
            "post_cleanup",
            "sample_shared_game_rows",
            "sample_source_unique_games",
        }
    }


def _game_signature(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("Game_Date"),
        _optional_int(row.get("Round")),
        _optional_int(row.get("Pin_Player_1")),
        _optional_int(row.get("Pin_Player_2")),
        _optional_int(row.get("Handicap")),
        _optional_decimal_text(row.get("Komi")),
        str(row.get("Result") or "").strip().upper() or None,
    )


def _sample_games_for_signatures(
    rows: list[dict[str, Any]],
    signatures: Counter[tuple[Any, ...]],
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    if not signatures:
        return []
    remaining = Counter(signatures)
    result = []
    for row in rows:
        signature = _game_signature(row)
        if remaining[signature] <= 0:
            continue
        result.append(row)
        remaining[signature] -= 1
        if len(result) >= limit:
            break
    return result


def _stable_hash(payload: dict[str, Any]) -> str:
    stable_payload = json.dumps(payload, default=_json_safe_value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(stable_payload.encode("utf-8")).hexdigest()


def _id_range(values: Any) -> dict[str, int | None]:
    ids = [_optional_int(value) for value in values]
    ids = [value for value in ids if value is not None]
    if not ids:
        return {"first": None, "last": None}
    return {"first": min(ids), "last": max(ids)}


def _format_id_range(value: dict[str, int | None]) -> str:
    if not value or value.get("first") is None:
        return "none"
    if value.get("first") == value.get("last"):
        return str(value.get("first"))
    return f"{value.get('first')}..{value.get('last')}"


def _placeholders(values: list[Any]) -> str:
    return ", ".join("?" for _ in values)


def _clean_code(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required.")
    if len(text) > 32:
        raise ValueError(f"{name} must be 32 characters or fewer.")
    return text


def _int_value(value: Any) -> int:
    result = _optional_int(value)
    return result if result is not None else 0


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _optional_decimal_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return f"{Decimal(str(value)):.4f}"


def _json_safe_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _json_safe_value(value) for key, value in row.items()}


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


if __name__ == "__main__":
    main()
