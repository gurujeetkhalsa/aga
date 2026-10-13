"""Preview or eliminate a duplicate production tournament row.

The cleanup is intentionally narrow: it only deletes the duplicate tournament
and its games after verifying that every duplicate game has an exact matching
game under the canonical tournament code and that no rating rows reference the
duplicate code.
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
    [Country_Code],
    [Host_ChapterID],
    [Host_ChapterCode],
    [Host_ChapterName],
    [Reward_Event_Key],
    [Reward_Event_Name],
    [Reward_Is_State_Championship]
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


DELETE_GUARD_SQL = """
DECLARE @DuplicateCode nvarchar(32) = ?;
DECLARE @CanonicalCode nvarchar(32) = ?;
DECLARE @IgnoreKomi bit = ?;

IF @DuplicateCode = @CanonicalCode
BEGIN
    THROW 51060, N'Duplicate and canonical tournament codes must be different.', 1;
END;

IF NOT EXISTS (SELECT 1 FROM [ratings].[tournaments] WHERE [Tournament_Code] = @DuplicateCode)
BEGIN
    THROW 51061, N'Duplicate tournament was not found.', 1;
END;

IF NOT EXISTS (SELECT 1 FROM [ratings].[tournaments] WHERE [Tournament_Code] = @CanonicalCode)
BEGIN
    THROW 51062, N'Canonical tournament was not found.', 1;
END;

IF EXISTS (SELECT 1 FROM [ratings].[ratings] WHERE [Tournament_Code] = @DuplicateCode)
BEGIN
    THROW 51063, N'Duplicate tournament still has rating rows and cannot be deleted by this cleanup.', 1;
END;

IF EXISTS
(
    SELECT 1
    FROM
    (
        SELECT [_Signature], COUNT_BIG(*) AS [SourceCount]
        FROM
        (
            SELECT
                CONCAT(
                    COALESCE(CONVERT(nvarchar(10), CAST([Game_Date] AS date), 23), N'<null>'), N'|',
                    COALESCE(CONVERT(nvarchar(32), [Round]), N'<null>'), N'|',
                    COALESCE(CONVERT(nvarchar(32), [Pin_Player_1]), N'<null>'), N'|',
                    COALESCE(CONVERT(nvarchar(32), [Pin_Player_2]), N'<null>'), N'|',
                    COALESCE(CONVERT(nvarchar(32), [Handicap]), N'<null>'), N'|',
                    CASE
                        WHEN @IgnoreKomi = 1 THEN N'<ignored>'
                        ELSE COALESCE(CONVERT(nvarchar(32), CONVERT(decimal(18, 4), [Komi])), N'<null>')
                    END, N'|',
                    UPPER(LTRIM(RTRIM(COALESCE([Result], N''))))
                ) AS [_Signature]
            FROM [ratings].[games]
            WHERE [Tournament_Code] = @DuplicateCode
        ) AS source_rows
        GROUP BY [_Signature]
    ) AS source_sigs
    LEFT JOIN
    (
        SELECT [_Signature], COUNT_BIG(*) AS [TargetCount]
        FROM
        (
            SELECT
                CONCAT(
                    COALESCE(CONVERT(nvarchar(10), CAST([Game_Date] AS date), 23), N'<null>'), N'|',
                    COALESCE(CONVERT(nvarchar(32), [Round]), N'<null>'), N'|',
                    COALESCE(CONVERT(nvarchar(32), [Pin_Player_1]), N'<null>'), N'|',
                    COALESCE(CONVERT(nvarchar(32), [Pin_Player_2]), N'<null>'), N'|',
                    COALESCE(CONVERT(nvarchar(32), [Handicap]), N'<null>'), N'|',
                    CASE
                        WHEN @IgnoreKomi = 1 THEN N'<ignored>'
                        ELSE COALESCE(CONVERT(nvarchar(32), CONVERT(decimal(18, 4), [Komi])), N'<null>')
                    END, N'|',
                    UPPER(LTRIM(RTRIM(COALESCE([Result], N''))))
                ) AS [_Signature]
            FROM [ratings].[games]
            WHERE [Tournament_Code] = @CanonicalCode
        ) AS target_rows
        GROUP BY [_Signature]
    ) AS target_sigs
        ON target_sigs.[_Signature] = source_sigs.[_Signature]
    WHERE source_sigs.[SourceCount] > COALESCE(target_sigs.[TargetCount], 0)
)
BEGIN
    THROW 51064, N'Duplicate tournament has one or more games that are not exact duplicates of canonical games.', 1;
END;
"""


DELETE_GAMES_SQL = """
DELETE FROM [ratings].[games]
WHERE [Tournament_Code] = ?
"""


DELETE_TOURNAMENT_SQL = """
DELETE FROM [ratings].[tournaments]
WHERE [Tournament_Code] = ?
"""


def main() -> None:
    args = parse_args()
    if args.execute and not args.confirm_eliminate_duplicate_tournament:
        print("--execute requires --confirm-eliminate-duplicate-tournament.", file=sys.stderr)
        raise SystemExit(2)

    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        raise SystemExit(1)

    adapter = SqlAdapter(conn_str)
    try:
        if args.execute:
            plan = eliminate_duplicate_tournament(
                adapter,
                duplicate_code=args.duplicate_code,
                canonical_code=args.canonical_code,
                ignore_komi=args.ignore_komi,
                confirm_eliminate_duplicate_tournament=True,
            )
        else:
            plan = build_duplicate_tournament_plan(
                adapter,
                duplicate_code=args.duplicate_code,
                canonical_code=args.canonical_code,
                ignore_komi=args.ignore_komi,
            )
    except Exception as exc:
        print(f"Duplicate tournament cleanup failed: {exc}", file=sys.stderr)
        raise SystemExit(1)

    print_duplicate_tournament_plan(plan, sys.stdout)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duplicate-code", required=True, help="Tournament_Code to delete.")
    parser.add_argument("--canonical-code", required=True, help="Tournament_Code that already contains the games.")
    parser.add_argument(
        "--ignore-komi",
        action="store_true",
        help="Allow duplicate games to match canonical games when only Komi differs.",
    )
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    parser.add_argument("--execute", action="store_true", help="Delete duplicate production games and tournament row.")
    parser.add_argument(
        "--confirm-eliminate-duplicate-tournament",
        action="store_true",
        help="Required with --execute to acknowledge production deletes.",
    )
    return parser.parse_args()


def eliminate_duplicate_tournament(
    adapter: Any,
    *,
    duplicate_code: str,
    canonical_code: str,
    ignore_komi: bool = False,
    confirm_eliminate_duplicate_tournament: bool = False,
) -> dict[str, Any]:
    if not confirm_eliminate_duplicate_tournament:
        raise ValueError("confirm_eliminate_duplicate_tournament=True is required to delete production rows.")
    plan = build_duplicate_tournament_plan(
        adapter,
        duplicate_code=duplicate_code,
        canonical_code=canonical_code,
        ignore_komi=ignore_komi,
    )
    validate_plan_for_execution(plan)
    adapter.execute_statements(build_delete_statements(plan))
    plan["executed"] = True
    plan["post_cleanup"] = build_post_cleanup_summary(adapter, duplicate_code, canonical_code)
    return plan


def build_duplicate_tournament_plan(
    adapter: Any,
    *,
    duplicate_code: str,
    canonical_code: str,
    ignore_komi: bool = False,
) -> dict[str, Any]:
    duplicate = _clean_code(duplicate_code, "duplicate_code")
    canonical = _clean_code(canonical_code, "canonical_code")
    if duplicate == canonical:
        raise ValueError("duplicate_code and canonical_code must be different.")

    codes = [canonical, duplicate]
    tournament_rows = load_tournament_rows(adapter, codes)
    game_rows = load_game_rows(adapter, codes)
    rating_rows = load_rating_summaries(adapter, codes)

    by_code = {row.get("Tournament_Code"): row for row in tournament_rows}
    games_by_code = {code: [row for row in game_rows if row.get("Tournament_Code") == code] for code in codes}
    ratings_by_code = {row.get("Tournament_Code"): row for row in rating_rows}

    canonical_games = games_by_code[canonical]
    duplicate_games = games_by_code[duplicate]
    canonical_signatures = Counter(_game_signature(row, ignore_komi=ignore_komi) for row in canonical_games)
    duplicate_signatures = Counter(_game_signature(row, ignore_komi=ignore_komi) for row in duplicate_games)
    missing_signatures = duplicate_signatures - canonical_signatures
    extra_canonical_signatures = canonical_signatures - duplicate_signatures

    plan = {
        "duplicate_code": duplicate,
        "canonical_code": canonical,
        "ignore_komi": bool(ignore_komi),
        "executed": False,
        "duplicate_tournament": by_code.get(duplicate),
        "canonical_tournament": by_code.get(canonical),
        "duplicate_game_count": len(duplicate_games),
        "canonical_game_count": len(canonical_games),
        "duplicate_distinct_game_signature_count": len(duplicate_signatures),
        "canonical_distinct_game_signature_count": len(canonical_signatures),
        "duplicate_games_missing_from_canonical_count": sum(missing_signatures.values()),
        "canonical_extra_game_count": sum(extra_canonical_signatures.values()),
        "duplicate_rating_summary": ratings_by_code.get(duplicate),
        "canonical_rating_summary": ratings_by_code.get(canonical),
        "duplicate_game_id_range": _id_range(row.get("Game_ID") for row in duplicate_games),
        "canonical_game_id_range": _id_range(row.get("Game_ID") for row in canonical_games),
        "sample_duplicate_games_missing_from_canonical": _sample_games_for_signatures(
            duplicate_games,
            missing_signatures,
            ignore_komi=ignore_komi,
        ),
        "sample_canonical_extra_games": _sample_games_for_signatures(
            canonical_games,
            extra_canonical_signatures,
            ignore_komi=ignore_komi,
        ),
    }
    plan["production_delete_count"] = plan["duplicate_game_count"] + (1 if plan["duplicate_tournament"] else 0)
    plan["ready_to_execute"] = _is_ready_to_execute(plan)
    plan["plan_hash"] = _stable_hash(_plan_fingerprint(plan))
    return plan


def validate_plan_for_execution(plan: dict[str, Any]) -> None:
    if not plan.get("duplicate_tournament"):
        raise ValueError(f"Duplicate tournament {plan.get('duplicate_code')} was not found.")
    if not plan.get("canonical_tournament"):
        raise ValueError(f"Canonical tournament {plan.get('canonical_code')} was not found.")
    duplicate_rating_count = _int_value((plan.get("duplicate_rating_summary") or {}).get("RatingRowCount"))
    if duplicate_rating_count:
        raise ValueError(
            f"Duplicate tournament {plan.get('duplicate_code')} still has {duplicate_rating_count} rating rows."
        )
    if plan.get("duplicate_games_missing_from_canonical_count"):
        raise ValueError(
            f"Duplicate tournament {plan.get('duplicate_code')} has games that are not exact duplicates under "
            f"{plan.get('canonical_code')}."
        )
    if not plan.get("production_delete_count"):
        raise ValueError("No production rows would be deleted.")


def build_delete_statements(plan: dict[str, Any]) -> list[SqlStatement]:
    duplicate = str(plan["duplicate_code"])
    canonical = str(plan["canonical_code"])
    ignore_komi = 1 if plan.get("ignore_komi") else 0
    return [
        (DELETE_GUARD_SQL, (duplicate, canonical, ignore_komi)),
        (DELETE_GAMES_SQL, (duplicate,)),
        (DELETE_TOURNAMENT_SQL, (duplicate,)),
    ]


def build_post_cleanup_summary(adapter: Any, duplicate_code: str, canonical_code: str) -> dict[str, Any]:
    codes = [canonical_code, duplicate_code]
    game_rows = load_game_rows(adapter, codes)
    tournament_rows = load_tournament_rows(adapter, codes)
    rating_rows = load_rating_summaries(adapter, codes)
    return {
        "remaining_duplicate_tournament_rows": sum(1 for row in tournament_rows if row.get("Tournament_Code") == duplicate_code),
        "remaining_duplicate_game_rows": sum(1 for row in game_rows if row.get("Tournament_Code") == duplicate_code),
        "remaining_duplicate_rating_rows": _int_value(
            next((row.get("RatingRowCount") for row in rating_rows if row.get("Tournament_Code") == duplicate_code), 0)
        ),
        "canonical_tournament_rows": sum(1 for row in tournament_rows if row.get("Tournament_Code") == canonical_code),
        "canonical_game_rows": sum(1 for row in game_rows if row.get("Tournament_Code") == canonical_code),
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


def print_duplicate_tournament_plan(plan: dict[str, Any], output: Any) -> None:
    print("Duplicate Tournament Cleanup Plan", file=output)
    print(f"  Duplicate code: {plan['duplicate_code']}", file=output)
    print(f"  Canonical code: {plan['canonical_code']}", file=output)
    print(f"  Match ignores Komi: {'yes' if plan.get('ignore_komi') else 'no'}", file=output)
    print(f"  Duplicate games to delete: {plan['duplicate_game_count']}", file=output)
    print(f"  Duplicate tournament row to delete: {'yes' if plan.get('duplicate_tournament') else 'no'}", file=output)
    print(f"  Duplicate rating rows: {_int_value((plan.get('duplicate_rating_summary') or {}).get('RatingRowCount'))}", file=output)
    print(f"  Duplicate games missing from canonical: {plan['duplicate_games_missing_from_canonical_count']}", file=output)
    print(f"  Canonical games retained: {plan['canonical_game_count']}", file=output)
    print(f"  Canonical extra games retained: {plan['canonical_extra_game_count']}", file=output)
    print(f"  Duplicate Game_IDs: {_format_id_range(plan['duplicate_game_id_range'])}", file=output)
    print(f"  Canonical Game_IDs: {_format_id_range(plan['canonical_game_id_range'])}", file=output)
    print(f"  Estimated production deletes: {plan['production_delete_count']}", file=output)
    print(f"  Ready to execute: {'yes' if plan['ready_to_execute'] else 'no'}", file=output)
    print(f"  Plan hash: {plan['plan_hash']}", file=output)
    if plan.get("post_cleanup"):
        post = plan["post_cleanup"]
        print("  Post-cleanup verification:", file=output)
        print(f"    Remaining duplicate tournament rows: {post['remaining_duplicate_tournament_rows']}", file=output)
        print(f"    Remaining duplicate game rows: {post['remaining_duplicate_game_rows']}", file=output)
        print(f"    Remaining duplicate rating rows: {post['remaining_duplicate_rating_rows']}", file=output)
        print(f"    Canonical tournament rows: {post['canonical_tournament_rows']}", file=output)
        print(f"    Canonical game rows: {post['canonical_game_rows']}", file=output)
    if plan.get("executed"):
        print("  Executed: yes", file=output)


def _is_ready_to_execute(plan: dict[str, Any]) -> bool:
    duplicate_rating_count = _int_value((plan.get("duplicate_rating_summary") or {}).get("RatingRowCount"))
    return (
        bool(plan.get("duplicate_tournament"))
        and bool(plan.get("canonical_tournament"))
        and duplicate_rating_count == 0
        and int(plan.get("duplicate_games_missing_from_canonical_count") or 0) == 0
        and int(plan.get("production_delete_count") or 0) > 0
    )


def _plan_fingerprint(plan: dict[str, Any]) -> dict[str, Any]:
    result = {
        key: value
        for key, value in plan.items()
        if key
        not in {
            "executed",
            "plan_hash",
            "post_cleanup",
            "sample_duplicate_games_missing_from_canonical",
            "sample_canonical_extra_games",
        }
    }
    return result


def _game_signature(row: dict[str, Any], *, ignore_komi: bool = False) -> tuple[Any, ...]:
    signature = [
        row.get("Game_Date"),
        _optional_int(row.get("Round")),
        _optional_int(row.get("Pin_Player_1")),
        _optional_int(row.get("Pin_Player_2")),
        _optional_int(row.get("Handicap")),
    ]
    if not ignore_komi:
        signature.append(_optional_decimal_text(row.get("Komi")))
    signature.append(str(row.get("Result") or "").strip().upper() or None)
    return tuple(signature)


def _sample_games_for_signatures(
    rows: list[dict[str, Any]],
    signatures: Counter[tuple[Any, ...]],
    *,
    ignore_komi: bool = False,
    limit: int = 10,
) -> list[dict[str, Any]]:
    if not signatures:
        return []
    remaining = Counter(signatures)
    result = []
    for row in rows:
        signature = _game_signature(row, ignore_komi=ignore_komi)
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
