import argparse
import hashlib
import json
import sys
from collections import defaultdict
from datetime import date
from typing import Any, Iterable, TextIO

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string
from bayrate.stage_reports import _coerce_date, _json_default


RATING_ROWS_SQL_TEMPLATE = """
SELECT
    [id] AS [Rating_Row_ID],
    [Pin_Player],
    [Rating],
    [Sigma],
    [Elab_Date],
    [Tournament_Code]
FROM [ratings].[ratings]
WHERE [Pin_Player] IN ({placeholders})
ORDER BY [Pin_Player], [Elab_Date], [id]
"""

GAME_ROWS_SQL_TEMPLATE = """
SELECT
    [Game_ID],
    [Tournament_Code],
    [Game_Date],
    [Round],
    [Pin_Player_1],
    [Pin_Player_2],
    [Sgf_Code]
FROM [ratings].[games]
WHERE [Pin_Player_1] IN ({placeholders})
   OR [Pin_Player_2] IN ({placeholders})
ORDER BY [Game_Date], [Tournament_Code], [Round], [Game_ID]
"""

MERGE_GUARD_SQL_TEMPLATE = """
DECLARE @TargetAGAID int = ?;

DECLARE @Sources TABLE
(
    [AGAID] int NOT NULL PRIMARY KEY
);

INSERT INTO @Sources ([AGAID])
VALUES {source_values};

DECLARE @Involved TABLE
(
    [AGAID] int NOT NULL PRIMARY KEY
);

INSERT INTO @Involved ([AGAID])
SELECT @TargetAGAID
UNION
SELECT [AGAID] FROM @Sources;

IF EXISTS
(
    SELECT 1
    FROM @Sources
    WHERE [AGAID] = @TargetAGAID
)
BEGIN
    THROW 51040, N'Source AGAID cannot be the same as target AGAID.', 1;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM [ratings].[ratings]
    WHERE [Pin_Player] IN (SELECT [AGAID] FROM @Sources)
    UNION ALL
    SELECT 1
    FROM [ratings].[games]
    WHERE [Pin_Player_1] IN (SELECT [AGAID] FROM @Sources)
       OR [Pin_Player_2] IN (SELECT [AGAID] FROM @Sources)
)
BEGIN
    THROW 51041, N'Source AGAID has no production rating or game rows to merge.', 1;
END;

IF EXISTS
(
    SELECT 1
    FROM [ratings].[ratings]
    WHERE [Pin_Player] IN (SELECT [AGAID] FROM @Involved)
    GROUP BY [Tournament_Code]
    HAVING COUNT(*) > 1
)
BEGIN
    THROW 51042, N'Merge would create duplicate ratings rows for one or more tournaments.', 1;
END;

IF EXISTS
(
    SELECT 1
    FROM [ratings].[games] AS g
    WHERE g.[Pin_Player_1] IN (SELECT [AGAID] FROM @Involved)
      AND g.[Pin_Player_2] IN (SELECT [AGAID] FROM @Involved)
      AND
      (
          g.[Pin_Player_1] IN (SELECT [AGAID] FROM @Sources)
          OR g.[Pin_Player_2] IN (SELECT [AGAID] FROM @Sources)
      )
)
BEGIN
    THROW 51043, N'Merge would create a game row where one AGAID plays itself.', 1;
END;
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge production BayRate rating/game rows from duplicate member AGAIDs into one AGAID."
    )
    parser.add_argument(
        "--source-agaid",
        action="append",
        required=True,
        type=int,
        help="Old/duplicate AGAID to merge into the target. Can be repeated.",
    )
    parser.add_argument("--target-agaid", required=True, type=int, help="AGAID that should keep the merged history.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    parser.add_argument("--execute", action="store_true", help="Execute the production ID reassignment.")
    parser.add_argument(
        "--confirm-member-rating-merge",
        action="store_true",
        help="Required with --execute to acknowledge production ratings/games writes.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.execute and not args.confirm_member_rating_merge:
        print("--execute requires --confirm-member-rating-merge.", file=sys.stderr)
        raise SystemExit(2)
    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        raise SystemExit(1)
    try:
        adapter = SqlAdapter(conn_str)
        if args.execute:
            plan = merge_member_ratings(
                adapter,
                source_agaids=args.source_agaid,
                target_agaid=args.target_agaid,
                confirm_member_rating_merge=True,
            )
        else:
            plan = build_member_merge_plan(adapter, source_agaids=args.source_agaid, target_agaid=args.target_agaid)
        print_member_merge_plan(plan, sys.stdout)
    except Exception as exc:
        print(f"BayRate member merge failed: {exc}", file=sys.stderr)
        raise SystemExit(1)


def merge_member_ratings(
    adapter: Any,
    *,
    source_agaids: Iterable[int | str],
    target_agaid: int | str,
    confirm_member_rating_merge: bool = False,
    expected_plan_hash: str | None = None,
) -> dict[str, Any]:
    if not confirm_member_rating_merge:
        raise ValueError("confirm_member_rating_merge=True is required to write production rating tables.")
    plan = build_member_merge_plan(adapter, source_agaids=source_agaids, target_agaid=target_agaid)
    validate_member_merge_plan_for_execution(plan, expected_plan_hash=expected_plan_hash)
    adapter.execute_statements(build_member_merge_statements(plan))
    plan["executed"] = True
    return plan


def build_member_merge_plan(
    adapter: Any,
    *,
    source_agaids: Iterable[int | str],
    target_agaid: int | str,
) -> dict[str, Any]:
    target_id = _require_int(target_agaid, "target_agaid")
    source_ids = _normalize_source_ids(source_agaids, target_id)
    involved_ids = [target_id, *source_ids]

    rating_rows = load_member_rating_rows(adapter, involved_ids)
    game_rows = load_member_game_rows(adapter, involved_ids)

    source_set = set(source_ids)
    target_rating_rows = [row for row in rating_rows if row["pin_player"] == target_id]
    source_rating_rows = [row for row in rating_rows if row["pin_player"] in source_set]
    target_game_rows = [
        row
        for row in game_rows
        if row.get("pin_player_1") == target_id or row.get("pin_player_2") == target_id
    ]
    source_game_rows = [
        row
        for row in game_rows
        if row.get("pin_player_1") in source_set or row.get("pin_player_2") in source_set
    ]

    plan = {
        "source_agaids": source_ids,
        "target_agaid": target_id,
        "executed": False,
        "source_rating_count": len(source_rating_rows),
        "target_rating_count": len(target_rating_rows),
        "source_game_count": len(source_game_rows),
        "target_game_count": len(target_game_rows),
        "rating_update_count": len(source_rating_rows),
        "game_player_1_update_count": sum(1 for row in game_rows if row.get("pin_player_1") in source_set),
        "game_player_2_update_count": sum(1 for row in game_rows if row.get("pin_player_2") in source_set),
        "latest_source_ratings": [
            _latest_rating_summary(source_id, [row for row in source_rating_rows if row["pin_player"] == source_id])
            for source_id in source_ids
        ],
        "latest_target_rating": _latest_rating_summary(target_id, target_rating_rows),
        "rating_conflicts": _build_rating_conflicts(rating_rows, source_ids, target_id),
        "preexisting_rating_duplicates": _build_preexisting_rating_duplicates(rating_rows, involved_ids),
        "self_play_game_rows": _build_self_play_game_rows(game_rows, source_ids, target_id),
        "source_rating_id_range": _id_range(row["rating_row_id"] for row in source_rating_rows),
        "source_game_id_range": _id_range(row["game_id"] for row in source_game_rows),
    }
    plan["production_write_count"] = (
        plan["rating_update_count"]
        + plan["game_player_1_update_count"]
        + plan["game_player_2_update_count"]
    )
    return plan


def load_member_rating_rows(adapter: Any, involved_ids: list[int]) -> list[dict[str, Any]]:
    if not involved_ids:
        return []
    placeholders = _placeholders(involved_ids)
    query = RATING_ROWS_SQL_TEMPLATE.format(placeholders=placeholders)
    return [_normalize_rating_row(row) for row in adapter.query_rows(query, tuple(involved_ids))]


def load_member_game_rows(adapter: Any, involved_ids: list[int]) -> list[dict[str, Any]]:
    if not involved_ids:
        return []
    placeholders = _placeholders(involved_ids)
    query = GAME_ROWS_SQL_TEMPLATE.format(placeholders=placeholders)
    params = tuple(involved_ids) + tuple(involved_ids)
    return [_normalize_game_row(row) for row in adapter.query_rows(query, params)]


def validate_member_merge_plan_for_execution(
    plan: dict[str, Any],
    *,
    expected_plan_hash: str | None = None,
) -> None:
    if expected_plan_hash:
        actual_plan_hash = printable_member_merge_plan(plan).get("plan_hash")
        if expected_plan_hash != actual_plan_hash:
            raise ValueError("Member merge plan changed since preview. Preview the merge again before executing.")
    if int(plan.get("production_write_count") or 0) == 0:
        raise ValueError("Source AGAID has no production rating or game rows to merge.")
    if plan.get("rating_conflicts"):
        codes = ", ".join(str(row.get("tournament_code") or "<blank>") for row in plan["rating_conflicts"][:8])
        raise ValueError(
            "Merge would create duplicate ratings rows for tournament(s): "
            + codes
            + ". Resolve those rows manually before merging."
        )
    if plan.get("self_play_game_rows"):
        ids = ", ".join(str(row.get("game_id") or "<blank>") for row in plan["self_play_game_rows"][:8])
        raise ValueError(
            "Merge would create game rows where the merged AGAID plays itself. "
            + "Resolve production Game_ID(s) first: "
            + ids
        )


def build_member_merge_statements(plan: dict[str, Any]) -> list[SqlStatement]:
    source_ids = [int(value) for value in plan["source_agaids"]]
    target_id = int(plan["target_agaid"])
    source_placeholders = _placeholders(source_ids)
    guard_sql = MERGE_GUARD_SQL_TEMPLATE.format(source_values=", ".join("(?)" for _ in source_ids))

    return [
        (guard_sql, (target_id, *source_ids)),
        (
            f"""
UPDATE [ratings].[ratings]
SET [Pin_Player] = ?
WHERE [Pin_Player] IN ({source_placeholders})
""",
            (target_id, *source_ids),
        ),
        (
            f"""
UPDATE [ratings].[games]
SET [Pin_Player_1] = ?
WHERE [Pin_Player_1] IN ({source_placeholders})
""",
            (target_id, *source_ids),
        ),
        (
            f"""
UPDATE [ratings].[games]
SET [Pin_Player_2] = ?
WHERE [Pin_Player_2] IN ({source_placeholders})
""",
            (target_id, *source_ids),
        ),
    ]


def print_member_merge_plan(plan: dict[str, Any], output: TextIO) -> None:
    printable = printable_member_merge_plan(plan)
    print("BayRate Member Rating Merge Plan", file=output)
    print(f"  Source AGAID(s): {', '.join(str(value) for value in printable['source_agaids'])}", file=output)
    print(f"  Target AGAID: {printable['target_agaid']}", file=output)
    print("  Operation: production ID reassignment only; BayRate is not rerun.", file=output)
    print(f"  Rating rows to move: {printable['rating_update_count']}", file=output)
    print(f"  Game player-1 references to move: {printable['game_player_1_update_count']}", file=output)
    print(f"  Game player-2 references to move: {printable['game_player_2_update_count']}", file=output)
    print(f"  Estimated production updates: {printable['production_write_count']}", file=output)
    print(f"  Source rating row IDs: {_format_id_range(printable['source_rating_id_range'])}", file=output)
    print(f"  Source game IDs: {_format_id_range(printable['source_game_id_range'])}", file=output)

    for summary in printable.get("latest_source_ratings") or []:
        print(f"  Latest source {summary['agaid']}: {_format_rating_summary(summary)}", file=output)
    print(f"  Latest target: {_format_rating_summary(printable['latest_target_rating'])}", file=output)

    if printable["rating_conflict_count"]:
        print(f"  Blocker: {printable['rating_conflict_count']} duplicate tournament rating conflict(s).", file=output)
        for conflict in printable["rating_conflicts"][:8]:
            print(
                "    "
                + f"{conflict.get('tournament_code') or '<blank>'}: AGAIDs "
                + ", ".join(str(value) for value in conflict.get("agaids") or [])
                + f", rating rows {conflict.get('rating_row_ids')}",
                file=output,
            )
    if printable["preexisting_rating_duplicate_count"]:
        print(
            f"  Warning: {printable['preexisting_rating_duplicate_count']} pre-existing same-ID duplicate rating group(s).",
            file=output,
        )
        for duplicate in printable["preexisting_rating_duplicates"][:8]:
            print(
                "    "
                + f"AGAID {duplicate.get('pin_player')} {duplicate.get('tournament_code') or '<blank>'}: "
                + f"rating rows {duplicate.get('rating_row_ids')}",
                file=output,
            )
    if printable["self_play_game_count"]:
        print(f"  Blocker: {printable['self_play_game_count']} game(s) would become self-play.", file=output)
        for game in printable["self_play_game_rows"][:8]:
            print(
                f"    Game_ID {game.get('game_id')}: {game.get('pin_player_1')}-{game.get('pin_player_2')} "
                f"{game.get('tournament_code') or ''}",
                file=output,
            )
    print(f"  Ready to execute: {'yes' if printable['ready_to_execute'] else 'no'}", file=output)
    print(f"  Plan hash: {printable['plan_hash']}", file=output)
    if printable.get("executed"):
        print("  Executed: yes", file=output)


def printable_member_merge_plan(plan: dict[str, Any]) -> dict[str, Any]:
    result = {
        "source_agaids": plan.get("source_agaids") or [],
        "target_agaid": plan.get("target_agaid"),
        "executed": bool(plan.get("executed", False)),
        "source_rating_count": plan.get("source_rating_count", 0),
        "target_rating_count": plan.get("target_rating_count", 0),
        "source_game_count": plan.get("source_game_count", 0),
        "target_game_count": plan.get("target_game_count", 0),
        "rating_update_count": plan.get("rating_update_count", 0),
        "game_player_1_update_count": plan.get("game_player_1_update_count", 0),
        "game_player_2_update_count": plan.get("game_player_2_update_count", 0),
        "production_write_count": plan.get("production_write_count", 0),
        "source_rating_id_range": plan.get("source_rating_id_range") or {"first": None, "last": None},
        "source_game_id_range": plan.get("source_game_id_range") or {"first": None, "last": None},
        "latest_source_ratings": plan.get("latest_source_ratings") or [],
        "latest_target_rating": plan.get("latest_target_rating") or None,
        "rating_conflicts": plan.get("rating_conflicts") or [],
        "rating_conflict_count": len(plan.get("rating_conflicts") or []),
        "preexisting_rating_duplicates": plan.get("preexisting_rating_duplicates") or [],
        "preexisting_rating_duplicate_count": len(plan.get("preexisting_rating_duplicates") or []),
        "self_play_game_rows": plan.get("self_play_game_rows") or [],
        "self_play_game_count": len(plan.get("self_play_game_rows") or []),
    }
    result["ready_to_execute"] = (
        result["production_write_count"] > 0
        and result["rating_conflict_count"] == 0
        and result["self_play_game_count"] == 0
    )
    fingerprint = dict(result)
    fingerprint.pop("executed", None)
    result["plan_hash"] = _stable_hash(fingerprint)
    return result


def _normalize_source_ids(source_agaids: Iterable[int | str], target_id: int) -> list[int]:
    source_ids: list[int] = []
    seen = set()
    for value in source_agaids:
        source_id = _require_int(value, "source_agaid")
        if source_id == target_id:
            raise ValueError("source_agaid cannot match target_agaid.")
        if source_id in seen:
            continue
        seen.add(source_id)
        source_ids.append(source_id)
    if not source_ids:
        raise ValueError("At least one source_agaid is required.")
    return source_ids


def _normalize_rating_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "rating_row_id": _optional_int(row.get("Rating_Row_ID") or row.get("id")),
        "pin_player": _require_int(row.get("Pin_Player"), "Pin_Player"),
        "rating": _optional_float(row.get("Rating")),
        "sigma": _optional_float(row.get("Sigma")),
        "elab_date": _coerce_date(row.get("Elab_Date")),
        "tournament_code": row.get("Tournament_Code"),
    }


def _normalize_game_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "game_id": _optional_int(row.get("Game_ID")),
        "tournament_code": row.get("Tournament_Code"),
        "game_date": _coerce_date(row.get("Game_Date")),
        "round": _optional_int(row.get("Round")),
        "pin_player_1": _optional_int(row.get("Pin_Player_1")),
        "pin_player_2": _optional_int(row.get("Pin_Player_2")),
        "sgf_code": row.get("Sgf_Code"),
    }


def _build_rating_conflicts(
    rating_rows: list[dict[str, Any]],
    source_ids: list[int],
    target_id: int,
) -> list[dict[str, Any]]:
    involved = {target_id, *source_ids}
    sources = set(source_ids)
    by_tournament: dict[str, list[dict[str, Any]]] = defaultdict(list)
    code_display: dict[str, Any] = {}
    for row in rating_rows:
        if row.get("pin_player") not in involved:
            continue
        key = str(row.get("tournament_code") or "")
        code_display[key] = row.get("tournament_code")
        by_tournament[key].append(row)

    conflicts = []
    for key, rows in by_tournament.items():
        agaids = _ordered_unique(row.get("pin_player") for row in rows)
        if len(agaids) <= 1 or not any(agaid in sources for agaid in agaids):
            continue
        conflicts.append(
            {
                "tournament_code": code_display.get(key),
                "row_count": len(rows),
                "agaids": agaids,
                "rating_row_ids": [row.get("rating_row_id") for row in rows],
                "rows": [_rating_conflict_row(row) for row in rows],
            }
        )
    return sorted(conflicts, key=lambda item: str(item.get("tournament_code") or ""))


def _build_preexisting_rating_duplicates(
    rating_rows: list[dict[str, Any]],
    involved_ids: list[int],
) -> list[dict[str, Any]]:
    involved = set(involved_ids)
    by_player_tournament: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    code_display: dict[tuple[int, str], Any] = {}
    for row in rating_rows:
        player_id = row.get("pin_player")
        if player_id not in involved:
            continue
        key = (player_id, str(row.get("tournament_code") or ""))
        code_display[key] = row.get("tournament_code")
        by_player_tournament[key].append(row)

    duplicates = []
    for key, rows in by_player_tournament.items():
        if len(rows) <= 1:
            continue
        player_id, _ = key
        duplicates.append(
            {
                "pin_player": player_id,
                "tournament_code": code_display.get(key),
                "row_count": len(rows),
                "rating_row_ids": [row.get("rating_row_id") for row in rows],
                "rows": [_rating_conflict_row(row) for row in rows],
            }
        )
    return sorted(
        duplicates,
        key=lambda item: (int(item.get("pin_player") or 0), str(item.get("tournament_code") or "")),
    )


def _rating_conflict_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "rating_row_id": row.get("rating_row_id"),
        "pin_player": row.get("pin_player"),
        "rating": row.get("rating"),
        "sigma": row.get("sigma"),
        "elab_date": row.get("elab_date"),
        "tournament_code": row.get("tournament_code"),
    }


def _build_self_play_game_rows(
    game_rows: list[dict[str, Any]],
    source_ids: list[int],
    target_id: int,
) -> list[dict[str, Any]]:
    sources = set(source_ids)
    involved = {target_id, *sources}
    result = []
    for row in game_rows:
        player_1 = row.get("pin_player_1")
        player_2 = row.get("pin_player_2")
        if player_1 in involved and player_2 in involved and (player_1 in sources or player_2 in sources):
            result.append(row)
    return result


def _latest_rating_summary(agaid: int, rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "agaid": agaid,
            "rating_row_id": None,
            "rating": None,
            "sigma": None,
            "elab_date": None,
            "tournament_code": None,
        }
    latest = sorted(rows, key=lambda row: (row.get("elab_date") or date.min, row.get("rating_row_id") or -1))[-1]
    return {
        "agaid": agaid,
        "rating_row_id": latest.get("rating_row_id"),
        "rating": latest.get("rating"),
        "sigma": latest.get("sigma"),
        "elab_date": latest.get("elab_date"),
        "tournament_code": latest.get("tournament_code"),
    }


def _format_rating_summary(summary: dict[str, Any] | None) -> str:
    if not summary or summary.get("rating_row_id") is None:
        return "no rating rows"
    return (
        f"{summary.get('rating')} +/- {summary.get('sigma')} on {summary.get('elab_date')} "
        f"({summary.get('tournament_code')}, row {summary.get('rating_row_id')})"
    )


def _format_id_range(value: dict[str, int | None]) -> str:
    if not value or value.get("first") is None:
        return "none"
    if value.get("first") == value.get("last"):
        return str(value.get("first"))
    return f"{value.get('first')}..{value.get('last')}"


def _stable_hash(payload: dict[str, Any]) -> str:
    stable_payload = json.dumps(payload, default=_json_default, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(stable_payload.encode("utf-8")).hexdigest()


def _id_range(values: Iterable[int | None]) -> dict[str, int | None]:
    ids = [value for value in values if value is not None]
    if not ids:
        return {"first": None, "last": None}
    return {"first": min(ids), "last": max(ids)}


def _ordered_unique(values: Iterable[Any]) -> list[Any]:
    seen = set()
    result = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _placeholders(values: Iterable[Any]) -> str:
    return ", ".join("?" for _ in values)


def _require_int(value: Any, name: str) -> int:
    result = _optional_int(value)
    if result is None:
        raise ValueError(f"{name} is required.")
    return result


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return int(float(text))


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return float(text)


if __name__ == "__main__":
    main()
