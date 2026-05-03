import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, TextIO

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string
from bayrate.stage_reports import StageSqlAdapter, _coerce_date, _coerce_int, _json_default, load_staged_run


STAGED_RATINGS_SQL = """
SELECT
    [RunID],
    [Event_Ordinal],
    [Player_Ordinal],
    [Event_Source],
    [Event_Key],
    [Tournament_Code],
    [Staged_Tournament_Code],
    [Replaced_Production_Code],
    [Source_Report_Ordinal],
    [Pin_Player],
    [Rating],
    [Sigma],
    [Elab_Date],
    [Rank_Seed],
    [Seed_Before_Closing_Boundary],
    [Prior_Rating],
    [Prior_Sigma],
    [Planned_Rating_Row_ID],
    [Production_Rating_Row_ID],
    [Rating_Delta],
    [Sigma_Delta],
    [MetadataJson]
FROM [ratings].[bayrate_staged_ratings]
WHERE [RunID] = ?
ORDER BY [Event_Ordinal], [Player_Ordinal], [Pin_Player]
"""

PRODUCTION_MAX_IDS_SQL = """
SELECT
    (SELECT MAX([Game_ID]) FROM [ratings].[games]) AS [MaxGameID],
    (SELECT MAX([id]) FROM [ratings].[ratings]) AS [MaxRatingID]
"""

UPSERT_TOURNAMENT_SQL = """
UPDATE [ratings].[tournaments]
SET
    [Tournament_Descr] = ?,
    [Tournament_Date] = ?,
    [City] = ?,
    [State_Code] = ?,
    [Country_Code] = ?,
    [Host_ChapterID] = ?,
    [Host_ChapterCode] = ?,
    [Host_ChapterName] = ?,
    [Reward_Event_Key] = ?,
    [Reward_Event_Name] = ?,
    [Reward_Is_State_Championship] = ?,
    [Rounds] = ?,
    [Total_Players] = ?,
    [Wallist] = ?,
    [Elab_Date] = ?
WHERE [Tournament_Code] = ?;

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO [ratings].[tournaments]
    (
        [Tournament_Code],
        [Tournament_Descr],
        [Tournament_Date],
        [City],
        [State_Code],
        [Country_Code],
        [Host_ChapterID],
        [Host_ChapterCode],
        [Host_ChapterName],
        [Reward_Event_Key],
        [Reward_Event_Name],
        [Reward_Is_State_Championship],
        [Rounds],
        [Total_Players],
        [Wallist],
        [Elab_Date]
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
END
"""

INSERT_GAME_SQL = """
INSERT INTO [ratings].[games]
(
    [Game_ID],
    [Tournament_Code],
    [Game_Date],
    [Round],
    [Pin_Player_1],
    [Color_1],
    [Rank_1],
    [Pin_Player_2],
    [Color_2],
    [Rank_2],
    [Handicap],
    [Komi],
    [Result],
    [Sgf_Code],
    [Online],
    [Exclude],
    [Rated],
    [Elab_Date]
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_RATING_SQL = """
INSERT INTO [ratings].[ratings]
(
    [Pin_Player],
    [Rating],
    [Sigma],
    [Elab_Date],
    [Tournament_Code],
    [id]
)
VALUES (?, ?, ?, ?, ?, ?)
"""

UPDATE_STAGED_GAME_ID_SQL = """
UPDATE [ratings].[bayrate_staged_games]
SET [Game_ID] = ?
WHERE [RunID] = ?
  AND [Source_Report_Ordinal] = ?
  AND [Source_Game_Ordinal] = ?
"""

UPDATE_STAGED_RATING_ID_SQL = """
UPDATE [ratings].[bayrate_staged_ratings]
SET [Planned_Rating_Row_ID] = ?
WHERE [RunID] = ?
  AND [Event_Ordinal] = ?
  AND [Pin_Player] = ?
"""

COMMIT_RUN_GUARD_SQL = """
DECLARE @RunID int = ?;

IF NOT EXISTS
(
    SELECT 1
    FROM [ratings].[bayrate_runs] WITH (UPDLOCK, HOLDLOCK)
    WHERE [RunID] = @RunID
)
BEGIN
    THROW 51020, N'BayRate staged run was not found during commit.', 1;
END;

IF EXISTS
(
    SELECT 1
    FROM [ratings].[bayrate_staged_ratings] WITH (UPDLOCK, HOLDLOCK)
    WHERE [RunID] = @RunID
      AND [Planned_Rating_Row_ID] IS NOT NULL
)
BEGIN
    THROW 51021, N'BayRate staged run already has production rating row IDs and appears committed.', 1;
END;

IF EXISTS
(
    SELECT 1
    FROM [ratings].[bayrate_staged_games] WITH (UPDLOCK, HOLDLOCK)
    WHERE [RunID] = @RunID
      AND [Game_ID] IS NOT NULL
)
BEGIN
    THROW 51022, N'BayRate staged run already has production game IDs and appears committed.', 1;
END;
"""

UPDATE_COMMIT_AUDIT_SQL = """
UPDATE [ratings].[bayrate_runs]
SET
    [Last_Updated_At] = SYSUTCDATETIME(),
    [SummaryJson] = ?
WHERE [RunID] = ?
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview or execute a BayRate staged-run production commit.")
    parser.add_argument("--run-id", required=True, type=int, help="Integer BayRate staging RunID.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    parser.add_argument("--execute", action="store_true", help="Execute the generated production commit statements.")
    parser.add_argument(
        "--confirm-production-commit",
        action="store_true",
        help="Required with --execute to acknowledge production ratings writes.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.execute and not args.confirm_production_commit:
        print("--execute requires --confirm-production-commit.", file=sys.stderr)
        raise SystemExit(2)
    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        raise SystemExit(1)
    try:
        adapter = SqlAdapter(conn_str)
        if args.execute:
            plan = commit_staged_run(adapter, args.run_id, confirm_production_commit=True)
        else:
            plan = build_commit_plan(adapter, args.run_id)
        print_commit_plan(plan, sys.stdout)
    except Exception as exc:
        print(f"BayRate commit failed: {exc}", file=sys.stderr)
        raise SystemExit(1)


def commit_staged_run(
    adapter: StageSqlAdapter,
    run_id: int | str,
    *,
    confirm_production_commit: bool = False,
    expected_plan_hash: str | None = None,
    confirm_sgf_replacement: bool = False,
    operator_principal_name: str | None = None,
    operator_principal_id: str | None = None,
) -> dict[str, Any]:
    if not confirm_production_commit:
        raise ValueError("confirm_production_commit=True is required to write production rating tables.")
    plan = build_commit_plan(adapter, run_id)
    validate_commit_plan_for_execution(
        plan,
        expected_plan_hash=expected_plan_hash,
        confirm_sgf_replacement=confirm_sgf_replacement,
    )
    adapter.execute_statements(
        build_commit_statements(
            plan,
            operator_principal_name=operator_principal_name,
            operator_principal_id=operator_principal_id,
        )
    )
    plan["executed"] = True
    return plan


def build_commit_plan(adapter: StageSqlAdapter, run_id: int | str) -> dict[str, Any]:
    run_id_int = _require_int(run_id, "run_id")
    payload = load_staged_run(adapter, run_id_int)
    if payload.get("status") != "ready_for_rating":
        raise ValueError(f"RunID {run_id_int} is {payload.get('status')}; only ready_for_rating runs can be committed.")

    staged_ratings = load_staged_rating_rows(adapter, run_id_int)
    if not staged_ratings:
        raise ValueError(f"RunID {run_id_int} has no staged rating rows. Run Replay before commit.")
    if any(row.get("planned_rating_row_id") is not None for row in staged_ratings):
        raise ValueError(
            f"RunID {run_id_int} already has production rating row IDs and appears to have been committed."
        )

    staged_tournaments = list(payload.get("staged_tournaments") or [])
    missing_host_chapter = [
        str((entry.get("tournament_row") or {}).get("Tournament_Code") or entry.get("source_report_ordinal"))
        for entry in staged_tournaments
        if not _has_host_chapter(entry.get("tournament_row") or {})
    ]
    if missing_host_chapter:
        raise ValueError(
            "Host chapter is required before production commit for tournament(s): "
            + ", ".join(missing_host_chapter)
        )
    for entry in staged_tournaments:
        _ensure_reward_event_defaults(entry.get("tournament_row") or {})
    staged_games = list(payload.get("staged_games") or [])
    if any(_optional_int((entry.get("game_row") or {}).get("Game_ID")) is not None for entry in staged_games):
        raise ValueError(f"RunID {run_id_int} already has production game IDs and appears to have been committed.")

    staged_tournament_codes = _ordered_unique(
        entry["tournament_row"].get("Tournament_Code")
        for entry in staged_tournaments
    )
    affected_rating_codes = _ordered_unique(row.get("tournament_code") for row in staged_ratings)
    production_cascade_codes = _ordered_unique(
        row.get("tournament_code")
        for row in staged_ratings
        if row.get("event_source") == "production"
    )

    production_game_rows = load_production_games_for_codes(adapter, staged_tournament_codes)
    production_rating_summaries = load_production_rating_summaries(adapter, affected_rating_codes)
    production_tournament_rows = load_production_tournaments_for_codes(adapter, staged_tournament_codes)
    max_ids = load_production_max_ids(adapter)

    planned_games = plan_game_ids(staged_games, production_game_rows, int(max_ids.get("MaxGameID") or 0))
    planned_ratings = plan_rating_ids(staged_ratings, production_rating_summaries, int(max_ids.get("MaxRatingID") or 0))
    warnings = build_commit_warnings(
        staged_tournament_codes=staged_tournament_codes,
        production_cascade_codes=production_cascade_codes,
        production_tournament_rows=production_tournament_rows,
        production_game_rows=production_game_rows,
    )
    requires_sgf_acknowledgement = any(row.get("Sgf_Code") for row in production_game_rows)

    production_write_count = (
        len(staged_tournaments)
        + len(planned_games)
        + len(planned_ratings)
        + (1 if affected_rating_codes else 0)
        + (1 if staged_tournament_codes else 0)
    )
    return {
        "run_id": run_id_int,
        "executed": False,
        "status": payload.get("status"),
        "affected_tournament_codes": affected_rating_codes,
        "staged_tournament_codes": staged_tournament_codes,
        "production_cascade_tournament_codes": production_cascade_codes,
        "existing_tournament_codes": _ordered_unique(row.get("Tournament_Code") for row in production_tournament_rows),
        "delete_rating_tournament_codes": affected_rating_codes,
        "replace_game_tournament_codes": staged_tournament_codes,
        "tournament_upsert_count": len(staged_tournaments),
        "game_insert_count": len(planned_games),
        "rating_insert_count": len(planned_ratings),
        "production_write_count": production_write_count,
        "requires_sgf_acknowledgement": requires_sgf_acknowledgement,
        "warnings": warnings,
        "staged_tournaments": staged_tournaments,
        "planned_games": planned_games,
        "planned_ratings": planned_ratings,
    }


def load_staged_rating_rows(adapter: StageSqlAdapter, run_id: int) -> list[dict[str, Any]]:
    rows = adapter.query_rows(STAGED_RATINGS_SQL, (run_id,))
    return [_normalize_staged_rating_row(row) for row in rows]


def load_production_max_ids(adapter: StageSqlAdapter) -> dict[str, Any]:
    rows = adapter.query_rows(PRODUCTION_MAX_IDS_SQL)
    return rows[0] if rows else {"MaxGameID": 0, "MaxRatingID": 0}


def load_production_tournaments_for_codes(adapter: StageSqlAdapter, codes: list[str]) -> list[dict[str, Any]]:
    if not codes:
        return []
    query = f"""
SELECT
    [Tournament_Code],
    [Tournament_Descr],
    [Tournament_Date],
    [City],
    [State_Code],
    [Country_Code],
    [Host_ChapterID],
    [Host_ChapterCode],
    [Host_ChapterName],
    [Reward_Event_Key],
    [Reward_Event_Name],
    [Reward_Is_State_Championship],
    [Rounds],
    [Total_Players],
    [Wallist],
    [Elab_Date],
    [status]
FROM [ratings].[tournaments]
WHERE [Tournament_Code] IN ({_placeholders(codes)})
ORDER BY [Tournament_Code]
"""
    return adapter.query_rows(query, tuple(codes))


def load_production_games_for_codes(adapter: StageSqlAdapter, codes: list[str]) -> list[dict[str, Any]]:
    if not codes:
        return []
    query = f"""
SELECT
    [Game_ID],
    [Tournament_Code],
    [Game_Date],
    [Round],
    [Pin_Player_1],
    [Pin_Player_2],
    [Sgf_Code]
FROM [ratings].[games]
WHERE [Tournament_Code] IN ({_placeholders(codes)})
ORDER BY [Tournament_Code], [Game_Date], [Round], [Game_ID]
"""
    return adapter.query_rows(query, tuple(codes))


def load_production_rating_summaries(adapter: StageSqlAdapter, codes: list[str]) -> list[dict[str, Any]]:
    if not codes:
        return []
    query = f"""
SELECT
    [Tournament_Code],
    COUNT(*) AS [RatingRowCount],
    MIN([id]) AS [FirstRatingRowID],
    MAX([id]) AS [LastRatingRowID]
FROM [ratings].[ratings]
WHERE [Tournament_Code] IN ({_placeholders(codes)})
GROUP BY [Tournament_Code]
ORDER BY MIN([id]), [Tournament_Code]
"""
    return adapter.query_rows(query, tuple(codes))


def plan_game_ids(staged_games: list[dict[str, Any]], production_games: list[dict[str, Any]], max_game_id: int) -> list[dict[str, Any]]:
    production_by_code: dict[str, list[dict[str, Any]]] = {}
    for row in production_games:
        production_by_code.setdefault(str(row.get("Tournament_Code")), []).append(row)

    staged_by_code: dict[str, list[dict[str, Any]]] = {}
    for entry in staged_games:
        code = str(entry["game_row"].get("Tournament_Code") or "")
        staged_by_code.setdefault(code, []).append(entry)

    next_game_id = max_game_id + 1
    planned: list[dict[str, Any]] = []
    for code in _ordered_unique(entry["game_row"].get("Tournament_Code") for entry in staged_games):
        staged_rows = sorted(staged_by_code.get(code, []), key=lambda entry: (entry["source_report_ordinal"], entry["source_game_ordinal"]))
        existing_rows = production_by_code.get(code, [])
        if existing_rows:
            if len(existing_rows) != len(staged_rows):
                raise ValueError(
                    f"Tournament {code} already has {len(existing_rows)} production game rows, "
                    f"but the staged run has {len(staged_rows)}. Commit needs an explicit game-ID replacement strategy."
                )
            planned_ids = [int(row["Game_ID"]) for row in existing_rows]
        else:
            planned_ids = list(range(next_game_id, next_game_id + len(staged_rows)))
            next_game_id += len(staged_rows)
        for entry, planned_id in zip(staged_rows, planned_ids):
            planned.append({**entry, "planned_game_id": planned_id})
    return planned


def plan_rating_ids(
    staged_ratings: list[dict[str, Any]],
    production_rating_summaries: list[dict[str, Any]],
    max_rating_id: int,
) -> list[dict[str, Any]]:
    # Production rating IDs are not guaranteed to be contiguous by tournament.
    # Older unaffected events can be interleaved inside the cascade's old ID range,
    # so allocate a fresh append-only block rather than reusing deleted IDs.
    next_rating_id = max_rating_id + 1
    planned = []
    for index, row in enumerate(staged_ratings, start=0):
        planned.append({**row, "planned_rating_row_id": next_rating_id + index})
    return planned


def build_commit_warnings(
    *,
    staged_tournament_codes: list[str],
    production_cascade_codes: list[str],
    production_tournament_rows: list[dict[str, Any]],
    production_game_rows: list[dict[str, Any]],
) -> list[str]:
    warnings = []
    existing_codes = {str(row.get("Tournament_Code")) for row in production_tournament_rows}
    replacing_codes = [code for code in staged_tournament_codes if code in existing_codes]
    if replacing_codes:
        warnings.append("Existing production tournament(s) will be updated/replaced: " + ", ".join(replacing_codes))
    if production_cascade_codes:
        warnings.append("Production cascade ratings will be replaced for: " + ", ".join(production_cascade_codes))
    sgf_codes = [row for row in production_game_rows if row.get("Sgf_Code")]
    if sgf_codes:
        warnings.append(f"{len(sgf_codes)} existing production game row(s) have Sgf_Code values; replacement preserves Game_IDs but not SGF matching semantics.")
    return warnings


def validate_commit_plan_for_execution(
    plan: dict[str, Any],
    *,
    expected_plan_hash: str | None = None,
    confirm_sgf_replacement: bool = False,
) -> None:
    if expected_plan_hash:
        actual_plan_hash = printable_commit_plan(plan).get("plan_hash")
        if expected_plan_hash != actual_plan_hash:
            raise ValueError("Commit plan changed since preview. Preview the production commit again before committing.")
    if plan.get("requires_sgf_acknowledgement") and not confirm_sgf_replacement:
        raise ValueError("Existing SGF-linked production game rows would be replaced; confirm_sgf_replacement=True is required.")


def build_commit_statements(
    plan: dict[str, Any],
    *,
    operator_principal_name: str | None = None,
    operator_principal_id: str | None = None,
) -> list[SqlStatement]:
    statements: list[SqlStatement] = [(COMMIT_RUN_GUARD_SQL, (plan["run_id"],))]
    delete_rating_codes = list(plan.get("delete_rating_tournament_codes") or [])
    replace_game_codes = list(plan.get("replace_game_tournament_codes") or [])
    if delete_rating_codes:
        statements.append(
            (
                f"DELETE FROM [ratings].[ratings] WHERE [Tournament_Code] IN ({_placeholders(delete_rating_codes)})",
                tuple(delete_rating_codes),
            )
        )
    if replace_game_codes:
        statements.append(
            (
                f"DELETE FROM [ratings].[games] WHERE [Tournament_Code] IN ({_placeholders(replace_game_codes)})",
                tuple(replace_game_codes),
            )
        )
    for tournament in plan.get("staged_tournaments") or []:
        statements.append(build_tournament_upsert_statement(tournament))
    for game in plan.get("planned_games") or []:
        statements.append(build_game_insert_statement(game))
        statements.append(
            (
                UPDATE_STAGED_GAME_ID_SQL,
                (
                    game["planned_game_id"],
                    plan["run_id"],
                    game["source_report_ordinal"],
                    game["source_game_ordinal"],
                ),
            )
        )
    for rating in plan.get("planned_ratings") or []:
        statements.append(build_rating_insert_statement(rating))
        statements.append(
            (
                UPDATE_STAGED_RATING_ID_SQL,
                (
                    rating["planned_rating_row_id"],
                    plan["run_id"],
                    rating["event_ordinal"],
                    rating["pin_player"],
                ),
            )
        )
    statements.append(
        (
            UPDATE_COMMIT_AUDIT_SQL,
            (
                _json_dumps(_commit_audit_summary(plan, operator_principal_name, operator_principal_id)),
                plan["run_id"],
            ),
        )
    )
    return statements


def build_tournament_upsert_statement(tournament: dict[str, Any]) -> SqlStatement:
    row = tournament["tournament_row"]
    values = (
        row.get("Tournament_Descr"),
        row.get("Tournament_Date"),
        row.get("City"),
        row.get("State_Code"),
        row.get("Country_Code"),
        _optional_int(row.get("Host_ChapterID")),
        row.get("Host_ChapterCode"),
        row.get("Host_ChapterName"),
        row.get("Reward_Event_Key"),
        row.get("Reward_Event_Name"),
        1 if _coerce_bool(row.get("Reward_Is_State_Championship")) else 0,
        _coerce_int(row.get("Rounds")),
        _coerce_int(row.get("Total_Players")),
        row.get("Wallist"),
        row.get("Elab_Date"),
        row.get("Tournament_Code"),
        row.get("Tournament_Code"),
        row.get("Tournament_Descr"),
        row.get("Tournament_Date"),
        row.get("City"),
        row.get("State_Code"),
        row.get("Country_Code"),
        _optional_int(row.get("Host_ChapterID")),
        row.get("Host_ChapterCode"),
        row.get("Host_ChapterName"),
        row.get("Reward_Event_Key"),
        row.get("Reward_Event_Name"),
        1 if _coerce_bool(row.get("Reward_Is_State_Championship")) else 0,
        _coerce_int(row.get("Rounds")),
        _coerce_int(row.get("Total_Players")),
        row.get("Wallist"),
        row.get("Elab_Date"),
    )
    return UPSERT_TOURNAMENT_SQL, values


def build_game_insert_statement(game: dict[str, Any]) -> SqlStatement:
    row = game["game_row"]
    return (
        INSERT_GAME_SQL,
        (
            game["planned_game_id"],
            row.get("Tournament_Code"),
            row.get("Game_Date"),
            _coerce_int(row.get("Round")),
            _coerce_int(row.get("Pin_Player_1")),
            row.get("Color_1"),
            row.get("Rank_1"),
            _coerce_int(row.get("Pin_Player_2")),
            row.get("Color_2"),
            row.get("Rank_2"),
            _coerce_int(row.get("Handicap")),
            # Production ratings.games.Komi is int; keep the existing table convention.
            _coerce_int(row.get("Komi")),
            row.get("Result"),
            row.get("Sgf_Code"),
            _coerce_int(row.get("Online")) or 0,
            _coerce_int(row.get("Exclude")) or 0,
            1 if row.get("Rated") is None else _coerce_int(row.get("Rated")),
            row.get("Elab_Date"),
        ),
    )


def build_rating_insert_statement(rating: dict[str, Any]) -> SqlStatement:
    return (
        INSERT_RATING_SQL,
        (
            rating.get("pin_player"),
            rating.get("rating"),
            rating.get("sigma"),
            rating.get("elab_date"),
            rating.get("tournament_code"),
            rating.get("planned_rating_row_id"),
        ),
    )


def print_commit_plan(plan: dict[str, Any], output: TextIO) -> None:
    print(f"BayRate Commit Plan for RunID {plan['run_id']}", file=output)
    print(f"  Status: {plan['status']}", file=output)
    print(f"  Staged tournament(s): {', '.join(plan['staged_tournament_codes'])}", file=output)
    print(f"  Affected rating tournament(s): {', '.join(plan['affected_tournament_codes'])}", file=output)
    print(f"  Tournament upserts: {plan['tournament_upsert_count']}", file=output)
    print(f"  Game inserts: {plan['game_insert_count']}", file=output)
    print(f"  Rating inserts: {plan['rating_insert_count']}", file=output)
    print(f"  Estimated production writes: {plan['production_write_count']}", file=output)
    for warning in plan.get("warnings") or []:
        print(f"  Warning: {warning}", file=output)


def printable_commit_plan(plan: dict[str, Any]) -> dict[str, Any]:
    result = {
        "run_id": plan.get("run_id"),
        "executed": plan.get("executed", False),
        "status": plan.get("status"),
        "affected_tournament_codes": plan.get("affected_tournament_codes") or [],
        "staged_tournament_codes": plan.get("staged_tournament_codes") or [],
        "production_cascade_tournament_codes": plan.get("production_cascade_tournament_codes") or [],
        "existing_tournament_codes": plan.get("existing_tournament_codes") or [],
        "delete_rating_tournament_codes": plan.get("delete_rating_tournament_codes") or [],
        "replace_game_tournament_codes": plan.get("replace_game_tournament_codes") or [],
        "tournament_upsert_count": plan.get("tournament_upsert_count", 0),
        "game_insert_count": plan.get("game_insert_count", 0),
        "rating_insert_count": plan.get("rating_insert_count", 0),
        "production_write_count": plan.get("production_write_count", 0),
        "requires_sgf_acknowledgement": bool(plan.get("requires_sgf_acknowledgement", False)),
        "warnings": plan.get("warnings") or [],
        "game_id_range": _id_range(row["planned_game_id"] for row in plan.get("planned_games") or []),
        "rating_id_range": _id_range(row["planned_rating_row_id"] for row in plan.get("planned_ratings") or []),
    }
    fingerprint = dict(result)
    fingerprint.pop("executed", None)
    result["plan_hash"] = _commit_plan_hash(fingerprint)
    return result


def _commit_plan_hash(payload: dict[str, Any]) -> str:
    stable_payload = json.dumps(payload, default=_json_default, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(stable_payload.encode("utf-8")).hexdigest()


def _commit_audit_summary(
    plan: dict[str, Any],
    operator_principal_name: str | None,
    operator_principal_id: str | None,
) -> dict[str, Any]:
    printable = printable_commit_plan(plan)
    return {
        "run_id": plan.get("run_id"),
        "run_status": plan.get("status"),
        "commit_status": "committed",
        "committed_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "committed_by": operator_principal_name,
        "committed_principal_id": operator_principal_id,
        "commit_plan_hash": printable.get("plan_hash"),
        "affected_tournament_codes": printable.get("affected_tournament_codes") or [],
        "staged_tournament_codes": printable.get("staged_tournament_codes") or [],
        "production_cascade_tournament_codes": printable.get("production_cascade_tournament_codes") or [],
        "production_write_count": printable.get("production_write_count", 0),
        "game_id_range": printable.get("game_id_range"),
        "rating_id_range": printable.get("rating_id_range"),
        "warnings": printable.get("warnings") or [],
    }


def _normalize_staged_rating_row(row: dict[str, Any]) -> dict[str, Any]:
    metadata = row.get("MetadataJson")
    return {
        "run_id": _require_int(row.get("RunID"), "RunID"),
        "event_ordinal": _require_int(row.get("Event_Ordinal"), "Event_Ordinal"),
        "player_ordinal": _require_int(row.get("Player_Ordinal"), "Player_Ordinal"),
        "event_source": row.get("Event_Source"),
        "event_key": row.get("Event_Key"),
        "tournament_code": row.get("Tournament_Code"),
        "staged_tournament_code": row.get("Staged_Tournament_Code"),
        "replaced_production_code": row.get("Replaced_Production_Code"),
        "source_report_ordinal": _optional_int(row.get("Source_Report_Ordinal")),
        "pin_player": _require_int(row.get("Pin_Player"), "Pin_Player"),
        "rating": _optional_float(row.get("Rating")),
        "sigma": _optional_float(row.get("Sigma")),
        "elab_date": _coerce_date(row.get("Elab_Date")),
        "rank_seed": _optional_float(row.get("Rank_Seed")),
        "seed_before_closing_boundary": _optional_float(row.get("Seed_Before_Closing_Boundary")),
        "prior_rating": _optional_float(row.get("Prior_Rating")),
        "prior_sigma": _optional_float(row.get("Prior_Sigma")),
        "planned_rating_row_id": _optional_int(row.get("Planned_Rating_Row_ID")),
        "production_rating_row_id": _optional_int(row.get("Production_Rating_Row_ID")),
        "rating_delta": _optional_float(row.get("Rating_Delta")),
        "sigma_delta": _optional_float(row.get("Sigma_Delta")),
        "metadata": _json_loads(metadata, {}),
    }


def _placeholders(values: list[Any]) -> str:
    return ", ".join("?" for _ in values)


def _ordered_unique(values: Iterable[Any]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _has_host_chapter(row: dict[str, Any]) -> bool:
    return _optional_int(row.get("Host_ChapterID")) is not None and bool(str(row.get("Host_ChapterCode") or "").strip())


def _ensure_reward_event_defaults(row: dict[str, Any]) -> None:
    if not str(row.get("Reward_Event_Key") or "").strip():
        row["Reward_Event_Key"] = str(row.get("Tournament_Code") or "").strip() or None
    if not str(row.get("Reward_Event_Name") or "").strip():
        row["Reward_Event_Name"] = str(row.get("Tournament_Descr") or "").strip() or None
    row["Reward_Is_State_Championship"] = 1 if _coerce_bool(row.get("Reward_Is_State_Championship")) else 0


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off", ""}:
        return False
    try:
        return bool(_optional_int(value))
    except ValueError:
        return False


def _id_range(values: Iterable[int]) -> dict[str, int | None]:
    ids = list(values)
    if not ids:
        return {"first": None, "last": None}
    return {"first": min(ids), "last": max(ids)}


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


def _json_loads(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return default


def _json_dumps(value: Any) -> str:
    return json.dumps(value, default=_json_default, sort_keys=True)


if __name__ == "__main__":
    main()
