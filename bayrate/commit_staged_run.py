# SPDX-FileCopyrightText: 2010 Philip Waldron
# SPDX-FileCopyrightText: 2026 American Go Association
# SPDX-License-Identifier: GPL-3.0-or-later
import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, TextIO

from bayrate.reward_reconciliation import build_reward_reconciliation
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

REWARD_RECONCILIATION_SCHEMA_GUARD_SQL = """
IF OBJECT_ID(N'ratings.bayrate_reward_reconciliations', N'U') IS NULL
BEGIN
    THROW 51023, N'BayRate reward reconciliation schema is missing. Apply bayrate/sql/bayrate_staging_schema.sql before committing a rerun.', 1;
END;
"""

INSERT_REWARD_RECONCILIATION_SQL = """
INSERT INTO [ratings].[bayrate_reward_reconciliations]
(
    [RunID],
    [ReconciliationJson],
    [Created_By],
    [Created_Principal_Id]
)
VALUES (?, ?, ?, ?)
"""

PREVIOUS_COMMITTED_RUNS_SQL_TEMPLATE = """
SELECT DISTINCT r.[RunID]
FROM [ratings].[bayrate_runs] AS r
WHERE r.[RunID] <> ?
  AND JSON_VALUE(
        CASE WHEN ISJSON(r.[SummaryJson]) = 1 THEN r.[SummaryJson] ELSE N'{{}}' END,
        N'$.commit_status'
      ) = N'committed'
  AND
  (
      EXISTS
      (
          SELECT 1
          FROM [ratings].[bayrate_staged_ratings] AS sr
          WHERE sr.[RunID] = r.[RunID]
            AND sr.[Planned_Rating_Row_ID] IS NOT NULL
            AND sr.[Tournament_Code] IN ({rating_placeholders})
      )
      OR EXISTS
      (
          SELECT 1
          FROM [ratings].[bayrate_staged_games] AS sg
          WHERE sg.[RunID] = r.[RunID]
            AND sg.[Game_ID] IS NOT NULL
            AND sg.[Tournament_Code] IN ({game_placeholders})
      )
  )
ORDER BY r.[RunID]
"""

MARK_SUPERSEDED_RUNS_SQL_TEMPLATE = """
UPDATE [ratings].[bayrate_runs]
SET
    [Last_Updated_At] = SYSUTCDATETIME(),
    [SummaryJson] = JSON_MODIFY(
        JSON_MODIFY(
            JSON_MODIFY(
                JSON_MODIFY(
                    CASE WHEN ISJSON([SummaryJson]) = 1 THEN [SummaryJson] ELSE N'{{}}' END,
                    N'$.commit_status',
                    N'superseded'
                ),
                N'$.superseded_by_run_id',
                ?
            ),
            N'$.superseded_at_utc',
            CONVERT(nvarchar(33), SYSUTCDATETIME(), 127) + N'Z'
        ),
        N'$.superseded_reason',
        ?
    )
WHERE [RunID] IN ({run_placeholders})
"""


def parse_args() -> argparse.Namespace:
    """Parse args."""
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
    """Run the command-line entry point for this module."""
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
    """Commit staged run."""
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
    """Build commit plan."""
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
    rerun_tournament_codes = _ordered_unique(row.get("Tournament_Code") for row in production_tournament_rows)
    max_ids = load_production_max_ids(adapter)
    superseded_run_ids = load_previous_committed_run_ids(
        adapter,
        run_id_int,
        rating_codes=affected_rating_codes,
        game_codes=staged_tournament_codes,
    )

    planned_games, game_id_reconciliation = plan_game_ids(
        staged_games,
        production_game_rows,
        int(max_ids.get("MaxGameID") or 0),
        replacement_codes=staged_tournament_codes,
    )
    planned_ratings = plan_rating_ids(staged_ratings, production_rating_summaries, int(max_ids.get("MaxRatingID") or 0))
    reward_reconciliation = build_reward_reconciliation(
        adapter,
        run_id=run_id_int,
        staged_tournaments=staged_tournaments,
        staged_games=planned_games,
        production_tournaments=production_tournament_rows,
        production_games=production_game_rows,
        reconciliation_tournament_codes=rerun_tournament_codes,
    )
    warnings = build_commit_warnings(
        staged_tournament_codes=staged_tournament_codes,
        production_cascade_codes=production_cascade_codes,
        production_tournament_rows=production_tournament_rows,
        game_id_reconciliation=game_id_reconciliation,
        reward_reconciliation=reward_reconciliation,
    )
    if superseded_run_ids:
        warnings.append(
            "Previously committed BayRate run(s) will be marked superseded: "
            + ", ".join(str(run_id) for run_id in superseded_run_ids)
        )
    if rerun_tournament_codes:
        warnings.append(
            "Automatic played-game, total-games, and State Championship awards will be suppressed for rerun "
            "tournament(s); use the persisted reconciliation report for reward adjustments: "
            + ", ".join(rerun_tournament_codes)
        )
    requires_sgf_acknowledgement = bool(game_id_reconciliation.get("retired_sgf_game_count"))

    production_write_count = (
        len(staged_tournaments)
        + len(planned_games)
        + len(planned_ratings)
        + (1 if affected_rating_codes else 0)
        + (1 if staged_tournament_codes else 0)
        + (1 if rerun_tournament_codes else 0)
    )
    return {
        "run_id": run_id_int,
        "executed": False,
        "status": payload.get("status"),
        "affected_tournament_codes": affected_rating_codes,
        "staged_tournament_codes": staged_tournament_codes,
        "production_cascade_tournament_codes": production_cascade_codes,
        "existing_tournament_codes": _ordered_unique(row.get("Tournament_Code") for row in production_tournament_rows),
        "rerun_tournament_codes": rerun_tournament_codes,
        "reward_automation_suppressed": bool(rerun_tournament_codes),
        "delete_rating_tournament_codes": affected_rating_codes,
        "replace_game_tournament_codes": staged_tournament_codes,
        "superseded_run_ids": superseded_run_ids,
        "tournament_upsert_count": len(staged_tournaments),
        "game_insert_count": len(planned_games),
        "rating_insert_count": len(planned_ratings),
        "production_write_count": production_write_count,
        "requires_sgf_acknowledgement": requires_sgf_acknowledgement,
        "warnings": warnings,
        "game_id_reconciliation": game_id_reconciliation,
        "reward_reconciliation": reward_reconciliation,
        "staged_tournaments": staged_tournaments,
        "planned_games": planned_games,
        "planned_ratings": planned_ratings,
    }


def load_staged_rating_rows(adapter: StageSqlAdapter, run_id: int) -> list[dict[str, Any]]:
    """Load staged rating rows."""
    rows = adapter.query_rows(STAGED_RATINGS_SQL, (run_id,))
    return [_normalize_staged_rating_row(row) for row in rows]


def load_production_max_ids(adapter: StageSqlAdapter) -> dict[str, Any]:
    """Load production max ids."""
    rows = adapter.query_rows(PRODUCTION_MAX_IDS_SQL)
    return rows[0] if rows else {"MaxGameID": 0, "MaxRatingID": 0}


def load_previous_committed_run_ids(
    adapter: StageSqlAdapter,
    run_id: int,
    *,
    rating_codes: list[str],
    game_codes: list[str],
) -> list[int]:
    """Load previous committed run ids."""
    code_set = _ordered_unique([*rating_codes, *game_codes])
    if not code_set:
        return []
    query = PREVIOUS_COMMITTED_RUNS_SQL_TEMPLATE.format(
        rating_placeholders=_placeholders(code_set),
        game_placeholders=_placeholders(code_set),
    )
    rows = adapter.query_rows(query, (run_id, *code_set, *code_set))
    return [_require_int(row.get("RunID"), "RunID") for row in rows]


def load_production_tournaments_for_codes(adapter: StageSqlAdapter, codes: list[str]) -> list[dict[str, Any]]:
    """Load production tournaments for codes."""
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
    """Load production games for codes."""
    if not codes:
        return []
    query = f"""
SELECT
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
FROM [ratings].[games]
WHERE [Tournament_Code] IN ({_placeholders(codes)})
ORDER BY [Tournament_Code], [Game_Date], [Round], [Game_ID]
"""
    return adapter.query_rows(query, tuple(codes))


def load_production_rating_summaries(adapter: StageSqlAdapter, codes: list[str]) -> list[dict[str, Any]]:
    """Load production rating summaries."""
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


def plan_game_ids(
    staged_games: list[dict[str, Any]],
    production_games: list[dict[str, Any]],
    max_game_id: int,
    *,
    replacement_codes: list[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Match physical games, retain their IDs, and allocate new IDs append-only."""
    production_by_code: dict[str, list[dict[str, Any]]] = {}
    for row in production_games:
        production_by_code.setdefault(str(row.get("Tournament_Code")), []).append(row)

    staged_by_code: dict[str, list[dict[str, Any]]] = {}
    for entry in staged_games:
        code = str(entry["game_row"].get("Tournament_Code") or "")
        staged_by_code.setdefault(code, []).append(entry)

    next_game_id = max_game_id + 1
    planned: list[dict[str, Any]] = []
    assignments: list[dict[str, Any]] = []
    retired_games: list[dict[str, Any]] = []
    codes = replacement_codes or _ordered_unique(entry["game_row"].get("Tournament_Code") for entry in staged_games)
    for code in codes:
        staged_rows = sorted(staged_by_code.get(code, []), key=lambda entry: (entry["source_report_ordinal"], entry["source_game_ordinal"]))
        existing_rows = sorted(production_by_code.get(code, []), key=lambda row: int(row.get("Game_ID") or 0))
        existing_by_identity: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        staged_by_identity: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in existing_rows:
            existing_by_identity.setdefault(_physical_game_identity(row), []).append(row)
        for entry in staged_rows:
            staged_by_identity.setdefault(_physical_game_identity(entry["game_row"]), []).append(entry)

        ambiguous = [
            identity
            for identity, staged_matches in staged_by_identity.items()
            if len(staged_matches) != 1 or len(existing_by_identity.get(identity) or []) > 1
        ]
        if ambiguous:
            raise ValueError(
                f"Tournament {code} has ambiguous duplicate games with the same date, round, and players. "
                "Correct the round/game data before committing so production Game_IDs and SGFs can be matched safely."
            )

        matched_existing_ids: set[int] = set()
        for entry in staged_rows:
            identity = _physical_game_identity(entry["game_row"])
            candidates = existing_by_identity.get(identity) or []
            existing = candidates[0] if len(candidates) == 1 else None
            if existing is not None:
                planned_id = _require_int(existing.get("Game_ID"), "Game_ID")
                matched_existing_ids.add(planned_id)
                game_row = dict(entry["game_row"])
                if existing.get("Sgf_Code"):
                    game_row["Sgf_Code"] = existing.get("Sgf_Code")
                planned_entry = {
                    **entry,
                    "game_row": game_row,
                    "planned_game_id": planned_id,
                    "game_id_match_status": "retained",
                    "previous_game_id": planned_id,
                }
            else:
                planned_id = next_game_id
                next_game_id += 1
                planned_entry = {
                    **entry,
                    "planned_game_id": planned_id,
                    "game_id_match_status": "new",
                    "previous_game_id": None,
                }
            planned.append(planned_entry)
            assignments.append(_format_game_id_assignment(planned_entry))

        for row in existing_rows:
            game_id = _require_int(row.get("Game_ID"), "Game_ID")
            if game_id not in matched_existing_ids:
                retired_games.append(_format_retired_game(row))

    reconciliation = {
        "matching_rule": "tournament code + game date + round + unordered player pair",
        "retained_game_count": sum(1 for row in assignments if row["match_status"] == "retained"),
        "new_game_count": sum(1 for row in assignments if row["match_status"] == "new"),
        "retired_game_count": len(retired_games),
        "retired_sgf_game_count": sum(1 for row in retired_games if row.get("sgf_linked")),
        "assignments": assignments,
        "retired_games": retired_games,
    }
    return planned, reconciliation


def plan_rating_ids(
    staged_ratings: list[dict[str, Any]],
    production_rating_summaries: list[dict[str, Any]],
    max_rating_id: int,
) -> list[dict[str, Any]]:
    # Production rating IDs are not guaranteed to be contiguous by tournament.
    # Older unaffected events can be interleaved inside the cascade's old ID range,
    # so allocate a fresh append-only block rather than reusing deleted IDs.
    """Execute the plan rating ids routine."""
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
    game_id_reconciliation: dict[str, Any],
    reward_reconciliation: dict[str, Any],
) -> list[str]:
    """Build commit warnings."""
    warnings = []
    existing_codes = {str(row.get("Tournament_Code")) for row in production_tournament_rows}
    replacing_codes = [code for code in staged_tournament_codes if code in existing_codes]
    if replacing_codes:
        warnings.append("Existing production tournament(s) will be updated/replaced: " + ", ".join(replacing_codes))
    if production_cascade_codes:
        warnings.append("Production cascade ratings will be replaced for: " + ", ".join(production_cascade_codes))
    if game_id_reconciliation.get("new_game_count") or game_id_reconciliation.get("retired_game_count"):
        warnings.append(
            "Game-ID reconciliation will retain "
            f"{game_id_reconciliation.get('retained_game_count', 0)} matched game(s), allocate "
            f"{game_id_reconciliation.get('new_game_count', 0)} new ID(s), and retire "
            f"{game_id_reconciliation.get('retired_game_count', 0)} old ID(s)."
        )
    if game_id_reconciliation.get("retired_sgf_game_count"):
        warnings.append(
            f"{game_id_reconciliation['retired_sgf_game_count']} retired production game row(s) have SGF links; "
            "explicit acknowledgement is required."
        )
    if reward_reconciliation.get("old_total_points") != reward_reconciliation.get("new_total_points"):
        warnings.append(
            "Chapter Rewards reconciliation changes total calculated points from "
            f"{reward_reconciliation.get('old_total_points', 0):,} to "
            f"{reward_reconciliation.get('new_total_points', 0):,}; rewards are not adjusted by this commit."
        )
    warnings.extend(str(warning) for warning in reward_reconciliation.get("warnings") or [])
    return warnings


def validate_commit_plan_for_execution(
    plan: dict[str, Any],
    *,
    expected_plan_hash: str | None = None,
    confirm_sgf_replacement: bool = False,
) -> None:
    """Validate commit plan for execution."""
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
    """Build commit statements."""
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
    superseded_run_ids = [int(run_id) for run_id in plan.get("superseded_run_ids") or []]
    if superseded_run_ids:
        statements.append(build_superseded_runs_statement(plan, superseded_run_ids))
    if plan.get("rerun_tournament_codes"):
        statements.append((REWARD_RECONCILIATION_SCHEMA_GUARD_SQL, ()))
        statements.append(
            (
                INSERT_REWARD_RECONCILIATION_SQL,
                (
                    plan["run_id"],
                    _json_dumps(plan.get("reward_reconciliation") or {}),
                    operator_principal_name,
                    operator_principal_id,
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


def build_superseded_runs_statement(plan: dict[str, Any], superseded_run_ids: list[int]) -> SqlStatement:
    """Build superseded runs statement."""
    affected_codes = _ordered_unique(plan.get("affected_tournament_codes") or [])
    reason = (
        f"Superseded by BayRate RunID {plan['run_id']}"
        + (f"; replaced tournament codes: {', '.join(affected_codes)}" if affected_codes else "")
    )
    query = MARK_SUPERSEDED_RUNS_SQL_TEMPLATE.format(run_placeholders=_placeholders(superseded_run_ids))
    return query, (plan["run_id"], reason, *superseded_run_ids)


def build_tournament_upsert_statement(tournament: dict[str, Any]) -> SqlStatement:
    """Build tournament upsert statement."""
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
    """Build game insert statement."""
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
    """Build rating insert statement."""
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
    """Execute the print commit plan routine."""
    print(f"BayRate Commit Plan for RunID {plan['run_id']}", file=output)
    print(f"  Status: {plan['status']}", file=output)
    print(f"  Staged tournament(s): {', '.join(plan['staged_tournament_codes'])}", file=output)
    print(f"  Affected rating tournament(s): {', '.join(plan['affected_tournament_codes'])}", file=output)
    if plan.get("superseded_run_ids"):
        print(f"  Superseded previous run(s): {', '.join(str(run_id) for run_id in plan['superseded_run_ids'])}", file=output)
    print(f"  Tournament upserts: {plan['tournament_upsert_count']}", file=output)
    print(f"  Game inserts: {plan['game_insert_count']}", file=output)
    print(f"  Rating inserts: {plan['rating_insert_count']}", file=output)
    print(f"  Estimated production writes: {plan['production_write_count']}", file=output)
    reconciliation = plan.get("reward_reconciliation") or {}
    print(
        "  Chapter Rewards reconciliation: "
        f"{reconciliation.get('old_total_points', 0):,} old / {reconciliation.get('new_total_points', 0):,} new points",
        file=output,
    )
    for warning in plan.get("warnings") or []:
        print(f"  Warning: {warning}", file=output)


def printable_commit_plan(plan: dict[str, Any]) -> dict[str, Any]:
    """Execute the printable commit plan routine."""
    result = {
        "run_id": plan.get("run_id"),
        "executed": plan.get("executed", False),
        "status": plan.get("status"),
        "affected_tournament_codes": plan.get("affected_tournament_codes") or [],
        "staged_tournament_codes": plan.get("staged_tournament_codes") or [],
        "production_cascade_tournament_codes": plan.get("production_cascade_tournament_codes") or [],
        "existing_tournament_codes": plan.get("existing_tournament_codes") or [],
        "rerun_tournament_codes": plan.get("rerun_tournament_codes") or [],
        "reward_automation_suppressed": bool(plan.get("reward_automation_suppressed", False)),
        "delete_rating_tournament_codes": plan.get("delete_rating_tournament_codes") or [],
        "replace_game_tournament_codes": plan.get("replace_game_tournament_codes") or [],
        "superseded_run_ids": plan.get("superseded_run_ids") or [],
        "tournament_upsert_count": plan.get("tournament_upsert_count", 0),
        "game_insert_count": plan.get("game_insert_count", 0),
        "rating_insert_count": plan.get("rating_insert_count", 0),
        "production_write_count": plan.get("production_write_count", 0),
        "requires_sgf_acknowledgement": bool(plan.get("requires_sgf_acknowledgement", False)),
        "warnings": plan.get("warnings") or [],
        "game_id_reconciliation": plan.get("game_id_reconciliation") or {},
        "reward_reconciliation": plan.get("reward_reconciliation") or {},
        "game_id_range": _id_range(row["planned_game_id"] for row in plan.get("planned_games") or []),
        "rating_id_range": _id_range(row["planned_rating_row_id"] for row in plan.get("planned_ratings") or []),
    }
    fingerprint = dict(result)
    fingerprint.pop("executed", None)
    result["plan_hash"] = _commit_plan_hash(fingerprint)
    return result


def _commit_plan_hash(payload: dict[str, Any]) -> str:
    """Commit plan hash."""
    stable_payload = json.dumps(payload, default=_json_default, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(stable_payload.encode("utf-8")).hexdigest()


def _commit_audit_summary(
    plan: dict[str, Any],
    operator_principal_name: str | None,
    operator_principal_id: str | None,
) -> dict[str, Any]:
    """Commit audit summary."""
    printable = printable_commit_plan(plan)
    return {
        **printable,
        "run_id": plan.get("run_id"),
        "executed": True,
        "run_status": plan.get("status"),
        "commit_status": "committed",
        "committed_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "committed_by": operator_principal_name,
        "committed_principal_id": operator_principal_id,
        "commit_plan_hash": printable.get("plan_hash"),
    }


def _physical_game_identity(row: dict[str, Any]) -> tuple[Any, ...]:
    """Return the stable identity of a physical tournament game."""
    player_1 = _require_int(row.get("Pin_Player_1"), "Pin_Player_1")
    player_2 = _require_int(row.get("Pin_Player_2"), "Pin_Player_2")
    players = tuple(sorted((player_1, player_2)))
    return (
        _coerce_date(row.get("Game_Date")),
        _optional_int(row.get("Round")),
        players[0],
        players[1],
    )


def _format_game_id_assignment(entry: dict[str, Any]) -> dict[str, Any]:
    """Format one staged-to-production game-ID assignment for preview and hashing."""
    row = entry["game_row"]
    return {
        "source_report_ordinal": entry.get("source_report_ordinal"),
        "source_game_ordinal": entry.get("source_game_ordinal"),
        "tournament_code": row.get("Tournament_Code"),
        "game_date": row.get("Game_Date"),
        "round": _optional_int(row.get("Round")),
        "pin_player_1": _optional_int(row.get("Pin_Player_1")),
        "pin_player_2": _optional_int(row.get("Pin_Player_2")),
        "match_status": entry.get("game_id_match_status"),
        "previous_game_id": entry.get("previous_game_id"),
        "planned_game_id": entry.get("planned_game_id"),
        "sgf_preserved": bool(row.get("Sgf_Code") and entry.get("game_id_match_status") == "retained"),
    }


def _format_retired_game(row: dict[str, Any]) -> dict[str, Any]:
    """Format a production game whose ID will be retired."""
    return {
        "game_id": _require_int(row.get("Game_ID"), "Game_ID"),
        "tournament_code": row.get("Tournament_Code"),
        "game_date": row.get("Game_Date"),
        "round": _optional_int(row.get("Round")),
        "pin_player_1": _optional_int(row.get("Pin_Player_1")),
        "pin_player_2": _optional_int(row.get("Pin_Player_2")),
        "sgf_linked": bool(row.get("Sgf_Code")),
    }


def _normalize_staged_rating_row(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize staged rating row."""
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
    """Execute the placeholders routine."""
    return ", ".join("?" for _ in values)


def _ordered_unique(values: Iterable[Any]) -> list[str]:
    """Execute the ordered unique routine."""
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
    """Return whether host chapter."""
    return _optional_int(row.get("Host_ChapterID")) is not None and bool(str(row.get("Host_ChapterCode") or "").strip())


def _ensure_reward_event_defaults(row: dict[str, Any]) -> None:
    """Ensure reward event defaults."""
    if not str(row.get("Reward_Event_Key") or "").strip():
        row["Reward_Event_Key"] = str(row.get("Tournament_Code") or "").strip() or None
    if not str(row.get("Reward_Event_Name") or "").strip():
        row["Reward_Event_Name"] = str(row.get("Tournament_Descr") or "").strip() or None
    row["Reward_Is_State_Championship"] = 1 if _coerce_bool(row.get("Reward_Is_State_Championship")) else 0


def _coerce_bool(value: Any) -> bool:
    """Coerce bool."""
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
    """Execute the id range routine."""
    ids = list(values)
    if not ids:
        return {"first": None, "last": None}
    return {"first": min(ids), "last": max(ids)}


def _require_int(value: Any, name: str) -> int:
    """Execute the require int routine."""
    result = _optional_int(value)
    if result is None:
        raise ValueError(f"{name} is required.")
    return result


def _optional_int(value: Any) -> int | None:
    """Execute the optional int routine."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return int(float(text))


def _optional_float(value: Any) -> float | None:
    """Execute the optional float routine."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return float(text)


def _json_loads(value: Any, default: Any) -> Any:
    """Execute the json loads routine."""
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return default


def _json_dumps(value: Any) -> str:
    """Execute the json dumps routine."""
    return json.dumps(value, default=_json_default, sort_keys=True)


if __name__ == "__main__":
    main()
