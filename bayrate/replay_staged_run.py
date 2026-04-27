import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, TextIO

from bayrate.core import (
    BayrateConfig,
    BayrateRunResult,
    GameRecord,
    TdListEntry,
    rank_to_seed,
    result_to_json,
    run_bayrate_loaded,
)
from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string
from bayrate.stage_reports import StageSqlAdapter, _coerce_date, _coerce_float, _coerce_int, load_staged_run


PRODUCTION_EVENT_SUMMARY_SQL = """
SELECT
    t.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date],
    COUNT(DISTINCT g.[Game_ID]) AS [GameCount],
    MIN(g.[Game_ID]) AS [FirstGameID],
    MIN(r.[id]) AS [FirstRatingRowID],
    MAX(r.[id]) AS [LastRatingRowID],
    COUNT(DISTINCT r.[id]) AS [RatingRowCount]
FROM [ratings].[tournaments] AS t
LEFT JOIN [ratings].[games] AS g
    ON g.[Tournament_Code] = t.[Tournament_Code]
LEFT JOIN [ratings].[ratings] AS r
    ON r.[Tournament_Code] = t.[Tournament_Code]
WHERE t.[Tournament_Code] = ?
GROUP BY
    t.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date]
"""

PRODUCTION_EVENT_SUMMARIES_FROM_DATE_SQL = """
SELECT
    t.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date],
    COUNT(DISTINCT g.[Game_ID]) AS [GameCount],
    MIN(g.[Game_ID]) AS [FirstGameID],
    MIN(r.[id]) AS [FirstRatingRowID],
    MAX(r.[id]) AS [LastRatingRowID],
    COUNT(DISTINCT r.[id]) AS [RatingRowCount]
FROM [ratings].[tournaments] AS t
LEFT JOIN [ratings].[games] AS g
    ON g.[Tournament_Code] = t.[Tournament_Code]
LEFT JOIN [ratings].[ratings] AS r
    ON r.[Tournament_Code] = t.[Tournament_Code]
WHERE t.[Tournament_Date] >= ?
GROUP BY
    t.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date]
ORDER BY
    t.[Tournament_Date],
    CASE WHEN MIN(r.[id]) IS NULL THEN 1 ELSE 0 END,
    MIN(r.[id]),
    MIN(g.[Game_ID]),
    t.[Tournament_Code]
"""

PRODUCTION_GAMES_FOR_REPLAY_SQL = """
SELECT
    g.[Game_ID],
    g.[Tournament_Code],
    g.[Game_Date],
    g.[Round],
    g.[Pin_Player_1],
    g.[Color_1],
    g.[Rank_1],
    g.[Pin_Player_2],
    g.[Color_2],
    g.[Rank_2],
    g.[Handicap],
    g.[Komi],
    g.[Result],
    g.[Sgf_Code],
    g.[Online],
    g.[Exclude],
    g.[Rated],
    g.[Elab_Date]
FROM [ratings].[games] AS g
WHERE g.[Tournament_Code] = ?
ORDER BY g.[Game_Date], g.[Round], g.[Game_ID]
"""

STARTER_TDLIST_BEFORE_RATING_ROW_SQL = """
WITH ranked AS (
    SELECT
        r.[Pin_Player],
        r.[Rating],
        r.[Sigma],
        r.[Elab_Date],
        r.[id],
        ROW_NUMBER() OVER (
            PARTITION BY r.[Pin_Player]
            ORDER BY r.[Elab_Date] DESC, r.[id] DESC
        ) AS [rn]
    FROM [ratings].[ratings] AS r
    WHERE r.[Pin_Player] IS NOT NULL
      AND r.[Rating] IS NOT NULL
      AND r.[Sigma] IS NOT NULL
      AND r.[id] < ?
)
SELECT
    [Pin_Player],
    [Rating],
    [Sigma],
    [Elab_Date],
    [id]
FROM ranked
WHERE [rn] = 1
ORDER BY [Pin_Player]
"""

STARTER_TDLIST_BEFORE_DATE_SQL = """
WITH ranked AS (
    SELECT
        r.[Pin_Player],
        r.[Rating],
        r.[Sigma],
        r.[Elab_Date],
        r.[id],
        ROW_NUMBER() OVER (
            PARTITION BY r.[Pin_Player]
            ORDER BY r.[Elab_Date] DESC, r.[id] DESC
        ) AS [rn]
    FROM [ratings].[ratings] AS r
    WHERE r.[Pin_Player] IS NOT NULL
      AND r.[Rating] IS NOT NULL
      AND r.[Sigma] IS NOT NULL
      AND r.[Elab_Date] < ?
)
SELECT
    [Pin_Player],
    [Rating],
    [Sigma],
    [Elab_Date],
    [id]
FROM ranked
WHERE [rn] = 1
ORDER BY [Pin_Player]
"""

STARTER_TDLIST_WITH_SAME_DAY_PREDECESSORS_SQL_TEMPLATE = """
WITH ranked AS (
    SELECT
        r.[Pin_Player],
        r.[Rating],
        r.[Sigma],
        r.[Elab_Date],
        r.[id],
        ROW_NUMBER() OVER (
            PARTITION BY r.[Pin_Player]
            ORDER BY r.[Elab_Date] DESC, r.[id] DESC
        ) AS [rn]
    FROM [ratings].[ratings] AS r
    WHERE r.[Pin_Player] IS NOT NULL
      AND r.[Rating] IS NOT NULL
      AND r.[Sigma] IS NOT NULL
      AND (
          r.[Elab_Date] < ?
          OR (
              r.[Elab_Date] = ?
              AND r.[Tournament_Code] IN ({placeholders})
          )
      )
)
SELECT
    [Pin_Player],
    [Rating],
    [Sigma],
    [Elab_Date],
    [id]
FROM ranked
WHERE [rn] = 1
ORDER BY [Pin_Player]
"""

DELETE_STAGED_RATINGS_SQL = """
DELETE FROM [ratings].[bayrate_staged_ratings]
WHERE [RunID] = ?
"""

INSERT_STAGED_RATING_SQL = """
INSERT INTO [ratings].[bayrate_staged_ratings]
(
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
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


@dataclass(frozen=True)
class PlannedEvent:
    source: str
    tournament_code: str
    tournament_date: date
    game_count: int
    source_report_ordinal: int | None = None
    staged_code: str | None = None
    replaced_production_code: str | None = None
    first_rating_row_id: int | None = None
    last_rating_row_id: int | None = None
    rating_row_count: int = 0
    tournament_descr: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a staged BayRate replay without writing production tables.")
    parser.add_argument("--run-id", required=True, help="Staged BayRate RunID to replay.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    parser.add_argument("--output", type=Path, help="JSON artifact path. Defaults under bayrate/output.")
    parser.add_argument("--max-production-events", type=int, help="Limit cascade production events after the staged anchor.")
    parser.add_argument("--require-ready", action="store_true", help="Require staged tournaments to be ready_for_rating.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        raise SystemExit(1)
    try:
        artifact = run_staged_replay(
            SqlAdapter(conn_str),
            run_id=args.run_id,
            output_path=args.output,
            allow_needs_review=not args.require_ready,
            max_production_events=args.max_production_events,
        )
        print_replay_summary(artifact, sys.stdout)
    except Exception as exc:
        print(f"Staged replay failed: {exc}", file=sys.stderr)
        raise SystemExit(1)


def run_staged_replay(
    adapter: StageSqlAdapter,
    *,
    run_id: str | None = None,
    payload: dict[str, Any] | None = None,
    output_path: Path | None = None,
    allow_needs_review: bool = True,
    max_production_events: int | None = None,
    config: BayrateConfig | None = None,
    write_artifact: bool = True,
    persist_staged_ratings: bool = False,
) -> dict[str, Any]:
    replay_input = build_staged_replay_input(
        adapter,
        run_id=run_id,
        payload=payload,
        allow_needs_review=allow_needs_review,
        max_production_events=max_production_events,
    )
    result = run_bayrate_loaded(
        replay_input["games"],
        {},
        config or BayrateConfig(random_seed=1),
        initial_td_list=replay_input["initial_td_list"],
    )
    if persist_staged_ratings and replay_input["plan"].get("run_id") is None:
        raise ValueError("Cannot stage replay rating rows before a RunID has been reserved.")
    staged_rating_rows = build_staged_rating_rows(replay_input["plan"], result)
    replay_input["plan"]["rating_result_count"] = len(staged_rating_rows)
    replay_input["plan"]["staged_rating_count"] = 0
    if persist_staged_ratings:
        adapter.execute_statements(build_staged_rating_statements(replay_input["plan"]["run_id"], staged_rating_rows))
        replay_input["plan"]["staging_write_count"] = len(staged_rating_rows)
        replay_input["plan"]["staged_rating_count"] = len(staged_rating_rows)
    artifact = {
        "plan": replay_input["plan"],
        "bayrate_result": json.loads(result_to_json(result)),
        "staged_rating_summary": summarize_staged_rating_rows(staged_rating_rows),
    }
    if write_artifact:
        path = output_path or default_output_path(replay_input["plan"].get("run_id"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(artifact, indent=2, default=_json_default) + "\n", encoding="utf-8")
        artifact["output_path"] = str(path)
    return artifact


def build_staged_replay_input(
    adapter: StageSqlAdapter,
    *,
    run_id: str | None = None,
    payload: dict[str, Any] | None = None,
    allow_needs_review: bool = True,
    max_production_events: int | None = None,
) -> dict[str, Any]:
    if payload is None:
        if not run_id:
            raise ValueError("Either run_id or payload is required.")
        payload = load_staged_run(adapter, run_id)

    staged_events, warnings = plan_staged_replacement_events(
        adapter,
        payload,
        allow_needs_review=allow_needs_review,
    )
    if not staged_events:
        raise ValueError("No staged tournaments are available to replay.")

    anchor_event = min(staged_events, key=_planned_event_sort_key)
    anchor_date = anchor_event.tournament_date

    replaced_codes = {
        event.replaced_production_code
        for event in staged_events
        if event.replaced_production_code
    }
    staged_codes = {event.tournament_code for event in staged_events}
    same_day_production_events = plan_same_day_production_events(adapter, anchor_date)
    same_day_predecessor_codes = [
        event.tournament_code
        for event in same_day_production_events
        if _planned_event_sort_key(event) < _planned_event_sort_key(anchor_event)
        and event.tournament_code not in replaced_codes
        and event.tournament_code not in staged_codes
    ]
    initial_td_list, starter_source = load_starter_td_list(
        adapter,
        anchor_date,
        same_day_predecessor_codes=same_day_predecessor_codes,
    )
    production_events = plan_production_cascade_events(
        adapter,
        anchor_event=anchor_event,
        replaced_codes=replaced_codes | staged_codes,
        max_production_events=max_production_events,
    )

    events = sorted([*staged_events, *production_events], key=_planned_event_sort_key)
    games: list[GameRecord] = []
    for event in events:
        if event.source == "staged":
            games.extend(_staged_event_games(payload, event))
        else:
            games.extend(_production_event_games(adapter, event))

    plan = {
        "run_id": payload["run_id"],
        "read_only": True,
        "production_write_count": 0,
        "staging_write_count": 0,
        "staged_run_status": payload.get("status"),
        "allow_needs_review": allow_needs_review,
        "anchor": _planned_event_to_dict(anchor_event),
        "starter": {
            "source": starter_source,
            "player_count": len(initial_td_list),
            "same_day_predecessor_tournament_codes": same_day_predecessor_codes,
        },
        "event_count": len(events),
        "game_count": len(games),
        "staged_event_count": sum(1 for event in events if event.source == "staged"),
        "production_cascade_event_count": sum(1 for event in events if event.source == "production"),
        "events": [_planned_event_to_dict(event) for event in events],
        "warnings": warnings,
    }
    return {
        "payload": payload,
        "plan": plan,
        "games": games,
        "initial_td_list": initial_td_list,
    }


def build_staged_rating_rows(plan: dict[str, Any], result: BayrateRunResult) -> list[dict[str, Any]]:
    events_by_key = {
        _event_key_for_plan_event(event): (index, event)
        for index, event in enumerate(plan.get("events") or [], start=1)
    }
    fallback_events: dict[str, tuple[int, dict[str, Any]]] = {}
    per_event_counts: dict[str, int] = {}
    rows: list[dict[str, Any]] = []
    for player_result in result.player_results:
        event_key = player_result.event_key
        event_info = events_by_key.get(event_key)
        if event_info is None:
            event_info = fallback_events.setdefault(
                event_key,
                (
                    len(events_by_key) + len(fallback_events) + 1,
                    {
                        "source": "staged",
                        "tournament_code": player_result.tournament_code,
                        "staged_code": player_result.tournament_code,
                        "replaced_production_code": None,
                        "source_report_ordinal": None,
                    },
                ),
            )
        event_ordinal, event = event_info
        per_event_counts[event_key] = per_event_counts.get(event_key, 0) + 1
        rows.append(
            {
                "run_id": plan["run_id"],
                "event_ordinal": event_ordinal,
                "player_ordinal": per_event_counts[event_key],
                "event_source": event.get("source") or "staged",
                "event_key": event_key,
                "tournament_code": player_result.tournament_code or event.get("tournament_code"),
                "staged_tournament_code": event.get("staged_code"),
                "replaced_production_code": event.get("replaced_production_code"),
                "source_report_ordinal": event.get("source_report_ordinal"),
                "pin_player": player_result.player_id,
                "rating": player_result.rating_after,
                "sigma": player_result.sigma_after,
                "elab_date": player_result.event_date,
                "rank_seed": player_result.rank_seed,
                "seed_before_closing_boundary": player_result.seed_before_closing_boundary,
                "prior_rating": player_result.prior_rating,
                "prior_sigma": player_result.prior_sigma,
                "planned_rating_row_id": None,
                "production_rating_row_id": None,
                "rating_delta": None,
                "sigma_delta": None,
                "metadata": {},
            }
        )
    rows.sort(key=lambda row: (row["event_ordinal"], row["player_ordinal"], row["pin_player"]))
    return rows


def build_staged_rating_statements(run_id: str, rows: list[dict[str, Any]]) -> list[SqlStatement]:
    statements: list[SqlStatement] = [(DELETE_STAGED_RATINGS_SQL, (run_id,))]
    for row in rows:
        statements.append(
            (
                INSERT_STAGED_RATING_SQL,
                (
                    row["run_id"],
                    row["event_ordinal"],
                    row["player_ordinal"],
                    row["event_source"],
                    row.get("event_key"),
                    row.get("tournament_code"),
                    row.get("staged_tournament_code"),
                    row.get("replaced_production_code"),
                    row.get("source_report_ordinal"),
                    row["pin_player"],
                    row.get("rating"),
                    row.get("sigma"),
                    row.get("elab_date"),
                    row.get("rank_seed"),
                    row.get("seed_before_closing_boundary"),
                    row.get("prior_rating"),
                    row.get("prior_sigma"),
                    row.get("planned_rating_row_id"),
                    row.get("production_rating_row_id"),
                    row.get("rating_delta"),
                    row.get("sigma_delta"),
                    json.dumps(row.get("metadata") or {}, sort_keys=True),
                ),
            )
        )
    return statements


def summarize_staged_rating_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    events: dict[tuple[int, str | None], dict[str, Any]] = {}
    for row in rows:
        key = (row["event_ordinal"], row.get("tournament_code"))
        event = events.setdefault(
            key,
            {
                "event_ordinal": row["event_ordinal"],
                "event_source": row.get("event_source"),
                "tournament_code": row.get("tournament_code"),
                "elab_date": row.get("elab_date"),
                "rating_count": 0,
            },
        )
        event["rating_count"] += 1
    return {
        "rating_count": len(rows),
        "events": list(events.values()),
    }


def plan_staged_replacement_events(
    adapter: StageSqlAdapter,
    payload: dict[str, Any],
    *,
    allow_needs_review: bool,
) -> tuple[list[PlannedEvent], list[str]]:
    if payload.get("validation_failed_count"):
        raise ValueError("Cannot replay a run with validation_failed tournaments.")

    events = []
    warnings = []
    for tournament in payload.get("staged_tournaments") or []:
        status = tournament.get("status")
        if status == "validation_failed":
            raise ValueError("Cannot replay a validation_failed staged tournament.")
        if status == "needs_review" and not allow_needs_review:
            raise ValueError("Run is needs_review. Review it first or omit --require-ready.")

        row = tournament["tournament_row"]
        staged_code = str(row.get("Tournament_Code") or "").strip()
        tournament_date = _require_date(row.get("Tournament_Date"), "Tournament_Date")
        duplicate_code = _clean_text((tournament.get("duplicate_candidate") or {}).get("tournament_code"))
        effective_code = duplicate_code or staged_code
        if not effective_code:
            raise ValueError("Staged tournament is missing Tournament_Code.")

        summary = load_production_event_summary(adapter, duplicate_code or staged_code)
        if duplicate_code and duplicate_code != staged_code:
            warnings.append(
                "Read-only replay is using duplicate candidate "
                f"{duplicate_code} in memory for staged code {staged_code}; staging rows were not changed."
            )
        if status in {"needs_review", "staged"}:
            warnings.append(f"Read-only replay includes {effective_code} while staged status is {status}.")

        staged_game_count = sum(
            1
            for game in payload.get("staged_games") or []
            if game.get("source_report_ordinal") == tournament.get("source_report_ordinal")
        )
        events.append(
            PlannedEvent(
                source="staged",
                tournament_code=effective_code,
                tournament_date=tournament_date,
                game_count=staged_game_count,
                source_report_ordinal=int(tournament["source_report_ordinal"]),
                staged_code=staged_code,
                replaced_production_code=duplicate_code,
                first_rating_row_id=_coerce_int((summary or {}).get("FirstRatingRowID")),
                last_rating_row_id=_coerce_int((summary or {}).get("LastRatingRowID")),
                rating_row_count=_coerce_int((summary or {}).get("RatingRowCount")) or 0,
                tournament_descr=row.get("Tournament_Descr"),
            )
        )
    return events, warnings


def plan_production_cascade_events(
    adapter: StageSqlAdapter,
    *,
    anchor_event: PlannedEvent,
    replaced_codes: set[str],
    max_production_events: int | None,
) -> list[PlannedEvent]:
    summaries = load_production_event_summaries_from_date(adapter, anchor_event.tournament_date)
    events = []
    for row in summaries:
        code = _clean_text(row.get("Tournament_Code"))
        if not code or code in replaced_codes:
            continue
        first_rating_row_id = _coerce_int(row.get("FirstRatingRowID"))
        tournament_date = _coerce_date(row.get("Tournament_Date"))
        if tournament_date is None:
            continue
        event = PlannedEvent(
            source="production",
            tournament_code=code,
            tournament_date=tournament_date,
            game_count=_coerce_int(row.get("GameCount")) or 0,
            first_rating_row_id=first_rating_row_id,
            last_rating_row_id=_coerce_int(row.get("LastRatingRowID")),
            rating_row_count=_coerce_int(row.get("RatingRowCount")) or 0,
            tournament_descr=row.get("Tournament_Descr"),
        )
        if tournament_date < anchor_event.tournament_date:
            continue
        if tournament_date == anchor_event.tournament_date and _planned_event_sort_key(event) <= _planned_event_sort_key(anchor_event):
            continue
        events.append(event)

    events.sort(key=_planned_event_sort_key)
    if max_production_events is not None:
        events = events[:max_production_events]
    return events


def plan_same_day_production_events(adapter: StageSqlAdapter, tournament_date: date) -> list[PlannedEvent]:
    events = []
    for row in load_production_event_summaries_from_date(adapter, tournament_date):
        code = _clean_text(row.get("Tournament_Code"))
        row_date = _coerce_date(row.get("Tournament_Date"))
        if not code or row_date != tournament_date:
            continue
        events.append(
            PlannedEvent(
                source="production",
                tournament_code=code,
                tournament_date=row_date,
                game_count=_coerce_int(row.get("GameCount")) or 0,
                first_rating_row_id=_coerce_int(row.get("FirstRatingRowID")),
                last_rating_row_id=_coerce_int(row.get("LastRatingRowID")),
                rating_row_count=_coerce_int(row.get("RatingRowCount")) or 0,
                tournament_descr=row.get("Tournament_Descr"),
            )
        )
    events.sort(key=_planned_event_sort_key)
    return events


def load_production_event_summary(adapter: StageSqlAdapter, tournament_code: str | None) -> dict[str, Any] | None:
    if not tournament_code:
        return None
    rows = adapter.query_rows(PRODUCTION_EVENT_SUMMARY_SQL, (tournament_code,))
    return rows[0] if rows else None


def load_production_event_summaries_from_date(adapter: StageSqlAdapter, anchor_date: date) -> list[dict[str, Any]]:
    return adapter.query_rows(PRODUCTION_EVENT_SUMMARIES_FROM_DATE_SQL, (anchor_date,))


def load_starter_td_list(
    adapter: StageSqlAdapter,
    anchor_date: date,
    *,
    same_day_predecessor_codes: list[str] | None = None,
) -> tuple[dict[int, TdListEntry], str]:
    predecessor_codes = list(same_day_predecessor_codes or [])
    if predecessor_codes:
        placeholders = ", ".join("?" for _ in predecessor_codes)
        query = STARTER_TDLIST_WITH_SAME_DAY_PREDECESSORS_SQL_TEMPLATE.format(placeholders=placeholders)
        rows = adapter.query_rows(query, (anchor_date, anchor_date, *predecessor_codes))
        return (
            _td_list_from_rows(rows),
            "ratings.ratings latest row per player before "
            f"{anchor_date.isoformat()} plus same-day predecessors: {', '.join(predecessor_codes)}",
        )
    rows = adapter.query_rows(STARTER_TDLIST_BEFORE_DATE_SQL, (anchor_date,))
    return _td_list_from_rows(rows), f"ratings.ratings latest row per player where Elab_Date < {anchor_date.isoformat()}"


def _td_list_from_rows(rows: Iterable[dict[str, Any]]) -> dict[int, TdListEntry]:
    td_list = {}
    for row in rows:
        player_id = _coerce_int(row.get("Pin_Player"))
        rating = _coerce_float(row.get("Rating"))
        sigma = _coerce_float(row.get("Sigma"))
        elab_date = _coerce_date(row.get("Elab_Date"))
        if player_id is None or rating is None or sigma is None or elab_date is None:
            continue
        td_list[player_id] = TdListEntry(
            player_id=player_id,
            rating=rating,
            sigma=sigma,
            last_rating_date=elab_date,
        )
    return td_list


def _staged_event_games(payload: dict[str, Any], event: PlannedEvent) -> list[GameRecord]:
    games = []
    for entry in payload.get("staged_games") or []:
        if entry.get("source_report_ordinal") != event.source_report_ordinal:
            continue
        record = _game_record_from_row(
            entry["game_row"],
            tournament_code=event.tournament_code,
            synthetic_source_game_id=_coerce_int(entry.get("source_game_ordinal")),
        )
        if record is not None:
            games.append(record)
    return games


def _production_event_games(adapter: StageSqlAdapter, event: PlannedEvent) -> list[GameRecord]:
    rows = adapter.query_rows(PRODUCTION_GAMES_FOR_REPLAY_SQL, (event.tournament_code,))
    games = []
    for row in rows:
        record = _game_record_from_row(row, tournament_code=event.tournament_code)
        if record is not None:
            games.append(record)
    return games


def _game_record_from_row(
    row: dict[str, Any],
    *,
    tournament_code: str,
    synthetic_source_game_id: int | None = None,
) -> GameRecord | None:
    rated = _coerce_bool(row.get("Rated"), default=True)
    excluded = _coerce_bool(row.get("Exclude"), default=False)
    is_online = _coerce_bool(row.get("Online"), default=False)
    if not rated or excluded or is_online:
        return None

    color_1 = str(row.get("Color_1") or "").strip().upper()
    result = str(row.get("Result") or "").strip().upper()
    if color_1 not in {"W", "B"}:
        raise ValueError(f"Color_1 must be W or B for tournament {tournament_code}.")
    if result not in {"W", "B"}:
        raise ValueError(f"Result must be W or B for tournament {tournament_code}.")

    source_game_id = _coerce_int(row.get("Game_ID")) or synthetic_source_game_id
    game_date = _coerce_date(row.get("Game_Date"))
    pin_1 = _coerce_int(row.get("Pin_Player_1"))
    pin_2 = _coerce_int(row.get("Pin_Player_2"))
    handicap = _coerce_int(row.get("Handicap"))
    komi = _coerce_float(row.get("Komi"))
    seed_1 = rank_to_seed(str(row.get("Rank_1") or ""))
    seed_2 = rank_to_seed(str(row.get("Rank_2") or ""))
    if (
        source_game_id is None
        or game_date is None
        or pin_1 is None
        or pin_2 is None
        or handicap is None
        or komi is None
        or seed_1 is None
        or seed_2 is None
    ):
        raise ValueError(f"Game row for tournament {tournament_code} is missing required replay fields.")

    if color_1 == "W":
        white_agaid = pin_1
        black_agaid = pin_2
        white_seed_rank = seed_1
        black_seed_rank = seed_2
    else:
        white_agaid = pin_2
        black_agaid = pin_1
        white_seed_rank = seed_2
        black_seed_rank = seed_1

    return GameRecord(
        source_game_id=source_game_id,
        tournament_code=tournament_code,
        game_date=game_date,
        round_number=_coerce_int(row.get("Round")),
        white_agaid=white_agaid,
        black_agaid=black_agaid,
        white_seed_rank=white_seed_rank,
        black_seed_rank=black_seed_rank,
        handicap=handicap,
        komi=komi,
        white_wins=(result == "W"),
        is_online_game=is_online,
    )


def _coerce_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")


def _planned_event_sort_key(event: PlannedEvent) -> tuple[Any, ...]:
    rating_key = event.first_rating_row_id
    if rating_key is None:
        rating_key = -1 if event.source == "staged" else 10**18
    return (
        event.tournament_date,
        rating_key,
        0 if event.source == "staged" else 1,
        event.tournament_code,
    )


def _planned_event_to_dict(event: PlannedEvent) -> dict[str, Any]:
    return {
        "source": event.source,
        "tournament_code": event.tournament_code,
        "staged_code": event.staged_code,
        "replaced_production_code": event.replaced_production_code,
        "tournament_date": event.tournament_date,
        "game_count": event.game_count,
        "source_report_ordinal": event.source_report_ordinal,
        "first_rating_row_id": event.first_rating_row_id,
        "last_rating_row_id": event.last_rating_row_id,
        "rating_row_count": event.rating_row_count,
        "tournament_descr": event.tournament_descr,
    }


def _event_key_for_plan_event(event: dict[str, Any]) -> str:
    tournament_code = event.get("tournament_code")
    if tournament_code:
        return f"code:{tournament_code}"
    tournament_date = event.get("tournament_date")
    if hasattr(tournament_date, "isoformat"):
        tournament_date = tournament_date.isoformat()
    return f"date:{tournament_date}"


def default_output_path(run_id: str | int | None) -> Path:
    run_id_text = "unwritten" if run_id is None else str(run_id)
    safe_run_id = "".join(ch for ch in run_id_text if ch.isalnum() or ch == "-")
    return Path(__file__).resolve().parent / "output" / f"staged_replay_{safe_run_id}.json"


def print_replay_summary(artifact: dict[str, Any], output: TextIO) -> None:
    plan = artifact["plan"]
    anchor = plan["anchor"]
    result = artifact["bayrate_result"]
    print("", file=output)
    print("Read-only BayRate Replay", file=output)
    print(f"  RunID: {plan['run_id']}", file=output)
    print(f"  Staged status: {plan['staged_run_status']}", file=output)
    print(
        f"  Anchor: {anchor['tournament_code']} on {anchor['tournament_date']} "
        f"rating_row={anchor['first_rating_row_id']}",
        file=output,
    )
    print(
        f"  Events: {plan['event_count']} "
        f"({plan['staged_event_count']} staged, {plan['production_cascade_event_count']} production cascade)",
        file=output,
    )
    print(f"  Games: {plan['game_count']}", file=output)
    print(f"  Starter players: {plan['starter']['player_count']}", file=output)
    print(f"  BayRate players after replay: {result['player_count']}", file=output)
    print(f"  Output: {artifact.get('output_path')}", file=output)
    if plan.get("warnings"):
        print("  Warnings:", file=output)
        for warning in plan["warnings"]:
            print(f"    - {warning}", file=output)
    print("  SQL writes: 0", file=output)


def _require_date(value: Any, column: str) -> date:
    parsed = _coerce_date(value)
    if parsed is None:
        raise ValueError(f"{column} is required for staged replay.")
    return parsed


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


if __name__ == "__main__":
    main()
