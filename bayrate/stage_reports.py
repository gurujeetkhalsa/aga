import argparse
import hashlib
import json
import math
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Protocol

from bayrate.core import rank_to_seed
from bayrate.report_parser import CSV_TABLE_SPECS, _normalize_title_for_match, parse_reports_to_rows
from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string


VALIDATION_STATUSES = {"staged", "validation_failed", "needs_review", "ready_for_rating"}
DUPLICATE_DATE_WINDOW_DAYS = 7
DUPLICATE_SCORE_THRESHOLD = 0.74

PRODUCTION_CANDIDATES_SQL = """
SELECT
    t.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date],
    t.[City],
    t.[State_Code],
    t.[Country_Code],
    g.[Game_ID],
    g.[Game_Date],
    g.[Round],
    g.[Pin_Player_1],
    g.[Pin_Player_2],
    g.[Handicap],
    g.[Komi],
    g.[Result]
FROM [ratings].[tournaments] AS t
LEFT JOIN [ratings].[games] AS g
    ON g.[Tournament_Code] = t.[Tournament_Code]
WHERE t.[Tournament_Date] BETWEEN ? AND ?
ORDER BY t.[Tournament_Date], t.[Tournament_Code], g.[Game_Date], g.[Round], g.[Game_ID]
"""

PRODUCTION_TOURNAMENT_GAMES_SQL = """
SELECT
    t.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date],
    t.[City],
    t.[State_Code],
    t.[Country_Code],
    g.[Game_ID],
    g.[Game_Date],
    g.[Round],
    g.[Pin_Player_1],
    g.[Pin_Player_2],
    g.[Handicap],
    g.[Komi],
    g.[Result]
FROM [ratings].[tournaments] AS t
LEFT JOIN [ratings].[games] AS g
    ON g.[Tournament_Code] = t.[Tournament_Code]
WHERE t.[Tournament_Code] = ?
ORDER BY g.[Game_Date], g.[Round], g.[Game_ID]
"""

PRODUCTION_SAME_DATE_TOURNAMENTS_SQL = """
SELECT
    t.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date],
    t.[City],
    t.[State_Code],
    t.[Country_Code],
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
WHERE t.[Tournament_Date] = ?
GROUP BY
    t.[Tournament_Code],
    t.[Tournament_Descr],
    t.[Tournament_Date],
    t.[City],
    t.[State_Code],
    t.[Country_Code]
ORDER BY
    CASE WHEN MIN(r.[id]) IS NULL THEN 1 ELSE 0 END,
    MIN(r.[id]),
    MIN(g.[Game_ID]),
    t.[Tournament_Code]
"""

BAYRATE_NEXT_RUN_ID_SQL = """
SELECT NEXT VALUE FOR [ratings].[bayrate_run_id_seq] AS [RunID]
"""

MEMBERSHIP_RECORDS_SQL_TEMPLATE = """
SELECT
    [AGAID],
    [FirstName],
    [LastName],
    [ExpirationDate]
FROM [membership].[members]
WHERE [AGAID] IN ({placeholders})
"""

HOST_CHAPTER_OPTIONS_SQL = """
SELECT
    [ChapterID],
    [ChapterCode],
    [ChapterName],
    [City],
    [State]
FROM [membership].[chapters]
WHERE [ChapterID] IS NOT NULL
  AND NULLIF(LTRIM(RTRIM([ChapterCode])), N'') IS NOT NULL
  AND NULLIF(LTRIM(RTRIM([ChapterName])), N'') IS NOT NULL
ORDER BY [ChapterCode], [ChapterName], [ChapterID]
"""

CURRENT_RATINGS_BEFORE_DATE_SQL_TEMPLATE = """
WITH ranked AS (
    SELECT
        r.[Pin_Player],
        r.[Rating],
        r.[Sigma],
        r.[Elab_Date],
        r.[Tournament_Code],
        r.[id],
        ROW_NUMBER() OVER (
            PARTITION BY r.[Pin_Player]
            ORDER BY r.[Elab_Date] DESC, r.[id] DESC
        ) AS [rn]
    FROM [ratings].[ratings] AS r
    WHERE r.[Pin_Player] IN ({placeholders})
      AND r.[Rating] IS NOT NULL
      AND r.[Elab_Date] < ?
)
SELECT
    [Pin_Player],
    [Rating],
    [Sigma],
    [Elab_Date],
    [Tournament_Code],
    [id]
FROM ranked
WHERE [rn] = 1
ORDER BY [Pin_Player]
"""

NAME_TOKEN_RE = re.compile(r"[a-z0-9]+")


class StageSqlAdapter(Protocol):
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        ...

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        ...


def reserve_bayrate_run_id(adapter: StageSqlAdapter) -> int:
    rows = adapter.query_rows(BAYRATE_NEXT_RUN_ID_SQL)
    if not rows:
        raise ValueError("Could not reserve a BayRate RunID.")
    return _coerce_run_id(rows[0].get("RunID"))


def assign_payload_run_id(payload: dict[str, Any], run_id: int | str) -> dict[str, Any]:
    run_identifier = _coerce_run_id(run_id)
    payload["run_id"] = run_identifier
    for entry in payload.get("staged_tournaments") or []:
        entry["run_id"] = run_identifier
    for entry in payload.get("staged_games") or []:
        entry["run_id"] = run_identifier
    return payload


def ensure_payload_run_id(payload: dict[str, Any], adapter: StageSqlAdapter) -> dict[str, Any]:
    if payload.get("run_id") is None:
        assign_payload_run_id(payload, reserve_bayrate_run_id(adapter))
    else:
        assign_payload_run_id(payload, payload["run_id"])
    return payload


@dataclass
class ProductionTournamentCandidate:
    tournament_code: str
    tournament_descr: str | None = None
    tournament_date: date | None = None
    city: str | None = None
    state_code: str | None = None
    country_code: str | None = None
    players: set[int] = field(default_factory=set)
    games: set[tuple[Any, ...]] = field(default_factory=set)

    @property
    def normalized_title(self) -> str:
        return _normalize_title_for_match(self.tournament_descr or "")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage one or more AGA tournament reports for a future BayRate run.")
    parser.add_argument("inputs", nargs="+", type=Path, help="Report text files to parse and stage.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    parser.add_argument("--dry-run", action="store_true", help="Parse and validate without inserting staging rows.")
    parser.add_argument("--skip-duplicate-checks", action="store_true", help="Do not read production tournaments/games for duplicate triage.")
    parser.add_argument("--include-games", action="store_true", help="Include staged game rows in the printed JSON payload.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        adapter: StageSqlAdapter | None = None
        conn_str = args.connection_string or get_sql_connection_string()
        if conn_str:
            adapter = SqlAdapter(conn_str)
        elif not args.dry_run:
            raise ValueError("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.")

        payload = stage_report_files(
            args.inputs,
            adapter=adapter,
            dry_run=args.dry_run,
            duplicate_check=not args.skip_duplicate_checks,
        )
        output_payload = printable_payload(payload, include_games=args.include_games)
        print(json.dumps(output_payload, indent=2 if args.pretty else None, default=_json_default))
    except Exception as exc:
        print(f"stage_reports failed: {exc}", file=sys.stderr)
        raise SystemExit(1)


def stage_report_files(
    input_paths: list[Path],
    *,
    adapter: StageSqlAdapter | None = None,
    connection_string: str | None = None,
    dry_run: bool = False,
    duplicate_check: bool = True,
    run_id: int | str | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    if adapter is None and connection_string:
        adapter = SqlAdapter(connection_string)
    if adapter is None and not dry_run:
        raise ValueError("A SQL adapter or connection string is required unless dry_run=True.")

    reports = [(str(path), path.read_text(encoding="utf-8")) for path in input_paths]
    payload = build_staging_payload(reports, adapter=adapter, duplicate_check=duplicate_check, run_id=run_id, today=today)
    if not dry_run:
        assert adapter is not None
        ensure_payload_run_id(payload, adapter)
        adapter.execute_statements(build_insert_statements(payload))
        payload["written"] = True
    else:
        payload["written"] = False
    payload["dry_run"] = dry_run
    return payload


def build_staging_payload(
    reports: list[tuple[str, str]],
    *,
    adapter: StageSqlAdapter | None = None,
    duplicate_check: bool = True,
    run_id: int | str | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    run_identifier = _coerce_run_id(run_id) if run_id is not None else None
    processing_date = today or date.today()
    parsed = parse_reports_to_rows(reports, continue_on_error=True)
    staged_tournaments: list[dict[str, Any]] = []
    staged_games: list[dict[str, Any]] = []
    run_warnings: list[dict[str, Any]] = []
    assigned_tournament_codes: set[str] = set()
    if adapter is None:
        run_warnings.append(
            {
                "type": "sql_adapter_unavailable",
                "message": "No SQL adapter was provided, so duplicate checks, AGA membership validation, and rating-system rank comparisons were skipped.",
            }
        )

    for report_ordinal, report in enumerate(parsed["reports"], start=1):
        source_name = str(report["source_name"])
        raw_text = str(report.get("raw_text") or "")
        tournament_row = dict(report["tournament_row"])
        tournament_row.setdefault("Host_ChapterID", None)
        tournament_row.setdefault("Host_ChapterCode", None)
        tournament_row.setdefault("Host_ChapterName", None)
        tournament_row.setdefault("Reward_Event_Key", None)
        tournament_row.setdefault("Reward_Event_Name", None)
        tournament_row.setdefault("Reward_Is_State_Championship", 0)
        game_rows = [dict(row) for row in report["game_rows"]]
        player_rows = [dict(row) for row in report.get("players") or []]
        parser_warnings = [dict(warning) for warning in report.get("warnings") or []]
        normalized_title = _normalize_title_for_match(tournament_row.get("Tournament_Descr") or "")
        original_code = tournament_row.get("Tournament_Code")
        code_source = _initial_code_source(parser_warnings)
        if report.get("parse_error"):
            code_source = "generated"

        duplicate_candidates: list[ProductionTournamentCandidate] | None = None
        if duplicate_check and adapter is not None:
            try:
                duplicate_candidates = load_production_candidates(adapter, tournament_row.get("Tournament_Date"))
            except Exception as exc:
                duplicate_candidates = None
                parser_warnings.append(
                    {
                        "type": "duplicate_lookup_unavailable",
                        "message": f"Production duplicate lookup was unavailable: {exc}",
                    }
                )

        if duplicate_candidates is not None:
            exact_matches = [
                candidate
                for candidate in duplicate_candidates
                if candidate.tournament_date == tournament_row.get("Tournament_Date")
                and candidate.normalized_title == normalized_title
            ]
            if len(exact_matches) == 1:
                tournament_row["Tournament_Code"] = exact_matches[0].tournament_code
                for game_row in game_rows:
                    game_row["Tournament_Code"] = exact_matches[0].tournament_code
                if exact_matches[0].tournament_code != original_code:
                    code_source = "reused"
                    parser_warnings.append(
                        {
                            "type": "reused_existing_tournament_code",
                            "original_code": original_code,
                            "reused_code": exact_matches[0].tournament_code,
                            "message": "Existing Tournament_Code was reused for exact title/date match.",
                        }
                    )
            elif len(exact_matches) > 1:
                parser_warnings.append(
                    {
                        "type": "ambiguous_existing_tournament_match",
                        "matches": [candidate.tournament_code for candidate in exact_matches],
                        "message": "Multiple production tournaments matched this title/date; keeping generated code.",
                    }
                )
            exact_match_codes = {candidate.tournament_code for candidate in exact_matches}
            reserved_codes = {candidate.tournament_code for candidate in duplicate_candidates}
            reserved_codes.update(assigned_tournament_codes)
            current_code = str(tournament_row.get("Tournament_Code") or "").strip()
            collides_with_different_production = current_code in reserved_codes and current_code not in exact_match_codes
            if code_source == "generated" and collides_with_different_production:
                replacement_code = _unique_generated_tournament_code(current_code, reserved_codes)
                tournament_row["Tournament_Code"] = replacement_code
                for game_row in game_rows:
                    game_row["Tournament_Code"] = replacement_code
                parser_warnings.append(
                    {
                        "type": "generated_tournament_code_collision",
                        "original_code": current_code,
                        "replacement_code": replacement_code,
                        "message": (
                            f"Generated Tournament_Code {current_code!r} already belongs to another "
                            f"same-date production tournament. Using {replacement_code!r} instead."
                        ),
                    }
                )

        _ensure_reward_event_defaults(tournament_row)

        if adapter is not None:
            try:
                membership_records = load_membership_records(adapter, _players_from_games(game_rows))
                parser_warnings.extend(
                    build_membership_validation_warnings(
                        adapter,
                        tournament_row.get("Tournament_Date"),
                        game_rows,
                        today=processing_date,
                        membership_records=membership_records,
                    )
                )
                parser_warnings.extend(build_membership_name_warnings(player_rows, membership_records))
            except Exception as exc:
                parser_warnings.append(
                    {
                        "type": "membership_validation_lookup_failed",
                        "severity": "review",
                        "review_required": True,
                        "message": f"Membership validation lookup failed: {exc}",
                    }
                )
            try:
                parser_warnings.extend(
                    build_rank_mismatch_warnings(
                        adapter,
                        tournament_row.get("Tournament_Date"),
                        game_rows,
                        player_rows,
                    )
                )
            except Exception as exc:
                parser_warnings.append(
                    {
                        "type": "rank_lookup_unavailable",
                        "message": f"Rating-system rank lookup was unavailable: {exc}",
                    }
                )
        if not report.get("parse_error") and not _has_host_chapter(tournament_row):
            parser_warnings.append(_host_chapter_required_warning())
        tournament_errors = []
        if report.get("parse_error"):
            tournament_errors.append(f"Report parse failed: {report['parse_error']}")
        tournament_errors.extend(validate_tournament_row(tournament_row, game_rows))
        game_entries: list[dict[str, Any]] = []
        for game_ordinal, game_row in enumerate(game_rows, start=1):
            game_errors = validate_game_row(game_row)
            game_status = "validation_failed" if game_errors else "staged"
            game_entries.append(
                {
                    "run_id": run_identifier,
                    "source_report_ordinal": report_ordinal,
                    "source_game_ordinal": game_ordinal,
                    "source_report_name": source_name,
                    "game_row": game_row,
                    "status": game_status,
                    "validation_errors": game_errors,
                }
            )

        all_errors = tournament_errors + [
            f"game {entry['source_game_ordinal']}: {error}"
            for entry in game_entries
            for error in entry["validation_errors"]
        ]
        review_required_warnings = [warning for warning in parser_warnings if warning_requires_review(warning)]
        review_warning_reason = review_reason_for_warnings(review_required_warnings)
        duplicate_candidate = None
        review_reason = None
        if all_errors:
            status = "validation_failed"
        elif duplicate_candidates is None:
            status = "needs_review" if review_required_warnings else "staged"
            review_reason = review_warning_reason
        else:
            duplicate_candidate = find_likely_duplicate(tournament_row, game_rows, duplicate_candidates)
            if duplicate_candidate is not None:
                status = "needs_review"
                review_reason = (
                    f"Likely duplicate of production Tournament_Code "
                    f"{duplicate_candidate['tournament_code']} with score {duplicate_candidate['score']:.2f}."
                )
                if review_warning_reason:
                    review_reason = f"{review_reason} {review_warning_reason}"
            elif review_required_warnings:
                status = "needs_review"
                review_reason = review_warning_reason
            else:
                status = "ready_for_rating"

        tournament_entry = {
            "run_id": run_identifier,
            "source_report_ordinal": report_ordinal,
            "source_report_name": source_name,
            "source_report_sha256": hashlib.sha256(raw_text.encode("utf-8")).hexdigest(),
            "tournament_row": tournament_row,
            "normalized_title": normalized_title,
            "original_tournament_code": original_code if original_code != tournament_row.get("Tournament_Code") else None,
            "code_source": code_source,
            "status": status,
            "validation_errors": all_errors,
            "parser_warnings": parser_warnings,
            "duplicate_candidate": duplicate_candidate,
            "review_reason": review_reason,
            "metadata": dict(report.get("metadata") or {}),
        }
        staged_tournaments.append(tournament_entry)
        assigned_code = str(tournament_row.get("Tournament_Code") or "").strip()
        if assigned_code:
            assigned_tournament_codes.add(assigned_code)
        for entry in game_entries:
            if status in {"ready_for_rating", "needs_review"} and not entry["validation_errors"]:
                entry["status"] = status
        staged_games.extend(game_entries)

    run_status = rollup_status([entry["status"] for entry in staged_tournaments])
    validation_error_count = sum(len(entry["validation_errors"]) for entry in staged_tournaments)
    summary = {
        "run_id": run_identifier,
        "status": run_status,
        "source_report_count": len(reports),
        "tournament_count": len(staged_tournaments),
        "game_count": len(staged_games),
        "validation_error_count": validation_error_count,
        "ready_tournament_count": sum(1 for entry in staged_tournaments if entry["status"] == "ready_for_rating"),
        "needs_review_count": sum(1 for entry in staged_tournaments if entry["status"] == "needs_review"),
        "validation_failed_count": sum(1 for entry in staged_tournaments if entry["status"] == "validation_failed"),
    }
    return {
        **summary,
        "source_report_names": [source_name for source_name, _ in reports],
        "staged_tournaments": staged_tournaments,
        "staged_games": staged_games,
        "warnings": run_warnings,
    }


def _initial_code_source(parser_warnings: list[dict[str, Any]]) -> str:
    if any(warning.get("type") == "generated_tournament_code" for warning in parser_warnings):
        return "generated"
    return "parser"


def _unique_generated_tournament_code(base_code: str, reserved_codes: set[str]) -> str:
    base = str(base_code or "").strip()[:32] or "generated"
    if base not in reserved_codes:
        return base
    for suffix_number in range(2, 1000):
        suffix = f"-{suffix_number}"
        candidate = f"{base[: 32 - len(suffix)]}{suffix}"
        if candidate not in reserved_codes:
            return candidate
    raise ValueError(f"Could not generate a unique Tournament_Code from {base_code!r}.")


def validate_tournament_row(tournament_row: dict[str, Any], game_rows: list[dict[str, Any]]) -> list[str]:
    errors = []
    for column in ("Tournament_Code", "Tournament_Descr", "Tournament_Date", "Elab_Date"):
        if _is_blank(tournament_row.get(column)):
            errors.append(f"{column} is required")
    code = str(tournament_row.get("Tournament_Code") or "")
    if len(code) > 32:
        errors.append("Tournament_Code must be 32 characters or fewer")
    if not game_rows:
        errors.append("at least one game row is required")
    return errors


def validate_game_row(game_row: dict[str, Any]) -> list[str]:
    errors = []
    required_columns = (
        "Tournament_Code",
        "Game_Date",
        "Pin_Player_1",
        "Color_1",
        "Rank_1",
        "Pin_Player_2",
        "Color_2",
        "Rank_2",
        "Handicap",
        "Komi",
        "Result",
        "Rated",
        "Exclude",
        "Online",
    )
    for column in required_columns:
        if _is_blank(game_row.get(column)):
            errors.append(f"{column} is required")

    color_1 = str(game_row.get("Color_1") or "").upper()
    color_2 = str(game_row.get("Color_2") or "").upper()
    result = str(game_row.get("Result") or "").upper()
    if color_1 and color_1 not in {"W", "B"}:
        errors.append("Color_1 must be W or B")
    if color_2 and color_2 not in {"W", "B"}:
        errors.append("Color_2 must be W or B")
    if color_1 and color_2 and color_1 == color_2:
        errors.append("Color_1 and Color_2 must be different")
    if result and result not in {"W", "B"}:
        errors.append("Result must be W or B")
    if rank_to_seed(str(game_row.get("Rank_1") or "")) is None:
        errors.append("Rank_1 must be a BayRate rank like 3k or 1d")
    if rank_to_seed(str(game_row.get("Rank_2") or "")) is None:
        errors.append("Rank_2 must be a BayRate rank like 3k or 1d")
    pin_1 = _coerce_int(game_row.get("Pin_Player_1"))
    pin_2 = _coerce_int(game_row.get("Pin_Player_2"))
    if pin_1 is not None and pin_2 is not None and pin_1 == pin_2:
        errors.append("Pin_Player_1 and Pin_Player_2 must be different")
    return errors


def validate_tournament_memberships(
    adapter: StageSqlAdapter,
    tournament_date: Any,
    game_rows: list[dict[str, Any]],
    *,
    today: date | None = None,
    membership_records: dict[int, dict[str, Any]] | None = None,
) -> list[str]:
    return [
        str(warning.get("message") or warning.get("type"))
        for warning in build_membership_validation_warnings(
            adapter,
            tournament_date,
            game_rows,
            today=today,
            membership_records=membership_records,
        )
    ]


def build_membership_validation_warnings(
    adapter: StageSqlAdapter,
    tournament_date: Any,
    game_rows: list[dict[str, Any]],
    *,
    today: date | None = None,
    membership_records: dict[int, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    event_date = _coerce_date(tournament_date)
    if event_date is None:
        return []

    player_ids = sorted(_players_from_games(game_rows))
    if not player_ids:
        return []

    processing_date = today or date.today()
    records_by_player = membership_records if membership_records is not None else load_membership_records(adapter, player_ids)
    warnings: list[dict[str, Any]] = []
    for player_id in player_ids:
        record = records_by_player.get(player_id)
        if record is None:
            warnings.append(
                {
                    "type": "missing_membership_record",
                    "severity": "review",
                    "review_required": True,
                    "agaid": player_id,
                    "event_date": event_date,
                    "message": f"AGAID {player_id} is missing a membership record.",
                }
            )
            continue

        expiration_date = record.get("expiration_date")
        if expiration_date is None:
            warnings.append(
                {
                    "type": "missing_membership_expiration",
                    "severity": "review",
                    "review_required": True,
                    "agaid": player_id,
                    "event_date": event_date,
                    "message": f"AGAID {player_id} membership expiration date is missing.",
                }
            )
        elif expiration_date >= processing_date:
            continue
        elif expiration_date < event_date:
            warnings.append(
                {
                    "type": "expired_membership_on_event_date",
                    "severity": "review",
                    "review_required": True,
                    "agaid": player_id,
                    "event_date": event_date,
                    "expiration_date": expiration_date,
                    "message": (
                        f"AGAID {player_id} membership expired on {expiration_date.isoformat()} "
                        f"before event date {event_date.isoformat()}."
                    ),
                }
            )
    return warnings


def warning_requires_review(warning: dict[str, Any]) -> bool:
    return bool(warning.get("review_required") or warning.get("severity") == "review")


def review_reason_for_warnings(warnings: list[dict[str, Any]]) -> str | None:
    if not warnings:
        return None
    messages = [str(warning.get("message") or warning.get("type") or "Review required.") for warning in warnings]
    visible_messages = messages[:3]
    if len(messages) > 3:
        visible_messages.append(f"{len(messages) - 3} more review-required warning(s).")
    return "Review required: " + " ".join(visible_messages)


def build_membership_name_warnings(
    player_rows: list[dict[str, Any]],
    membership_records: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for player in player_rows:
        player_id = _coerce_int(player.get("agaid") or player.get("AGAID"))
        if player_id is None:
            continue
        record = membership_records.get(player_id)
        if record is None:
            continue
        report_name = _clean_text(player.get("name"))
        membership_name = _clean_text(record.get("display_name"))
        if not report_name or not membership_name or _names_compatible(report_name, membership_name):
            continue
        warnings.append(
            {
                "type": "membership_name_mismatch",
                "severity": "warning",
                "agaid": player_id,
                "report_name": report_name,
                "membership_name": membership_name,
                "message": (
                    f"AGAID {player_id} report name '{report_name}' differs from "
                    f"membership name '{membership_name}'."
                ),
            }
        )
    return warnings


def build_rank_mismatch_warnings(
    adapter: StageSqlAdapter,
    tournament_date: Any,
    game_rows: list[dict[str, Any]],
    player_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    event_date = _coerce_date(tournament_date)
    if event_date is None:
        return []
    entry_ranks = _entry_ranks_by_player(game_rows)
    if not entry_ranks:
        return []

    rating_rows = load_current_ratings_before_date(adapter, entry_ranks.keys(), event_date)
    names_by_player = {
        player_id: name
        for player in player_rows
        if (player_id := _coerce_int(player.get("agaid") or player.get("AGAID"))) is not None
        and (name := _clean_text(player.get("name"))) is not None
    }
    warnings: list[dict[str, Any]] = []
    for player_id, entry_rank in sorted(entry_ranks.items()):
        rating_row = rating_rows.get(player_id)
        if rating_row is None:
            continue
        current_rating = _coerce_float(rating_row.get("Rating"))
        current_rank = _rating_to_compact_rank(current_rating)
        if current_rating is None or current_rank is None:
            continue
        entry_index = _rank_index(entry_rank)
        current_index = _rank_index(current_rank)
        if entry_index is None or current_index is None or entry_index == current_index:
            continue

        rank_distance = abs(entry_index - current_index)
        entry_below_current = entry_index < current_index
        highlight = rank_distance >= 3 or entry_below_current
        warning = {
            "type": "entry_rank_mismatch",
            "severity": "highlight" if highlight else "warning",
            "highlight": highlight,
            "agaid": player_id,
            "player_name": names_by_player.get(player_id),
            "entry_rank": entry_rank,
            "rating_system_rank": current_rank,
            "rating": round(current_rating, 3),
            "rating_elab_date": _coerce_date(rating_row.get("Elab_Date")),
            "rating_tournament_code": rating_row.get("Tournament_Code"),
            "rank_distance": rank_distance,
            "entry_below_current": entry_below_current,
        }
        context = f" ({names_by_player[player_id]})" if names_by_player.get(player_id) else ""
        warning["message"] = (
            f"AGAID {player_id}{context} entered as {entry_rank}, but rating system rank before "
            f"{event_date.isoformat()} is {current_rank}."
        )
        if rank_distance >= 3:
            warning["message"] += f" Difference is {rank_distance} ranks."
        if entry_below_current:
            warning["message"] += " Entry rank is below the rating-system rank."
        warnings.append(warning)
    return warnings


def load_membership_records(adapter: StageSqlAdapter, player_ids: Iterable[int]) -> dict[int, dict[str, Any]]:
    distinct_player_ids = _distinct_player_ids(player_ids)
    if not distinct_player_ids:
        return {}
    placeholders = ", ".join("?" for _ in distinct_player_ids)
    rows = adapter.query_rows(
        MEMBERSHIP_RECORDS_SQL_TEMPLATE.format(placeholders=placeholders),
        tuple(distinct_player_ids),
    )
    records: dict[int, dict[str, Any]] = {}
    for row in rows:
        player_id = _coerce_int(_row_value(row, "AGAID"))
        if player_id is None:
            continue
        first_name = _clean_text(_row_value(row, "FirstName"))
        last_name = _clean_text(_row_value(row, "LastName"))
        display_name = " ".join(part for part in (first_name, last_name) if part) or None
        records[player_id] = {
            "agaid": player_id,
            "first_name": first_name,
            "last_name": last_name,
            "display_name": display_name,
            "expiration_date": _coerce_date(_row_value(row, "ExpirationDate")),
        }
    return records


def load_membership_expirations(adapter: StageSqlAdapter, player_ids: Iterable[int]) -> dict[int, date | None]:
    return {
        player_id: record.get("expiration_date")
        for player_id, record in load_membership_records(adapter, player_ids).items()
    }


def load_current_ratings_before_date(
    adapter: StageSqlAdapter,
    player_ids: Iterable[int],
    event_date: date,
) -> dict[int, dict[str, Any]]:
    distinct_player_ids = _distinct_player_ids(player_ids)
    if not distinct_player_ids:
        return {}
    placeholders = ", ".join("?" for _ in distinct_player_ids)
    rows = adapter.query_rows(
        CURRENT_RATINGS_BEFORE_DATE_SQL_TEMPLATE.format(placeholders=placeholders),
        (*distinct_player_ids, event_date),
    )
    ratings: dict[int, dict[str, Any]] = {}
    for row in rows:
        player_id = _coerce_int(_row_value(row, "Pin_Player"))
        if player_id is None:
            continue
        ratings[player_id] = row
    return ratings


def _distinct_player_ids(player_ids: Iterable[int]) -> list[int]:
    return sorted(
        {
            player_id
            for player_id in (_coerce_int(player_id) for player_id in player_ids)
            if player_id is not None
        }
    )


def _entry_ranks_by_player(game_rows: list[dict[str, Any]]) -> dict[int, str]:
    ranks: dict[int, str] = {}
    for row in game_rows:
        for player_column, rank_column in (("Pin_Player_1", "Rank_1"), ("Pin_Player_2", "Rank_2")):
            player_id = _coerce_int(row.get(player_column))
            rank = _normalize_rank_text(row.get(rank_column))
            if player_id is not None and rank and player_id not in ranks:
                ranks[player_id] = rank
    return ranks


def _normalize_rank_text(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    compact = text.lower().replace(" ", "")
    return compact if _rank_index(compact) is not None else None


def _rank_index(rank_text: str | None) -> int | None:
    if rank_text is None:
        return None
    text = str(rank_text).strip().lower().replace(" ", "")
    if len(text) < 2:
        return None
    suffix = text[-1]
    try:
        value = int(text[:-1])
    except ValueError:
        return None
    if value < 1:
        return None
    if suffix == "d":
        return value
    if suffix == "k":
        return 1 - value
    return None


def _rating_to_compact_rank(rating: float | None) -> str | None:
    if rating is None:
        return None
    if rating >= 1:
        return f"{max(1, int(math.floor(rating)))}d"
    return f"{max(1, int(math.floor(-rating)))}k"


def _names_compatible(left: str, right: str) -> bool:
    left_tokens = _name_tokens(left)
    right_tokens = _name_tokens(right)
    if not left_tokens or not right_tokens:
        return True
    if left_tokens == right_tokens:
        return True
    left_set = set(left_tokens)
    right_set = set(right_tokens)
    if left_set == right_set:
        return True
    return _token_set_compatible(left_set, right_set) or _token_set_compatible(right_set, left_set)


def _token_set_compatible(shorter: set[str], longer: set[str]) -> bool:
    if len(shorter) > len(longer):
        return False
    for token in shorter:
        if token in longer:
            continue
        if len(token) == 1 and any(candidate.startswith(token) for candidate in longer):
            continue
        return False
    return True


def _name_tokens(value: str) -> tuple[str, ...]:
    ascii_text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii").lower()
    return tuple(NAME_TOKEN_RE.findall(ascii_text))


def _row_value(row: dict[str, Any], key: str) -> Any:
    if key in row:
        return row[key]
    lowered = key.lower()
    for row_key, value in row.items():
        if str(row_key).lower() == lowered:
            return value
    return None


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def load_production_candidates(
    adapter: StageSqlAdapter,
    tournament_date: Any,
    *,
    window_days: int = DUPLICATE_DATE_WINDOW_DAYS,
) -> list[ProductionTournamentCandidate]:
    parsed_date = _coerce_date(tournament_date)
    if parsed_date is None:
        return []
    start = parsed_date - timedelta(days=window_days)
    finish = parsed_date + timedelta(days=window_days)
    rows = adapter.query_rows(PRODUCTION_CANDIDATES_SQL, (start, finish))
    candidates_by_code: dict[str, ProductionTournamentCandidate] = {}
    for row in rows:
        code = str(row.get("Tournament_Code") or "").strip()
        if not code:
            continue
        candidate = candidates_by_code.get(code)
        if candidate is None:
            candidate = ProductionTournamentCandidate(
                tournament_code=code,
                tournament_descr=row.get("Tournament_Descr"),
                tournament_date=_coerce_date(row.get("Tournament_Date")),
                city=_clean_text(row.get("City")),
                state_code=_clean_text(row.get("State_Code")),
                country_code=_clean_text(row.get("Country_Code")),
            )
            candidates_by_code[code] = candidate
        for player_column in ("Pin_Player_1", "Pin_Player_2"):
            player_id = _coerce_int(row.get(player_column))
            if player_id is not None:
                candidate.players.add(player_id)
        game_signature = _game_signature(row)
        if game_signature is not None:
            candidate.games.add(game_signature)
    return list(candidates_by_code.values())


def find_likely_duplicate(
    tournament_row: dict[str, Any],
    game_rows: list[dict[str, Any]],
    candidates: list[ProductionTournamentCandidate],
) -> dict[str, Any] | None:
    staged_code = str(tournament_row.get("Tournament_Code") or "").strip()
    staged_players = _players_from_games(game_rows)
    staged_games = {signature for row in game_rows if (signature := _game_signature(row)) is not None}
    best: dict[str, Any] | None = None

    for candidate in candidates:
        if candidate.tournament_code == staged_code:
            continue
        score_parts = duplicate_score_parts(tournament_row, staged_players, staged_games, candidate)
        score = (
            0.34 * score_parts["title"]
            + 0.20 * score_parts["date"]
            + 0.14 * score_parts["location"]
            + 0.22 * score_parts["player_overlap"]
            + 0.10 * score_parts["game_overlap"]
        )
        likely = score >= DUPLICATE_SCORE_THRESHOLD or (
            score_parts["title"] >= 0.92
            and score_parts["date"] >= 0.75
            and score_parts["player_overlap"] >= 0.50
        )
        if not likely:
            continue
        candidate_payload = {
            "tournament_code": candidate.tournament_code,
            "tournament_descr": candidate.tournament_descr,
            "tournament_date": candidate.tournament_date,
            "score": round(score, 4),
            "score_parts": score_parts,
        }
        if best is None or candidate_payload["score"] > best["score"]:
            best = candidate_payload
    return best


def duplicate_score_parts(
    tournament_row: dict[str, Any],
    staged_players: set[int],
    staged_games: set[tuple[Any, ...]],
    candidate: ProductionTournamentCandidate,
) -> dict[str, float]:
    staged_title = _normalize_title_for_match(tournament_row.get("Tournament_Descr") or "")
    title_score = _token_jaccard(staged_title, candidate.normalized_title)
    staged_date = _coerce_date(tournament_row.get("Tournament_Date"))
    date_score = _date_score(staged_date, candidate.tournament_date)
    location_score = _location_score(tournament_row, candidate)
    return {
        "title": title_score,
        "date": date_score,
        "location": location_score,
        "player_overlap": _overlap_score(staged_players, candidate.players),
        "game_overlap": _overlap_score(staged_games, candidate.games),
    }


def _token_jaccard(left: str, right: str) -> float:
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    if not left_tokens and not right_tokens:
        return 1.0
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _date_score(left: date | None, right: date | None) -> float:
    if left is None or right is None:
        return 0.0
    days = abs((left - right).days)
    if days == 0:
        return 1.0
    if days <= 2:
        return 0.75
    if days <= DUPLICATE_DATE_WINDOW_DAYS:
        return 0.45
    return 0.0


def _location_score(tournament_row: dict[str, Any], candidate: ProductionTournamentCandidate) -> float:
    city = _clean_text(tournament_row.get("City"))
    state = _clean_text(tournament_row.get("State_Code"))
    country = _clean_text(tournament_row.get("Country_Code"))
    city_match = bool(city and candidate.city and city.lower() == candidate.city.lower())
    state_match = bool(state and candidate.state_code and state.upper() == candidate.state_code.upper())
    country_match = bool(country and candidate.country_code and country.upper() == candidate.country_code.upper())
    if city_match and state_match:
        return 1.0
    if state_match and country_match:
        return 0.7
    if city_match or state_match:
        return 0.5
    if country_match:
        return 0.2
    return 0.0


def _overlap_score(left: set[Any], right: set[Any]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / min(len(left), len(right))


def _players_from_games(game_rows: list[dict[str, Any]]) -> set[int]:
    players = set()
    for row in game_rows:
        for column in ("Pin_Player_1", "Pin_Player_2"):
            player_id = _coerce_int(row.get(column))
            if player_id is not None:
                players.add(player_id)
    return players


def _game_signature(row: dict[str, Any]) -> tuple[Any, ...] | None:
    player_1 = _coerce_int(row.get("Pin_Player_1"))
    player_2 = _coerce_int(row.get("Pin_Player_2"))
    if player_1 is None or player_2 is None:
        return None
    game_date = _coerce_date(row.get("Game_Date"))
    players = tuple(sorted((player_1, player_2)))
    return (
        game_date,
        _coerce_int(row.get("Round")),
        players[0],
        players[1],
        _coerce_int(row.get("Handicap")),
        _coerce_float(row.get("Komi")),
        str(row.get("Result") or "").strip().upper() or None,
    )


def rollup_status(statuses: list[str]) -> str:
    if any(status == "validation_failed" for status in statuses):
        return "validation_failed"
    if any(status == "needs_review" for status in statuses):
        return "needs_review"
    if statuses and all(status == "ready_for_rating" for status in statuses):
        return "ready_for_rating"
    return "staged"


def refresh_payload_summary(payload: dict[str, Any]) -> dict[str, Any]:
    tournaments = payload.get("staged_tournaments") or []
    games = payload.get("staged_games") or []
    payload["tournament_count"] = len(tournaments)
    payload["game_count"] = len(games)
    payload["validation_error_count"] = sum(len(entry.get("validation_errors") or []) for entry in tournaments)
    payload["ready_tournament_count"] = sum(1 for entry in tournaments if entry.get("status") == "ready_for_rating")
    payload["needs_review_count"] = sum(1 for entry in tournaments if entry.get("status") == "needs_review")
    payload["validation_failed_count"] = sum(1 for entry in tournaments if entry.get("status") == "validation_failed")
    payload["status"] = rollup_status([entry.get("status") for entry in tournaments])
    return payload


def apply_tournament_review_decision(
    payload: dict[str, Any],
    source_report_ordinal: int,
    *,
    use_duplicate_code: bool = False,
    mark_ready: bool = False,
    operator_note: str | None = None,
    host_chapter_id: int | str | None = None,
    host_chapter_code: str | None = None,
    host_chapter_name: str | None = None,
    reward_event_key: str | None = None,
    reward_event_name: str | None = None,
    reward_is_state_championship: Any = None,
) -> dict[str, Any]:
    tournament = _find_staged_tournament(payload, source_report_ordinal)
    if tournament is None:
        raise ValueError(f"Source report ordinal {source_report_ordinal} was not found in staged payload.")
    if tournament.get("validation_errors"):
        raise ValueError("Cannot mark a tournament ready while validation errors are present.")

    tournament_row = tournament["tournament_row"]
    if reward_event_key is not None or reward_event_name is not None or reward_is_state_championship is not None:
        set_tournament_reward_event(
            tournament,
            reward_event_key=reward_event_key,
            reward_event_name=reward_event_name,
            reward_is_state_championship=reward_is_state_championship,
        )
    if host_chapter_id is not None:
        set_tournament_host_chapter(
            tournament,
            host_chapter_id,
            host_chapter_code=host_chapter_code,
            host_chapter_name=host_chapter_name,
        )
    old_code = tournament_row.get("Tournament_Code")
    old_reward_key = _clean_text(tournament_row.get("Reward_Event_Key"))
    reward_key_tracks_code = not old_reward_key or old_reward_key == _clean_text(old_code)
    duplicate = tournament.get("duplicate_candidate") or {}
    if use_duplicate_code:
        duplicate_code = duplicate.get("tournament_code")
        if not duplicate_code:
            raise ValueError("No duplicate candidate code is available for this tournament.")
        if duplicate_code != old_code:
            tournament["original_tournament_code"] = tournament.get("original_tournament_code") or old_code
            tournament_row["Tournament_Code"] = duplicate_code
            tournament["code_source"] = "reused"
            if reward_key_tracks_code:
                tournament_row["Reward_Event_Key"] = duplicate_code
            tournament.setdefault("parser_warnings", []).append(
                {
                    "type": "operator_reused_duplicate_tournament_code",
                    "original_code": old_code,
                    "reused_code": duplicate_code,
                    "message": "Operator confirmed the staged tournament should reuse a production Tournament_Code.",
                }
            )
            for game in _staged_games_for_tournament(payload, source_report_ordinal):
                game["game_row"]["Tournament_Code"] = duplicate_code

    if mark_ready:
        if not _has_host_chapter(tournament_row):
            raise ValueError("Host chapter must be selected before a tournament can be marked ready_for_rating.")
        tournament["status"] = "ready_for_rating"
        if use_duplicate_code and duplicate.get("tournament_code"):
            tournament["review_reason"] = (
                f"Operator confirmed duplicate candidate {duplicate['tournament_code']} "
                "and marked the tournament ready for rating."
            )
        else:
            tournament["review_reason"] = "Operator marked the tournament ready for rating."
    elif tournament.get("status") != "validation_failed":
        tournament["status"] = "needs_review"

    for game in _staged_games_for_tournament(payload, source_report_ordinal):
        if not game.get("validation_errors"):
            game["status"] = tournament["status"]
    note = _clean_text(operator_note)
    if note:
        metadata = dict(tournament.get("metadata") or {})
        metadata["operator_note"] = note
        tournament["metadata"] = metadata
        tournament.setdefault("parser_warnings", []).append(
            {
                "type": "operator_review_note",
                "message": note,
            }
        )
        if tournament.get("review_reason"):
            tournament["review_reason"] = f"{tournament['review_reason']} Operator note: {note}"
        else:
            tournament["review_reason"] = f"Operator note: {note}"
    return refresh_payload_summary(payload)


def set_tournament_reward_event(
    tournament: dict[str, Any],
    *,
    reward_event_key: str | None = None,
    reward_event_name: str | None = None,
    reward_is_state_championship: Any = None,
) -> None:
    row = tournament["tournament_row"]
    key = _clean_text(reward_event_key) if reward_event_key is not None else _clean_text(row.get("Reward_Event_Key"))
    name = _clean_text(reward_event_name) if reward_event_name is not None else _clean_text(row.get("Reward_Event_Name"))
    if key is None:
        key = _clean_text(row.get("Tournament_Code"))
    if key is None:
        raise ValueError("Reward_Event_Key is required.")
    if len(key) > 128:
        raise ValueError("Reward_Event_Key must be 128 characters or fewer.")
    if name is not None and len(name) > 255:
        raise ValueError("Reward_Event_Name must be 255 characters or fewer.")
    row["Reward_Event_Key"] = key
    row["Reward_Event_Name"] = name or _clean_text(row.get("Tournament_Descr"))
    if reward_is_state_championship is not None:
        row["Reward_Is_State_Championship"] = 1 if _coerce_bool(reward_is_state_championship) else 0
    if tournament.get("status") != "validation_failed":
        tournament["status"] = "needs_review"
        tournament["review_reason"] = f"Reward event group set: {key}. Mark ready after review."


def set_tournament_host_chapter(
    tournament: dict[str, Any],
    host_chapter_id: int | str,
    *,
    host_chapter_code: str | None = None,
    host_chapter_name: str | None = None,
) -> None:
    row = tournament["tournament_row"]
    parsed_id = _coerce_int(host_chapter_id)
    if parsed_id is None:
        raise ValueError("Host_ChapterID is required.")
    code = _clean_text(host_chapter_code)
    if not code:
        raise ValueError("Host_ChapterCode is required when Host_ChapterID is set.")
    row["Host_ChapterID"] = parsed_id
    row["Host_ChapterCode"] = code
    row["Host_ChapterName"] = _clean_text(host_chapter_name)
    tournament["parser_warnings"] = [
        warning
        for warning in tournament.get("parser_warnings") or []
        if warning.get("type") != "host_chapter_required"
    ]
    label = code
    if row.get("Host_ChapterName"):
        label = f"{code} - {row['Host_ChapterName']}"
    if tournament.get("status") != "validation_failed":
        tournament["status"] = "needs_review"
        tournament["review_reason"] = f"Host chapter selected: {label}. Mark ready after review."


def _find_staged_tournament(payload: dict[str, Any], source_report_ordinal: int) -> dict[str, Any] | None:
    for tournament in payload.get("staged_tournaments") or []:
        if tournament.get("source_report_ordinal") == source_report_ordinal:
            return tournament
    return None


def _staged_games_for_tournament(payload: dict[str, Any], source_report_ordinal: int) -> list[dict[str, Any]]:
    return [
        game
        for game in payload.get("staged_games") or []
        if game.get("source_report_ordinal") == source_report_ordinal
    ]


def build_insert_statements(payload: dict[str, Any]) -> list[SqlStatement]:
    if payload.get("run_id") is None:
        raise ValueError("Cannot write BayRate staging rows before a RunID has been reserved.")
    statements: list[SqlStatement] = []
    run_summary = {
        key: payload[key]
        for key in (
            "run_id",
            "status",
            "source_report_count",
            "tournament_count",
            "game_count",
            "validation_error_count",
            "ready_tournament_count",
            "needs_review_count",
            "validation_failed_count",
        )
    }
    statements.append(
        (
            """
INSERT INTO [ratings].[bayrate_runs]
(
    [RunID],
    [Status],
    [Source_Report_Count],
    [Source_Report_Names],
    [Tournament_Count],
    [Game_Count],
    [Validation_Error_Count],
    [Ready_Tournament_Count],
    [Needs_Review_Count],
    [Validation_Failed_Count],
    [SummaryJson]
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
            (
                payload["run_id"],
                payload["status"],
                payload["source_report_count"],
                _json_dumps(payload["source_report_names"]),
                payload["tournament_count"],
                payload["game_count"],
                payload["validation_error_count"],
                payload["ready_tournament_count"],
                payload["needs_review_count"],
                payload["validation_failed_count"],
                _json_dumps(run_summary),
            ),
        )
    )
    for entry in payload["staged_tournaments"]:
        row = entry["tournament_row"]
        duplicate = entry.get("duplicate_candidate") or {}
        statements.append(
            (
                """
INSERT INTO [ratings].[bayrate_staged_tournaments]
(
    [RunID],
    [Source_Report_Ordinal],
    [Source_Report_Name],
    [Source_Report_Sha256],
    [Tournament_Code],
    [Original_Tournament_Code],
    [Tournament_Code_Source],
    [Tournament_Descr],
    [Normalized_Title],
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
    [Validation_Status],
    [Validation_Errors],
    [Parser_Warnings],
    [Duplicate_Candidate_Code],
    [Duplicate_Score],
    [Review_Reason],
    [MetadataJson]
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
                (
                    entry["run_id"],
                    entry["source_report_ordinal"],
                    entry["source_report_name"],
                    entry["source_report_sha256"],
                    row.get("Tournament_Code"),
                    entry.get("original_tournament_code"),
                    entry["code_source"],
                    row.get("Tournament_Descr"),
                    entry.get("normalized_title"),
                    row.get("Tournament_Date"),
                    row.get("City"),
                    row.get("State_Code"),
                    row.get("Country_Code"),
                    _coerce_int(row.get("Host_ChapterID")),
                    row.get("Host_ChapterCode"),
                    row.get("Host_ChapterName"),
                    row.get("Reward_Event_Key"),
                    row.get("Reward_Event_Name"),
                    1 if _coerce_bool(row.get("Reward_Is_State_Championship")) else 0,
                    row.get("Rounds"),
                    row.get("Total_Players"),
                    row.get("Wallist"),
                    row.get("Elab_Date"),
                    entry["status"],
                    _json_dumps(entry["validation_errors"]),
                    _json_dumps(entry["parser_warnings"]),
                    duplicate.get("tournament_code"),
                    duplicate.get("score"),
                    entry.get("review_reason"),
                    _json_dumps(entry["metadata"]),
                ),
            )
        )
    for entry in payload["staged_games"]:
        row = entry["game_row"]
        statements.append(
            (
                """
INSERT INTO [ratings].[bayrate_staged_games]
(
    [RunID],
    [Source_Report_Ordinal],
    [Source_Game_Ordinal],
    [Source_Report_Name],
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
    [Elab_Date],
    [Validation_Status],
    [Validation_Errors]
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
                (
                    entry["run_id"],
                    entry["source_report_ordinal"],
                    entry["source_game_ordinal"],
                    entry["source_report_name"],
                    row.get("Game_ID"),
                    row.get("Tournament_Code"),
                    row.get("Game_Date"),
                    _coerce_int(row.get("Round")),
                    row.get("Pin_Player_1"),
                    row.get("Color_1"),
                    row.get("Rank_1"),
                    row.get("Pin_Player_2"),
                    row.get("Color_2"),
                    row.get("Rank_2"),
                    row.get("Handicap"),
                    row.get("Komi"),
                    row.get("Result"),
                    row.get("Sgf_Code"),
                    row.get("Online"),
                    row.get("Exclude"),
                    row.get("Rated"),
                    row.get("Elab_Date"),
                    entry["status"],
                    _json_dumps(entry["validation_errors"]),
                ),
            )
        )
    return statements


def load_staged_run(adapter: StageSqlAdapter, run_id: int | str) -> dict[str, Any]:
    run_identifier = _coerce_run_id(run_id)
    run_rows = adapter.query_rows(
        """
SELECT
    [RunID],
    [Status],
    [Source_Report_Count],
    [Source_Report_Names],
    [Tournament_Count],
    [Game_Count],
    [Validation_Error_Count],
    [Ready_Tournament_Count],
    [Needs_Review_Count],
    [Validation_Failed_Count]
FROM [ratings].[bayrate_runs]
WHERE [RunID] = ?
""",
        (run_identifier,),
    )
    if not run_rows:
        raise ValueError(f"Staged BayRate run {run_identifier} was not found.")
    run_row = run_rows[0]
    tournament_rows = adapter.query_rows(
        """
SELECT
    [RunID],
    [Source_Report_Ordinal],
    [Source_Report_Name],
    [Source_Report_Sha256],
    [Tournament_Code],
    [Original_Tournament_Code],
    [Tournament_Code_Source],
    [Tournament_Descr],
    [Normalized_Title],
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
    [Validation_Status],
    [Validation_Errors],
    [Parser_Warnings],
    [Duplicate_Candidate_Code],
    [Duplicate_Score],
    [Review_Reason],
    [MetadataJson]
FROM [ratings].[bayrate_staged_tournaments]
WHERE [RunID] = ?
ORDER BY [Source_Report_Ordinal]
""",
        (run_identifier,),
    )
    game_rows = adapter.query_rows(
        """
SELECT
    [RunID],
    [Source_Report_Ordinal],
    [Source_Game_Ordinal],
    [Source_Report_Name],
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
    [Elab_Date],
    [Validation_Status],
    [Validation_Errors]
FROM [ratings].[bayrate_staged_games]
WHERE [RunID] = ?
ORDER BY [Source_Report_Ordinal], [Source_Game_Ordinal]
""",
        (run_identifier,),
    )

    payload = {
        "run_id": run_identifier,
        "status": run_row.get("Status"),
        "source_report_count": int(run_row.get("Source_Report_Count") or 0),
        "source_report_names": _json_loads(run_row.get("Source_Report_Names"), []),
        "tournament_count": int(run_row.get("Tournament_Count") or 0),
        "game_count": int(run_row.get("Game_Count") or 0),
        "validation_error_count": int(run_row.get("Validation_Error_Count") or 0),
        "ready_tournament_count": int(run_row.get("Ready_Tournament_Count") or 0),
        "needs_review_count": int(run_row.get("Needs_Review_Count") or 0),
        "validation_failed_count": int(run_row.get("Validation_Failed_Count") or 0),
        "staged_tournaments": [_payload_tournament_from_sql_row(row) for row in tournament_rows],
        "staged_games": [_payload_game_from_sql_row(row) for row in game_rows],
        "warnings": [],
        "written": True,
        "dry_run": False,
    }
    return refresh_payload_summary(payload)


def build_review_update_statements(payload: dict[str, Any]) -> list[SqlStatement]:
    statements: list[SqlStatement] = []
    for entry in payload.get("staged_tournaments") or []:
        row = entry["tournament_row"]
        statements.append(
            (
                """
UPDATE [ratings].[bayrate_staged_tournaments]
SET
    [Tournament_Code] = ?,
    [Original_Tournament_Code] = ?,
    [Tournament_Code_Source] = ?,
    [Host_ChapterID] = ?,
    [Host_ChapterCode] = ?,
    [Host_ChapterName] = ?,
    [Reward_Event_Key] = ?,
    [Reward_Event_Name] = ?,
    [Reward_Is_State_Championship] = ?,
    [Validation_Status] = ?,
    [Validation_Errors] = ?,
    [Parser_Warnings] = ?,
    [Review_Reason] = ?,
    [MetadataJson] = ?
WHERE [RunID] = ?
  AND [Source_Report_Ordinal] = ?
""",
                (
                    row.get("Tournament_Code"),
                    entry.get("original_tournament_code"),
                    entry.get("code_source"),
                    _coerce_int(row.get("Host_ChapterID")),
                    row.get("Host_ChapterCode"),
                    row.get("Host_ChapterName"),
                    row.get("Reward_Event_Key"),
                    row.get("Reward_Event_Name"),
                    1 if _coerce_bool(row.get("Reward_Is_State_Championship")) else 0,
                    entry.get("status"),
                    _json_dumps(entry.get("validation_errors") or []),
                    _json_dumps(entry.get("parser_warnings") or []),
                    entry.get("review_reason"),
                    _json_dumps(entry.get("metadata") or {}),
                    payload["run_id"],
                    entry["source_report_ordinal"],
                ),
            )
        )
    for entry in payload.get("staged_games") or []:
        row = entry["game_row"]
        statements.append(
            (
                """
UPDATE [ratings].[bayrate_staged_games]
SET
    [Tournament_Code] = ?,
    [Validation_Status] = ?,
    [Validation_Errors] = ?
WHERE [RunID] = ?
  AND [Source_Report_Ordinal] = ?
  AND [Source_Game_Ordinal] = ?
""",
                (
                    row.get("Tournament_Code"),
                    entry.get("status"),
                    _json_dumps(entry.get("validation_errors") or []),
                    payload["run_id"],
                    entry["source_report_ordinal"],
                    entry["source_game_ordinal"],
                ),
            )
        )
    run_summary = {
        key: payload[key]
        for key in (
            "run_id",
            "status",
            "source_report_count",
            "tournament_count",
            "game_count",
            "validation_error_count",
            "ready_tournament_count",
            "needs_review_count",
            "validation_failed_count",
        )
    }
    statements.append(
        (
            """
UPDATE [ratings].[bayrate_runs]
SET
    [Status] = ?,
    [Last_Updated_At] = SYSUTCDATETIME(),
    [Tournament_Count] = ?,
    [Game_Count] = ?,
    [Validation_Error_Count] = ?,
    [Ready_Tournament_Count] = ?,
    [Needs_Review_Count] = ?,
    [Validation_Failed_Count] = ?,
    [SummaryJson] = ?
WHERE [RunID] = ?
""",
            (
                payload["status"],
                payload["tournament_count"],
                payload["game_count"],
                payload["validation_error_count"],
                payload["ready_tournament_count"],
                payload["needs_review_count"],
                payload["validation_failed_count"],
                _json_dumps(run_summary),
                payload["run_id"],
            ),
        )
    )
    return statements


def update_staged_run_review(adapter: StageSqlAdapter, payload: dict[str, Any]) -> None:
    adapter.execute_statements(build_review_update_statements(refresh_payload_summary(payload)))


def load_host_chapter_options(adapter: StageSqlAdapter) -> list[dict[str, Any]]:
    rows = adapter.query_rows(HOST_CHAPTER_OPTIONS_SQL)
    options: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    for row in rows:
        chapter_id = _coerce_int(row.get("ChapterID"))
        code = _clean_text(row.get("ChapterCode"))
        name = _clean_text(row.get("ChapterName"))
        if chapter_id is None or not code or not name or chapter_id in seen_ids:
            continue
        seen_ids.add(chapter_id)
        label = f"{code} - {name}"
        state = _clean_text(row.get("State"))
        if state:
            label = f"{label} ({state})"
        options.append(
            {
                "chapter_id": chapter_id,
                "chapter_code": code,
                "chapter_name": name,
                "city": _clean_text(row.get("City")),
                "state": state,
                "label": label,
            }
        )
    return options


def explain_staged_run_review(adapter: StageSqlAdapter, payload: dict[str, Any]) -> dict[str, Any]:
    explanations = []
    for tournament in payload.get("staged_tournaments") or []:
        explanations.append(explain_staged_tournament_review(adapter, payload, tournament))
    return {
        "run_id": payload.get("run_id"),
        "tournaments": explanations,
    }


def explain_staged_tournament_review(
    adapter: StageSqlAdapter,
    payload: dict[str, Any],
    tournament: dict[str, Any],
) -> dict[str, Any]:
    row = tournament["tournament_row"]
    duplicate = tournament.get("duplicate_candidate") or {}
    duplicate_code = duplicate.get("tournament_code")
    production_rows = load_production_tournament_game_rows(adapter, duplicate_code) if duplicate_code else []
    staged_games = _staged_games_for_tournament(payload, int(tournament["source_report_ordinal"]))
    game_diff = compare_staged_to_production_games(staged_games, production_rows)
    same_date_rows = load_same_date_production_tournaments(adapter, row.get("Tournament_Date"))
    same_date_order = []
    for index, same_date_row in enumerate(same_date_rows, start=1):
        same_date_order.append(
            {
                "order": index,
                "tournament_code": same_date_row.get("Tournament_Code"),
                "tournament_descr": same_date_row.get("Tournament_Descr"),
                "tournament_date": same_date_row.get("Tournament_Date"),
                "game_count": same_date_row.get("GameCount") or 0,
                "first_game_id": same_date_row.get("FirstGameID"),
                "first_rating_row_id": same_date_row.get("FirstRatingRowID"),
                "last_rating_row_id": same_date_row.get("LastRatingRowID"),
                "rating_row_count": same_date_row.get("RatingRowCount") or 0,
                "is_duplicate_candidate": bool(duplicate_code and same_date_row.get("Tournament_Code") == duplicate_code),
                "is_staged_code": same_date_row.get("Tournament_Code") == row.get("Tournament_Code"),
            }
        )
    return {
        "source_report_ordinal": tournament["source_report_ordinal"],
        "status": tournament.get("status"),
        "staged_code": row.get("Tournament_Code"),
        "staged_title": row.get("Tournament_Descr"),
        "staged_date": row.get("Tournament_Date"),
        "duplicate_candidate": duplicate,
        "game_diff": game_diff,
        "same_date_order": same_date_order,
    }


def load_production_tournament_game_rows(adapter: StageSqlAdapter, tournament_code: str | None) -> list[dict[str, Any]]:
    if not tournament_code:
        return []
    return adapter.query_rows(PRODUCTION_TOURNAMENT_GAMES_SQL, (tournament_code,))


def load_same_date_production_tournaments(adapter: StageSqlAdapter, tournament_date: Any) -> list[dict[str, Any]]:
    parsed_date = _coerce_date(tournament_date)
    if parsed_date is None:
        return []
    return adapter.query_rows(PRODUCTION_SAME_DATE_TOURNAMENTS_SQL, (parsed_date,))


def compare_staged_to_production_games(
    staged_game_entries: list[dict[str, Any]],
    production_game_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    staged_by_signature: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    production_by_signature: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)

    for entry in staged_game_entries:
        signature = _game_signature(entry["game_row"])
        if signature is not None:
            staged_by_signature[signature].append(entry)
    for row in production_game_rows:
        signature = _game_signature(row)
        if signature is not None:
            production_by_signature[signature].append(row)

    staged_counter = Counter({signature: len(rows) for signature, rows in staged_by_signature.items()})
    production_counter = Counter({signature: len(rows) for signature, rows in production_by_signature.items()})
    matched_count = sum(min(staged_counter[signature], production_counter[signature]) for signature in staged_counter)

    staged_only = []
    production_only = []
    for signature in sorted(set(staged_counter) | set(production_counter), key=_signature_sort_key):
        staged_extra = staged_counter[signature] - production_counter[signature]
        production_extra = production_counter[signature] - staged_counter[signature]
        if staged_extra > 0:
            staged_only.extend(
                _format_staged_game_for_review(entry)
                for entry in staged_by_signature[signature][production_counter[signature] : production_counter[signature] + staged_extra]
            )
        if production_extra > 0:
            production_only.extend(
                _format_production_game_for_review(row)
                for row in production_by_signature[signature][staged_counter[signature] : staged_counter[signature] + production_extra]
            )

    return {
        "staged_game_count": len(staged_game_entries),
        "production_game_count": sum(1 for row in production_game_rows if row.get("Game_ID") is not None),
        "matched_game_count": matched_count,
        "staged_only_count": len(staged_only),
        "production_only_count": len(production_only),
        "staged_only": staged_only,
        "production_only": production_only,
    }


def _format_staged_game_for_review(entry: dict[str, Any]) -> dict[str, Any]:
    row = entry["game_row"]
    return {
        "source_game_ordinal": entry.get("source_game_ordinal"),
        "game_date": _json_default(row.get("Game_Date")),
        "round": _coerce_int(row.get("Round")),
        "pin_player_1": row.get("Pin_Player_1"),
        "pin_player_2": row.get("Pin_Player_2"),
        "handicap": row.get("Handicap"),
        "komi": _float_or_none(row.get("Komi")),
        "result": row.get("Result"),
    }


def _format_production_game_for_review(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "game_id": row.get("Game_ID"),
        "game_date": _json_default(row.get("Game_Date")),
        "round": _coerce_int(row.get("Round")),
        "pin_player_1": row.get("Pin_Player_1"),
        "pin_player_2": row.get("Pin_Player_2"),
        "handicap": row.get("Handicap"),
        "komi": _float_or_none(row.get("Komi")),
        "result": row.get("Result"),
    }


def _signature_sort_key(signature: tuple[Any, ...]) -> tuple[Any, ...]:
    game_date = signature[0]
    date_key = game_date.isoformat() if isinstance(game_date, date) else str(game_date)
    return (date_key, *(str("" if item is None else item) for item in signature[1:]))


def _payload_tournament_from_sql_row(row: dict[str, Any]) -> dict[str, Any]:
    duplicate_code = row.get("Duplicate_Candidate_Code")
    duplicate_candidate = None
    if duplicate_code:
        duplicate_candidate = {
            "tournament_code": duplicate_code,
            "tournament_descr": None,
            "tournament_date": None,
            "score": _float_or_none(row.get("Duplicate_Score")),
            "score_parts": {},
        }
    return {
        "run_id": _coerce_run_id(row.get("RunID")),
        "source_report_ordinal": int(row.get("Source_Report_Ordinal")),
        "source_report_name": row.get("Source_Report_Name"),
        "source_report_sha256": row.get("Source_Report_Sha256"),
        "tournament_row": {
            "Tournament_Code": row.get("Tournament_Code"),
            "Tournament_Descr": row.get("Tournament_Descr"),
            "Tournament_Date": row.get("Tournament_Date"),
            "City": row.get("City"),
            "State_Code": row.get("State_Code"),
            "Country_Code": row.get("Country_Code"),
            "Host_ChapterID": row.get("Host_ChapterID"),
            "Host_ChapterCode": row.get("Host_ChapterCode"),
            "Host_ChapterName": row.get("Host_ChapterName"),
            "Reward_Event_Key": row.get("Reward_Event_Key"),
            "Reward_Event_Name": row.get("Reward_Event_Name"),
            "Reward_Is_State_Championship": 1 if _coerce_bool(row.get("Reward_Is_State_Championship")) else 0,
            "Rounds": row.get("Rounds"),
            "Total_Players": row.get("Total_Players"),
            "Wallist": row.get("Wallist"),
            "Elab_Date": row.get("Elab_Date"),
            "status": None,
        },
        "normalized_title": row.get("Normalized_Title"),
        "original_tournament_code": row.get("Original_Tournament_Code"),
        "code_source": row.get("Tournament_Code_Source"),
        "status": row.get("Validation_Status"),
        "validation_errors": _json_loads(row.get("Validation_Errors"), []),
        "parser_warnings": _json_loads(row.get("Parser_Warnings"), []),
        "duplicate_candidate": duplicate_candidate,
        "review_reason": row.get("Review_Reason"),
        "metadata": _json_loads(row.get("MetadataJson"), {}),
    }


def _payload_game_from_sql_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": _coerce_run_id(row.get("RunID")),
        "source_report_ordinal": int(row.get("Source_Report_Ordinal")),
        "source_game_ordinal": int(row.get("Source_Game_Ordinal")),
        "source_report_name": row.get("Source_Report_Name"),
        "game_row": {
            "Game_ID": row.get("Game_ID"),
            "Tournament_Code": row.get("Tournament_Code"),
            "Game_Date": row.get("Game_Date"),
            "Round": row.get("Round"),
            "Pin_Player_1": row.get("Pin_Player_1"),
            "Color_1": row.get("Color_1"),
            "Rank_1": row.get("Rank_1"),
            "Pin_Player_2": row.get("Pin_Player_2"),
            "Color_2": row.get("Color_2"),
            "Rank_2": row.get("Rank_2"),
            "Handicap": row.get("Handicap"),
            "Komi": _float_or_none(row.get("Komi")),
            "Result": row.get("Result"),
            "Sgf_Code": row.get("Sgf_Code"),
            "Online": row.get("Online"),
            "Exclude": row.get("Exclude"),
            "Rated": row.get("Rated"),
            "Elab_Date": row.get("Elab_Date"),
        },
        "status": row.get("Validation_Status"),
        "validation_errors": _json_loads(row.get("Validation_Errors"), []),
    }


def bayrate_game_csv_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    columns = CSV_TABLE_SPECS["games"]
    for entry in payload["staged_games"]:
        row = dict(entry["game_row"])
        if _is_blank(row.get("Game_ID")):
            row["Game_ID"] = entry["source_game_ordinal"]
        rows.append({column: row.get(column) for column in columns})
    return rows


def printable_payload(payload: dict[str, Any], *, include_games: bool = False) -> dict[str, Any]:
    result = {
        "run_id": payload["run_id"],
        "status": payload["status"],
        "written": payload.get("written", False),
        "dry_run": payload.get("dry_run", False),
        "source_report_count": payload["source_report_count"],
        "tournament_count": payload["tournament_count"],
        "game_count": payload["game_count"],
        "validation_error_count": payload["validation_error_count"],
        "ready_tournament_count": payload["ready_tournament_count"],
        "needs_review_count": payload["needs_review_count"],
        "validation_failed_count": payload["validation_failed_count"],
        "warnings": payload.get("warnings", []),
        "tournaments": [
            {
                "source_report_ordinal": entry["source_report_ordinal"],
                "source_report_name": entry["source_report_name"],
                "tournament_code": entry["tournament_row"].get("Tournament_Code"),
                "original_tournament_code": entry.get("original_tournament_code"),
                "code_source": entry["code_source"],
                "description": entry["tournament_row"].get("Tournament_Descr"),
                "tournament_date": entry["tournament_row"].get("Tournament_Date"),
                "host_chapter_id": entry["tournament_row"].get("Host_ChapterID"),
                "host_chapter_code": entry["tournament_row"].get("Host_ChapterCode"),
                "host_chapter_name": entry["tournament_row"].get("Host_ChapterName"),
                "host_chapter_label": _host_chapter_label(entry["tournament_row"]),
                "reward_event_key": entry["tournament_row"].get("Reward_Event_Key"),
                "reward_event_name": entry["tournament_row"].get("Reward_Event_Name"),
                "reward_is_state_championship": bool(_coerce_bool(entry["tournament_row"].get("Reward_Is_State_Championship"))),
                "status": entry["status"],
                "validation_errors": entry["validation_errors"],
                "warnings": entry["parser_warnings"],
                "duplicate_candidate": entry.get("duplicate_candidate"),
                "review_reason": entry.get("review_reason"),
            }
            for entry in payload["staged_tournaments"]
        ],
    }
    if include_games:
        result["games"] = [
            {
                "source_report_ordinal": entry["source_report_ordinal"],
                "source_game_ordinal": entry["source_game_ordinal"],
                "tournament_code": entry["game_row"].get("Tournament_Code"),
                "game_date": entry["game_row"].get("Game_Date"),
                "round": entry["game_row"].get("Round"),
                "pin_player_1": entry["game_row"].get("Pin_Player_1"),
                "pin_player_2": entry["game_row"].get("Pin_Player_2"),
                "result": entry["game_row"].get("Result"),
                "status": entry["status"],
                "validation_errors": entry["validation_errors"],
            }
            for entry in payload["staged_games"]
        ]
    return result


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        text = str(value).strip()
        if not text:
            return None
        return int(float(text))
    except (TypeError, ValueError):
        return None


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
    return bool(_coerce_int(value))


def _coerce_run_id(value: Any) -> int:
    if value is None:
        raise ValueError("BayRate RunID is required.")
    text = str(value).strip()
    if not text:
        raise ValueError("BayRate RunID is required.")
    try:
        run_id = int(text)
    except ValueError as exc:
        raise ValueError(f"BayRate RunID must be an integer, got {value!r}.") from exc
    if run_id <= 0:
        raise ValueError("BayRate RunID must be a positive integer.")
    return run_id


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _has_host_chapter(row: dict[str, Any]) -> bool:
    return _coerce_int(row.get("Host_ChapterID")) is not None and bool(_clean_text(row.get("Host_ChapterCode")))


def _ensure_reward_event_defaults(row: dict[str, Any]) -> None:
    if not _clean_text(row.get("Reward_Event_Key")):
        row["Reward_Event_Key"] = _clean_text(row.get("Tournament_Code"))
    if not _clean_text(row.get("Reward_Event_Name")):
        row["Reward_Event_Name"] = _clean_text(row.get("Tournament_Descr"))


def _host_chapter_label(row: dict[str, Any]) -> str | None:
    code = _clean_text(row.get("Host_ChapterCode"))
    name = _clean_text(row.get("Host_ChapterName"))
    if code and name:
        return f"{code} - {name}"
    return code or name


def _host_chapter_required_warning() -> dict[str, Any]:
    return {
        "type": "host_chapter_required",
        "severity": "review",
        "review_required": True,
        "message": "Host chapter must be selected before this tournament can be marked ready for rating.",
    }


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, default=_json_default, sort_keys=True)


def _json_loads(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    main()
