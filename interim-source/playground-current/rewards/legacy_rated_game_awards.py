import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Protocol, TextIO

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string
from rewards.rated_game_awards import BASE_POINTS, SOURCE_TYPE
from rewards.snapshot_generator import MAX_MEMBER_AGAID, parse_snapshot_date


RULE_VERSION = "2026-05-02"
DEFAULT_SOURCE_AS_OF_DATE = date(2026, 2, 8)
DEFAULT_LEDGER_START_DATE = date(2026, 5, 2)
DEFAULT_VALUATION_SNAPSHOT_DATE = date(2026, 5, 2)


class LegacyRatedGameAwardSqlAdapter(Protocol):
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        ...

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        ...


@dataclass(frozen=True)
class LegacyRatedGameAwardResult:
    game_id_from: int
    game_id_to: int
    source_as_of_date: date
    ledger_start_date: date
    valuation_snapshot_date: date
    dry_run: bool
    run_id: int | None
    game_count: int
    participant_count: int
    eligible_award_count: int
    already_awarded_count: int
    new_award_count: int
    point_total: int
    missing_member_snapshot_count: int
    missing_chapter_snapshot_count: int
    inactive_player_count: int
    no_chapter_count: int
    chapter_not_current_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "game_id_from": self.game_id_from,
            "game_id_to": self.game_id_to,
            "source_as_of_date": self.source_as_of_date.isoformat(),
            "ledger_start_date": self.ledger_start_date.isoformat(),
            "valuation_snapshot_date": self.valuation_snapshot_date.isoformat(),
            "dry_run": self.dry_run,
            "run_id": self.run_id,
            "game_count": self.game_count,
            "participant_count": self.participant_count,
            "eligible_award_count": self.eligible_award_count,
            "already_awarded_count": self.already_awarded_count,
            "new_award_count": self.new_award_count,
            "point_total": self.point_total,
            "missing_member_snapshot_count": self.missing_member_snapshot_count,
            "missing_chapter_snapshot_count": self.missing_chapter_snapshot_count,
            "inactive_player_count": self.inactive_player_count,
            "no_chapter_count": self.no_chapter_count,
            "chapter_not_current_count": self.chapter_not_current_count,
        }


PROCESS_LEGACY_RATED_GAME_AWARDS_SQL = """
EXEC [rewards].[sp_process_legacy_gap_rated_game_awards]
    @GameIDFrom = ?,
    @GameIDTo = ?,
    @SourceAsOfDate = ?,
    @LedgerStartDate = ?,
    @ValuationSnapshotDate = ?,
    @RunType = ?,
    @DryRun = ?,
    @MaxMemberAGAID = ?,
    @BasePoints = ?
"""


LEGACY_RATED_GAME_AWARD_RESULT_SQL = """
SELECT TOP 1
    [RunID],
    [Snapshot_Date],
    [SummaryJson]
FROM [rewards].[reward_runs]
WHERE [Run_Type] = ?
  AND [Snapshot_Date] = ?
  AND JSON_VALUE([SummaryJson], '$.processor') = ?
  AND JSON_VALUE([SummaryJson], '$.legacy_gap') = 'true'
ORDER BY [RunID] DESC
"""


def process_legacy_rated_game_awards(
    adapter: LegacyRatedGameAwardSqlAdapter,
    game_id_from: int,
    game_id_to: int,
    *,
    source_as_of_date: date = DEFAULT_SOURCE_AS_OF_DATE,
    ledger_start_date: date = DEFAULT_LEDGER_START_DATE,
    valuation_snapshot_date: date = DEFAULT_VALUATION_SNAPSHOT_DATE,
    run_type: str = "import",
    dry_run: bool = False,
    max_member_agaid: int = MAX_MEMBER_AGAID,
    base_points: int = BASE_POINTS,
) -> LegacyRatedGameAwardResult:
    if game_id_to < game_id_from:
        raise ValueError("game_id_to must be on or after game_id_from.")

    params = (
        game_id_from,
        game_id_to,
        source_as_of_date,
        ledger_start_date,
        valuation_snapshot_date,
        run_type,
        bool(dry_run),
        max_member_agaid,
        base_points,
    )
    if dry_run:
        rows = adapter.query_rows(PROCESS_LEGACY_RATED_GAME_AWARDS_SQL, params)
        if not rows:
            raise RuntimeError("Legacy rated-game award preview did not return a result row.")
        return _result_from_row(rows[0], dry_run=True)

    adapter.execute_statements([(PROCESS_LEGACY_RATED_GAME_AWARDS_SQL, params)])
    rows = adapter.query_rows(LEGACY_RATED_GAME_AWARD_RESULT_SQL, (run_type, ledger_start_date, SOURCE_TYPE))
    if not rows:
        raise RuntimeError("Legacy rated-game award run did not produce a reward_runs row.")
    summary = json.loads(rows[0].get("SummaryJson") or "{}")
    return LegacyRatedGameAwardResult(
        game_id_from=_coerce_int(summary.get("game_id_from", game_id_from)),
        game_id_to=_coerce_int(summary.get("game_id_to", game_id_to)),
        source_as_of_date=_coerce_date(summary.get("source_balance_as_of_date")) or source_as_of_date,
        ledger_start_date=_coerce_date(rows[0].get("Snapshot_Date")) or ledger_start_date,
        valuation_snapshot_date=_coerce_date(summary.get("valuation_snapshot_date")) or valuation_snapshot_date,
        dry_run=False,
        run_id=_coerce_optional_int(rows[0].get("RunID")),
        game_count=_coerce_int(summary.get("game_count")),
        participant_count=_coerce_int(summary.get("participant_count")),
        eligible_award_count=_coerce_int(summary.get("eligible_award_count")),
        already_awarded_count=_coerce_int(summary.get("already_awarded_count")),
        new_award_count=_coerce_int(summary.get("new_award_count")),
        point_total=_coerce_int(summary.get("point_total")),
        missing_member_snapshot_count=_coerce_int(summary.get("missing_member_snapshot_count")),
        missing_chapter_snapshot_count=_coerce_int(summary.get("missing_chapter_snapshot_count")),
        inactive_player_count=_coerce_int(summary.get("inactive_player_count")),
        no_chapter_count=_coerce_int(summary.get("no_chapter_count")),
        chapter_not_current_count=_coerce_int(summary.get("chapter_not_current_count")),
    )


def _result_from_row(row: dict[str, Any], *, dry_run: bool) -> LegacyRatedGameAwardResult:
    return LegacyRatedGameAwardResult(
        game_id_from=_coerce_int(row.get("GameIDFrom")),
        game_id_to=_coerce_int(row.get("GameIDTo")),
        source_as_of_date=_coerce_date(row.get("SourceAsOfDate")) or DEFAULT_SOURCE_AS_OF_DATE,
        ledger_start_date=_coerce_date(row.get("LedgerStartDate")) or DEFAULT_LEDGER_START_DATE,
        valuation_snapshot_date=_coerce_date(row.get("ValuationSnapshotDate")) or DEFAULT_VALUATION_SNAPSHOT_DATE,
        dry_run=dry_run,
        run_id=_coerce_optional_int(row.get("RunID")),
        game_count=_coerce_int(row.get("GameCount")),
        participant_count=_coerce_int(row.get("ParticipantCount")),
        eligible_award_count=_coerce_int(row.get("EligibleAwardCount")),
        already_awarded_count=_coerce_int(row.get("AlreadyAwardedCount")),
        new_award_count=_coerce_int(row.get("NewAwardCount")),
        point_total=_coerce_int(row.get("PointTotal")),
        missing_member_snapshot_count=_coerce_int(row.get("MissingMemberSnapshotCount")),
        missing_chapter_snapshot_count=_coerce_int(row.get("MissingChapterSnapshotCount")),
        inactive_player_count=_coerce_int(row.get("InactivePlayerCount")),
        no_chapter_count=_coerce_int(row.get("NoChapterCount")),
        chapter_not_current_count=_coerce_int(row.get("ChapterNotCurrentCount")),
    )


def _coerce_int(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _coerce_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def print_award_result(result: LegacyRatedGameAwardResult, output: TextIO) -> None:
    label = "Legacy Rated Game Awards Preview" if result.dry_run else "Legacy Rated Game Awards"
    print(label, file=output)
    print(f"  Game IDs: {result.game_id_from} to {result.game_id_to}", file=output)
    print(f"  Source balance as of: {result.source_as_of_date.isoformat()}", file=output)
    print(f"  Ledger start: {result.ledger_start_date.isoformat()}", file=output)
    print(f"  Valuation snapshot: {result.valuation_snapshot_date.isoformat()}", file=output)
    if result.run_id is not None:
        print(f"  RunID: {result.run_id}", file=output)
    print(f"  Games: {result.game_count}", file=output)
    print(f"  Participant rows: {result.participant_count}", file=output)
    print(f"  Eligible awards: {result.eligible_award_count}", file=output)
    print(f"  Already awarded: {result.already_awarded_count}", file=output)
    print(f"  New awards: {result.new_award_count}", file=output)
    print(f"  New points: {result.point_total}", file=output)
    print("  Ineligible or blocked:", file=output)
    print(f"    missing member snapshot: {result.missing_member_snapshot_count}", file=output)
    print(f"    missing chapter snapshot: {result.missing_chapter_snapshot_count}", file=output)
    print(f"    inactive player: {result.inactive_player_count}", file=output)
    print(f"    no chapter: {result.no_chapter_count}", file=output)
    print(f"    chapter not current: {result.chapter_not_current_count}", file=output)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def main(argv: list[str] | None = None, output: TextIO = sys.stdout) -> int:
    parser = argparse.ArgumentParser(description="Award legacy-gap AGA Chapter Rewards points for rated games using a fixed valuation snapshot.")
    parser.add_argument("--game-id-from", type=_positive_int, required=True, help="First ratings.games Game_ID to consider.")
    parser.add_argument("--game-id-to", type=_positive_int, required=True, help="Last ratings.games Game_ID to consider.")
    parser.add_argument("--source-as-of-date", type=parse_snapshot_date, default=DEFAULT_SOURCE_AS_OF_DATE, help="Legacy source balance date. Defaults to 2026-02-08.")
    parser.add_argument("--ledger-start-date", type=parse_snapshot_date, default=DEFAULT_LEDGER_START_DATE, help="New ledger start date. Defaults to 2026-05-02.")
    parser.add_argument("--valuation-snapshot-date", type=parse_snapshot_date, default=DEFAULT_VALUATION_SNAPSHOT_DATE, help="Snapshot date used for member chapter and chapter multiplier. Defaults to 2026-05-02.")
    parser.add_argument("--dry-run", action="store_true", help="Preview awards without writing transactions or point lots.")
    parser.add_argument("--run-type", default="import", choices=["daily", "manual", "import", "backfill"], help="reward_runs.Run_Type value.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    args = parser.parse_args(argv)

    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    result = process_legacy_rated_game_awards(
        SqlAdapter(conn_str),
        args.game_id_from,
        args.game_id_to,
        source_as_of_date=args.source_as_of_date,
        ledger_start_date=args.ledger_start_date,
        valuation_snapshot_date=args.valuation_snapshot_date,
        run_type=args.run_type,
        dry_run=args.dry_run,
    )
    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True), file=output)
    else:
        print_award_result(result, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
