import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Protocol, TextIO

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string
from rewards.snapshot_generator import parse_snapshot_date


PROCESSOR_NAME = "tournament_awards"
HOST_SOURCE_TYPE = "tournament_host"
STATE_SOURCE_TYPE = "state_championship"
RULE_VERSION = "2026-05-03"
MIN_GAMES = 15
MAX_GAMES = 700
MAX_SUPPORT = 1000
EXPONENT = 0.93
STATE_CHAMPIONSHIP_POINTS = 200000


class TournamentAwardSqlAdapter(Protocol):
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        ...

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        ...


@dataclass(frozen=True)
class TournamentAwardResult:
    date_from: date | None
    date_to: date
    dry_run: bool
    run_id: int | None
    event_group_count: int
    tournament_section_count: int
    rated_game_count: int
    host_eligible_award_count: int
    host_already_awarded_count: int
    host_new_award_count: int
    host_point_total: int
    state_championship_group_count: int
    state_already_awarded_count: int
    state_new_award_count: int
    state_championship_point_total: int
    new_award_count: int
    point_total: int
    missing_host_chapter_count: int
    missing_reward_event_key_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "date_from": self.date_from.isoformat() if self.date_from else None,
            "date_to": self.date_to.isoformat(),
            "dry_run": self.dry_run,
            "run_id": self.run_id,
            "event_group_count": self.event_group_count,
            "tournament_section_count": self.tournament_section_count,
            "rated_game_count": self.rated_game_count,
            "host_eligible_award_count": self.host_eligible_award_count,
            "host_already_awarded_count": self.host_already_awarded_count,
            "host_new_award_count": self.host_new_award_count,
            "host_point_total": self.host_point_total,
            "state_championship_group_count": self.state_championship_group_count,
            "state_already_awarded_count": self.state_already_awarded_count,
            "state_new_award_count": self.state_new_award_count,
            "state_championship_point_total": self.state_championship_point_total,
            "new_award_count": self.new_award_count,
            "point_total": self.point_total,
            "missing_host_chapter_count": self.missing_host_chapter_count,
            "missing_reward_event_key_count": self.missing_reward_event_key_count,
        }


PROCESS_TOURNAMENT_AWARDS_SQL = """
EXEC [rewards].[sp_process_tournament_awards]
    @TournamentDateFrom = ?,
    @TournamentDateTo = ?,
    @RunType = ?,
    @DryRun = ?,
    @MinGames = ?,
    @MaxGames = ?,
    @MaxSupport = ?,
    @Exponent = ?,
    @StateChampionshipPoints = ?,
    @HostSourceType = ?,
    @StateSourceType = ?,
    @RuleVersion = ?
"""


TOURNAMENT_AWARD_RESULT_SQL = """
SELECT TOP 1
    [RunID],
    [Snapshot_Date],
    [SummaryJson]
FROM [rewards].[reward_runs]
WHERE [Run_Type] = ?
  AND [Snapshot_Date] = ?
  AND JSON_VALUE([SummaryJson], '$.processor') = ?
ORDER BY [RunID] DESC
"""


def calculate_tournament_host_points(
    games: int,
    *,
    min_games: int = MIN_GAMES,
    max_games: int = MAX_GAMES,
    max_support: int = MAX_SUPPORT,
    exponent: float = EXPONENT,
) -> int:
    _validate_formula_parameters(
        min_games=min_games,
        max_games=max_games,
        max_support=max_support,
        exponent=exponent,
        state_championship_points=STATE_CHAMPIONSHIP_POINTS,
    )
    if games <= min_games:
        return 0
    if games >= max_games:
        return max_support * 1000
    scaled = ((games - min_games) / (max_games - min_games)) ** exponent
    return math.floor(max_support * scaled * 1000 + 0.5)


def process_tournament_awards(
    adapter: TournamentAwardSqlAdapter,
    date_from: date | None = None,
    date_to: date | None = None,
    *,
    run_type: str = "manual",
    dry_run: bool = False,
    min_games: int = MIN_GAMES,
    max_games: int = MAX_GAMES,
    max_support: int = MAX_SUPPORT,
    exponent: float = EXPONENT,
    state_championship_points: int = STATE_CHAMPIONSHIP_POINTS,
    host_source_type: str = HOST_SOURCE_TYPE,
    state_source_type: str = STATE_SOURCE_TYPE,
    rule_version: str = RULE_VERSION,
) -> TournamentAwardResult:
    date_to = date_to or date.today()
    if date_from and date_to < date_from:
        raise ValueError("date_to must be on or after date_from.")
    _validate_formula_parameters(
        min_games=min_games,
        max_games=max_games,
        max_support=max_support,
        exponent=exponent,
        state_championship_points=state_championship_points,
    )

    params = (
        date_from,
        date_to,
        run_type,
        bool(dry_run),
        min_games,
        max_games,
        max_support,
        float(exponent),
        state_championship_points,
        host_source_type,
        state_source_type,
        rule_version,
    )
    if dry_run:
        rows = adapter.query_rows(PROCESS_TOURNAMENT_AWARDS_SQL, params)
        if not rows:
            raise RuntimeError("Tournament award preview did not return a result row.")
        return _result_from_row(rows[0], dry_run=True)

    adapter.execute_statements([(PROCESS_TOURNAMENT_AWARDS_SQL, params)])
    rows = adapter.query_rows(TOURNAMENT_AWARD_RESULT_SQL, (run_type, date_to, PROCESSOR_NAME))
    if not rows:
        raise RuntimeError("Tournament award processing did not produce a reward_runs row.")
    summary = json.loads(rows[0].get("SummaryJson") or "{}")
    return TournamentAwardResult(
        date_from=date_from,
        date_to=_coerce_date(rows[0].get("Snapshot_Date")) or date_to,
        dry_run=False,
        run_id=_coerce_optional_int(rows[0].get("RunID")),
        event_group_count=_coerce_int(summary.get("event_group_count")),
        tournament_section_count=_coerce_int(summary.get("tournament_section_count")),
        rated_game_count=_coerce_int(summary.get("rated_game_count")),
        host_eligible_award_count=_coerce_int(summary.get("host_eligible_award_count")),
        host_already_awarded_count=_coerce_int(summary.get("host_already_awarded_count")),
        host_new_award_count=_coerce_int(summary.get("host_new_award_count")),
        host_point_total=_coerce_int(summary.get("host_point_total")),
        state_championship_group_count=_coerce_int(summary.get("state_championship_group_count")),
        state_already_awarded_count=_coerce_int(summary.get("state_already_awarded_count")),
        state_new_award_count=_coerce_int(summary.get("state_new_award_count")),
        state_championship_point_total=_coerce_int(summary.get("state_championship_point_total")),
        new_award_count=_coerce_int(summary.get("new_award_count")),
        point_total=_coerce_int(summary.get("point_total")),
        missing_host_chapter_count=_coerce_int(summary.get("missing_host_chapter_count")),
        missing_reward_event_key_count=_coerce_int(summary.get("missing_reward_event_key_count")),
    )


def _validate_formula_parameters(
    *,
    min_games: int,
    max_games: int,
    max_support: int,
    exponent: float,
    state_championship_points: int,
) -> None:
    if min_games < 0:
        raise ValueError("min_games must be nonnegative.")
    if max_games <= min_games:
        raise ValueError("max_games must be greater than min_games.")
    if max_support < 0:
        raise ValueError("max_support must be nonnegative.")
    if exponent <= 0:
        raise ValueError("exponent must be greater than zero.")
    if state_championship_points < 0:
        raise ValueError("state_championship_points must be nonnegative.")


def _result_from_row(row: dict[str, Any], *, dry_run: bool) -> TournamentAwardResult:
    return TournamentAwardResult(
        date_from=_coerce_date(row.get("TournamentDateFrom")),
        date_to=_coerce_date(row.get("TournamentDateTo")) or date.today(),
        dry_run=dry_run,
        run_id=_coerce_optional_int(row.get("RunID")),
        event_group_count=_coerce_int(row.get("EventGroupCount")),
        tournament_section_count=_coerce_int(row.get("TournamentSectionCount")),
        rated_game_count=_coerce_int(row.get("RatedGameCount")),
        host_eligible_award_count=_coerce_int(row.get("HostEligibleAwardCount")),
        host_already_awarded_count=_coerce_int(row.get("HostAlreadyAwardedCount")),
        host_new_award_count=_coerce_int(row.get("HostNewAwardCount")),
        host_point_total=_coerce_int(row.get("HostPointTotal")),
        state_championship_group_count=_coerce_int(row.get("StateChampionshipGroupCount")),
        state_already_awarded_count=_coerce_int(row.get("StateAlreadyAwardedCount")),
        state_new_award_count=_coerce_int(row.get("StateNewAwardCount")),
        state_championship_point_total=_coerce_int(row.get("StateChampionshipPointTotal")),
        new_award_count=_coerce_int(row.get("NewAwardCount")),
        point_total=_coerce_int(row.get("PointTotal")),
        missing_host_chapter_count=_coerce_int(row.get("MissingHostChapterCount")),
        missing_reward_event_key_count=_coerce_int(row.get("MissingRewardEventKeyCount")),
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


def print_award_result(result: TournamentAwardResult, output: TextIO) -> None:
    label = "Tournament Awards Preview" if result.dry_run else "Tournament Awards"
    print(label, file=output)
    start = result.date_from.isoformat() if result.date_from else "all"
    print(f"  Event dates: {start} to {result.date_to.isoformat()}", file=output)
    if result.run_id is not None:
        print(f"  RunID: {result.run_id}", file=output)
    print(f"  Event groups: {result.event_group_count}", file=output)
    print(f"  Tournament sections: {result.tournament_section_count}", file=output)
    print(f"  Rated games: {result.rated_game_count}", file=output)
    print(f"  Host awards: {result.host_new_award_count} new / {result.host_already_awarded_count} already", file=output)
    print(f"  Host points: {result.host_point_total}", file=output)
    print(f"  State Championship awards: {result.state_new_award_count} new / {result.state_already_awarded_count} already", file=output)
    print(f"  State Championship points: {result.state_championship_point_total}", file=output)
    print(f"  New awards: {result.new_award_count}", file=output)
    print(f"  New points: {result.point_total}", file=output)
    print(f"  Missing host chapter sections: {result.missing_host_chapter_count}", file=output)
    print(f"  Missing reward event key sections: {result.missing_reward_event_key_count}", file=output)


def main(argv: list[str] | None = None, output: TextIO = sys.stdout) -> int:
    parser = argparse.ArgumentParser(description="Award AGA Chapter Rewards points for hosted tournaments.")
    parser.add_argument("--date", type=parse_snapshot_date, help="Single event date in YYYY-MM-DD format.")
    parser.add_argument("--date-from", type=parse_snapshot_date, help="Start event date in YYYY-MM-DD format.")
    parser.add_argument("--date-to", type=parse_snapshot_date, help="End event date in YYYY-MM-DD format. Defaults to today.")
    parser.add_argument("--dry-run", action="store_true", help="Preview awards without writing transactions or point lots.")
    parser.add_argument("--run-type", default="manual", choices=["daily", "manual", "import", "backfill"], help="reward_runs.Run_Type value.")
    parser.add_argument("--min-games", type=int, default=MIN_GAMES, help="Rated-game threshold below which no host points are awarded.")
    parser.add_argument("--max-games", type=int, default=MAX_GAMES, help="Rated-game count that earns maximum host support.")
    parser.add_argument("--max-support", type=int, default=MAX_SUPPORT, help="Maximum host support before multiplying by 1000.")
    parser.add_argument("--exponent", type=float, default=EXPONENT, help="Host support curve exponent.")
    parser.add_argument("--state-championship-points", type=int, default=STATE_CHAMPIONSHIP_POINTS, help="Fixed State Championship award.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    args = parser.parse_args(argv)

    if args.date and (args.date_from or args.date_to):
        parser.error("Use either --date or --date-from/--date-to, not both.")
    if args.date:
        date_from = args.date
        date_to = args.date
    else:
        date_from = args.date_from
        date_to = args.date_to or date.today()

    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    result = process_tournament_awards(
        SqlAdapter(conn_str),
        date_from,
        date_to,
        run_type=args.run_type,
        dry_run=args.dry_run,
        min_games=args.min_games,
        max_games=args.max_games,
        max_support=args.max_support,
        exponent=args.exponent,
        state_championship_points=args.state_championship_points,
    )
    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True), file=output)
    else:
        print_award_result(result, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
