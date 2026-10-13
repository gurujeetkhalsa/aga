import argparse
import csv
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Protocol, TextIO

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string
from rewards.snapshot_generator import parse_snapshot_date
from rewards.tournament_awards import (
    EXPONENT,
    HOST_SOURCE_TYPE,
    MAX_GAMES,
    MAX_SUPPORT,
    MIN_GAMES,
    RULE_VERSION,
    STATE_CHAMPIONSHIP_POINTS,
    STATE_SOURCE_TYPE,
)


PROCESSOR_NAME = "legacy_tournament_gap_award"
DEFAULT_SOURCE_AS_OF_DATE = date(2026, 2, 8)
DEFAULT_LEDGER_START_DATE = date(2026, 5, 2)


class LegacyTournamentAwardSqlAdapter(Protocol):
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        ...

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        ...


@dataclass(frozen=True)
class LegacyTournamentAwardRow:
    source_row_number: int
    chapter_id: int
    chapter_code: str
    chapter_name: str
    tournament_date: date
    tournament_code: str
    rated_game_count: int
    is_state_championship: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_row_number": self.source_row_number,
            "chapter_id": self.chapter_id,
            "chapter_code": self.chapter_code,
            "chapter_name": self.chapter_name,
            "tournament_date": self.tournament_date.isoformat(),
            "tournament_code": self.tournament_code,
            "rated_game_count": self.rated_game_count,
            "is_state_championship": self.is_state_championship,
        }


@dataclass(frozen=True)
class LegacyTournamentAwardResult:
    source_as_of_date: date
    ledger_start_date: date
    dry_run: bool
    run_id: int | None
    input_row_count: int
    chapter_count: int
    tournament_count: int
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

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_as_of_date": self.source_as_of_date.isoformat(),
            "ledger_start_date": self.ledger_start_date.isoformat(),
            "dry_run": self.dry_run,
            "run_id": self.run_id,
            "input_row_count": self.input_row_count,
            "chapter_count": self.chapter_count,
            "tournament_count": self.tournament_count,
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
        }


IMPORT_LEGACY_TOURNAMENT_AWARDS_SQL = """
EXEC [rewards].[sp_import_legacy_gap_tournament_awards]
    @TournamentAwardsJson = ?,
    @DryRun = ?,
    @RunType = ?,
    @SourceAsOfDate = ?,
    @LedgerStartDate = ?,
    @MinGames = ?,
    @MaxGames = ?,
    @MaxSupport = ?,
    @Exponent = ?,
    @StateChampionshipPoints = ?,
    @HostSourceType = ?,
    @StateSourceType = ?,
    @RuleVersion = ?
"""


LEGACY_TOURNAMENT_AWARD_RESULT_SQL = """
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


def import_legacy_tournament_awards(
    adapter: LegacyTournamentAwardSqlAdapter,
    rows: Iterable[LegacyTournamentAwardRow],
    *,
    source_as_of_date: date = DEFAULT_SOURCE_AS_OF_DATE,
    ledger_start_date: date = DEFAULT_LEDGER_START_DATE,
    run_type: str = "import",
    dry_run: bool = False,
    min_games: int = MIN_GAMES,
    max_games: int = MAX_GAMES,
    max_support: int = MAX_SUPPORT,
    exponent: float = EXPONENT,
    state_championship_points: int = STATE_CHAMPIONSHIP_POINTS,
    host_source_type: str = HOST_SOURCE_TYPE,
    state_source_type: str = STATE_SOURCE_TYPE,
    rule_version: str = RULE_VERSION,
) -> LegacyTournamentAwardResult:
    row_list = list(rows)
    if not row_list:
        raise ValueError("At least one legacy tournament award row is required.")

    payload = json.dumps([row.as_dict() for row in row_list], sort_keys=True)
    params = (
        payload,
        bool(dry_run),
        run_type,
        source_as_of_date,
        ledger_start_date,
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
        preview_rows = adapter.query_rows(IMPORT_LEGACY_TOURNAMENT_AWARDS_SQL, params)
        if not preview_rows:
            raise RuntimeError("Legacy tournament award preview did not return a result row.")
        return _result_from_row(preview_rows[0], dry_run=True)

    adapter.execute_statements([(IMPORT_LEGACY_TOURNAMENT_AWARDS_SQL, params)])
    result_rows = adapter.query_rows(LEGACY_TOURNAMENT_AWARD_RESULT_SQL, (run_type, ledger_start_date, PROCESSOR_NAME))
    if not result_rows:
        raise RuntimeError("Legacy tournament award import did not produce a reward_runs row.")
    summary = json.loads(result_rows[0].get("SummaryJson") or "{}")
    return LegacyTournamentAwardResult(
        source_as_of_date=_coerce_date(summary.get("source_balance_as_of_date")) or source_as_of_date,
        ledger_start_date=_coerce_date(result_rows[0].get("Snapshot_Date")) or ledger_start_date,
        dry_run=False,
        run_id=_coerce_optional_int(result_rows[0].get("RunID")),
        input_row_count=_coerce_int(summary.get("input_row_count")),
        chapter_count=_coerce_int(summary.get("chapter_count")),
        tournament_count=_coerce_int(summary.get("tournament_count")),
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
    )


def read_legacy_tournament_awards_csv(path: Path) -> list[LegacyTournamentAwardRow]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("Legacy tournament award CSV is missing a header row.")
        headers = _resolve_headers(reader.fieldnames)
        return [
            legacy_tournament_award_from_record(record, index + 1, headers)
            for index, record in enumerate(reader)
        ]


def legacy_tournament_award_from_record(
    record: dict[str, Any],
    source_row_number: int,
    headers: dict[str, str] | None = None,
) -> LegacyTournamentAwardRow:
    headers = headers or _resolve_headers(record.keys())
    return LegacyTournamentAwardRow(
        source_row_number=source_row_number,
        chapter_id=_parse_positive_int(_field(record, headers, "chapter_id"), "ChapterID"),
        chapter_code=_field(record, headers, "chapter_code").strip().upper(),
        chapter_name=_field(record, headers, "chapter_name").strip(),
        tournament_date=_parse_tournament_date(_field(record, headers, "tournament_date")),
        tournament_code=_field(record, headers, "tournament_code").strip(),
        rated_game_count=_parse_nonnegative_int(_field(record, headers, "rated_game_count"), "RatedGameCount"),
        is_state_championship=_parse_yes_no(_field(record, headers, "is_state_championship")),
    )


_HEADER_ALIASES = {
    "chapter_id": ("ChapterID", "Chapter_ID"),
    "chapter_code": ("ChapterCode", "Chapter_Code"),
    "chapter_name": ("ChapterName", "Chapter_Name"),
    "tournament_date": ("Tournament_Date", "TournamentDate", "Date"),
    "tournament_code": ("Tournament_Code", "TournamentCode", "TournamentID", "Tournament_ID"),
    "rated_game_count": ("RatedGameCount", "Rated_Game_Count", "Rated Games", "Game_Count", "Game_ID"),
    "is_state_championship": ("State Championship", "State_Championship", "StateChampionship", "Is_State_Championship"),
}


def _resolve_headers(fieldnames: Iterable[str]) -> dict[str, str]:
    available = {_normalize_header(name): name for name in fieldnames}
    resolved = {}
    missing = []
    for canonical, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            header = available.get(_normalize_header(alias))
            if header is not None:
                resolved[canonical] = header
                break
        else:
            missing.append(aliases[0])
    if missing:
        raise ValueError(f"Legacy tournament award CSV is missing required columns: {', '.join(missing)}")
    return resolved


def _normalize_header(value: str) -> str:
    return "".join(character for character in value.lower() if character.isalnum())


def _field(record: dict[str, Any], headers: dict[str, str], canonical: str) -> str:
    value = record.get(headers[canonical])
    if value is None or str(value).strip() == "":
        raise ValueError(f"Missing required tournament award field: {headers[canonical]}")
    return str(value)


def _parse_tournament_date(value: Any) -> date:
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported tournament date: {value}")


def _parse_yes_no(value: Any) -> bool:
    text = str(value).strip().upper()
    if text in {"Y", "YES", "TRUE", "1"}:
        return True
    if text in {"N", "NO", "FALSE", "0"}:
        return False
    raise ValueError(f"State Championship must be Y or N: {value}")


def _parse_positive_int(value: Any, field_name: str) -> int:
    parsed = _parse_nonnegative_int(value, field_name)
    if parsed < 1:
        raise ValueError(f"{field_name} must be positive.")
    return parsed


def _parse_nonnegative_int(value: Any, field_name: str) -> int:
    try:
        parsed = int(str(value).strip())
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer.") from exc
    if parsed < 0:
        raise ValueError(f"{field_name} must be nonnegative.")
    return parsed


def _result_from_row(row: dict[str, Any], *, dry_run: bool) -> LegacyTournamentAwardResult:
    return LegacyTournamentAwardResult(
        source_as_of_date=_coerce_date(row.get("SourceAsOfDate")) or DEFAULT_SOURCE_AS_OF_DATE,
        ledger_start_date=_coerce_date(row.get("LedgerStartDate")) or DEFAULT_LEDGER_START_DATE,
        dry_run=dry_run,
        run_id=_coerce_optional_int(row.get("RunID")),
        input_row_count=_coerce_int(row.get("InputRowCount")),
        chapter_count=_coerce_int(row.get("ChapterCount")),
        tournament_count=_coerce_int(row.get("TournamentCount")),
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


def print_award_result(result: LegacyTournamentAwardResult, output: TextIO) -> None:
    label = "Legacy Tournament Awards Preview" if result.dry_run else "Legacy Tournament Awards"
    print(label, file=output)
    print(f"  Source balance as of: {result.source_as_of_date.isoformat()}", file=output)
    print(f"  Ledger start: {result.ledger_start_date.isoformat()}", file=output)
    if result.run_id is not None:
        print(f"  RunID: {result.run_id}", file=output)
    print(f"  Input rows: {result.input_row_count}", file=output)
    print(f"  Chapters: {result.chapter_count}", file=output)
    print(f"  Tournaments: {result.tournament_count}", file=output)
    print(f"  Rated games: {result.rated_game_count}", file=output)
    print(f"  Host awards: {result.host_new_award_count} new / {result.host_already_awarded_count} already", file=output)
    print(f"  Host points: {result.host_point_total}", file=output)
    print(f"  State Championship awards: {result.state_new_award_count} new / {result.state_already_awarded_count} already", file=output)
    print(f"  State Championship points: {result.state_championship_point_total}", file=output)
    print(f"  New awards: {result.new_award_count}", file=output)
    print(f"  New points: {result.point_total}", file=output)


def main(argv: list[str] | None = None, output: TextIO = sys.stdout) -> int:
    parser = argparse.ArgumentParser(description="Import legacy-gap AGA Chapter Rewards tournament awards from a CSV.")
    parser.add_argument("csv_path", type=Path, help="Path to the tournament award aggregate CSV.")
    parser.add_argument("--source-as-of-date", type=parse_snapshot_date, default=DEFAULT_SOURCE_AS_OF_DATE, help="Legacy source balance date. Defaults to 2026-02-08.")
    parser.add_argument("--ledger-start-date", type=parse_snapshot_date, default=DEFAULT_LEDGER_START_DATE, help="New ledger start date. Defaults to 2026-05-02.")
    parser.add_argument("--dry-run", action="store_true", help="Preview awards without writing transactions or point lots.")
    parser.add_argument("--run-type", default="import", choices=["daily", "manual", "import", "backfill"], help="reward_runs.Run_Type value.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    args = parser.parse_args(argv)

    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    rows = read_legacy_tournament_awards_csv(args.csv_path)
    result = import_legacy_tournament_awards(
        SqlAdapter(conn_str),
        rows,
        source_as_of_date=args.source_as_of_date,
        ledger_start_date=args.ledger_start_date,
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
