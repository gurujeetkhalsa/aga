import argparse
import csv
import json
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Protocol, TextIO

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string
from rewards.snapshot_generator import parse_snapshot_date


SOURCE_TYPE = "legacy_membership_gap_award"
RULE_VERSION = "2026-05-02"
DEFAULT_SOURCE_AS_OF_DATE = date(2026, 2, 8)
DEFAULT_LEDGER_START_DATE = date(2026, 5, 2)
DEFAULT_VALUATION_SNAPSHOT_DATE = date(2026, 5, 2)


class LegacyMembershipAwardSqlAdapter(Protocol):
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        ...

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        ...


@dataclass(frozen=True)
class LegacyMembershipAwardRow:
    source_row_number: int
    chapter_id: int
    chapter_code: str
    chapter_name: str
    member_type: str
    event_type: str
    count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_row_number": self.source_row_number,
            "chapter_id": self.chapter_id,
            "chapter_code": self.chapter_code,
            "chapter_name": self.chapter_name,
            "member_type": self.member_type,
            "event_type": self.event_type,
            "count": self.count,
        }


@dataclass(frozen=True)
class LegacyMembershipAwardResult:
    source_as_of_date: date
    ledger_start_date: date
    valuation_snapshot_date: date
    dry_run: bool
    run_id: int | None
    input_row_count: int
    chapter_count: int
    member_count_total: int
    eligible_award_count: int
    already_awarded_count: int
    new_award_count: int
    point_total: int
    missing_chapter_snapshot_count: int
    chapter_not_current_count: int
    unsupported_member_type_count: int
    ineligible_member_type_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_as_of_date": self.source_as_of_date.isoformat(),
            "ledger_start_date": self.ledger_start_date.isoformat(),
            "valuation_snapshot_date": self.valuation_snapshot_date.isoformat(),
            "dry_run": self.dry_run,
            "run_id": self.run_id,
            "input_row_count": self.input_row_count,
            "chapter_count": self.chapter_count,
            "member_count_total": self.member_count_total,
            "eligible_award_count": self.eligible_award_count,
            "already_awarded_count": self.already_awarded_count,
            "new_award_count": self.new_award_count,
            "point_total": self.point_total,
            "missing_chapter_snapshot_count": self.missing_chapter_snapshot_count,
            "chapter_not_current_count": self.chapter_not_current_count,
            "unsupported_member_type_count": self.unsupported_member_type_count,
            "ineligible_member_type_count": self.ineligible_member_type_count,
        }


IMPORT_LEGACY_MEMBERSHIP_AWARDS_SQL = """
EXEC [rewards].[sp_import_legacy_gap_membership_awards]
    @MembershipAwardsJson = ?,
    @DryRun = ?,
    @RunType = ?,
    @SourceAsOfDate = ?,
    @LedgerStartDate = ?,
    @ValuationSnapshotDate = ?,
    @SourceType = ?,
    @RuleVersion = ?
"""


LEGACY_MEMBERSHIP_AWARD_RESULT_SQL = """
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


def import_legacy_membership_awards(
    adapter: LegacyMembershipAwardSqlAdapter,
    rows: Iterable[LegacyMembershipAwardRow],
    *,
    source_as_of_date: date = DEFAULT_SOURCE_AS_OF_DATE,
    ledger_start_date: date = DEFAULT_LEDGER_START_DATE,
    valuation_snapshot_date: date = DEFAULT_VALUATION_SNAPSHOT_DATE,
    run_type: str = "import",
    dry_run: bool = False,
    source_type: str = SOURCE_TYPE,
    rule_version: str = RULE_VERSION,
) -> LegacyMembershipAwardResult:
    row_list = list(rows)
    if not row_list:
        raise ValueError("At least one legacy membership award row is required.")

    payload = json.dumps([row.as_dict() for row in row_list], sort_keys=True)
    params = (
        payload,
        bool(dry_run),
        run_type,
        source_as_of_date,
        ledger_start_date,
        valuation_snapshot_date,
        source_type,
        rule_version,
    )
    if dry_run:
        preview_rows = adapter.query_rows(IMPORT_LEGACY_MEMBERSHIP_AWARDS_SQL, params)
        if not preview_rows:
            raise RuntimeError("Legacy membership award preview did not return a result row.")
        return _result_from_row(preview_rows[0], dry_run=True)

    adapter.execute_statements([(IMPORT_LEGACY_MEMBERSHIP_AWARDS_SQL, params)])
    result_rows = adapter.query_rows(LEGACY_MEMBERSHIP_AWARD_RESULT_SQL, (run_type, ledger_start_date, source_type))
    if not result_rows:
        raise RuntimeError("Legacy membership award import did not produce a reward_runs row.")
    summary = json.loads(result_rows[0].get("SummaryJson") or "{}")
    return LegacyMembershipAwardResult(
        source_as_of_date=_coerce_date(summary.get("source_balance_as_of_date")) or source_as_of_date,
        ledger_start_date=_coerce_date(result_rows[0].get("Snapshot_Date")) or ledger_start_date,
        valuation_snapshot_date=_coerce_date(summary.get("valuation_snapshot_date")) or valuation_snapshot_date,
        dry_run=False,
        run_id=_coerce_optional_int(result_rows[0].get("RunID")),
        input_row_count=_coerce_int(summary.get("input_row_count")),
        chapter_count=_coerce_int(summary.get("chapter_count")),
        member_count_total=_coerce_int(summary.get("member_count_total")),
        eligible_award_count=_coerce_int(summary.get("eligible_award_count")),
        already_awarded_count=_coerce_int(summary.get("already_awarded_count")),
        new_award_count=_coerce_int(summary.get("new_award_count")),
        point_total=_coerce_int(summary.get("point_total")),
        missing_chapter_snapshot_count=_coerce_int(summary.get("missing_chapter_snapshot_count")),
        chapter_not_current_count=_coerce_int(summary.get("chapter_not_current_count")),
        unsupported_member_type_count=_coerce_int(summary.get("unsupported_member_type_count")),
        ineligible_member_type_count=_coerce_int(summary.get("ineligible_member_type_count")),
    )


def read_legacy_membership_awards_csv(path: Path) -> list[LegacyMembershipAwardRow]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("Legacy membership award CSV is missing a header row.")
        missing_headers = sorted(_REQUIRED_HEADERS - set(reader.fieldnames))
        if missing_headers:
            raise ValueError(f"Legacy membership award CSV is missing required columns: {', '.join(missing_headers)}")
        return [
            legacy_membership_award_from_record(record, index + 1)
            for index, record in enumerate(reader)
        ]


def legacy_membership_award_from_record(record: dict[str, Any], source_row_number: int) -> LegacyMembershipAwardRow:
    chapter_id = _parse_positive_int(_required(record, "ChapterID"), "ChapterID")
    count = _parse_positive_int(_required(record, "Count"), "Count")
    return LegacyMembershipAwardRow(
        source_row_number=source_row_number,
        chapter_id=chapter_id,
        chapter_code=_required(record, "ChapterCode").strip().upper(),
        chapter_name=_required(record, "ChapterName").strip(),
        member_type=_required(record, "MemberType").strip(),
        event_type=_normalize_event_type(_required(record, "EventType")),
        count=count,
    )


_REQUIRED_HEADERS = {"ChapterID", "ChapterCode", "ChapterName", "MemberType", "EventType", "Count"}


def _normalize_event_type(value: Any) -> str:
    text = str(value).strip().lower().replace(" ", "_")
    if text in {"new", "new_membership"}:
        return "new_membership"
    if text in {"renew", "renewal"}:
        return "renewal"
    raise ValueError(f"Unsupported membership event type: {value}")


def _required(record: dict[str, Any], key: str) -> str:
    value = record.get(key)
    if value is None or str(value).strip() == "":
        raise ValueError(f"Missing required membership award field: {key}")
    return str(value)


def _parse_positive_int(value: Any, field_name: str) -> int:
    try:
        parsed = int(str(value).strip())
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer.") from exc
    if parsed < 1:
        raise ValueError(f"{field_name} must be positive.")
    return parsed


def _result_from_row(row: dict[str, Any], *, dry_run: bool) -> LegacyMembershipAwardResult:
    return LegacyMembershipAwardResult(
        source_as_of_date=_coerce_date(row.get("SourceAsOfDate")) or DEFAULT_SOURCE_AS_OF_DATE,
        ledger_start_date=_coerce_date(row.get("LedgerStartDate")) or DEFAULT_LEDGER_START_DATE,
        valuation_snapshot_date=_coerce_date(row.get("ValuationSnapshotDate")) or DEFAULT_VALUATION_SNAPSHOT_DATE,
        dry_run=dry_run,
        run_id=_coerce_optional_int(row.get("RunID")),
        input_row_count=_coerce_int(row.get("InputRowCount")),
        chapter_count=_coerce_int(row.get("ChapterCount")),
        member_count_total=_coerce_int(row.get("MemberCountTotal")),
        eligible_award_count=_coerce_int(row.get("EligibleAwardCount")),
        already_awarded_count=_coerce_int(row.get("AlreadyAwardedCount")),
        new_award_count=_coerce_int(row.get("NewAwardCount")),
        point_total=_coerce_int(row.get("PointTotal")),
        missing_chapter_snapshot_count=_coerce_int(row.get("MissingChapterSnapshotCount")),
        chapter_not_current_count=_coerce_int(row.get("ChapterNotCurrentCount")),
        unsupported_member_type_count=_coerce_int(row.get("UnsupportedMemberTypeCount")),
        ineligible_member_type_count=_coerce_int(row.get("IneligibleMemberTypeCount")),
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


def print_award_result(result: LegacyMembershipAwardResult, output: TextIO) -> None:
    label = "Legacy Membership Awards Preview" if result.dry_run else "Legacy Membership Awards"
    print(label, file=output)
    print(f"  Source balance as of: {result.source_as_of_date.isoformat()}", file=output)
    print(f"  Ledger start: {result.ledger_start_date.isoformat()}", file=output)
    print(f"  Valuation snapshot: {result.valuation_snapshot_date.isoformat()}", file=output)
    if result.run_id is not None:
        print(f"  RunID: {result.run_id}", file=output)
    print(f"  Input rows: {result.input_row_count}", file=output)
    print(f"  Chapters: {result.chapter_count}", file=output)
    print(f"  Member count total: {result.member_count_total}", file=output)
    print(f"  Eligible aggregate awards: {result.eligible_award_count}", file=output)
    print(f"  Already awarded: {result.already_awarded_count}", file=output)
    print(f"  New aggregate awards: {result.new_award_count}", file=output)
    print(f"  New points: {result.point_total}", file=output)
    print("  Ineligible or blocked:", file=output)
    print(f"    missing chapter snapshot: {result.missing_chapter_snapshot_count}", file=output)
    print(f"    chapter not current: {result.chapter_not_current_count}", file=output)
    print(f"    unsupported member type: {result.unsupported_member_type_count}", file=output)
    print(f"    ineligible member type: {result.ineligible_member_type_count}", file=output)


def main(argv: list[str] | None = None, output: TextIO = sys.stdout) -> int:
    parser = argparse.ArgumentParser(description="Import legacy-gap aggregate AGA Chapter Rewards membership awards from a CSV.")
    parser.add_argument("csv_path", type=Path, help="Path to the new/renew membership aggregate CSV.")
    parser.add_argument("--source-as-of-date", type=parse_snapshot_date, default=DEFAULT_SOURCE_AS_OF_DATE, help="Legacy source balance date. Defaults to 2026-02-08.")
    parser.add_argument("--ledger-start-date", type=parse_snapshot_date, default=DEFAULT_LEDGER_START_DATE, help="New ledger start date. Defaults to 2026-05-02.")
    parser.add_argument("--valuation-snapshot-date", type=parse_snapshot_date, default=DEFAULT_VALUATION_SNAPSHOT_DATE, help="Snapshot date used for chapter multiplier. Defaults to 2026-05-02.")
    parser.add_argument("--dry-run", action="store_true", help="Preview awards without writing transactions or point lots.")
    parser.add_argument("--run-type", default="import", choices=["daily", "manual", "import", "backfill"], help="reward_runs.Run_Type value.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    args = parser.parse_args(argv)

    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    rows = read_legacy_membership_awards_csv(args.csv_path)
    result = import_legacy_membership_awards(
        SqlAdapter(conn_str),
        rows,
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
