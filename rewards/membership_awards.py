import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Protocol, TextIO

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string
from rewards.snapshot_generator import parse_snapshot_date


SOURCE_TYPE = "membership_event"


class MembershipAwardSqlAdapter(Protocol):
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        ...

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        ...


@dataclass(frozen=True)
class MembershipAwardResult:
    as_of_date: date
    dry_run: bool
    run_id: int | None
    pending_event_count: int
    eligible_event_count: int
    already_awarded_count: int
    new_award_count: int
    point_total: int
    expiring_no_chapter_count: int
    waiting_for_chapter_count: int
    missing_snapshot_coverage_count: int
    ineligible_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "as_of_date": self.as_of_date.isoformat(),
            "dry_run": self.dry_run,
            "run_id": self.run_id,
            "pending_event_count": self.pending_event_count,
            "eligible_event_count": self.eligible_event_count,
            "already_awarded_count": self.already_awarded_count,
            "new_award_count": self.new_award_count,
            "point_total": self.point_total,
            "expiring_no_chapter_count": self.expiring_no_chapter_count,
            "waiting_for_chapter_count": self.waiting_for_chapter_count,
            "missing_snapshot_coverage_count": self.missing_snapshot_coverage_count,
            "ineligible_count": self.ineligible_count,
        }


PROCESS_MEMBERSHIP_AWARDS_SQL = """
EXEC [rewards].[sp_process_membership_awards]
    @AsOfDate = ?,
    @RunType = ?,
    @DryRun = ?
"""


MEMBERSHIP_AWARD_RESULT_SQL = """
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


def process_membership_awards(
    adapter: MembershipAwardSqlAdapter,
    as_of_date: date,
    *,
    run_type: str = "manual",
    dry_run: bool = False,
) -> MembershipAwardResult:
    params = (as_of_date, run_type, bool(dry_run))
    if dry_run:
        rows = adapter.query_rows(PROCESS_MEMBERSHIP_AWARDS_SQL, params)
        if not rows:
            raise RuntimeError("Membership award preview did not return a result row.")
        return _result_from_row(rows[0], dry_run=True)

    adapter.execute_statements([(PROCESS_MEMBERSHIP_AWARDS_SQL, params)])
    rows = adapter.query_rows(MEMBERSHIP_AWARD_RESULT_SQL, (run_type, as_of_date, SOURCE_TYPE))
    if not rows:
        raise RuntimeError("Membership award processing did not produce a reward_runs row.")
    summary = json.loads(rows[0].get("SummaryJson") or "{}")
    return MembershipAwardResult(
        as_of_date=_coerce_date(rows[0].get("Snapshot_Date")) or as_of_date,
        dry_run=False,
        run_id=_coerce_optional_int(rows[0].get("RunID")),
        pending_event_count=_coerce_int(summary.get("pending_event_count")),
        eligible_event_count=_coerce_int(summary.get("eligible_event_count")),
        already_awarded_count=_coerce_int(summary.get("already_awarded_count")),
        new_award_count=_coerce_int(summary.get("new_award_count")),
        point_total=_coerce_int(summary.get("point_total")),
        expiring_no_chapter_count=_coerce_int(summary.get("expired_no_chapter_count")),
        waiting_for_chapter_count=_coerce_int(summary.get("waiting_for_chapter_count")),
        missing_snapshot_coverage_count=_coerce_int(summary.get("missing_snapshot_coverage_count")),
        ineligible_count=_coerce_int(summary.get("ineligible_count")),
    )


def _result_from_row(row: dict[str, Any], *, dry_run: bool) -> MembershipAwardResult:
    return MembershipAwardResult(
        as_of_date=_coerce_date(row.get("AsOfDate")) or date.today(),
        dry_run=dry_run,
        run_id=_coerce_optional_int(row.get("RunID")),
        pending_event_count=_coerce_int(row.get("PendingEventCount")),
        eligible_event_count=_coerce_int(row.get("EligibleEventCount")),
        already_awarded_count=_coerce_int(row.get("AlreadyAwardedCount")),
        new_award_count=_coerce_int(row.get("NewAwardCount")),
        point_total=_coerce_int(row.get("PointTotal")),
        expiring_no_chapter_count=_coerce_int(row.get("ExpiringNoChapterCount")),
        waiting_for_chapter_count=_coerce_int(row.get("WaitingForChapterCount")),
        missing_snapshot_coverage_count=_coerce_int(row.get("MissingSnapshotCoverageCount")),
        ineligible_count=_coerce_int(row.get("IneligibleCount")),
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


def print_award_result(result: MembershipAwardResult, output: TextIO) -> None:
    label = "Membership Awards Preview" if result.dry_run else "Membership Awards"
    print(label, file=output)
    print(f"  As of: {result.as_of_date.isoformat()}", file=output)
    if result.run_id is not None:
        print(f"  RunID: {result.run_id}", file=output)
    print(f"  Pending events considered: {result.pending_event_count}", file=output)
    print(f"  Eligible events: {result.eligible_event_count}", file=output)
    print(f"  Already awarded: {result.already_awarded_count}", file=output)
    print(f"  New awards: {result.new_award_count}", file=output)
    print(f"  New points: {result.point_total}", file=output)
    print(f"  Expiring no chapter: {result.expiring_no_chapter_count}", file=output)
    print(f"  Waiting for chapter: {result.waiting_for_chapter_count}", file=output)
    print(f"  Missing snapshot coverage: {result.missing_snapshot_coverage_count}", file=output)
    print(f"  Ineligible: {result.ineligible_count}", file=output)


def main(argv: list[str] | None = None, output: TextIO = sys.stdout) -> int:
    parser = argparse.ArgumentParser(description="Award AGA Chapter Rewards points for eligible membership events.")
    parser.add_argument("--date", dest="as_of_date", type=parse_snapshot_date, help="Processing date in YYYY-MM-DD format. Defaults to today.")
    parser.add_argument("--dry-run", action="store_true", help="Preview membership awards without writing transactions or point lots.")
    parser.add_argument("--run-type", default="manual", choices=["daily", "manual", "import", "backfill"], help="reward_runs.Run_Type value.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    args = parser.parse_args(argv)

    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    result = process_membership_awards(
        SqlAdapter(conn_str),
        args.as_of_date or date.today(),
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
