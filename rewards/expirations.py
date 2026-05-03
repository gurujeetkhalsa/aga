import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Protocol, TextIO

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string
from rewards.snapshot_generator import parse_snapshot_date


SOURCE_TYPE = "point_expiration"


class PointExpirationSqlAdapter(Protocol):
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        ...

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        ...


@dataclass(frozen=True)
class PointExpirationResult:
    as_of_date: date
    dry_run: bool
    run_id: int | None
    expiring_lot_count: int
    already_expired_count: int
    new_expiration_count: int
    expired_point_total: int
    chapter_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "as_of_date": self.as_of_date.isoformat(),
            "dry_run": self.dry_run,
            "run_id": self.run_id,
            "expiring_lot_count": self.expiring_lot_count,
            "already_expired_count": self.already_expired_count,
            "new_expiration_count": self.new_expiration_count,
            "expired_point_total": self.expired_point_total,
            "chapter_count": self.chapter_count,
        }


PROCESS_POINT_EXPIRATIONS_SQL = """
EXEC [rewards].[sp_process_point_expirations]
    @AsOfDate = ?,
    @RunType = ?,
    @DryRun = ?
"""


POINT_EXPIRATION_RESULT_SQL = """
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


def process_point_expirations(
    adapter: PointExpirationSqlAdapter,
    as_of_date: date,
    *,
    run_type: str = "manual",
    dry_run: bool = False,
) -> PointExpirationResult:
    params = (as_of_date, run_type, bool(dry_run))
    if dry_run:
        rows = adapter.query_rows(PROCESS_POINT_EXPIRATIONS_SQL, params)
        if not rows:
            raise RuntimeError("Point expiration preview did not return a result row.")
        return _result_from_row(rows[0], dry_run=True)

    adapter.execute_statements([(PROCESS_POINT_EXPIRATIONS_SQL, params)])
    rows = adapter.query_rows(POINT_EXPIRATION_RESULT_SQL, (run_type, as_of_date, SOURCE_TYPE))
    if not rows:
        raise RuntimeError("Point expiration processing did not produce a reward_runs row.")
    summary = json.loads(rows[0].get("SummaryJson") or "{}")
    return PointExpirationResult(
        as_of_date=_coerce_date(rows[0].get("Snapshot_Date")) or as_of_date,
        dry_run=False,
        run_id=_coerce_optional_int(rows[0].get("RunID")),
        expiring_lot_count=_coerce_int(summary.get("expiring_lot_count")),
        already_expired_count=_coerce_int(summary.get("already_expired_count")),
        new_expiration_count=_coerce_int(summary.get("new_expiration_count")),
        expired_point_total=_coerce_int(summary.get("expired_point_total")),
        chapter_count=_coerce_int(summary.get("chapter_count")),
    )


def _result_from_row(row: dict[str, Any], *, dry_run: bool) -> PointExpirationResult:
    return PointExpirationResult(
        as_of_date=_coerce_date(row.get("AsOfDate")) or date.today(),
        dry_run=dry_run,
        run_id=_coerce_optional_int(row.get("RunID")),
        expiring_lot_count=_coerce_int(row.get("ExpiringLotCount")),
        already_expired_count=_coerce_int(row.get("AlreadyExpiredCount")),
        new_expiration_count=_coerce_int(row.get("NewExpirationCount")),
        expired_point_total=_coerce_int(row.get("ExpiredPointTotal")),
        chapter_count=_coerce_int(row.get("ChapterCount")),
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


def print_expiration_result(result: PointExpirationResult, output: TextIO) -> None:
    label = "Point Expirations Preview" if result.dry_run else "Point Expirations"
    print(label, file=output)
    print(f"  As of: {result.as_of_date.isoformat()}", file=output)
    if result.run_id is not None:
        print(f"  RunID: {result.run_id}", file=output)
    print(f"  Expiring lots considered: {result.expiring_lot_count}", file=output)
    print(f"  Already expired: {result.already_expired_count}", file=output)
    print(f"  New expirations: {result.new_expiration_count}", file=output)
    print(f"  Expired points: {result.expired_point_total}", file=output)
    print(f"  Chapters affected: {result.chapter_count}", file=output)


def main(argv: list[str] | None = None, output: TextIO = sys.stdout) -> int:
    parser = argparse.ArgumentParser(description="Expire unused AGA Chapter Rewards point lots.")
    parser.add_argument("--date", dest="as_of_date", type=parse_snapshot_date, help="Processing date in YYYY-MM-DD format. Defaults to today.")
    parser.add_argument("--dry-run", action="store_true", help="Preview point expirations without writing transactions or lot allocations.")
    parser.add_argument("--run-type", default="manual", choices=["daily", "manual", "import", "backfill"], help="reward_runs.Run_Type value.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    args = parser.parse_args(argv)

    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    result = process_point_expirations(
        SqlAdapter(conn_str),
        args.as_of_date or date.today(),
        run_type=args.run_type,
        dry_run=args.dry_run,
    )
    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True), file=output)
    else:
        print_expiration_result(result, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
