import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Iterable, Protocol, TextIO

from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string


DEFAULT_TOP = 25


class RewardsReportAdapter(Protocol):
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        ...


Column = tuple[str, str]
FetchReport = Callable[[RewardsReportAdapter, argparse.Namespace], list[dict[str, Any]]]


@dataclass(frozen=True)
class ReportSpec:
    title: str
    columns: tuple[Column, ...]
    fetch: FetchReport


BALANCES_SQL = """
SELECT TOP (?)
    [Chapter_Code],
    [Chapter_Name],
    [Available_Points],
    [Total_Remaining_Points],
    [Expired_Unallocated_Points],
    [Ledger_Balance],
    [Balance_Reconciliation_Delta],
    [Expiring_30_Days],
    [Next_Expiration_Date],
    [Active_Member_Count],
    [Multiplier],
    [Latest_Snapshot_Date],
    [Transaction_Count]
FROM [rewards].[v_chapter_balances]
WHERE (? IS NULL OR [Chapter_Code] = ?)
ORDER BY [Available_Points] DESC, [Chapter_Code]
"""


TRANSACTIONS_SQL = """
SELECT TOP (?)
    [TransactionID],
    [Chapter_Code],
    [Transaction_Type],
    [Points_Delta],
    [Effective_Date],
    [Earned_Date],
    [Posted_At],
    [Source_Type],
    [Source_Key],
    [RunID],
    [Run_Processor],
    [LotID],
    [Lot_Remaining_Points],
    [Lot_Expires_On]
FROM [rewards].[v_chapter_transaction_history]
WHERE (? IS NULL OR [Chapter_Code] = ?)
  AND (? IS NULL OR [Source_Type] = ?)
ORDER BY [Posted_At] DESC, [TransactionID] DESC
"""


LOTS_SQL = """
SELECT TOP (?)
    [LotID],
    [Chapter_Code],
    [Original_Points],
    [Remaining_Points],
    [Allocated_Points],
    [Earned_Date],
    [Expires_On],
    [Days_Until_Expiration],
    [Aging_Status],
    [Source_Type],
    [Source_Key],
    [RunID]
FROM [rewards].[v_point_lot_aging]
WHERE (? IS NULL OR [Chapter_Code] = ?)
  AND (? IS NULL OR [Aging_Status] = ?)
ORDER BY
    CASE WHEN [Remaining_Points] > 0 THEN 0 ELSE 1 END,
    [Expires_On],
    [Earned_Date],
    [LotID]
"""


RUNS_SQL = """
SELECT TOP (?)
    [RunID],
    [Run_Type],
    [Snapshot_Date],
    [Started_At],
    [Completed_At],
    [Duration_Seconds],
    [Status],
    [Processor],
    [Transaction_Count],
    [Net_Points],
    [Credit_Points],
    [Debit_Points]
FROM [rewards].[v_reward_run_history]
WHERE (? IS NULL OR [Processor] = ?)
  AND (? IS NULL OR [Status] = ?)
ORDER BY [RunID] DESC
"""


MEMBERSHIP_EVENTS_SQL = """
SELECT TOP (?)
    [Membership_Event_ID],
    [AGAID],
    [Event_Type],
    [Event_Date],
    [Credit_Deadline],
    [Status],
    [Base_Points],
    [Credited_Chapter_Code],
    [Credited_Points],
    [Credited_Multiplier],
    [Credited_Posted_At],
    [Message_ID]
FROM [rewards].[v_membership_event_audit]
WHERE (? IS NULL OR [Status] = ?)
ORDER BY [Event_Date] DESC, [Membership_Event_ID] DESC
"""


BALANCE_COLUMNS: tuple[Column, ...] = (
    ("Chapter_Code", "Chapter"),
    ("Chapter_Name", "Name"),
    ("Available_Points", "Available"),
    ("Total_Remaining_Points", "Remaining"),
    ("Expired_Unallocated_Points", "Expired"),
    ("Ledger_Balance", "Ledger"),
    ("Balance_Reconciliation_Delta", "Delta"),
    ("Expiring_30_Days", "Exp 30d"),
    ("Next_Expiration_Date", "Next Exp"),
    ("Active_Member_Count", "Members"),
    ("Multiplier", "Mult"),
    ("Latest_Snapshot_Date", "Snapshot"),
    ("Transaction_Count", "Txns"),
)


TRANSACTION_COLUMNS: tuple[Column, ...] = (
    ("TransactionID", "Txn"),
    ("Chapter_Code", "Chapter"),
    ("Transaction_Type", "Type"),
    ("Points_Delta", "Points"),
    ("Effective_Date", "Effective"),
    ("Earned_Date", "Earned"),
    ("Posted_At", "Posted"),
    ("Source_Type", "Source"),
    ("Source_Key", "Source Key"),
    ("RunID", "Run"),
    ("Run_Processor", "Processor"),
    ("LotID", "Lot"),
    ("Lot_Remaining_Points", "Lot Rem"),
    ("Lot_Expires_On", "Lot Exp"),
)


LOT_COLUMNS: tuple[Column, ...] = (
    ("LotID", "Lot"),
    ("Chapter_Code", "Chapter"),
    ("Original_Points", "Original"),
    ("Remaining_Points", "Remaining"),
    ("Allocated_Points", "Allocated"),
    ("Earned_Date", "Earned"),
    ("Expires_On", "Expires"),
    ("Days_Until_Expiration", "Days"),
    ("Aging_Status", "Status"),
    ("Source_Type", "Source"),
    ("Source_Key", "Source Key"),
    ("RunID", "Run"),
)


RUN_COLUMNS: tuple[Column, ...] = (
    ("RunID", "Run"),
    ("Run_Type", "Type"),
    ("Snapshot_Date", "Date"),
    ("Started_At", "Started"),
    ("Completed_At", "Completed"),
    ("Duration_Seconds", "Sec"),
    ("Status", "Status"),
    ("Processor", "Processor"),
    ("Transaction_Count", "Txns"),
    ("Net_Points", "Net"),
    ("Credit_Points", "Credits"),
    ("Debit_Points", "Debits"),
)


MEMBERSHIP_EVENT_COLUMNS: tuple[Column, ...] = (
    ("Membership_Event_ID", "Event"),
    ("AGAID", "AGAID"),
    ("Event_Type", "Type"),
    ("Event_Date", "Date"),
    ("Credit_Deadline", "Deadline"),
    ("Status", "Status"),
    ("Base_Points", "Base"),
    ("Credited_Chapter_Code", "Chapter"),
    ("Credited_Points", "Points"),
    ("Credited_Multiplier", "Mult"),
    ("Credited_Posted_At", "Posted"),
    ("Message_ID", "Message"),
)


def fetch_balances(adapter: RewardsReportAdapter, args: argparse.Namespace) -> list[dict[str, Any]]:
    return adapter.query_rows(BALANCES_SQL, (args.top, args.chapter_code, args.chapter_code))


def fetch_transactions(adapter: RewardsReportAdapter, args: argparse.Namespace) -> list[dict[str, Any]]:
    return adapter.query_rows(
        TRANSACTIONS_SQL,
        (args.top, args.chapter_code, args.chapter_code, args.source_type, args.source_type),
    )


def fetch_lots(adapter: RewardsReportAdapter, args: argparse.Namespace) -> list[dict[str, Any]]:
    return adapter.query_rows(
        LOTS_SQL,
        (args.top, args.chapter_code, args.chapter_code, args.aging_status, args.aging_status),
    )


def fetch_runs(adapter: RewardsReportAdapter, args: argparse.Namespace) -> list[dict[str, Any]]:
    return adapter.query_rows(RUNS_SQL, (args.top, args.processor, args.processor, args.status, args.status))


def fetch_membership_events(adapter: RewardsReportAdapter, args: argparse.Namespace) -> list[dict[str, Any]]:
    return adapter.query_rows(MEMBERSHIP_EVENTS_SQL, (args.top, args.status, args.status))


REPORTS: dict[str, ReportSpec] = {
    "balances": ReportSpec("Chapter Balances", BALANCE_COLUMNS, fetch_balances),
    "transactions": ReportSpec("Chapter Reward Transactions", TRANSACTION_COLUMNS, fetch_transactions),
    "lots": ReportSpec("Point Lot Aging", LOT_COLUMNS, fetch_lots),
    "runs": ReportSpec("Reward Run History", RUN_COLUMNS, fetch_runs),
    "membership-events": ReportSpec("Membership Event Audit", MEMBERSHIP_EVENT_COLUMNS, fetch_membership_events),
}


def format_table(rows: list[dict[str, Any]], columns: tuple[Column, ...]) -> str:
    if not rows:
        return "(no rows)"

    rendered_rows = [[_format_value(row.get(key)) for key, _ in columns] for row in rows]
    widths = [
        max(len(heading), *(len(row[index]) for row in rendered_rows))
        for index, (_, heading) in enumerate(columns)
    ]
    headings = [heading.ljust(widths[index]) for index, (_, heading) in enumerate(columns)]
    divider = ["-" * width for width in widths]
    lines = [
        "  ".join(headings).rstrip(),
        "  ".join(divider).rstrip(),
    ]
    for row in rendered_rows:
        lines.append("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)).rstrip())
    return "\n".join(lines)


def print_report(title: str, rows: list[dict[str, Any]], columns: tuple[Column, ...], output: TextIO) -> None:
    print(title, file=output)
    print(format_table(rows, columns), file=output)


def rows_as_json(rows: list[dict[str, Any]]) -> str:
    return json.dumps(rows, default=_json_default, indent=2, sort_keys=True)


def _format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return str(int(value))
        return str(value)
    return str(value)


def _json_default(value: Any) -> str | int | float:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect AGA Chapter Rewards balances and audit history.")
    parser.add_argument("report", choices=sorted(REPORTS), help="Report to run.")
    parser.add_argument("--top", type=_positive_int, default=DEFAULT_TOP, help=f"Maximum rows to return. Defaults to {DEFAULT_TOP}.")
    parser.add_argument("--chapter-code", help="Limit chapter-scoped reports to one chapter code.")
    parser.add_argument("--status", help="Limit run or membership-event reports to one status.")
    parser.add_argument("--aging-status", help="Limit lot report to one aging status.")
    parser.add_argument("--processor", help="Limit run report to one processor value.")
    parser.add_argument("--source-type", help="Limit transaction report to one source type.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    return parser


def main(argv: list[str] | None = None, output: TextIO = sys.stdout) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    report = REPORTS[args.report]
    rows = report.fetch(SqlAdapter(conn_str), args)
    if args.json:
        print(rows_as_json(rows), file=output)
    else:
        print_report(report.title, rows, report.columns, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
