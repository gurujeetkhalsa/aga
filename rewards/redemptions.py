import argparse
import json
import re
import sys
import zipfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Protocol, TextIO
from xml.etree import ElementTree as ET

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string
from rewards.snapshot_generator import parse_snapshot_date


SOURCE_TYPE = "redemption"
DEFAULT_SOURCE_AS_OF_DATE = date(2026, 2, 8)
DEFAULT_LEDGER_START_DATE = date(2026, 5, 2)
DEFAULT_SHEET_NAME = "CashOuts"


class RedemptionSqlAdapter(Protocol):
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        ...

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        ...


@dataclass(frozen=True)
class LegacyRedemptionRow:
    source_row_number: int
    request_id: str
    chapter_id: int
    chapter_name: str
    request_date: date
    points: int
    notes: str
    redemption_category: str
    payment_mode: str
    description: str
    receipt_ref: str | None = None

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["request_date"] = self.request_date.isoformat()
        return result


@dataclass(frozen=True)
class LegacyRedemptionImportResult:
    dry_run: bool
    run_id: int | None
    source_as_of_date: date
    ledger_start_date: date
    input_row_count: int
    existing_request_count: int
    already_posted_count: int
    new_post_count: int
    missing_opening_lot_count: int
    insufficient_balance_count: int
    shortfall_adjustment_count: int
    shortfall_adjustment_points: int
    input_point_total: int
    new_point_total: int
    chapter_count: int
    dues_credit_count: int
    dues_credit_points: int
    reimbursement_count: int
    reimbursement_points: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "run_id": self.run_id,
            "source_as_of_date": self.source_as_of_date.isoformat(),
            "ledger_start_date": self.ledger_start_date.isoformat(),
            "input_row_count": self.input_row_count,
            "existing_request_count": self.existing_request_count,
            "already_posted_count": self.already_posted_count,
            "new_post_count": self.new_post_count,
            "missing_opening_lot_count": self.missing_opening_lot_count,
            "insufficient_balance_count": self.insufficient_balance_count,
            "shortfall_adjustment_count": self.shortfall_adjustment_count,
            "shortfall_adjustment_points": self.shortfall_adjustment_points,
            "input_point_total": self.input_point_total,
            "new_point_total": self.new_point_total,
            "chapter_count": self.chapter_count,
            "dues_credit_count": self.dues_credit_count,
            "dues_credit_points": self.dues_credit_points,
            "reimbursement_count": self.reimbursement_count,
            "reimbursement_points": self.reimbursement_points,
        }


IMPORT_LEGACY_REDEMPTIONS_SQL = """
EXEC [rewards].[sp_import_legacy_redemptions]
    @RedemptionsJson = ?,
    @DryRun = ?,
    @RunType = ?,
    @SourceAsOfDate = ?,
    @LedgerStartDate = ?,
    @PostedByPrincipalName = ?,
    @PostedByPrincipalId = ?
"""

IMPORT_LEGACY_REDEMPTIONS_WITH_ADJUSTMENTS_SQL = """
EXEC [rewards].[sp_import_legacy_redemptions_with_adjustments]
    @RedemptionsJson = ?,
    @DryRun = ?,
    @RunType = ?,
    @SourceAsOfDate = ?,
    @LedgerStartDate = ?,
    @PostedByPrincipalName = ?,
    @PostedByPrincipalId = ?,
    @AllowDuesCreditShortfallAdjustment = ?
"""


LEGACY_REDEMPTION_RESULT_SQL = """
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


def import_legacy_redemptions(
    adapter: RedemptionSqlAdapter,
    rows: list[LegacyRedemptionRow],
    *,
    dry_run: bool = False,
    run_type: str = "import",
    source_as_of_date: date = DEFAULT_SOURCE_AS_OF_DATE,
    ledger_start_date: date = DEFAULT_LEDGER_START_DATE,
    posted_by_principal_name: str | None = None,
    posted_by_principal_id: str | None = None,
    allow_dues_credit_shortfall_adjustment: bool = False,
) -> LegacyRedemptionImportResult:
    if not rows:
        raise ValueError("At least one legacy redemption row is required.")

    payload = json.dumps([row.as_dict() for row in rows], sort_keys=True)
    params = (
        payload,
        bool(dry_run),
        run_type,
        source_as_of_date,
        ledger_start_date,
        posted_by_principal_name,
        posted_by_principal_id,
    )
    sql = IMPORT_LEGACY_REDEMPTIONS_SQL
    if allow_dues_credit_shortfall_adjustment:
        sql = IMPORT_LEGACY_REDEMPTIONS_WITH_ADJUSTMENTS_SQL
        params = (*params, True)

    if dry_run:
        result_rows = adapter.query_rows(sql, params)
        if not result_rows:
            raise RuntimeError("Legacy redemption preview did not return a result row.")
        return _result_from_row(result_rows[0], dry_run=True)

    adapter.execute_statements([(sql, params)])
    result_rows = adapter.query_rows(
        LEGACY_REDEMPTION_RESULT_SQL,
        (run_type, ledger_start_date, SOURCE_TYPE),
    )
    if not result_rows:
        raise RuntimeError("Legacy redemption import did not produce a reward_runs row.")

    summary = json.loads(result_rows[0].get("SummaryJson") or "{}")
    return LegacyRedemptionImportResult(
        dry_run=False,
        run_id=_coerce_optional_int(result_rows[0].get("RunID")),
        source_as_of_date=_coerce_date(summary.get("source_balance_as_of_date")) or source_as_of_date,
        ledger_start_date=_coerce_date(result_rows[0].get("Snapshot_Date")) or ledger_start_date,
        input_row_count=_coerce_int(summary.get("input_row_count")),
        existing_request_count=_coerce_int(summary.get("existing_request_count")),
        already_posted_count=_coerce_int(summary.get("already_posted_count")),
        new_post_count=_coerce_int(summary.get("new_post_count")),
        missing_opening_lot_count=0,
        insufficient_balance_count=0,
        shortfall_adjustment_count=_coerce_int(summary.get("shortfall_adjustment_count")),
        shortfall_adjustment_points=_coerce_int(summary.get("shortfall_adjustment_points")),
        input_point_total=_coerce_int(summary.get("input_point_total")),
        new_point_total=_coerce_int(summary.get("new_point_total")),
        chapter_count=_coerce_int(summary.get("chapter_count")),
        dues_credit_count=_coerce_int(summary.get("dues_credit_count")),
        dues_credit_points=_coerce_int(summary.get("dues_credit_points")),
        reimbursement_count=_coerce_int(summary.get("reimbursement_count")),
        reimbursement_points=_coerce_int(summary.get("reimbursement_points")),
    )


def read_legacy_redemption_workbook(
    workbook_path: Path,
    *,
    sheet_name: str = DEFAULT_SHEET_NAME,
) -> list[LegacyRedemptionRow]:
    records = _read_xlsx_records(workbook_path, sheet_name=sheet_name)
    return [legacy_redemption_from_record(record, index + 1) for index, record in enumerate(records)]


def legacy_redemption_from_record(record: dict[str, Any], source_row_number: int) -> LegacyRedemptionRow:
    request_id = _required_text(record, "request_id")
    chapter_id = _required_int(record, "chapter_id")
    chapter_name = _required_text(record, "chapter_name")
    request_date = _coerce_record_date(_required_value(record, "request_date"))
    points = _required_int(record, "points")
    notes = _text_or_empty(record.get("notes"))
    category, payment_mode = _category_and_payment_mode(notes)
    description = notes or category.replace("_", " ")
    return LegacyRedemptionRow(
        source_row_number=source_row_number,
        request_id=request_id,
        chapter_id=chapter_id,
        chapter_name=chapter_name,
        request_date=request_date,
        points=points,
        notes=notes,
        redemption_category=category,
        payment_mode=payment_mode,
        description=description,
        receipt_ref=request_id,
    )


def print_redemption_result(result: LegacyRedemptionImportResult, output: TextIO) -> None:
    label = "Legacy Redemption Import Preview" if result.dry_run else "Legacy Redemption Import"
    print(label, file=output)
    if result.run_id is not None:
        print(f"  RunID: {result.run_id}", file=output)
    print(f"  Source balance as of: {result.source_as_of_date.isoformat()}", file=output)
    print(f"  Ledger start: {result.ledger_start_date.isoformat()}", file=output)
    print(f"  Input rows: {result.input_row_count}", file=output)
    print(f"  Existing requests: {result.existing_request_count}", file=output)
    print(f"  Already posted: {result.already_posted_count}", file=output)
    print(f"  New posts: {result.new_post_count}", file=output)
    print(f"  Missing opening lots: {result.missing_opening_lot_count}", file=output)
    print(f"  Insufficient opening balances: {result.insufficient_balance_count}", file=output)
    print(f"  Shortfall adjustments: {result.shortfall_adjustment_count} / {result.shortfall_adjustment_points}", file=output)
    print(f"  Input point total: {result.input_point_total}", file=output)
    print(f"  New point total: {result.new_point_total}", file=output)
    print(f"  Chapters affected: {result.chapter_count}", file=output)
    print(f"  Dues credits: {result.dues_credit_count} / {result.dues_credit_points}", file=output)
    print(f"  Reimbursements: {result.reimbursement_count} / {result.reimbursement_points}", file=output)


def _read_xlsx_records(workbook_path: Path, *, sheet_name: str) -> list[dict[str, Any]]:
    if not workbook_path.exists():
        raise FileNotFoundError(workbook_path)

    with zipfile.ZipFile(workbook_path) as archive:
        shared_strings = _load_shared_strings(archive)
        sheet_path = _sheet_path(archive, sheet_name)
        root = ET.fromstring(archive.read(sheet_path))

    ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    sheet_data = root.find("main:sheetData", ns)
    if sheet_data is None:
        return []

    rows: list[list[Any]] = []
    for row in sheet_data.findall("main:row", ns):
        values: dict[int, Any] = {}
        for cell in row.findall("main:c", ns):
            ref = cell.attrib.get("r", "")
            col_index = _column_index(ref)
            if col_index is None:
                continue
            values[col_index] = _cell_value(cell, shared_strings)
        if values:
            max_col = max(values)
            rows.append([values.get(index) for index in range(1, max_col + 1)])

    if not rows:
        return []

    headers = [_normalize_header(value) for value in rows[0]]
    records: list[dict[str, Any]] = []
    for row in rows[1:]:
        if all(_is_blank(value) for value in row):
            continue
        record = {
            header: row[index] if index < len(row) else None
            for index, header in enumerate(headers)
            if header
        }
        records.append(record)
    return records


def _load_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    strings: list[str] = []
    for item in root.findall("main:si", ns):
        text = "".join(node.text or "" for node in item.findall(".//main:t", ns))
        strings.append(text)
    return strings


def _sheet_path(archive: zipfile.ZipFile, sheet_name: str) -> str:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    rels_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    ns = {
        "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
        "pkg": "http://schemas.openxmlformats.org/package/2006/relationships",
    }
    rel_targets = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels_root.findall("pkg:Relationship", ns)
        if "Id" in rel.attrib and "Target" in rel.attrib
    }
    for sheet in workbook.findall("main:sheets/main:sheet", ns):
        if sheet.attrib.get("name") != sheet_name:
            continue
        rel_id = sheet.attrib.get(f"{{{ns['rel']}}}id")
        if not rel_id or rel_id not in rel_targets:
            break
        target = rel_targets[rel_id].replace("\\", "/")
        if target.startswith("/"):
            return target.lstrip("/")
        if target.startswith("xl/"):
            return target
        return f"xl/{target}"
    raise ValueError(f"Workbook does not contain a sheet named {sheet_name!r}.")


def _cell_value(cell: ET.Element, shared_strings: list[str]) -> Any:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t"))

    value_node = cell.find("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v")
    if value_node is None or value_node.text is None:
        return None
    text = value_node.text
    if cell_type == "s":
        return shared_strings[int(text)]
    if cell_type == "b":
        return text == "1"
    if cell_type in {"str", "e"}:
        return text
    try:
        number = float(text)
    except ValueError:
        return text
    if number.is_integer():
        return int(number)
    return number


def _column_index(cell_ref: str) -> int | None:
    match = re.match(r"([A-Z]+)", cell_ref.upper())
    if not match:
        return None
    index = 0
    for char in match.group(1):
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index


def _normalize_header(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    aliases = {
        "request_id": "request_id",
        "chapter_id": "chapter_id",
        "chapter_name": "chapter_name",
        "request_date": "request_date",
        "points": "points",
        "notes": "notes",
    }
    return aliases.get(text, text)


def _category_and_payment_mode(notes: str) -> tuple[str, str]:
    normalized = notes.strip().lower()
    if normalized == "chapter renewal":
        return "chapter_renewal", "dues_credit"
    if normalized == "go promotion":
        return "go_promotion", "reimbursement"
    return "other", "other"


def _required_value(record: dict[str, Any], key: str) -> Any:
    value = record.get(key)
    if _is_blank(value):
        raise ValueError(f"Missing required redemption field: {key}")
    return value


def _required_text(record: dict[str, Any], key: str) -> str:
    return str(_required_value(record, key)).strip()


def _required_int(record: dict[str, Any], key: str) -> int:
    value = _required_value(record, key)
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{key} must be an integer.")
    return int(value)


def _coerce_record_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        return (datetime(1899, 12, 30) + timedelta(days=float(value))).date()
    text = str(value).strip()
    if "T" in text:
        return datetime.fromisoformat(text).date()
    return date.fromisoformat(text.split(" ", 1)[0])


def _text_or_empty(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _result_from_row(row: dict[str, Any], *, dry_run: bool) -> LegacyRedemptionImportResult:
    return LegacyRedemptionImportResult(
        dry_run=dry_run,
        run_id=_coerce_optional_int(row.get("RunID")),
        source_as_of_date=_coerce_date(row.get("SourceAsOfDate")) or DEFAULT_SOURCE_AS_OF_DATE,
        ledger_start_date=_coerce_date(row.get("LedgerStartDate")) or DEFAULT_LEDGER_START_DATE,
        input_row_count=_coerce_int(row.get("InputRowCount")),
        existing_request_count=_coerce_int(row.get("ExistingRequestCount")),
        already_posted_count=_coerce_int(row.get("AlreadyPostedCount")),
        new_post_count=_coerce_int(row.get("NewPostCount")),
        missing_opening_lot_count=_coerce_int(row.get("MissingOpeningLotCount")),
        insufficient_balance_count=_coerce_int(row.get("InsufficientBalanceCount")),
        shortfall_adjustment_count=_coerce_int(row.get("ShortfallAdjustmentCount")),
        shortfall_adjustment_points=_coerce_int(row.get("ShortfallAdjustmentPoints")),
        input_point_total=_coerce_int(row.get("InputPointTotal")),
        new_point_total=_coerce_int(row.get("NewPointTotal")),
        chapter_count=_coerce_int(row.get("ChapterCount")),
        dues_credit_count=_coerce_int(row.get("DuesCreditCount")),
        dues_credit_points=_coerce_int(row.get("DuesCreditPoints")),
        reimbursement_count=_coerce_int(row.get("ReimbursementCount")),
        reimbursement_points=_coerce_int(row.get("ReimbursementPoints")),
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


def main(argv: list[str] | None = None, output: TextIO = sys.stdout) -> int:
    parser = argparse.ArgumentParser(description="Import legacy-gap AGA Chapter Rewards redemptions from a workbook.")
    parser.add_argument("workbook_path", type=Path, help="Path to the redemption workbook.")
    parser.add_argument("--sheet-name", default=DEFAULT_SHEET_NAME, help=f"Worksheet name. Defaults to {DEFAULT_SHEET_NAME}.")
    parser.add_argument("--source-as-of-date", type=parse_snapshot_date, default=DEFAULT_SOURCE_AS_OF_DATE, help="Legacy source balance date. Defaults to 2026-02-08.")
    parser.add_argument("--ledger-start-date", type=parse_snapshot_date, default=DEFAULT_LEDGER_START_DATE, help="New ledger start date. Defaults to 2026-05-02.")
    parser.add_argument("--dry-run", action="store_true", help="Preview the import without writing redemption requests, transactions, or lot allocations.")
    parser.add_argument("--run-type", default="import", choices=["daily", "manual", "import", "backfill"], help="reward_runs.Run_Type value.")
    parser.add_argument("--posted-by-principal-name", help="Operator principal name for audit fields.")
    parser.add_argument("--posted-by-principal-id", help="Operator principal ID for audit fields.")
    parser.add_argument("--allow-dues-credit-shortfall-adjustment", action="store_true", help="Create tagged adjustment lots for legacy chapter-dues credits that exceed the opening balance.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    args = parser.parse_args(argv)

    rows = read_legacy_redemption_workbook(args.workbook_path, sheet_name=args.sheet_name)
    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    result = import_legacy_redemptions(
        SqlAdapter(conn_str),
        rows,
        dry_run=args.dry_run,
        run_type=args.run_type,
        source_as_of_date=args.source_as_of_date,
        ledger_start_date=args.ledger_start_date,
        posted_by_principal_name=args.posted_by_principal_name,
        posted_by_principal_id=args.posted_by_principal_id,
        allow_dues_credit_shortfall_adjustment=args.allow_dues_credit_shortfall_adjustment,
    )
    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True), file=output)
    else:
        print_redemption_result(result, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
