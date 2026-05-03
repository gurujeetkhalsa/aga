import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Protocol, TextIO

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string
from rewards.snapshot_generator import parse_snapshot_date


SOURCE_TYPE = "opening_balance"
BALANCE_LINE_RE = re.compile(r"^\s*(?P<legacy_agaid>\d+)\s+(?P<label>.+?)\s+(?P<numbers>\d+(?:\s+\d+){2,3})\s*$")
CHAPTER_CODE_RE = re.compile(r"^[A-Z0-9]{3,5}$")


class OpeningBalanceSqlAdapter(Protocol):
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        ...

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        ...


@dataclass(frozen=True)
class OpeningBalanceRow:
    source_row_number: int
    legacy_agaid: int
    chapter_name: str
    chapter_code: str
    prior_available_points: int
    earned_points: int
    used_points: int
    opening_balance_points: int

    @property
    def reconciliation_delta(self) -> int:
        return self.opening_balance_points - (
            self.prior_available_points + self.earned_points - self.used_points
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OpeningBalanceImportResult:
    effective_date: date
    dry_run: bool
    run_id: int | None
    input_row_count: int
    setup_row_count: int
    added_zero_chapter_count: int
    positive_balance_row_count: int
    zero_balance_row_count: int
    missing_snapshot_count: int
    reconciliation_issue_count: int
    already_imported_count: int
    new_import_count: int
    input_point_total: int
    new_point_total: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "effective_date": self.effective_date.isoformat(),
            "dry_run": self.dry_run,
            "run_id": self.run_id,
            "input_row_count": self.input_row_count,
            "setup_row_count": self.setup_row_count,
            "added_zero_chapter_count": self.added_zero_chapter_count,
            "positive_balance_row_count": self.positive_balance_row_count,
            "zero_balance_row_count": self.zero_balance_row_count,
            "missing_snapshot_count": self.missing_snapshot_count,
            "reconciliation_issue_count": self.reconciliation_issue_count,
            "already_imported_count": self.already_imported_count,
            "new_import_count": self.new_import_count,
            "input_point_total": self.input_point_total,
            "new_point_total": self.new_point_total,
        }


IMPORT_OPENING_BALANCES_SQL = """
EXEC [rewards].[sp_import_opening_balances]
    @OpeningBalancesJson = ?,
    @EffectiveDate = ?,
    @RunType = ?,
    @DryRun = ?
"""


OPENING_BALANCE_RESULT_SQL = """
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


def parse_balance_line(line: str, source_row_number: int) -> OpeningBalanceRow | None:
    stripped = re.sub(r"\s+", " ", line).strip()
    if not stripped or stripped.startswith("AGAID "):
        return None

    match = BALANCE_LINE_RE.match(stripped)
    if not match:
        return None

    chapter_name, chapter_code = _split_chapter_label(match.group("label"))
    if not chapter_name or not chapter_code:
        return None

    numbers = [int(value) for value in match.group("numbers").split()]
    if len(numbers) == 3:
        prior_available, earned, opening_balance = numbers
        used = 0
    elif len(numbers) == 4:
        prior_available, earned, used, opening_balance = numbers
    else:
        return None

    return OpeningBalanceRow(
        source_row_number=source_row_number,
        legacy_agaid=int(match.group("legacy_agaid")),
        chapter_name=chapter_name,
        chapter_code=chapter_code,
        prior_available_points=prior_available,
        earned_points=earned,
        used_points=used,
        opening_balance_points=opening_balance,
    )


def _split_chapter_label(label: str) -> tuple[str | None, str | None]:
    normalized = label.strip()
    if not normalized:
        return None, None

    if " " in normalized:
        name, possible_code = normalized.rsplit(" ", 1)
        if CHAPTER_CODE_RE.fullmatch(possible_code):
            return name.strip(), possible_code.upper()

    for code_length in (5, 4, 3):
        if len(normalized) <= code_length:
            continue
        possible_code = normalized[-code_length:]
        if CHAPTER_CODE_RE.fullmatch(possible_code):
            return normalized[:-code_length].strip(), possible_code.upper()

    return None, None


def parse_balance_text(text: str) -> list[OpeningBalanceRow]:
    rows: list[OpeningBalanceRow] = []
    unparsed: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        parsed = parse_balance_line(stripped, len(rows) + 1)
        if parsed is not None:
            rows.append(parsed)
        elif stripped and not stripped.startswith("AGAID "):
            unparsed.append(stripped)

    if unparsed:
        preview = "; ".join(unparsed[:3])
        raise ValueError(f"Could not parse {len(unparsed)} non-empty PDF line(s): {preview}")
    if not rows:
        raise ValueError("No opening balance rows were found.")
    return rows


def parse_balance_pdf(pdf_path: Path) -> list[OpeningBalanceRow]:
    reader_class = _load_pdf_reader()
    reader = reader_class(str(pdf_path))
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    return parse_balance_text(text)


def import_opening_balances(
    adapter: OpeningBalanceSqlAdapter,
    rows: list[OpeningBalanceRow],
    effective_date: date,
    *,
    run_type: str = "import",
    dry_run: bool = False,
) -> OpeningBalanceImportResult:
    if not rows:
        raise ValueError("At least one opening balance row is required.")

    payload = json.dumps([row.as_dict() for row in rows], sort_keys=True)
    params = (payload, effective_date, run_type, bool(dry_run))
    if dry_run:
        result_rows = adapter.query_rows(IMPORT_OPENING_BALANCES_SQL, params)
        if not result_rows:
            raise RuntimeError("Opening balance preview did not return a result row.")
        return _result_from_row(result_rows[0], dry_run=True)

    adapter.execute_statements([(IMPORT_OPENING_BALANCES_SQL, params)])
    result_rows = adapter.query_rows(OPENING_BALANCE_RESULT_SQL, (run_type, effective_date, SOURCE_TYPE))
    if not result_rows:
        raise RuntimeError("Opening balance import did not produce a reward_runs row.")
    summary = json.loads(result_rows[0].get("SummaryJson") or "{}")
    return OpeningBalanceImportResult(
        effective_date=_coerce_date(result_rows[0].get("Snapshot_Date")) or effective_date,
        dry_run=False,
        run_id=_coerce_optional_int(result_rows[0].get("RunID")),
        input_row_count=_coerce_int(summary.get("input_row_count")),
        setup_row_count=_coerce_int(summary.get("setup_row_count", summary.get("input_row_count"))),
        added_zero_chapter_count=_coerce_int(summary.get("added_zero_chapter_count")),
        positive_balance_row_count=_coerce_int(summary.get("positive_balance_row_count")),
        zero_balance_row_count=_coerce_int(summary.get("zero_balance_row_count")),
        missing_snapshot_count=_coerce_int(summary.get("missing_snapshot_count")),
        reconciliation_issue_count=_coerce_int(summary.get("reconciliation_issue_count")),
        already_imported_count=_coerce_int(summary.get("already_imported_count")),
        new_import_count=_coerce_int(summary.get("new_import_count")),
        input_point_total=_coerce_int(summary.get("input_point_total")),
        new_point_total=_coerce_int(summary.get("new_point_total")),
    )


def print_import_result(result: OpeningBalanceImportResult, output: TextIO) -> None:
    label = "Opening Balance Import Preview" if result.dry_run else "Opening Balance Import"
    print(label, file=output)
    print(f"  Effective date: {result.effective_date.isoformat()}", file=output)
    if result.run_id is not None:
        print(f"  RunID: {result.run_id}", file=output)
    print(f"  PDF rows: {result.input_row_count}", file=output)
    print(f"  Setup rows: {result.setup_row_count}", file=output)
    print(f"  Added zero-balance chapters: {result.added_zero_chapter_count}", file=output)
    print(f"  Positive-balance rows: {result.positive_balance_row_count}", file=output)
    print(f"  Zero-balance rows: {result.zero_balance_row_count}", file=output)
    print(f"  Missing latest snapshot matches: {result.missing_snapshot_count}", file=output)
    print(f"  Reconciliation issues: {result.reconciliation_issue_count}", file=output)
    print(f"  Already imported: {result.already_imported_count}", file=output)
    print(f"  New imports: {result.new_import_count}", file=output)
    print(f"  Input point total: {result.input_point_total}", file=output)
    print(f"  New point total: {result.new_point_total}", file=output)


def _load_pdf_reader():
    try:
        from pypdf import PdfReader

        return PdfReader
    except Exception:
        fallback = Path.home() / ".cache" / "codex-runtimes" / "codex-primary-runtime" / "dependencies" / "python" / "Lib" / "site-packages"
        if fallback.exists():
            sys.path.append(str(fallback))
            from pypdf import PdfReader

            return PdfReader
        raise


def _result_from_row(row: dict[str, Any], *, dry_run: bool) -> OpeningBalanceImportResult:
    return OpeningBalanceImportResult(
        effective_date=_coerce_date(row.get("EffectiveDate")) or date.today(),
        dry_run=dry_run,
        run_id=_coerce_optional_int(row.get("RunID")),
        input_row_count=_coerce_int(row.get("InputRowCount")),
        setup_row_count=_coerce_int(row.get("SetupRowCount", row.get("InputRowCount"))),
        added_zero_chapter_count=_coerce_int(row.get("AddedZeroChapterCount")),
        positive_balance_row_count=_coerce_int(row.get("PositiveBalanceRowCount")),
        zero_balance_row_count=_coerce_int(row.get("ZeroBalanceRowCount")),
        missing_snapshot_count=_coerce_int(row.get("MissingSnapshotCount")),
        reconciliation_issue_count=_coerce_int(row.get("ReconciliationIssueCount")),
        already_imported_count=_coerce_int(row.get("AlreadyImportedCount")),
        new_import_count=_coerce_int(row.get("NewImportCount")),
        input_point_total=_coerce_int(row.get("InputPointTotal")),
        new_point_total=_coerce_int(row.get("NewPointTotal")),
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
    parser = argparse.ArgumentParser(description="Import AGA Chapter Rewards opening balances from the legacy PDF balance sheet.")
    parser.add_argument("pdf_path", type=Path, help="Path to the opening balance PDF.")
    parser.add_argument("--effective-date", type=parse_snapshot_date, default=date.today(), help="Grandfathered earned/effective date. Defaults to today.")
    parser.add_argument("--dry-run", action="store_true", help="Preview the import without writing transactions or point lots.")
    parser.add_argument("--run-type", default="import", choices=["daily", "manual", "import", "backfill"], help="reward_runs.Run_Type value.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    args = parser.parse_args(argv)

    rows = parse_balance_pdf(args.pdf_path)
    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    result = import_opening_balances(
        SqlAdapter(conn_str),
        rows,
        args.effective_date,
        run_type=args.run_type,
        dry_run=args.dry_run,
    )
    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True), file=output)
    else:
        print_import_result(result, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
