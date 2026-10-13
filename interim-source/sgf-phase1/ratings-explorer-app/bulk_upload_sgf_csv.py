import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ratings_explorer_support as explorer

REQUIRED_MATCH_COLUMNS = ("Tournament_Code", "Round", "Pin_Player_1", "Pin_Player_2", "Game_Date")
RECOMMENDED_COLUMNS = (
    "Game_ID",
    "Tournament_Code",
    "Round",
    "Pin_Player_1",
    "Pin_Player_2",
    "Game_Date",
    "Sgf_File",
    "Replace_Existing",
    "Sgf_Code",
)


@dataclass
class RowPlan:
    row_number: int
    csv_row: dict[str, str]
    game: dict[str, Any] | None
    status: str
    action: str
    message: str
    sgf_file: Path | None = None
    new_sgf_code: str | None = None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk upload SGFs from a CSV file into ratings.games.Sgf_Code and the SGF blob container."
    )
    parser.add_argument("csv_file", help="Path to the CSV manifest.")
    parser.add_argument(
        "--mode",
        choices=("dry-run", "apply"),
        default="dry-run",
        help="Use dry-run to validate only, or apply to upload/update rows.",
    )
    parser.add_argument(
        "--report-file",
        help="Optional path for a JSON results report. Defaults to <csv>.results.json.",
    )
    return parser.parse_args()


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _parse_int(value: str, field_name: str) -> int:
    text = _clean(value)
    if not text:
        raise ValueError(f"{field_name} is required.")
    try:
        return int(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be numeric.") from exc


def _parse_bool(value: str) -> bool:
    return _clean(value).lower() in {"1", "true", "yes", "y", "on"}


def _read_manifest(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("CSV is missing a header row.")
        rows: list[dict[str, str]] = []
        for row in reader:
            if not any(_clean(value) for value in row.values()):
                continue
            rows.append({key: _clean(value) for key, value in row.items()})
        return rows


def _validate_headers(fieldnames: list[str] | None) -> None:
    if not fieldnames:
        raise ValueError("CSV is missing headers.")
    missing = [name for name in ("Sgf_File",) if name not in fieldnames]
    if missing:
        raise ValueError(f"CSV is missing required column(s): {', '.join(missing)}.")


def _normalize_date_text(value: Any) -> str:
    text = explorer.json_safe_value(value)
    text = _clean(text)
    return text[:10] if len(text) >= 10 else text


def _read_sgf_text(path: Path) -> str:
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode SGF file: {path}")


def _load_game_by_match_fields(conn_str: str, row: dict[str, str]) -> dict[str, Any] | None:
    for name in REQUIRED_MATCH_COLUMNS:
        if not _clean(row.get(name)):
            raise ValueError(
                "Rows without Game_ID must include Tournament_Code, Round, Pin_Player_1, Pin_Player_2, and Game_Date."
            )
    matches = explorer.query_rows(
        conn_str,
        """
SELECT
    g.[Game_ID],
    g.[Tournament_Code],
    g.[Game_Date],
    g.[Round],
    g.[Result],
    g.[Handicap],
    g.[Komi],
    g.[Sgf_Code],
    g.[Pin_Player_1],
    g.[Pin_Player_2],
    p1.[FirstName] AS [Player1FirstName],
    p1.[LastName] AS [Player1LastName],
    p2.[FirstName] AS [Player2FirstName],
    p2.[LastName] AS [Player2LastName]
FROM [ratings].[games] AS g
LEFT JOIN [membership].[members] AS p1
    ON p1.[AGAID] = g.[Pin_Player_1]
LEFT JOIN [membership].[members] AS p2
    ON p2.[AGAID] = g.[Pin_Player_2]
WHERE g.[Tournament_Code] = ?
  AND g.[Round] = ?
  AND g.[Pin_Player_1] = ?
  AND g.[Pin_Player_2] = ?
  AND CAST(g.[Game_Date] AS date) = ?
ORDER BY g.[Game_ID]
""",
        [
            _clean(row["Tournament_Code"]),
            _parse_int(row["Round"], "Round"),
            _parse_int(row["Pin_Player_1"], "Pin_Player_1"),
            _parse_int(row["Pin_Player_2"], "Pin_Player_2"),
            _clean(row["Game_Date"]),
        ],
    )
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError("Match fields resolved to multiple games. Add Game_ID to disambiguate.")
    return matches[0]


def _load_target_game(conn_str: str, row: dict[str, str]) -> dict[str, Any] | None:
    game_id_text = _clean(row.get("Game_ID"))
    if game_id_text:
        game = explorer.load_game_for_sgf_upload(conn_str, _parse_int(game_id_text, "Game_ID"))
        if not game:
            return None
        return game
    return _load_game_by_match_fields(conn_str, row)


def _cross_check_game(row: dict[str, str], game: dict[str, Any]) -> None:
    checks = {
        "Tournament_Code": _clean(game.get("Tournament_Code")),
        "Round": _clean(game.get("Round")),
        "Pin_Player_1": _clean(game.get("Pin_Player_1")),
        "Pin_Player_2": _clean(game.get("Pin_Player_2")),
        "Game_Date": _normalize_date_text(game.get("Game_Date")),
    }
    mismatches: list[str] = []
    for field_name, actual in checks.items():
        expected = _clean(row.get(field_name))
        if expected and expected != actual:
            mismatches.append(f"{field_name} expected '{expected}' but found '{actual}'")
    if mismatches:
        raise ValueError("; ".join(mismatches))


def _build_plan_row(conn_str: str, csv_path: Path, row_number: int, row: dict[str, str]) -> RowPlan:
    try:
        sgf_file_text = _clean(row.get("Sgf_File"))
        if not sgf_file_text:
            raise ValueError("Sgf_File is required.")
        sgf_file = Path(sgf_file_text)
        if not sgf_file.is_absolute():
            sgf_file = (csv_path.parent / sgf_file).resolve()
        if not sgf_file.exists() or not sgf_file.is_file():
            raise ValueError(f"SGF file was not found: {sgf_file}")
        sgf_text = _read_sgf_text(sgf_file)
        if "(;" not in sgf_text or ")" not in sgf_text:
            raise ValueError("SGF file does not look like valid SGF.")

        game = _load_target_game(conn_str, row)
        if not game:
            game_id = _clean(row.get("Game_ID"))
            if game_id:
                raise ValueError(f"No game found for Game_ID {game_id}.")
            raise ValueError("No game matched the provided Tournament_Code/Round/players/date fields.")
        _cross_check_game(row, game)

        existing_sgf_code = _clean(game.get("Sgf_Code"))
        replace_existing = _parse_bool(row.get("Replace_Existing", ""))
        if existing_sgf_code and not replace_existing:
            return RowPlan(
                row_number=row_number,
                csv_row=row,
                game=game,
                status="skipped",
                action="none",
                message=f"Game already has Sgf_Code '{existing_sgf_code}'. Set Replace_Existing=true to overwrite.",
                sgf_file=sgf_file,
            )

        desired_sgf_code = _clean(row.get("Sgf_Code")) or explorer.build_uploaded_sgf_code(int(game["Game_ID"]), game)
        action = "replace" if existing_sgf_code else "create"
        return RowPlan(
            row_number=row_number,
            csv_row=row,
            game=game,
            status="ready",
            action=action,
            message="Validated.",
            sgf_file=sgf_file,
            new_sgf_code=desired_sgf_code,
        )
    except Exception as exc:
        return RowPlan(
            row_number=row_number,
            csv_row=row,
            game=None,
            status="error",
            action="none",
            message=str(exc),
        )


def _plan_to_result(plan: RowPlan) -> dict[str, Any]:
    game = plan.game or {}
    return {
        "row_number": plan.row_number,
        "status": plan.status,
        "action": plan.action,
        "message": plan.message,
        "game_id": game.get("Game_ID"),
        "tournament_code": game.get("Tournament_Code"),
        "game_date": explorer.json_safe_value(game.get("Game_Date")),
        "round": game.get("Round"),
        "existing_sgf_code": game.get("Sgf_Code"),
        "new_sgf_code": plan.new_sgf_code,
        "sgf_file": str(plan.sgf_file) if plan.sgf_file else None,
    }


def _apply_plan(conn_str: str, plans: list[RowPlan]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for plan in plans:
        result = _plan_to_result(plan)
        if plan.status != "ready" or not plan.game or not plan.sgf_file or not plan.new_sgf_code:
            results.append(result)
            continue
        try:
            sgf_text = _read_sgf_text(plan.sgf_file)
            explorer.upload_sgf_blob(plan.new_sgf_code, sgf_text)
            explorer.update_game_sgf_code(conn_str, int(plan.game["Game_ID"]), plan.new_sgf_code)
            updated = explorer.load_game_for_sgf_upload(conn_str, int(plan.game["Game_ID"])) or plan.game
            result["status"] = "applied"
            result["message"] = "Uploaded SGF and updated ratings.games.Sgf_Code."
            result["existing_sgf_code"] = updated.get("Sgf_Code")
            result["new_sgf_code"] = updated.get("Sgf_Code")
        except Exception as exc:
            result["status"] = "error"
            result["message"] = str(exc)
        results.append(result)
    return results


def _default_report_path(csv_path: Path) -> Path:
    return csv_path.with_suffix(csv_path.suffix + ".results.json")


def _summarize(results: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for result in results:
        status = str(result.get("status") or "unknown")
        summary[status] = summary.get(status, 0) + 1
    return summary


def main() -> int:
    args = _parse_args()
    csv_path = Path(args.csv_file).resolve()
    if not csv_path.exists():
        raise SystemExit(f"CSV file was not found: {csv_path}")

    conn_str = explorer.get_sql_connection_string()
    if not conn_str:
        raise SystemExit("Missing SQL connection string. Check local.settings.json or environment variables.")

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames
    _validate_headers(fieldnames)
    rows = _read_manifest(csv_path)
    if not rows:
        raise SystemExit("CSV contained no data rows.")

    plans = [_build_plan_row(conn_str, csv_path, index, row) for index, row in enumerate(rows, start=2)]
    results = [_plan_to_result(plan) for plan in plans]
    if args.mode == "apply":
        results = _apply_plan(conn_str, plans)

    report_path = Path(args.report_file).resolve() if args.report_file else _default_report_path(csv_path)
    report_payload = {
        "mode": args.mode,
        "csv_file": str(csv_path),
        "recommended_columns": list(RECOMMENDED_COLUMNS),
        "results": results,
        "summary": _summarize(results),
    }
    report_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")

    print(json.dumps(report_payload["summary"], indent=2))
    print(f"Report written to {report_path}")
    return 0 if not any(item["status"] == "error" for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
