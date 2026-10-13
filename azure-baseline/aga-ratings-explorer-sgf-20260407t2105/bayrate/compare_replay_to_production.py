import argparse
import json
import math
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, TextIO

from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string
from bayrate.stage_reports import StageSqlAdapter, _coerce_date, _coerce_float, _coerce_int


PRODUCTION_RATINGS_FOR_TOURNAMENTS_SQL_TEMPLATE = """
SELECT
    r.[id],
    r.[Pin_Player],
    r.[Rating],
    r.[Sigma],
    r.[Elab_Date],
    r.[Tournament_Code]
FROM [ratings].[ratings] AS r
WHERE r.[Tournament_Code] IN ({placeholders})
ORDER BY r.[Tournament_Code], r.[id], r.[Pin_Player]
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare a read-only BayRate replay artifact to production ratings rows.")
    parser.add_argument("artifact", type=Path, help="Replay artifact JSON from bayrate.replay_staged_run.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    parser.add_argument("--output", type=Path, help="Optional JSON comparison artifact path.")
    parser.add_argument("--top", type=int, default=8, help="Number of largest rating deltas to print per tournament.")
    parser.add_argument("--summary-only", action="store_true", help="Print overall summary and worst tournaments instead of every tournament.")
    parser.add_argument("--top-tournaments", type=int, default=10, help="Number of worst tournaments to print with --summary-only.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        raise SystemExit(1)
    try:
        comparison = compare_replay_artifact_to_production(
            SqlAdapter(conn_str),
            args.artifact,
            output_path=args.output,
            top=args.top,
        )
        if args.summary_only:
            print_comparison_rollup(comparison, sys.stdout, top_tournaments=args.top_tournaments)
        else:
            print_comparison_summary(comparison, sys.stdout)
    except Exception as exc:
        print(f"Replay comparison failed: {exc}", file=sys.stderr)
        raise SystemExit(1)


def compare_replay_artifact_to_production(
    adapter: StageSqlAdapter,
    artifact_path: Path,
    *,
    output_path: Path | None = None,
    top: int = 8,
) -> dict[str, Any]:
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    tournament_codes = [
        str(event["tournament_code"])
        for event in artifact.get("plan", {}).get("events", [])
        if event.get("tournament_code")
    ]
    if not tournament_codes:
        raise ValueError("Replay artifact does not contain any planned tournament events.")

    production_rows = load_production_rating_rows(adapter, tournament_codes)
    replay_by_key = {
        (str(row.get("tournament_code")), _coerce_int(row.get("player_id"))): row
        for row in artifact.get("bayrate_result", {}).get("player_results", [])
        if row.get("tournament_code") and _coerce_int(row.get("player_id")) is not None
    }
    production_by_key = {
        (str(row.get("Tournament_Code")), _coerce_int(row.get("Pin_Player"))): row
        for row in production_rows
        if row.get("Tournament_Code") and _coerce_int(row.get("Pin_Player")) is not None
    }

    tournaments = []
    for code in tournament_codes:
        tournaments.append(
            compare_tournament(
                code,
                replay_by_key,
                production_by_key,
                top=top,
            )
        )

    comparison = {
        "artifact_path": str(artifact_path),
        "run_id": artifact.get("plan", {}).get("run_id"),
        "compared_tournament_codes": tournament_codes,
        "tournaments": tournaments,
        "overall": summarize_deltas([row for tournament in tournaments for row in tournament["matched_rows"]]),
    }
    path = output_path or default_output_path(artifact_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(comparison, indent=2, default=_json_default) + "\n", encoding="utf-8")
    comparison["output_path"] = str(path)
    return comparison


def load_production_rating_rows(adapter: StageSqlAdapter, tournament_codes: list[str]) -> list[dict[str, Any]]:
    placeholders = ", ".join("?" for _ in tournament_codes)
    query = PRODUCTION_RATINGS_FOR_TOURNAMENTS_SQL_TEMPLATE.format(placeholders=placeholders)
    return adapter.query_rows(query, tuple(tournament_codes))


def compare_tournament(
    tournament_code: str,
    replay_by_key: dict[tuple[str, int | None], dict[str, Any]],
    production_by_key: dict[tuple[str, int | None], dict[str, Any]],
    *,
    top: int,
) -> dict[str, Any]:
    replay_keys = {key for key in replay_by_key if key[0] == tournament_code}
    production_keys = {key for key in production_by_key if key[0] == tournament_code}
    matched_keys = sorted(replay_keys & production_keys, key=lambda key: key[1] or 0)
    matched_rows = [
        compare_player_row(replay_by_key[key], production_by_key[key])
        for key in matched_keys
    ]
    matched_rows.sort(key=lambda row: row["abs_rating_delta"], reverse=True)
    return {
        "tournament_code": tournament_code,
        "replay_player_count": len(replay_keys),
        "production_player_count": len(production_keys),
        "matched_count": len(matched_keys),
        "replay_only_player_ids": sorted(key[1] for key in replay_keys - production_keys if key[1] is not None),
        "production_only_player_ids": sorted(key[1] for key in production_keys - replay_keys if key[1] is not None),
        **summarize_deltas(matched_rows),
        "largest_rating_deltas": matched_rows[:top],
        "matched_rows": matched_rows,
    }


def compare_player_row(replay_row: dict[str, Any], production_row: dict[str, Any]) -> dict[str, Any]:
    replay_rating = _require_float(replay_row.get("rating_after"), "rating_after")
    production_rating = _require_float(production_row.get("Rating"), "Rating")
    replay_sigma = _require_float(replay_row.get("sigma_after"), "sigma_after")
    production_sigma = _require_float(production_row.get("Sigma"), "Sigma")
    rating_delta = replay_rating - production_rating
    sigma_delta = replay_sigma - production_sigma
    return {
        "tournament_code": replay_row.get("tournament_code"),
        "player_id": _coerce_int(replay_row.get("player_id")),
        "event_date": _coerce_date(replay_row.get("event_date")),
        "production_row_id": _coerce_int(production_row.get("id")),
        "production_elab_date": _coerce_date(production_row.get("Elab_Date")),
        "replay_rating": replay_rating,
        "production_rating": production_rating,
        "rating_delta": rating_delta,
        "abs_rating_delta": abs(rating_delta),
        "replay_sigma": replay_sigma,
        "production_sigma": production_sigma,
        "sigma_delta": sigma_delta,
        "abs_sigma_delta": abs(sigma_delta),
    }


def summarize_deltas(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "average_abs_rating_delta": None,
            "max_abs_rating_delta": None,
            "max_abs_rating_delta_player_id": None,
            "average_abs_sigma_delta": None,
            "max_abs_sigma_delta": None,
            "max_abs_sigma_delta_player_id": None,
        }
    max_rating = max(rows, key=lambda row: row["abs_rating_delta"])
    max_sigma = max(rows, key=lambda row: row["abs_sigma_delta"])
    return {
        "average_abs_rating_delta": math.fsum(row["abs_rating_delta"] for row in rows) / len(rows),
        "max_abs_rating_delta": max_rating["abs_rating_delta"],
        "max_abs_rating_delta_player_id": max_rating["player_id"],
        "average_abs_sigma_delta": math.fsum(row["abs_sigma_delta"] for row in rows) / len(rows),
        "max_abs_sigma_delta": max_sigma["abs_sigma_delta"],
        "max_abs_sigma_delta_player_id": max_sigma["player_id"],
    }


def print_comparison_summary(comparison: dict[str, Any], output: TextIO) -> None:
    print("", file=output)
    print("Replay vs Production Ratings", file=output)
    print(f"  RunID: {comparison.get('run_id')}", file=output)
    print(f"  Output: {comparison.get('output_path')}", file=output)
    for tournament in comparison.get("tournaments") or []:
        print("", file=output)
        print(f"  {tournament['tournament_code']}", file=output)
        print(
            f"    matched {tournament['matched_count']} of "
            f"{tournament['replay_player_count']} replay / {tournament['production_player_count']} production players",
            file=output,
        )
        print(
            f"    rating delta avg_abs={_fmt(tournament['average_abs_rating_delta'])} "
            f"max_abs={_fmt(tournament['max_abs_rating_delta'])} "
            f"player={tournament['max_abs_rating_delta_player_id']}",
            file=output,
        )
        print(
            f"    sigma delta  avg_abs={_fmt(tournament['average_abs_sigma_delta'])} "
            f"max_abs={_fmt(tournament['max_abs_sigma_delta'])} "
            f"player={tournament['max_abs_sigma_delta_player_id']}",
            file=output,
        )
        if tournament["replay_only_player_ids"] or tournament["production_only_player_ids"]:
            print(f"    replay-only players: {tournament['replay_only_player_ids']}", file=output)
            print(f"    production-only players: {tournament['production_only_player_ids']}", file=output)
        print("    largest rating deltas:", file=output)
        for row in tournament.get("largest_rating_deltas") or []:
            print(
                f"      {row['player_id']}: replay={row['replay_rating']:.6f} "
                f"prod={row['production_rating']:.6f} "
                f"delta={row['rating_delta']:+.6f} "
                f"sigma_delta={row['sigma_delta']:+.6f}",
                file=output,
            )


def print_comparison_rollup(comparison: dict[str, Any], output: TextIO, *, top_tournaments: int = 10) -> None:
    overall = comparison.get("overall") or {}
    print("", file=output)
    print("Replay vs Production Ratings", file=output)
    print(f"  RunID: {comparison.get('run_id')}", file=output)
    print(f"  Output: {comparison.get('output_path')}", file=output)
    print(f"  Tournaments: {len(comparison.get('tournaments') or [])}", file=output)
    print(
        f"  Overall rating delta avg_abs={_fmt(overall.get('average_abs_rating_delta'))} "
        f"max_abs={_fmt(overall.get('max_abs_rating_delta'))} "
        f"player={overall.get('max_abs_rating_delta_player_id')}",
        file=output,
    )
    print(
        f"  Overall sigma delta  avg_abs={_fmt(overall.get('average_abs_sigma_delta'))} "
        f"max_abs={_fmt(overall.get('max_abs_sigma_delta'))} "
        f"player={overall.get('max_abs_sigma_delta_player_id')}",
        file=output,
    )
    tournaments = sorted(
        comparison.get("tournaments") or [],
        key=lambda row: float(row.get("max_abs_rating_delta") or 0.0),
        reverse=True,
    )
    print("", file=output)
    print(f"  Worst tournaments by max absolute rating delta:", file=output)
    for tournament in tournaments[:top_tournaments]:
        print(
            f"    {tournament['tournament_code']}: "
            f"matched={tournament['matched_count']} "
            f"avg_abs={_fmt(tournament['average_abs_rating_delta'])} "
            f"max_abs={_fmt(tournament['max_abs_rating_delta'])} "
            f"player={tournament['max_abs_rating_delta_player_id']}",
            file=output,
        )


def default_output_path(artifact_path: Path) -> Path:
    return artifact_path.with_name(f"{artifact_path.stem}_production_compare.json")


def _require_float(value: Any, column: str) -> float:
    parsed = _coerce_float(value)
    if parsed is None:
        raise ValueError(f"{column} is required for replay comparison.")
    return parsed


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.6f}"


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


if __name__ == "__main__":
    main()
