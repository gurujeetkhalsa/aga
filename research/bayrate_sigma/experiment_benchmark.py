"""Run and summarize BayRate rating experiments.

This module is intentionally read-only. It consumes local CSV snapshots, runs
the BayRate engine, and writes local artifacts for comparing algorithm changes.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
import time
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from bayrate.core import BayrateConfig, CsvValidationError, result_to_json, run_bayrate


SIGMA_THRESHOLDS = (0.15, 0.25, 0.5, 1.0)
CALIBRATION_BIN_COUNT = 10
RESPONSIVENESS_GAP_THRESHOLD = 1.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run BayRate against a local experiment dataset and summarize rating quality diagnostics."
    )
    parser.add_argument("--games", required=True, type=Path, help="Local games.csv export.")
    parser.add_argument("--ratings", required=True, type=Path, help="Local ratings.csv prior-state export.")
    parser.add_argument("--name", default="experiment", help="Experiment name used in output artifacts.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Artifact directory. Defaults to research/bayrate_sigma/output/experiments/<name>.",
    )
    parser.add_argument("--baseline-summary", type=Path, help="Optional baseline summary.json to compare against.")
    parser.add_argument("--config-json", type=Path, help="Optional BayrateConfig JSON, or best_config.json wrapper.")
    parser.add_argument("--write-full-result", action="store_true", help="Also write the full BayRate result JSON.")
    parser.add_argument("--allow-online-games", action="store_true", help="Include games marked as online.")
    parser.add_argument("--min-game-date", help="Earliest game date to include, YYYY-MM-DD.")
    parser.add_argument("--max-game-date", help="Latest game date to include, YYYY-MM-DD.")
    parser.add_argument("--max-events", type=int, help="Limit processing to the first N events.")
    parser.add_argument(
        "--inactivity-growth-per-day",
        type=float,
        default=None,
        help="Daily sigma growth for inactive players.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir or default_output_dir(args.name)
    config = load_config_from_json(args.config_json) if args.config_json else BayrateConfig()
    config.allow_online_games = args.allow_online_games
    config.max_events = args.max_events
    if args.inactivity_growth_per_day is not None:
        config.inactivity_growth_per_day = args.inactivity_growth_per_day
    if args.min_game_date:
        config.min_game_date = date.fromisoformat(args.min_game_date)
    if args.max_game_date:
        config.max_game_date = date.fromisoformat(args.max_game_date)

    try:
        artifact = run_experiment_benchmark(
            games_path=args.games,
            ratings_path=args.ratings,
            name=args.name,
            output_dir=output_dir,
            config=config,
            baseline_summary_path=args.baseline_summary,
            write_full_result=args.write_full_result,
        )
    except CsvValidationError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2) from exc
    except Exception as exc:
        print(f"Experiment benchmark failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print_benchmark_summary(artifact, sys.stdout)


def run_experiment_benchmark(
    *,
    games_path: Path,
    ratings_path: Path,
    name: str,
    output_dir: Path,
    config: BayrateConfig | None = None,
    baseline_summary_path: Path | None = None,
    write_full_result: bool = False,
) -> dict[str, Any]:
    start = time.perf_counter()
    result = run_bayrate(games_path, ratings_path, config or BayrateConfig())
    elapsed_seconds = time.perf_counter() - start

    output_dir.mkdir(parents=True, exist_ok=True)
    summary = summarize_result(
        result,
        name=name,
        games_path=games_path,
        ratings_path=ratings_path,
        elapsed_seconds=elapsed_seconds,
    )
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, default=_json_default) + "\n", encoding="utf-8")
    write_player_results_csv(output_dir / "player_results.csv", result.player_results)
    write_game_results_csv(output_dir / "game_results.csv", result.game_results)
    write_calibration_csv(output_dir / "calibration.csv", result.game_results)

    full_result_path = None
    if write_full_result:
        full_result_path = output_dir / "bayrate_result.json"
        full_result_path.write_text(result_to_json(result) + "\n", encoding="utf-8")

    comparison = None
    comparison_path = None
    if baseline_summary_path is not None:
        baseline = json.loads(baseline_summary_path.read_text(encoding="utf-8"))
        comparison = compare_summaries(baseline, summary)
        comparison_path = output_dir / "comparison_vs_baseline.json"
        comparison_path.write_text(json.dumps(comparison, indent=2, default=_json_default) + "\n", encoding="utf-8")

    return {
        "summary": summary,
        "comparison": comparison,
        "summary_path": str(summary_path),
        "comparison_path": str(comparison_path) if comparison_path else None,
        "full_result_path": str(full_result_path) if full_result_path else None,
        "output_dir": str(output_dir),
    }


def summarize_result(
    result: Any,
    *,
    name: str,
    games_path: Path,
    ratings_path: Path,
    elapsed_seconds: float,
) -> dict[str, Any]:
    player_rows = [asdict(row) for row in result.player_results]
    game_rows = [asdict(row) for row in result.game_results]
    rating_deltas = [
        row["rating_after"] - row["prior_rating"]
        for row in player_rows
        if row.get("prior_rating") is not None
    ]
    sigma_deltas = [
        row["sigma_after"] - row["prior_sigma"]
        for row in player_rows
        if row.get("prior_sigma") is not None
    ]
    sigma_after = [row["sigma_after"] for row in player_rows]
    responsiveness = responsiveness_summary(player_rows)
    calibration = calibration_bins(game_rows)

    return {
        "name": name,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "elapsed_seconds": elapsed_seconds,
        "inputs": {
            "games": str(games_path),
            "ratings": str(ratings_path),
        },
        "config": result.config,
        "event_count": result.event_count,
        "player_count": result.player_count,
        "player_result_count": len(player_rows),
        "game_result_count": len(game_rows),
        "pre_event_metrics": result.pre_event_metrics,
        "post_event_fit_metrics": result.post_event_fit_metrics,
        "rating_delta": signed_distribution(rating_deltas),
        "rating_abs_delta": distribution([abs(value) for value in rating_deltas]),
        "sigma_after": distribution(sigma_after),
        "sigma_delta": signed_distribution(sigma_deltas),
        "narrow_sigma_counts": {
            f"lt_{threshold:g}": sum(1 for value in sigma_after if value < threshold)
            for threshold in SIGMA_THRESHOLDS
        },
        "narrow_sigma_rates": {
            f"lt_{threshold:g}": _safe_divide(sum(1 for value in sigma_after if value < threshold), len(sigma_after))
            for threshold in SIGMA_THRESHOLDS
        },
        "responsiveness": responsiveness,
        "calibration": calibration,
    }


def responsiveness_summary(player_rows: list[dict[str, Any]]) -> dict[str, Any]:
    cases = []
    improving = []
    worsening = []
    for row in player_rows:
        prior_rating = row.get("prior_rating")
        performance_rating = row.get("performance_rating")
        if prior_rating is None or performance_rating is None:
            continue
        gap = performance_rating - prior_rating
        if abs(gap) < RESPONSIVENESS_GAP_THRESHOLD:
            continue
        rating_delta = row["rating_after"] - prior_rating
        capture_ratio = rating_delta / gap if gap else None
        case = {
            "player_id": row["player_id"],
            "tournament_code": row["tournament_code"],
            "event_date": row["event_date"],
            "prior_rating": prior_rating,
            "prior_sigma": row.get("prior_sigma"),
            "performance_rating": performance_rating,
            "rating_after": row["rating_after"],
            "sigma_after": row["sigma_after"],
            "performance_gap": gap,
            "rating_delta": rating_delta,
            "capture_ratio": capture_ratio,
            "unabsorbed_gap": gap - rating_delta,
        }
        cases.append(case)
        if gap > 0:
            improving.append(case)
        else:
            worsening.append(case)

    return {
        "gap_threshold": RESPONSIVENESS_GAP_THRESHOLD,
        "case_count": len(cases),
        "improving_count": len(improving),
        "worsening_count": len(worsening),
        "improving_capture_ratio": distribution(
            [case["capture_ratio"] for case in improving if case["capture_ratio"] is not None]
        ),
        "worsening_capture_ratio": distribution(
            [case["capture_ratio"] for case in worsening if case["capture_ratio"] is not None]
        ),
        "improving_rating_delta": signed_distribution([case["rating_delta"] for case in improving]),
        "largest_improving_underresponses": sorted(
            improving,
            key=lambda case: (case["unabsorbed_gap"], case["performance_gap"]),
            reverse=True,
        )[:10],
    }


def calibration_bins(game_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bins = [
        {
            "bin": index,
            "lower": index / CALIBRATION_BIN_COUNT,
            "upper": (index + 1) / CALIBRATION_BIN_COUNT,
            "count": 0,
            "predicted_sum": 0.0,
            "actual_sum": 0.0,
            "brier_sum": 0.0,
        }
        for index in range(CALIBRATION_BIN_COUNT)
    ]
    for row in game_rows:
        predicted = float(row["pre_event_expected_white"])
        actual = 1.0 if row["white_wins"] else 0.0
        index = min(CALIBRATION_BIN_COUNT - 1, max(0, int(predicted * CALIBRATION_BIN_COUNT)))
        bucket = bins[index]
        bucket["count"] += 1
        bucket["predicted_sum"] += predicted
        bucket["actual_sum"] += actual
        bucket["brier_sum"] += (predicted - actual) ** 2

    result = []
    for bucket in bins:
        count = bucket["count"]
        result.append(
            {
                "bin": bucket["bin"],
                "lower": bucket["lower"],
                "upper": bucket["upper"],
                "count": count,
                "average_predicted": _safe_divide(bucket["predicted_sum"], count),
                "actual_win_rate": _safe_divide(bucket["actual_sum"], count),
                "average_brier": _safe_divide(bucket["brier_sum"], count),
            }
        )
    return result


def compare_summaries(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    primary_paths = [
        ("pre_event_metrics", "accuracy"),
        ("pre_event_metrics", "average_log_loss"),
        ("pre_event_metrics", "average_brier"),
        ("sigma_after", "median"),
        ("sigma_after", "p10"),
        ("rating_abs_delta", "mean"),
        ("responsiveness", "improving_capture_ratio", "mean"),
    ]
    deltas = {}
    for path in primary_paths:
        baseline_value = nested_get(baseline, path)
        candidate_value = nested_get(candidate, path)
        deltas[".".join(path)] = {
            "baseline": baseline_value,
            "candidate": candidate_value,
            "delta": numeric_delta(candidate_value, baseline_value),
        }

    narrow_delta = {}
    for key, candidate_value in (candidate.get("narrow_sigma_counts") or {}).items():
        baseline_value = (baseline.get("narrow_sigma_counts") or {}).get(key)
        narrow_delta[key] = {
            "baseline": baseline_value,
            "candidate": candidate_value,
            "delta": numeric_delta(candidate_value, baseline_value),
        }

    return {
        "baseline_name": baseline.get("name"),
        "candidate_name": candidate.get("name"),
        "baseline_generated_at": baseline.get("generated_at"),
        "candidate_generated_at": candidate.get("generated_at"),
        "same_game_count": baseline.get("game_result_count") == candidate.get("game_result_count"),
        "same_event_count": baseline.get("event_count") == candidate.get("event_count"),
        "metric_deltas": deltas,
        "narrow_sigma_count_deltas": narrow_delta,
        "notes": [
            "For average_log_loss and average_brier, negative delta is better.",
            "For accuracy and improving_capture_ratio, positive delta is usually better.",
            "Post-event fit metrics are in-sample diagnostics, not the main model-quality score.",
        ],
    }


def distribution(values: Iterable[float | int | None]) -> dict[str, Any]:
    clean_values = sorted(float(value) for value in values if value is not None and math.isfinite(float(value)))
    if not clean_values:
        return {
            "count": 0,
            "min": None,
            "p10": None,
            "p25": None,
            "median": None,
            "p75": None,
            "p90": None,
            "max": None,
            "mean": None,
        }
    return {
        "count": len(clean_values),
        "min": clean_values[0],
        "p10": percentile(clean_values, 10),
        "p25": percentile(clean_values, 25),
        "median": statistics.median(clean_values),
        "p75": percentile(clean_values, 75),
        "p90": percentile(clean_values, 90),
        "max": clean_values[-1],
        "mean": math.fsum(clean_values) / len(clean_values),
    }


def signed_distribution(values: Iterable[float | int | None]) -> dict[str, Any]:
    clean_values = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    result = distribution(clean_values)
    result.update(
        {
            "negative_count": sum(1 for value in clean_values if value < 0.0),
            "positive_count": sum(1 for value in clean_values if value > 0.0),
            "zero_count": sum(1 for value in clean_values if value == 0.0),
        }
    )
    return result


def percentile(sorted_values: list[float], percentile_value: float) -> float:
    if not sorted_values:
        raise ValueError("percentile requires at least one value.")
    if len(sorted_values) == 1:
        return sorted_values[0]
    index = (len(sorted_values) - 1) * percentile_value / 100.0
    lower = int(math.floor(index))
    upper = int(math.ceil(index))
    if lower == upper:
        return sorted_values[lower]
    fraction = index - lower
    return sorted_values[lower] * (1.0 - fraction) + sorted_values[upper] * fraction


def write_player_results_csv(path: Path, player_results: Iterable[Any]) -> None:
    columns = [
        "player_id",
        "event_key",
        "event_date",
        "tournament_code",
        "rank_seed",
        "prior_rating",
        "prior_sigma",
        "performance_rating",
        "rating_after",
        "sigma_after",
        "rating_delta",
        "sigma_delta",
        "performance_gap",
        "capture_ratio",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for result in player_results:
            row = asdict(result)
            prior_rating = row.get("prior_rating")
            prior_sigma = row.get("prior_sigma")
            performance_rating = row.get("performance_rating")
            rating_delta = None if prior_rating is None else row["rating_after"] - prior_rating
            sigma_delta = None if prior_sigma is None else row["sigma_after"] - prior_sigma
            performance_gap = None
            capture_ratio = None
            if prior_rating is not None and performance_rating is not None:
                performance_gap = performance_rating - prior_rating
                if performance_gap:
                    capture_ratio = rating_delta / performance_gap
            row.update(
                {
                    "rating_delta": rating_delta,
                    "sigma_delta": sigma_delta,
                    "performance_gap": performance_gap,
                    "capture_ratio": capture_ratio,
                }
            )
            writer.writerow({column: _csv_value(row.get(column)) for column in columns})


def write_game_results_csv(path: Path, game_results: Iterable[Any]) -> None:
    columns = [
        "source_game_id",
        "event_key",
        "event_date",
        "tournament_code",
        "white_agaid",
        "black_agaid",
        "handicap",
        "komi",
        "white_wins",
        "white_seed_before",
        "black_seed_before",
        "pre_event_expected_white",
        "post_event_expected_white",
        "pre_event_brier",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for result in game_results:
            row = asdict(result)
            actual = 1.0 if row["white_wins"] else 0.0
            row["pre_event_brier"] = (row["pre_event_expected_white"] - actual) ** 2
            writer.writerow({column: _csv_value(row.get(column)) for column in columns})


def write_calibration_csv(path: Path, game_results: Iterable[Any]) -> None:
    rows = calibration_bins([asdict(result) for result in game_results])
    columns = ["bin", "lower", "upper", "count", "average_predicted", "actual_win_rate", "average_brier"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_value(row.get(column)) for column in columns})


def print_benchmark_summary(artifact: dict[str, Any], output: Any) -> None:
    summary = artifact["summary"]
    pre = summary["pre_event_metrics"]
    sigma = summary["sigma_after"]
    responsiveness = summary["responsiveness"]
    capture = responsiveness["improving_capture_ratio"]
    print("", file=output)
    print("BayRate Experiment Benchmark", file=output)
    print(f"  Name: {summary['name']}", file=output)
    print(f"  Output: {artifact['output_dir']}", file=output)
    print(f"  Events: {summary['event_count']}  Games: {summary['game_result_count']}", file=output)
    print(
        "  Pre-event quality: "
        f"accuracy={pre['accuracy']:.6f} "
        f"log_loss={pre['average_log_loss']:.6f} "
        f"brier={pre['average_brier']:.6f}",
        file=output,
    )
    print(
        "  Sigma after: "
        f"median={_fmt(sigma['median'])} "
        f"p10={_fmt(sigma['p10'])} "
        f"p90={_fmt(sigma['p90'])}",
        file=output,
    )
    print(
        "  Improving performance-gap capture: "
        f"cases={responsiveness['improving_count']} "
        f"mean={_fmt(capture['mean'])} "
        f"median={_fmt(capture['median'])}",
        file=output,
    )
    print(f"  Summary: {artifact['summary_path']}", file=output)
    if artifact.get("comparison_path"):
        comparison = artifact["comparison"]
        log_loss_delta = comparison["metric_deltas"]["pre_event_metrics.average_log_loss"]["delta"]
        brier_delta = comparison["metric_deltas"]["pre_event_metrics.average_brier"]["delta"]
        print(
            "  Vs baseline: "
            f"log_loss_delta={_fmt(log_loss_delta)} "
            f"brier_delta={_fmt(brier_delta)}",
            file=output,
        )
        print(f"  Comparison: {artifact['comparison_path']}", file=output)


def default_output_dir(name: str) -> Path:
    return Path(__file__).resolve().parent / "output" / "experiments" / safe_name(name)


def load_config_from_json(path: Path) -> BayrateConfig:
    payload = json.loads(path.read_text(encoding="utf-8"))
    values = payload.get("config", payload)
    if not isinstance(values, dict):
        raise ValueError(f"Config JSON must contain an object: {path}")
    allowed = set(BayrateConfig.__dataclass_fields__)
    return BayrateConfig(**{key: value for key, value in values.items() if key in allowed})


def safe_name(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in value.strip().lower())
    return cleaned.strip("-") or "experiment"


def nested_get(value: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = value
    for part in path:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def numeric_delta(candidate_value: Any, baseline_value: Any) -> float | None:
    if candidate_value is None or baseline_value is None:
        return None
    try:
        return float(candidate_value) - float(baseline_value)
    except (TypeError, ValueError):
        return None


def _safe_divide(numerator: float | int, denominator: float | int) -> float | None:
    if not denominator:
        return None
    return float(numerator) / float(denominator)


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.6f}"


def _csv_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if value is None:
        return ""
    return value


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


if __name__ == "__main__":
    main()
