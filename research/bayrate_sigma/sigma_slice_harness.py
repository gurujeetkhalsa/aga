"""Slice BayRate experiment results by player trajectory diagnostics.

The benchmark summary is intentionally aggregate. This module adds a second
view: label player-events from a fixed baseline chronology, then compare game
prediction quality and player movement inside those labels for any number of
candidate experiment directories.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Iterable


DEFAULT_LOOKAHEAD_EVENTS = 5
DEFAULT_TREND_THRESHOLD = 1.0
DEFAULT_STABLE_THRESHOLD = 0.25
DEFAULT_PERFORMANCE_THRESHOLD = 1.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build player-slice diagnostics from BayRate experiment output directories."
    )
    parser.add_argument("--baseline-dir", required=True, type=Path, help="Experiment output directory for baseline.")
    parser.add_argument(
        "--candidate",
        action="append",
        default=[],
        metavar="NAME=DIR",
        help="Candidate experiment directory to compare. May be repeated.",
    )
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory to write slice artifacts.")
    parser.add_argument("--lookahead-events", type=int, default=DEFAULT_LOOKAHEAD_EVENTS)
    parser.add_argument("--trend-threshold", type=float, default=DEFAULT_TREND_THRESHOLD)
    parser.add_argument("--stable-threshold", type=float, default=DEFAULT_STABLE_THRESHOLD)
    parser.add_argument("--performance-threshold", type=float, default=DEFAULT_PERFORMANCE_THRESHOLD)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    candidates = [_parse_candidate(value) for value in args.candidate]
    artifact = build_slice_harness(
        baseline_dir=args.baseline_dir,
        candidates=candidates,
        output_dir=args.output_dir,
        lookahead_events=args.lookahead_events,
        trend_threshold=args.trend_threshold,
        stable_threshold=args.stable_threshold,
        performance_threshold=args.performance_threshold,
    )
    print("BayRate Sigma Slice Harness")
    print(f"  Output: {artifact['output_dir']}")
    print(f"  Game slice metrics: {artifact['game_metrics_path']}")
    print(f"  Player slice metrics: {artifact['player_metrics_path']}")
    if artifact.get("game_comparison_path"):
        print(f"  Game slice comparison: {artifact['game_comparison_path']}")
    if artifact.get("player_comparison_path"):
        print(f"  Player slice comparison: {artifact['player_comparison_path']}")


def build_slice_harness(
    *,
    baseline_dir: Path,
    candidates: list[tuple[str, Path]],
    output_dir: Path,
    lookahead_events: int = DEFAULT_LOOKAHEAD_EVENTS,
    trend_threshold: float = DEFAULT_TREND_THRESHOLD,
    stable_threshold: float = DEFAULT_STABLE_THRESHOLD,
    performance_threshold: float = DEFAULT_PERFORMANCE_THRESHOLD,
) -> dict[str, Any]:
    baseline_players = read_csv_rows(baseline_dir / "player_results.csv")
    labels = label_player_events(
        baseline_players,
        lookahead_events=lookahead_events,
        trend_threshold=trend_threshold,
        stable_threshold=stable_threshold,
        performance_threshold=performance_threshold,
    )

    experiments = [("baseline", baseline_dir), *candidates]
    game_rows: list[dict[str, Any]] = []
    player_rows: list[dict[str, Any]] = []
    for name, directory in experiments:
        game_rows.extend(
            summarize_game_slices(
                experiment_name=name,
                game_rows=read_csv_rows(directory / "game_results.csv"),
                labels=labels,
            )
        )
        player_rows.extend(
            summarize_player_slices(
                experiment_name=name,
                player_rows=read_csv_rows(directory / "player_results.csv"),
                labels=labels,
            )
        )

    game_comparison = compare_slice_metrics(game_rows, baseline_name="baseline")
    player_comparison = compare_slice_metrics(player_rows, baseline_name="baseline")

    output_dir.mkdir(parents=True, exist_ok=True)
    game_metrics_path = output_dir / "player_slice_game_metrics.csv"
    player_metrics_path = output_dir / "player_slice_event_metrics.csv"
    game_comparison_path = output_dir / "player_slice_game_comparison.csv"
    player_comparison_path = output_dir / "player_slice_event_comparison.csv"
    metadata_path = output_dir / "slice_metadata.json"

    write_csv(game_metrics_path, game_rows, GAME_METRIC_FIELDS)
    write_csv(player_metrics_path, player_rows, PLAYER_METRIC_FIELDS)
    write_csv(game_comparison_path, game_comparison, COMPARISON_FIELDS)
    write_csv(player_comparison_path, player_comparison, COMPARISON_FIELDS)
    metadata = {
        "baseline_dir": str(baseline_dir),
        "candidates": [{"name": name, "dir": str(directory)} for name, directory in candidates],
        "lookahead_events": lookahead_events,
        "trend_threshold": trend_threshold,
        "stable_threshold": stable_threshold,
        "performance_threshold": performance_threshold,
        "label_count": len(labels),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    return {
        "output_dir": str(output_dir),
        "game_metrics_path": str(game_metrics_path),
        "player_metrics_path": str(player_metrics_path),
        "game_comparison_path": str(game_comparison_path),
        "player_comparison_path": str(player_comparison_path),
        "metadata_path": str(metadata_path),
    }


def label_player_events(
    player_rows: list[dict[str, str]],
    *,
    lookahead_events: int = DEFAULT_LOOKAHEAD_EVENTS,
    trend_threshold: float = DEFAULT_TREND_THRESHOLD,
    stable_threshold: float = DEFAULT_STABLE_THRESHOLD,
    performance_threshold: float = DEFAULT_PERFORMANCE_THRESHOLD,
) -> dict[tuple[str, str], dict[str, Any]]:
    rows_by_player: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in player_rows:
        rows_by_player[str(row["player_id"])].append(row)

    labels: dict[tuple[str, str], dict[str, Any]] = {}
    for player_id, rows in rows_by_player.items():
        rows.sort(key=lambda row: (parse_date(row["event_date"]), row["event_key"]))
        previous_date: date | None = None
        for index, row in enumerate(rows):
            event_date = parse_date(row["event_date"])
            future_index = min(len(rows) - 1, index + lookahead_events)
            future_count = future_index - index
            current_rating = parse_float(row.get("rating_after"))
            future_rating = parse_float(rows[future_index].get("rating_after")) if future_count else None
            future_delta = (
                None if current_rating is None or future_rating is None else future_rating - current_rating
            )
            prior_rating = parse_float(row.get("prior_rating"))
            prior_sigma = parse_float(row.get("prior_sigma"))
            performance_gap = parse_float(row.get("performance_gap"))
            days_inactive = None if previous_date is None else max(0, (event_date - previous_date).days)
            previous_date = event_date

            event_key = str(row["event_key"])
            labels[(player_id, event_key)] = {
                "player_id": player_id,
                "event_key": event_key,
                "future_event_count": future_count,
                "future_delta": future_delta,
                "future_trend": label_future_trend(
                    future_delta,
                    future_count=future_count,
                    trend_threshold=trend_threshold,
                    stable_threshold=stable_threshold,
                ),
                "event_performance": label_event_performance(
                    performance_gap,
                    performance_threshold=performance_threshold,
                    stable_threshold=stable_threshold,
                ),
                "activity": label_activity(days_inactive),
                "prior_sigma_band": label_prior_sigma(prior_sigma),
                "prior_rating_band": label_rating(prior_rating),
                "days_inactive": days_inactive,
            }
    return labels


def summarize_game_slices(
    *,
    experiment_name: str,
    game_rows: list[dict[str, str]],
    labels: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], dict[str, Any]] = defaultdict(_game_bucket)
    for row in game_rows:
        expected_white = parse_float(row.get("pre_event_expected_white"))
        if expected_white is None:
            continue
        white_wins = parse_bool(row.get("white_wins"))
        players = [
            (row["white_agaid"], expected_white, 1.0 if white_wins else 0.0),
            (row["black_agaid"], 1.0 - expected_white, 0.0 if white_wins else 1.0),
        ]
        for player_id, predicted, actual in players:
            label = labels.get((str(player_id), str(row["event_key"])))
            if label is None:
                continue
            for slice_type in SLICE_TYPES:
                add_game_metric(
                    buckets[(slice_type, label[slice_type])],
                    predicted=predicted,
                    actual=actual,
                    player_event_key=(str(player_id), str(row["event_key"])),
                )

    result = []
    for (slice_type, slice_value), bucket in buckets.items():
        count = bucket["player_games"]
        result.append(
            {
                "experiment": experiment_name,
                "slice_type": slice_type,
                "slice": slice_value,
                "player_games": count,
                "player_events": len(bucket["player_events"]),
                "accuracy": safe_divide(bucket["correct"], count),
                "log_loss": safe_divide(bucket["log_loss_sum"], count),
                "brier": safe_divide(bucket["brier_sum"], count),
                "mean_residual": safe_divide(bucket["residual_sum"], count),
                "mean_predicted_win": safe_divide(bucket["predicted_sum"], count),
                "actual_win_rate": safe_divide(bucket["actual_sum"], count),
            }
        )
    return sorted(result, key=slice_sort_key)


def summarize_player_slices(
    *,
    experiment_name: str,
    player_rows: list[dict[str, str]],
    labels: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], dict[str, list[Any]]] = defaultdict(_player_bucket)
    for row in player_rows:
        label = labels.get((str(row["player_id"]), str(row["event_key"])))
        if label is None:
            continue
        for slice_type in SLICE_TYPES:
            bucket = buckets[(slice_type, label[slice_type])]
            bucket["player_events"].append((row["player_id"], row["event_key"]))
            bucket["future_delta"].append(label["future_delta"])
            bucket["days_inactive"].append(label["days_inactive"])
            bucket["sigma_after"].append(parse_float(row.get("sigma_after")))
            rating_delta = parse_float(row.get("rating_delta"))
            bucket["abs_rating_delta"].append(None if rating_delta is None else abs(rating_delta))
            bucket["capture_ratio"].append(parse_float(row.get("capture_ratio")))
            performance_gap = parse_float(row.get("performance_gap"))
            if performance_gap is not None and performance_gap >= DEFAULT_PERFORMANCE_THRESHOLD:
                bucket["improving_capture_ratio"].append(parse_float(row.get("capture_ratio")))

    result = []
    for (slice_type, slice_value), bucket in buckets.items():
        result.append(
            {
                "experiment": experiment_name,
                "slice_type": slice_type,
                "slice": slice_value,
                "player_events": len(bucket["player_events"]),
                "future_delta_mean": mean(bucket["future_delta"]),
                "days_inactive_mean": mean(bucket["days_inactive"]),
                "sigma_after_p10": percentile(bucket["sigma_after"], 10),
                "sigma_after_median": median(bucket["sigma_after"]),
                "sigma_after_mean": mean(bucket["sigma_after"]),
                "abs_rating_delta_mean": mean(bucket["abs_rating_delta"]),
                "capture_ratio_mean": mean(bucket["capture_ratio"]),
                "improving_capture_ratio_mean": mean(bucket["improving_capture_ratio"]),
            }
        )
    return sorted(result, key=slice_sort_key)


def compare_slice_metrics(rows: list[dict[str, Any]], *, baseline_name: str) -> list[dict[str, Any]]:
    baseline_by_key = {
        (row["slice_type"], row["slice"]): row
        for row in rows
        if row["experiment"] == baseline_name
    }
    result = []
    for row in rows:
        if row["experiment"] == baseline_name:
            continue
        baseline = baseline_by_key.get((row["slice_type"], row["slice"]))
        if baseline is None:
            continue
        for metric in comparable_metrics(row, baseline):
            result.append(
                {
                    "experiment": row["experiment"],
                    "slice_type": row["slice_type"],
                    "slice": row["slice"],
                    "metric": metric,
                    "baseline": baseline.get(metric),
                    "candidate": row.get(metric),
                    "delta": numeric_delta(row.get(metric), baseline.get(metric)),
                }
            )
    return sorted(result, key=lambda row: (row["experiment"], row["slice_type"], row["slice"], row["metric"]))


def comparable_metrics(row: dict[str, Any], baseline: dict[str, Any]) -> list[str]:
    return [
        key
        for key, value in row.items()
        if key not in {"experiment", "slice_type", "slice"}
        and key in baseline
        and isinstance(value, (int, float))
        and isinstance(baseline.get(key), (int, float))
    ]


def add_game_metric(bucket: dict[str, Any], *, predicted: float, actual: float, player_event_key: tuple[str, str]) -> None:
    clipped = min(max(predicted, 1e-12), 1.0 - 1e-12)
    bucket["player_games"] += 1
    bucket["player_events"].add(player_event_key)
    bucket["correct"] += int((predicted >= 0.5) == (actual >= 0.5))
    bucket["log_loss_sum"] += -(actual * math.log(clipped) + (1.0 - actual) * math.log(1.0 - clipped))
    bucket["brier_sum"] += (predicted - actual) ** 2
    bucket["residual_sum"] += actual - predicted
    bucket["predicted_sum"] += predicted
    bucket["actual_sum"] += actual


def label_future_trend(
    future_delta: float | None,
    *,
    future_count: int,
    trend_threshold: float,
    stable_threshold: float,
) -> str:
    if future_count <= 0 or future_delta is None:
        return "no_future"
    if future_delta >= trend_threshold:
        return "future_improver"
    if future_delta <= -trend_threshold:
        return "future_decliner"
    if abs(future_delta) <= stable_threshold:
        return "future_stable"
    return "future_mixed"


def label_event_performance(
    performance_gap: float | None,
    *,
    performance_threshold: float,
    stable_threshold: float,
) -> str:
    if performance_gap is None:
        return "no_performance"
    if performance_gap >= performance_threshold:
        return "event_overperformer"
    if performance_gap <= -performance_threshold:
        return "event_underperformer"
    if abs(performance_gap) <= stable_threshold:
        return "event_near_expected"
    return "event_mixed"


def label_activity(days_inactive: int | None) -> str:
    if days_inactive is None:
        return "first_event"
    if days_inactive == 0:
        return "same_day"
    if days_inactive < 30:
        return "<30d"
    if days_inactive < 180:
        return "30-180d"
    if days_inactive < 365:
        return "180-365d"
    if days_inactive < 730:
        return "1-2y"
    return "2y+"


def label_prior_sigma(value: float | None) -> str:
    if value is None:
        return "new/no_prior"
    if value < 0.25:
        return "<0.25"
    if value < 0.50:
        return "0.25-0.50"
    if value < 0.70:
        return "0.50-0.70"
    if value < 1.00:
        return "0.70-1.00"
    return ">=1.00"


def label_rating(value: float | None) -> str:
    if value is None:
        return "new/no_prior"
    if value <= -10:
        return "<=-10"
    if value <= -5:
        return "-10_to_-5"
    if value <= 0:
        return "-5_to_0"
    if value <= 3:
        return "0_to_3"
    if value <= 6:
        return "3_to_6"
    return ">6"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: csv_value(row.get(field)) for field in fields})


def parse_date(value: str) -> date:
    return date.fromisoformat(value[:10])


def parse_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    result = float(text)
    return result if math.isfinite(result) else None


def parse_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def mean(values: Iterable[Any]) -> float | None:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return math.fsum(clean) / len(clean) if clean else None


def median(values: Iterable[Any]) -> float | None:
    clean = sorted(float(value) for value in values if value is not None and math.isfinite(float(value)))
    return statistics.median(clean) if clean else None


def percentile(values: Iterable[Any], percentile_value: float) -> float | None:
    clean = sorted(float(value) for value in values if value is not None and math.isfinite(float(value)))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    index = (len(clean) - 1) * percentile_value / 100.0
    lower = int(math.floor(index))
    upper = int(math.ceil(index))
    if lower == upper:
        return clean[lower]
    fraction = index - lower
    return clean[lower] * (1.0 - fraction) + clean[upper] * fraction


def safe_divide(numerator: float | int, denominator: float | int) -> float | None:
    if not denominator:
        return None
    return float(numerator) / float(denominator)


def numeric_delta(candidate_value: Any, baseline_value: Any) -> float | None:
    if candidate_value is None or baseline_value is None:
        return None
    return float(candidate_value) - float(baseline_value)


def csv_value(value: Any) -> Any:
    if value is None:
        return ""
    return value


def _parse_candidate(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise SystemExit(f"Candidate must be NAME=DIR, got {value!r}.")
    name, directory = value.split("=", 1)
    if not name.strip() or not directory.strip():
        raise SystemExit(f"Candidate must be NAME=DIR, got {value!r}.")
    return name.strip(), Path(directory)


def _game_bucket() -> dict[str, Any]:
    return {
        "player_games": 0,
        "player_events": set(),
        "correct": 0,
        "log_loss_sum": 0.0,
        "brier_sum": 0.0,
        "residual_sum": 0.0,
        "predicted_sum": 0.0,
        "actual_sum": 0.0,
    }


def _player_bucket() -> dict[str, list[Any]]:
    return {
        "player_events": [],
        "future_delta": [],
        "days_inactive": [],
        "sigma_after": [],
        "abs_rating_delta": [],
        "capture_ratio": [],
        "improving_capture_ratio": [],
    }


def slice_sort_key(row: dict[str, Any]) -> tuple[str, int, str]:
    return (row["slice_type"], SLICE_ORDER.get((row["slice_type"], row["slice"]), 999), row["slice"])


SLICE_TYPES = [
    "future_trend",
    "event_performance",
    "activity",
    "prior_sigma_band",
    "prior_rating_band",
]

SLICE_ORDER = {
    ("future_trend", "future_improver"): 0,
    ("future_trend", "future_stable"): 1,
    ("future_trend", "future_mixed"): 2,
    ("future_trend", "future_decliner"): 3,
    ("future_trend", "no_future"): 4,
    ("event_performance", "event_overperformer"): 0,
    ("event_performance", "event_near_expected"): 1,
    ("event_performance", "event_mixed"): 2,
    ("event_performance", "event_underperformer"): 3,
    ("event_performance", "no_performance"): 4,
    ("activity", "first_event"): 0,
    ("activity", "same_day"): 1,
    ("activity", "<30d"): 2,
    ("activity", "30-180d"): 3,
    ("activity", "180-365d"): 4,
    ("activity", "1-2y"): 5,
    ("activity", "2y+"): 6,
    ("prior_sigma_band", "<0.25"): 0,
    ("prior_sigma_band", "0.25-0.50"): 1,
    ("prior_sigma_band", "0.50-0.70"): 2,
    ("prior_sigma_band", "0.70-1.00"): 3,
    ("prior_sigma_band", ">=1.00"): 4,
    ("prior_sigma_band", "new/no_prior"): 5,
    ("prior_rating_band", "<=-10"): 0,
    ("prior_rating_band", "-10_to_-5"): 1,
    ("prior_rating_band", "-5_to_0"): 2,
    ("prior_rating_band", "0_to_3"): 3,
    ("prior_rating_band", "3_to_6"): 4,
    ("prior_rating_band", ">6"): 5,
    ("prior_rating_band", "new/no_prior"): 6,
}

GAME_METRIC_FIELDS = [
    "experiment",
    "slice_type",
    "slice",
    "player_games",
    "player_events",
    "accuracy",
    "log_loss",
    "brier",
    "mean_residual",
    "mean_predicted_win",
    "actual_win_rate",
]

PLAYER_METRIC_FIELDS = [
    "experiment",
    "slice_type",
    "slice",
    "player_events",
    "future_delta_mean",
    "days_inactive_mean",
    "sigma_after_p10",
    "sigma_after_median",
    "sigma_after_mean",
    "abs_rating_delta_mean",
    "capture_ratio_mean",
    "improving_capture_ratio_mean",
]

COMPARISON_FIELDS = [
    "experiment",
    "slice_type",
    "slice",
    "metric",
    "baseline",
    "candidate",
    "delta",
]


if __name__ == "__main__":
    main()
