"""Run repeated paired improver simulations and summarize seed variability."""

from __future__ import annotations

import argparse
import csv
import json
import random
import statistics
import time
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from bayrate.core import BayrateConfig, load_games_from_csv
from research.bayrate_sigma.simulate_improver_catchup import (
    DEFAULT_DATASET,
    DEFAULT_OUTPUT_DIR,
    build_encounter_templates,
    category_summaries,
    expand_simulated_players,
    load_latest_ratings,
    parse_activity_levels,
    run_algorithms,
    sample_base_players,
    simulate_games,
    summarize_player_outcomes,
)


DEFAULT_SWEEP_OUTPUT_DIR = DEFAULT_OUTPUT_DIR / "seed_sweeps"
BASELINE_ALGORITHM = "baseline"
CANDIDATE_ALGORITHM = "surprise_taper_floor_050"


def parse_args() -> argparse.Namespace:
    """Parse args."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--games", type=Path, default=DEFAULT_DATASET / "games.csv")
    parser.add_argument("--ratings", type=Path, default=DEFAULT_DATASET / "ratings.csv")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_SWEEP_OUTPUT_DIR)
    parser.add_argument("--name", default="one-rank-improver-100-seeds")
    parser.add_argument("--seed-start", type=int, default=2026060100)
    parser.add_argument("--seed-count", type=int, default=100)
    parser.add_argument("--start-date", type=lambda value: date.fromisoformat(value), default=date(2026, 6, 1))
    parser.add_argument("--years", type=float, default=3.0)
    parser.add_argument("--players-per-cell", type=int, default=4)
    parser.add_argument("--replications", type=int, default=4)
    parser.add_argument("--self-promote-rate", type=float, default=0.5)
    parser.add_argument(
        "--true-strength-closed-delta",
        type=float,
        default=1.0,
        help="Hidden true-strength shift in closed rating scale ranks.",
    )
    parser.add_argument(
        "--self-promote-closed-delta",
        type=float,
        help="Entry-rank shift for self-promoted players. Defaults to --true-strength-closed-delta.",
    )
    parser.add_argument(
        "--activity-levels",
        default="low:8,medium:24,high:50",
        help="Comma-separated label:games_per_year values.",
    )
    parser.add_argument("--allow-online-games", action="store_true")
    parser.add_argument("--max-players", type=int, help="Optional cap after stratified sampling.")
    return parser.parse_args()


def main() -> None:
    """Run the command-line entry point for this module."""
    args = parse_args()
    if args.seed_count <= 0:
        raise SystemExit("--seed-count must be positive")

    started = time.perf_counter()
    output_dir = args.output_dir / args.name
    output_dir.mkdir(parents=True, exist_ok=True)
    progress_path = output_dir / "progress.json"

    activity_levels = parse_activity_levels(args.activity_levels)
    self_promote_closed_delta = (
        args.self_promote_closed_delta
        if args.self_promote_closed_delta is not None
        else args.true_strength_closed_delta
    )

    latest_ratings = load_latest_ratings(args.ratings)
    source_games = load_games_from_csv(
        args.games,
        BayrateConfig(allow_online_games=args.allow_online_games),
    )
    templates_by_band = build_encounter_templates(source_games)

    seed_summary_rows: list[dict[str, object]] = []
    slice_summary_rows: list[dict[str, object]] = []
    comparison_rows: list[dict[str, object]] = []

    metadata = {
        "name": args.name,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "inputs": {"games": str(args.games), "ratings": str(args.ratings)},
        "seed_start": args.seed_start,
        "seed_count": args.seed_count,
        "start_date": args.start_date.isoformat(),
        "years": args.years,
        "players_per_cell": args.players_per_cell,
        "replications": args.replications,
        "self_promote_rate": args.self_promote_rate,
        "true_strength_closed_delta": args.true_strength_closed_delta,
        "self_promote_closed_delta": self_promote_closed_delta,
        "activity_levels": activity_levels,
        "allow_online_games": args.allow_online_games,
        "max_players": args.max_players,
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8")

    write_progress(
        progress_path,
        status="running",
        completed_seeds=0,
        seed_count=args.seed_count,
        elapsed_seconds=time.perf_counter() - started,
    )

    for offset in range(args.seed_count):
        seed = args.seed_start + offset
        seed_started = time.perf_counter()
        print(f"[{offset + 1}/{args.seed_count}] seed {seed}...", flush=True)
        seed_rows = run_seed(
            seed=seed,
            latest_ratings=latest_ratings,
            templates_by_band=templates_by_band,
            players_per_cell=args.players_per_cell,
            max_players=args.max_players,
            replications=args.replications,
            activity_levels=activity_levels,
            self_promote_rate=args.self_promote_rate,
            true_strength_closed_delta=args.true_strength_closed_delta,
            self_promote_closed_delta=self_promote_closed_delta,
            start_date=args.start_date,
            years=args.years,
        )
        for row in seed_rows:
            row["seed"] = seed
            row["seed_offset"] = offset
            row["seed_elapsed_seconds"] = time.perf_counter() - seed_started
        slice_summary_rows.extend(seed_rows)
        seed_summary_rows.extend(row for row in seed_rows if row["slice_type"] == "all")
        comparison_rows.extend(comparison_for_seed(seed_rows, seed=seed, seed_offset=offset))

        write_csv(output_dir / "seed_summary.csv", seed_summary_rows)
        write_csv(output_dir / "slice_seed_summary.csv", slice_summary_rows)
        write_csv(output_dir / "seed_comparison.csv", comparison_rows)
        write_progress(
            progress_path,
            status="running",
            completed_seeds=offset + 1,
            seed_count=args.seed_count,
            last_seed=seed,
            elapsed_seconds=time.perf_counter() - started,
        )

    aggregate_algorithm = aggregate_rows(slice_summary_rows, key_fields=["algorithm", "slice_type", "slice_value"])
    aggregate_comparison = aggregate_rows(comparison_rows, key_fields=["slice_type", "slice_value"])
    write_csv(output_dir / "aggregate_by_algorithm.csv", aggregate_algorithm)
    write_csv(output_dir / "aggregate_comparison.csv", aggregate_comparison)

    write_progress(
        progress_path,
        status="complete",
        completed_seeds=args.seed_count,
        seed_count=args.seed_count,
        elapsed_seconds=time.perf_counter() - started,
    )
    print(f"Seed sweep written to {output_dir}", flush=True)


def run_seed(
    *,
    seed: int,
    latest_ratings,
    templates_by_band,
    players_per_cell: int,
    max_players: int | None,
    replications: int,
    activity_levels: list[tuple[str, int]],
    self_promote_rate: float,
    true_strength_closed_delta: float,
    self_promote_closed_delta: float,
    start_date: date,
    years: float,
) -> list[dict[str, object]]:
    """Run seed."""
    rng = random.Random(seed)
    base_players = sample_base_players(
        latest_ratings,
        players_per_cell=players_per_cell,
        rng=rng,
        max_players=max_players,
    )
    simulated_players = expand_simulated_players(
        base_players,
        replications=replications,
        activity_levels=activity_levels,
        self_promote_rate=self_promote_rate,
        true_strength_closed_delta=true_strength_closed_delta,
        self_promote_closed_delta=self_promote_closed_delta,
        rng=rng,
    )
    simulated_games, _game_infos, initial_td_list = simulate_games(
        simulated_players,
        templates_by_band,
        latest_ratings,
        start_date=start_date,
        years=years,
        rng=rng,
    )
    simulated_games.sort(key=lambda game: (game.game_date, game.source_game_id))
    results = run_algorithms(simulated_games, initial_td_list)

    player_outcomes: list[dict[str, object]] = []
    for algorithm, result in results.items():
        player_outcomes.extend(summarize_player_outcomes(algorithm, result, simulated_players))

    summaries = category_summaries(player_outcomes)
    for row in summaries:
        row["base_player_count"] = len(base_players)
        row["simulated_player_count"] = len(simulated_players)
        row["simulated_game_count"] = len(simulated_games)
    return summaries


def comparison_for_seed(rows: list[dict[str, object]], *, seed: int, seed_offset: int) -> list[dict[str, object]]:
    """Execute the comparison for seed routine."""
    by_key = {
        (str(row["algorithm"]), str(row["slice_type"]), str(row["slice_value"])): row
        for row in rows
    }
    output: list[dict[str, object]] = []
    slice_keys = sorted({(str(row["slice_type"]), str(row["slice_value"])) for row in rows})
    for slice_type, slice_value in slice_keys:
        baseline = by_key.get((BASELINE_ALGORITHM, slice_type, slice_value))
        candidate = by_key.get((CANDIDATE_ALGORITHM, slice_type, slice_value))
        if baseline is None or candidate is None:
            continue
        output.append(
            {
                "seed": seed,
                "seed_offset": seed_offset,
                "slice_type": slice_type,
                "slice_value": slice_value,
                "players": candidate["players"],
                "baseline_reached_rate": baseline["reached_rate"],
                "candidate_reached_rate": candidate["reached_rate"],
                "delta_reached_rate": none_safe_subtract(candidate["reached_rate"], baseline["reached_rate"]),
                "baseline_games_to_reach_median": baseline["games_to_reach_median"],
                "candidate_games_to_reach_median": candidate["games_to_reach_median"],
                "delta_games_to_reach_median": none_safe_subtract(
                    candidate["games_to_reach_median"],
                    baseline["games_to_reach_median"],
                ),
                "baseline_days_to_reach_median": baseline["days_to_reach_median"],
                "candidate_days_to_reach_median": candidate["days_to_reach_median"],
                "delta_days_to_reach_median": none_safe_subtract(
                    candidate["days_to_reach_median"],
                    baseline["days_to_reach_median"],
                ),
                "baseline_final_gap_to_true_median": baseline["final_gap_to_true_median"],
                "candidate_final_gap_to_true_median": candidate["final_gap_to_true_median"],
                "delta_final_gap_to_true_median": none_safe_subtract(
                    candidate["final_gap_to_true_median"],
                    baseline["final_gap_to_true_median"],
                ),
            }
        )
    return output


def aggregate_rows(rows: list[dict[str, object]], *, key_fields: list[str]) -> list[dict[str, object]]:
    """Execute the aggregate rows routine."""
    groups: dict[tuple[object, ...], list[dict[str, object]]] = {}
    for row in rows:
        groups.setdefault(tuple(row[field] for field in key_fields), []).append(row)

    output: list[dict[str, object]] = []
    for key, group_rows in sorted(groups.items(), key=lambda item: tuple(str(part) for part in item[0])):
        base = {field: value for field, value in zip(key_fields, key)}
        base["seed_count"] = len({row.get("seed") for row in group_rows})
        for metric in numeric_metrics(group_rows, exclude=set(key_fields)):
            values = [float(row[metric]) for row in group_rows if is_number(row.get(metric))]
            if not values:
                continue
            output.append(
                {
                    **base,
                    "metric": metric,
                    "count": len(values),
                    "mean": statistics.fmean(values),
                    "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
                    "min": min(values),
                    "p025": percentile(values, 0.025),
                    "p50": percentile(values, 0.50),
                    "p975": percentile(values, 0.975),
                    "max": max(values),
                }
            )
    return output


def numeric_metrics(rows: list[dict[str, object]], *, exclude: set[str]) -> list[str]:
    """Execute the numeric metrics routine."""
    metrics = []
    for key in rows[0]:
        if key in exclude:
            continue
        if any(is_number(row.get(key)) for row in rows):
            metrics.append(key)
    return metrics


def percentile(values: Iterable[float], quantile: float) -> float:
    """Execute the percentile routine."""
    ordered = sorted(values)
    if not ordered:
        raise ValueError("percentile requires at least one value")
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def none_safe_subtract(left: object, right: object) -> float | None:
    """Execute the none safe subtract routine."""
    if left is None or right is None:
        return None
    return float(left) - float(right)


def is_number(value: object) -> bool:
    """Return whether number."""
    if value is None:
        return False
    if isinstance(value, bool):
        return False
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    """Write csv."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_progress(path: Path, **fields: object) -> None:
    """Write progress."""
    path.write_text(json.dumps(fields, indent=2, default=str) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
