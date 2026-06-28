"""Tune BayRate surprise-sigma parameters with Optuna.

The harness pre-generates simulated one-rank-stronger games, then replays the
same scenarios for each Optuna trial. Optuna is an optional dependency; importing
this module does not require it.
"""

from __future__ import annotations

import argparse
import csv
import gc
import json
import math
import random
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from bayrate.core import BayrateConfig, GameRecord, TdListEntry, load_games_from_csv, run_bayrate, run_bayrate_loaded
from research.bayrate_sigma.simulate_activity_grid import (
    DEFAULT_FULL_HISTORY,
    active_snapshots,
    build_players,
    last_game_dates_by_player,
    load_config_from_json,
    milestone_rows_for_result,
    parse_activity_values,
)
from research.bayrate_sigma.simulate_improver_catchup import (
    DEFAULT_DATASET,
    DEFAULT_OUTPUT_DIR,
    SimulatedPlayer,
    build_encounter_templates,
    clone_td_list,
    load_latest_ratings,
    simulate_games,
)


DEFAULT_OPTUNA_OUTPUT_DIR = DEFAULT_OUTPUT_DIR / "optuna"
SCORE_MODES = (
    "pre",
    "post",
    "min_pre_post",
    "pre_shannon",
    "post_shannon",
    "min_pre_post_shannon",
    "pre_shannon_excess",
    "post_shannon_excess",
    "min_pre_post_shannon_excess",
    "volatility_pre",
    "volatility_post",
    "volatility_min_pre_post",
    "volatility_hybrid_pre_post",
    "volatility_hybrid_pre_min",
)
DEFAULT_RANK_GUARDRAIL_BANDS = (
    "1d_to_<3d",
    "3d_to_<5d",
    "5d_to_<6d",
    "6d_to_<7d",
    "7d_to_<8d",
    "8d+",
)


@dataclass(frozen=True)
class PreparedScenario:
    """Represent prepared scenario."""
    name: str
    games_per_year: int
    self_promote_delta: float
    players: list[SimulatedPlayer]
    games: list[GameRecord]
    initial_td_list: dict[int, TdListEntry]


@dataclass(frozen=True)
class ScoreWeights:
    """Represent score weights."""
    mean_abs_gap: float = 0.0
    gap_gaussian_nll: float = 0.0
    under_progress: float = 1.0
    over_progress: float = 0.6
    overshoot_125: float = 0.4
    overshoot_150: float = 1.5
    reached_shortfall: float = 0.3
    sigma_excess: float = 0.2
    target_reached_rate: float = 55.0
    target_sigma: float = 0.65
    rank_overshoot_125: float = 0.0
    rank_overshoot_150: float = 0.0
    rank_overshoot_target_125: float = 35.0
    rank_overshoot_target_150: float = 10.0
    rank_hot_overshoot_125: float = 0.0
    rank_hot_overshoot_150: float = 0.0
    rank_hot_overshoot_target_125: float = 45.0
    rank_hot_overshoot_target_150: float = 20.0
    rank_guardrail_min_players: int = 8
    rank_guardrail_bands: tuple[str, ...] = DEFAULT_RANK_GUARDRAIL_BANDS
    historical_log_loss: float = 0.0
    historical_brier: float = 0.0
    historical_accuracy: float = 0.0


@dataclass(frozen=True)
class HistoricalBenchmark:
    """Represent historical benchmark."""
    games_path: Path
    ratings_path: Path
    baseline_log_loss: float
    baseline_brier: float
    baseline_accuracy: float | None
    allow_online_games: bool = False
    min_game_date: date | None = None
    max_game_date: date | None = None
    max_events: int | None = None


def parse_args() -> argparse.Namespace:
    """Parse args."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ratings", type=Path, default=DEFAULT_DATASET / "ratings.csv")
    parser.add_argument("--template-games", type=Path, default=DEFAULT_FULL_HISTORY)
    parser.add_argument("--last-game-source", type=Path, default=DEFAULT_FULL_HISTORY)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OPTUNA_OUTPUT_DIR)
    parser.add_argument("--name", default=f"sigma-optuna-{date.today().isoformat()}")
    parser.add_argument("--trials", type=int, default=20)
    parser.add_argument(
        "--total-trials",
        type=int,
        help="Run until the loaded study has this many completed trials. Useful with --storage.",
    )
    parser.add_argument("--seed", type=int, default=20260605)
    parser.add_argument("--sampler-seed", type=int, default=20260605)
    parser.add_argument("--start-date", type=lambda value: date.fromisoformat(value), default=date(2026, 6, 1))
    parser.add_argument("--active-since", type=lambda value: date.fromisoformat(value), default=date(2021, 6, 5))
    parser.add_argument("--years", type=float, default=3.0)
    parser.add_argument("--games-per-year-values", default="5,10,25,50")
    parser.add_argument("--self-promote-deltas", default="0,1")
    parser.add_argument("--true-strength-closed-delta", type=float, default=1.0)
    parser.add_argument("--max-players", type=int, default=250)
    parser.add_argument("--score-years", default="3")
    parser.add_argument("--storage", help="Optional Optuna storage URI, for example sqlite:///sigma_optuna.db")
    parser.add_argument("--study-name", help="Optional Optuna study name.")
    parser.add_argument(
        "--enqueue-config-json",
        type=Path,
        action="append",
        default=[],
        help="Optional BayrateConfig JSON to evaluate first, repeatable. Useful for known candidates.",
    )
    parser.add_argument("--taper-start-min", type=float, default=3.0)
    parser.add_argument("--taper-start-max", type=float, default=8.0)
    parser.add_argument("--taper-width-min", type=float, default=0.5)
    parser.add_argument("--taper-width-max", type=float, default=5.0)
    parser.add_argument("--taper-floor-min", type=float, default=0.0)
    parser.add_argument("--taper-floor-max", type=float, default=1.0)
    parser.add_argument("--surprise-base-min", type=float, default=0.0)
    parser.add_argument("--surprise-base-max", type=float, default=0.35)
    parser.add_argument("--surprise-scale-min", type=float, default=0.0)
    parser.add_argument("--surprise-scale-max", type=float, default=0.80)
    parser.add_argument("--surprise-deadband-min", type=float, default=0.05)
    parser.add_argument("--surprise-deadband-max", type=float, default=0.80)
    parser.add_argument("--surprise-cap-min", type=float, default=0.20)
    parser.add_argument("--surprise-cap-max", type=float, default=0.80)
    parser.add_argument(
        "--score-modes",
        default=",".join(SCORE_MODES),
        help="Comma-separated surprise score modes Optuna may choose from.",
    )
    parser.add_argument("--surprise-persistence-weight-min", type=float, default=0.0)
    parser.add_argument("--surprise-persistence-weight-max", type=float, default=0.0)
    parser.add_argument("--surprise-persistence-decay-min", type=float, default=0.0)
    parser.add_argument("--surprise-persistence-decay-max", type=float, default=0.0)
    parser.add_argument("--surprise-persistence-cap-min", type=float, default=2.0)
    parser.add_argument("--surprise-persistence-cap-max", type=float, default=2.0)
    parser.add_argument(
        "--tune-surprise-persistence-same-direction",
        action="store_true",
        help="Let Optuna choose whether opposite-direction surprise resets the retained persistence score.",
    )
    parser.add_argument("--allow-online-games", action="store_true")
    parser.add_argument("--mean-abs-gap-weight", type=float, default=ScoreWeights.mean_abs_gap)
    parser.add_argument(
        "--gap-gaussian-nll-weight",
        type=float,
        default=ScoreWeights.gap_gaussian_nll,
        help="Weight for fitted Gaussian negative log likelihood of rating gap to true strength.",
    )
    parser.add_argument("--target-reached-rate", type=float, default=ScoreWeights.target_reached_rate)
    parser.add_argument("--target-sigma", type=float, default=ScoreWeights.target_sigma)
    parser.add_argument("--under-progress-weight", type=float, default=ScoreWeights.under_progress)
    parser.add_argument("--over-progress-weight", type=float, default=ScoreWeights.over_progress)
    parser.add_argument("--overshoot-125-weight", type=float, default=ScoreWeights.overshoot_125)
    parser.add_argument("--overshoot-150-weight", type=float, default=ScoreWeights.overshoot_150)
    parser.add_argument("--reached-shortfall-weight", type=float, default=ScoreWeights.reached_shortfall)
    parser.add_argument("--sigma-excess-weight", type=float, default=ScoreWeights.sigma_excess)
    parser.add_argument("--rank-overshoot-125-weight", type=float, default=ScoreWeights.rank_overshoot_125)
    parser.add_argument("--rank-overshoot-150-weight", type=float, default=ScoreWeights.rank_overshoot_150)
    parser.add_argument("--rank-overshoot-target-125", type=float, default=ScoreWeights.rank_overshoot_target_125)
    parser.add_argument("--rank-overshoot-target-150", type=float, default=ScoreWeights.rank_overshoot_target_150)
    parser.add_argument("--rank-hot-overshoot-125-weight", type=float, default=ScoreWeights.rank_hot_overshoot_125)
    parser.add_argument("--rank-hot-overshoot-150-weight", type=float, default=ScoreWeights.rank_hot_overshoot_150)
    parser.add_argument("--rank-hot-overshoot-target-125", type=float, default=ScoreWeights.rank_hot_overshoot_target_125)
    parser.add_argument("--rank-hot-overshoot-target-150", type=float, default=ScoreWeights.rank_hot_overshoot_target_150)
    parser.add_argument("--rank-guardrail-min-players", type=int, default=ScoreWeights.rank_guardrail_min_players)
    parser.add_argument("--rank-guardrail-bands", default=",".join(DEFAULT_RANK_GUARDRAIL_BANDS))
    parser.add_argument("--historical-games", type=Path, help="Optional real games.csv replay for predictive guardrails.")
    parser.add_argument("--historical-ratings", type=Path, help="Optional real ratings.csv replay for predictive guardrails.")
    parser.add_argument(
        "--historical-baseline-summary",
        type=Path,
        help="Optional baseline summary.json with pre-event metrics to compare against.",
    )
    parser.add_argument("--historical-log-loss-weight", type=float, default=ScoreWeights.historical_log_loss)
    parser.add_argument("--historical-brier-weight", type=float, default=ScoreWeights.historical_brier)
    parser.add_argument("--historical-accuracy-weight", type=float, default=ScoreWeights.historical_accuracy)
    parser.add_argument("--historical-allow-online-games", action="store_true")
    parser.add_argument("--historical-min-game-date", type=lambda value: date.fromisoformat(value))
    parser.add_argument("--historical-max-game-date", type=lambda value: date.fromisoformat(value))
    parser.add_argument("--historical-max-events", type=int)
    return parser.parse_args()


def main() -> None:
    """Run the command-line entry point for this module."""
    args = parse_args()
    validate_search_ranges(args)
    optuna = load_optuna()
    started = time.perf_counter()
    output_dir = args.output_dir / args.name
    output_dir.mkdir(parents=True, exist_ok=True)

    activity_values = parse_activity_values(args.games_per_year_values)
    self_promote_deltas = parse_float_list(args.self_promote_deltas, field_name="self-promote delta")
    score_years = set(parse_int_list(args.score_years, field_name="score year"))
    score_modes = parse_score_modes(args.score_modes)
    weights = ScoreWeights(
        mean_abs_gap=args.mean_abs_gap_weight,
        gap_gaussian_nll=args.gap_gaussian_nll_weight,
        under_progress=args.under_progress_weight,
        over_progress=args.over_progress_weight,
        overshoot_125=args.overshoot_125_weight,
        overshoot_150=args.overshoot_150_weight,
        reached_shortfall=args.reached_shortfall_weight,
        sigma_excess=args.sigma_excess_weight,
        target_reached_rate=args.target_reached_rate,
        target_sigma=args.target_sigma,
        rank_overshoot_125=args.rank_overshoot_125_weight,
        rank_overshoot_150=args.rank_overshoot_150_weight,
        rank_overshoot_target_125=args.rank_overshoot_target_125,
        rank_overshoot_target_150=args.rank_overshoot_target_150,
        rank_hot_overshoot_125=args.rank_hot_overshoot_125_weight,
        rank_hot_overshoot_150=args.rank_hot_overshoot_150_weight,
        rank_hot_overshoot_target_125=args.rank_hot_overshoot_target_125,
        rank_hot_overshoot_target_150=args.rank_hot_overshoot_target_150,
        rank_guardrail_min_players=args.rank_guardrail_min_players,
        rank_guardrail_bands=tuple(parse_string_list(args.rank_guardrail_bands, field_name="rank guardrail band")),
        historical_log_loss=args.historical_log_loss_weight,
        historical_brier=args.historical_brier_weight,
        historical_accuracy=args.historical_accuracy_weight,
    )

    print("Preparing simulated scenarios...", flush=True)
    scenarios = prepare_scenarios(
        ratings_path=args.ratings,
        template_games_path=args.template_games,
        last_game_source_path=args.last_game_source,
        activity_values=activity_values,
        self_promote_deltas=self_promote_deltas,
        true_strength_closed_delta=args.true_strength_closed_delta,
        max_players=args.max_players,
        active_since=args.active_since,
        start_date=args.start_date,
        years=args.years,
        seed=args.seed,
        allow_online_games=args.allow_online_games,
    )
    historical_benchmark = load_historical_benchmark(args)

    metadata = {
        "name": args.name,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "ratings": str(args.ratings),
        "template_games": str(args.template_games),
        "last_game_source": str(args.last_game_source),
        "trials": args.trials,
        "total_trials": args.total_trials,
        "seed": args.seed,
        "sampler_seed": args.sampler_seed,
        "start_date": args.start_date.isoformat(),
        "active_since": args.active_since.isoformat(),
        "years": args.years,
        "games_per_year_values": activity_values,
        "self_promote_deltas": self_promote_deltas,
        "true_strength_closed_delta": args.true_strength_closed_delta,
        "max_players": args.max_players,
        "score_years": sorted(score_years),
        "weights": asdict(weights),
        "search_space": {
            "taper_start_min": args.taper_start_min,
            "taper_start_max": args.taper_start_max,
            "taper_width_min": args.taper_width_min,
            "taper_width_max": args.taper_width_max,
            "taper_floor_min": args.taper_floor_min,
            "taper_floor_max": args.taper_floor_max,
            "surprise_base_min": args.surprise_base_min,
            "surprise_base_max": args.surprise_base_max,
            "surprise_scale_min": args.surprise_scale_min,
            "surprise_scale_max": args.surprise_scale_max,
            "surprise_deadband_min": args.surprise_deadband_min,
            "surprise_deadband_max": args.surprise_deadband_max,
            "surprise_cap_min": args.surprise_cap_min,
            "surprise_cap_max": args.surprise_cap_max,
            "score_modes": score_modes,
            "surprise_persistence_weight_min": args.surprise_persistence_weight_min,
            "surprise_persistence_weight_max": args.surprise_persistence_weight_max,
            "surprise_persistence_decay_min": args.surprise_persistence_decay_min,
            "surprise_persistence_decay_max": args.surprise_persistence_decay_max,
            "surprise_persistence_cap_min": args.surprise_persistence_cap_min,
            "surprise_persistence_cap_max": args.surprise_persistence_cap_max,
            "tune_surprise_persistence_same_direction": args.tune_surprise_persistence_same_direction,
        },
        "historical_benchmark": None
        if historical_benchmark is None
        else asdict(historical_benchmark),
        "scenario_count": len(scenarios),
        "scenario_game_counts": {scenario.name: len(scenario.games) for scenario in scenarios},
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8")

    sampler = optuna.samplers.TPESampler(seed=args.sampler_seed)
    study = optuna.create_study(
        direction="minimize",
        sampler=sampler,
        storage=args.storage,
        study_name=args.study_name or args.name,
        load_if_exists=bool(args.storage),
    )
    enqueue_seed_configs(
        study,
        args.enqueue_config_json,
        taper_width_range=(args.taper_width_min, args.taper_width_max),
        score_modes=score_modes,
    )
    trial_rows = read_csv(output_dir / "trials.csv")
    completed_before = completed_trial_count(study)
    trials_to_run = args.trials
    if args.total_trials is not None:
        trials_to_run = max(0, args.total_trials - completed_before)
    print(
        f"Study has {completed_before} completed trial(s); running {trials_to_run} new trial(s).",
        flush=True,
    )

    def objective(trial: Any) -> float:
        """Execute the objective routine."""
        trial_started = time.perf_counter()
        config = suggest_config(
            trial,
            taper_start_range=(args.taper_start_min, args.taper_start_max),
            taper_width_range=(args.taper_width_min, args.taper_width_max),
            taper_floor_range=(args.taper_floor_min, args.taper_floor_max),
            surprise_base_range=(args.surprise_base_min, args.surprise_base_max),
            surprise_scale_range=(args.surprise_scale_min, args.surprise_scale_max),
            surprise_deadband_range=(args.surprise_deadband_min, args.surprise_deadband_max),
            surprise_cap_range=(args.surprise_cap_min, args.surprise_cap_max),
            persistence_weight_range=(
                args.surprise_persistence_weight_min,
                args.surprise_persistence_weight_max,
            ),
            persistence_decay_range=(
                args.surprise_persistence_decay_min,
                args.surprise_persistence_decay_max,
            ),
            persistence_cap_range=(
                args.surprise_persistence_cap_min,
                args.surprise_persistence_cap_max,
            ),
            tune_persistence_same_direction=args.tune_surprise_persistence_same_direction,
            score_modes=score_modes,
        )
        score, summaries = evaluate_config(
            config,
            scenarios,
            start_date=args.start_date,
            year_count=int(args.years),
            score_years=score_years,
            weights=weights,
        )
        historical_metrics = (
            {}
            if historical_benchmark is None
            else evaluate_historical_config(config, historical_benchmark)
        )
        if historical_metrics:
            score += score_historical_metrics(historical_metrics, weights)
        aggregate = {
            **aggregate_summaries(summaries, weights=weights),
            **historical_metrics,
        }
        row = {
            "trial": trial.number,
            "score": score,
            "elapsed_seconds": time.perf_counter() - trial_started,
            **flatten_config(config),
            **aggregate,
        }
        trial_rows.append(row)
        write_csv(output_dir / "trials.csv", trial_rows)
        write_csv(output_dir / f"trial_{trial.number:04d}_summary.csv", summaries)
        trial.set_user_attr("aggregate", aggregate)
        history_note = ""
        if historical_metrics:
            history_note = (
                f" hist_ll={historical_metrics['historical_log_loss_delta']:+.6f} "
                f"hist_brier={historical_metrics['historical_brier_delta']:+.6f}"
            )
        print(
            f"trial {trial.number}: score={score:.3f} "
            f"reached={aggregate['score_reached_rate']:.1f}% "
            f"progress={aggregate['score_mean_rank_change_pct']:.1f}% "
            f"abs_gap={aggregate['score_mean_abs_gap_to_true']:.3f} "
            f"rms={aggregate['score_rms_gap_to_true']:.3f} "
            f">=125={aggregate['score_pct_ge_125']:.1f}% "
            f"rank>=125={aggregate['score_rank_pct_ge_125']:.1f}% "
            f"hot>150={aggregate['score_rank_hot_pct_gt_150']:.1f}% "
            f"sigma={aggregate['score_median_sigma']:.3f}"
            f"{history_note}",
            flush=True,
        )
        return score

    if trials_to_run > 0:
        study.optimize(objective, n_trials=trials_to_run)

    best_config = suggest_config(
        FixedTrial(study.best_trial.params),
        taper_start_range=(args.taper_start_min, args.taper_start_max),
        taper_width_range=(args.taper_width_min, args.taper_width_max),
        taper_floor_range=(args.taper_floor_min, args.taper_floor_max),
        surprise_base_range=(args.surprise_base_min, args.surprise_base_max),
        surprise_scale_range=(args.surprise_scale_min, args.surprise_scale_max),
        surprise_deadband_range=(args.surprise_deadband_min, args.surprise_deadband_max),
        surprise_cap_range=(args.surprise_cap_min, args.surprise_cap_max),
        persistence_weight_range=(
            args.surprise_persistence_weight_min,
            args.surprise_persistence_weight_max,
        ),
        persistence_decay_range=(
            args.surprise_persistence_decay_min,
            args.surprise_persistence_decay_max,
        ),
        persistence_cap_range=(
            args.surprise_persistence_cap_min,
            args.surprise_persistence_cap_max,
        ),
        tune_persistence_same_direction=args.tune_surprise_persistence_same_direction,
        score_modes=score_modes,
    )
    best_score, best_summaries, best_milestones = evaluate_config(
        best_config,
        scenarios,
        start_date=args.start_date,
        year_count=int(args.years),
        score_years=score_years,
        weights=weights,
        keep_milestones=True,
    )
    best_historical_metrics = (
        {}
        if historical_benchmark is None
        else evaluate_historical_config(best_config, historical_benchmark)
    )
    if best_historical_metrics:
        best_score += score_historical_metrics(best_historical_metrics, weights)
    (output_dir / "best_config.json").write_text(
        json.dumps(
            {
                "score": best_score,
                "simulation_score": score_summaries(best_summaries, score_years=score_years, weights=weights),
                "historical_metrics": best_historical_metrics,
                "config": asdict(best_config),
            },
            indent=2,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )
    write_csv(output_dir / "best_summary.csv", best_summaries)
    write_csv(output_dir / "best_milestones.csv", best_milestones)
    print(f"Best score: {study.best_value:.3f}", flush=True)
    print(f"Best config written to {output_dir / 'best_config.json'}", flush=True)
    print(f"Completed in {time.perf_counter() - started:.1f}s.", flush=True)


def load_optuna() -> Any:
    """Load optuna."""
    try:
        import optuna
    except ModuleNotFoundError as exc:
        raise SystemExit("Optuna is not installed. Install it with: py -3 -m pip install optuna") from exc
    return optuna


def parse_float_list(text: str, *, field_name: str) -> list[float]:
    """Parse float list."""
    values = [float(part.strip()) for part in text.split(",") if part.strip()]
    if not values:
        raise ValueError(f"At least one {field_name} is required")
    return values


def parse_int_list(text: str, *, field_name: str) -> list[int]:
    """Parse int list."""
    values = [int(part.strip()) for part in text.split(",") if part.strip()]
    if not values:
        raise ValueError(f"At least one {field_name} is required")
    return values


def parse_string_list(text: str, *, field_name: str) -> list[str]:
    """Parse string list."""
    values = [part.strip() for part in text.split(",") if part.strip()]
    if not values:
        raise ValueError(f"At least one {field_name} is required")
    return values


def parse_score_modes(text: str) -> tuple[str, ...]:
    """Parse score modes."""
    modes = tuple(parse_string_list(text, field_name="score mode"))
    unsupported = sorted(set(modes) - set(SCORE_MODES))
    if unsupported:
        raise ValueError(f"Unsupported score mode(s): {', '.join(unsupported)}")
    return modes


def validate_search_ranges(args: argparse.Namespace) -> None:
    """Validate search ranges."""
    parse_score_modes(args.score_modes)
    ranges = [
        ("taper start", args.taper_start_min, args.taper_start_max),
        ("taper width", args.taper_width_min, args.taper_width_max),
        ("taper floor", args.taper_floor_min, args.taper_floor_max),
        ("surprise base", args.surprise_base_min, args.surprise_base_max),
        ("surprise scale", args.surprise_scale_min, args.surprise_scale_max),
        ("surprise deadband", args.surprise_deadband_min, args.surprise_deadband_max),
        ("surprise cap", args.surprise_cap_min, args.surprise_cap_max),
        ("surprise persistence weight", args.surprise_persistence_weight_min, args.surprise_persistence_weight_max),
        ("surprise persistence decay", args.surprise_persistence_decay_min, args.surprise_persistence_decay_max),
        ("surprise persistence cap", args.surprise_persistence_cap_min, args.surprise_persistence_cap_max),
    ]
    for label, low, high in ranges:
        if low > high:
            raise ValueError(f"{label} min must be less than or equal to max")
    if args.taper_width_min <= 0.0:
        raise ValueError("taper width min must be positive")
    if args.taper_floor_min < 0.0 or args.taper_floor_max > 1.0:
        raise ValueError("taper floor bounds must be between 0 and 1")
    if args.surprise_base_min < 0.0:
        raise ValueError("surprise base min must be non-negative")
    if args.surprise_scale_min < 0.0:
        raise ValueError("surprise scale min must be non-negative")
    if args.surprise_deadband_min < 0.0:
        raise ValueError("surprise deadband min must be non-negative")
    if args.surprise_cap_min < 0.0:
        raise ValueError("surprise cap min must be non-negative")
    if args.surprise_persistence_weight_min < 0.0:
        raise ValueError("surprise persistence weight min must be non-negative")
    if args.surprise_persistence_decay_min < 0.0 or args.surprise_persistence_decay_max > 1.0:
        raise ValueError("surprise persistence decay bounds must be between 0 and 1")
    if args.surprise_persistence_cap_min < 0.0:
        raise ValueError("surprise persistence cap min must be non-negative")


def completed_trial_count(study: Any) -> int:
    """Execute the completed trial count routine."""
    return sum(1 for trial in study.trials if trial.state.name == "COMPLETE")


def suggest_config(
    trial: Any,
    *,
    taper_start_range: tuple[float, float] = (3.0, 8.0),
    taper_width_range: tuple[float, float] = (0.5, 5.0),
    taper_floor_range: tuple[float, float] = (0.0, 1.0),
    surprise_base_range: tuple[float, float] = (0.0, 0.35),
    surprise_scale_range: tuple[float, float] = (0.0, 0.80),
    surprise_deadband_range: tuple[float, float] = (0.05, 0.80),
    surprise_cap_range: tuple[float, float] = (0.20, 0.80),
    persistence_weight_range: tuple[float, float] = (0.0, 0.0),
    persistence_decay_range: tuple[float, float] = (0.0, 0.0),
    persistence_cap_range: tuple[float, float] = (2.0, 2.0),
    tune_persistence_same_direction: bool = False,
    score_modes: tuple[str, ...] = SCORE_MODES,
) -> BayrateConfig:
    """Execute the suggest config routine."""
    taper_start = suggest_float_range(trial, "surprise_taper_start_rating", taper_start_range)
    taper_width = suggest_float_range(trial, "surprise_taper_width", taper_width_range)
    persistence_weight = suggest_float_range(
        trial,
        "surprise_persistence_weight",
        persistence_weight_range,
    )
    persistence_decay = suggest_float_range(
        trial,
        "surprise_persistence_decay",
        persistence_decay_range,
    )
    persistence_cap = suggest_float_range(
        trial,
        "surprise_persistence_cap",
        persistence_cap_range,
    )
    persistence_same_direction = (
        trial.suggest_categorical("surprise_persistence_same_direction_only", [False, True])
        if tune_persistence_same_direction
        else False
    )
    return BayrateConfig(
        optimizer_random_jitter=0.0,
        posterior_process_noise=trial.suggest_float("posterior_process_noise", 0.0, 0.04),
        surprise_sigma_base=suggest_float_range(trial, "surprise_sigma_base", surprise_base_range),
        surprise_sigma_scale=suggest_float_range(trial, "surprise_sigma_scale", surprise_scale_range),
        surprise_sigma_deadband=suggest_float_range(trial, "surprise_sigma_deadband", surprise_deadband_range),
        surprise_sigma_cap=suggest_float_range(trial, "surprise_sigma_cap", surprise_cap_range),
        surprise_sigma_score_mode=trial.suggest_categorical("surprise_sigma_score_mode", list(score_modes)),
        surprise_sigma_gate_base_by_deadband=trial.suggest_categorical(
            "surprise_sigma_gate_base_by_deadband",
            [False, True],
        ),
        surprise_taper_start_rating=taper_start,
        surprise_taper_end_rating=taper_start + taper_width,
        surprise_taper_floor=suggest_float_range(trial, "surprise_taper_floor", taper_floor_range),
        surprise_persistence_weight=persistence_weight,
        surprise_persistence_decay=persistence_decay,
        surprise_persistence_cap=persistence_cap,
        surprise_persistence_same_direction_only=persistence_same_direction,
    )


def suggest_float_range(trial: Any, name: str, value_range: tuple[float, float]) -> float:
    """Execute the suggest float range routine."""
    low, high = value_range
    if math.isclose(low, high, rel_tol=0.0, abs_tol=1e-12):
        return float(low)
    return float(trial.suggest_float(name, low, high))


def enqueue_seed_configs(
    study: Any,
    config_paths: Iterable[Path],
    *,
    taper_width_range: tuple[float, float] = (0.5, 5.0),
    score_modes: tuple[str, ...] = SCORE_MODES,
) -> None:
    """Execute the enqueue seed configs routine."""
    for path in config_paths:
        params = trial_params_from_config(
            load_config_from_json(path),
            taper_width_range=taper_width_range,
            score_modes=score_modes,
        )
        if study_has_trial_params(study, params):
            print(f"Seed config already present: {path}", flush=True)
            continue
        study.enqueue_trial(params, user_attrs={"seed_config": str(path)})
        print(f"Queued seed config: {path}", flush=True)


def trial_params_from_config(
    config: BayrateConfig,
    *,
    taper_width_range: tuple[float, float] = (0.5, 5.0),
    score_modes: tuple[str, ...] = SCORE_MODES,
) -> dict[str, object]:
    """Execute the trial params from config routine."""
    if config.surprise_sigma_base is None:
        raise ValueError("Seed config must set surprise_sigma_base")
    if config.surprise_sigma_cap is None:
        raise ValueError("Seed config must set surprise_sigma_cap")
    if config.surprise_taper_start_rating is None or config.surprise_taper_end_rating is None:
        raise ValueError("Seed config must set surprise_taper_start_rating and surprise_taper_end_rating")
    taper_width = config.surprise_taper_end_rating - config.surprise_taper_start_rating
    if not taper_width_range[0] <= taper_width <= taper_width_range[1]:
        raise ValueError(f"Seed config taper width {taper_width} is outside the search range")
    if config.surprise_sigma_score_mode not in SCORE_MODES:
        raise ValueError(f"Seed config score mode {config.surprise_sigma_score_mode!r} is unsupported")
    if config.surprise_sigma_score_mode not in score_modes:
        raise ValueError(f"Seed config score mode {config.surprise_sigma_score_mode!r} is outside the search modes")
    return {
        "surprise_taper_start_rating": config.surprise_taper_start_rating,
        "surprise_taper_width": taper_width,
        "posterior_process_noise": config.posterior_process_noise,
        "surprise_sigma_base": config.surprise_sigma_base,
        "surprise_sigma_scale": config.surprise_sigma_scale,
        "surprise_sigma_deadband": config.surprise_sigma_deadband,
        "surprise_sigma_cap": config.surprise_sigma_cap,
        "surprise_sigma_score_mode": config.surprise_sigma_score_mode,
        "surprise_sigma_gate_base_by_deadband": config.surprise_sigma_gate_base_by_deadband,
        "surprise_taper_floor": config.surprise_taper_floor,
        "surprise_persistence_weight": config.surprise_persistence_weight,
        "surprise_persistence_decay": config.surprise_persistence_decay,
        "surprise_persistence_cap": config.surprise_persistence_cap
        if config.surprise_persistence_cap is not None
        else 2.0,
        "surprise_persistence_same_direction_only": config.surprise_persistence_same_direction_only,
    }


def study_has_trial_params(study: Any, params: dict[str, object]) -> bool:
    """Execute the study has trial params routine."""
    return any(trial_params_match(existing_trial_params(trial), params) for trial in study.trials)


def existing_trial_params(trial: Any) -> dict[str, object]:
    """Execute the existing trial params routine."""
    if trial.params:
        return dict(trial.params)
    fixed_params = trial.system_attrs.get("fixed_params", {})
    return dict(fixed_params) if isinstance(fixed_params, dict) else {}


def trial_params_match(left: dict[str, object], right: dict[str, object]) -> bool:
    """Execute the trial params match routine."""
    if set(left) != set(right):
        return False
    for key, left_value in left.items():
        right_value = right[key]
        if isinstance(left_value, (int, float)) and isinstance(right_value, (int, float)):
            if not math.isclose(float(left_value), float(right_value), rel_tol=1e-12, abs_tol=1e-12):
                return False
        elif left_value != right_value:
            return False
    return True


def prepare_scenarios(
    *,
    ratings_path: Path,
    template_games_path: Path,
    last_game_source_path: Path,
    activity_values: Iterable[int],
    self_promote_deltas: Iterable[float],
    true_strength_closed_delta: float,
    max_players: int | None,
    active_since: date,
    start_date: date,
    years: float,
    seed: int,
    allow_online_games: bool,
) -> list[PreparedScenario]:
    """Execute the prepare scenarios routine."""
    latest_ratings = load_latest_ratings(ratings_path)
    full_history_games = load_games_from_csv(
        last_game_source_path,
        BayrateConfig(allow_online_games=allow_online_games),
    )
    last_game_dates = last_game_dates_by_player(full_history_games)
    snapshots = active_snapshots(
        latest_ratings.values(),
        last_game_dates=last_game_dates,
        active_since=active_since,
        max_players=max_players,
    )
    template_games = (
        full_history_games
        if template_games_path == last_game_source_path
        else load_games_from_csv(template_games_path, BayrateConfig(allow_online_games=allow_online_games))
    )
    templates_by_band = build_encounter_templates(template_games)

    scenarios: list[PreparedScenario] = []
    for self_promote_delta in self_promote_deltas:
        players, initial_dates = build_players(
            snapshots,
            last_game_dates=last_game_dates,
            true_strength_closed_delta=true_strength_closed_delta,
            self_promote_closed_delta=self_promote_delta,
        )
        for games_per_year in activity_values:
            scenario_players = [
                SimulatedPlayer(
                    simulation_player_id=player.simulation_player_id,
                    original_agaid=player.original_agaid,
                    replication=0,
                    rating_band=player.rating_band,
                    sigma_band=player.sigma_band,
                    activity_label=f"{games_per_year}_games_per_year",
                    games_per_year=games_per_year,
                    self_promote=player.self_promote,
                    start_rating=player.start_rating,
                    start_sigma=player.start_sigma,
                    true_strength=player.true_strength,
                    entry_rating=player.entry_rating,
                )
                for player in players
            ]
            rng = random.Random(seed + games_per_year * 10_000)
            simulated_games, _game_infos, initial_td_list = simulate_games(
                scenario_players,
                templates_by_band,
                latest_ratings,
                start_date=start_date,
                years=years,
                rng=rng,
                target_initial_dates=initial_dates,
            )
            simulated_games.sort(key=lambda game: (game.game_date, game.source_game_id))
            scenario_name = f"self_{self_promote_delta:g}_{games_per_year}gpy"
            scenarios.append(
                PreparedScenario(
                    name=scenario_name,
                    games_per_year=games_per_year,
                    self_promote_delta=self_promote_delta,
                    players=scenario_players,
                    games=simulated_games,
                    initial_td_list=initial_td_list,
                )
            )
    return scenarios


def load_historical_benchmark(args: argparse.Namespace) -> HistoricalBenchmark | None:
    """Load historical benchmark."""
    if args.historical_games is None and args.historical_ratings is None:
        return None
    if args.historical_games is None or args.historical_ratings is None:
        raise ValueError("--historical-games and --historical-ratings must be provided together")

    if args.historical_baseline_summary is not None:
        baseline = json.loads(args.historical_baseline_summary.read_text(encoding="utf-8"))
        baseline_metrics = baseline["pre_event_metrics"]
    else:
        print("No historical baseline summary supplied; calculating baseline replay once...", flush=True)
        baseline_config = historical_run_config(BayrateConfig(), args)
        baseline_result = run_bayrate(args.historical_games, args.historical_ratings, baseline_config)
        baseline_metrics = baseline_result.pre_event_metrics
        del baseline_result
        gc.collect()

    return HistoricalBenchmark(
        games_path=args.historical_games,
        ratings_path=args.historical_ratings,
        baseline_log_loss=float(baseline_metrics["average_log_loss"]),
        baseline_brier=float(baseline_metrics["average_brier"]),
        baseline_accuracy=float(baseline_metrics["accuracy"]) if "accuracy" in baseline_metrics else None,
        allow_online_games=args.historical_allow_online_games,
        min_game_date=args.historical_min_game_date,
        max_game_date=args.historical_max_game_date,
        max_events=args.historical_max_events,
    )


def historical_run_config(config: BayrateConfig, args_or_benchmark: argparse.Namespace | HistoricalBenchmark) -> BayrateConfig:
    """Execute the historical run config routine."""
    values = asdict(config)
    run_config = BayrateConfig(**values)
    run_config.allow_online_games = bool(args_or_benchmark.historical_allow_online_games) if isinstance(
        args_or_benchmark,
        argparse.Namespace,
    ) else args_or_benchmark.allow_online_games
    run_config.min_game_date = args_or_benchmark.historical_min_game_date if isinstance(
        args_or_benchmark,
        argparse.Namespace,
    ) else args_or_benchmark.min_game_date
    run_config.max_game_date = args_or_benchmark.historical_max_game_date if isinstance(
        args_or_benchmark,
        argparse.Namespace,
    ) else args_or_benchmark.max_game_date
    run_config.max_events = args_or_benchmark.historical_max_events if isinstance(
        args_or_benchmark,
        argparse.Namespace,
    ) else args_or_benchmark.max_events
    return run_config


def evaluate_historical_config(config: BayrateConfig, benchmark: HistoricalBenchmark) -> dict[str, float]:
    """Execute the evaluate historical config routine."""
    result = run_bayrate(
        benchmark.games_path,
        benchmark.ratings_path,
        historical_run_config(config, benchmark),
    )
    metrics = result.pre_event_metrics
    accuracy = float(metrics["accuracy"])
    log_loss = float(metrics["average_log_loss"])
    brier = float(metrics["average_brier"])
    output = {
        "historical_games": float(metrics["games"]),
        "historical_accuracy": accuracy,
        "historical_log_loss": log_loss,
        "historical_brier": brier,
        "historical_log_loss_delta": log_loss - benchmark.baseline_log_loss,
        "historical_brier_delta": brier - benchmark.baseline_brier,
    }
    if benchmark.baseline_accuracy is not None:
        output["historical_accuracy_delta"] = accuracy - benchmark.baseline_accuracy
    del result
    gc.collect()
    return output


def score_historical_metrics(metrics: dict[str, float], weights: ScoreWeights) -> float:
    """Execute the score historical metrics routine."""
    score = (
        weights.historical_log_loss * float(metrics["historical_log_loss_delta"])
        + weights.historical_brier * float(metrics["historical_brier_delta"])
    )
    if "historical_accuracy_delta" in metrics:
        score -= weights.historical_accuracy * float(metrics["historical_accuracy_delta"])
    return score


def evaluate_config(
    config: BayrateConfig,
    scenarios: Iterable[PreparedScenario],
    *,
    start_date: date,
    year_count: int,
    score_years: set[int],
    weights: ScoreWeights,
    keep_milestones: bool = False,
) -> tuple[float, list[dict[str, object]]] | tuple[float, list[dict[str, object]], list[dict[str, object]]]:
    """Execute the evaluate config routine."""
    summaries: list[dict[str, object]] = []
    all_milestones: list[dict[str, object]] = []
    for scenario in scenarios:
        result = run_bayrate_loaded(
            scenario.games,
            {},
            config,
            initial_td_list=clone_td_list(scenario.initial_td_list),
        )
        milestones = milestone_rows_for_result(
            result.player_results,
            scenario.players,
            algorithm_name="trial",
            games_per_year=scenario.games_per_year,
            start_date=start_date,
            year_count=year_count,
            inactivity_growth_per_day=config.inactivity_growth_per_day,
        )
        for row in milestones:
            row["scenario"] = scenario.name
            row["self_promote_delta"] = scenario.self_promote_delta
        summaries.extend(summarize_for_score(milestones, scenario=scenario))
        if keep_milestones:
            all_milestones.extend(milestones)
        del result
        gc.collect()
    score = score_summaries(summaries, score_years=score_years, weights=weights)
    if keep_milestones:
        return score, summaries, all_milestones
    return score, summaries


def summarize_for_score(rows: list[dict[str, object]], *, scenario: PreparedScenario) -> list[dict[str, object]]:
    """Summarize for score."""
    summaries = []
    player_by_id = {player.simulation_player_id: player for player in scenario.players}
    for year in sorted({int(row["year"]) for row in rows}):
        year_rows = [row for row in rows if int(row["year"]) == year]
        summaries.append(summary_for_rows(year_rows, scenario=scenario, year=year, slice_type="all", slice_value="all"))
        for band in sorted(rating_bands_for_rows(year_rows, player_by_id)):
            band_rows = [
                row
                for row in year_rows
                if rating_band_for_milestone(row, player_by_id) == band
            ]
            if band_rows:
                summaries.append(
                    summary_for_rows(
                        band_rows,
                        scenario=scenario,
                        year=year,
                        slice_type="rating_band",
                        slice_value=band,
                    )
                )
    return summaries


def summary_for_rows(
    rows: list[dict[str, object]],
    *,
    scenario: PreparedScenario,
    year: int,
    slice_type: str,
    slice_value: str,
) -> dict[str, object]:
    """Execute the summary for rows routine."""
    progress = [rank_change_percent(row) for row in rows]
    sigmas = [float(row["sigma_at_milestone"]) for row in rows]
    gaps = [float(row["gap_to_true"]) for row in rows]
    abs_gaps = [abs(gap) for gap in gaps]
    squared_gaps = [gap * gap for gap in gaps]
    mean_squared_gap = statistics.fmean(squared_gaps) if squared_gaps else 0.0
    rms_gap = math.sqrt(mean_squared_gap)
    gaussian_gap_nll = gaussian_gap_nll_for_rms(rms_gap)
    reached_count = sum(1 for row in rows if bool(row["reached_true_strength"]))
    within_sigma_count = sum(
        1
        for row in rows
        if abs(float(row["gap_to_true"])) <= float(row["sigma_at_milestone"])
    )
    return {
        "scenario": scenario.name,
        "games_per_year": scenario.games_per_year,
        "self_promote_delta": scenario.self_promote_delta,
        "slice_type": slice_type,
        "slice_value": slice_value,
        "year": year,
        "players": len(rows),
        "reached_rate": reached_count / len(rows) * 100.0 if rows else 0.0,
        "within_sigma_rate": within_sigma_count / len(rows) * 100.0 if rows else 0.0,
        "mean_rank_change_pct": statistics.fmean(progress) if progress else 0.0,
        "median_rank_change_pct": statistics.median(progress) if progress else 0.0,
        "pct_ge_100": rate_at_least(progress, 100.0),
        "pct_ge_125": rate_at_least(progress, 125.0),
        "pct_gt_150": rate_greater_than(progress, 150.0),
        "mean_gap_to_true": statistics.fmean(gaps) if gaps else 0.0,
        "median_gap_to_true": statistics.median(gaps) if gaps else 0.0,
        "mean_abs_gap_to_true": statistics.fmean(abs_gaps) if abs_gaps else 0.0,
        "median_abs_gap_to_true": statistics.median(abs_gaps) if abs_gaps else 0.0,
        "mean_squared_gap_to_true": mean_squared_gap,
        "rms_gap_to_true": rms_gap,
        "gaussian_gap_nll": gaussian_gap_nll,
        "mean_sigma": statistics.fmean(sigmas) if sigmas else 0.0,
        "median_sigma": statistics.median(sigmas) if sigmas else 0.0,
    }


def rating_bands_for_rows(
    rows: list[dict[str, object]],
    player_by_id: dict[int, SimulatedPlayer],
) -> set[str]:
    """Execute the rating bands for rows routine."""
    return {
        band
        for row in rows
        if (band := rating_band_for_milestone(row, player_by_id)) is not None
    }


def rating_band_for_milestone(
    row: dict[str, object],
    player_by_id: dict[int, SimulatedPlayer],
) -> str | None:
    """Execute the rating band for milestone routine."""
    if row.get("rating_band"):
        return str(row["rating_band"])
    player_id = row.get("simulation_player_id")
    if player_id is None:
        return None
    player = player_by_id.get(int(player_id))
    return None if player is None else player.rating_band


def rank_change_percent(row: dict[str, object]) -> float:
    """Execute the rank change percent routine."""
    start_rating = float(row["start_rating"])
    true_strength = float(row["true_strength"])
    denom = true_strength - start_rating
    if abs(denom) < 1e-12:
        return 0.0
    return 100.0 * float(row["rating_delta_from_start"]) / denom


def rate_at_least(values: list[float], threshold: float) -> float:
    """Execute the rate at least routine."""
    return sum(value >= threshold for value in values) / len(values) * 100.0 if values else 0.0


def rate_greater_than(values: list[float], threshold: float) -> float:
    """Execute the rate greater than routine."""
    return sum(value > threshold for value in values) / len(values) * 100.0 if values else 0.0


def gaussian_gap_nll_for_rms(rms_gap: float) -> float:
    """Execute the gaussian gap nll for rms routine."""
    fitted_sd = max(float(rms_gap), 1e-6)
    return math.log(fitted_sd) + 0.5


def score_summaries(
    summaries: list[dict[str, object]],
    *,
    score_years: set[int],
    weights: ScoreWeights,
) -> float:
    """Execute the score summaries routine."""
    score_rows = [
        row
        for row in summaries
        if int(row["year"]) in score_years and row.get("slice_type", "all") == "all"
    ]
    if not score_rows:
        raise ValueError("No summary rows matched the configured score years")
    scenario_scores = [score_summary(row, weights) for row in score_rows]
    return statistics.fmean(scenario_scores) + score_rank_guardrails(
        summaries,
        score_years=score_years,
        weights=weights,
    )


def score_summary(row: dict[str, object], weights: ScoreWeights) -> float:
    """Execute the score summary routine."""
    mean_progress = float(row["mean_rank_change_pct"])
    reached_rate = float(row["reached_rate"])
    median_sigma = float(row["median_sigma"])
    mean_abs_gap = float(row.get("mean_abs_gap_to_true", 0.0)) * 100.0
    gaussian_gap_nll = float(row.get("gaussian_gap_nll", 0.0))
    under_progress = max(0.0, 100.0 - mean_progress)
    over_progress = max(0.0, mean_progress - 105.0)
    reached_shortfall = max(0.0, weights.target_reached_rate - reached_rate)
    sigma_excess = max(0.0, median_sigma - weights.target_sigma) * 100.0
    return (
        weights.under_progress * under_progress
        + weights.over_progress * over_progress
        + weights.overshoot_125 * float(row["pct_ge_125"])
        + weights.overshoot_150 * float(row["pct_gt_150"])
        + weights.mean_abs_gap * mean_abs_gap
        + weights.gap_gaussian_nll * gaussian_gap_nll
        + weights.reached_shortfall * reached_shortfall
        + weights.sigma_excess * sigma_excess
    )


def score_rank_guardrails(
    summaries: list[dict[str, object]],
    *,
    score_years: set[int],
    weights: ScoreWeights,
) -> float:
    """Execute the score rank guardrails routine."""
    if (
        weights.rank_overshoot_125 == 0.0
        and weights.rank_overshoot_150 == 0.0
        and weights.rank_hot_overshoot_125 == 0.0
        and weights.rank_hot_overshoot_150 == 0.0
    ):
        return 0.0
    guardrail_rows = rank_guardrail_rows(summaries, score_years=score_years, weights=weights)
    if not guardrail_rows:
        return 0.0
    average_penalties = []
    for row in guardrail_rows:
        penalty_125 = max(0.0, float(row["pct_ge_125"]) - weights.rank_overshoot_target_125)
        penalty_150 = max(0.0, float(row["pct_gt_150"]) - weights.rank_overshoot_target_150)
        average_penalties.append(
            weights.rank_overshoot_125 * penalty_125
            + weights.rank_overshoot_150 * penalty_150
        )
    hot_penalty_125 = max(
        max(0.0, float(row["pct_ge_125"]) - weights.rank_hot_overshoot_target_125)
        for row in guardrail_rows
    )
    hot_penalty_150 = max(
        max(0.0, float(row["pct_gt_150"]) - weights.rank_hot_overshoot_target_150)
        for row in guardrail_rows
    )
    return (
        statistics.fmean(average_penalties)
        + weights.rank_hot_overshoot_125 * hot_penalty_125
        + weights.rank_hot_overshoot_150 * hot_penalty_150
    )


def rank_guardrail_rows(
    summaries: list[dict[str, object]],
    *,
    score_years: set[int],
    weights: ScoreWeights,
) -> list[dict[str, object]]:
    """Execute the rank guardrail rows routine."""
    return [
        row
        for row in summaries
        if int(row["year"]) in score_years
        and row.get("slice_type") == "rating_band"
        and str(row.get("slice_value")) in weights.rank_guardrail_bands
        and int(row["players"]) >= weights.rank_guardrail_min_players
    ]


def aggregate_summaries(summaries: list[dict[str, object]], *, weights: ScoreWeights) -> dict[str, float]:
    """Execute the aggregate summaries routine."""
    final_year = max(int(item["year"]) for item in summaries)
    score_rows = [
        row
        for row in summaries
        if int(row["year"]) == final_year and row.get("slice_type", "all") == "all"
    ]
    rank_rows = rank_guardrail_rows(summaries, score_years={final_year}, weights=weights)
    if not rank_rows:
        rank_rows = score_rows
    return {
        "score_reached_rate": statistics.fmean(float(row["reached_rate"]) for row in score_rows),
        "score_within_sigma_rate": statistics.fmean(float(row["within_sigma_rate"]) for row in score_rows),
        "score_mean_rank_change_pct": statistics.fmean(float(row["mean_rank_change_pct"]) for row in score_rows),
        "score_mean_abs_gap_to_true": statistics.fmean(float(row["mean_abs_gap_to_true"]) for row in score_rows),
        "score_rms_gap_to_true": statistics.fmean(float(row["rms_gap_to_true"]) for row in score_rows),
        "score_gaussian_gap_nll": statistics.fmean(float(row["gaussian_gap_nll"]) for row in score_rows),
        "score_pct_ge_125": statistics.fmean(float(row["pct_ge_125"]) for row in score_rows),
        "score_pct_gt_150": statistics.fmean(float(row["pct_gt_150"]) for row in score_rows),
        "score_rank_pct_ge_125": statistics.fmean(float(row["pct_ge_125"]) for row in rank_rows),
        "score_rank_pct_gt_150": statistics.fmean(float(row["pct_gt_150"]) for row in rank_rows),
        "score_rank_hot_pct_ge_125": max(float(row["pct_ge_125"]) for row in rank_rows),
        "score_rank_hot_pct_gt_150": max(float(row["pct_gt_150"]) for row in rank_rows),
        "score_median_sigma": statistics.fmean(float(row["median_sigma"]) for row in score_rows),
    }


def flatten_config(config: BayrateConfig) -> dict[str, object]:
    """Execute the flatten config routine."""
    values = asdict(config)
    return {f"config_{key}": value for key, value in values.items()}


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
        for row in rows:
            writer.writerow(row)


def read_csv(path: Path) -> list[dict[str, object]]:
    """Read csv."""
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


class FixedTrial:
    """Represent fixed trial."""
    def __init__(self, params: dict[str, object]) -> None:
        """Initialize the fixed trial instance."""
        self.params = params

    def suggest_float(self, name: str, low: float, high: float) -> float:
        """Execute the suggest float routine."""
        value = self.params[name]
        if not isinstance(value, (int, float)):
            raise TypeError(f"Expected numeric fixed value for {name}")
        return float(value)

    def suggest_categorical(self, name: str, choices: list[object]) -> object:
        """Execute the suggest categorical routine."""
        value = self.params[name]
        if value not in choices:
            raise ValueError(f"Fixed value {value!r} for {name} is not in {choices!r}")
        return value


if __name__ == "__main__":
    main()
