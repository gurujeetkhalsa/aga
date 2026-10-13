import argparse
import sys
from datetime import datetime
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from function2.bayrate import BayrateConfig, result_to_json, run_bayrate


def _parse_date(value: str | None):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Bayrate port against local AGA CSV data.")
    parser.add_argument("--games", type=Path, default=Path("games.csv"))
    parser.add_argument("--ratings", type=Path, default=Path("aga-ratings.csv"))
    parser.add_argument("--max-events", type=int, default=None)
    parser.add_argument("--min-game-date", type=str, default=None)
    parser.add_argument("--max-game-date", type=str, default=None)
    parser.add_argument("--allow-online-games", action="store_true")
    parser.add_argument("--posterior-sigma-floor", type=float, default=None)
    parser.add_argument("--posterior-sigma-multiplier", type=float, default=1.0)
    parser.add_argument("--inactivity-growth-per-day", type=float, default=0.0005)
    parser.add_argument("--performance-trigger-threshold", type=float, default=None)
    parser.add_argument("--performance-trigger-sigma-scale", type=float, default=0.0)
    parser.add_argument("--performance-trigger-sigma-cap", type=float, default=None)
    parser.add_argument("--performance-trigger-low-sigma-reference", type=float, default=None)
    parser.add_argument("--performance-trigger-low-sigma-power", type=float, default=0.0)
    parser.add_argument("--performance-trigger-low-sigma-rating-taper-start", type=float, default=None)
    parser.add_argument("--performance-trigger-low-sigma-rating-taper-end", type=float, default=None)
    parser.add_argument("--performance-trigger-momentum-min-streak", type=int, default=0)
    parser.add_argument("--performance-trigger-momentum-residual-threshold", type=float, default=None)
    parser.add_argument("--performance-trigger-momentum-sigma-scale", type=float, default=0.0)
    parser.add_argument("--performance-trigger-momentum-sigma-cap", type=float, default=None)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    config = BayrateConfig(
        allow_online_games=args.allow_online_games,
        min_game_date=_parse_date(args.min_game_date),
        max_game_date=_parse_date(args.max_game_date),
        max_events=args.max_events,
        posterior_sigma_floor=args.posterior_sigma_floor,
        posterior_sigma_multiplier=args.posterior_sigma_multiplier,
        inactivity_growth_per_day=args.inactivity_growth_per_day,
        performance_trigger_threshold=args.performance_trigger_threshold,
        performance_trigger_sigma_scale=args.performance_trigger_sigma_scale,
        performance_trigger_sigma_cap=args.performance_trigger_sigma_cap,
        performance_trigger_low_sigma_reference=args.performance_trigger_low_sigma_reference,
        performance_trigger_low_sigma_power=args.performance_trigger_low_sigma_power,
        performance_trigger_low_sigma_rating_taper_start=args.performance_trigger_low_sigma_rating_taper_start,
        performance_trigger_low_sigma_rating_taper_end=args.performance_trigger_low_sigma_rating_taper_end,
        performance_trigger_momentum_min_streak=args.performance_trigger_momentum_min_streak,
        performance_trigger_momentum_residual_threshold=args.performance_trigger_momentum_residual_threshold,
        performance_trigger_momentum_sigma_scale=args.performance_trigger_momentum_sigma_scale,
        performance_trigger_momentum_sigma_cap=args.performance_trigger_momentum_sigma_cap,
    )
    result = run_bayrate(args.games, args.ratings, config)
    payload = result_to_json(result)
    if args.output is not None:
        args.output.write_text(payload, encoding="utf-8")
    print(payload)


if __name__ == "__main__":
    main()




