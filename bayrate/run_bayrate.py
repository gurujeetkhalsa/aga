"""Command-line entry point for the clean Python BayRate port."""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date
from pathlib import Path

try:
    from .core import BayrateConfig, CsvValidationError, result_to_json, run_bayrate
except ImportError:  # pragma: no cover - supports direct script execution
    from core import BayrateConfig, CsvValidationError, result_to_json, run_bayrate


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the clean Python BayRate calculation.")
    parser.add_argument("--games", required=True, type=Path, help="CSV export of rated game rows.")
    parser.add_argument("--ratings", required=True, type=Path, help="CSV export of prior rating rows.")
    parser.add_argument("--output", type=Path, help="Write JSON output to this file instead of stdout.")
    parser.add_argument("--allow-online-games", action="store_true", help="Include games marked as online.")
    parser.add_argument("--min-game-date", help="Earliest game date to include, YYYY-MM-DD.")
    parser.add_argument("--max-game-date", help="Latest game date to include, YYYY-MM-DD.")
    parser.add_argument("--max-events", type=int, help="Limit processing to the first N events.")
    parser.add_argument(
        "--inactivity-growth-per-day",
        type=float,
        default=BayrateConfig().inactivity_growth_per_day,
        help="Daily sigma growth for inactive players.",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    config = replace(
        BayrateConfig(),
        allow_online_games=args.allow_online_games,
        max_events=args.max_events,
        inactivity_growth_per_day=args.inactivity_growth_per_day,
    )
    if args.min_game_date:
        config.min_game_date = date.fromisoformat(args.min_game_date)
    if args.max_game_date:
        config.max_game_date = date.fromisoformat(args.max_game_date)

    try:
        payload = result_to_json(run_bayrate(args.games, args.ratings, config))
    except CsvValidationError as exc:
        parser.error(str(exc))
    if args.output:
        args.output.write_text(payload + "\n", encoding="utf-8")
    else:
        print(payload)


if __name__ == "__main__":
    main()
