"""Run an all-player activity grid for BayRate sigma experiments."""

from __future__ import annotations

import argparse
import csv
import gc
import json
import math
import random
import statistics
import time
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

from bayrate.core import (
    BayrateConfig,
    GameRecord,
    TdListEntry,
    close_boundary,
    load_games_from_csv,
    run_bayrate_loaded,
)
from research.bayrate_sigma.simulate_improver_catchup import (
    DEFAULT_DATASET,
    DEFAULT_OUTPUT_DIR,
    RatingSnapshot,
    SimulatedPlayer,
    build_encounter_templates,
    clone_td_list,
    guarded_surprise_config,
    guarded_surprise_min_pre_post_gated_base_config,
    guarded_surprise_min_pre_post_config,
    load_latest_ratings,
    rating_band,
    reached_true_strength,
    sigma_band,
    simulate_games,
    shift_by_closed_delta,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FULL_HISTORY = REPO_ROOT / "data" / "bayrate-sigma-full-history-20260526" / "games.csv"
DEFAULT_GRID_OUTPUT_DIR = DEFAULT_OUTPUT_DIR / "activity_grids"
ALGORITHMS = {
    "baseline": BayrateConfig(optimizer_random_jitter=0.0),
    "surprise_taper_floor_050": guarded_surprise_config(),
    "surprise_taper_min_pre_post_floor_050": guarded_surprise_min_pre_post_config(),
    "surprise_taper_min_pre_post_gated_base_floor_050": guarded_surprise_min_pre_post_gated_base_config(),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ratings", type=Path, default=DEFAULT_DATASET / "ratings.csv")
    parser.add_argument("--template-games", type=Path, default=DEFAULT_FULL_HISTORY)
    parser.add_argument("--last-game-source", type=Path, default=DEFAULT_FULL_HISTORY)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_GRID_OUTPUT_DIR)
    parser.add_argument("--name", default="one-rank-stronger-activity-grid")
    parser.add_argument("--seed", type=int, default=20260605)
    parser.add_argument("--start-date", type=lambda value: date.fromisoformat(value), default=date(2026, 6, 1))
    parser.add_argument("--active-since", type=lambda value: date.fromisoformat(value), default=date(2021, 6, 5))
    parser.add_argument("--years", type=float, default=3.0)
    parser.add_argument("--games-per-year-values", default="5,10,15,20,25,30,35,40,45,50")
    parser.add_argument(
        "--algorithms",
        default=",".join(ALGORITHMS),
        help="Comma-separated algorithm names to replay.",
    )
    parser.add_argument(
        "--config-json",
        type=Path,
        help="Run one custom BayrateConfig loaded from JSON instead of the named algorithm set.",
    )
    parser.add_argument("--config-name", default="custom_config", help="Algorithm label for --config-json output.")
    parser.add_argument("--true-strength-closed-delta", type=float, default=1.0)
    parser.add_argument(
        "--self-promote-closed-delta",
        type=float,
        default=0.0,
        help="Entry-rank shift for every simulated player, in closed rating scale ranks.",
    )
    parser.add_argument("--max-players", type=int, help="Optional cap for smoke tests.")
    parser.add_argument("--allow-online-games", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    output_dir = args.output_dir / args.name
    output_dir.mkdir(parents=True, exist_ok=True)
    progress_path = output_dir / "progress.json"
    activity_values = parse_activity_values(args.games_per_year_values)
    algorithms = (
        {args.config_name: load_config_from_json(args.config_json)}
        if args.config_json
        else select_algorithms(args.algorithms)
    )

    latest_ratings = load_latest_ratings(args.ratings)
    full_history_games = load_games_from_csv(
        args.last_game_source,
        BayrateConfig(allow_online_games=args.allow_online_games),
    )
    last_game_dates = last_game_dates_by_player(full_history_games)
    snapshots = active_snapshots(
        latest_ratings.values(),
        last_game_dates=last_game_dates,
        active_since=args.active_since,
        max_players=args.max_players,
    )
    players, initial_dates = build_players(
        snapshots,
        last_game_dates=last_game_dates,
        true_strength_closed_delta=args.true_strength_closed_delta,
        self_promote_closed_delta=args.self_promote_closed_delta,
    )

    template_games = (
        full_history_games
        if args.template_games == args.last_game_source
        else load_games_from_csv(args.template_games, BayrateConfig(allow_online_games=args.allow_online_games))
    )
    templates_by_band = build_encounter_templates(template_games)

    metadata = {
        "name": args.name,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "ratings": str(args.ratings),
        "template_games": str(args.template_games),
        "last_game_source": str(args.last_game_source),
        "seed": args.seed,
        "start_date": args.start_date.isoformat(),
        "active_since": args.active_since.isoformat(),
        "years": args.years,
        "games_per_year_values": activity_values,
        "true_strength_closed_delta": args.true_strength_closed_delta,
        "self_promote_closed_delta": args.self_promote_closed_delta,
        "allow_online_games": args.allow_online_games,
        "snapshot_count": len(latest_ratings),
        "active_snapshot_count": len(snapshots),
        "fallback_last_game_date_count": sum(1 for player in players if player.original_agaid not in last_game_dates),
        "algorithms": {name: asdict(config) for name, config in algorithms.items()},
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8")
    write_csv(output_dir / "players.csv", player_rows(players, initial_dates, snapshots_by_player_id(snapshots), args.start_date))

    milestone_path = output_dir / "milestones.csv"
    summary_path = output_dir / "summary_by_activity.csv"
    milestone_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []

    write_progress(
        progress_path,
        status="running",
        completed_scenarios=0,
        total_scenarios=len(activity_values),
        algorithms=list(algorithms),
        elapsed_seconds=time.perf_counter() - started,
    )

    for scenario_index, games_per_year in enumerate(activity_values, start=1):
        scenario_started = time.perf_counter()
        print(f"[{scenario_index}/{len(activity_values)}] {games_per_year} games/year: simulating games...", flush=True)
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
        rng = random.Random(args.seed + games_per_year * 10_000)
        simulated_games, _game_infos, initial_td_list = simulate_games(
            scenario_players,
            templates_by_band,
            latest_ratings,
            start_date=args.start_date,
            years=args.years,
            rng=rng,
            target_initial_dates=initial_dates,
        )
        simulated_games.sort(key=lambda game: (game.game_date, game.source_game_id))
        print(
            f"[{scenario_index}/{len(activity_values)}] {games_per_year} games/year: "
            f"{len(simulated_games):,} games generated.",
            flush=True,
        )

        scenario_milestones: list[dict[str, object]] = []
        for algorithm_name, config in algorithms.items():
            algorithm_started = time.perf_counter()
            print(f"  {algorithm_name}: replaying...", flush=True)
            result = run_bayrate_loaded(
                simulated_games,
                {},
                config,
                initial_td_list=clone_td_list(initial_td_list),
            )
            rows = milestone_rows_for_result(
                result.player_results,
                scenario_players,
                algorithm_name=algorithm_name,
                games_per_year=games_per_year,
                start_date=args.start_date,
                year_count=int(args.years),
                inactivity_growth_per_day=config.inactivity_growth_per_day,
            )
            scenario_milestones.extend(rows)
            algorithm_summary = summarize_milestones(rows, games_per_year=games_per_year, algorithm=algorithm_name)
            summary_rows.extend(algorithm_summary)
            print_summary(algorithm_summary, prefix=f"  {algorithm_name}")
            print(f"  {algorithm_name}: done in {time.perf_counter() - algorithm_started:.1f}s.", flush=True)
            del result
            gc.collect()

        milestone_rows.extend(scenario_milestones)
        write_csv(milestone_path, milestone_rows)
        write_csv(summary_path, summary_rows)
        write_progress(
            progress_path,
            status="running",
            completed_scenarios=scenario_index,
            total_scenarios=len(activity_values),
            last_games_per_year=games_per_year,
            last_scenario_seconds=time.perf_counter() - scenario_started,
            elapsed_seconds=time.perf_counter() - started,
            milestone_rows=len(milestone_rows),
        )
        del simulated_games
        del initial_td_list
        gc.collect()

    write_progress(
        progress_path,
        status="complete",
        completed_scenarios=len(activity_values),
        total_scenarios=len(activity_values),
        elapsed_seconds=time.perf_counter() - started,
        milestone_rows=len(milestone_rows),
    )
    print(f"Activity grid written to {output_dir}", flush=True)


def parse_activity_values(text: str) -> list[int]:
    values = [int(part.strip()) for part in text.split(",") if part.strip()]
    if not values:
        raise ValueError("At least one games-per-year value is required")
    if any(value <= 0 for value in values):
        raise ValueError("Games-per-year values must be positive")
    return values


def select_algorithms(text: str) -> dict[str, BayrateConfig]:
    names = [part.strip() for part in text.split(",") if part.strip()]
    if not names:
        raise ValueError("At least one algorithm name is required")
    unknown = [name for name in names if name not in ALGORITHMS]
    if unknown:
        allowed = ", ".join(ALGORITHMS)
        raise ValueError(f"Unknown algorithm(s): {', '.join(unknown)}. Allowed: {allowed}")
    return {name: ALGORITHMS[name] for name in names}


def load_config_from_json(path: Path) -> BayrateConfig:
    payload = json.loads(path.read_text(encoding="utf-8"))
    values = payload.get("config", payload)
    if not isinstance(values, dict):
        raise ValueError(f"Config JSON must contain an object: {path}")
    allowed = set(BayrateConfig.__dataclass_fields__)
    return BayrateConfig(**{key: value for key, value in values.items() if key in allowed})


def last_game_dates_by_player(games: Iterable[GameRecord]) -> dict[int, date]:
    dates: dict[int, date] = {}
    for game in games:
        dates[game.white_agaid] = max(dates.get(game.white_agaid, game.game_date), game.game_date)
        dates[game.black_agaid] = max(dates.get(game.black_agaid, game.game_date), game.game_date)
    return dates


def active_snapshots(
    snapshots: Iterable[RatingSnapshot],
    *,
    last_game_dates: dict[int, date],
    active_since: date,
    max_players: int | None,
) -> list[RatingSnapshot]:
    selected = [
        snapshot
        for snapshot in snapshots
        if last_game_dates.get(snapshot.player_id, snapshot.rating_date) >= active_since
    ]
    selected.sort(key=lambda snapshot: snapshot.player_id)
    if max_players is not None:
        selected = selected[:max_players]
    return selected


def build_players(
    snapshots: list[RatingSnapshot],
    *,
    last_game_dates: dict[int, date],
    true_strength_closed_delta: float,
    self_promote_closed_delta: float,
) -> tuple[list[SimulatedPlayer], dict[int, date]]:
    players: list[SimulatedPlayer] = []
    initial_dates: dict[int, date] = {}
    next_id = 10_000_000
    for snapshot in snapshots:
        player = SimulatedPlayer(
            simulation_player_id=next_id,
            original_agaid=snapshot.player_id,
            replication=0,
            rating_band=rating_band(snapshot.rating),
            sigma_band=sigma_band(snapshot.sigma),
            activity_label="activity_grid",
            games_per_year=0,
            self_promote=self_promote_closed_delta != 0.0,
            start_rating=snapshot.rating,
            start_sigma=snapshot.sigma,
            true_strength=shift_by_closed_delta(snapshot.rating, true_strength_closed_delta),
            entry_rating=shift_by_closed_delta(snapshot.rating, self_promote_closed_delta),
        )
        players.append(player)
        initial_dates[player.simulation_player_id] = last_game_dates.get(snapshot.player_id, snapshot.rating_date)
        next_id += 1
    return players, initial_dates


def snapshots_by_player_id(snapshots: list[RatingSnapshot]) -> dict[int, RatingSnapshot]:
    return {snapshot.player_id: snapshot for snapshot in snapshots}


def player_rows(
    players: list[SimulatedPlayer],
    initial_dates: dict[int, date],
    snapshots: dict[int, RatingSnapshot],
    start_date: date,
) -> list[dict[str, object]]:
    rows = []
    for player in players:
        snapshot = snapshots[player.original_agaid]
        initial_date = initial_dates[player.simulation_player_id]
        rows.append(
            {
                "simulation_player_id": player.simulation_player_id,
                "original_agaid": player.original_agaid,
                "start_rating": player.start_rating,
                "start_sigma": player.start_sigma,
                "true_strength": player.true_strength,
                "entry_rating": player.entry_rating,
                "self_promote": player.self_promote,
                "rating_band": player.rating_band,
                "sigma_band": player.sigma_band,
                "snapshot_rating_date": snapshot.rating_date.isoformat(),
                "snapshot_row_id": snapshot.row_id,
                "initial_last_game_date": initial_date.isoformat(),
                "days_since_initial_to_start": max(0, (start_date - initial_date).days),
            }
        )
    return rows


def milestone_rows_for_result(
    player_results,
    players: list[SimulatedPlayer],
    *,
    algorithm_name: str,
    games_per_year: int,
    start_date: date,
    year_count: int,
    inactivity_growth_per_day: float,
) -> list[dict[str, object]]:
    target_ids = {player.simulation_player_id for player in players}
    rows_by_player = {player_id: [] for player_id in target_ids}
    for row in player_results:
        if row.player_id in target_ids:
            rows_by_player[row.player_id].append(row)
    for rows in rows_by_player.values():
        rows.sort(key=lambda row: (row.event_date, row.event_key))

    player_by_id = {player.simulation_player_id: player for player in players}
    output: list[dict[str, object]] = []
    for player_id, rows in rows_by_player.items():
        player = player_by_id[player_id]
        index = 0
        state_rating = player.start_rating
        state_sigma = player.start_sigma
        state_date = start_date
        for year in range(1, year_count + 1):
            milestone_date = start_date + timedelta(days=365 * year)
            while index < len(rows) and rows[index].event_date <= milestone_date:
                state_rating = rows[index].rating_after
                state_sigma = rows[index].sigma_after
                state_date = rows[index].event_date
                index += 1
            day_count = max(0, (milestone_date - state_date).days)
            sigma_at_milestone = math.sqrt(
                state_sigma * state_sigma + (inactivity_growth_per_day * day_count) ** 2
            )
            rating_delta = close_boundary(state_rating) - close_boundary(player.start_rating)
            gap_to_true = close_boundary(player.true_strength) - close_boundary(state_rating)
            output.append(
                {
                    "games_per_year": games_per_year,
                    "algorithm": algorithm_name,
                    "year": year,
                    "milestone_date": milestone_date.isoformat(),
                    "simulation_player_id": player.simulation_player_id,
                    "original_agaid": player.original_agaid,
                    "start_rating": player.start_rating,
                    "start_sigma": player.start_sigma,
                    "true_strength": player.true_strength,
                    "rating_after": state_rating,
                    "sigma_after_state_event": state_sigma,
                    "sigma_at_milestone": sigma_at_milestone,
                    "state_event_date": state_date.isoformat(),
                    "rating_delta_from_start": rating_delta,
                    "gap_to_true": gap_to_true,
                    "reached_true_strength": reached_true_strength(state_rating, player.true_strength),
                }
            )
    return output


def summarize_milestones(rows: list[dict[str, object]], *, games_per_year: int, algorithm: str) -> list[dict[str, object]]:
    output = []
    for year in sorted({int(row["year"]) for row in rows}):
        year_rows = [row for row in rows if int(row["year"]) == year]
        reached = [row for row in year_rows if row["reached_true_strength"]]
        gaps = [float(row["gap_to_true"]) for row in year_rows]
        deltas = [float(row["rating_delta_from_start"]) for row in year_rows]
        sigmas = [float(row["sigma_at_milestone"]) for row in year_rows]
        output.append(
            {
                "games_per_year": games_per_year,
                "algorithm": algorithm,
                "year": year,
                "players": len(year_rows),
                "reached_count": len(reached),
                "reached_rate": len(reached) / len(year_rows) if year_rows else None,
                "rating_delta_mean": statistics.fmean(deltas) if deltas else None,
                "rating_delta_median": statistics.median(deltas) if deltas else None,
                "gap_to_true_mean": statistics.fmean(gaps) if gaps else None,
                "gap_to_true_median": statistics.median(gaps) if gaps else None,
                "sigma_mean": statistics.fmean(sigmas) if sigmas else None,
                "sigma_median": statistics.median(sigmas) if sigmas else None,
            }
        )
    return output


def print_summary(rows: list[dict[str, object]], *, prefix: str) -> None:
    parts = []
    for row in rows:
        parts.append(
            f"Y{row['year']} reached={row['reached_rate']:.1%}, "
            f"median_gap={row['gap_to_true_median']:.3f}, "
            f"median_sigma={row['sigma_median']:.3f}"
        )
    print(f"{prefix}: " + " | ".join(parts), flush=True)


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
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
    path.write_text(json.dumps(fields, indent=2, default=str) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
