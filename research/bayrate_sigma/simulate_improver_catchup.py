"""Simulate rating changes under configurable hidden player strength.

The simulation samples real players from a ratings CSV, gives each sampled
player a hidden true strength relative to their starting rating, then generates
rated games with empirical opponent/handicap templates from the games CSV. It
runs the same simulated games through BayRate baseline and an optional
surprise-driven sigma rule, then reports how quickly each run reaches the hidden
strength target.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

from bayrate.core import (
    BayrateConfig,
    GameRecord,
    TdListEntry,
    calc_handicap_eqv,
    close_boundary,
    load_games_from_csv,
    normal_win_probability,
    open_boundary,
    run_bayrate_loaded,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET = REPO_ROOT / "data" / "bayrate-sigma-20260526"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "bayrate" / "output" / "simulations"


@dataclass(frozen=True)
class RatingSnapshot:
    player_id: int
    rating: float
    sigma: float
    rating_date: date
    row_id: int


@dataclass(frozen=True)
class EncounterTemplate:
    rating_band: str
    anchor_seed: float
    opponent_seed: float
    target_is_white: bool
    handicap: int
    komi: float


@dataclass(frozen=True)
class SimulatedPlayer:
    simulation_player_id: int
    original_agaid: int
    replication: int
    rating_band: str
    sigma_band: str
    activity_label: str
    games_per_year: int
    self_promote: bool
    start_rating: float
    start_sigma: float
    true_strength: float
    entry_rating: float


@dataclass(frozen=True)
class SimulatedGameInfo:
    source_game_id: int
    simulation_player_id: int
    original_agaid: int
    game_number: int
    game_date: date
    target_is_white: bool
    target_entry_seed: float
    opponent_entry_seed: float
    target_true_strength: float
    opponent_true_strength: float
    target_win_probability: float
    target_won: bool
    handicap: int
    komi: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--games", type=Path, default=DEFAULT_DATASET / "games.csv")
    parser.add_argument("--ratings", type=Path, default=DEFAULT_DATASET / "ratings.csv")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--name", default="one-rank-improver")
    parser.add_argument("--seed", type=int, default=20260531)
    parser.add_argument("--start-date", type=lambda value: date.fromisoformat(value), default=date(2026, 6, 1))
    parser.add_argument("--years", type=float, default=3.0)
    parser.add_argument("--players-per-cell", type=int, default=2)
    parser.add_argument("--replications", type=int, default=2)
    parser.add_argument("--self-promote-rate", type=float, default=0.5)
    parser.add_argument(
        "--true-strength-closed-delta",
        type=float,
        default=1.0,
        help="Hidden true-strength shift in closed rating scale ranks. Use 0 for no hidden improvement.",
    )
    parser.add_argument(
        "--self-promote-closed-delta",
        type=float,
        help=(
            "Entry-rank shift for self-promoted players in closed rating scale ranks. "
            "Defaults to --true-strength-closed-delta."
        ),
    )
    parser.add_argument(
        "--activity-levels",
        default="low:8,medium:24,high:50",
        help="Comma-separated label:games_per_year values.",
    )
    parser.add_argument("--allow-online-games", action="store_true")
    parser.add_argument("--max-players", type=int, help="Optional cap after stratified sampling.")
    return parser.parse_args()


def rating_band(rating: float) -> str:
    if rating < -20:
        return "<20k"
    if rating < -10:
        return "20k_to_<10k"
    if rating < -5:
        return "10k_to_<5k"
    if rating < 1:
        return "5k_to_<1d"
    if rating < 3:
        return "1d_to_<3d"
    if rating < 5:
        return "3d_to_<5d"
    if rating < 6:
        return "5d_to_<6d"
    if rating < 7:
        return "6d_to_<7d"
    if rating < 8:
        return "7d_to_<8d"
    return "8d+"


def sigma_band(sigma: float) -> str:
    if sigma < 0.25:
        return "narrow_<0.25"
    if sigma < 0.50:
        return "medium_0.25_to_<0.50"
    if sigma < 0.90:
        return "wide_0.50_to_<0.90"
    return "very_wide_>=0.90"


def one_rank_stronger(rating: float) -> float:
    return open_boundary(close_boundary(rating) + 1.0)


def shift_by_closed_delta(rating: float, delta: float) -> float:
    return open_boundary(close_boundary(rating) + delta)


def parse_activity_levels(text: str) -> list[tuple[str, int]]:
    levels: list[tuple[str, int]] = []
    for part in text.split(","):
        if not part.strip():
            continue
        label, _, value = part.partition(":")
        if not label or not value:
            raise ValueError(f"Invalid activity level {part!r}; expected label:games_per_year")
        games_per_year = int(value)
        if games_per_year <= 0:
            raise ValueError("games_per_year must be positive")
        levels.append((label.strip(), games_per_year))
    if not levels:
        raise ValueError("At least one activity level is required")
    return levels


def load_latest_ratings(path: Path) -> dict[int, RatingSnapshot]:
    latest: dict[int, RatingSnapshot] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for row_number, row in enumerate(reader, start=1):
            if len(row) < 6:
                continue
            try:
                player_id = int(row[0])
                rating = float(row[1])
                sigma = float(row[2])
                rating_date = date.fromisoformat(row[3][:10])
                row_id = int(row[5])
            except ValueError:
                continue
            snapshot = RatingSnapshot(player_id, rating, sigma, rating_date, row_id)
            previous = latest.get(player_id)
            if previous is None or (snapshot.rating_date, snapshot.row_id) > (previous.rating_date, previous.row_id):
                latest[player_id] = snapshot
    return latest


def build_encounter_templates(games: list[GameRecord]) -> dict[str, list[EncounterTemplate]]:
    templates: dict[str, list[EncounterTemplate]] = {}
    for game in games:
        white_template = EncounterTemplate(
            rating_band=rating_band(game.white_seed_rank),
            anchor_seed=game.white_seed_rank,
            opponent_seed=game.black_seed_rank,
            target_is_white=True,
            handicap=game.handicap,
            komi=game.komi,
        )
        black_template = EncounterTemplate(
            rating_band=rating_band(game.black_seed_rank),
            anchor_seed=game.black_seed_rank,
            opponent_seed=game.white_seed_rank,
            target_is_white=False,
            handicap=game.handicap,
            komi=game.komi,
        )
        templates.setdefault(white_template.rating_band, []).append(white_template)
        templates.setdefault(black_template.rating_band, []).append(black_template)
    return templates


def sample_base_players(
    latest_ratings: dict[int, RatingSnapshot],
    *,
    players_per_cell: int,
    rng: random.Random,
    max_players: int | None,
) -> list[RatingSnapshot]:
    cells: dict[tuple[str, str], list[RatingSnapshot]] = {}
    for snapshot in latest_ratings.values():
        cells.setdefault((rating_band(snapshot.rating), sigma_band(snapshot.sigma)), []).append(snapshot)

    selected: list[RatingSnapshot] = []
    for key in sorted(cells):
        snapshots = cells[key]
        snapshots.sort(key=lambda item: item.player_id)
        count = min(players_per_cell, len(snapshots))
        selected.extend(rng.sample(snapshots, count))
    selected.sort(key=lambda item: (rating_band(item.rating), sigma_band(item.sigma), item.player_id))
    if max_players is not None and len(selected) > max_players:
        selected = rng.sample(selected, max_players)
        selected.sort(key=lambda item: (rating_band(item.rating), sigma_band(item.sigma), item.player_id))
    return selected


def expand_simulated_players(
    base_players: list[RatingSnapshot],
    *,
    replications: int,
    activity_levels: list[tuple[str, int]],
    self_promote_rate: float,
    true_strength_closed_delta: float,
    self_promote_closed_delta: float,
    rng: random.Random,
) -> list[SimulatedPlayer]:
    players: list[SimulatedPlayer] = []
    next_id = 10_000_000
    for snapshot in base_players:
        for replication in range(replications):
            activity_label, games_per_year = activity_levels[(len(players) + replication) % len(activity_levels)]
            self_promote = rng.random() < self_promote_rate
            players.append(
                SimulatedPlayer(
                    simulation_player_id=next_id,
                    original_agaid=snapshot.player_id,
                    replication=replication,
                    rating_band=rating_band(snapshot.rating),
                    sigma_band=sigma_band(snapshot.sigma),
                    activity_label=activity_label,
                    games_per_year=games_per_year,
                    self_promote=self_promote,
                    start_rating=snapshot.rating,
                    start_sigma=snapshot.sigma,
                    true_strength=shift_by_closed_delta(snapshot.rating, true_strength_closed_delta),
                    entry_rating=shift_by_closed_delta(snapshot.rating, self_promote_closed_delta)
                    if self_promote
                    else snapshot.rating,
                )
            )
            next_id += 1
    return players


def scheduled_dates(start_date: date, games_per_year: int, years: float, rng: random.Random) -> list[date]:
    game_count = max(1, round(games_per_year * years))
    mean_gap = 365.0 / games_per_year
    current_day = 0.0
    dates: list[date] = []
    for _ in range(game_count):
        # Use variable gaps so equally active players do not all follow the same cadence.
        gap = rng.expovariate(1.0 / mean_gap)
        current_day += max(1.0, gap)
        max_day = years * 365.0
        if current_day > max_day:
            current_day = max_day
        dates.append(start_date + timedelta(days=round(current_day)))
    return dates


def opponent_sigma_for_rating(latest_ratings: dict[int, RatingSnapshot], rating: float, rng: random.Random) -> float:
    band = rating_band(rating)
    sigmas = [snapshot.sigma for snapshot in latest_ratings.values() if rating_band(snapshot.rating) == band]
    if not sigmas:
        return 0.50
    return rng.choice(sigmas)


def simulate_games(
    players: list[SimulatedPlayer],
    templates_by_band: dict[str, list[EncounterTemplate]],
    latest_ratings: dict[int, RatingSnapshot],
    *,
    start_date: date,
    years: float,
    rng: random.Random,
    target_initial_dates: dict[int, date] | None = None,
) -> tuple[list[GameRecord], list[SimulatedGameInfo], dict[int, TdListEntry]]:
    games: list[GameRecord] = []
    infos: list[SimulatedGameInfo] = []
    initial_td_list: dict[int, TdListEntry] = {}
    all_templates = [template for templates in templates_by_band.values() for template in templates]
    source_game_id = 900_000_000
    opponent_id = 20_000_000
    initial_date = start_date - timedelta(days=1)

    for player in players:
        initial_td_list[player.simulation_player_id] = TdListEntry(
            player_id=player.simulation_player_id,
            rating=player.start_rating,
            sigma=player.start_sigma,
            last_rating_date=(target_initial_dates or {}).get(player.simulation_player_id, initial_date),
        )
        target_entry_seed = player.entry_rating
        dates = scheduled_dates(start_date, player.games_per_year, years, rng)
        templates = templates_by_band.get(player.rating_band) or all_templates
        for game_number, game_date in enumerate(dates, start=1):
            template = rng.choice(templates)
            opponent_delta = close_boundary(template.opponent_seed) - close_boundary(template.anchor_seed)
            opponent_rating = shift_by_closed_delta(player.start_rating, opponent_delta)
            opponent_sigma = opponent_sigma_for_rating(latest_ratings, opponent_rating, rng)
            initial_td_list[opponent_id] = TdListEntry(
                player_id=opponent_id,
                rating=opponent_rating,
                sigma=opponent_sigma,
                last_rating_date=initial_date,
            )

            if template.target_is_white:
                white_id = player.simulation_player_id
                black_id = opponent_id
                white_seed = target_entry_seed
                black_seed = opponent_rating
                white_true = player.true_strength
                black_true = opponent_rating
            else:
                white_id = opponent_id
                black_id = player.simulation_player_id
                white_seed = opponent_rating
                black_seed = target_entry_seed
                white_true = opponent_rating
                black_true = player.true_strength

            handicap_eqv, sigma_px = calc_handicap_eqv(template.handicap, template.komi)
            p_white = normal_win_probability(white_true - black_true - handicap_eqv, sigma_px)
            if template.target_is_white:
                target_win_probability = p_white
                target_won = rng.random() < target_win_probability
                white_wins = target_won
            else:
                target_win_probability = 1.0 - p_white
                target_won = rng.random() < target_win_probability
                white_wins = not target_won

            games.append(
                GameRecord(
                    source_game_id=source_game_id,
                    tournament_code=f"sim{source_game_id}",
                    game_date=game_date,
                    round_number=1,
                    white_agaid=white_id,
                    black_agaid=black_id,
                    white_seed_rank=white_seed,
                    black_seed_rank=black_seed,
                    handicap=template.handicap,
                    komi=template.komi,
                    white_wins=white_wins,
                    is_online_game=False,
                )
            )
            infos.append(
                SimulatedGameInfo(
                    source_game_id=source_game_id,
                    simulation_player_id=player.simulation_player_id,
                    original_agaid=player.original_agaid,
                    game_number=game_number,
                    game_date=game_date,
                    target_is_white=template.target_is_white,
                    target_entry_seed=target_entry_seed,
                    opponent_entry_seed=opponent_rating,
                    target_true_strength=player.true_strength,
                    opponent_true_strength=opponent_rating,
                    target_win_probability=target_win_probability,
                    target_won=target_won,
                    handicap=template.handicap,
                    komi=template.komi,
                )
            )
            source_game_id += 1
            opponent_id += 1
    return games, infos, initial_td_list


def clone_td_list(td_list: dict[int, TdListEntry]) -> dict[int, TdListEntry]:
    return {
        player_id: TdListEntry(
            player_id=entry.player_id,
            rating=entry.rating,
            sigma=entry.sigma,
            last_rating_date=entry.last_rating_date,
            surprise_persistence_score=entry.surprise_persistence_score,
            temporary_sigma_boost=entry.temporary_sigma_boost,
        )
        for player_id, entry in td_list.items()
    }


def baseline_config() -> BayrateConfig:
    return BayrateConfig(optimizer_random_jitter=0.0)


def guarded_surprise_config() -> BayrateConfig:
    return BayrateConfig(
        optimizer_random_jitter=0.0,
        surprise_sigma_base=0.24,
        surprise_sigma_scale=0.40,
        surprise_sigma_deadband=0.35,
        surprise_sigma_cap=0.55,
        surprise_taper_start_rating=6.0,
        surprise_taper_end_rating=8.0,
        surprise_taper_floor=0.50,
    )


def guarded_surprise_min_pre_post_config() -> BayrateConfig:
    config = guarded_surprise_config()
    config.surprise_sigma_score_mode = "min_pre_post"
    return config


def guarded_surprise_min_pre_post_gated_base_config() -> BayrateConfig:
    config = guarded_surprise_min_pre_post_config()
    config.surprise_sigma_gate_base_by_deadband = True
    return config


def run_algorithms(
    games: list[GameRecord],
    initial_td_list: dict[int, TdListEntry],
) -> dict[str, object]:
    algorithms = {
        "baseline": baseline_config(),
        "surprise_taper_floor_050": guarded_surprise_config(),
        "surprise_taper_min_pre_post_floor_050": guarded_surprise_min_pre_post_config(),
        "surprise_taper_min_pre_post_gated_base_floor_050": guarded_surprise_min_pre_post_gated_base_config(),
    }
    results: dict[str, object] = {}
    for name, config in algorithms.items():
        results[name] = run_bayrate_loaded(
            games,
            {},
            config,
            initial_td_list=clone_td_list(initial_td_list),
        )
    return results


def target_result_rows(result: object, players: list[SimulatedPlayer]) -> dict[int, list[object]]:
    target_ids = {player.simulation_player_id for player in players}
    rows: dict[int, list[object]] = {player_id: [] for player_id in target_ids}
    for row in result.player_results:  # type: ignore[attr-defined]
        if row.player_id in target_ids:
            rows[row.player_id].append(row)
    for player_rows in rows.values():
        player_rows.sort(key=lambda row: (row.event_date, row.event_key))
    return rows


def reached_true_strength(rating: float, true_strength: float) -> bool:
    return close_boundary(rating) >= close_boundary(true_strength)


def summarize_player_outcomes(
    algorithm: str,
    result: object,
    players: list[SimulatedPlayer],
) -> list[dict[str, object]]:
    rows_by_player = target_result_rows(result, players)
    output: list[dict[str, object]] = []
    player_by_id = {player.simulation_player_id: player for player in players}
    for player_id, rows in rows_by_player.items():
        player = player_by_id[player_id]
        reached_row = None
        for index, row in enumerate(rows, start=1):
            if reached_true_strength(row.rating_after, player.true_strength):
                reached_row = (index, row)
                break
        final_row = rows[-1] if rows else None
        output.append(
            {
                "algorithm": algorithm,
                "simulation_player_id": player.simulation_player_id,
                "original_agaid": player.original_agaid,
                "replication": player.replication,
                "rating_band": player.rating_band,
                "sigma_band": player.sigma_band,
                "activity_label": player.activity_label,
                "games_per_year": player.games_per_year,
                "self_promote": player.self_promote,
                "start_rating": player.start_rating,
                "start_sigma": player.start_sigma,
                "true_strength": player.true_strength,
                "simulated_games": len(rows),
                "reached": reached_row is not None,
                "games_to_reach": None if reached_row is None else reached_row[0],
                "days_to_reach": None if reached_row is None else (reached_row[1].event_date - rows[0].event_date).days,
                "final_rating": None if final_row is None else final_row.rating_after,
                "final_sigma": None if final_row is None else final_row.sigma_after,
                "final_gap_to_true": None if final_row is None else close_boundary(player.true_strength) - close_boundary(final_row.rating_after),
            }
        )
    return output


def trajectory_rows(algorithm: str, result: object, players: list[SimulatedPlayer]) -> list[dict[str, object]]:
    rows_by_player = target_result_rows(result, players)
    player_by_id = {player.simulation_player_id: player for player in players}
    output: list[dict[str, object]] = []
    for player_id, rows in rows_by_player.items():
        player = player_by_id[player_id]
        for game_number, row in enumerate(rows, start=1):
            output.append(
                {
                    "algorithm": algorithm,
                    "simulation_player_id": player.simulation_player_id,
                    "original_agaid": player.original_agaid,
                    "replication": player.replication,
                    "game_number": game_number,
                    "event_date": row.event_date.isoformat(),
                    "rating_after": row.rating_after,
                    "sigma_after": row.sigma_after,
                    "true_strength": player.true_strength,
                    "gap_to_true": close_boundary(player.true_strength) - close_boundary(row.rating_after),
                    "reached": reached_true_strength(row.rating_after, player.true_strength),
                }
            )
    return output


def summarize_slice(rows: list[dict[str, object]], slice_type: str, slice_value: str) -> dict[str, object]:
    reached = [row for row in rows if row["reached"]]
    games_to_reach = [int(row["games_to_reach"]) for row in reached if row["games_to_reach"] is not None]
    days_to_reach = [int(row["days_to_reach"]) for row in reached if row["days_to_reach"] is not None]
    final_gaps = [float(row["final_gap_to_true"]) for row in rows if row["final_gap_to_true"] is not None]
    return {
        "algorithm": rows[0]["algorithm"] if rows else "",
        "slice_type": slice_type,
        "slice_value": slice_value,
        "players": len(rows),
        "reached_count": len(reached),
        "reached_rate": len(reached) / len(rows) if rows else None,
        "games_to_reach_median": statistics.median(games_to_reach) if games_to_reach else None,
        "games_to_reach_mean": statistics.fmean(games_to_reach) if games_to_reach else None,
        "days_to_reach_median": statistics.median(days_to_reach) if days_to_reach else None,
        "final_gap_to_true_mean": statistics.fmean(final_gaps) if final_gaps else None,
        "final_gap_to_true_median": statistics.median(final_gaps) if final_gaps else None,
    }


def category_summaries(player_outcomes: list[dict[str, object]]) -> list[dict[str, object]]:
    summaries: list[dict[str, object]] = []
    algorithms = sorted({str(row["algorithm"]) for row in player_outcomes})
    slice_fields = ["rating_band", "sigma_band", "activity_label", "self_promote"]
    for algorithm in algorithms:
        algorithm_rows = [row for row in player_outcomes if row["algorithm"] == algorithm]
        summaries.append(summarize_slice(algorithm_rows, "all", "all"))
        for field in slice_fields:
            for value in sorted({str(row[field]) for row in algorithm_rows}):
                rows = [row for row in algorithm_rows if str(row[field]) == value]
                summaries.append(summarize_slice(rows, field, value))
        for rating_value in sorted({str(row["rating_band"]) for row in algorithm_rows}):
            for sigma_value in sorted({str(row["sigma_band"]) for row in algorithm_rows}):
                rows = [
                    row
                    for row in algorithm_rows
                    if str(row["rating_band"]) == rating_value and str(row["sigma_band"]) == sigma_value
                ]
                if rows:
                    summaries.append(summarize_slice(rows, "rating_band|sigma_band", f"{rating_value}|{sigma_value}"))
    return summaries


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def simulation_player_rows(players: list[SimulatedPlayer]) -> list[dict[str, object]]:
    return [asdict(player) for player in players]


def simulated_game_rows(infos: list[SimulatedGameInfo]) -> list[dict[str, object]]:
    rows = []
    for info in infos:
        row = asdict(info)
        row["game_date"] = info.game_date.isoformat()
        rows.append(row)
    return rows


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    rng = random.Random(args.seed)
    activity_levels = parse_activity_levels(args.activity_levels)
    latest_ratings = load_latest_ratings(args.ratings)
    games = load_games_from_csv(
        args.games,
        BayrateConfig(allow_online_games=args.allow_online_games),
    )
    templates_by_band = build_encounter_templates(games)
    base_players = sample_base_players(
        latest_ratings,
        players_per_cell=args.players_per_cell,
        rng=rng,
        max_players=args.max_players,
    )
    simulated_players = expand_simulated_players(
        base_players,
        replications=args.replications,
        activity_levels=activity_levels,
        self_promote_rate=args.self_promote_rate,
        true_strength_closed_delta=args.true_strength_closed_delta,
        self_promote_closed_delta=args.self_promote_closed_delta
        if args.self_promote_closed_delta is not None
        else args.true_strength_closed_delta,
        rng=rng,
    )
    simulated_games, game_infos, initial_td_list = simulate_games(
        simulated_players,
        templates_by_band,
        latest_ratings,
        start_date=args.start_date,
        years=args.years,
        rng=rng,
    )
    simulated_games.sort(key=lambda game: (game.game_date, game.source_game_id))
    results = run_algorithms(simulated_games, initial_td_list)

    output_dir = args.output_dir / args.name
    output_dir.mkdir(parents=True, exist_ok=True)
    all_player_outcomes: list[dict[str, object]] = []
    all_trajectories: list[dict[str, object]] = []
    for algorithm, result in results.items():
        all_player_outcomes.extend(summarize_player_outcomes(algorithm, result, simulated_players))
        all_trajectories.extend(trajectory_rows(algorithm, result, simulated_players))

    summaries = category_summaries(all_player_outcomes)
    write_csv(output_dir / "simulation_players.csv", simulation_player_rows(simulated_players))
    write_csv(output_dir / "simulated_games.csv", simulated_game_rows(game_infos))
    write_csv(output_dir / "player_outcomes.csv", all_player_outcomes)
    write_csv(output_dir / "player_trajectories.csv", all_trajectories)
    write_csv(output_dir / "category_summary.csv", summaries)
    metadata = {
        "name": args.name,
        "generated_at": datetime_now_iso(),
        "elapsed_seconds": time.perf_counter() - started,
        "seed": args.seed,
        "inputs": {"games": str(args.games), "ratings": str(args.ratings)},
        "start_date": args.start_date.isoformat(),
        "years": args.years,
        "players_per_cell": args.players_per_cell,
        "replications": args.replications,
        "self_promote_rate": args.self_promote_rate,
        "true_strength_closed_delta": args.true_strength_closed_delta,
        "self_promote_closed_delta": args.self_promote_closed_delta
        if args.self_promote_closed_delta is not None
        else args.true_strength_closed_delta,
        "activity_levels": activity_levels,
        "base_player_count": len(base_players),
        "simulated_player_count": len(simulated_players),
        "simulated_game_count": len(simulated_games),
        "algorithms": {
            "baseline": asdict(baseline_config()),
            "surprise_taper_floor_050": asdict(guarded_surprise_config()),
            "surprise_taper_min_pre_post_floor_050": asdict(guarded_surprise_min_pre_post_config()),
            "surprise_taper_min_pre_post_gated_base_floor_050": asdict(
                guarded_surprise_min_pre_post_gated_base_config()
            ),
        },
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str) + "\n", encoding="utf-8")

    print(f"Simulation written to {output_dir}")
    for row in summaries:
        if row["slice_type"] == "all":
            print(
                f"{row['algorithm']}: reached {row['reached_count']}/{row['players']} "
                f"({row['reached_rate']:.1%}), median games={row['games_to_reach_median']}, "
                f"median final gap={row['final_gap_to_true_median']:.3f}"
            )


def datetime_now_iso() -> str:
    from datetime import datetime

    return datetime.now().replace(microsecond=0).isoformat()


if __name__ == "__main__":
    main()
