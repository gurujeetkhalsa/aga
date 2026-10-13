import argparse
import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from function2.bayrate import BayrateConfig, load_official_history, run_bayrate

OUTPUT_PATH = Path("function2/bayrate_cohort_comparison.json")
RECENT_START = date(2018, 1, 1)

DEFAULT_VARIANTS: dict[str, dict[str, Any]] = {
    "baseline": {
        "posterior_sigma_multiplier": 1.1,
        "inactivity_growth_per_day": 0.0005,
        "performance_trigger_threshold": 1.0,
        "performance_trigger_sigma_scale": 0.25,
        "performance_trigger_sigma_cap": 1.5,
    },
    "best_taper": {
        "posterior_sigma_multiplier": 1.1,
        "inactivity_growth_per_day": 0.0005,
        "performance_trigger_threshold": 1.0,
        "performance_trigger_sigma_scale": 0.25,
        "performance_trigger_sigma_cap": 1.5,
        "performance_trigger_low_sigma_reference": 0.35,
        "performance_trigger_low_sigma_power": 0.15,
        "performance_trigger_low_sigma_rating_taper_start": 5.0,
        "performance_trigger_low_sigma_rating_taper_end": 7.0,
    },
}

TRACKED_CASES = [
    {"agaid": 12635, "event_key": "code:cong20180722open"},
    {"agaid": 17802, "event_key": "code:cong20180722open"},
    {"agaid": 18587, "event_key": "code:cong20180722open"},
    {"agaid": 22315, "event_key": "code:mdopen20190526"},
    {"agaid": 57, "event_key": "code:ngc20180210"},
]

LOW_SIGMA_STUCK_PRIOR_SIGMA_MAX = 0.4
LOW_SIGMA_STUCK_RANK_GAP_MIN = 0.75
RAPID_IMPROVER_JUMP_MIN = 2.0
RAPID_IMPROVER_WINDOW_DAYS = 365
VERY_STRONG_RATING_MIN = 7.0


def build_config(payload: dict[str, Any], min_game_date: date | None) -> BayrateConfig:
    config_payload = dict(payload)
    if min_game_date is not None and "min_game_date" not in config_payload:
        config_payload["min_game_date"] = min_game_date
    return BayrateConfig(**config_payload)


def load_variant_payloads(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return dict(DEFAULT_VARIANTS)
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict) or not payload:
        raise ValueError("Variant file must contain a non-empty JSON object mapping names to config payloads.")
    normalized: dict[str, dict[str, Any]] = {}
    for name, config in payload.items():
        if not isinstance(name, str) or not isinstance(config, dict):
            raise ValueError("Variant file entries must map string names to JSON objects.")
        normalized[name] = config
    return normalized


def result_indexes(result):
    by_player: dict[int, list[Any]] = defaultdict(list)
    by_event: dict[tuple[int, str], Any] = {}
    by_snapshot_key: dict[tuple[int, date, str | None], Any] = {}
    for row in result.player_results:
        by_player[row.player_id].append(row)
        by_event[(row.player_id, row.event_key)] = row
        by_snapshot_key[(row.player_id, row.event_date, row.tournament_code)] = row
    for rows in by_player.values():
        rows.sort(key=lambda row: (row.event_date, row.event_key))
    return by_player, by_event, by_snapshot_key


def build_player_cohorts(official_history):
    rapid_improvers: set[int] = set()
    very_strong: set[int] = set()
    rapid_examples: list[dict[str, Any]] = []
    strong_examples: list[dict[str, Any]] = []

    for player_id, snapshots in official_history.items():
        ordered = sorted(snapshots, key=lambda snap: (snap.elab_date, snap.row_id))
        max_rating = max(snapshot.rating for snapshot in ordered)
        if max_rating >= VERY_STRONG_RATING_MIN:
            very_strong.add(player_id)
            if len(strong_examples) < 12:
                strong_examples.append({"agaid": player_id, "max_official_rating": max_rating})

        best_jump = None
        for idx, start in enumerate(ordered):
            for finish in ordered[idx + 1 :]:
                days = (finish.elab_date - start.elab_date).days
                if days <= 0:
                    continue
                if days > RAPID_IMPROVER_WINDOW_DAYS:
                    break
                jump = finish.rating - start.rating
                if jump >= RAPID_IMPROVER_JUMP_MIN:
                    candidate = {
                        "agaid": player_id,
                        "jump": jump,
                        "days": days,
                        "start_date": start.elab_date.isoformat(),
                        "end_date": finish.elab_date.isoformat(),
                    }
                    if best_jump is None or jump > best_jump["jump"]:
                        best_jump = candidate
        if best_jump is not None:
            rapid_improvers.add(player_id)
            if len(rapid_examples) < 12:
                rapid_examples.append(best_jump)

    rapid_examples.sort(key=lambda row: (-row["jump"], row["days"], row["agaid"]))
    strong_examples.sort(key=lambda row: (-row["max_official_rating"], row["agaid"]))
    return {
        "rapid_improvers": {
            "player_ids": rapid_improvers,
            "definition": f"Official rating increases by at least {RAPID_IMPROVER_JUMP_MIN:.1f} within {RAPID_IMPROVER_WINDOW_DAYS} days.",
            "examples": rapid_examples[:12],
        },
        "very_strong": {
            "player_ids": very_strong,
            "definition": f"Player reaches official rating >= {VERY_STRONG_RATING_MIN:.1f} at any snapshot.",
            "examples": strong_examples[:12],
        },
    }


def build_event_cohorts(reference_result):
    low_sigma_stuck = []
    tracked = []
    tracked_set = {(row["agaid"], row["event_key"]) for row in TRACKED_CASES}

    for row in reference_result.player_results:
        event_key = (row.player_id, row.event_key)
        if event_key in tracked_set:
            tracked.append(event_key)
        if row.prior_rating is None or row.prior_sigma is None:
            continue
        if row.prior_sigma > LOW_SIGMA_STUCK_PRIOR_SIGMA_MAX:
            continue
        if row.rank_seed - row.prior_rating < LOW_SIGMA_STUCK_RANK_GAP_MIN:
            continue
        low_sigma_stuck.append(event_key)

    return {
        "low_sigma_stuck": {
            "event_keys": low_sigma_stuck,
            "definition": (
                "Reference-variant events with prior_sigma <= "
                f"{LOW_SIGMA_STUCK_PRIOR_SIGMA_MAX:.2f} and rank_seed - prior_rating >= {LOW_SIGMA_STUCK_RANK_GAP_MIN:.2f}."
            ),
            "examples": [
                {"agaid": player_id, "event_key": event_key}
                for player_id, event_key in low_sigma_stuck[:12]
            ],
        },
        "tracked_cases": {
            "event_keys": tracked,
            "definition": "Explicitly tracked case list from the current Bayrate tuning notes.",
            "examples": [
                {"agaid": player_id, "event_key": event_key}
                for player_id, event_key in tracked
            ],
        },
    }


def metric_summary(result):
    return {
        "pre_event_metrics": result.pre_event_metrics,
        "post_event_fit_metrics": result.post_event_fit_metrics,
        "event_count": result.event_count,
        "player_count": result.player_count,
        "row_count": len(result.player_results),
    }


def compare_player_cohort(player_ids, official_history, reference_idx, candidate_idx):
    reference_by_player, _, reference_by_snapshot = reference_idx
    candidate_by_player, _, candidate_by_snapshot = candidate_idx

    event_deltas = []
    last_rating_deltas = []
    last_sigma_deltas = []
    snapshot_error_deltas = []
    improved = worsened = unchanged = 0
    largest_event_moves = []

    for player_id in sorted(player_ids):
        reference_rows = reference_by_player.get(player_id)
        candidate_rows = candidate_by_player.get(player_id)
        if reference_rows and candidate_rows and len(reference_rows) == len(candidate_rows):
            for ref_row, cand_row in zip(reference_rows, candidate_rows):
                delta = cand_row.rating_after - ref_row.rating_after
                sigma_delta = cand_row.sigma_after - ref_row.sigma_after
                event_deltas.append(delta)
                if abs(delta) >= 0.01 and len(largest_event_moves) < 20:
                    largest_event_moves.append(
                        {
                            "magnitude": abs(delta),
                            "agaid": player_id,
                            "event_key": ref_row.event_key,
                            "event_date": ref_row.event_date.isoformat(),
                            "reference_rating_after": ref_row.rating_after,
                            "candidate_rating_after": cand_row.rating_after,
                            "reference_sigma_after": ref_row.sigma_after,
                            "candidate_sigma_after": cand_row.sigma_after,
                            "rating_after_delta": delta,
                            "sigma_after_delta": sigma_delta,
                        }
                    )
            last_rating_deltas.append(candidate_rows[-1].rating_after - reference_rows[-1].rating_after)
            last_sigma_deltas.append(candidate_rows[-1].sigma_after - reference_rows[-1].sigma_after)

        for snapshot in official_history.get(player_id, []):
            key = (player_id, snapshot.elab_date, snapshot.tournament_code)
            ref_row = reference_by_snapshot.get(key)
            cand_row = candidate_by_snapshot.get(key)
            if ref_row is None or cand_row is None:
                continue
            ref_error = abs(ref_row.rating_after - snapshot.rating)
            cand_error = abs(cand_row.rating_after - snapshot.rating)
            delta = cand_error - ref_error
            snapshot_error_deltas.append(delta)
            if delta < -1e-12:
                improved += 1
            elif delta > 1e-12:
                worsened += 1
            else:
                unchanged += 1

    largest_event_moves.sort(key=lambda row: (-row["magnitude"], row["agaid"], row["event_key"]))
    return {
        "players_in_cohort": len(player_ids),
        "players_with_rows": sum(1 for player_id in player_ids if player_id in reference_by_player and player_id in candidate_by_player),
        "avg_event_rating_after_delta": (sum(event_deltas) / len(event_deltas)) if event_deltas else None,
        "avg_last_rating_delta": (sum(last_rating_deltas) / len(last_rating_deltas)) if last_rating_deltas else None,
        "avg_last_sigma_delta": (sum(last_sigma_deltas) / len(last_sigma_deltas)) if last_sigma_deltas else None,
        "matched_snapshot_mean_abs_error_delta": (sum(snapshot_error_deltas) / len(snapshot_error_deltas)) if snapshot_error_deltas else None,
        "matched_snapshot_improved": improved,
        "matched_snapshot_worsened": worsened,
        "matched_snapshot_unchanged": unchanged,
        "largest_event_movements": largest_event_moves[:10],
    }


def compare_event_cohort(event_keys, official_history, reference_idx, candidate_idx):
    _, reference_by_event, reference_by_snapshot = reference_idx
    _, candidate_by_event, candidate_by_snapshot = candidate_idx

    rating_deltas = []
    sigma_deltas = []
    snapshot_error_deltas = []
    improved = worsened = unchanged = 0
    examples = []

    for player_id, event_key in event_keys:
        ref_row = reference_by_event.get((player_id, event_key))
        cand_row = candidate_by_event.get((player_id, event_key))
        if ref_row is None or cand_row is None:
            continue
        rating_delta = cand_row.rating_after - ref_row.rating_after
        sigma_delta = cand_row.sigma_after - ref_row.sigma_after
        rating_deltas.append(rating_delta)
        sigma_deltas.append(sigma_delta)

        snapshot_key = (player_id, ref_row.event_date, ref_row.tournament_code)
        ref_snapshot_row = reference_by_snapshot.get(snapshot_key)
        cand_snapshot_row = candidate_by_snapshot.get(snapshot_key)
        official_match = None
        for snapshot in official_history.get(player_id, []):
            if snapshot.elab_date == ref_row.event_date and snapshot.tournament_code == ref_row.tournament_code:
                official_match = snapshot
                break
        snapshot_delta = None
        if ref_snapshot_row is not None and cand_snapshot_row is not None and official_match is not None:
            ref_error = abs(ref_snapshot_row.rating_after - official_match.rating)
            cand_error = abs(cand_snapshot_row.rating_after - official_match.rating)
            snapshot_delta = cand_error - ref_error
            snapshot_error_deltas.append(snapshot_delta)
            if snapshot_delta < -1e-12:
                improved += 1
            elif snapshot_delta > 1e-12:
                worsened += 1
            else:
                unchanged += 1

        if len(examples) < 20:
            examples.append(
                {
                    "agaid": player_id,
                    "event_key": event_key,
                    "event_date": ref_row.event_date.isoformat(),
                    "prior_rating": ref_row.prior_rating,
                    "prior_sigma": ref_row.prior_sigma,
                    "rank_seed": ref_row.rank_seed,
                    "reference_rating_after": ref_row.rating_after,
                    "candidate_rating_after": cand_row.rating_after,
                    "rating_after_delta": rating_delta,
                    "reference_sigma_after": ref_row.sigma_after,
                    "candidate_sigma_after": cand_row.sigma_after,
                    "sigma_after_delta": sigma_delta,
                    "matched_snapshot_error_delta": snapshot_delta,
                }
            )

    examples.sort(key=lambda row: (-abs(row["rating_after_delta"]), row["agaid"], row["event_key"]))
    return {
        "events_in_cohort": len(event_keys),
        "events_with_rows": len(rating_deltas),
        "distinct_players": len({player_id for player_id, _ in event_keys}),
        "avg_rating_after_delta": (sum(rating_deltas) / len(rating_deltas)) if rating_deltas else None,
        "avg_sigma_after_delta": (sum(sigma_deltas) / len(sigma_deltas)) if sigma_deltas else None,
        "matched_snapshot_mean_abs_error_delta": (sum(snapshot_error_deltas) / len(snapshot_error_deltas)) if snapshot_error_deltas else None,
        "matched_snapshot_improved": improved,
        "matched_snapshot_worsened": worsened,
        "matched_snapshot_unchanged": unchanged,
        "examples": examples[:10],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare Bayrate variants on tracked cohorts.")
    parser.add_argument("--games", type=Path, default=Path("games.csv"))
    parser.add_argument("--ratings", type=Path, default=Path("aga-ratings.csv"))
    parser.add_argument("--variant-file", type=Path, default=None, help="Optional JSON file mapping variant names to BayrateConfig payloads.")
    parser.add_argument("--reference", type=str, default="baseline")
    parser.add_argument("--min-game-date", type=str, default=None, help="Optional YYYY-MM-DD override applied to variants that do not already set min_game_date.")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()

    min_game_date = RECENT_START if args.min_game_date is None else date.fromisoformat(args.min_game_date)
    variant_payloads = load_variant_payloads(args.variant_file)
    if args.reference not in variant_payloads:
        raise SystemExit(f"Reference variant {args.reference!r} is not defined.")

    results = {}
    indexes = {}
    for name, payload in variant_payloads.items():
        config = build_config(payload, min_game_date)
        print(f"running {name}...")
        result = run_bayrate(args.games, args.ratings, config)
        results[name] = result
        indexes[name] = result_indexes(result)

    official_history = load_official_history(args.ratings)
    player_cohorts = build_player_cohorts(official_history)
    event_cohorts = build_event_cohorts(results[args.reference])

    payload = {
        "scope": {
            "games": str(args.games),
            "ratings": str(args.ratings),
            "reference_variant": args.reference,
            "min_game_date": min_game_date.isoformat() if min_game_date is not None else None,
        },
        "variants": {name: {"config": result.config, "metrics": metric_summary(result)} for name, result in results.items()},
        "cohort_definitions": {
            name: {
                "type": "player",
                "definition": data["definition"],
                "size": len(data["player_ids"]),
                "examples": data["examples"],
            }
            for name, data in player_cohorts.items()
        },
        "event_cohort_definitions": {
            name: {
                "type": "event",
                "definition": data["definition"],
                "size": len(data["event_keys"]),
                "examples": data["examples"],
            }
            for name, data in event_cohorts.items()
        },
        "comparisons": {},
    }

    reference_idx = indexes[args.reference]
    for name in variant_payloads:
        if name == args.reference:
            continue
        candidate_idx = indexes[name]
        payload["comparisons"][name] = {
            "overall_metric_deltas_vs_reference": {
                "pre_event_accuracy": results[name].pre_event_metrics["accuracy"] - results[args.reference].pre_event_metrics["accuracy"],
                "pre_event_log_loss": results[name].pre_event_metrics["average_log_loss"] - results[args.reference].pre_event_metrics["average_log_loss"],
                "pre_event_brier": results[name].pre_event_metrics["average_brier"] - results[args.reference].pre_event_metrics["average_brier"],
                "post_event_accuracy": results[name].post_event_fit_metrics["accuracy"] - results[args.reference].post_event_fit_metrics["accuracy"],
                "post_event_log_loss": results[name].post_event_fit_metrics["average_log_loss"] - results[args.reference].post_event_fit_metrics["average_log_loss"],
                "post_event_brier": results[name].post_event_fit_metrics["average_brier"] - results[args.reference].post_event_fit_metrics["average_brier"],
            },
            "player_cohorts": {
                cohort_name: compare_player_cohort(data["player_ids"], official_history, reference_idx, candidate_idx)
                for cohort_name, data in player_cohorts.items()
            },
            "event_cohorts": {
                cohort_name: compare_event_cohort(data["event_keys"], official_history, reference_idx, candidate_idx)
                for cohort_name, data in event_cohorts.items()
            },
        }

    args.output.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    main()

