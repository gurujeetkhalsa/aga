import itertools
import json
import statistics
import sys
from datetime import date
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from function2.legacy_bayrate import LegacyBayrateConfig, run_legacy_bayrate

RECENT_START = date(2018, 1, 1)
OUTPUT_PATH = Path("function2/legacy_bayrate_recent_sweep.json")

SIGMA_FLOORS = [None, 0.45, 0.6, 0.75]
SIGMA_MULTIPLIERS = [1.0, 1.1, 1.25]
INACTIVITY_GROWTH = [0.0005, 0.001, 0.002]


def summarize_candidate(result):
    sigmas = [row.sigma_after for row in result.player_results]
    sigma_floor = result.config["posterior_sigma_floor"]
    floor_hits = 0
    if sigma_floor is not None:
        floor_hits = sum(1 for sigma in sigmas if abs(sigma - sigma_floor) < 1e-9)
    return {
        "config": result.config,
        "event_count": result.event_count,
        "player_count": result.player_count,
        "pre_event_metrics": result.pre_event_metrics,
        "post_event_fit_metrics": result.post_event_fit_metrics,
        "sigma_summary": {
            "count": len(sigmas),
            "min": min(sigmas) if sigmas else None,
            "median": statistics.median(sigmas) if sigmas else None,
            "mean": (sum(sigmas) / len(sigmas)) if sigmas else None,
            "max": max(sigmas) if sigmas else None,
            "floor_hits": floor_hits,
        },
    }


def sort_key(summary):
    pre = summary["pre_event_metrics"]
    return (
        float(pre["average_log_loss"]),
        float(pre["average_brier"]),
        -float(pre["accuracy"]),
    )


def main() -> None:
    baseline_config = LegacyBayrateConfig(min_game_date=RECENT_START)
    baseline_result = run_legacy_bayrate(Path("games.csv"), Path("aga-ratings.csv"), baseline_config)
    baseline_summary = summarize_candidate(baseline_result)

    candidates = []
    total = 0
    for sigma_floor, sigma_multiplier, inactivity_growth_per_day in itertools.product(
        SIGMA_FLOORS,
        SIGMA_MULTIPLIERS,
        INACTIVITY_GROWTH,
    ):
        config = LegacyBayrateConfig(
            min_game_date=RECENT_START,
            posterior_sigma_floor=sigma_floor,
            posterior_sigma_multiplier=sigma_multiplier,
            inactivity_growth_per_day=inactivity_growth_per_day,
        )
        result = run_legacy_bayrate(Path("games.csv"), Path("aga-ratings.csv"), config)
        summary = summarize_candidate(result)
        summary["delta_vs_baseline"] = {
            "pre_event_accuracy": float(summary["pre_event_metrics"]["accuracy"]) - float(baseline_summary["pre_event_metrics"]["accuracy"]),
            "pre_event_log_loss": float(summary["pre_event_metrics"]["average_log_loss"]) - float(baseline_summary["pre_event_metrics"]["average_log_loss"]),
            "pre_event_brier": float(summary["pre_event_metrics"]["average_brier"]) - float(baseline_summary["pre_event_metrics"]["average_brier"]),
            "post_event_log_loss": float(summary["post_event_fit_metrics"]["average_log_loss"]) - float(baseline_summary["post_event_fit_metrics"]["average_log_loss"]),
            "mean_sigma": float(summary["sigma_summary"]["mean"]) - float(baseline_summary["sigma_summary"]["mean"]),
            "min_sigma": float(summary["sigma_summary"]["min"]) - float(baseline_summary["sigma_summary"]["min"]),
        }
        candidates.append(summary)
        total += 1
        print(
            total,
            sigma_floor,
            sigma_multiplier,
            inactivity_growth_per_day,
            summary["pre_event_metrics"]["average_log_loss"],
            summary["pre_event_metrics"]["average_brier"],
            summary["pre_event_metrics"]["accuracy"],
            summary["sigma_summary"]["mean"],
            summary["sigma_summary"]["min"],
            summary["sigma_summary"]["floor_hits"],
        )

    candidates.sort(key=sort_key)
    payload = {
        "scope": {
            "games": "games.csv",
            "ratings": "aga-ratings.csv",
            "min_game_date": RECENT_START.isoformat(),
        },
        "grid": {
            "posterior_sigma_floor": SIGMA_FLOORS,
            "posterior_sigma_multiplier": SIGMA_MULTIPLIERS,
            "inactivity_growth_per_day": INACTIVITY_GROWTH,
        },
        "baseline": baseline_summary,
        "candidate_count": len(candidates),
        "top_12_by_pre_event_log_loss": candidates[:12],
        "all_candidates": candidates,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(json.dumps(payload["top_12_by_pre_event_log_loss"], indent=2, default=str))


if __name__ == "__main__":
    main()
