import itertools
import json
import statistics
import sys
from datetime import date
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from function2.bayrate import BayrateConfig, run_bayrate

RECENT_START = date(2018, 1, 1)
OUTPUT_PATH = Path("function2/bayrate_recent_performance_sweep.json")

BASE_CONFIG = {
    "posterior_sigma_floor": None,
    "posterior_sigma_multiplier": 1.1,
    "inactivity_growth_per_day": 0.0005,
}

THRESHOLDS = [0.25, 0.5, 0.75, 1.0]
SCALES = [0.25, 0.5, 0.75, 1.0]
CAPS = [1.5, 2.0, None]


def summarize_candidate(result):
    sigmas = [row.sigma_after for row in result.player_results]
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
        },
    }


def sort_key(summary):
    pre = summary["pre_event_metrics"]
    post = summary["post_event_fit_metrics"]
    return (
        float(pre["average_log_loss"]),
        float(pre["average_brier"]),
        -float(pre["accuracy"]),
        float(post["average_log_loss"]),
    )


def build_config(**overrides):
    payload = dict(BASE_CONFIG)
    payload.update(overrides)
    return BayrateConfig(min_game_date=RECENT_START, **payload)


def main() -> None:
    baseline_result = run_bayrate(Path("games.csv"), Path("aga-ratings.csv"), build_config())
    baseline_summary = summarize_candidate(baseline_result)

    candidates = []
    total = 0
    for threshold, scale, cap in itertools.product(THRESHOLDS, SCALES, CAPS):
        config = build_config(
            performance_trigger_threshold=threshold,
            performance_trigger_sigma_scale=scale,
            performance_trigger_sigma_cap=cap,
        )
        result = run_bayrate(Path("games.csv"), Path("aga-ratings.csv"), config)
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
            threshold,
            scale,
            cap,
            summary["pre_event_metrics"]["average_log_loss"],
            summary["pre_event_metrics"]["average_brier"],
            summary["pre_event_metrics"]["accuracy"],
            summary["post_event_fit_metrics"]["average_log_loss"],
            summary["sigma_summary"]["mean"],
            summary["sigma_summary"]["min"],
        )

    candidates.sort(key=sort_key)
    payload = {
        "scope": {
            "games": "games.csv",
            "ratings": "aga-ratings.csv",
            "min_game_date": RECENT_START.isoformat(),
        },
        "base_config": BASE_CONFIG,
        "grid": {
            "performance_trigger_threshold": THRESHOLDS,
            "performance_trigger_sigma_scale": SCALES,
            "performance_trigger_sigma_cap": CAPS,
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
