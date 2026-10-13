import argparse
import csv
import json
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from function2.bayrate import BayrateConfig, run_bayrate

BEST_CONFIG = BayrateConfig(
    posterior_sigma_multiplier=1.1,
    inactivity_growth_per_day=0.0005,
    performance_trigger_threshold=1.0,
    performance_trigger_sigma_scale=0.25,
    performance_trigger_sigma_cap=1.5,
    performance_trigger_low_sigma_reference=0.35,
    performance_trigger_low_sigma_power=0.15,
    performance_trigger_low_sigma_rating_taper_start=5.0,
    performance_trigger_low_sigma_rating_taper_end=7.0,
)


def export_full_history(games_path: Path, ratings_path: Path, csv_path: Path, summary_path: Path | None) -> dict[str, object]:
    started = datetime.now()
    result = run_bayrate(games_path, ratings_path, BEST_CONFIG)
    finished = datetime.now()

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "AGAID",
            "EventDate",
            "TournamentCode",
            "EventKey",
            "RankSeed",
            "SeedBefore",
            "PriorRating",
            "PriorSigma",
            "RatingAfter",
            "SigmaAfter",
        ])
        for row in result.player_results:
            writer.writerow([
                row.player_id,
                row.event_date.isoformat(),
                row.tournament_code or "",
                row.event_key,
                row.rank_seed,
                row.seed_before_closing_boundary,
                "" if row.prior_rating is None else row.prior_rating,
                "" if row.prior_sigma is None else row.prior_sigma,
                row.rating_after,
                row.sigma_after,
            ])

    summary = {
        "config": {
            "posterior_sigma_multiplier": BEST_CONFIG.posterior_sigma_multiplier,
            "inactivity_growth_per_day": BEST_CONFIG.inactivity_growth_per_day,
            "performance_trigger_threshold": BEST_CONFIG.performance_trigger_threshold,
            "performance_trigger_sigma_scale": BEST_CONFIG.performance_trigger_sigma_scale,
            "performance_trigger_sigma_cap": BEST_CONFIG.performance_trigger_sigma_cap,
            "performance_trigger_low_sigma_reference": BEST_CONFIG.performance_trigger_low_sigma_reference,
            "performance_trigger_low_sigma_power": BEST_CONFIG.performance_trigger_low_sigma_power,
            "performance_trigger_low_sigma_rating_taper_start": BEST_CONFIG.performance_trigger_low_sigma_rating_taper_start,
            "performance_trigger_low_sigma_rating_taper_end": BEST_CONFIG.performance_trigger_low_sigma_rating_taper_end,
        },
        "games_path": str(games_path),
        "ratings_path": str(ratings_path),
        "csv_path": str(csv_path),
        "event_count": result.event_count,
        "player_count": result.player_count,
        "row_count": len(result.player_results),
        "pre_event_metrics": result.pre_event_metrics,
        "post_event_fit_metrics": result.post_event_fit_metrics,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "duration_seconds": (finished - started).total_seconds(),
    }
    if summary_path is not None:
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Export one full-history Bayrate event-rating table for all AGAIDs.")
    parser.add_argument("--games", type=Path, default=Path("games.csv"))
    parser.add_argument("--ratings", type=Path, default=Path("aga-ratings.csv"))
    parser.add_argument("--csv-output", type=Path, default=Path("bayrate-best-event-ratings.csv"))
    parser.add_argument("--summary-output", type=Path, default=Path("bayrate-best-event-ratings-summary.json"))
    args = parser.parse_args()

    summary = export_full_history(args.games, args.ratings, args.csv_output, args.summary_output)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()




