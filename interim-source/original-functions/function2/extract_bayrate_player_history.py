import json
import sys
from datetime import datetime
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from function2.bayrate import BayrateConfig, load_official_history, run_bayrate

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


def extract_player_history(target: int, games_path: Path, ratings_path: Path) -> dict[str, object]:
    result = run_bayrate(games_path, ratings_path, BEST_CONFIG)
    official_history = load_official_history(ratings_path)

    model_rows = [
        {
            "type": "event",
            "date": row.event_date.isoformat(),
            "event_key": row.event_key,
            "tournament_code": row.tournament_code,
            "rank_seed": row.rank_seed,
            "seed_before": row.seed_before_closing_boundary,
            "prior_rating": row.prior_rating,
            "prior_sigma": row.prior_sigma,
            "rating_after": row.rating_after,
            "sigma_after": row.sigma_after,
        }
        for row in result.player_results
        if row.player_id == target
    ]

    official_rows = [
        {
            "type": "official",
            "date": snapshot.elab_date.isoformat(),
            "rating": snapshot.rating,
            "sigma": snapshot.sigma,
            "tournament_code": snapshot.tournament_code,
            "row_id": snapshot.row_id,
        }
        for snapshot in official_history.get(target, [])
    ]

    summary = {
        "agaid": target,
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
        "model_points": len(model_rows),
        "official_points": len(official_rows),
        "first_model_point": model_rows[0] if model_rows else None,
        "last_model_point": model_rows[-1] if model_rows else None,
        "first_official_point": official_rows[0] if official_rows else None,
        "last_official_point": official_rows[-1] if official_rows else None,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }
    return {"summary": summary, "official_history": official_rows, "model_history": model_rows}


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Extract full-history Bayrate event ratings for one AGAID.")
    parser.add_argument("agaid", type=int)
    parser.add_argument("--games", type=Path, default=Path("games.csv"))
    parser.add_argument("--ratings", type=Path, default=Path("aga-ratings.csv"))
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    payload = extract_player_history(args.agaid, args.games, args.ratings)
    out_path = args.output or Path(f"agaid-{args.agaid}-bayrate-history.json")
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2))


if __name__ == "__main__":
    main()




