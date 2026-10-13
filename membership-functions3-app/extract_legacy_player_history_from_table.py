import csv
import json
from pathlib import Path

from function2.legacy_bayrate import load_official_history


def extract_player_history(target: int, table_path: Path, ratings_path: Path) -> dict[str, object]:
    model_rows = []
    with table_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if int(row["AGAID"]) != target:
                continue
            model_rows.append({
                "type": "event",
                "date": row["EventDate"],
                "event_key": row["EventKey"],
                "tournament_code": row["TournamentCode"] or None,
                "rank_seed": float(row["RankSeed"]),
                "seed_before": float(row["SeedBefore"]),
                "prior_rating": None if not row["PriorRating"] else float(row["PriorRating"]),
                "prior_sigma": None if not row["PriorSigma"] else float(row["PriorSigma"]),
                "rating_after": float(row["RatingAfter"]),
                "sigma_after": float(row["SigmaAfter"]),
            })

    official_history = load_official_history(ratings_path)
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
        "model_points": len(model_rows),
        "official_points": len(official_rows),
        "first_model_point": model_rows[0] if model_rows else None,
        "last_model_point": model_rows[-1] if model_rows else None,
        "first_official_point": official_rows[0] if official_rows else None,
        "last_official_point": official_rows[-1] if official_rows else None,
        "table_path": str(table_path),
    }
    return {"summary": summary, "official_history": official_rows, "model_history": model_rows}


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Extract one AGAID's legacy Bayrate history from the precomputed full-history table.")
    parser.add_argument("agaid", type=int)
    parser.add_argument("--table", type=Path, default=Path("legacy-bayrate-best-event-ratings.csv"))
    parser.add_argument("--ratings", type=Path, default=Path("aga-ratings.csv"))
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    payload = extract_player_history(args.agaid, args.table, args.ratings)
    out_path = args.output or Path(f"agaid-{args.agaid}-legacy-bayrate-history.json")
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2))


if __name__ == "__main__":
    main()
