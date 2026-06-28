import csv
import shutil
import unittest
from pathlib import Path

from research.bayrate_sigma.sigma_slice_harness import build_slice_harness, label_player_events


TMP_DIR = Path(__file__).parent / "tmp_sigma_slice_harness"


class SigmaSliceHarnessTest(unittest.TestCase):
    """Represent sigma slice harness test."""
    def tearDown(self) -> None:
        """Execute the tearDown routine."""
        if TMP_DIR.exists():
            shutil.rmtree(TMP_DIR)

    def test_label_player_events_marks_future_trends_and_activity(self) -> None:
        """Verify that label player events marks future trends and activity."""
        rows = [
            player_row(1, "A", "2026-01-01", rating_after=0.0, prior_rating=-0.2, prior_sigma=0.2),
            player_row(1, "B", "2026-02-01", rating_after=1.2, prior_rating=0.0, prior_sigma=0.3),
            player_row(1, "C", "2026-03-01", rating_after=2.1, prior_rating=1.2, prior_sigma=0.4),
        ]

        labels = label_player_events(rows, lookahead_events=2, trend_threshold=1.0)

        self.assertEqual(labels[("1", "A")]["future_trend"], "future_improver")
        self.assertEqual(labels[("1", "B")]["future_trend"], "future_mixed")
        self.assertEqual(labels[("1", "C")]["future_trend"], "no_future")
        self.assertEqual(labels[("1", "A")]["activity"], "first_event")
        self.assertEqual(labels[("1", "B")]["activity"], "30-180d")
        self.assertEqual(labels[("1", "A")]["prior_sigma_band"], "<0.25")

    def test_build_slice_harness_writes_candidate_deltas_on_fixed_labels(self) -> None:
        """Verify that build slice harness writes candidate deltas on fixed labels."""
        baseline_dir = TMP_DIR / "baseline"
        candidate_dir = TMP_DIR / "candidate"
        output_dir = TMP_DIR / "slices"
        write_experiment(
            baseline_dir,
            expected_white_a=0.40,
            expected_white_b=0.60,
        )
        write_experiment(
            candidate_dir,
            expected_white_a=0.70,
            expected_white_b=0.55,
        )

        artifact = build_slice_harness(
            baseline_dir=baseline_dir,
            candidates=[("candidate", candidate_dir)],
            output_dir=output_dir,
            lookahead_events=1,
            trend_threshold=0.75,
        )

        comparison_rows = read_csv(Path(artifact["game_comparison_path"]))
        improver_log_loss = [
            row
            for row in comparison_rows
            if row["slice_type"] == "future_trend"
            and row["slice"] == "future_improver"
            and row["metric"] == "log_loss"
        ][0]

        self.assertEqual(improver_log_loss["experiment"], "candidate")
        self.assertLess(float(improver_log_loss["delta"]), 0.0)
        self.assertTrue(Path(artifact["player_metrics_path"]).exists())


def write_experiment(path: Path, *, expected_white_a: float, expected_white_b: float) -> None:
    """Write experiment."""
    path.mkdir(parents=True, exist_ok=True)
    write_csv(
        path / "player_results.csv",
        [
            player_row(1, "A", "2026-01-01", rating_after=0.0, prior_rating=-0.5, prior_sigma=0.2),
            player_row(2, "A", "2026-01-01", rating_after=0.0, prior_rating=0.0, prior_sigma=0.4),
            player_row(1, "B", "2026-02-01", rating_after=1.0, prior_rating=0.0, prior_sigma=0.3),
            player_row(2, "B", "2026-02-01", rating_after=0.1, prior_rating=0.0, prior_sigma=0.4),
        ],
    )
    write_csv(
        path / "game_results.csv",
        [
            game_row(101, "A", "2026-01-01", white=1, black=2, white_wins=True, expected_white=expected_white_a),
            game_row(102, "B", "2026-02-01", white=1, black=2, white_wins=False, expected_white=expected_white_b),
        ],
    )


def player_row(
    player_id: int,
    event_key: str,
    event_date: str,
    *,
    rating_after: float,
    prior_rating: float,
    prior_sigma: float,
) -> dict[str, object]:
    """Execute the player row routine."""
    performance_rating = prior_rating + 1.2
    rating_delta = rating_after - prior_rating
    performance_gap = performance_rating - prior_rating
    return {
        "player_id": player_id,
        "event_key": event_key,
        "event_date": event_date,
        "tournament_code": event_key,
        "rank_seed": prior_rating,
        "prior_rating": prior_rating,
        "prior_sigma": prior_sigma,
        "performance_rating": performance_rating,
        "rating_after": rating_after,
        "sigma_after": prior_sigma,
        "rating_delta": rating_delta,
        "sigma_delta": 0.0,
        "performance_gap": performance_gap,
        "capture_ratio": rating_delta / performance_gap,
    }


def game_row(
    source_game_id: int,
    event_key: str,
    event_date: str,
    *,
    white: int,
    black: int,
    white_wins: bool,
    expected_white: float,
) -> dict[str, object]:
    """Execute the game row routine."""
    return {
        "source_game_id": source_game_id,
        "event_key": event_key,
        "event_date": event_date,
        "tournament_code": event_key,
        "white_agaid": white,
        "black_agaid": black,
        "handicap": 0,
        "komi": 7.5,
        "white_wins": white_wins,
        "white_seed_before": 0.0,
        "black_seed_before": 0.0,
        "pre_event_expected_white": expected_white,
        "post_event_expected_white": expected_white,
        "pre_event_brier": (expected_white - (1.0 if white_wins else 0.0)) ** 2,
    }


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    """Write csv."""
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict[str, str]]:
    """Read csv."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


if __name__ == "__main__":
    unittest.main()
