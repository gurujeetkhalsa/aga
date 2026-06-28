import json
import shutil
import unittest
from datetime import date
from pathlib import Path

from research.bayrate_sigma.experiment_benchmark import distribution, load_config_from_json, run_experiment_benchmark
from research.bayrate_sigma.export_experiment_dataset import export_experiment_dataset


FIXTURE_DIR = Path(__file__).resolve().parents[3] / "bayrate" / "tests" / "fixtures"
TMP_DIR = Path(__file__).parent / "tmp_experiment_tools"


class FakeExperimentExportAdapter:
    """Represent fake experiment export adapter."""
    def __init__(self) -> None:
        """Initialize the fake experiment export adapter instance."""
        self.queries = []

    def query_rows(self, query, params=()):
        """Query rows."""
        self.queries.append((query, tuple(params)))
        if "FROM [ratings].[games] AS g" in query and "SELECT" in query and "Game_ID" in query:
            return [
                {
                    "Game_ID": 1,
                    "Tournament_Code": "EVENT-A",
                    "Game_Date": date(2026, 1, 1),
                    "Round": 1,
                    "Pin_Player_1": 1001,
                    "Pin_Player_2": 1002,
                    "Rank_1": "1d",
                    "Rank_2": "1d",
                    "Color_1": "W",
                    "Handicap": 0,
                    "Komi": 7,
                    "Result": "W",
                    "Rated": 1,
                    "Exclude": 0,
                    "Online": 0,
                }
            ]
        return [
            {
                "AGAID": 1001,
                "Rating": 1.2,
                "Sigma": 0.4,
                "Elab_Date": date(2025, 12, 1),
                "Tournament_Code": "PRIOR",
                "row_id": 10,
            }
        ]

    def execute_statements(self, statements):
        """Execute statements."""
        raise AssertionError("Experiment dataset export should not write SQL.")


class ExperimentToolTest(unittest.TestCase):
    """Represent experiment tool test."""
    def tearDown(self) -> None:
        """Execute the tearDown routine."""
        if TMP_DIR.exists():
            shutil.rmtree(TMP_DIR)

    def test_run_experiment_benchmark_writes_summary_and_comparison(self) -> None:
        """Verify that run experiment benchmark writes summary and comparison."""
        baseline = run_experiment_benchmark(
            games_path=FIXTURE_DIR / "smoke_games.csv",
            ratings_path=FIXTURE_DIR / "smoke_ratings.csv",
            name="baseline",
            output_dir=TMP_DIR / "baseline",
        )
        candidate = run_experiment_benchmark(
            games_path=FIXTURE_DIR / "smoke_games.csv",
            ratings_path=FIXTURE_DIR / "smoke_ratings.csv",
            name="candidate",
            output_dir=TMP_DIR / "candidate",
            baseline_summary_path=Path(baseline["summary_path"]),
        )

        summary = json.loads(Path(candidate["summary_path"]).read_text(encoding="utf-8"))
        comparison = json.loads(Path(candidate["comparison_path"]).read_text(encoding="utf-8"))

        self.assertEqual(summary["game_result_count"], 3)
        self.assertEqual(summary["pre_event_metrics"]["games"], 3)
        self.assertTrue((TMP_DIR / "candidate" / "player_results.csv").exists())
        self.assertTrue((TMP_DIR / "candidate" / "calibration.csv").exists())
        self.assertEqual(comparison["metric_deltas"]["pre_event_metrics.average_log_loss"]["delta"], 0.0)

    def test_distribution_handles_empty_and_percentiles(self) -> None:
        """Verify that distribution handles empty and percentiles."""
        self.assertEqual(distribution([])["count"], 0)
        self.assertEqual(distribution([1, 2, 3])["median"], 2.0)
        self.assertAlmostEqual(distribution([1, 2, 3])["p90"], 2.8)

    def test_load_config_from_json_accepts_best_config_wrapper(self) -> None:
        """Verify that load config from json accepts best config wrapper."""
        config_path = TMP_DIR / "best_config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            '{"score": 1.0, "config": {"surprise_sigma_base": 0.3, "surprise_sigma_score_mode": "pre"}}',
            encoding="utf-8",
        )

        config = load_config_from_json(config_path)

        self.assertAlmostEqual(config.surprise_sigma_base, 0.3)
        self.assertEqual(config.surprise_sigma_score_mode, "pre")

    def test_export_experiment_dataset_writes_csv_snapshot(self) -> None:
        """Verify that export experiment dataset writes csv snapshot."""
        adapter = FakeExperimentExportAdapter()

        metadata = export_experiment_dataset(
            adapter,
            output_dir=TMP_DIR / "dataset",
            min_game_date=date(2026, 1, 1),
            max_game_date=date(2026, 2, 1),
        )

        games_text = Path(metadata["games_path"]).read_text(encoding="utf-8")
        ratings_text = Path(metadata["ratings_path"]).read_text(encoding="utf-8")

        self.assertIn("Game_ID,Tournament_Code,Game_Date", games_text)
        self.assertIn("1,EVENT-A,2026-01-01", games_text)
        self.assertNotIn("AGAID,Rating,Sigma,Elab_Date", ratings_text)
        self.assertIn("1001,1.2,0.4,2025-12-01", ratings_text)
        self.assertEqual(metadata["game_count"], 1)
        self.assertEqual(metadata["rating_count"], 1)
        self.assertFalse(metadata["ratings_has_header"])
        self.assertEqual(len(adapter.queries), 2)


if __name__ == "__main__":
    unittest.main()
