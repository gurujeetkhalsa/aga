import json
import unittest
from datetime import date
from pathlib import Path

from bayrate.compare_replay_to_production import compare_replay_artifact_to_production


class FakeRatingsAdapter:
    def query_rows(self, query, params=()):
        return [
            {
                "id": 10,
                "Pin_Player": 1001,
                "Rating": 1.25,
                "Sigma": 0.4,
                "Elab_Date": date(2026, 1, 1),
                "Tournament_Code": "EVENT-A",
            },
            {
                "id": 11,
                "Pin_Player": 1002,
                "Rating": 2.0,
                "Sigma": 0.5,
                "Elab_Date": date(2026, 1, 1),
                "Tournament_Code": "EVENT-B",
            },
        ]

    def execute_statements(self, statements):
        raise AssertionError("Replay comparison should not write SQL.")


class CompareReplayToProductionTest(unittest.TestCase):
    def test_compare_replay_artifact_to_production_summarizes_rating_deltas(self) -> None:
        artifact_path = Path(__file__).parent / "tmp_compare_replay_artifact.json"
        output_path = Path(__file__).parent / "tmp_compare_replay_output.json"
        artifact = {
            "plan": {
                "run_id": 999,
                "events": [
                    {"tournament_code": "EVENT-A"},
                    {"tournament_code": "EVENT-B"},
                ],
            },
            "bayrate_result": {
                "player_results": [
                    {
                        "tournament_code": "EVENT-A",
                        "player_id": 1001,
                        "event_date": "2026-01-01",
                        "rating_after": 1.2,
                        "sigma_after": 0.41,
                    },
                    {
                        "tournament_code": "EVENT-B",
                        "player_id": 1002,
                        "event_date": "2026-01-01",
                        "rating_after": 2.1,
                        "sigma_after": 0.49,
                    },
                ],
            },
        }
        try:
            artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

            comparison = compare_replay_artifact_to_production(
                FakeRatingsAdapter(),
                artifact_path,
                output_path=output_path,
                top=2,
            )

            self.assertTrue(output_path.exists())
            self.assertEqual(comparison["overall"]["max_abs_rating_delta_player_id"], 1002)
            self.assertAlmostEqual(comparison["overall"]["max_abs_rating_delta"], 0.1)
            self.assertEqual(comparison["tournaments"][0]["matched_count"], 1)
            self.assertEqual(comparison["tournaments"][1]["matched_count"], 1)
        finally:
            for path in (artifact_path, output_path):
                if path.exists():
                    path.unlink()


if __name__ == "__main__":
    unittest.main()
