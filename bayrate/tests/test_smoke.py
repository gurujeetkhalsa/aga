from pathlib import Path
import unittest

from bayrate.core import BayrateConfig, calc_handicap_eqv, rank_to_seed, run_bayrate


FIXTURE_DIR = Path(__file__).parent / "fixtures"


class BayrateSmokeTest(unittest.TestCase):
    def test_rank_and_handicap_helpers(self) -> None:
        self.assertEqual(rank_to_seed("12k"), -12.5)
        self.assertEqual(rank_to_seed("1d"), 1.5)

        handicap_eqv, sigma_px = calc_handicap_eqv(0, 7.5)
        self.assertAlmostEqual(handicap_eqv, 0.01225)
        self.assertGreater(sigma_px, 1.0)

    def test_two_tournament_replay_carries_rating_forward(self) -> None:
        result = run_bayrate(
            FIXTURE_DIR / "smoke_games.csv",
            FIXTURE_DIR / "smoke_ratings.csv",
            BayrateConfig(random_seed=1),
        )

        self.assertEqual(result.event_count, 2)
        self.assertEqual(len(result.game_results), 3)

        first_event_1001 = next(
            row
            for row in result.player_results
            if row.player_id == 1001 and row.tournament_code == "SMOKE-A"
        )
        second_event_1001 = next(
            row
            for row in result.player_results
            if row.player_id == 1001 and row.tournament_code == "SMOKE-B"
        )

        self.assertAlmostEqual(second_event_1001.prior_rating, first_event_1001.rating_after)
        self.assertAlmostEqual(second_event_1001.prior_sigma, first_event_1001.sigma_after)


if __name__ == "__main__":
    unittest.main()
