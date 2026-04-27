from pathlib import Path
import unittest

from bayrate.core import (
    BayrateConfig,
    CsvValidationError,
    calc_handicap_eqv,
    load_games_from_csv,
    load_official_history,
    rank_to_seed,
    run_bayrate,
)


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

    def test_same_day_tournaments_follow_game_csv_order(self) -> None:
        result = run_bayrate(
            FIXTURE_DIR / "same_day_order_games.csv",
            FIXTURE_DIR / "smoke_ratings.csv",
            BayrateConfig(random_seed=1),
        )

        event_order = []
        for game in result.game_results:
            if game.tournament_code not in event_order:
                event_order.append(game.tournament_code)

        self.assertEqual(event_order, ["SMOKE-Z", "SMOKE-A"])

        first_event_1001 = next(
            row
            for row in result.player_results
            if row.player_id == 1001 and row.tournament_code == "SMOKE-Z"
        )
        second_event_1001 = next(
            row
            for row in result.player_results
            if row.player_id == 1001 and row.tournament_code == "SMOKE-A"
        )

        self.assertAlmostEqual(second_event_1001.prior_rating, first_event_1001.rating_after)
        self.assertAlmostEqual(second_event_1001.prior_sigma, first_event_1001.sigma_after)

    def test_events_process_chronologically_before_csv_tie_breaker(self) -> None:
        result = run_bayrate(
            FIXTURE_DIR / "chronological_order_games.csv",
            FIXTURE_DIR / "smoke_ratings.csv",
            BayrateConfig(random_seed=1),
        )

        event_order = []
        for game in result.game_results:
            if game.tournament_code not in event_order:
                event_order.append(game.tournament_code)

        self.assertEqual(event_order, ["SMOKE-EARLY", "SMOKE-MULTIDAY", "SMOKE-LATE"])

        multiday_1001 = next(
            row
            for row in result.player_results
            if row.player_id == 1001 and row.tournament_code == "SMOKE-MULTIDAY"
        )
        late_1001 = next(
            row
            for row in result.player_results
            if row.player_id == 1001 and row.tournament_code == "SMOKE-LATE"
        )

        self.assertEqual(multiday_1001.event_date.isoformat(), "2026-01-02")
        self.assertEqual(late_1001.event_date.isoformat(), "2026-02-01")
        self.assertAlmostEqual(late_1001.prior_rating, multiday_1001.rating_after)
        self.assertAlmostEqual(late_1001.prior_sigma, multiday_1001.sigma_after)

    def test_game_loader_reports_malformed_rows(self) -> None:
        with self.assertRaises(CsvValidationError) as context:
            load_games_from_csv(FIXTURE_DIR / "bad_games.csv", BayrateConfig())

        self.assertEqual(len(context.exception.errors), 2)
        self.assertEqual(context.exception.errors[0].line_number, 2)
        self.assertEqual(context.exception.errors[0].column, "Pin_Player_2")
        self.assertEqual(context.exception.errors[1].line_number, 3)
        self.assertEqual(context.exception.errors[1].column, "Rank_1")

    def test_rating_loader_reports_malformed_rows(self) -> None:
        with self.assertRaises(CsvValidationError) as context:
            load_official_history(FIXTURE_DIR / "bad_ratings.csv")

        self.assertEqual(len(context.exception.errors), 2)
        self.assertEqual(context.exception.errors[0].line_number, 2)
        self.assertEqual(context.exception.errors[1].line_number, 3)


if __name__ == "__main__":
    unittest.main()
