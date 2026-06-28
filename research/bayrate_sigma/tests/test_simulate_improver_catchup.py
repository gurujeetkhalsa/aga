from datetime import date
import random
import unittest

from bayrate.core import (
    BayrateConfig,
    GameRecord,
    TdListEntry,
    _calc_posterior_sigma_after_noise,
    _signed_observed_information_excess,
    _signed_surprise_sigma_score,
    _signed_volatility_trigger_score,
    close_boundary,
    run_bayrate_loaded,
)
from research.bayrate_sigma.simulate_improver_catchup import (
    one_rank_stronger,
    parse_activity_levels,
    scheduled_dates,
    sigma_band,
)


class SimulateImproverCatchupTest(unittest.TestCase):
    """Represent simulate improver catchup test."""
    def test_one_rank_stronger_handles_kyu_dan_boundary(self) -> None:
        """Verify that one rank stronger handles kyu dan boundary."""
        self.assertAlmostEqual(one_rank_stronger(-2.5), -1.5)
        self.assertAlmostEqual(one_rank_stronger(-1.5), 1.5)
        self.assertAlmostEqual(one_rank_stronger(1.5), 2.5)

    def test_activity_parser_and_schedule(self) -> None:
        """Verify that activity parser and schedule."""
        self.assertEqual(parse_activity_levels("low:8, high:50"), [("low", 8), ("high", 50)])

        dates = scheduled_dates(date(2026, 1, 1), games_per_year=8, years=1.0, rng=random.Random(1))

        self.assertEqual(len(dates), 8)
        self.assertTrue(all(date(2026, 1, 1) <= value <= date(2027, 1, 1) for value in dates))
        self.assertEqual(dates, sorted(dates))

    def test_sigma_band_boundaries(self) -> None:
        """Verify that sigma band boundaries."""
        self.assertEqual(sigma_band(0.24), "narrow_<0.25")
        self.assertEqual(sigma_band(0.25), "medium_0.25_to_<0.50")
        self.assertEqual(sigma_band(0.50), "wide_0.50_to_<0.90")
        self.assertEqual(sigma_band(0.90), "very_wide_>=0.90")

    def test_surprise_sigma_config_is_opt_in(self) -> None:
        """Verify that surprise sigma config is opt in."""
        game = GameRecord(
            source_game_id=1,
            tournament_code="SIM",
            game_date=date(2026, 1, 1),
            round_number=1,
            white_agaid=1001,
            black_agaid=1002,
            white_seed_rank=1.5,
            black_seed_rank=1.5,
            handicap=0,
            komi=7.5,
            white_wins=True,
            is_online_game=False,
        )
        initial_td_list = {
            1001: TdListEntry(1001, rating=1.5, sigma=0.20, last_rating_date=date(2025, 12, 31)),
            1002: TdListEntry(1002, rating=1.5, sigma=0.20, last_rating_date=date(2025, 12, 31)),
        }

        baseline = run_bayrate_loaded(
            [game],
            {},
            BayrateConfig(optimizer_random_jitter=0.0),
            initial_td_list=initial_td_list,
        )
        surprise = run_bayrate_loaded(
            [game],
            {},
            BayrateConfig(optimizer_random_jitter=0.0, surprise_sigma_base=0.24),
            initial_td_list=initial_td_list,
        )

        baseline_white = next(row for row in baseline.player_results if row.player_id == 1001)
        surprise_white = next(row for row in surprise.player_results if row.player_id == 1001)

        self.assertAlmostEqual(close_boundary(baseline_white.rating_after), close_boundary(surprise_white.rating_after))
        self.assertGreater(surprise_white.sigma_after, baseline_white.sigma_after)

    def test_surprise_persistence_can_wait_for_repeated_surprise(self) -> None:
        """Verify that surprise persistence can wait for repeated surprise."""
        games = [
            GameRecord(
                source_game_id=1,
                tournament_code="SIM1",
                game_date=date(2026, 1, 1),
                round_number=1,
                white_agaid=1001,
                black_agaid=1002,
                white_seed_rank=1.5,
                black_seed_rank=1.5,
                handicap=0,
                komi=7.5,
                white_wins=True,
                is_online_game=False,
            ),
            GameRecord(
                source_game_id=2,
                tournament_code="SIM2",
                game_date=date(2026, 2, 1),
                round_number=1,
                white_agaid=1001,
                black_agaid=1002,
                white_seed_rank=1.5,
                black_seed_rank=1.5,
                handicap=0,
                komi=7.5,
                white_wins=True,
                is_online_game=False,
            ),
        ]
        initial_td_list = {
            1001: TdListEntry(1001, rating=1.5, sigma=0.20, last_rating_date=date(2025, 12, 31)),
            1002: TdListEntry(1002, rating=1.5, sigma=0.20, last_rating_date=date(2025, 12, 31)),
        }
        one_event_config = BayrateConfig(
            optimizer_random_jitter=0.0,
            surprise_sigma_base=0.0,
            surprise_sigma_scale=0.5,
            surprise_sigma_deadband=0.6,
            surprise_sigma_cap=1.0,
            surprise_sigma_gate_base_by_deadband=True,
        )
        persistent_config = BayrateConfig(
            optimizer_random_jitter=0.0,
            surprise_sigma_base=0.0,
            surprise_sigma_scale=0.5,
            surprise_sigma_deadband=0.6,
            surprise_sigma_cap=1.0,
            surprise_sigma_gate_base_by_deadband=True,
            surprise_persistence_weight=1.0,
            surprise_persistence_decay=1.0,
            surprise_persistence_cap=2.0,
            surprise_persistence_same_direction_only=True,
        )

        one_event = run_bayrate_loaded(games, {}, one_event_config, initial_td_list=initial_td_list)
        persistent = run_bayrate_loaded(games, {}, persistent_config, initial_td_list=initial_td_list)

        one_event_white_second = [
            row for row in one_event.player_results if row.player_id == 1001
        ][1]
        persistent_white_second = [
            row for row in persistent.player_results if row.player_id == 1001
        ][1]

        self.assertGreater(persistent_white_second.sigma_after, one_event_white_second.sigma_after)

    def test_shannon_surprise_mode_uses_log_observed_probability(self) -> None:
        """Verify that shannon surprise mode uses log observed probability."""
        game = GameRecord(
            source_game_id=1,
            tournament_code="SIM",
            game_date=date(2026, 1, 1),
            round_number=1,
            white_agaid=1001,
            black_agaid=1002,
            white_seed_rank=1.5,
            black_seed_rank=1.5,
            handicap=0,
            komi=7.5,
            white_wins=True,
            is_online_game=False,
        )
        initial_td_list = {
            1001: TdListEntry(1001, rating=1.5, sigma=0.20, last_rating_date=date(2025, 12, 31)),
            1002: TdListEntry(1002, rating=1.5, sigma=0.20, last_rating_date=date(2025, 12, 31)),
        }
        residual = run_bayrate_loaded(
            [game],
            {},
            BayrateConfig(
                optimizer_random_jitter=0.0,
                surprise_sigma_base=0.0,
                surprise_sigma_scale=0.5,
                surprise_sigma_deadband=0.6,
                surprise_sigma_cap=1.0,
                surprise_sigma_gate_base_by_deadband=True,
                surprise_sigma_score_mode="pre",
            ),
            initial_td_list=initial_td_list,
        )
        shannon = run_bayrate_loaded(
            [game],
            {},
            BayrateConfig(
                optimizer_random_jitter=0.0,
                surprise_sigma_base=0.0,
                surprise_sigma_scale=0.5,
                surprise_sigma_deadband=0.6,
                surprise_sigma_cap=1.0,
                surprise_sigma_gate_base_by_deadband=True,
                surprise_sigma_score_mode="pre_shannon",
            ),
            initial_td_list=initial_td_list,
        )

        residual_white = next(row for row in residual.player_results if row.player_id == 1001)
        shannon_white = next(row for row in shannon.player_results if row.player_id == 1001)

        self.assertGreater(shannon_white.sigma_after, residual_white.sigma_after)

    def test_shannon_excess_is_centered_around_expectation(self) -> None:
        """Verify that shannon excess is centered around expectation."""
        predicted = 0.75
        win_excess = _signed_observed_information_excess(1.0, predicted)
        loss_excess = _signed_observed_information_excess(0.0, predicted)

        self.assertAlmostEqual(predicted * win_excess + (1.0 - predicted) * loss_excess, 0.0)
        self.assertGreater(win_excess, 0.0)
        self.assertLess(loss_excess, 0.0)

    def test_volatility_trigger_scales_by_explainable_variance(self) -> None:
        """Verify that volatility trigger scales by explainable variance."""
        narrow = _signed_volatility_trigger_score(
            rating_score=4.0,
            rating_information=4.0,
            prior_sigma=0.2,
        )
        wide = _signed_volatility_trigger_score(
            rating_score=4.0,
            rating_information=4.0,
            prior_sigma=2.0,
        )
        negative = _signed_volatility_trigger_score(
            rating_score=-4.0,
            rating_information=4.0,
            prior_sigma=0.2,
        )

        self.assertGreater(narrow, 1.0)
        self.assertLess(wide, narrow)
        self.assertLess(negative, 0.0)

    def test_volatility_min_pre_post_uses_smaller_remaining_jump(self) -> None:
        """Verify that volatility min pre post uses smaller remaining jump."""
        score = _signed_surprise_sigma_score(
            pre_residual=0.0,
            post_residual=0.0,
            pre_information=0.0,
            post_information=0.0,
            pre_information_excess=0.0,
            post_information_excess=0.0,
            pre_volatility_score=4.0,
            pre_volatility_information=4.0,
            post_volatility_score=0.5,
            post_volatility_information=4.0,
            prior_sigma=0.2,
            game_count=1,
            mode="volatility_min_pre_post",
        )

        self.assertLess(score, 0.5)

    def test_hybrid_volatility_keeps_stored_sigma_cool_and_carries_temporary_boost(self) -> None:
        """Verify that hybrid volatility keeps stored sigma cool and carries temporary boost."""
        sigma_after, _persistence, temporary_boost = _calc_posterior_sigma_after_noise(
            0.2,
            surprise_residual=0.0,
            post_surprise_residual=0.0,
            surprise_information=0.0,
            post_surprise_information=0.0,
            surprise_information_excess=0.0,
            post_surprise_information_excess=0.0,
            pre_volatility_score=4.0,
            pre_volatility_information=4.0,
            post_volatility_score=0.2,
            post_volatility_information=4.0,
            surprise_game_count=1,
            previous_surprise_persistence=0.0,
            pre_event_rating=2.0,
            prior_sigma=0.2,
            config=BayrateConfig(
                surprise_sigma_base=0.0,
                surprise_sigma_scale=1.0,
                surprise_sigma_deadband=0.5,
                surprise_sigma_cap=1.0,
                surprise_sigma_gate_base_by_deadband=True,
                surprise_sigma_score_mode="volatility_hybrid_pre_post",
            ),
        )

        self.assertAlmostEqual(sigma_after, 0.2)
        self.assertGreater(temporary_boost, 0.0)


if __name__ == "__main__":
    unittest.main()
