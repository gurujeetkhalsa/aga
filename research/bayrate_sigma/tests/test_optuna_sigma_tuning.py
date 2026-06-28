import math
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from research.bayrate_sigma.simulate_activity_grid import load_config_from_json
from bayrate.core import BayrateConfig
from research.bayrate_sigma.optuna_sigma_tuning import (
    FixedTrial,
    PreparedScenario,
    ScoreWeights,
    gaussian_gap_nll_for_rms,
    parse_float_list,
    parse_int_list,
    parse_score_modes,
    parse_string_list,
    score_historical_metrics,
    score_summaries,
    score_summary,
    summarize_for_score,
    suggest_config,
    trial_params_from_config,
    validate_search_ranges,
)


class OptunaSigmaTuningTest(unittest.TestCase):
    """Represent optuna sigma tuning test."""
    def test_parse_lists(self) -> None:
        """Verify that parse lists."""
        self.assertEqual(parse_float_list("0, 1.0", field_name="x"), [0.0, 1.0])
        self.assertEqual(parse_int_list("1,3", field_name="year"), [1, 3])
        self.assertEqual(parse_string_list("a, b", field_name="label"), ["a", "b"])
        self.assertEqual(
            parse_score_modes("pre, volatility_hybrid_pre_post"),
            ("pre", "volatility_hybrid_pre_post"),
        )

        with self.assertRaises(ValueError):
            parse_float_list("", field_name="x")
        with self.assertRaises(ValueError):
            parse_score_modes("not_a_mode")

    def test_fixed_trial_builds_bayrate_config(self) -> None:
        """Verify that fixed trial builds bayrate config."""
        config = suggest_config(
            FixedTrial(
                {
                    "posterior_process_noise": 0.01,
                    "surprise_sigma_base": 0.22,
                    "surprise_sigma_scale": 0.45,
                    "surprise_sigma_deadband": 0.30,
                    "surprise_sigma_cap": 0.60,
                    "surprise_sigma_score_mode": "min_pre_post",
                    "surprise_sigma_gate_base_by_deadband": True,
                    "surprise_taper_start_rating": 5.5,
                    "surprise_taper_width": 2.0,
                    "surprise_taper_floor": 0.40,
                }
            )
        )

        self.assertEqual(config.surprise_sigma_score_mode, "min_pre_post")
        self.assertTrue(config.surprise_sigma_gate_base_by_deadband)
        self.assertAlmostEqual(config.surprise_taper_start_rating, 5.5)
        self.assertAlmostEqual(config.surprise_taper_end_rating, 7.5)

    def test_fixed_trial_honors_custom_taper_ranges(self) -> None:
        """Verify that fixed trial honors custom taper ranges."""
        config = suggest_config(
            FixedTrial(
                {
                    "posterior_process_noise": 0.01,
                    "surprise_sigma_base": 0.22,
                    "surprise_sigma_scale": 0.45,
                    "surprise_sigma_deadband": 0.30,
                    "surprise_sigma_cap": 0.60,
                    "surprise_sigma_score_mode": "pre",
                    "surprise_sigma_gate_base_by_deadband": False,
                    "surprise_taper_start_rating": 1.2,
                    "surprise_taper_width": 3.2,
                    "surprise_taper_floor": 0.55,
                }
            ),
            taper_start_range=(1.0, 3.0),
            taper_width_range=(2.0, 4.0),
            taper_floor_range=(0.3, 0.8),
        )

        self.assertAlmostEqual(config.surprise_taper_start_rating, 1.2)
        self.assertAlmostEqual(config.surprise_taper_end_rating, 4.4)
        self.assertAlmostEqual(config.surprise_taper_floor, 0.55)

    def test_fixed_trial_honors_score_mode_filter(self) -> None:
        """Verify that fixed trial honors score mode filter."""
        config = suggest_config(
            FixedTrial(
                {
                    "posterior_process_noise": 0.01,
                    "surprise_sigma_base": 0.22,
                    "surprise_sigma_scale": 0.45,
                    "surprise_sigma_deadband": 0.30,
                    "surprise_sigma_cap": 0.60,
                    "surprise_sigma_score_mode": "volatility_min_pre_post",
                    "surprise_sigma_gate_base_by_deadband": True,
                    "surprise_taper_start_rating": 5.5,
                    "surprise_taper_width": 2.0,
                    "surprise_taper_floor": 0.40,
                }
            ),
            score_modes=("volatility_min_pre_post",),
        )

        self.assertEqual(config.surprise_sigma_score_mode, "volatility_min_pre_post")

    def test_validate_search_ranges_rejects_inverted_bounds(self) -> None:
        """Verify that validate search ranges rejects inverted bounds."""
        class Args:
            """Represent args."""
            score_modes = "pre"
            taper_start_min = 4.0
            taper_start_max = 1.0
            taper_width_min = 0.5
            taper_width_max = 5.0
            taper_floor_min = 0.0
            taper_floor_max = 1.0
            surprise_base_min = 0.0
            surprise_base_max = 0.35
            surprise_scale_min = 0.0
            surprise_scale_max = 0.80
            surprise_deadband_min = 0.05
            surprise_deadband_max = 0.80
            surprise_cap_min = 0.20
            surprise_cap_max = 0.80
            surprise_persistence_weight_min = 0.0
            surprise_persistence_weight_max = 0.0
            surprise_persistence_decay_min = 0.0
            surprise_persistence_decay_max = 0.0
            surprise_persistence_cap_min = 2.0
            surprise_persistence_cap_max = 2.0

        with self.assertRaises(ValueError):
            validate_search_ranges(Args())

    def test_trial_params_from_config_round_trips_seed_config(self) -> None:
        """Verify that trial params from config round trips seed config."""
        params = trial_params_from_config(
            BayrateConfig(
                posterior_process_noise=0.02,
                surprise_sigma_base=0.24,
                surprise_sigma_scale=0.40,
                surprise_sigma_deadband=0.35,
                surprise_sigma_cap=0.55,
                surprise_sigma_score_mode="post",
                surprise_sigma_gate_base_by_deadband=True,
                surprise_taper_start_rating=6.0,
                surprise_taper_end_rating=8.0,
                surprise_taper_floor=0.50,
                surprise_persistence_weight=0.7,
                surprise_persistence_decay=0.8,
                surprise_persistence_cap=1.5,
                surprise_persistence_same_direction_only=True,
            )
        )

        self.assertAlmostEqual(params["surprise_taper_width"], 2.0)
        self.assertEqual(params["surprise_sigma_score_mode"], "post")
        self.assertTrue(params["surprise_sigma_gate_base_by_deadband"])
        self.assertAlmostEqual(params["surprise_persistence_weight"], 0.7)
        self.assertTrue(params["surprise_persistence_same_direction_only"])

    def test_trial_params_from_config_uses_active_seed_ranges(self) -> None:
        """Verify that trial params from config uses active seed ranges."""
        config = BayrateConfig(
            posterior_process_noise=0.02,
            surprise_sigma_base=0.24,
            surprise_sigma_scale=0.40,
            surprise_sigma_deadband=0.35,
            surprise_sigma_cap=0.55,
            surprise_sigma_score_mode="volatility_hybrid_pre_min",
            surprise_taper_start_rating=1.8,
            surprise_taper_end_rating=7.1,
            surprise_taper_floor=0.90,
        )

        params = trial_params_from_config(
            config,
            taper_width_range=(1.0, 6.0),
            score_modes=("volatility_hybrid_pre_min",),
        )

        self.assertAlmostEqual(params["surprise_taper_width"], 5.3)
        with self.assertRaises(ValueError):
            trial_params_from_config(config, score_modes=("volatility_pre",))

    def test_summary_converts_milestones_to_percent_progress(self) -> None:
        """Verify that summary converts milestones to percent progress."""
        scenario = PreparedScenario(
            name="self_0_25gpy",
            games_per_year=25,
            self_promote_delta=0.0,
            players=[],
            games=[],
            initial_td_list={},
        )
        rows = [
            milestone_row(year=3, delta=1.0, gap=0.0, sigma=0.45, reached=True),
            milestone_row(year=3, delta=0.5, gap=0.5, sigma=0.45, reached=False),
            milestone_row(year=2, delta=0.25, gap=0.75, sigma=0.50, reached=False),
        ]

        summaries = summarize_for_score(rows, scenario=scenario)
        year3 = next(row for row in summaries if row["year"] == 3)

        self.assertEqual(year3["players"], 2)
        self.assertAlmostEqual(year3["mean_rank_change_pct"], 75.0)
        self.assertAlmostEqual(year3["reached_rate"], 50.0)
        self.assertAlmostEqual(year3["within_sigma_rate"], 50.0)
        self.assertAlmostEqual(year3["mean_abs_gap_to_true"], 0.25)
        self.assertAlmostEqual(year3["rms_gap_to_true"], math.sqrt(0.125))

    def test_summary_adds_rating_band_slices(self) -> None:
        """Verify that summary adds rating band slices."""
        scenario = PreparedScenario(
            name="self_0_25gpy",
            games_per_year=25,
            self_promote_delta=0.0,
            players=[],
            games=[],
            initial_td_list={},
        )
        rows = [
            {**milestone_row(year=3, delta=1.3, gap=-0.3, sigma=0.45, reached=True), "rating_band": "1d_to_<3d"},
            {**milestone_row(year=3, delta=0.5, gap=0.5, sigma=0.45, reached=False), "rating_band": "5k_to_<1d"},
            {**milestone_row(year=3, delta=1.6, gap=-0.6, sigma=0.50, reached=True), "rating_band": "1d_to_<3d"},
        ]

        summaries = summarize_for_score(rows, scenario=scenario)
        all_row = next(row for row in summaries if row["slice_type"] == "all")
        dan_row = next(row for row in summaries if row["slice_value"] == "1d_to_<3d")

        self.assertEqual(all_row["players"], 3)
        self.assertEqual(dan_row["players"], 2)
        self.assertAlmostEqual(dan_row["pct_ge_125"], 100.0)

    def test_score_rewards_balanced_progress_with_less_overshoot(self) -> None:
        """Verify that score rewards balanced progress with less overshoot."""
        weights = ScoreWeights()
        current_like = {
            "mean_rank_change_pct": 103.0,
            "reached_rate": 58.0,
            "pct_ge_125": 31.0,
            "pct_gt_150": 5.0,
            "median_sigma": 0.71,
        }
        balanced_like = {
            "mean_rank_change_pct": 101.0,
            "reached_rate": 59.0,
            "pct_ge_125": 24.0,
            "pct_gt_150": 2.0,
            "median_sigma": 0.45,
        }

        self.assertLess(score_summary(balanced_like, weights), score_summary(current_like, weights))

    def test_score_can_penalize_mean_absolute_gap(self) -> None:
        """Verify that score can penalize mean absolute gap."""
        close_but_slow = {
            "mean_rank_change_pct": 80.0,
            "reached_rate": 20.0,
            "pct_ge_125": 0.0,
            "pct_gt_150": 0.0,
            "median_sigma": 0.45,
            "mean_abs_gap_to_true": 0.20,
        }
        crossed_but_far = {
            "mean_rank_change_pct": 120.0,
            "reached_rate": 80.0,
            "pct_ge_125": 0.0,
            "pct_gt_150": 0.0,
            "median_sigma": 0.45,
            "mean_abs_gap_to_true": 0.60,
        }

        self.assertLess(
            score_summary(close_but_slow, ScoreWeights(mean_abs_gap=1.0, reached_shortfall=0.0)),
            score_summary(crossed_but_far, ScoreWeights(mean_abs_gap=1.0, reached_shortfall=0.0)),
        )

    def test_gaussian_gap_nll_rewards_smaller_fitted_sd(self) -> None:
        """Verify that gaussian gap nll rewards smaller fitted sd."""
        self.assertLess(gaussian_gap_nll_for_rms(0.25), gaussian_gap_nll_for_rms(0.50))

    def test_score_penalizes_rank_band_overshoot_guardrail(self) -> None:
        """Verify that score penalizes rank band overshoot guardrail."""
        summaries = [
            {
                "slice_type": "all",
                "slice_value": "all",
                "year": 3,
                "players": 20,
                "mean_rank_change_pct": 100.0,
                "reached_rate": 60.0,
                "pct_ge_125": 20.0,
                "pct_gt_150": 0.0,
                "median_sigma": 0.45,
            },
            {
                "slice_type": "rating_band",
                "slice_value": "1d_to_<3d",
                "year": 3,
                "players": 20,
                "mean_rank_change_pct": 130.0,
                "reached_rate": 90.0,
                "pct_ge_125": 80.0,
                "pct_gt_150": 20.0,
                "median_sigma": 0.45,
            },
        ]

        without_guardrail = score_summaries(summaries, score_years={3}, weights=ScoreWeights())
        with_guardrail = score_summaries(
            summaries,
            score_years={3},
            weights=ScoreWeights(rank_overshoot_125=1.0, rank_overshoot_150=2.0),
        )

        self.assertGreater(with_guardrail, without_guardrail)

    def test_score_penalizes_hot_rank_band_slice(self) -> None:
        """Verify that score penalizes hot rank band slice."""
        summaries = [
            {
                "slice_type": "all",
                "slice_value": "all",
                "year": 3,
                "players": 30,
                "mean_rank_change_pct": 100.0,
                "reached_rate": 60.0,
                "pct_ge_125": 20.0,
                "pct_gt_150": 0.0,
                "median_sigma": 0.45,
            },
            {
                "slice_type": "rating_band",
                "slice_value": "1d_to_<3d",
                "year": 3,
                "players": 20,
                "mean_rank_change_pct": 130.0,
                "reached_rate": 90.0,
                "pct_ge_125": 50.0,
                "pct_gt_150": 30.0,
                "median_sigma": 0.45,
            },
            {
                "slice_type": "rating_band",
                "slice_value": "3d_to_<5d",
                "year": 3,
                "players": 20,
                "mean_rank_change_pct": 100.0,
                "reached_rate": 60.0,
                "pct_ge_125": 0.0,
                "pct_gt_150": 0.0,
                "median_sigma": 0.45,
            },
        ]

        without_hot = score_summaries(summaries, score_years={3}, weights=ScoreWeights())
        with_hot = score_summaries(
            summaries,
            score_years={3},
            weights=ScoreWeights(
                rank_hot_overshoot_125=1.0,
                rank_hot_overshoot_150=1.0,
                rank_hot_overshoot_target_125=45.0,
                rank_hot_overshoot_target_150=20.0,
            ),
        )

        self.assertGreater(with_hot, without_hot)

    def test_historical_score_rewards_predictive_improvement(self) -> None:
        """Verify that historical score rewards predictive improvement."""
        weights = ScoreWeights(historical_log_loss=1000.0, historical_brier=500.0, historical_accuracy=10.0)

        self.assertLess(
            score_historical_metrics(
                {
                    "historical_log_loss_delta": -0.001,
                    "historical_brier_delta": -0.002,
                    "historical_accuracy_delta": 0.01,
                },
                weights,
            ),
            0.0,
        )

    def test_activity_grid_loads_config_json_wrapped_or_plain(self) -> None:
        """Verify that activity grid loads config json wrapped or plain."""
        with TemporaryDirectory() as tmp:
            wrapped = Path(tmp) / "wrapped.json"
            wrapped.write_text(
                '{"score": 1.0, "config": {"surprise_sigma_base": 0.21, "surprise_sigma_score_mode": "pre"}}',
                encoding="utf-8",
            )
            plain = Path(tmp) / "plain.json"
            plain.write_text('{"surprise_sigma_base": 0.22}', encoding="utf-8")

            self.assertAlmostEqual(load_config_from_json(wrapped).surprise_sigma_base, 0.21)
            self.assertEqual(load_config_from_json(wrapped).surprise_sigma_score_mode, "pre")
            self.assertAlmostEqual(load_config_from_json(plain).surprise_sigma_base, 0.22)


def milestone_row(*, year: int, delta: float, gap: float, sigma: float, reached: bool) -> dict[str, object]:
    """Execute the milestone row routine."""
    return {
        "year": year,
        "start_rating": 1.0,
        "true_strength": 2.0,
        "rating_delta_from_start": delta,
        "gap_to_true": gap,
        "sigma_at_milestone": sigma,
        "reached_true_strength": reached,
    }


if __name__ == "__main__":
    unittest.main()
