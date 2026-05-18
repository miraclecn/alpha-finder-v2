import unittest

from alpha_find_v2.config_loader import CONFIG_ROOT, load_regime_overlay
from alpha_find_v2.regime_overlay import (
    RegimeOverlayEvaluator,
    RegimeOverlayObservationStep,
)


class RegimeOverlayEvaluatorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.overlay = load_regime_overlay(
            CONFIG_ROOT / "regime_overlays" / "a_share_risk_overlay.toml"
        )
        self.evaluator = RegimeOverlayEvaluator(self.overlay)

    def test_evaluator_maps_green_inputs_into_three_portfolio_states(self) -> None:
        evidence = self.evaluator.evaluate_history(
            trade_dates=["2026-04-06", "2026-04-13", "2026-04-20"],
            observations=[
                RegimeOverlayObservationStep(
                    trade_date="2026-04-06",
                    input_states={
                        "benchmark_trend": "supportive",
                        "market_breadth": "supportive",
                        "dispersion": "neutral",
                        "realized_volatility": "neutral",
                        "price_limit_stress": "neutral",
                    },
                ),
                RegimeOverlayObservationStep(
                    trade_date="2026-04-13",
                    input_states={
                        "benchmark_trend": "risk_off",
                        "market_breadth": "risk_off",
                        "dispersion": "neutral",
                        "realized_volatility": "neutral",
                        "price_limit_stress": "neutral",
                    },
                ),
                RegimeOverlayObservationStep(
                    trade_date="2026-04-20",
                    input_states={
                        "benchmark_trend": "risk_off",
                        "market_breadth": "risk_off",
                        "dispersion": "risk_off",
                        "realized_volatility": "neutral",
                        "price_limit_stress": "neutral",
                    },
                ),
            ],
        )

        self.assertEqual(
            [decision.state for decision in evidence.decisions],
            ["normal", "de_risk", "cash_heavier"],
        )
        self.assertEqual(
            [decision.status for decision in evidence.decisions],
            ["active", "active", "active"],
        )
        self.assertEqual(evidence.summary.normal_periods, 1)
        self.assertEqual(evidence.summary.de_risk_periods, 1)
        self.assertEqual(evidence.summary.cash_heavier_periods, 1)
        self.assertEqual(evidence.summary.blocked_periods, 0)

    def test_missing_stop_input_blocks_overlay_and_forces_cash_heavier(self) -> None:
        evidence = self.evaluator.evaluate_history(
            trade_dates=["2026-04-27"],
            observations=[
                RegimeOverlayObservationStep(
                    trade_date="2026-04-27",
                    input_states={
                        "benchmark_trend": "supportive",
                        "market_breadth": "supportive",
                        "dispersion": "neutral",
                        "realized_volatility": "neutral",
                    },
                )
            ],
        )

        decision = evidence.decisions[0]
        self.assertEqual(decision.status, "blocked")
        self.assertEqual(decision.state, "cash_heavier")
        self.assertEqual(decision.missing_inputs, ["price_limit_stress"])
        self.assertEqual(evidence.summary.blocked_periods, 1)
        self.assertEqual(evidence.summary.missing_inputs, ["price_limit_stress"])

    def test_overlay_exposure_budgets_stay_monotonic(self) -> None:
        self.assertAlmostEqual(self.overlay.normal_gross_exposure, 1.0)
        self.assertGreater(self.overlay.normal_gross_exposure, self.overlay.de_risk_gross_exposure)
        self.assertGreater(
            self.overlay.de_risk_gross_exposure,
            self.overlay.cash_heavier_gross_exposure,
        )


if __name__ == "__main__":
    unittest.main()
