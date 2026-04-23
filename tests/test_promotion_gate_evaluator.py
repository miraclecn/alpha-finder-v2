import unittest

from alpha_find_v2.config_loader import CONFIG_ROOT, load_promotion_gate
from alpha_find_v2.promotion_gate_evaluator import (
    PortfolioPromotionGateEvaluator,
    SleevePromotionSnapshot,
)


class PortfolioPromotionGateEvaluatorTest(unittest.TestCase):
    def test_gate_passes_when_sleeve_improves_portfolio(self) -> None:
        gate = load_promotion_gate(
            CONFIG_ROOT / "promotion_gates" / "a_share_core_portfolio_gate.toml"
        )
        evaluator = PortfolioPromotionGateEvaluator(gate)

        decision = evaluator.evaluate(
            SleevePromotionSnapshot(
                oos_ir=0.42,
                oos_tstat=2.3,
                breadth=120,
                peak_to_trough_drawdown=0.12,
                cost_scenario_pass={"base": True, "high": True},
                regime_pass={
                    "bull": True,
                    "bear": True,
                    "high_dispersion": True,
                    "low_dispersion": True,
                },
                max_component_correlation=0.58,
                correlation_to_existing_portfolio=0.44,
                realized_turnover_vs_budget=1.03,
                limit_locked_name_share=0.05,
                marginal_ir_delta=0.05,
                marginal_drawdown_increase=0.01,
            )
        )

        self.assertTrue(decision.passed)
        self.assertEqual(decision.failed_checks, [])

    def test_gate_fails_when_marginal_portfolio_value_is_missing(self) -> None:
        gate = load_promotion_gate(
            CONFIG_ROOT / "promotion_gates" / "a_share_core_portfolio_gate.toml"
        )
        evaluator = PortfolioPromotionGateEvaluator(gate)

        decision = evaluator.evaluate(
            SleevePromotionSnapshot(
                oos_ir=0.42,
                oos_tstat=2.3,
                breadth=120,
                peak_to_trough_drawdown=0.12,
                cost_scenario_pass={"base": True, "high": True},
                regime_pass={
                    "bull": True,
                    "bear": True,
                    "high_dispersion": True,
                    "low_dispersion": True,
                },
                max_component_correlation=0.58,
                correlation_to_existing_portfolio=0.44,
                realized_turnover_vs_budget=1.03,
                limit_locked_name_share=0.05,
                marginal_ir_delta=0.0,
                marginal_drawdown_increase=0.03,
            )
        )

        self.assertFalse(decision.passed)
        self.assertIn("minimum_marginal_ir_delta", decision.failed_checks)
        self.assertIn("max_marginal_drawdown_increase", decision.failed_checks)


if __name__ == "__main__":
    unittest.main()
