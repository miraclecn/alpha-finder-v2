import unittest

from alpha_find_v2.config_loader import CONFIG_ROOT, load_cost_model, load_risk_model, load_target
from alpha_find_v2.risk_model import (
    AssetRiskObservation,
    ConfiguredRiskModelResidualizer,
    RiskModelSnapshot,
)
from alpha_find_v2.target_builder import ExecutableResidualTargetBuilder, TargetObservation


class TargetRiskIntegrationTest(unittest.TestCase):
    def test_target_builder_accepts_risk_model_decomposition(self) -> None:
        target = load_target(CONFIG_ROOT / "targets" / "open_t1_to_open_t20_residual_net_cost.toml")
        cost_model = load_cost_model(CONFIG_ROOT / "cost_models" / "base_a_share_cash.toml")
        risk_model = load_risk_model(CONFIG_ROOT / "risk_models" / "a_share_core_equity.toml")

        decomposition = ConfiguredRiskModelResidualizer(risk_model).decompose(
            observation=AssetRiskObservation(
                asset_id="000001.SZ",
                forward_return=0.0500,
                exposures={
                    "benchmark": 1.0,
                    "industry:bank": 1.0,
                    "size": -0.5,
                    "beta": 1.2,
                },
            ),
            snapshot=RiskModelSnapshot(
                factor_returns={
                    "benchmark": 0.0100,
                    "industry:bank": 0.0050,
                    "size": 0.0040,
                    "beta": 0.0025,
                }
            ),
        )

        evaluation = ExecutableResidualTargetBuilder(target, cost_model).evaluate(
            TargetObservation(
                entry_open=10.0,
                exit_open=10.5,
                risk_decomposition=decomposition,
            )
        )

        self.assertTrue(evaluation.eligible)
        self.assertAlmostEqual(evaluation.common_return, 0.0160)
        self.assertAlmostEqual(evaluation.residual_return, 0.0316)


if __name__ == "__main__":
    unittest.main()
