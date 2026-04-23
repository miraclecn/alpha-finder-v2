import unittest

from alpha_find_v2.config_loader import CONFIG_ROOT, load_risk_model
from alpha_find_v2.risk_model import (
    AssetRiskObservation,
    ConfiguredRiskModelResidualizer,
    RiskModelSnapshot,
)


class ConfiguredRiskModelResidualizerTest(unittest.TestCase):
    def test_decompose_groups_dynamic_industry_and_style_components(self) -> None:
        risk_model = load_risk_model(CONFIG_ROOT / "risk_models" / "a_share_core_equity.toml")
        residualizer = ConfiguredRiskModelResidualizer(risk_model)

        decomposition = residualizer.decompose(
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

        self.assertAlmostEqual(decomposition.components["benchmark"], 0.0100)
        self.assertAlmostEqual(decomposition.components["industry"], 0.0050)
        self.assertAlmostEqual(decomposition.components["size"], -0.0020)
        self.assertAlmostEqual(decomposition.components["beta"], 0.0030)
        self.assertAlmostEqual(decomposition.common_return, 0.0160)
        self.assertAlmostEqual(decomposition.residual_return, 0.0340)

    def test_decompose_rejects_missing_factor_return_for_used_exposure(self) -> None:
        risk_model = load_risk_model(CONFIG_ROOT / "risk_models" / "a_share_core_equity.toml")
        residualizer = ConfiguredRiskModelResidualizer(risk_model)

        with self.assertRaisesRegex(ValueError, "Missing factor returns"):
            residualizer.decompose(
                observation=AssetRiskObservation(
                    asset_id="000001.SZ",
                    forward_return=0.0500,
                    exposures={
                        "benchmark": 1.0,
                        "industry:bank": 1.0,
                    },
                ),
                snapshot=RiskModelSnapshot(factor_returns={"benchmark": 0.0100}),
            )


if __name__ == "__main__":
    unittest.main()
