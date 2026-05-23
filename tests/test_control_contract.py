from __future__ import annotations

import unittest

from alpha_find_v2.config_loader import (
    CONFIG_ROOT,
    load_execution_policy,
    load_mandate,
    load_portfolio,
    load_portfolio_construction_model,
    load_sleeve,
)
from alpha_find_v2.control_contract import (
    ControlContractError,
    assert_no_false_enforced_controls,
    build_control_contract_report,
)
from alpha_find_v2.models import Mandate, PortfolioConstructionModel, PortfolioRecipe


class ControlContractTest(unittest.TestCase):
    def test_production_config_reports_only_implemented_controls_as_enforced(self) -> None:
        mandate = load_mandate(CONFIG_ROOT / "mandates" / "a_share_long_only_eod.toml")
        portfolio = load_portfolio(CONFIG_ROOT / "portfolio" / "a_share_core.toml")
        construction_model = load_portfolio_construction_model(
            CONFIG_ROOT / "portfolio_construction" / f"{portfolio.construction_model_id}.toml"
        )
        execution_policy = load_execution_policy(
            CONFIG_ROOT / "execution_policies" / f"{portfolio.execution_policy_id}.toml"
        )
        sleeves = [
            load_sleeve(CONFIG_ROOT / "sleeves" / f"{sleeve_id}.toml")
            for sleeve_id in portfolio.sleeves
        ]

        report = build_control_contract_report(
            mandate=mandate,
            portfolio=portfolio,
            construction_model=construction_model,
            execution_policy=execution_policy,
            sleeves=sleeves,
        )

        self.assertEqual(report.status_for("industry"), "enforced")
        self.assertEqual(report.status_for("size"), "planned")
        self.assertEqual(report.status_for("beta"), "planned")
        self.assertEqual(report.status_for("turnover_penalty"), "report_only")
        self.assertEqual(report.status_for("min_trade_weight"), "planned")
        assert_no_false_enforced_controls(report)

    def test_false_enforced_size_or_beta_control_is_rejected(self) -> None:
        mandate = Mandate(
            id="test",
            name="Test",
            market="CN-A",
            benchmark="CSI 800",
            account_type="cash_equity",
            description="Synthetic mandate.",
            risk={
                "industry_neutral": {"status": "enforced"},
                "size_control": {"status": "enforced"},
                "beta_band": {"status": "enforced", "band": [0.85, 1.15]},
            },
        )
        portfolio = PortfolioRecipe(
            id="test_portfolio",
            name="Test Portfolio",
            mandate_id="test",
            benchmark="CSI 800",
            rebalance_policy="weekly",
            description="Synthetic portfolio.",
            construction_model_id="test_model",
            execution_policy_id="test_policy",
            sleeves=[],
            constraints={"turnover_penalty": "enabled"},
        )
        construction_model = PortfolioConstructionModel(
            id="test_model",
            name="Test Model",
            description="Synthetic construction model.",
            sleeve_weight_source="portfolio_allocation",
            overlap_mode="sum",
            name_selection="top_weight",
            excess_weight_policy="hold_cash",
            industry_budget_mode="benchmark_relative",
        )
        execution_policy = load_execution_policy(
            CONFIG_ROOT / "execution_policies" / "a_share_next_open_v1.toml"
        )

        report = build_control_contract_report(
            mandate=mandate,
            portfolio=portfolio,
            construction_model=construction_model,
            execution_policy=execution_policy,
            sleeves=[],
        )

        with self.assertRaisesRegex(ControlContractError, "size.*beta"):
            assert_no_false_enforced_controls(report)


if __name__ == "__main__":
    unittest.main()
