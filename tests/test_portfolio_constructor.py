import unittest

from alpha_find_v2.models import Mandate, PortfolioConstructionModel, PortfolioRecipe
from alpha_find_v2.portfolio_constructor import (
    PortfolioConstructionInput,
    PortfolioConstructor,
    SleeveConstructionInput,
)
from alpha_find_v2.portfolio_simulator import PortfolioSecuritySignal


class PortfolioConstructorTest(unittest.TestCase):
    def test_constructor_combines_overlap_and_holds_cash_after_caps(self) -> None:
        mandate = Mandate(
            id="test_mandate",
            name="Test Mandate",
            market="CN-A",
            benchmark="CSI 800",
            account_type="cash_equity",
            description="Test mandate for constructor behavior.",
            max_single_name_weight=0.35,
            risk={"max_industry_overweight": 0.10},
        )
        portfolio = PortfolioRecipe(
            id="test_portfolio",
            name="Test Portfolio",
            mandate_id="test_mandate",
            benchmark="CSI 800",
            rebalance_policy="weekly",
            description="Test multi-sleeve blend.",
            construction_model_id="test_blend",
            sleeves=["slow", "event"],
            allocation={"slow": 0.60, "event": 0.40},
            constraints={"max_names": 4, "max_single_name_weight": 0.35},
        )
        construction_model = PortfolioConstructionModel(
            id="test_blend",
            name="Test Blend",
            description="Budgeted sum combiner with hard caps and hold-cash overflow.",
            sleeve_weight_source="portfolio_allocation",
            overlap_mode="sum",
            name_selection="top_weight",
            excess_weight_policy="hold_cash",
            industry_budget_mode="benchmark_relative",
        )
        constructor = PortfolioConstructor(
            mandate=mandate,
            portfolio=portfolio,
            construction_model=construction_model,
        )

        result = constructor.build(
            [
                PortfolioConstructionInput(
                    trade_date="2026-04-06",
                    benchmark_industry_weights={
                        "bank": 0.25,
                        "tech": 0.20,
                        "industrial": 0.10,
                    },
                    sleeves=[
                        SleeveConstructionInput(
                            sleeve_id="slow",
                            signals=[
                                PortfolioSecuritySignal(
                                    asset_id="AAA",
                                    target_weight=0.50,
                                    realized_return=0.0100,
                                    industry="bank",
                                ),
                                PortfolioSecuritySignal(
                                    asset_id="BBB",
                                    target_weight=0.30,
                                    realized_return=0.0200,
                                    industry="tech",
                                ),
                                PortfolioSecuritySignal(
                                    asset_id="DDD",
                                    target_weight=0.20,
                                    realized_return=0.0150,
                                    industry="industrial",
                                ),
                            ],
                        ),
                        SleeveConstructionInput(
                            sleeve_id="event",
                            signals=[
                                PortfolioSecuritySignal(
                                    asset_id="AAA",
                                    target_weight=0.50,
                                    realized_return=0.0100,
                                    industry="bank",
                                ),
                                PortfolioSecuritySignal(
                                    asset_id="CCC",
                                    target_weight=0.30,
                                    realized_return=0.0300,
                                    industry="tech",
                                ),
                                PortfolioSecuritySignal(
                                    asset_id="EEE",
                                    target_weight=0.20,
                                    realized_return=0.0250,
                                    industry="industrial",
                                ),
                            ],
                        ),
                    ],
                )
            ]
        )

        self.assertEqual(len(result.steps), 1)
        step = result.steps[0]

        self.assertEqual(step.overlap_names, ["AAA"])
        self.assertEqual(step.dropped_names, ["EEE"])
        self.assertAlmostEqual(step.combined_weights["AAA"], 0.35)
        self.assertAlmostEqual(step.combined_weights["BBB"], 0.18)
        self.assertAlmostEqual(step.combined_weights["CCC"], 0.12)
        self.assertAlmostEqual(step.combined_weights["DDD"], 0.12)
        self.assertAlmostEqual(step.cash_weight, 0.23)

    def test_constructor_rejects_missing_industry_under_benchmark_relative_caps(self) -> None:
        mandate = Mandate(
            id="test_mandate",
            name="Test Mandate",
            market="CN-A",
            benchmark="CSI 800",
            account_type="cash_equity",
            description="Test mandate for constructor behavior.",
            max_single_name_weight=0.35,
            risk={"max_industry_overweight": 0.10},
        )
        portfolio = PortfolioRecipe(
            id="test_portfolio",
            name="Test Portfolio",
            mandate_id="test_mandate",
            benchmark="CSI 800",
            rebalance_policy="weekly",
            description="Test multi-sleeve blend.",
            construction_model_id="test_blend",
            sleeves=["slow"],
            allocation={"slow": 1.0},
            constraints={"max_names": 4, "max_single_name_weight": 0.35},
        )
        construction_model = PortfolioConstructionModel(
            id="test_blend",
            name="Test Blend",
            description="Budgeted sum combiner with hard caps and hold-cash overflow.",
            sleeve_weight_source="portfolio_allocation",
            overlap_mode="sum",
            name_selection="top_weight",
            excess_weight_policy="hold_cash",
            industry_budget_mode="benchmark_relative",
        )
        constructor = PortfolioConstructor(
            mandate=mandate,
            portfolio=portfolio,
            construction_model=construction_model,
        )

        with self.assertRaisesRegex(
            ValueError,
            "Industry labels are required for benchmark-relative industry caps: AAA",
        ):
            constructor.build(
                [
                    PortfolioConstructionInput(
                        trade_date="2026-04-06",
                        benchmark_industry_weights={"bank": 0.25},
                        sleeves=[
                            SleeveConstructionInput(
                                sleeve_id="slow",
                                signals=[
                                    PortfolioSecuritySignal(
                                        asset_id="AAA",
                                        target_weight=1.0,
                                        realized_return=0.0100,
                                        industry="",
                                    )
                                ],
                            )
                        ],
                    )
                ]
            )


if __name__ == "__main__":
    unittest.main()
