import unittest

from alpha_find_v2.models import Mandate, PortfolioConstructionModel, PortfolioRecipe
from alpha_find_v2.portfolio_constructor import (
    PortfolioConstructionInput,
    PortfolioConstructor,
    SleeveConstructionInput,
)
from alpha_find_v2.portfolio_simulator import PortfolioSecuritySignal, TradeConstraintState


class PortfolioConstructorTest(unittest.TestCase):
    def _mandate(self) -> Mandate:
        return Mandate(
            id="test_mandate",
            name="Test Mandate",
            market="CN-A",
            benchmark="CSI 800",
            account_type="cash_equity",
            description="Test mandate for constructor behavior.",
            max_single_name_weight=1.0,
            risk={"max_industry_overweight": 1.0},
        )

    def _portfolio(self) -> PortfolioRecipe:
        return PortfolioRecipe(
            id="test_portfolio",
            name="Test Portfolio",
            mandate_id="test_mandate",
            benchmark="CSI 800",
            rebalance_policy="weekly",
            description="Test multi-sleeve blend.",
            construction_model_id="test_blend",
            sleeves=["slow", "event"],
            allocation={"slow": 0.60, "event": 0.40},
            constraints={"max_names": 10, "max_single_name_weight": 1.0},
        )

    def _construction_model(self, overlap_mode: str) -> PortfolioConstructionModel:
        return PortfolioConstructionModel(
            id="test_blend",
            name="Test Blend",
            description="Budgeted combiner.",
            sleeve_weight_source="portfolio_allocation",
            overlap_mode=overlap_mode,
            name_selection="top_weight",
            excess_weight_policy="hold_cash",
            industry_budget_mode="",
        )

    def _overlap_result(self, overlap_mode: str):
        constructor = PortfolioConstructor(
            mandate=self._mandate(),
            portfolio=self._portfolio(),
            construction_model=self._construction_model(overlap_mode),
        )
        return constructor.build(
            [
                PortfolioConstructionInput(
                    trade_date="2026-04-06",
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
                                    target_weight=0.50,
                                    realized_return=0.0200,
                                    industry="tech",
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
                                    target_weight=0.50,
                                    realized_return=0.0300,
                                    industry="tech",
                                ),
                            ],
                        ),
                    ],
                )
            ]
        )

    def test_overlap_mode_sum_preserves_existing_weight_addition(self) -> None:
        step = self._overlap_result("sum").steps[0]

        self.assertEqual(step.overlap_names, ["AAA"])
        self.assertAlmostEqual(step.combined_weights["AAA"], 0.50)
        self.assertAlmostEqual(step.combined_weights["BBB"], 0.30)
        self.assertAlmostEqual(step.combined_weights["CCC"], 0.20)

    def test_overlap_mode_max_uses_largest_sleeve_contribution(self) -> None:
        step = self._overlap_result("max").steps[0]

        self.assertEqual(step.overlap_names, ["AAA"])
        self.assertAlmostEqual(step.combined_weights["AAA"], 0.30)
        self.assertAlmostEqual(step.combined_weights["BBB"], 0.30)
        self.assertAlmostEqual(step.combined_weights["CCC"], 0.20)

    def test_overlap_mode_average_uses_mean_sleeve_contribution(self) -> None:
        step = self._overlap_result("average").steps[0]

        self.assertEqual(step.overlap_names, ["AAA"])
        self.assertAlmostEqual(step.combined_weights["AAA"], 0.25)
        self.assertAlmostEqual(step.combined_weights["BBB"], 0.30)
        self.assertAlmostEqual(step.combined_weights["CCC"], 0.20)

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
        self.assertAlmostEqual(step.selection_cash_weight, 0.08)
        self.assertAlmostEqual(step.single_name_cap_cash_weight, 0.15)
        self.assertAlmostEqual(step.industry_cap_cash_weight, 0.0)

    def test_constructor_attributes_industry_cap_cash(self) -> None:
        mandate = Mandate(
            id="test_mandate",
            name="Test Mandate",
            market="CN-A",
            benchmark="CSI 800",
            account_type="cash_equity",
            description="Test mandate for constructor behavior.",
            max_single_name_weight=1.0,
            risk={"max_industry_overweight": 0.10},
        )
        portfolio = PortfolioRecipe(
            id="test_portfolio",
            name="Test Portfolio",
            mandate_id="test_mandate",
            benchmark="CSI 800",
            rebalance_policy="weekly",
            description="Test industry cap cash attribution.",
            construction_model_id="test_blend",
            sleeves=["slow"],
            allocation={"slow": 1.0},
            constraints={"max_names": 4, "max_single_name_weight": 1.0},
        )
        construction_model = PortfolioConstructionModel(
            id="test_blend",
            name="Test Blend",
            description="Benchmark-relative industry cap.",
            sleeve_weight_source="portfolio_allocation",
            overlap_mode="sum",
            name_selection="top_weight",
            excess_weight_policy="hold_cash",
            industry_budget_mode="benchmark_relative",
        )

        result = PortfolioConstructor(
            mandate=mandate,
            portfolio=portfolio,
            construction_model=construction_model,
        ).build(
            [
                PortfolioConstructionInput(
                    trade_date="2026-04-06",
                    benchmark_industry_weights={"tech": 0.20},
                    sleeves=[
                        SleeveConstructionInput(
                            sleeve_id="slow",
                            signals=[
                                PortfolioSecuritySignal(
                                    asset_id="AAA",
                                    target_weight=0.50,
                                    realized_return=0.0100,
                                    industry="tech",
                                ),
                                PortfolioSecuritySignal(
                                    asset_id="BBB",
                                    target_weight=0.50,
                                    realized_return=0.0200,
                                    industry="tech",
                                ),
                            ],
                        )
                    ],
                )
            ]
        )

        step = result.steps[0]
        self.assertAlmostEqual(step.combined_weights["AAA"], 0.15)
        self.assertAlmostEqual(step.combined_weights["BBB"], 0.15)
        self.assertAlmostEqual(step.industry_cap_cash_weight, 0.70)
        self.assertAlmostEqual(step.cash_weight, 0.70)

    def test_constructor_rejects_unsupported_construction_model_fields(self) -> None:
        for field_name, value in (
            ("sleeve_weight_source", "dynamic"),
            ("name_selection", "score_blend"),
            ("excess_weight_policy", "redistribute"),
            ("industry_budget_mode", "sector_absolute"),
        ):
            model_kwargs = {
                "id": "test_blend",
                "name": "Test Blend",
                "description": "Unsupported construction model.",
                "sleeve_weight_source": "portfolio_allocation",
                "overlap_mode": "sum",
                "name_selection": "top_weight",
                "excess_weight_policy": "hold_cash",
                "industry_budget_mode": "",
            }
            model_kwargs[field_name] = value
            with self.subTest(field_name=field_name):
                with self.assertRaisesRegex(ValueError, field_name):
                    PortfolioConstructor(
                        mandate=self._mandate(),
                        portfolio=self._portfolio(),
                        construction_model=PortfolioConstructionModel(**model_kwargs),
                    )

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

    def test_constructor_merges_overlapping_trade_state_conservatively(self) -> None:
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
                                    trade_state=TradeConstraintState(
                                        can_enter=False,
                                        can_exit=True,
                                    ),
                                ),
                                PortfolioSecuritySignal(
                                    asset_id="BBB",
                                    target_weight=0.50,
                                    realized_return=0.0200,
                                    industry="tech",
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
                                    trade_state=TradeConstraintState(
                                        can_enter=True,
                                        can_exit=False,
                                    ),
                                ),
                                PortfolioSecuritySignal(
                                    asset_id="CCC",
                                    target_weight=0.50,
                                    realized_return=0.0300,
                                    industry="tech",
                                ),
                            ],
                        ),
                    ],
                )
            ]
        )

        step = result.steps[0]
        merged_signal = next(signal for signal in step.signals if signal.asset_id == "AAA")

        self.assertEqual(step.overlap_names, ["AAA"])
        self.assertFalse(merged_signal.trade_state.can_enter)
        self.assertFalse(merged_signal.trade_state.can_exit)


if __name__ == "__main__":
    unittest.main()
