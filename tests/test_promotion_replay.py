import unittest

from alpha_find_v2.models import (
    CostModel,
    Mandate,
    PortfolioConstructionModel,
    PortfolioRecipe,
    PromotionGate,
)
from alpha_find_v2.portfolio_promotion_replay import (
    PortfolioPromotionReplay,
    PortfolioPromotionReplayInput,
    SleeveResearchArtifact,
    SleeveResearchStep,
    SleeveSignalRecord,
)


class PortfolioPromotionReplayTest(unittest.TestCase):
    def setUp(self) -> None:
        self.mandate = Mandate(
            id="test_mandate",
            name="Test Mandate",
            market="CN-A",
            benchmark="CSI 800",
            account_type="cash_equity",
            description="Promotion replay mandate.",
            max_single_name_weight=0.60,
            max_turnover_per_rebalance=1.50,
            risk={"max_industry_overweight": 0.30},
        )
        self.construction_model = PortfolioConstructionModel(
            id="test_construction",
            name="Test Construction",
            description="Budgeted sum combiner for promotion replay tests.",
            sleeve_weight_source="portfolio_allocation",
            overlap_mode="sum",
            name_selection="top_weight",
            excess_weight_policy="hold_cash",
            industry_budget_mode="benchmark_relative",
        )
        self.cost_model = CostModel(
            id="base",
            name="Base",
            description="Zero-cost test model.",
            buy_commission_bps=0.0,
            sell_commission_bps=0.0,
            buy_slippage_bps=0.0,
            sell_slippage_bps=0.0,
        )
        self.gate = PromotionGate(
            id="test_gate",
            name="Test Gate",
            scope="portfolio",
            description="Candidate sleeve must improve the portfolio path.",
            minimum_oos_ir=0.0,
            minimum_oos_tstat=0.0,
            minimum_breadth=5,
            max_peak_to_trough_drawdown=0.10,
            cost_scenarios=["base"],
            regime_requirements=["bull"],
            correlation_limits={
                "max_component_correlation": 0.80,
                "max_to_existing_portfolio": 0.70,
            },
            turnover_limits={
                "max_realized_vs_budget": 1.10,
                "max_names_with_limit_lock_share": 0.20,
            },
            portfolio_contribution={
                "minimum_marginal_ir_delta": 0.10,
                "max_marginal_drawdown_increase": 0.02,
            },
        )
        self.replay = PortfolioPromotionReplay(
            mandate=self.mandate,
            construction_model=self.construction_model,
            default_cost_model=self.cost_model,
            gate=self.gate,
        )

    def test_replay_requires_artifacts_on_the_full_decision_calendar(self) -> None:
        baseline_portfolio = self._baseline_portfolio()
        candidate_portfolio = self._candidate_portfolio()

        sparse_slow = SleeveResearchArtifact(
            sleeve_id="slow",
            mandate_id="test_mandate",
            target_id="target_20d",
            steps=[
                SleeveResearchStep(
                    trade_date="2026-04-06",
                    records=[
                        self._record("AAA", rank=1, target_weight=0.60, realized_return=0.01, industry="bank"),
                        self._record("BBB", rank=2, target_weight=0.40, realized_return=0.02, industry="tech"),
                    ],
                ),
                SleeveResearchStep(
                    trade_date="2026-04-20",
                    records=[
                        self._record("BBB", rank=1, target_weight=0.60, realized_return=-0.01, industry="tech"),
                        self._record("CCC", rank=2, target_weight=0.40, realized_return=0.015, industry="tech"),
                    ],
                ),
            ],
        )
        event = self._event_artifact()

        replay_input = PortfolioPromotionReplayInput(
            baseline_portfolio=baseline_portfolio,
            candidate_portfolio=candidate_portfolio,
            artifacts=[sparse_slow, event],
            periods_per_year=52,
            benchmark_industry_weights_by_date=self._benchmark_industry_weights(),
            cost_scenario_pass={"base": True},
            regime_pass={"bull": True},
            max_component_correlation=0.35,
            correlation_to_existing_portfolio=0.25,
        )

        with self.assertRaisesRegex(ValueError, "must cover trade date 2026-04-13"):
            self.replay.replay(replay_input)

    def test_replay_builds_portfolio_paths_and_auto_populates_marginal_metrics(self) -> None:
        replay_input = PortfolioPromotionReplayInput(
            baseline_portfolio=self._baseline_portfolio(),
            candidate_portfolio=self._candidate_portfolio(),
            artifacts=[self._slow_artifact(), self._event_artifact()],
            periods_per_year=52,
            benchmark_industry_weights_by_date=self._benchmark_industry_weights(),
            cost_scenario_pass={"base": True},
            regime_pass={"bull": True},
            max_component_correlation=0.35,
            correlation_to_existing_portfolio=0.25,
        )

        result = self.replay.replay(replay_input)

        self.assertEqual(len(result.baseline_construction.steps), 3)
        self.assertEqual(len(result.candidate_construction.steps), 3)
        self.assertEqual(result.candidate_construction.steps[0].overlap_names, ["BBB"])

        self.assertGreater(
            result.candidate_summary.average_return,
            result.baseline_summary.average_return,
        )
        self.assertGreater(result.marginal.average_return_delta, 0.0)
        self.assertGreater(result.marginal.marginal_ir_delta, 0.0)
        self.assertLessEqual(result.marginal.marginal_drawdown_increase, 0.0)

        self.assertAlmostEqual(
            result.snapshot.marginal_ir_delta,
            result.marginal.marginal_ir_delta,
        )
        self.assertAlmostEqual(
            result.snapshot.marginal_drawdown_increase,
            result.marginal.marginal_drawdown_increase,
        )
        self.assertAlmostEqual(
            result.snapshot.realized_turnover_vs_budget,
            result.candidate_summary.average_turnover / self.mandate.max_turnover_per_rebalance,
        )
        self.assertTrue(result.decision.passed)
        self.assertIn("minimum_marginal_ir_delta", result.decision.passed_checks)
        self.assertIn("cost_scenario:base", result.decision.passed_checks)
        self.assertIn("regime:bull", result.decision.passed_checks)

    def _baseline_portfolio(self) -> PortfolioRecipe:
        return PortfolioRecipe(
            id="baseline_portfolio",
            name="Baseline Portfolio",
            mandate_id="test_mandate",
            benchmark="CSI 800",
            rebalance_policy="weekly",
            description="Existing live sleeve only.",
            construction_model_id="test_construction",
            promotion_gate_id="test_gate",
            sleeves=["slow"],
            allocation={"slow": 1.0},
            constraints={
                "max_names": 4,
                "max_single_name_weight": 0.60,
                "max_industry_overweight": 0.30,
            },
        )

    def _candidate_portfolio(self) -> PortfolioRecipe:
        return PortfolioRecipe(
            id="candidate_portfolio",
            name="Candidate Portfolio",
            mandate_id="test_mandate",
            benchmark="CSI 800",
            rebalance_policy="weekly",
            description="Baseline portfolio plus candidate sleeve.",
            construction_model_id="test_construction",
            promotion_gate_id="test_gate",
            sleeves=["slow", "event"],
            allocation={"slow": 0.60, "event": 0.40},
            constraints={
                "max_names": 5,
                "max_single_name_weight": 0.60,
                "max_industry_overweight": 0.30,
            },
        )

    def _slow_artifact(self) -> SleeveResearchArtifact:
        return SleeveResearchArtifact(
            sleeve_id="slow",
            mandate_id="test_mandate",
            target_id="target_20d",
            steps=[
                SleeveResearchStep(
                    trade_date="2026-04-06",
                    records=[
                        self._record("AAA", rank=1, target_weight=0.60, realized_return=0.01, industry="bank"),
                        self._record("BBB", rank=2, target_weight=0.40, realized_return=0.02, industry="tech"),
                    ],
                ),
                SleeveResearchStep(
                    trade_date="2026-04-13",
                    records=[
                        self._record("AAA", rank=1, target_weight=0.50, realized_return=0.015, industry="bank"),
                        self._record("CCC", rank=2, target_weight=0.50, realized_return=0.01, industry="tech"),
                    ],
                ),
                SleeveResearchStep(
                    trade_date="2026-04-20",
                    records=[
                        self._record("BBB", rank=1, target_weight=0.60, realized_return=-0.01, industry="tech"),
                        self._record("CCC", rank=2, target_weight=0.40, realized_return=0.015, industry="tech"),
                    ],
                ),
            ],
        )

    def _event_artifact(self) -> SleeveResearchArtifact:
        return SleeveResearchArtifact(
            sleeve_id="event",
            mandate_id="test_mandate",
            target_id="target_05d",
            steps=[
                SleeveResearchStep(
                    trade_date="2026-04-06",
                    records=[
                        self._record("BBB", rank=1, target_weight=0.50, realized_return=0.02, industry="tech"),
                        self._record("DDD", rank=2, target_weight=0.50, realized_return=0.03, industry="industrial"),
                    ],
                ),
                SleeveResearchStep(
                    trade_date="2026-04-13",
                    records=[
                        self._record("AAA", rank=1, target_weight=0.40, realized_return=0.015, industry="bank"),
                        self._record("DDD", rank=2, target_weight=0.60, realized_return=0.02, industry="industrial"),
                    ],
                ),
                SleeveResearchStep(
                    trade_date="2026-04-20",
                    records=[
                        self._record("CCC", rank=1, target_weight=0.50, realized_return=0.015, industry="tech"),
                        self._record("EEE", rank=2, target_weight=0.50, realized_return=0.025, industry="industrial"),
                    ],
                ),
            ],
        )

    def _benchmark_industry_weights(self) -> dict[str, dict[str, float]]:
        return {
            "2026-04-06": {"bank": 0.35, "tech": 0.35, "industrial": 0.30},
            "2026-04-13": {"bank": 0.35, "tech": 0.35, "industrial": 0.30},
            "2026-04-20": {"bank": 0.35, "tech": 0.35, "industrial": 0.30},
        }

    def _record(
        self,
        asset_id: str,
        *,
        rank: int,
        target_weight: float,
        realized_return: float,
        industry: str,
    ) -> SleeveSignalRecord:
        return SleeveSignalRecord(
            asset_id=asset_id,
            rank=rank,
            score=float(10 - rank),
            target_weight=target_weight,
            realized_return=realized_return,
            cost_model_id="base",
            industry=industry,
        )


if __name__ == "__main__":
    unittest.main()
