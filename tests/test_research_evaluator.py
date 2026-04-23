import unittest

from alpha_find_v2.portfolio_simulator import PortfolioSimulationResult, PortfolioStepResult
from alpha_find_v2.research_evaluator import PortfolioResearchEvaluator


class PortfolioResearchEvaluatorTest(unittest.TestCase):
    def test_summary_and_promotion_snapshot_use_executable_path_metrics(self) -> None:
        result = PortfolioSimulationResult(
            steps=[
                PortfolioStepResult(
                    trade_date="2026-04-06",
                    target_weights={"AAA": 0.50, "BBB": 0.48},
                    executed_weights={"AAA": 0.50, "BBB": 0.48},
                    turnover=0.98,
                    net_return=0.014163,
                ),
                PortfolioStepResult(
                    trade_date="2026-04-13",
                    target_weights={"AAA": 0.0, "CCC": 0.70, "DDD": 0.28},
                    executed_weights={
                        "AAA": 0.50,
                        "CCC": 0.3428571428571428,
                        "DDD": 0.13714285714285715,
                    },
                    turnover=0.48,
                    net_return=0.00619085714285714,
                    blocked_exits=["AAA"],
                ),
                PortfolioStepResult(
                    trade_date="2026-04-20",
                    target_weights={"CCC": 0.49, "DDD": 0.49},
                    executed_weights={"CCC": 0.49, "DDD": 0.49},
                    turnover=0.50,
                    net_return=0.0095,
                ),
            ]
        )
        evaluator = PortfolioResearchEvaluator()

        summary = evaluator.summarize(result, periods_per_year=52)
        snapshot = evaluator.to_promotion_snapshot(
            summary=summary,
            turnover_budget=0.60,
            cost_scenario_pass={"base": True, "high": False},
            regime_pass={"bull": True, "bear": True},
            max_component_correlation=0.42,
            correlation_to_existing_portfolio=0.35,
            marginal_ir_delta=0.07,
            marginal_drawdown_increase=0.01,
        )

        self.assertEqual(summary.periods, 3)
        self.assertEqual(summary.breadth, 4)
        self.assertAlmostEqual(summary.average_turnover, 0.6533333333333333)
        self.assertAlmostEqual(summary.blocked_name_share, 0.1111111111111111)
        self.assertGreater(summary.ir, 0.0)
        self.assertGreater(summary.tstat, 0.0)

        self.assertAlmostEqual(snapshot.oos_ir, summary.ir)
        self.assertAlmostEqual(snapshot.oos_tstat, summary.tstat)
        self.assertAlmostEqual(snapshot.realized_turnover_vs_budget, 1.0888888888888888)
        self.assertAlmostEqual(snapshot.limit_locked_name_share, summary.blocked_name_share)
        self.assertEqual(snapshot.cost_scenario_pass["high"], False)

    def test_marginal_contribution_uses_baseline_and_candidate_paths(self) -> None:
        baseline = PortfolioSimulationResult(
            steps=[
                PortfolioStepResult(
                    trade_date="2026-04-06",
                    executed_weights={"AAA": 0.49, "BBB": 0.49},
                    turnover=0.98,
                    net_return=0.0080,
                ),
                PortfolioStepResult(
                    trade_date="2026-04-13",
                    executed_weights={"AAA": 0.45, "CCC": 0.53},
                    turnover=0.08,
                    net_return=0.0040,
                ),
                PortfolioStepResult(
                    trade_date="2026-04-20",
                    executed_weights={"BBB": 0.48, "CCC": 0.50},
                    turnover=0.10,
                    net_return=-0.0020,
                ),
            ]
        )
        candidate = PortfolioSimulationResult(
            steps=[
                PortfolioStepResult(
                    trade_date="2026-04-06",
                    executed_weights={"AAA": 0.35, "BBB": 0.18, "CCC": 0.12, "DDD": 0.12},
                    turnover=0.77,
                    net_return=0.0100,
                ),
                PortfolioStepResult(
                    trade_date="2026-04-13",
                    executed_weights={"AAA": 0.30, "BBB": 0.17, "CCC": 0.16, "DDD": 0.12},
                    turnover=0.06,
                    net_return=0.0060,
                ),
                PortfolioStepResult(
                    trade_date="2026-04-20",
                    executed_weights={"AAA": 0.28, "BBB": 0.18, "CCC": 0.19, "DDD": 0.10},
                    turnover=0.05,
                    net_return=0.0010,
                ),
            ]
        )
        evaluator = PortfolioResearchEvaluator()

        marginal = evaluator.marginal_contribution(
            baseline_result=baseline,
            candidate_result=candidate,
            periods_per_year=52,
        )

        self.assertGreater(marginal.marginal_ir_delta, 0.0)
        self.assertGreater(marginal.average_return_delta, 0.0)
        self.assertLess(marginal.marginal_drawdown_increase, 0.0)
        self.assertLess(marginal.average_turnover_delta, 0.0)


if __name__ == "__main__":
    unittest.main()
