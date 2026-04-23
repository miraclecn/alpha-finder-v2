import unittest

from alpha_find_v2.config_loader import CONFIG_ROOT, load_cost_model, load_mandate, load_portfolio
from alpha_find_v2.portfolio_simulator import (
    PortfolioRebalanceInput,
    PortfolioSecuritySignal,
    PortfolioSimulator,
    TradeConstraintState,
)


class PortfolioSimulatorTest(unittest.TestCase):
    def test_simulator_applies_turnover_costs_and_trade_blocks(self) -> None:
        mandate = load_mandate(CONFIG_ROOT / "mandates" / "a_share_long_only_eod.toml")
        portfolio = load_portfolio(CONFIG_ROOT / "portfolio" / "a_share_core.toml")
        cost_model = load_cost_model(CONFIG_ROOT / "cost_models" / "base_a_share_cash.toml")
        simulator = PortfolioSimulator(
            mandate=mandate,
            portfolio=portfolio,
            default_cost_model=cost_model,
        )

        result = simulator.run(
            [
                PortfolioRebalanceInput(
                    trade_date="2026-04-06",
                    signals=[
                        PortfolioSecuritySignal(
                            asset_id="AAA",
                            target_weight=0.50,
                            realized_return=0.0200,
                        ),
                        PortfolioSecuritySignal(
                            asset_id="BBB",
                            target_weight=0.48,
                            realized_return=0.0100,
                        ),
                    ],
                ),
                PortfolioRebalanceInput(
                    trade_date="2026-04-13",
                    signals=[
                        PortfolioSecuritySignal(
                            asset_id="AAA",
                            target_weight=0.00,
                            realized_return=-0.0100,
                            trade_state=TradeConstraintState(can_exit=False),
                        ),
                        PortfolioSecuritySignal(
                            asset_id="CCC",
                            target_weight=0.70,
                            realized_return=0.0300,
                        ),
                        PortfolioSecuritySignal(
                            asset_id="DDD",
                            target_weight=0.28,
                            realized_return=0.0150,
                        ),
                    ],
                ),
            ]
        )

        self.assertEqual(len(result.steps), 2)
        first_step = result.steps[0]
        second_step = result.steps[1]

        self.assertAlmostEqual(first_step.executed_weights["AAA"], 0.50)
        self.assertAlmostEqual(first_step.executed_weights["BBB"], 0.48)
        self.assertAlmostEqual(first_step.net_return, 0.014163)

        self.assertAlmostEqual(second_step.executed_weights["AAA"], 0.50)
        self.assertAlmostEqual(second_step.executed_weights["CCC"], 0.3428571428571428)
        self.assertAlmostEqual(second_step.executed_weights["DDD"], 0.13714285714285715)
        self.assertIn("AAA", second_step.blocked_exits)
        self.assertAlmostEqual(second_step.turnover, 0.48)
        self.assertAlmostEqual(second_step.trading_cost, 0.001152)
        self.assertAlmostEqual(second_step.net_return, 0.00619085714285714)


if __name__ == "__main__":
    unittest.main()
