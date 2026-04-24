import unittest

from alpha_find_v2.config_loader import CONFIG_ROOT, load_cost_model, load_target
from alpha_find_v2.target_builder import (
    ExecutableResidualTargetBuilder,
    TargetObservation,
    TradeLegState,
)


class ExecutableResidualTargetBuilderTest(unittest.TestCase):
    def test_definition_matches_target_clock_and_cost_model(self) -> None:
        target = load_target(CONFIG_ROOT / "targets" / "open_t1_to_open_t20_residual_net_cost.toml")
        cost_model = load_cost_model(CONFIG_ROOT / "cost_models" / "base_a_share_cash.toml")

        definition = ExecutableResidualTargetBuilder(target, cost_model).definition()

        self.assertEqual(definition.signal_observation, "trade_date_close")
        self.assertEqual(definition.entry_offset_days, 1)
        self.assertEqual(definition.exit_offset_days, 20)
        self.assertAlmostEqual(definition.round_trip_cost_bps, cost_model.round_trip_bps())

    def test_evaluate_computes_net_residual_return_after_costs(self) -> None:
        target = load_target(CONFIG_ROOT / "targets" / "open_t1_to_open_t20_residual_net_cost.toml")
        cost_model = load_cost_model(CONFIG_ROOT / "cost_models" / "base_a_share_cash.toml")
        builder = ExecutableResidualTargetBuilder(target, cost_model)

        evaluation = builder.evaluate(
            TargetObservation(
                entry_open=10.0,
                exit_open=10.5,
                residual_components={
                    "benchmark": 0.0100,
                    "industry": 0.0050,
                    "size": -0.0020,
                    "beta": 0.0030,
                },
            )
        )

        self.assertTrue(evaluation.eligible)
        self.assertAlmostEqual(evaluation.gross_return, 0.05)
        self.assertAlmostEqual(evaluation.net_return, 0.0476)
        self.assertAlmostEqual(evaluation.common_return, 0.0160)
        self.assertAlmostEqual(evaluation.residual_return, 0.0316)

    def test_evaluate_rejects_trade_paths_that_are_not_executable(self) -> None:
        target = load_target(CONFIG_ROOT / "targets" / "open_t1_to_open_t05_residual_net_cost.toml")
        cost_model = load_cost_model(CONFIG_ROOT / "cost_models" / "high_a_share_cash.toml")
        builder = ExecutableResidualTargetBuilder(target, cost_model)

        evaluation = builder.evaluate(
            TargetObservation(
                entry_open=12.0,
                exit_open=12.4,
                entry_state=TradeLegState(limit_locked=True),
                residual_components={
                    "benchmark": 0.0,
                    "industry": 0.0,
                    "size": 0.0,
                    "beta": 0.0,
                },
            )
        )

        self.assertFalse(evaluation.eligible)
        self.assertIn("entry_limit_locked", evaluation.excluded_reasons)

    def test_evaluate_supports_honest_non_residual_net_target(self) -> None:
        target = load_target(CONFIG_ROOT / "targets" / "open_t1_to_open_t20_net_cost.toml")
        cost_model = load_cost_model(CONFIG_ROOT / "cost_models" / "base_a_share_cash.toml")

        definition = ExecutableResidualTargetBuilder(target, cost_model).definition()
        evaluation = ExecutableResidualTargetBuilder(target, cost_model).evaluate(
            TargetObservation(
                entry_open=10.0,
                exit_open=10.224,
            )
        )

        self.assertEqual(definition.label_name, "open_t1_to_open_t20_net_cost__net_return")
        self.assertEqual(definition.required_residual_components, [])
        self.assertTrue(evaluation.eligible)
        self.assertAlmostEqual(evaluation.gross_return, 0.0224)
        self.assertAlmostEqual(evaluation.net_return, 0.0200)
        self.assertAlmostEqual(evaluation.common_return, 0.0)
        self.assertIsNone(evaluation.residual_return)
        self.assertAlmostEqual(evaluation.realized_return, 0.0200)


if __name__ == "__main__":
    unittest.main()
