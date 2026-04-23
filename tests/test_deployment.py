import unittest
from pathlib import Path

from alpha_find_v2.deployment import DecayMonitorEvaluator, ExecutableSignalBuilder
from alpha_find_v2.deployment_loader import (
    load_decay_watch_case,
    load_executable_signal_case,
)
from alpha_find_v2.portfolio_constructor import PortfolioConstructor


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_ROOT = PROJECT_ROOT / "research" / "examples" / "deployment_minimal"


class DeploymentLayerTest(unittest.TestCase):
    def test_executable_signal_case_builds_next_open_package(self) -> None:
        loaded_case = load_executable_signal_case(EXAMPLE_ROOT / "executable_signal_case.toml")

        construction_step = PortfolioConstructor(
            mandate=loaded_case.mandate,
            portfolio=loaded_case.portfolio,
            construction_model=loaded_case.construction_model,
        ).build([loaded_case.construction_input]).steps[0]
        package = ExecutableSignalBuilder(
            mandate=loaded_case.mandate,
            portfolio=loaded_case.portfolio,
            execution_policy=loaded_case.execution_policy,
            default_cost_model=loaded_case.default_cost_model,
            cost_models=loaded_case.cost_models,
        ).build(
            trade_date=loaded_case.definition.trade_date,
            execution_date=loaded_case.definition.execution_date,
            signals=construction_step.signals,
            portfolio_state=loaded_case.portfolio_state,
        )

        instructions_by_asset = {
            instruction.asset_id: instruction for instruction in package.instructions
        }

        self.assertIsNotNone(loaded_case.account_state_snapshot)
        self.assertEqual(package.execution_policy_id, "a_share_next_open_v1")
        self.assertEqual(package.trade_date, "2026-04-20")
        self.assertEqual(package.execution_date, "2026-04-21")
        self.assertAlmostEqual(package.investable_budget, 0.98)
        self.assertGreater(package.estimated_turnover, 0.0)
        self.assertEqual(instructions_by_asset["AAA"].action, "sell")
        self.assertEqual(instructions_by_asset["DDD"].action, "hold_exit_blocked")
        self.assertEqual(instructions_by_asset["EEE"].action, "buy")
        self.assertIn("CCC", instructions_by_asset)
        self.assertLess(
            instructions_by_asset["EEE"].executable_target_weight,
            instructions_by_asset["EEE"].proposed_target_weight,
        )

    def test_decay_watch_case_evaluates_to_healthy_record(self) -> None:
        loaded_case = load_decay_watch_case(EXAMPLE_ROOT / "decay_watch_case.toml")

        record = DecayMonitorEvaluator(loaded_case.decay_monitor).evaluate(
            portfolio=loaded_case.portfolio,
            evaluation_date=loaded_case.definition.evaluation_date,
            window_label=loaded_case.definition.window_label,
            promotion_snapshot=loaded_case.promotion_snapshot,
            realized_summary=loaded_case.realized_summary,
        )

        self.assertEqual(record.decay_monitor_id, "a_share_core_watch")
        self.assertEqual(record.status, "healthy")
        self.assertGreater(record.ir_ratio, 0.60)
        self.assertLess(record.drawdown_multiple, 1.50)
        self.assertFalse(record.warning_breaches)
        self.assertFalse(record.retirement_breaches)

    def test_decay_monitor_flags_retirement_when_live_path_breaks(self) -> None:
        loaded_case = load_decay_watch_case(EXAMPLE_ROOT / "decay_watch_case.toml")
        broken_summary = loaded_case.realized_summary.__class__(
            periods=20,
            average_return=-0.0010,
            volatility=0.0100,
            ir=0.10,
            tstat=0.20,
            peak_to_trough_drawdown=0.18,
            breadth=2,
            average_turnover=1.40,
            blocked_name_share=0.25,
        )

        record = DecayMonitorEvaluator(loaded_case.decay_monitor).evaluate(
            portfolio=loaded_case.portfolio,
            evaluation_date=loaded_case.definition.evaluation_date,
            window_label="20d_break",
            promotion_snapshot=loaded_case.promotion_snapshot,
            realized_summary=broken_summary,
        )

        self.assertEqual(record.status, "retire")
        self.assertIn("max_drawdown_multiple", record.retirement_breaches)
        self.assertIn("max_turnover_vs_budget", record.retirement_breaches)
        self.assertIn("max_blocked_name_share", record.retirement_breaches)


if __name__ == "__main__":
    unittest.main()
