import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from alpha_find_v2.deployment import DecayMonitorEvaluator, ExecutableSignalBuilder
from alpha_find_v2.deployment_loader import (
    load_decay_watch_case,
    load_executable_signal_case,
)
from alpha_find_v2.portfolio_constructor import PortfolioConstructor


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_ROOT = PROJECT_ROOT / "research" / "examples" / "deployment_minimal"


class DeploymentLayerTest(unittest.TestCase):
    def test_build_run_manifest_case_persists_execution_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "run_manifest.json"
            case_text = (
                (EXAMPLE_ROOT / "run_manifest_case.toml").read_text(encoding="utf-8").replace(
                    "research/examples/deployment_minimal/run_manifest_2026_04_20.json",
                    str(manifest_path),
                )
            )
            case_path = Path(temp_dir) / "run_manifest_case.toml"
            case_path.write_text(case_text, encoding="utf-8")

            result = subprocess.run(
                [
                    "python3",
                    "-m",
                    "alpha_find_v2",
                    "build-run-manifest",
                    "--case",
                    str(case_path),
                ],
                cwd=PROJECT_ROOT,
                env={"PYTHONPATH": "src"},
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["case_id"], "candidate_portfolio_run_manifest_2026_04_20")
            self.assertEqual(payload["manifest"]["artifact_type"], "run_manifest")
            self.assertEqual(payload["manifest"]["trade_date"], "2026-04-20")
            self.assertEqual(payload["manifest"]["execution_date"], "2026-04-21")
            self.assertEqual(payload["manifest"]["portfolio_id"], "research_example_candidate_portfolio")
            self.assertEqual(payload["manifest"]["operator_id"], "nan")
            self.assertEqual(
                payload["manifest"]["sleeve_artifact_paths"],
                [
                    "research/examples/promotion_replay_minimal/sleeve_artifacts/fundamental_rerating_core.json",
                    "research/examples/promotion_replay_minimal/sleeve_artifacts/trend_leadership_core.json",
                ],
            )
            written_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(written_manifest["artifact_type"], "run_manifest")
            self.assertEqual(
                written_manifest["package_id"],
                "research_example_candidate_portfolio:2026-04-20",
            )

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
            portfolio_overlay=loaded_case.regime_overlay,
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

    def test_executable_signal_case_carries_regime_overlay_decision(self) -> None:
        loaded_case = load_executable_signal_case(
            EXAMPLE_ROOT / "executable_signal_case_with_overlay.toml"
        )

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
            portfolio_overlay=loaded_case.regime_overlay,
        ).build(
            trade_date=loaded_case.definition.trade_date,
            execution_date=loaded_case.definition.execution_date,
            signals=construction_step.signals,
            portfolio_state=loaded_case.portfolio_state,
            regime_overlay_decision=loaded_case.regime_overlay_decision,
        )

        self.assertIsNotNone(loaded_case.regime_overlay_decision)
        self.assertEqual(package.regime_overlay_status, "active")
        self.assertEqual(package.regime_overlay_state, "cash_heavier")
        self.assertEqual(package.regime_overlay_missing_inputs, [])
        self.assertAlmostEqual(package.base_investable_budget, 0.98)
        self.assertAlmostEqual(package.investable_budget, 0.343)
        self.assertAlmostEqual(package.target_cash_weight, 0.657)
        self.assertLess(package.cash_weight, package.target_cash_weight)

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

    def test_decay_watch_case_can_bind_realized_execution_trace(self) -> None:
        loaded_case = load_decay_watch_case(
            EXAMPLE_ROOT / "decay_watch_case_with_realized_window.toml"
        )

        self.assertEqual(loaded_case.realized_summary.periods, 20)
        self.assertEqual(
            getattr(getattr(loaded_case, "run_manifest", None), "run_id", ""),
            "candidate_portfolio_next_open_2026_04_20",
        )
        self.assertEqual(
            getattr(getattr(loaded_case, "manual_execution_outcome", None), "cash_drift_weight", -1.0),
            0.02,
        )
        self.assertEqual(
            getattr(
                getattr(loaded_case, "realized_trading_window", None),
                "realized_execution_basis",
                "",
            ),
            "manual_next_open_writeback",
        )

    def test_cli_evaluate_decay_watch_reports_realized_execution_trace(self) -> None:
        result = subprocess.run(
            [
                "python3",
                "-m",
                "alpha_find_v2",
                "evaluate-decay-watch",
                "--case",
                str(EXAMPLE_ROOT / "decay_watch_case_with_realized_window.toml"),
            ],
            cwd=PROJECT_ROOT,
            env={"PYTHONPATH": "src"},
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["record"]["run_id"], "candidate_portfolio_next_open_2026_04_20")
        self.assertEqual(payload["record"]["realized_execution_basis"], "manual_next_open_writeback")
        self.assertEqual(payload["record"]["blocked_trade_count"], 1)
        self.assertEqual(payload["record"]["manual_override_count"], 1)
        self.assertAlmostEqual(payload["record"]["cash_drift_weight"], 0.02)
        self.assertEqual(payload["record"]["exception_count"], 2)

    def test_cli_build_executable_signal_allows_live_candidate_with_passing_multi_year_audit(self) -> None:
        result = subprocess.run(
            [
                "python3",
                "-m",
                "alpha_find_v2",
                "build-executable-signal",
                "--case",
                str(EXAMPLE_ROOT / "executable_signal_case_trend_live_candidate_with_overlay.toml"),
            ],
            cwd=PROJECT_ROOT,
            env={"PYTHONPATH": "src"},
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["case_id"], "trend_live_candidate_next_open_2026_04_20")
        self.assertEqual(
            payload["package"]["portfolio_id"],
            "trend_live_candidate_portfolio_with_overlay",
        )
        self.assertEqual(payload["package"]["regime_overlay_state"], "cash_heavier")

    def test_cli_build_run_manifest_allows_live_candidate_with_passing_multi_year_audit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "trend_live_candidate_run_manifest.json"
            case_text = (
                (EXAMPLE_ROOT / "run_manifest_case_trend_live_candidate_2026_04_20.toml")
                .read_text(encoding="utf-8")
                .replace(
                    "research/examples/deployment_minimal/trend_live_candidate_run_manifest_2026_04_20.json",
                    str(manifest_path),
                )
            )
            case_path = Path(temp_dir) / "run_manifest_case.toml"
            case_path.write_text(case_text, encoding="utf-8")

            result = subprocess.run(
                [
                    "python3",
                    "-m",
                    "alpha_find_v2",
                    "build-run-manifest",
                    "--case",
                    str(case_path),
                ],
                cwd=PROJECT_ROOT,
                env={"PYTHONPATH": "src"},
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["case_id"], "trend_live_candidate_run_manifest_2026_04_20")
            self.assertEqual(payload["output_path"], str(manifest_path))
            written_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(
                written_manifest["portfolio_id"],
                "trend_live_candidate_portfolio_with_overlay",
            )
            self.assertEqual(written_manifest["regime_overlay_state"], "cash_heavier")


if __name__ == "__main__":
    unittest.main()
