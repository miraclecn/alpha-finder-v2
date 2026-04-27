import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from alpha_find_v2.config_loader import CONFIG_ROOT
from alpha_find_v2.deployment_loader import load_portfolio_state_snapshot
from alpha_find_v2.live_state import (
    account_state_to_portfolio_state,
    load_account_state_snapshot,
    load_benchmark_state_artifact,
)
from alpha_find_v2.portfolio_promotion_replay import PortfolioPromotionReplay
from alpha_find_v2.research_artifact_loader import (
    load_portfolio_promotion_replay_case,
    load_sleeve_artifact,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_ROOT = PROJECT_ROOT / "research" / "examples" / "promotion_replay_minimal"
DEPLOYMENT_EXAMPLE_ROOT = PROJECT_ROOT / "research" / "examples" / "deployment_minimal"


class ResearchArtifactLoaderTest(unittest.TestCase):
    def test_load_sleeve_artifact_from_json(self) -> None:
        artifact = load_sleeve_artifact(
            EXAMPLE_ROOT / "sleeve_artifacts" / "fundamental_rerating_core.json"
        )

        self.assertEqual(artifact.sleeve_id, "fundamental_rerating_core")
        self.assertEqual(
            artifact.trade_dates(),
            ["2026-04-06", "2026-04-13", "2026-04-20"],
        )
        self.assertEqual(artifact.steps[0].records[0].cost_model_id, "base_a_share_cash")
        self.assertTrue(artifact.steps[0].records[0].trade_state.can_enter)
        self.assertTrue(artifact.steps[0].records[0].trade_state.can_exit)

    def test_load_benchmark_state_artifact(self) -> None:
        artifact = load_benchmark_state_artifact(
            EXAMPLE_ROOT / "benchmark_state_history.json"
        )

        step = artifact.step_for_date("2026-04-20")
        self.assertEqual(artifact.benchmark_id, "CSI 800")
        self.assertEqual(artifact.classification, "citics_l1")
        self.assertEqual(artifact.weighting_method, "manual_sample")
        self.assertEqual(step.trade_date, "2026-04-20")
        self.assertEqual(step.available_at, "2026-04-20T15:30:00+08:00")
        self.assertAlmostEqual(
            sum(step.industry_weights.values()),
            1.0,
        )
        self.assertEqual(step.constituents[0].industry, "bank")

    def test_load_account_state_snapshot_and_adapt_to_portfolio_state(self) -> None:
        account_state = load_account_state_snapshot(
            DEPLOYMENT_EXAMPLE_ROOT / "account_state_2026_04_20.json"
        )
        portfolio_state = account_state_to_portfolio_state(
            portfolio_id="research_example_candidate_portfolio",
            account_state=account_state,
        )

        self.assertEqual(account_state.account_id, "shadow_a_share_cash")
        self.assertEqual(account_state.as_of_date, "2026-04-20")
        self.assertAlmostEqual(account_state.available_cash_cny, 1_200_000.0)
        self.assertEqual(portfolio_state.blocked_exit_assets, ["DDD"])
        self.assertAlmostEqual(portfolio_state.cash_weight, 0.12)
        self.assertAlmostEqual(portfolio_state.current_weights()["AAA"], 0.30)

    def test_load_portfolio_state_snapshot(self) -> None:
        state = load_portfolio_state_snapshot(
            DEPLOYMENT_EXAMPLE_ROOT / "portfolio_state_2026_04_20.json"
        )

        self.assertEqual(state.portfolio_id, "research_example_candidate_portfolio")
        self.assertEqual(state.as_of_date, "2026-04-20")
        self.assertAlmostEqual(state.cash_weight, 0.12)
        self.assertEqual(state.blocked_exit_assets, ["DDD"])
        self.assertEqual(state.current_weights()["AAA"], 0.30)

    def test_load_portfolio_promotion_replay_case_runs_end_to_end(self) -> None:
        loaded_case = load_portfolio_promotion_replay_case(EXAMPLE_ROOT / "replay_case.toml")

        replay = PortfolioPromotionReplay(
            mandate=loaded_case.mandate,
            construction_model=loaded_case.construction_model,
            default_cost_model=loaded_case.default_cost_model,
            gate=loaded_case.gate,
            cost_models=loaded_case.cost_models,
        )
        result = replay.replay(loaded_case.replay_input)

        self.assertEqual(
            loaded_case.replay_input.baseline_portfolio.sleeves,
            ["fundamental_rerating_core"],
        )
        self.assertEqual(
            loaded_case.replay_input.candidate_portfolio.sleeves,
            ["fundamental_rerating_core", "fundamental_rerating_satellite"],
        )
        self.assertEqual(
            loaded_case.replay_input.artifacts[1].sleeve_id,
            "fundamental_rerating_satellite",
        )
        self.assertIsNotNone(loaded_case.benchmark_state_artifact)
        self.assertTrue(result.decision.passed)
        self.assertGreater(result.marginal.marginal_ir_delta, 0.0)
        self.assertIsNotNone(result.regime_breakdown)

    def test_replay_case_can_bind_regime_overlay_evidence(self) -> None:
        loaded_case = load_portfolio_promotion_replay_case(
            EXAMPLE_ROOT / "replay_case_with_overlay.toml"
        )

        replay = PortfolioPromotionReplay(
            mandate=loaded_case.mandate,
            construction_model=loaded_case.construction_model,
            default_cost_model=loaded_case.default_cost_model,
            gate=loaded_case.gate,
            cost_models=loaded_case.cost_models,
        )
        result = replay.replay(loaded_case.replay_input)

        self.assertIsNotNone(loaded_case.regime_overlay)
        self.assertIsNotNone(result.regime_overlay)
        assert result.regime_overlay is not None
        self.assertEqual(
            [decision.state for decision in result.regime_overlay.decisions],
            ["normal", "de_risk", "cash_heavier"],
        )
        self.assertEqual(result.regime_overlay.summary.blocked_periods, 0)
        self.assertIn("regime_overlay_complete", result.decision.passed_checks)

    def test_cli_run_promotion_replay_separates_research_evidence_from_gate_output(self) -> None:
        result = subprocess.run(
            [
                "python3",
                "-m",
                "alpha_find_v2",
                "run-promotion-replay",
                "--case",
                str(EXAMPLE_ROOT / "replay_case.toml"),
            ],
            cwd=PROJECT_ROOT,
            env={"PYTHONPATH": "src"},
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        payload = json.loads(result.stdout)
        self.assertIn("decision", payload)
        self.assertIn("snapshot", payload)
        self.assertIn("research_evidence", payload)
        self.assertEqual(
            sorted(payload["research_evidence"].keys()),
            [
                "baseline_summary",
                "candidate_summary",
                "diagnostics",
                "marginal",
                "regime_breakdown",
                "regime_overlay",
                "walk_forward",
            ],
        )
        self.assertNotIn("baseline_summary", payload)
        self.assertNotIn("candidate_summary", payload)
        self.assertNotIn("marginal", payload)
        self.assertNotIn("diagnostics", payload)
        self.assertNotIn("walk_forward", payload)
        self.assertNotIn("regime_breakdown", payload)
        self.assertEqual(
            sorted(payload["research_evidence"]["regime_breakdown"].keys()),
            [
                "buckets",
                "stability",
                "weak_breadth_threshold_name_count",
                "weak_subperiods",
            ],
        )

    def test_load_portfolio_promotion_replay_case_reads_walk_forward_splits(self) -> None:
        case_text = (
            (EXAMPLE_ROOT / "replay_case.toml").read_text(encoding="utf-8")
            + "\n[[walk_forward_splits]]\n"
            + 'split_id = "full_window"\n'
            + 'start_trade_date = "2026-04-06"\n'
            + "\n[[walk_forward_splits]]\n"
            + 'split_id = "late_entry"\n'
            + 'start_trade_date = "2026-04-13"\n'
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            case_path = Path(temp_dir) / "replay_case.toml"
            case_path.write_text(case_text, encoding="utf-8")

            loaded_case = load_portfolio_promotion_replay_case(case_path)

        self.assertEqual(
            [split.split_id for split in loaded_case.definition.walk_forward_splits],
            ["full_window", "late_entry"],
        )
        self.assertEqual(
            [split.start_trade_date for split in loaded_case.replay_input.walk_forward_splits],
            ["2026-04-06", "2026-04-13"],
        )

    def test_load_portfolio_promotion_replay_case_rejects_mixed_target_ids(self) -> None:
        baseline_portfolio = """id = "mixed_target_baseline"
name = "Mixed Target Baseline"
mandate_id = "a_share_long_only_eod"
benchmark = "CSI 800"
rebalance_policy = "weekly"
description = "Baseline portfolio for mixed-target validation."
construction_model_id = "a_share_core_blend"
promotion_gate_id = "research_example_replay_gate"
execution_policy_id = "a_share_next_open_v1"
decay_monitor_id = "a_share_core_watch"
sleeves = ["fundamental_rerating_core"]

[allocation]
fundamental_rerating_core = 1.0

[constraints]
max_names = 4
max_single_name_weight = 0.60
max_industry_overweight = 0.30
"""
        candidate_portfolio = """id = "mixed_target_candidate"
name = "Mixed Target Candidate"
mandate_id = "a_share_long_only_eod"
benchmark = "CSI 800"
rebalance_policy = "weekly"
description = "Candidate portfolio for mixed-target validation."
construction_model_id = "a_share_core_blend"
promotion_gate_id = "research_example_replay_gate"
execution_policy_id = "a_share_next_open_v1"
decay_monitor_id = "a_share_core_watch"
sleeves = ["fundamental_rerating_core", "trend_leadership_core"]

[allocation]
fundamental_rerating_core = 0.30
trend_leadership_core = 0.70

[constraints]
max_names = 5
max_single_name_weight = 0.60
max_industry_overweight = 0.30
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            baseline_path = temp_root / "baseline.toml"
            baseline_path.write_text(baseline_portfolio, encoding="utf-8")
            candidate_path = temp_root / "candidate.toml"
            candidate_path.write_text(candidate_portfolio, encoding="utf-8")
            case_path = temp_root / "replay_case.toml"
            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "portfolio_promotion_replay_case"',
                        'case_id = "mixed_target_validation"',
                        'description = "Reject replay cases that mix incompatible target labels."',
                        f'baseline_portfolio_path = "{baseline_path}"',
                        f'candidate_portfolio_path = "{candidate_path}"',
                        f'default_cost_model_path = "{CONFIG_ROOT / "cost_models" / "base_a_share_cash.toml"}"',
                        f'benchmark_state_path = "{EXAMPLE_ROOT / "benchmark_state_history.json"}"',
                        'artifact_paths = [',
                        f'  "{EXAMPLE_ROOT / "sleeve_artifacts" / "fundamental_rerating_core.json"}",',
                        f'  "{EXAMPLE_ROOT / "sleeve_artifacts" / "trend_leadership_core.json"}",',
                        ']',
                        'periods_per_year = 52',
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "must share the same target_id"):
                load_portfolio_promotion_replay_case(case_path)

    def test_load_sleeve_artifact_rejects_unknown_schema_version(self) -> None:
        payload = {
            "schema_version": 99,
            "artifact_type": "sleeve_research_artifact",
            "sleeve_id": "fundamental_rerating_core",
            "mandate_id": "a_share_long_only_eod",
            "target_id": "open_t1_to_open_t20_residual_net_cost",
            "steps": [],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "bad_artifact.json"
            path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "Unsupported sleeve artifact schema version"):
                load_sleeve_artifact(path)


if __name__ == "__main__":
    unittest.main()
