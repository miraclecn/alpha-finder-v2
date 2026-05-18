from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from alpha_find_v2.strategy_generation_guardrails import (
    evaluate_generated_strategy_manifest,
    validate_generated_strategy_manifest,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class StrategyGenerationGuardrailsTest(unittest.TestCase):
    def test_valid_manifest_binds_object_chain_and_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            manifest_path = self._write_manifest(temp_root)

            result = validate_generated_strategy_manifest(manifest_path)

            self.assertTrue(result.valid)
            self.assertTrue(result.promotion_review_allowed)
            self.assertEqual(result.rejected_objectives, [])
            self.assertEqual(result.bound_ids["mandate_id"], "a_share_long_only_eod")
            self.assertEqual(result.bound_ids["thesis_id"], "trend_leadership")
            self.assertEqual(result.bound_ids["descriptor_set_id"], "trend_leadership_core")
            self.assertEqual(result.bound_ids["sleeve_id"], "trend_leadership_core")
            self.assertEqual(result.bound_ids["target_id"], "open_t1_to_open_t20_net_cost")
            self.assertEqual(result.bound_ids["portfolio_id"], "a_share_core")
            self.assertEqual(result.bound_ids["cost_model_id"], "base_a_share_cash")
            self.assertTrue(result.evidence_paths["data_quality_audit_path"].endswith("audit.json"))
            self.assertTrue(result.evidence_paths["daily_backtest_path"].endswith("backtest.json"))

    def test_rejects_bare_return_and_friction_ignoring_objectives(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            manifest_path = self._write_manifest(
                temp_root,
                objectives=["gross_return_only", "ignore_costs", "ignore_tradeability"],
            )

            result = evaluate_generated_strategy_manifest(manifest_path)

            self.assertFalse(result.valid)
            self.assertEqual(
                result.rejected_objectives,
                ["gross_return_only", "ignore_costs", "ignore_tradeability"],
            )
            self.assertIn("rejected_objectives", result.blockers)
            with self.assertRaisesRegex(ValueError, "rejected objectives"):
                validate_generated_strategy_manifest(manifest_path)

    def test_promotion_review_requires_replay_and_executable_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            manifest_path = self._write_manifest(
                temp_root,
                daily_backtest_path="",
                promotion_replay_path="",
                promotion_review_requested=True,
            )

            result = evaluate_generated_strategy_manifest(manifest_path)

            self.assertFalse(result.valid)
            self.assertFalse(result.promotion_review_allowed)
            self.assertIn("daily_backtest_path", result.blockers)
            self.assertIn("promotion_replay_path", result.blockers)

    def test_rejects_broken_object_chain_reference(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            manifest_path = self._write_manifest(
                temp_root,
                descriptor_set_path="config/descriptor_sets/fundamental_rerating_core.toml",
            )

            result = evaluate_generated_strategy_manifest(manifest_path)

            self.assertFalse(result.valid)
            self.assertIn("descriptor_set_thesis_mismatch", result.blockers)
            self.assertIn("sleeve_descriptor_set_mismatch", result.blockers)

    def test_cli_validate_generated_strategy_reports_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            manifest_path = self._write_manifest(temp_root)

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "alpha_find_v2",
                    "validate-generated-strategy",
                    "--manifest",
                    str(manifest_path),
                ],
                check=False,
                cwd=PROJECT_ROOT,
                env={"PYTHONPATH": "src"},
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["strategy_id"], "generated_trend_strategy_v1")
            self.assertEqual(payload["status"], "validated")
            self.assertTrue(payload["promotion_review_allowed"])
            self.assertEqual(payload["rejected_objectives"], [])

    def _write_manifest(
        self,
        temp_root: Path,
        *,
        objectives: list[str] | None = None,
        descriptor_set_path: str = "config/descriptor_sets/trend_leadership_core.toml",
        daily_backtest_path: str | None = None,
        promotion_replay_path: str | None = "research/examples/promotion_replay_real_output/replay_case.toml",
        promotion_review_requested: bool = True,
    ) -> Path:
        audit_path = temp_root / "audit.json"
        backtest_path = temp_root / "backtest.json"
        self._write_data_quality_audit(audit_path)
        self._write_daily_backtest(backtest_path)
        payload = {
            "schema_version": 1,
            "artifact_type": "generated_strategy_manifest",
            "strategy_id": "generated_trend_strategy_v1",
            "objectives": objectives or ["active_net_information_ratio"],
            "promotion_review_requested": promotion_review_requested,
            "mandate_path": "config/mandates/a_share_long_only_eod.toml",
            "thesis_path": "config/theses/trend_leadership.toml",
            "descriptor_set_path": descriptor_set_path,
            "sleeve_path": "config/sleeves/trend_leadership_core.toml",
            "target_path": "config/targets/open_t1_to_open_t20_net_cost.toml",
            "portfolio_path": "config/portfolio/a_share_core.toml",
            "cost_model_path": "config/cost_models/base_a_share_cash.toml",
            "data_quality_audit_path": str(audit_path),
            "daily_backtest_path": (
                str(backtest_path) if daily_backtest_path is None else daily_backtest_path
            ),
            "promotion_replay_path": promotion_replay_path or "",
        }
        manifest_path = temp_root / "manifest.json"
        manifest_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _write_data_quality_audit(self, path: Path) -> None:
        payload = {
            "schema_version": 1,
            "artifact_type": "market_data_quality_audit",
            "summary": {
                "promotion_blocking_quality_state": "blocked",
            },
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _write_daily_backtest(self, path: Path) -> None:
        payload = {
            "schema_version": 1,
            "artifact_type": "portfolio_backtest_result",
            "case_id": "generated_strategy_backtest",
            "artifact": {
                "daily_curve": [],
                "summary": {},
            },
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
