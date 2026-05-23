from __future__ import annotations

import json
from pathlib import Path
import subprocess
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class BacktesterValidationTest(unittest.TestCase):
    def test_validation_suite_reports_success(self) -> None:
        from alpha_find_v2.backtester_validation import run_backtester_validation_suite

        result = run_backtester_validation_suite()

        self.assertTrue(result.success)
        self.assertGreater(result.tests_run, 0)
        self.assertEqual(result.failures, 0)
        self.assertEqual(result.errors, 0)
        self.assertIn("t_plus_one", result.covered_capabilities)
        self.assertIn("corporate_actions", result.covered_capabilities)
        self.assertIn("benchmark_active_metrics", result.covered_capabilities)

    def test_cli_prints_validation_summary_json(self) -> None:
        completed = subprocess.run(
            [
                "python3",
                "-m",
                "alpha_find_v2",
                "validate-backtester",
            ],
            cwd=PROJECT_ROOT,
            env={"PYTHONPATH": "src"},
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertTrue(payload["success"])
        self.assertGreater(payload["tests_run"], 0)
        self.assertEqual(payload["failures"], 0)
        self.assertEqual(payload["errors"], 0)


if __name__ == "__main__":
    unittest.main()
