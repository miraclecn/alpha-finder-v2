from __future__ import annotations

import json
from pathlib import Path
import subprocess
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class BacktraderPocTest(unittest.TestCase):
    def test_single_name_round_trip_matches_internal_backtester(self) -> None:
        from alpha_find_v2.backtrader_poc import run_named_backtrader_poc

        result = run_named_backtrader_poc("single_name_round_trip")

        self.assertEqual(result.scenario_id, "single_name_round_trip")
        self.assertEqual(
            result.internal.fills,
            [
                {
                    "asset_id": "AAA",
                    "side": "buy",
                    "execution_date": "20260106",
                    "quantity": 1000.0,
                    "price": 10.0,
                },
                {
                    "asset_id": "AAA",
                    "side": "sell",
                    "execution_date": "20260107",
                    "quantity": 1000.0,
                    "price": 11.0,
                },
            ],
        )
        self.assertEqual(result.backtrader.fills, result.internal.fills)
        self.assertEqual(
            result.internal.daily_equity,
            [
                {"trade_date": "20260105", "equity": 10000.0},
                {"trade_date": "20260106", "equity": 10000.0},
                {"trade_date": "20260107", "equity": 11000.0},
            ],
        )
        self.assertEqual(result.backtrader.daily_equity, result.internal.daily_equity)
        self.assertAlmostEqual(result.internal.final_equity, 11000.0)
        self.assertAlmostEqual(result.backtrader.final_equity, 11000.0)
        self.assertAlmostEqual(result.equity_delta, 0.0)

    def test_cli_prints_backtrader_poc_comparison_json(self) -> None:
        completed = subprocess.run(
            [
                "python3",
                "-m",
                "alpha_find_v2",
                "run-backtrader-poc",
                "--scenario",
                "single_name_round_trip",
            ],
            cwd=PROJECT_ROOT,
            env={"PYTHONPATH": "src"},
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["scenario_id"], "single_name_round_trip")
        self.assertEqual(payload["equity_delta"], 0.0)
        self.assertEqual(payload["internal"]["final_equity"], 11000.0)
        self.assertEqual(payload["backtrader"]["final_equity"], 11000.0)

    def test_weekly_sixteen_name_rotation_matches_current_strategy_parameters(self) -> None:
        from alpha_find_v2.backtrader_poc import run_named_backtrader_poc

        result = run_named_backtrader_poc("weekly_16_name_rotation_current_params")

        self.assertEqual(result.scenario_id, "weekly_16_name_rotation_current_params")
        self.assertGreaterEqual(len(result.internal.rebalance_dates), 4)
        self.assertEqual(result.backtrader.rebalance_dates, result.internal.rebalance_dates)
        self.assertGreaterEqual(len(result.internal.fills), 40)
        self.assertEqual(len(result.backtrader.fills), len(result.internal.fills))
        self.assertLessEqual(abs(result.equity_delta), 2000.0)
        self.assertAlmostEqual(
            result.backtrader.final_equity,
            result.internal.final_equity + result.equity_delta,
            places=6,
        )
        self.assertGreater(result.internal.final_equity, 0.0)
        self.assertGreater(result.backtrader.final_equity, 0.0)


if __name__ == "__main__":
    unittest.main()
