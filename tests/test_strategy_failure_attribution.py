from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
import unittest

import duckdb

from alpha_find_v2.strategy_failure_attribution import (
    build_strategy_failure_attribution,
    write_strategy_failure_attribution,
)


class StrategyFailureAttributionTest(unittest.TestCase):
    def test_builds_failure_report_from_backtest_json_and_source_db(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            backtest_path = temp_root / "portfolio_backtest.json"
            source_db_path = temp_root / "source.duckdb"
            overlay_path = temp_root / "overlay_observations.json"
            output_path = temp_root / "failure_report.json"

            self._write_backtest(backtest_path)
            self._write_source_db(source_db_path)
            self._write_overlay_observations(overlay_path)

            report = build_strategy_failure_attribution(
                backtest_path=backtest_path,
                source_db_path=source_db_path,
                overlay_observations_path=overlay_path,
            )
            written_path = write_strategy_failure_attribution(report, output_path)

            self.assertEqual(written_path, output_path)
            self.assertEqual(report["artifact_type"], "strategy_failure_attribution_report")
            self.assertEqual(report["backtest"]["case_id"], "synthetic_failure_case")

            self.assertAlmostEqual(
                report["return_buckets"]["yearly"]["2021"]["return"],
                -0.20,
            )
            self.assertAlmostEqual(
                report["return_buckets"]["monthly"]["2021-01"]["return"],
                -0.15,
            )

            top_loser = report["holding_contribution"]["top_losers"][0]
            self.assertEqual(top_loser["asset_id"], "AAA.SZ")
            self.assertAlmostEqual(top_loser["contribution"], -0.102)
            top_winner = report["holding_contribution"]["top_winners"][0]
            self.assertEqual(top_winner["asset_id"], "BBB.SZ")
            self.assertAlmostEqual(top_winner["contribution"], 0.004)

            industry_loser = report["industry_contribution"]["top_losers"][0]
            self.assertEqual(industry_loser["industry_code"], "I10")
            self.assertAlmostEqual(industry_loser["contribution"], -0.102)

            self.assertEqual(report["trade_friction"]["orders"]["total"], 3)
            self.assertEqual(
                report["trade_friction"]["blocked_orders"]["by_reason"],
                {"limit_down_open_lock": 1},
            )
            self.assertEqual(
                report["trade_friction"]["partial_fills"]["by_reason"],
                {"participation_cap": 1},
            )
            self.assertAlmostEqual(report["trade_friction"]["cost_drag"]["total_cost_cny"], 1.5)

            overlay = report["overlay_state_comparison"]
            self.assertEqual(overlay["state_counts"], {"normal": 1, "cash_heavier": 1})
            self.assertAlmostEqual(overlay["by_state"]["cash_heavier"]["average_daily_return"], -0.1)

            concentration = report["loss_concentration"]
            self.assertEqual(concentration["classification"], "concentrated")
            self.assertAlmostEqual(concentration["top_5_holding_loss_share"], 1.0)

            written = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(written["artifact_type"], "strategy_failure_attribution_report")

    def test_cli_writes_failure_attribution_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            backtest_path = temp_root / "portfolio_backtest.json"
            source_db_path = temp_root / "source.duckdb"
            overlay_path = temp_root / "overlay_observations.json"
            output_path = temp_root / "failure_report.json"

            self._write_backtest(backtest_path)
            self._write_source_db(source_db_path)
            self._write_overlay_observations(overlay_path)

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "alpha_find_v2",
                    "explain-strategy-failure",
                    "--backtest",
                    str(backtest_path),
                    "--source-db",
                    str(source_db_path),
                    "--overlay-observations",
                    str(overlay_path),
                    "--output",
                    str(output_path),
                ],
                check=False,
                cwd=Path(__file__).resolve().parents[1],
                env={"PYTHONPATH": "src"},
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["output_path"], str(output_path))
            self.assertTrue(output_path.exists())

    def _write_backtest(self, path: Path) -> None:
        payload = {
            "schema_version": 1,
            "artifact_type": "portfolio_backtest_result",
            "case_id": "synthetic_failure_case",
            "description": "Synthetic strategy failure attribution fixture.",
            "artifact": {
                "daily_curve": [
                    {
                        "trade_date": "20210104",
                        "equity": 100.0,
                        "cash": 0.0,
                        "positions_value": 100.0,
                        "gross_exposure": 1.0,
                        "daily_return": 0.0,
                        "weights": {"AAA.SZ": 0.6, "BBB.SZ": 0.4},
                    },
                    {
                        "trade_date": "20210105",
                        "equity": 90.0,
                        "cash": 0.0,
                        "positions_value": 90.0,
                        "gross_exposure": 1.0,
                        "daily_return": -0.1,
                        "weights": {"AAA.SZ": 0.6, "BBB.SZ": 0.4},
                    },
                    {
                        "trade_date": "20210106",
                        "equity": 85.0,
                        "cash": 0.0,
                        "positions_value": 85.0,
                        "gross_exposure": 1.0,
                        "daily_return": -0.05555555555555555,
                        "weights": {"AAA.SZ": 0.6, "BBB.SZ": 0.4},
                    },
                    {
                        "trade_date": "20210201",
                        "equity": 80.0,
                        "cash": 0.0,
                        "positions_value": 80.0,
                        "gross_exposure": 0.35,
                        "daily_return": -0.058823529411764705,
                        "weights": {"AAA.SZ": 0.6, "BBB.SZ": 0.4},
                    },
                ],
                "daily_holdings": [
                    {
                        "trade_date": "20210104",
                        "asset_id": "AAA.SZ",
                        "shares": 60.0,
                        "available_shares": 60.0,
                        "mark_price": 1.0,
                        "market_value": 60.0,
                        "weight": 0.6,
                    },
                    {
                        "trade_date": "20210104",
                        "asset_id": "BBB.SZ",
                        "shares": 40.0,
                        "available_shares": 40.0,
                        "mark_price": 1.0,
                        "market_value": 40.0,
                        "weight": 0.4,
                    },
                    {
                        "trade_date": "20210105",
                        "asset_id": "AAA.SZ",
                        "shares": 60.0,
                        "available_shares": 60.0,
                        "mark_price": 0.9,
                        "market_value": 54.0,
                        "weight": 0.6,
                    },
                    {
                        "trade_date": "20210105",
                        "asset_id": "BBB.SZ",
                        "shares": 40.0,
                        "available_shares": 40.0,
                        "mark_price": 1.05,
                        "market_value": 42.0,
                        "weight": 0.4,
                    },
                    {
                        "trade_date": "20210106",
                        "asset_id": "AAA.SZ",
                        "shares": 60.0,
                        "available_shares": 60.0,
                        "mark_price": 0.855,
                        "market_value": 51.3,
                        "weight": 0.6,
                    },
                    {
                        "trade_date": "20210106",
                        "asset_id": "BBB.SZ",
                        "shares": 40.0,
                        "available_shares": 40.0,
                        "mark_price": 0.9975,
                        "market_value": 39.9,
                        "weight": 0.4,
                    },
                ],
                "orders": [
                    {
                        "order_id": "20210104:0001",
                        "decision_date": "20210104",
                        "execution_date": "20210104",
                        "asset_id": "AAA.SZ",
                        "side": "buy",
                        "requested_quantity": 60.0,
                        "target_weight": 0.6,
                        "reason": "rebalance",
                    },
                    {
                        "order_id": "20210105:0002",
                        "decision_date": "20210104",
                        "execution_date": "20210105",
                        "asset_id": "AAA.SZ",
                        "side": "sell",
                        "requested_quantity": 10.0,
                        "target_weight": 0.5,
                        "reason": "rebalance",
                    },
                    {
                        "order_id": "20210106:0003",
                        "decision_date": "20210105",
                        "execution_date": "20210106",
                        "asset_id": "BBB.SZ",
                        "side": "sell",
                        "requested_quantity": 20.0,
                        "target_weight": 0.2,
                        "reason": "rebalance",
                    },
                ],
                "fills": [
                    {
                        "order_id": "20210104:0001",
                        "decision_date": "20210104",
                        "execution_date": "20210104",
                        "asset_id": "AAA.SZ",
                        "side": "buy",
                        "quantity": 60.0,
                        "price": 1.0,
                        "gross_value": 60.0,
                        "cost": 1.0,
                        "net_cash_flow": -61.0,
                        "cost_model_id": "base",
                        "participation_cap": 0.1,
                    },
                    {
                        "order_id": "20210106:0003",
                        "decision_date": "20210105",
                        "execution_date": "20210106",
                        "asset_id": "BBB.SZ",
                        "side": "sell",
                        "quantity": 20.0,
                        "price": 1.0,
                        "gross_value": 20.0,
                        "cost": 0.5,
                        "net_cash_flow": 19.5,
                        "cost_model_id": "base",
                        "participation_cap": 0.1,
                    },
                ],
                "diagnostics": {
                    "blocked_orders": [
                        {
                            "order_id": "20210105:0002",
                            "decision_date": "20210104",
                            "execution_date": "20210105",
                            "asset_id": "AAA.SZ",
                            "side": "sell",
                            "requested_quantity": 10.0,
                            "reason": "limit_down_open_lock",
                        }
                    ],
                    "partial_fills": [
                        {
                            "order_id": "20210106:0003",
                            "decision_date": "20210105",
                            "execution_date": "20210106",
                            "asset_id": "BBB.SZ",
                            "side": "sell",
                            "requested_quantity": 20.0,
                            "filled_quantity": 12.0,
                            "reason": "participation_cap",
                        }
                    ],
                    "known_limitations": [],
                    "unresolved_corporate_actions": [],
                    "corporate_action_exception_exposures": [],
                    "qfq_fallback_price_exposures": [],
                    "tradeability_fallback_exposures": [],
                },
                "summary": {
                    "start_date": "20210104",
                    "end_date": "20210201",
                    "trading_days": 4,
                    "initial_cash_cny": 100.0,
                    "final_equity": 80.0,
                    "total_return": -0.2,
                    "information_ratio": -1.2,
                    "max_drawdown": -0.2,
                    "turnover": 0.45,
                    "buy_turnover": 0.67,
                    "sell_turnover": 0.22,
                    "total_costs": 1.5,
                    "blocked_trade_share": 1 / 3,
                    "partial_fill_share": 1 / 3,
                },
            },
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def _write_source_db(self, path: Path) -> None:
        conn = duckdb.connect(str(path))
        try:
            conn.execute(
                """
                CREATE TABLE daily_bar_pit (
                    security_id VARCHAR,
                    trade_date VARCHAR,
                    close DOUBLE,
                    pre_close DOUBLE,
                    pct_chg DOUBLE
                )
                """
            )
            conn.execute(
                """
                INSERT INTO daily_bar_pit VALUES
                    ('AAA.SZ', '20210104', 1.00, 1.00, 0.0),
                    ('BBB.SZ', '20210104', 1.00, 1.00, 0.0),
                    ('AAA.SZ', '20210105', 0.90, 1.00, -10.0),
                    ('BBB.SZ', '20210105', 1.05, 1.00, 5.0),
                    ('AAA.SZ', '20210106', 0.855, 0.90, -5.0),
                    ('BBB.SZ', '20210106', 0.9975, 1.05, -5.0),
                    ('AAA.SZ', '20210201', 0.8379, 0.855, -2.0),
                    ('BBB.SZ', '20210201', 1.007475, 0.9975, 1.0)
                """
            )
            conn.execute(
                """
                CREATE TABLE industry_classification_pit (
                    security_id VARCHAR,
                    industry_schema VARCHAR,
                    industry_code VARCHAR,
                    effective_at VARCHAR,
                    removed_at VARCHAR
                )
                """
            )
            conn.execute(
                """
                INSERT INTO industry_classification_pit VALUES
                    ('AAA.SZ', 'sw2021_l1', 'I10', '2020-01-01 00:00:00', NULL),
                    ('BBB.SZ', 'sw2021_l1', 'I20', '2020-01-01 00:00:00', NULL)
                """
            )
        finally:
            conn.close()

    def _write_overlay_observations(self, path: Path) -> None:
        payload = {
            "schema_version": 1,
            "artifact_type": "regime_overlay_observation_history",
            "overlay_id": "a_share_risk_overlay",
            "steps": [
                {
                    "trade_date": "20210104",
                    "input_states": {
                        "benchmark_trend": "supportive",
                        "market_breadth": "supportive",
                        "dispersion": "neutral",
                        "realized_volatility": "supportive",
                        "price_limit_stress": "supportive",
                    },
                },
                {
                    "trade_date": "20210105",
                    "input_states": {
                        "benchmark_trend": "risk_off",
                        "market_breadth": "risk_off",
                        "dispersion": "risk_off",
                        "realized_volatility": "neutral",
                        "price_limit_stress": "supportive",
                    },
                },
            ],
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
