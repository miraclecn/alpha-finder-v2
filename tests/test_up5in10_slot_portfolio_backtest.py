from __future__ import annotations

import unittest

import pandas as pd

from alpha_find_v2.up5in10_slot_portfolio_backtest import (
    DynamicTrailingPolicy,
    FixedTpSlPolicy,
    run_dynamic_slot_portfolio_backtest,
    run_fixed_slot_portfolio_backtest,
    summarize_continuous_slot_backtest_by_year,
    summarize_slot_backtest,
)


class Up5In10SlotPortfolioBacktestTest(unittest.TestCase):
    def test_enters_next_open_and_respects_t_plus_one_exit(self) -> None:
        selected_rows = pd.DataFrame(
            [
                {
                    "security_id": "AAA.SZ",
                    "trade_date": "20240102",
                    "year": 2024,
                    "path_score": 2.0,
                }
            ]
        )
        bars = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "open_adj": 9.8, "high_adj": 10.0, "low_adj": 9.7, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240103", "open_adj": 10.0, "high_adj": 11.0, "low_adj": 9.9, "close_adj": 10.8},
                {"security_id": "AAA.SZ", "trade_date": "20240104", "open_adj": 10.7, "high_adj": 10.8, "low_adj": 10.1, "close_adj": 10.4},
            ]
        )

        curve, trades = run_fixed_slot_portfolio_backtest(
            selected_rows=selected_rows,
            bars=bars,
            calendar=["20240102", "20240103", "20240104"],
            policy=FixedTpSlPolicy(take_profit=0.05, stop_loss=0.10, max_hold_days=10),
            initial_cash=100_000.0,
            slot_count=1,
            buy_cost_bps=0.0,
            sell_cost_bps=0.0,
        )

        self.assertEqual(len(trades), 1)
        trade = trades.iloc[0]
        self.assertEqual(trade["entry_trade_date"], "20240103")
        self.assertEqual(trade["exit_trade_date"], "20240104")
        self.assertEqual(trade["exit_reason"], "take_profit")
        self.assertAlmostEqual(float(trade["gross_ret"]), 0.05)
        self.assertGreater(float(curve.iloc[-1]["portfolio_value"]), 100_000.0)

    def test_uses_stop_first_when_take_profit_and_stop_hit_same_day(self) -> None:
        selected_rows = pd.DataFrame(
            [
                {
                    "security_id": "AAA.SZ",
                    "trade_date": "20240102",
                    "year": 2024,
                    "path_score": 2.0,
                }
            ]
        )
        bars = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240103", "open_adj": 10.0, "high_adj": 10.2, "low_adj": 9.9, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240104", "open_adj": 10.0, "high_adj": 10.6, "low_adj": 8.9, "close_adj": 9.5},
            ]
        )

        _, trades = run_fixed_slot_portfolio_backtest(
            selected_rows=selected_rows,
            bars=bars,
            calendar=["20240102", "20240103", "20240104"],
            policy=FixedTpSlPolicy(take_profit=0.05, stop_loss=0.10, max_hold_days=10),
            initial_cash=100_000.0,
            slot_count=1,
            buy_cost_bps=0.0,
            sell_cost_bps=0.0,
        )

        self.assertEqual(trades.loc[0, "exit_reason"], "same_day_stop_first")
        self.assertAlmostEqual(float(trades.loc[0, "gross_ret"]), -0.10)

    def test_summary_reports_annual_return_drawdown_and_trade_stats(self) -> None:
        selected_rows = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "year": 2024, "path_score": 2.0},
            ]
        )
        bars = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240103", "open_adj": 10.0, "high_adj": 10.2, "low_adj": 9.9, "close_adj": 10.1},
                {"security_id": "AAA.SZ", "trade_date": "20240104", "open_adj": 10.1, "high_adj": 10.4, "low_adj": 10.0, "close_adj": 10.2},
            ]
        )

        curve, trades = run_fixed_slot_portfolio_backtest(
            selected_rows=selected_rows,
            bars=bars,
            calendar=["20240102", "20240103", "20240104"],
            policy=FixedTpSlPolicy(take_profit=0.50, stop_loss=0.50, max_hold_days=1),
            initial_cash=100_000.0,
            slot_count=1,
            buy_cost_bps=0.0,
            sell_cost_bps=0.0,
        )

        summary = summarize_slot_backtest(
            daily_curve=curve,
            trades=trades,
            initial_cash=100_000.0,
        )

        self.assertEqual(int(summary.loc[0, "year"]), 2024)
        self.assertEqual(int(summary.loc[0, "closed_trades"]), 1)
        self.assertAlmostEqual(float(summary.loc[0, "trade_win_rate"]), 1.0)
        self.assertGreater(float(summary.loc[0, "total_return"]), 0.0)

    def test_dynamic_trailing_policy_ignores_entry_day_hard_stop(self) -> None:
        selected_rows = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "year": 2024, "path_score": 2.0},
            ]
        )
        bars = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240103", "open_adj": 10.0, "high_adj": 10.1, "low_adj": 8.0, "close_adj": 9.0},
                {"security_id": "AAA.SZ", "trade_date": "20240104", "open_adj": 9.4, "high_adj": 9.5, "low_adj": 9.3, "close_adj": 9.4},
            ]
        )

        _, trades = run_dynamic_slot_portfolio_backtest(
            selected_rows=selected_rows,
            bars=bars,
            calendar=["20240102", "20240103", "20240104"],
            policy=DynamicTrailingPolicy(stop_loss=0.08, activation_return=0.10, trailing_drawdown=0.05),
            initial_cash=100_000.0,
            max_positions=10,
            buy_cost_bps=0.0,
            sell_cost_bps=0.0,
        )

        self.assertEqual(trades.loc[0, "exit_trade_date"], "20240104")
        self.assertEqual(trades.loc[0, "exit_reason"], "forced_final_close")

    def test_dynamic_policy_can_target_twenty_percent_position_size(self) -> None:
        selected_rows = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "year": 2024, "path_score": 2.0},
            ]
        )
        bars = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240103", "open_adj": 10.0, "high_adj": 10.2, "low_adj": 9.9, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240104", "open_adj": 10.0, "high_adj": 10.2, "low_adj": 9.9, "close_adj": 10.0},
            ]
        )

        curve, _ = run_dynamic_slot_portfolio_backtest(
            selected_rows=selected_rows,
            bars=bars,
            calendar=["20240102", "20240103", "20240104"],
            policy=DynamicTrailingPolicy(stop_loss=0.08, activation_return=0.10, trailing_drawdown=0.03),
            initial_cash=100_000.0,
            max_positions=10,
            target_position_fraction=0.20,
            buy_cost_bps=0.0,
            sell_cost_bps=0.0,
        )

        entry_day = curve.loc[curve["trade_date"] == "20240103"].iloc[0]
        self.assertAlmostEqual(float(entry_day["gross_exposure"]), 0.20)

    def test_dynamic_trailing_policy_exits_after_prior_peak_drawdown(self) -> None:
        selected_rows = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "year": 2024, "path_score": 2.0},
            ]
        )
        bars = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240103", "open_adj": 10.0, "high_adj": 10.2, "low_adj": 9.8, "close_adj": 10.1},
                {"security_id": "AAA.SZ", "trade_date": "20240104", "open_adj": 10.2, "high_adj": 11.2, "low_adj": 10.1, "close_adj": 11.0},
                {"security_id": "AAA.SZ", "trade_date": "20240105", "open_adj": 10.9, "high_adj": 11.0, "low_adj": 10.5, "close_adj": 10.6},
            ]
        )

        _, trades = run_dynamic_slot_portfolio_backtest(
            selected_rows=selected_rows,
            bars=bars,
            calendar=["20240102", "20240103", "20240104", "20240105"],
            policy=DynamicTrailingPolicy(stop_loss=0.08, activation_return=0.10, trailing_drawdown=0.05),
            initial_cash=100_000.0,
            max_positions=10,
            buy_cost_bps=0.0,
            sell_cost_bps=0.0,
        )

        self.assertEqual(trades.loc[0, "exit_trade_date"], "20240105")
        self.assertEqual(trades.loc[0, "exit_reason"], "dynamic_trailing_stop")
        self.assertAlmostEqual(float(trades.loc[0, "gross_ret"]), 11.2 * 0.95 / 10.0 - 1.0)

    def test_dynamic_policy_uses_signal_specific_priority_before_path_score(self) -> None:
        selected_rows = pd.DataFrame(
            [
                {
                    "security_id": "AAA.SZ",
                    "trade_date": "20240102",
                    "year": 2024,
                    "path_score": 1.0,
                    "signal_priority": 10,
                    "target_position_fraction": 0.15,
                },
                {
                    "security_id": "BBB.SZ",
                    "trade_date": "20240102",
                    "year": 2024,
                    "path_score": 9.0,
                    "signal_priority": 20,
                    "target_position_fraction": 0.05,
                },
            ]
        )
        bars = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "BBB.SZ", "trade_date": "20240102", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240103", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "BBB.SZ", "trade_date": "20240103", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240104", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "BBB.SZ", "trade_date": "20240104", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
            ]
        )

        _, trades = run_dynamic_slot_portfolio_backtest(
            selected_rows=selected_rows,
            bars=bars,
            calendar=["20240102", "20240103", "20240104"],
            policy=DynamicTrailingPolicy(stop_loss=0.08, activation_return=0.10, trailing_drawdown=0.03),
            initial_cash=100_000.0,
            max_positions=1,
            buy_cost_bps=0.0,
            sell_cost_bps=0.0,
        )

        self.assertEqual(trades["security_id"].tolist(), ["AAA.SZ"])

    def test_dynamic_policy_uses_signal_specific_target_position_fraction(self) -> None:
        selected_rows = pd.DataFrame(
            [
                {
                    "security_id": "AAA.SZ",
                    "trade_date": "20240102",
                    "year": 2024,
                    "path_score": 2.0,
                    "signal_priority": 10,
                    "target_position_fraction": 0.15,
                },
                {
                    "security_id": "BBB.SZ",
                    "trade_date": "20240102",
                    "year": 2024,
                    "path_score": 1.0,
                    "signal_priority": 20,
                    "target_position_fraction": 0.05,
                },
            ]
        )
        bars = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "BBB.SZ", "trade_date": "20240102", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240103", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "BBB.SZ", "trade_date": "20240103", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240104", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "BBB.SZ", "trade_date": "20240104", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
            ]
        )

        curve, trades = run_dynamic_slot_portfolio_backtest(
            selected_rows=selected_rows,
            bars=bars,
            calendar=["20240102", "20240103", "20240104"],
            policy=DynamicTrailingPolicy(stop_loss=0.08, activation_return=0.10, trailing_drawdown=0.03),
            initial_cash=100_000.0,
            max_positions=5,
            buy_cost_bps=0.0,
            sell_cost_bps=0.0,
        )

        entry_day = curve.loc[curve["trade_date"] == "20240103"].iloc[0]
        self.assertEqual(sorted(trades["security_id"].tolist()), ["AAA.SZ", "BBB.SZ"])
        self.assertAlmostEqual(float(entry_day["gross_exposure"]), 0.20)

    def test_dynamic_policy_carries_signal_origin_into_trade_rows(self) -> None:
        selected_rows = pd.DataFrame(
            [
                {
                    "security_id": "AAA.SZ",
                    "trade_date": "20240102",
                    "year": 2024,
                    "path_score": 2.0,
                    "signal_origin": "reentry_A2",
                }
            ]
        )
        bars = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240103", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240104", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
            ]
        )

        _, trades = run_dynamic_slot_portfolio_backtest(
            selected_rows=selected_rows,
            bars=bars,
            calendar=["20240102", "20240103", "20240104"],
            policy=DynamicTrailingPolicy(stop_loss=0.08, activation_return=0.10, trailing_drawdown=0.03),
            initial_cash=100_000.0,
            max_positions=10,
            buy_cost_bps=0.0,
            sell_cost_bps=0.0,
        )

        self.assertEqual(trades.loc[0, "signal_origin"], "reentry_A2")

    def test_dynamic_summary_uses_calendar_year_returns_without_year_end_reset(self) -> None:
        selected_rows = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20241230", "year": 2024, "path_score": 2.0},
            ]
        )
        bars = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20241230", "open_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0, "close_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20241231", "open_adj": 10.0, "high_adj": 10.2, "low_adj": 9.8, "close_adj": 10.2},
                {"security_id": "AAA.SZ", "trade_date": "20250102", "open_adj": 10.2, "high_adj": 10.4, "low_adj": 10.1, "close_adj": 10.4},
            ]
        )

        curve, trades = run_dynamic_slot_portfolio_backtest(
            selected_rows=selected_rows,
            bars=bars,
            calendar=["20241230", "20241231", "20250102"],
            policy=DynamicTrailingPolicy(stop_loss=0.08, activation_return=0.50, trailing_drawdown=0.05),
            initial_cash=100_000.0,
            max_positions=10,
            buy_cost_bps=0.0,
            sell_cost_bps=0.0,
        )
        summary = summarize_continuous_slot_backtest_by_year(
            daily_curve=curve,
            trades=trades,
        )

        self.assertEqual(summary["year"].tolist(), [2024, 2025])
        self.assertEqual(trades.loc[0, "exit_trade_date"], "20250102")
        self.assertEqual(trades.loc[0, "exit_reason"], "forced_final_close")
        self.assertGreater(float(summary.loc[0, "total_return"]), 0.0)
        self.assertGreater(float(summary.loc[1, "total_return"]), 0.0)


if __name__ == "__main__":
    unittest.main()
