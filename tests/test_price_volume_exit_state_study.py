from __future__ import annotations

import unittest

import pandas as pd

from alpha_find_v2.price_volume_exit_state_study import (
    _build_trade_forward_observations,
    classify_price_volume_state,
    replay_fixed_tp_sl_policy,
    replay_hybrid_exit_policy,
    replay_state_exit_policy,
    summarize_policy_replays,
    summarize_exit_state_candidates,
)


class PriceVolumeExitStateStudyTest(unittest.TestCase):
    def test_classify_price_volume_state_uses_existing_thresholds(self) -> None:
        self.assertEqual(classify_price_volume_state(daily_return=0.02, turnover_ratio=1.4), "expand_up")
        self.assertEqual(classify_price_volume_state(daily_return=-0.02, turnover_ratio=1.4), "expand_down")
        self.assertEqual(classify_price_volume_state(daily_return=0.0, turnover_ratio=0.7), "contract_flat")
        self.assertEqual(classify_price_volume_state(daily_return=0.003, turnover_ratio=1.0), "neutral")

    def test_build_trade_forward_observations_tracks_remaining_path_metrics(self) -> None:
        bars = pd.DataFrame(
            [
                {
                    "trade_date": "20240102",
                    "open_adj": 10.0,
                    "close_adj": 10.0,
                    "high_adj": 10.0,
                    "low_adj": 10.0,
                    "daily_ret1": 0.0,
                    "turnover_ratio": 1.0,
                },
                {
                    "trade_date": "20240103",
                    "open_adj": 10.1,
                    "close_adj": 10.4,
                    "high_adj": 11.05,
                    "low_adj": 10.2,
                    "daily_ret1": 0.04,
                    "turnover_ratio": 1.4,
                },
                {
                    "trade_date": "20240104",
                    "open_adj": 10.3,
                    "close_adj": 10.2,
                    "high_adj": 10.45,
                    "low_adj": 9.9,
                    "daily_ret1": -0.019230769230769232,
                    "turnover_ratio": 1.5,
                },
                {
                    "trade_date": "20240105",
                    "open_adj": 10.0,
                    "close_adj": 9.8,
                    "high_adj": 10.0,
                    "low_adj": 9.6,
                    "daily_ret1": -0.0392156862745098,
                    "turnover_ratio": 0.7,
                },
            ]
        )
        trade = {
            "security_id": "AAA.SZ",
            "trade_date": "20240102",
            "year": 2024,
            "predicted_regime": "repair_retake",
            "entry_close_adj": 10.0,
            "score": 1.5,
        }

        observations = _build_trade_forward_observations(
            trade=trade,
            bars=bars,
            entry_index=0,
            forward_days=3,
        )

        self.assertEqual(len(observations), 3)
        first = observations[0]
        self.assertEqual(first["state"], "expand_up")
        self.assertAlmostEqual(first["close_ret_from_entry"], 0.04)
        self.assertAlmostEqual(first["peak_ret_so_far"], 0.105)
        self.assertAlmostEqual(first["close_drawdown_from_peak"], 10.4 / 11.05 - 1.0)
        self.assertAlmostEqual(first["remaining_close_ret"], 9.8 / 10.4 - 1.0)
        self.assertAlmostEqual(first["future_max_ret_from_today"], 10.45 / 10.4 - 1.0)
        self.assertAlmostEqual(first["future_min_ret_from_today"], 9.6 / 10.4 - 1.0)
        self.assertEqual(first["future_first_hit_5"], "down5_first")

        second = observations[1]
        self.assertEqual(second["state"], "expand_down")
        self.assertEqual(second["peak_profit_bucket"], "10_20")
        self.assertEqual(second["drawdown_bucket"], "mild_pullback")

        third = observations[2]
        self.assertEqual(third["state"], "contract_down")
        self.assertTrue(pd.isna(third["future_max_ret_from_today"]))
        self.assertTrue(pd.isna(third["future_min_ret_from_today"]))
        self.assertEqual(third["future_first_hit_5"], "unresolved")

    def test_replay_state_exit_policy_uses_next_open_after_signal(self) -> None:
        observations = pd.DataFrame(
            [
                {
                    "security_id": "AAA.SZ",
                    "entry_trade_date": "20240102",
                    "future_trade_date": "20240103",
                    "next_trade_date": "20240104",
                    "year": 2021,
                    "step": 1,
                    "state": "neutral",
                    "peak_ret_so_far": 0.08,
                    "peak_profit_bucket": "5_10",
                    "drawdown_bucket": "tight",
                    "close_ret_from_entry": 0.04,
                    "next_open_ret_from_entry": 0.03,
                    "high_ret_from_entry": 0.05,
                    "low_ret_from_entry": -0.01,
                },
                {
                    "security_id": "AAA.SZ",
                    "entry_trade_date": "20240102",
                    "future_trade_date": "20240104",
                    "next_trade_date": "20240105",
                    "year": 2021,
                    "step": 2,
                    "state": "expand_down",
                    "peak_ret_so_far": 0.18,
                    "peak_profit_bucket": "10_20",
                    "drawdown_bucket": "deep_pullback",
                    "close_ret_from_entry": -0.06,
                    "next_open_ret_from_entry": -0.05,
                    "high_ret_from_entry": 0.02,
                    "low_ret_from_entry": -0.08,
                },
            ]
        )

        replay = replay_state_exit_policy(
            observations,
            policy_name="strict",
            round_trip_cost_bps=10.0,
        )

        self.assertEqual(len(replay), 1)
        self.assertEqual(replay.loc[0, "exit_step"], 2)
        self.assertEqual(replay.loc[0, "exit_trade_date"], "20240105")
        self.assertEqual(replay.loc[0, "exit_reason"], "state_exit_strict")
        self.assertAlmostEqual(float(replay.loc[0, "gross_ret"]), -0.05)
        self.assertAlmostEqual(float(replay.loc[0, "net_ret"]), -0.051)

        replay_time = replay_state_exit_policy(
            observations,
            policy_name="strict",
            max_hold_days=1,
            round_trip_cost_bps=0.0,
        )
        self.assertEqual(replay_time.loc[0, "policy_name"], "strict_hold1")
        self.assertEqual(replay_time.loc[0, "exit_reason"], "time_exit_1")
        self.assertEqual(replay_time.loc[0, "exit_trade_date"], "20240104")
        self.assertAlmostEqual(float(replay_time.loc[0, "gross_ret"]), 0.03)

    def test_replay_fixed_tp_sl_policy_matches_threshold_exit(self) -> None:
        observations = pd.DataFrame(
            [
                {
                    "security_id": "AAA.SZ",
                    "entry_trade_date": "20240102",
                    "future_trade_date": "20240103",
                    "year": 2021,
                    "step": 1,
                    "close_ret_from_entry": 0.06,
                    "high_ret_from_entry": 0.16,
                    "low_ret_from_entry": -0.02,
                },
                {
                    "security_id": "AAA.SZ",
                    "entry_trade_date": "20240102",
                    "future_trade_date": "20240104",
                    "year": 2021,
                    "step": 2,
                    "close_ret_from_entry": 0.03,
                    "high_ret_from_entry": 0.07,
                    "low_ret_from_entry": -0.03,
                },
            ]
        )

        replay = replay_fixed_tp_sl_policy(
            observations,
            take_profit=0.15,
            stop_loss=0.10,
            max_hold_days=10,
            round_trip_cost_bps=20.0,
        )
        self.assertEqual(len(replay), 1)
        self.assertEqual(replay.loc[0, "exit_reason"], "take_profit")
        self.assertAlmostEqual(float(replay.loc[0, "gross_ret"]), 0.15)
        self.assertAlmostEqual(float(replay.loc[0, "net_ret"]), 0.148)

        summary = summarize_policy_replays(replay)
        self.assertEqual(int(summary.loc[0, "trades"]), 1)
        self.assertAlmostEqual(float(summary.loc[0, "mean_net_ret"]), 0.148)

    def test_replay_hybrid_exit_policy_uses_state_exit_before_time_cap(self) -> None:
        observations = pd.DataFrame(
            [
                {
                    "security_id": "AAA.SZ",
                    "entry_trade_date": "20240102",
                    "future_trade_date": "20240103",
                    "next_trade_date": "20240104",
                    "year": 2021,
                    "step": 1,
                    "state": "expand_down",
                    "peak_ret_so_far": 0.18,
                    "peak_profit_bucket": "10_20",
                    "drawdown_bucket": "deep_pullback",
                    "close_ret_from_entry": -0.04,
                    "next_open_ret_from_entry": -0.03,
                    "high_ret_from_entry": 0.08,
                    "low_ret_from_entry": -0.06,
                },
                {
                    "security_id": "AAA.SZ",
                    "entry_trade_date": "20240102",
                    "future_trade_date": "20240104",
                    "next_trade_date": "20240105",
                    "year": 2021,
                    "step": 2,
                    "state": "neutral",
                    "peak_ret_so_far": 0.18,
                    "peak_profit_bucket": "10_20",
                    "drawdown_bucket": "deep_pullback",
                    "close_ret_from_entry": -0.05,
                    "next_open_ret_from_entry": -0.05,
                    "high_ret_from_entry": 0.07,
                    "low_ret_from_entry": -0.07,
                },
            ]
        )

        replay = replay_hybrid_exit_policy(
            observations,
            take_profit=0.15,
            stop_loss=0.10,
            max_hold_days=10,
            state_policy_name="strict",
            round_trip_cost_bps=10.0,
        )

        self.assertEqual(len(replay), 1)
        self.assertEqual(replay.loc[0, "policy_name"], "hybrid_strict_tp0.15_sl0.10_hold10")
        self.assertEqual(replay.loc[0, "exit_reason"], "hybrid_state_exit_strict")
        self.assertEqual(replay.loc[0, "exit_trade_date"], "20240104")
        self.assertAlmostEqual(float(replay.loc[0, "gross_ret"]), -0.03)
        self.assertAlmostEqual(float(replay.loc[0, "net_ret"]), -0.031)

    def test_replay_hybrid_exit_policy_keeps_outer_frame_priority(self) -> None:
        observations = pd.DataFrame(
            [
                {
                    "security_id": "AAA.SZ",
                    "entry_trade_date": "20240102",
                    "future_trade_date": "20240103",
                    "next_trade_date": "20240104",
                    "year": 2021,
                    "step": 1,
                    "state": "expand_down",
                    "peak_ret_so_far": 0.18,
                    "peak_profit_bucket": "10_20",
                    "drawdown_bucket": "deep_pullback",
                    "close_ret_from_entry": -0.04,
                    "next_open_ret_from_entry": -0.03,
                    "high_ret_from_entry": 0.16,
                    "low_ret_from_entry": -0.02,
                },
                {
                    "security_id": "BBB.SZ",
                    "entry_trade_date": "20240102",
                    "future_trade_date": "20240103",
                    "next_trade_date": "20240104",
                    "year": 2021,
                    "step": 1,
                    "state": "expand_down",
                    "peak_ret_so_far": 0.18,
                    "peak_profit_bucket": "10_20",
                    "drawdown_bucket": "deep_pullback",
                    "close_ret_from_entry": -0.04,
                    "next_open_ret_from_entry": -0.03,
                    "high_ret_from_entry": 0.08,
                    "low_ret_from_entry": -0.06,
                },
                {
                    "security_id": "BBB.SZ",
                    "entry_trade_date": "20240102",
                    "future_trade_date": "20240104",
                    "next_trade_date": "20240105",
                    "year": 2021,
                    "step": 2,
                    "state": "expand_down",
                    "peak_ret_so_far": 0.18,
                    "peak_profit_bucket": "10_20",
                    "drawdown_bucket": "deep_pullback",
                    "close_ret_from_entry": -0.01,
                    "next_open_ret_from_entry": 0.00,
                    "high_ret_from_entry": 0.10,
                    "low_ret_from_entry": -0.02,
                },
            ]
        )

        replay = replay_hybrid_exit_policy(
            observations,
            take_profit=0.15,
            stop_loss=0.10,
            max_hold_days=1,
            state_policy_name="strict",
            round_trip_cost_bps=0.0,
        )

        aaa = replay.loc[replay["security_id"] == "AAA.SZ"].iloc[0]
        self.assertEqual(aaa["exit_reason"], "take_profit")
        self.assertEqual(aaa["exit_trade_date"], "20240103")
        self.assertAlmostEqual(float(aaa["gross_ret"]), 0.15)

        bbb = replay.loc[replay["security_id"] == "BBB.SZ"].iloc[0]
        self.assertEqual(bbb["exit_reason"], "time_exit")
        self.assertEqual(bbb["exit_trade_date"], "20240103")
        self.assertAlmostEqual(float(bbb["gross_ret"]), -0.04)

    def test_summarize_exit_state_candidates_counts_cross_year_negative_states(self) -> None:
        rows: list[dict[str, object]] = []
        for year in (2022, 2023, 2024, 2025):
            rows.append(
                {
                    "year": year,
                    "predicted_regime": "repair_retake" if year == 2024 else "trend_continuation",
                    "state": "expand_down",
                    "peak_profit_bucket": "10_20",
                    "drawdown_bucket": "mild_pullback",
                    "peak_ret_so_far": 0.15,
                    "remaining_close_ret": -0.06,
                    "future_max_ret_from_today": 0.01,
                    "future_min_ret_from_today": -0.08,
                    "future_first_hit_5": "down5_first",
                }
            )
            rows.append(
                {
                    "year": year,
                    "predicted_regime": "trend_continuation",
                    "state": "contract_up",
                    "peak_profit_bucket": "10_20",
                    "drawdown_bucket": "tight",
                    "peak_ret_so_far": 0.16,
                    "remaining_close_ret": 0.05,
                    "future_max_ret_from_today": 0.07,
                    "future_min_ret_from_today": -0.02,
                    "future_first_hit_5": "up5_first",
                }
            )

        summary = summarize_exit_state_candidates(
            pd.DataFrame(rows),
            min_peak_ret=0.10,
            min_count=1,
        )

        expand_down = summary.loc[
            (summary["state"] == "expand_down")
            & (summary["drawdown_bucket"] == "mild_pullback")
        ].iloc[0]
        self.assertEqual(int(expand_down["negative_remaining_years"]), 4)
        self.assertEqual(int(expand_down["drop_gt_rebound_years"]), 4)
        self.assertEqual(int(expand_down["downside_first_gt_upside_years"]), 4)
        self.assertAlmostEqual(float(expand_down["remaining_close_ret_mean"]), -0.06)

        contract_up = summary.loc[
            (summary["state"] == "contract_up")
            & (summary["drawdown_bucket"] == "tight")
        ].iloc[0]
        self.assertEqual(int(contract_up["negative_remaining_years"]), 0)
        self.assertEqual(int(contract_up["downside_first_gt_upside_years"]), 0)
        self.assertAlmostEqual(float(contract_up["remaining_close_ret_mean"]), 0.05)


if __name__ == "__main__":
    unittest.main()
