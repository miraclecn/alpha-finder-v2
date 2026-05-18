from __future__ import annotations

import unittest

import pandas as pd

from alpha_find_v2.up5in10_path_score_rebound_band_study import (
    assign_position_band,
    build_filter_event_summary,
    build_filter_replay_summary,
)


class Up5In10PathScoreReboundBandStudyTest(unittest.TestCase):
    def test_assign_position_band_uses_custom_edges(self) -> None:
        self.assertEqual(assign_position_band(0.05), "0_10")
        self.assertEqual(assign_position_band(0.19), "10_20")
        self.assertEqual(assign_position_band(0.34), "20_35")
        self.assertEqual(assign_position_band(0.84), "80_100")

    def test_build_filter_event_summary_applies_single_and_dual_window_filters(self) -> None:
        rows = pd.DataFrame(
            [
                {"top_n": 1, "range_pos_120": 0.12, "range_pos_250": 0.18, "success_label": True, "close_ret30": 0.10, "max_ret30": 0.20, "min_ret30": -0.05},
                {"top_n": 1, "range_pos_120": 0.24, "range_pos_250": 0.30, "success_label": False, "close_ret30": -0.02, "max_ret30": 0.08, "min_ret30": -0.10},
                {"top_n": 1, "range_pos_120": 0.55, "range_pos_250": 0.60, "success_label": False, "close_ret30": -0.07, "max_ret30": 0.04, "min_ret30": -0.12},
            ]
        )
        filter_specs = {
            "10_35": {"range_pos_120": (0.10, 0.35)},
            "dual": {"range_pos_120": (0.10, 0.35), "range_pos_250": (0.10, 0.35)},
        }

        summary = build_filter_event_summary(rows, filter_specs=filter_specs, group_columns=("top_n",))

        simple = summary.loc[summary["filter_name"] == "10_35"].iloc[0]
        dual = summary.loc[summary["filter_name"] == "dual"].iloc[0]
        self.assertEqual(int(simple["selected_rows"]), 2)
        self.assertAlmostEqual(float(simple["success_rate"]), 0.5)
        self.assertEqual(int(dual["selected_rows"]), 2)
        self.assertAlmostEqual(float(dual["mean_close_ret30"]), 0.04)

    def test_build_filter_replay_summary_aggregates_net_returns(self) -> None:
        rows = pd.DataFrame(
            [
                {"top_n": 1, "policy_name": "fixed_tp0.05_sl0.10_hold10", "range_pos_120": 0.12, "range_pos_250": 0.18, "net_ret": 0.0476, "exit_step": 2},
                {"top_n": 1, "policy_name": "fixed_tp0.05_sl0.10_hold10", "range_pos_120": 0.24, "range_pos_250": 0.30, "net_ret": -0.1024, "exit_step": 4},
                {"top_n": 1, "policy_name": "fixed_tp0.05_sl0.10_hold10", "range_pos_120": 0.55, "range_pos_250": 0.60, "net_ret": -0.1024, "exit_step": 5},
            ]
        )
        filter_specs = {
            "10_35": {"range_pos_120": (0.10, 0.35)},
            "dual": {"range_pos_120": (0.10, 0.35), "range_pos_250": (0.10, 0.35)},
        }

        summary = build_filter_replay_summary(rows, filter_specs=filter_specs, group_columns=("top_n", "policy_name"))

        simple = summary.loc[summary["filter_name"] == "10_35"].iloc[0]
        self.assertEqual(int(simple["trades"]), 2)
        self.assertAlmostEqual(float(simple["mean_net_ret"]), -0.0274)
        self.assertAlmostEqual(float(simple["win_rate_pos"]), 0.5)


if __name__ == "__main__":
    unittest.main()
