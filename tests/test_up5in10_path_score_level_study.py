from __future__ import annotations

import unittest

import pandas as pd

from alpha_find_v2.up5in10_path_score_level_study import (
    attach_trailing_price_levels,
    build_zone_summary,
    classify_range_zone,
)


class Up5In10PathScoreLevelStudyTest(unittest.TestCase):
    def test_classify_range_zone_uses_low_mid_high_buckets(self) -> None:
        self.assertEqual(classify_range_zone(0.10), "low")
        self.assertEqual(classify_range_zone(0.20), "low")
        self.assertEqual(classify_range_zone(0.50), "mid")
        self.assertEqual(classify_range_zone(0.80), "high")
        self.assertEqual(classify_range_zone(0.95), "high")

    def test_attach_trailing_price_levels_computes_range_position_per_security(self) -> None:
        bars = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240101", "close_adj": 10.0, "high_adj": 10.0, "low_adj": 10.0},
                {"security_id": "AAA.SZ", "trade_date": "20240102", "close_adj": 11.0, "high_adj": 11.0, "low_adj": 9.0},
                {"security_id": "AAA.SZ", "trade_date": "20240103", "close_adj": 9.5, "high_adj": 12.0, "low_adj": 9.0},
                {"security_id": "AAA.SZ", "trade_date": "20240104", "close_adj": 11.5, "high_adj": 12.0, "low_adj": 9.0},
                {"security_id": "BBB.SZ", "trade_date": "20240101", "close_adj": 20.0, "high_adj": 20.0, "low_adj": 18.0},
                {"security_id": "BBB.SZ", "trade_date": "20240102", "close_adj": 18.5, "high_adj": 21.0, "low_adj": 18.0},
                {"security_id": "BBB.SZ", "trade_date": "20240103", "close_adj": 18.2, "high_adj": 21.0, "low_adj": 18.0},
            ]
        )

        enriched = attach_trailing_price_levels(bars, windows=(3,))

        aaa = enriched.loc[
            (enriched["security_id"] == "AAA.SZ") & (enriched["trade_date"] == "20240104")
        ].iloc[0]
        bbb = enriched.loc[
            (enriched["security_id"] == "BBB.SZ") & (enriched["trade_date"] == "20240103")
        ].iloc[0]

        self.assertAlmostEqual(float(aaa["range_pos_3"]), 0.8333333333, places=6)
        self.assertEqual(aaa["range_zone_3"], "high")
        self.assertAlmostEqual(float(bbb["range_pos_3"]), 0.0666666667, places=6)
        self.assertEqual(bbb["range_zone_3"], "low")

    def test_build_zone_summary_aggregates_outcomes_by_window_zone_and_top_n(self) -> None:
        rows = pd.DataFrame(
            [
                {"top_n": 1, "range_zone_120": "high", "success_label": True, "close_ret30": 0.12, "max_ret30": 0.25, "min_ret30": -0.05},
                {"top_n": 1, "range_zone_120": "high", "success_label": False, "close_ret30": -0.08, "max_ret30": 0.10, "min_ret30": -0.12},
                {"top_n": 1, "range_zone_120": "low", "success_label": False, "close_ret30": -0.03, "max_ret30": 0.05, "min_ret30": -0.09},
                {"top_n": 3, "range_zone_120": "low", "success_label": True, "close_ret30": 0.06, "max_ret30": 0.15, "min_ret30": -0.04},
            ]
        )

        summary = build_zone_summary(rows, window_size=120, group_columns=("top_n",))

        high_row = summary.loc[(summary["top_n"] == 1) & (summary["range_zone"] == "high")].iloc[0]
        low_row = summary.loc[(summary["top_n"] == 1) & (summary["range_zone"] == "low")].iloc[0]

        self.assertEqual(int(high_row["selected_rows"]), 2)
        self.assertAlmostEqual(float(high_row["success_rate"]), 0.5)
        self.assertAlmostEqual(float(high_row["mean_close_ret30"]), 0.02)
        self.assertEqual(int(low_row["selected_rows"]), 1)
        self.assertAlmostEqual(float(low_row["mean_min_ret30"]), -0.09)


if __name__ == "__main__":
    unittest.main()
