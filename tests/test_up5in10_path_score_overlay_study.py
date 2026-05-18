from __future__ import annotations

import unittest

import pandas as pd

from alpha_find_v2.up5in10_path_score_overlay_study import (
    apply_thresholds_to_selected_rows,
    summarize_filtered_selection,
)


class Up5In10PathScoreOverlayStudyTest(unittest.TestCase):
    def test_apply_thresholds_keeps_only_rows_above_named_cutoffs(self) -> None:
        selected_rows = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "year": 2024, "path_score": 2.0},
                {"security_id": "BBB.SZ", "trade_date": "20240102", "year": 2024, "path_score": 0.5},
                {"security_id": "CCC.SZ", "trade_date": "20240103", "year": 2024, "path_score": -0.2},
            ]
        )
        threshold_rows = pd.DataFrame(
            [
                {"scope": "main_board", "split_name": "unit", "test_year": "2024", "threshold_name": "p80", "score_threshold": 1.0},
                {"scope": "main_board", "split_name": "unit", "test_year": "2024", "threshold_name": "p90", "score_threshold": 1.8},
            ]
        )

        filtered = apply_thresholds_to_selected_rows(
            selected_rows,
            threshold_rows=threshold_rows,
            threshold_names=("p80", "p90"),
            scope="main_board",
        )

        p80 = filtered.loc[filtered["threshold_name"] == "p80"].reset_index(drop=True)
        p90 = filtered.loc[filtered["threshold_name"] == "p90"].reset_index(drop=True)
        self.assertEqual(p80["security_id"].tolist(), ["AAA.SZ"])
        self.assertEqual(p90["security_id"].tolist(), ["AAA.SZ"])

    def test_summarize_filtered_selection_reports_coverage_and_quality(self) -> None:
        base_rows = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "year": 2024, "success_label": True, "close_ret30": 0.10, "max_ret30": 0.20, "min_ret30": -0.05},
                {"security_id": "BBB.SZ", "trade_date": "20240102", "year": 2024, "success_label": False, "close_ret30": -0.02, "max_ret30": 0.03, "min_ret30": -0.12},
                {"security_id": "CCC.SZ", "trade_date": "20240103", "year": 2024, "success_label": True, "close_ret30": 0.12, "max_ret30": 0.25, "min_ret30": -0.04},
            ]
        )
        filtered_rows = pd.DataFrame(
            [
                {"threshold_name": "p80", "security_id": "AAA.SZ", "trade_date": "20240102", "year": 2024, "success_label": True, "close_ret30": 0.10, "max_ret30": 0.20, "min_ret30": -0.05},
                {"threshold_name": "p80", "security_id": "CCC.SZ", "trade_date": "20240103", "year": 2024, "success_label": True, "close_ret30": 0.12, "max_ret30": 0.25, "min_ret30": -0.04},
            ]
        )

        summary = summarize_filtered_selection(
            base_selected_rows=base_rows,
            filtered_selected_rows=filtered_rows,
            scope="main_board",
            split_name="unit",
        )

        row = summary.iloc[0]
        self.assertEqual(row["threshold_name"], "p80")
        self.assertEqual(int(row["base_selected_rows"]), 3)
        self.assertEqual(int(row["filtered_selected_rows"]), 2)
        self.assertAlmostEqual(float(row["coverage_vs_base"]), 2 / 3)
        self.assertAlmostEqual(float(row["filtered_success_rate"]), 1.0)
