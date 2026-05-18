from __future__ import annotations

import unittest

import pandas as pd

from alpha_find_v2.up5in10_dynamic_reentry_combo_validation import (
    build_reentry_selected_rows,
    filter_reentry_candidates_by_rule,
)


class Up5In10DynamicReentryComboValidationTest(unittest.TestCase):
    def test_filter_reentry_candidates_by_rule_applies_a_variants(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "security_id": "AAA.SZ",
                    "signal_date": "20240110",
                    "reentry_date": "20240111",
                    "source_gross_ret": 0.28,
                    "reentry_open_vs_exit_price": -0.03,
                    "signal_turnover_vs_prev5": 1.20,
                    "source_holding_days": 12,
                    "path_score": 8.0,
                },
                {
                    "security_id": "BBB.SZ",
                    "signal_date": "20240110",
                    "reentry_date": "20240111",
                    "source_gross_ret": 0.28,
                    "reentry_open_vs_exit_price": -0.03,
                    "signal_turnover_vs_prev5": 1.60,
                    "source_holding_days": 12,
                    "path_score": 7.0,
                },
                {
                    "security_id": "CCC.SZ",
                    "signal_date": "20240110",
                    "reentry_date": "20240111",
                    "source_gross_ret": 0.28,
                    "reentry_open_vs_exit_price": -0.01,
                    "signal_turnover_vs_prev5": 1.20,
                    "source_holding_days": 12,
                    "path_score": 6.0,
                },
                {
                    "security_id": "DDD.SZ",
                    "signal_date": "20240110",
                    "reentry_date": "20240111",
                    "source_gross_ret": 0.18,
                    "reentry_open_vs_exit_price": -0.03,
                    "signal_turnover_vs_prev5": 1.20,
                    "source_holding_days": 12,
                    "path_score": 5.0,
                },
                {
                    "security_id": "EEE.SZ",
                    "signal_date": "20240110",
                    "reentry_date": "20240111",
                    "source_gross_ret": 0.40,
                    "reentry_open_vs_exit_price": -0.03,
                    "signal_turnover_vs_prev5": 1.20,
                    "source_holding_days": 12,
                    "path_score": 4.0,
                },
                {
                    "security_id": "FFF.SZ",
                    "signal_date": "20240110",
                    "reentry_date": "20240111",
                    "source_gross_ret": 0.28,
                    "reentry_open_vs_exit_price": -0.03,
                    "signal_turnover_vs_prev5": 1.20,
                    "source_holding_days": 8,
                    "path_score": 3.0,
                },
            ]
        )

        self.assertEqual(
            filter_reentry_candidates_by_rule(candidates, "A")["security_id"].tolist(),
            ["AAA.SZ", "BBB.SZ", "CCC.SZ", "EEE.SZ", "FFF.SZ"],
        )
        self.assertEqual(
            filter_reentry_candidates_by_rule(candidates, "A1")["security_id"].tolist(),
            ["AAA.SZ", "CCC.SZ", "EEE.SZ", "FFF.SZ"],
        )
        self.assertEqual(
            filter_reentry_candidates_by_rule(candidates, "A2")["security_id"].tolist(),
            ["AAA.SZ", "CCC.SZ", "EEE.SZ"],
        )
        self.assertEqual(
            filter_reentry_candidates_by_rule(candidates, "A3")["security_id"].tolist(),
            ["AAA.SZ", "CCC.SZ", "FFF.SZ"],
        )
        self.assertEqual(
            filter_reentry_candidates_by_rule(candidates, "A4")["security_id"].tolist(),
            ["AAA.SZ", "EEE.SZ", "FFF.SZ"],
        )

    def test_build_reentry_selected_rows_uses_signal_date_priority_and_fraction(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "security_id": "AAA.SZ",
                    "signal_date": "20240110",
                    "reentry_date": "20240111",
                    "source_gross_ret": 0.28,
                    "reentry_open_vs_exit_price": -0.03,
                    "signal_turnover_vs_prev5": 1.20,
                    "source_holding_days": 12,
                    "path_score": 8.0,
                }
            ]
        )

        selected = build_reentry_selected_rows(
            candidates,
            rule_name="A2",
            target_position_fraction=0.05,
            signal_priority=20.0,
        )

        self.assertEqual(selected.loc[0, "trade_date"], "20240110")
        self.assertEqual(selected.loc[0, "year"], 2024)
        self.assertEqual(selected.loc[0, "signal_origin"], "reentry_A2")
        self.assertAlmostEqual(float(selected.loc[0, "target_position_fraction"]), 0.05)
        self.assertAlmostEqual(float(selected.loc[0, "signal_priority"]), 20.0)
        self.assertAlmostEqual(float(selected.loc[0, "path_score"]), 8.0)


if __name__ == "__main__":
    unittest.main()
