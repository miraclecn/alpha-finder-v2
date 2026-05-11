from __future__ import annotations

import math
import unittest

import pandas as pd

from alpha_find_v2.vshape_first_break_confirmation import (
    build_confirmation_variant,
    summarize_variant_years,
)


def _bars() -> dict[str, pd.DataFrame]:
    return {
        "AAA.SZ": pd.DataFrame(
            [
                {"trade_date": "20240102", "open_adj": 10.0, "high_adj": 10.2, "low_adj": 9.8, "close_adj": 10.0},
                {"trade_date": "20240103", "open_adj": 10.2, "high_adj": 10.4, "low_adj": 10.2, "close_adj": 10.3},
                {"trade_date": "20240104", "open_adj": 10.3, "high_adj": 10.5, "low_adj": 10.1, "close_adj": 10.4},
                {"trade_date": "20240105", "open_adj": 10.5, "high_adj": 10.6, "low_adj": 9.9, "close_adj": 10.2},
                {"trade_date": "20240108", "open_adj": 10.4, "high_adj": 10.6, "low_adj": 10.2, "close_adj": 10.5},
                {"trade_date": "20240109", "open_adj": 10.6, "high_adj": 10.8, "low_adj": 10.4, "close_adj": 10.7},
            ]
        ),
        "BBB.SZ": pd.DataFrame(
            [
                {"trade_date": "20240102", "open_adj": 20.0, "high_adj": 20.2, "low_adj": 19.8, "close_adj": 20.0},
                {"trade_date": "20240103", "open_adj": 20.1, "high_adj": 20.2, "low_adj": 19.9, "close_adj": 20.0},
                {"trade_date": "20240104", "open_adj": 20.2, "high_adj": 20.3, "low_adj": 20.0, "close_adj": 20.1},
                {"trade_date": "20240105", "open_adj": 20.3, "high_adj": 20.4, "low_adj": 20.1, "close_adj": 20.2},
                {"trade_date": "20240108", "open_adj": 20.4, "high_adj": 20.5, "low_adj": 20.2, "close_adj": 20.3},
            ]
        ),
    }


def _events() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "security_id": "AAA.SZ",
                "signal_date": "20240102",
                "start_high": 10.0,
                "start_date": "20231228",
                "trough_date": "20240101",
                "buy_date": "20240102",
            },
            {
                "security_id": "BBB.SZ",
                "signal_date": "20240102",
                "start_high": 20.0,
                "start_date": "20231227",
                "trough_date": "20240101",
                "buy_date": "20240102",
            },
        ]
    )


class VshapeFirstBreakConfirmationTest(unittest.TestCase):
    def test_build_confirmation_variant_enforces_strict_low_rule(self) -> None:
        events = _events()
        bars_by_security = _bars()

        baseline = build_confirmation_variant(
            events,
            bars_by_security,
            variant_name="baseline",
            confirm_days=0,
        )
        confirm_2d = build_confirmation_variant(
            events,
            bars_by_security,
            variant_name="confirm_2d",
            confirm_days=2,
        )
        confirm_3d = build_confirmation_variant(
            events,
            bars_by_security,
            variant_name="confirm_3d",
            confirm_days=3,
        )

        self.assertEqual(baseline["confirmation_pass"].tolist(), [True, True])
        self.assertEqual(confirm_2d["confirmation_pass"].tolist(), [True, False])
        self.assertEqual(confirm_3d["confirmation_pass"].tolist(), [False, False])

        self.assertEqual(confirm_2d.loc[0, "candidate_entry_date"], "20240105")
        self.assertEqual(confirm_3d.loc[0, "candidate_entry_date"], "20240108")
        self.assertEqual(confirm_2d.loc[0, "entry_open"], 10.5)
        self.assertTrue(math.isnan(confirm_3d.loc[0, "entry_open"]))

    def test_summarize_variant_years_uses_candidate_rows_as_denominator(self) -> None:
        confirm_2d = build_confirmation_variant(
            _events(),
            _bars(),
            variant_name="confirm_2d",
            confirm_days=2,
        )
        confirm_3d = build_confirmation_variant(
            _events(),
            _bars(),
            variant_name="confirm_3d",
            confirm_days=3,
        )

        summary_2d, density_2d = summarize_variant_years(confirm_2d)
        summary_3d, _ = summarize_variant_years(confirm_3d)

        summary_2d_row = summary_2d.loc[summary_2d["year"] == 2024].iloc[0]
        summary_3d_row = summary_3d.loc[summary_3d["year"] == 2024].iloc[0]
        density_2d_row = density_2d.loc[density_2d["year"] == 2024].iloc[0]

        self.assertEqual(summary_2d_row["confirmation_pass_rate"], 0.5)
        self.assertEqual(summary_2d_row["events"], 1)
        self.assertEqual(summary_3d_row["confirmation_pass_rate"], 0.0)
        self.assertEqual(summary_3d_row["events"], 0)
        self.assertEqual(density_2d_row["signal_days"], 1)
        self.assertEqual(density_2d_row["avg_per_day"], 1.0)


if __name__ == "__main__":
    unittest.main()
