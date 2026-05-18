from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb
import pandas as pd

from alpha_find_v2.up5in10_price_volume_study import run_up5in10_price_volume_study


def _write_source_db(path: Path) -> str:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE daily_bar_pit (
                security_id VARCHAR,
                trade_date VARCHAR,
                board VARCHAR,
                is_st BOOLEAN,
                price_basis VARCHAR,
                close_adj DOUBLE,
                high_adj DOUBLE,
                turnover_value_cny DOUBLE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE tradeability_state_daily (
                security_id VARCHAR,
                trade_date VARCHAR,
                is_suspended BOOLEAN
            )
            """
        )

        dates = pd.date_range("2022-01-03", periods=90, freq="B")
        event_date = dates[70].strftime("%Y%m%d")
        bar_rows: list[tuple[object, ...]] = []
        trade_rows: list[tuple[object, ...]] = []

        specs = [
            ("AAA.SZ", 10.0, True),
            ("BBB.SZ", 8.0, False),
        ]
        for security_id, start_close, success in specs:
            close_value = start_close
            for idx, day in enumerate(dates):
                trade_date = day.strftime("%Y%m%d")
                if idx > 0:
                    daily_ret = 0.002
                    if idx % 7 == 0:
                        daily_ret = -0.003
                    if success and idx in {68, 69}:
                        daily_ret = 0.02
                    if (not success) and idx in {68, 69}:
                        daily_ret = 0.001
                    close_value *= 1.0 + daily_ret

                turnover = 100_000_000.0 + (idx % 5) * 5_000_000.0
                if success and idx in {68, 69}:
                    turnover = 260_000_000.0
                high_adj = close_value * 1.01
                if success and idx == 73:
                    high_adj = close_value * 1.07
                if (not success) and idx == 73:
                    high_adj = close_value * 1.03

                bar_rows.append(
                    (
                        security_id,
                        trade_date,
                        "main_board",
                        False,
                        "unadjusted",
                        close_value,
                        high_adj,
                        turnover,
                    )
                )
                trade_rows.append((security_id, trade_date, False))

        conn.executemany("INSERT INTO daily_bar_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?)", bar_rows)
        conn.executemany("INSERT INTO tradeability_state_daily VALUES (?, ?, ?)", trade_rows)
    finally:
        conn.close()
    return event_date


class Up5In10PriceVolumeStudyTest(unittest.TestCase):
    def test_run_up5in10_study_emits_expected_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source_db = root / "source.duckdb"
            event_date = _write_source_db(source_db)

            result = run_up5in10_price_volume_study(
                source_db_path=source_db,
                event_summary_csv_path=root / "event_summary.csv",
                daily_profile_csv_path=root / "daily_profile.csv",
                transition_profile_csv_path=root / "transition_profile.csv",
                sequence_summary_csv_path=root / "sequence_summary.csv",
                report_markdown_path=root / "report.md",
                entry_start=event_date,
                entry_end=event_date,
                query_start="20220101",
                query_end="20221231",
            )

            event_summary = result["event_summary"]
            daily_profile = result["daily_profile"]
            transition_profile = result["transition_profile"]
            sequence_summary = result["sequence_summary"]

            self.assertFalse(event_summary.empty)
            self.assertFalse(daily_profile.empty)
            self.assertFalse(transition_profile.empty)
            self.assertFalse(sequence_summary.empty)

            all_summary = event_summary.loc[
                (event_summary["scope"] == "all") & (event_summary["year_group"] == "all")
            ].reset_index(drop=True)
            self.assertEqual(int(all_summary.loc[0, "candidate_events"]), 2)
            self.assertEqual(int(all_summary.loc[0, "success_events"]), 1)

            success_profile = daily_profile.loc[
                (daily_profile["scope"] == "all") & (daily_profile["sample"] == "success")
            ].reset_index(drop=True)
            self.assertEqual(success_profile["relative_day"].tolist(), list(range(-30, 0)))

            success_transitions = transition_profile.loc[
                (transition_profile["scope"] == "all") & (transition_profile["sample"] == "success")
            ].reset_index(drop=True)
            self.assertEqual(int(success_transitions["start_relative_day"].min()), -30)
            self.assertEqual(int(success_transitions["start_relative_day"].max()), -2)

            success_sequence = sequence_summary.loc[
                (sequence_summary["scope"] == "all") & (sequence_summary["sample"] == "success")
            ].reset_index(drop=True)
            self.assertEqual(int(success_sequence.loc[0, "events"]), 1)
            self.assertAlmostEqual(float(success_sequence.loc[0, "expand_up_streak2_rate"]), 1.0)

            self.assertTrue((root / "event_summary.csv").exists())
            self.assertTrue((root / "daily_profile.csv").exists())
            self.assertTrue((root / "transition_profile.csv").exists())
            self.assertTrue((root / "sequence_summary.csv").exists())
            report_text = (root / "report.md").read_text(encoding="utf-8")
            self.assertIn("10-Day +5% Price-Volume Study", report_text)
