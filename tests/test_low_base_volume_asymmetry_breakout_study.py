from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb
import pandas as pd

from alpha_find_v2.low_base_volume_asymmetry_breakout_study import (
    run_low_base_volume_asymmetry_breakout_study,
)


def _build_security_rows(
    *,
    security_id: str,
    board: str,
    current_name: str,
    has_shadow_signal: bool,
) -> tuple[list[tuple[object, ...]], list[tuple[str, str, bool]], tuple[str, str]]:
    dates = pd.date_range("2023-01-02", periods=420, freq="B")
    event_idx = 280
    breakout_date = dates[event_idx].strftime("%Y%m%d")
    entry_date = dates[event_idx + 1].strftime("%Y%m%d")

    base_pattern = [
        10.1,
        10.3,
        11.7,
        11.9,
        10.0,
        10.2,
        11.8,
        11.9,
        10.1,
        10.4,
        11.6,
        11.8,
        10.2,
        10.3,
        11.7,
        11.9,
        10.0,
        10.4,
        11.6,
        11.8,
    ]

    rows: list[tuple[object, ...]] = []
    trade_rows: list[tuple[str, str, bool]] = []
    shadow_signal_date = dates[event_idx - 4].strftime("%Y%m%d")

    close_value = 20.0
    for idx, trade_day in enumerate(dates):
        trade_date = trade_day.strftime("%Y%m%d")
        if idx < event_idx - 20:
            close_value = max(8.6, 20.0 - idx * 0.06)
            turnover = 100_000_000.0
        elif idx < event_idx:
            close_value = base_pattern[idx - (event_idx - 20)]
            if close_value <= 10.4:
                turnover = 280_000_000.0
            else:
                turnover = 90_000_000.0
        elif idx == event_idx:
            close_value = 12.4
            turnover = 160_000_000.0
        else:
            close_value = close_value * 1.006
            turnover = 150_000_000.0

        open_adj = close_value * 0.995
        high_adj = close_value * 1.02
        low_adj = close_value * 0.98
        if trade_date == shadow_signal_date and has_shadow_signal:
            open_adj = close_value * 0.99
            high_adj = close_value * 1.01
            low_adj = close_value * 0.86
        if idx == event_idx + 35:
            high_adj = close_value * 1.28

        rows.append(
            (
                security_id,
                trade_date,
                board,
                False,
                "unadjusted",
                open_adj,
                high_adj,
                low_adj,
                close_value,
                turnover,
            )
        )
        trade_rows.append((security_id, trade_date, False))

    return rows, trade_rows, (breakout_date, entry_date)


def _write_source_db(path: Path) -> tuple[str, str]:
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
                open_adj DOUBLE,
                high_adj DOUBLE,
                low_adj DOUBLE,
                close_adj DOUBLE,
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
        conn.execute(
            """
            CREATE TABLE security_master_ref (
                security_id VARCHAR,
                symbol VARCHAR,
                current_name VARCHAR,
                exchange VARCHAR,
                board VARCHAR,
                area VARCHAR,
                list_date VARCHAR,
                delist_date VARCHAR,
                is_hs VARCHAR,
                is_a_share BOOLEAN,
                ingested_at TIMESTAMP
            )
            """
        )

        all_bar_rows: list[tuple[object, ...]] = []
        all_trade_rows: list[tuple[str, str, bool]] = []
        names = [
            ("AAA.SZ", "Alpha Range", True),
            ("BBB.SZ", "Beta No Shadow", False),
        ]
        breakout_date = ""
        entry_date = ""
        for security_id, current_name, has_shadow_signal in names:
            bar_rows, trade_rows, dates = _build_security_rows(
                security_id=security_id,
                board="main_board",
                current_name=current_name,
                has_shadow_signal=has_shadow_signal,
            )
            all_bar_rows.extend(bar_rows)
            all_trade_rows.extend(trade_rows)
            if has_shadow_signal:
                breakout_date, entry_date = dates
            conn.execute(
                """
                INSERT INTO security_master_ref
                VALUES (?, ?, ?, 'SZ', 'main_board', 'CN', '20200101', NULL, 'N', TRUE, current_timestamp)
                """,
                [security_id, security_id.split(".")[0], current_name],
            )

        conn.executemany("INSERT INTO daily_bar_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", all_bar_rows)
        conn.executemany("INSERT INTO tradeability_state_daily VALUES (?, ?, ?)", all_trade_rows)
    finally:
        conn.close()

    return breakout_date, entry_date


class LowBaseVolumeAsymmetryBreakoutStudyTest(unittest.TestCase):
    def test_run_study_filters_for_low_base_volume_asymmetry_and_emits_forward_stats(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source_db = root / "source.duckdb"
            breakout_date, entry_date = _write_source_db(source_db)

            result = run_low_base_volume_asymmetry_breakout_study(
                source_db_path=source_db,
                events_csv_path=root / "events.csv",
                event_summary_csv_path=root / "event_summary.csv",
                forward_summary_csv_path=root / "forward_summary.csv",
                report_markdown_path=root / "report.md",
                entry_start=breakout_date,
                entry_end=breakout_date,
                query_start="20230101",
                query_end="20251231",
            )

            events = result["events"]
            event_summary = result["event_summary"]
            forward_summary = result["forward_summary"]

            self.assertEqual(events["security_id"].tolist(), ["AAA.SZ"])
            self.assertEqual(events.loc[0, "current_name"], "Alpha Range")
            self.assertEqual(events.loc[0, "breakout_date"], breakout_date)
            self.assertEqual(events.loc[0, "entry_date"], entry_date)
            self.assertEqual(int(events.loc[0, "base_days"]), 20)
            self.assertGreater(float(events.loc[0, "volume_asymmetry_ratio"]), 2.0)
            self.assertEqual(int(events.loc[0, "shadow_signal_days"]), 1)
            self.assertEqual(int(events.loc[0, "last_shadow_gap_days"]), 4)
            self.assertGreater(float(events.loc[0, "close_ret120"]), 0.15)
            self.assertEqual(events.loc[0, "first_hit"], "up20_first")

            overall_events = event_summary.loc[
                (event_summary["scope"] == "all") & (event_summary["year_group"] == "all")
            ].reset_index(drop=True)
            self.assertEqual(int(overall_events.loc[0, "events"]), 1)
            self.assertEqual(int(overall_events.loc[0, "unique_stocks"]), 1)

            overall_forward = forward_summary.loc[
                (forward_summary["scope"] == "all") & (forward_summary["year_group"] == "all")
            ].reset_index(drop=True)
            self.assertAlmostEqual(float(overall_forward.loc[0, "up20_rate"]), 1.0)
            self.assertAlmostEqual(float(overall_forward.loc[0, "loss10_rate"]), 0.0)

            self.assertTrue((root / "events.csv").exists())
            self.assertTrue((root / "event_summary.csv").exists())
            self.assertTrue((root / "forward_summary.csv").exists())
            report_text = (root / "report.md").read_text(encoding="utf-8")
            self.assertIn("Low-Base Volume-Asymmetry Breakout Study", report_text)
            self.assertIn("Alpha Range", report_text)
