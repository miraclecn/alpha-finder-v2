from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb
import pandas as pd

from alpha_find_v2.up5in10_path_score_study import (
    PATH_SCORE_FEATURES,
    apply_path_score,
    build_threshold_summary,
    fit_path_score_spec,
    run_up5in10_path_score_study,
)


def _synthetic_event_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "security_id": "AAA.SZ",
                "event_trade_date": "20240110",
                "event_year": 2024,
                "event_board": "main_board",
                "hit5": True,
                "mean_turnover10": 1.9,
                "contract_flat_rate10": 0.00,
                "expand_up_persist": 4.0,
                "down_to_up": 2.0,
            },
            {
                "security_id": "BBB.SZ",
                "event_trade_date": "20240110",
                "event_year": 2024,
                "event_board": "main_board",
                "hit5": True,
                "mean_turnover10": 1.5,
                "contract_flat_rate10": 0.03,
                "expand_up_persist": 2.0,
                "down_to_up": 1.0,
            },
            {
                "security_id": "CCC.SZ",
                "event_trade_date": "20240110",
                "event_year": 2024,
                "event_board": "main_board",
                "hit5": False,
                "mean_turnover10": 0.9,
                "contract_flat_rate10": 0.12,
                "expand_up_persist": 0.0,
                "down_to_up": 0.0,
            },
        ]
    )


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
        dates = pd.date_range("2022-01-03", periods=110, freq="B")
        entry_start = dates[65].strftime("%Y%m%d")
        entry_end = dates[95].strftime("%Y%m%d")
        bar_rows: list[tuple[object, ...]] = []
        trade_rows: list[tuple[object, ...]] = []
        specs = [
            ("AAA.SZ", 10.0, True),
            ("BBB.SZ", 9.0, False),
            ("CCC.SZ", 8.0, True),
        ]
        for security_id, start_close, success in specs:
            close_value = start_close
            for idx, day in enumerate(dates):
                trade_date = day.strftime("%Y%m%d")
                if idx > 0:
                    daily_ret = 0.001
                    if idx % 8 == 0:
                        daily_ret = -0.002
                    if success and idx % 9 in {6, 7}:
                        daily_ret = 0.018
                    if (not success) and idx % 9 in {6, 7}:
                        daily_ret = 0.0005
                    close_value *= 1.0 + daily_ret

                turnover = 90_000_000.0 + (idx % 6) * 6_000_000.0
                if success and idx % 9 in {6, 7}:
                    turnover = 240_000_000.0
                high_adj = close_value * 1.01
                if success and idx % 15 == 10:
                    high_adj = close_value * 1.08
                if (not success) and idx % 15 == 10:
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
    return entry_start, entry_end


class Up5In10PathScoreStudyTest(unittest.TestCase):
    def test_fit_and_apply_path_score_favors_activation_over_sleep(self) -> None:
        events = _synthetic_event_rows()

        spec = fit_path_score_spec(events)
        scored = apply_path_score(events, spec)

        self.assertEqual([row["feature"] for row in spec], PATH_SCORE_FEATURES)
        ordered = scored.sort_values("path_score", ascending=False)["security_id"].tolist()
        self.assertEqual(ordered, ["AAA.SZ", "BBB.SZ", "CCC.SZ"])

    def test_threshold_summary_reports_coverage_and_lift(self) -> None:
        events = _synthetic_event_rows()
        spec = fit_path_score_spec(events)
        scored = apply_path_score(events, spec)

        thresholds = [
            {"threshold_name": "all", "score_threshold": -999.0},
            {"threshold_name": "topish", "score_threshold": float(scored["path_score"].median())},
        ]
        summary = build_threshold_summary(
            scored,
            thresholds=thresholds,
            split_name="unit",
            scope="main_board",
            test_year="2024",
        )

        all_row = summary.loc[summary["threshold_name"] == "all"].reset_index(drop=True)
        top_row = summary.loc[summary["threshold_name"] == "topish"].reset_index(drop=True)
        self.assertEqual(int(all_row.loc[0, "selected_events"]), 3)
        self.assertAlmostEqual(float(all_row.loc[0, "coverage"]), 1.0)
        self.assertGreater(float(top_row.loc[0, "success_rate"]), float(all_row.loc[0, "success_rate"]))

    def test_run_path_score_study_writes_threshold_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source_db = root / "source.duckdb"
            entry_start, entry_end = _write_source_db(source_db)

            result = run_up5in10_path_score_study(
                source_db_path=source_db,
                event_feature_csv_path=root / "event_features.csv",
                threshold_summary_csv_path=root / "threshold_summary.csv",
                quintile_summary_csv_path=root / "quintile_summary.csv",
                oos_summary_csv_path=root / "oos_summary.csv",
                report_markdown_path=root / "report.md",
                entry_start=entry_start,
                entry_end=entry_end,
                query_start="20220101",
                query_end="20221231",
                train_years=(2022,),
                extra_test_years=(2022,),
            )

            self.assertFalse(result["event_features"].empty)
            self.assertFalse(result["threshold_summary"].empty)
            self.assertFalse(result["quintile_summary"].empty)
            self.assertFalse(result["oos_summary"].empty)
            self.assertTrue((root / "threshold_summary.csv").exists())
            report_text = (root / "report.md").read_text(encoding="utf-8")
            self.assertIn("Path Score Threshold Study", report_text)
