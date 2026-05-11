from __future__ import annotations

import math
import tempfile
import unittest
from pathlib import Path

import duckdb
import pandas as pd

from alpha_find_v2.vshape_first_break_confirmation import (
    build_confirmation_variant,
    main,
    run_first_break_confirmation_study,
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
        self.assertEqual(confirm_2d.loc[0, "first_hit"], "unresolved")
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

    def test_summarize_variant_years_handles_mixed_variant_rows(self) -> None:
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
        mixed = pd.concat([confirm_2d, confirm_3d], ignore_index=True)

        summary, density = summarize_variant_years(mixed)

        self.assertEqual(len(summary), 2)
        self.assertEqual(len(density), 2)

        summary_2d_row = summary.loc[
            (summary["variant_name"] == "confirm_2d") & (summary["year"] == 2024)
        ].iloc[0]
        summary_3d_row = summary.loc[
            (summary["variant_name"] == "confirm_3d") & (summary["year"] == 2024)
        ].iloc[0]
        density_2d_row = density.loc[
            (density["variant_name"] == "confirm_2d") & (density["year"] == 2024)
        ].iloc[0]

        self.assertEqual(summary_2d_row["confirmation_pass_rate"], 0.5)
        self.assertEqual(summary_2d_row["events"], 1)
        self.assertEqual(summary_3d_row["confirmation_pass_rate"], 0.0)
        self.assertEqual(summary_3d_row["events"], 0)
        self.assertEqual(density_2d_row["signal_days"], 1)
        self.assertEqual(density_2d_row["avg_per_day"], 1.0)


class VShapeFirstBreakConfirmationIntegrationTest(unittest.TestCase):
    def _write_source_db(self, path: Path) -> None:
        conn = duckdb.connect(str(path))
        try:
            conn.execute(
                """
                CREATE TABLE daily_bar_pit (
                    security_id VARCHAR,
                    trade_date VARCHAR,
                    exchange VARCHAR,
                    board VARCHAR,
                    is_st BOOLEAN,
                    open_adj DOUBLE,
                    high_adj DOUBLE,
                    low_adj DOUBLE,
                    close_adj DOUBLE
                )
                """
            )
            rows: list[tuple[str, str, str, str, bool, float, float, float, float]] = []
            for security_id, bars in _bars().items():
                for row in bars.to_dict(orient="records"):
                    rows.append(
                        (
                            security_id,
                            str(row["trade_date"]),
                            "SZ",
                            "main_board",
                            False,
                            float(row["open_adj"]),
                            float(row["high_adj"]),
                            float(row["low_adj"]),
                            float(row["close_adj"]),
                        )
                    )
            conn.executemany("INSERT INTO daily_bar_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        finally:
            conn.close()

    def _write_events_csv(self, path: Path) -> None:
        _events().to_csv(path, index=False)

    def test_run_study_writes_summary_density_events_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            source_db = base / "source.duckdb"
            events_csv = base / "events.csv"
            summary_csv = base / "summary.csv"
            density_csv = base / "density.csv"
            events_output_csv = base / "events_output.csv"
            report_markdown = base / "report.md"

            self._write_source_db(source_db)
            self._write_events_csv(events_csv)

            outputs = run_first_break_confirmation_study(
                events_csv_path=events_csv,
                source_db_path=source_db,
                summary_csv_path=summary_csv,
                density_csv_path=density_csv,
                events_output_csv_path=events_output_csv,
                report_markdown_path=report_markdown,
            )

            self.assertEqual(set(outputs.keys()), {"summary", "density", "events"})
            self.assertTrue(summary_csv.exists())
            self.assertTrue(density_csv.exists())
            self.assertTrue(events_output_csv.exists())
            self.assertTrue(report_markdown.exists())

            summary = pd.read_csv(summary_csv)
            self.assertEqual(
                set(summary["variant_name"].tolist()),
                {"baseline_first_break", "confirm_2d", "confirm_3d"},
            )
            self.assertIn("confirmation_pass_rate", summary.columns)

            markdown = report_markdown.read_text(encoding="utf-8")
            self.assertIn("# V Shape First Break Confirmation Study - 2026-05-12", markdown)
            self.assertIn("confirm_2d", markdown)

    def test_main_accepts_file_paths_and_exits_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            source_db = base / "source.duckdb"
            events_csv = base / "events.csv"
            summary_csv = base / "summary.csv"
            density_csv = base / "density.csv"
            events_output_csv = base / "events_output.csv"
            report_markdown = base / "report.md"

            self._write_source_db(source_db)
            self._write_events_csv(events_csv)

            exit_code = main(
                [
                    "--events-csv",
                    str(events_csv),
                    "--source-db",
                    str(source_db),
                    "--summary-csv",
                    str(summary_csv),
                    "--density-csv",
                    str(density_csv),
                    "--events-output-csv",
                    str(events_output_csv),
                    "--report-markdown",
                    str(report_markdown),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(summary_csv.exists())


if __name__ == "__main__":
    unittest.main()
