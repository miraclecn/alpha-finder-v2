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

    def test_summarize_variant_years_density_uses_candidate_entry_date_basis(self) -> None:
        rows = pd.DataFrame(
            [
                {
                    "variant_name": "confirm_2d",
                    "signal_date": "20241230",
                    "candidate_entry_date": "20250102",
                    "confirmation_pass": True,
                },
                {
                    "variant_name": "confirm_2d",
                    "signal_date": "20241231",
                    "candidate_entry_date": "20250102",
                    "confirmation_pass": True,
                },
                {
                    "variant_name": "confirm_2d",
                    "signal_date": "20241215",
                    "candidate_entry_date": "20241220",
                    "confirmation_pass": True,
                },
                {
                    "variant_name": "confirm_2d",
                    "signal_date": "20241216",
                    "candidate_entry_date": "20241220",
                    "confirmation_pass": False,
                },
            ]
        )

        summary, density = summarize_variant_years(rows)

        summary_row = summary.loc[(summary["variant_name"] == "confirm_2d") & (summary["year"] == 2024)].iloc[0]
        self.assertEqual(summary_row["candidate_rows"], 4)
        self.assertEqual(summary_row["events"], 3)

        self.assertEqual(set(density["year"].tolist()), {2024, 2025})
        density_2024 = density.loc[(density["variant_name"] == "confirm_2d") & (density["year"] == 2024)].iloc[0]
        density_2025 = density.loc[(density["variant_name"] == "confirm_2d") & (density["year"] == 2025)].iloc[0]

        self.assertEqual(density_2024["events"], 1)
        self.assertEqual(density_2024["signal_days"], 1)
        self.assertEqual(density_2024["avg_per_day"], 1.0)
        self.assertEqual(density_2025["events"], 2)
        self.assertEqual(density_2025["signal_days"], 1)
        self.assertEqual(density_2025["avg_per_day"], 2.0)


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

    def _write_empty_events_csv(self, path: Path) -> None:
        _events().head(0).to_csv(path, index=False)

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
            self.assertIn("## Object", markdown)
            self.assertIn("- Variants: `baseline_first_break`, `confirm_2d`, `confirm_3d`", markdown)
            self.assertIn("confirm_2d", markdown)
            self.assertIn("Judgment must be curated manually after reviewing full distribution metrics.", markdown)

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

    def test_run_study_handles_header_only_events_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            source_db = base / "source.duckdb"
            events_csv = base / "events_empty.csv"
            summary_csv = base / "summary.csv"
            density_csv = base / "density.csv"
            events_output_csv = base / "events_output.csv"
            report_markdown = base / "report.md"

            self._write_source_db(source_db)
            self._write_empty_events_csv(events_csv)

            outputs = run_first_break_confirmation_study(
                events_csv_path=events_csv,
                source_db_path=source_db,
                summary_csv_path=summary_csv,
                density_csv_path=density_csv,
                events_output_csv_path=events_output_csv,
                report_markdown_path=report_markdown,
            )

            self.assertTrue(summary_csv.exists())
            self.assertTrue(density_csv.exists())
            self.assertTrue(events_output_csv.exists())
            self.assertTrue(report_markdown.exists())

            self.assertTrue(outputs["summary"].empty)
            self.assertTrue(outputs["density"].empty)
            self.assertTrue(outputs["events"].empty)

            self.assertEqual(
                outputs["summary"].columns.tolist(),
                ["variant_name", "year", "candidate_rows", "events", "confirmation_pass_rate"],
            )
            self.assertEqual(
                outputs["density"].columns.tolist(),
                ["variant_name", "year", "events", "signal_days", "avg_per_day"],
            )
            self.assertEqual(
                outputs["events"].columns.tolist(),
                [
                    "variant_name",
                    "confirm_days",
                    "security_id",
                    "signal_date",
                    "start_high",
                    "start_date",
                    "trough_date",
                    "buy_date",
                    "candidate_entry_date",
                    "confirmation_pass",
                    "entry_open",
                    "close_ret30",
                    "max_ret30",
                    "min_ret30",
                    "up10",
                    "up20",
                    "up30",
                    "loss10",
                    "first_hit",
                ],
            )

            summary = pd.read_csv(summary_csv)
            density = pd.read_csv(density_csv)
            events = pd.read_csv(events_output_csv)
            self.assertTrue(summary.empty)
            self.assertTrue(density.empty)
            self.assertTrue(events.empty)
            self.assertEqual(summary.columns.tolist(), outputs["summary"].columns.tolist())
            self.assertEqual(density.columns.tolist(), outputs["density"].columns.tolist())
            self.assertEqual(events.columns.tolist(), outputs["events"].columns.tolist())


if __name__ == "__main__":
    unittest.main()
