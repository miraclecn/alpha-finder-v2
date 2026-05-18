# V Shape First Break Confirmation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible event-level study that compares the existing first-break baseline against `confirm_2d` and `confirm_3d` entry rules without changing the underlying V-shape detector.

**Architecture:** Add a standalone research module under `src/alpha_find_v2/` that loads an existing `first_break` CSV, reads forward-adjusted bars from `daily_bar_pit`, emits per-event rows for baseline and confirmation variants, and writes summary, density, and markdown-report outputs. Keep `src/alpha_find_v2/cli.py` untouched so the experiment remains outside the product CLI surface.

**Tech Stack:** Python 3.11, pandas, duckdb, unittest, markdown docs

---

## File Structure

- Create: `src/alpha_find_v2/vshape_first_break_confirmation.py`
  - Responsibility: load the source event CSV, load per-security bar history from `daily_bar_pit`, build baseline / `confirm_2d` / `confirm_3d` event rows, compute 30-trading-day forward stats, and write CSV/markdown outputs through a `python -m alpha_find_v2.vshape_first_break_confirmation` entrypoint.
- Create: `tests/test_vshape_first_break_confirmation.py`
  - Responsibility: synthetic regression coverage for strict `low > start_high` confirmation rules, candidate-entry-date timing, summary pass-rate math, CSV writers, and markdown report generation.
- Create: `docs/research/vshape-first-break-confirmation-2026-05-12.md`
  - Responsibility: persisted study note generated from the real run against `/tmp/vshape_first_break_events.csv` and `output/research_source.duckdb`.

The implementation deliberately avoids:

- `src/alpha_find_v2/cli.py`
- `src/alpha_find_v2/trend_research_input_builder.py`
- any V-shape detector rewrite

The only runtime prerequisite outside the repo is the existing event artifact:

- input events: `/tmp/vshape_first_break_events.csv`
- source DB: `output/research_source.duckdb`

Non-negotiable semantic rule for every task below:

- confirmation passes only when every observation-day low satisfies `low > start_high`
- any observation-day `low <= start_high` is an immediate failure

### Task 1: Lock The Confirmation-Window Semantics With Unit Tests

**Files:**
- Create: `tests/test_vshape_first_break_confirmation.py`
- Create: `src/alpha_find_v2/vshape_first_break_confirmation.py`
- Test: `tests/test_vshape_first_break_confirmation.py`

- [ ] **Step 1: Write the failing test file for confirmation timing and summary math**

```python
from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import pandas as pd

from alpha_find_v2.vshape_first_break_confirmation import (
    build_confirmation_variant,
    summarize_variant_years,
)


def _bars() -> dict[str, pd.DataFrame]:
    return {
        "000001.SZ": pd.DataFrame(
            [
                {"trade_date": "20240102", "open_adj": 10.2, "high_adj": 10.7, "low_adj": 10.1, "close_adj": 10.6},
                {"trade_date": "20240103", "open_adj": 10.3, "high_adj": 10.8, "low_adj": 10.1, "close_adj": 10.7},
                {"trade_date": "20240104", "open_adj": 10.4, "high_adj": 10.9, "low_adj": 10.05, "close_adj": 10.8},
                {"trade_date": "20240105", "open_adj": 10.5, "high_adj": 11.1, "low_adj": 9.95, "close_adj": 10.7},
                {"trade_date": "20240108", "open_adj": 10.7, "high_adj": 11.6, "low_adj": 10.4, "close_adj": 11.3},
                {"trade_date": "20240109", "open_adj": 11.2, "high_adj": 12.4, "low_adj": 10.9, "close_adj": 12.1},
            ]
        ),
        "000002.SZ": pd.DataFrame(
            [
                {"trade_date": "20240102", "open_adj": 20.2, "high_adj": 20.6, "low_adj": 19.8, "close_adj": 20.3},
                {"trade_date": "20240103", "open_adj": 20.1, "high_adj": 20.4, "low_adj": 19.9, "close_adj": 20.0},
                {"trade_date": "20240104", "open_adj": 19.8, "high_adj": 20.1, "low_adj": 19.7, "close_adj": 19.9},
                {"trade_date": "20240105", "open_adj": 19.7, "high_adj": 20.0, "low_adj": 19.6, "close_adj": 19.8},
            ]
        ),
    }


def _events() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "security_id": "000001.SZ",
                "signal_date": "20240102",
                "start_high": 10.0,
                "start_date": "20231220",
                "trough_date": "20231227",
                "buy_date": "20240103",
            },
            {
                "security_id": "000002.SZ",
                "signal_date": "20240102",
                "start_high": 20.0,
                "start_date": "20231221",
                "trough_date": "20231228",
                "buy_date": "20240103",
            },
        ]
    )


class VShapeFirstBreakConfirmationTest(unittest.TestCase):
    def test_build_confirmation_variant_enforces_strict_low_rule(self) -> None:
        events = _events()
        bars = _bars()

        baseline = build_confirmation_variant(events, bars, variant_name="baseline_first_break", confirm_days=0)
        confirm_2d = build_confirmation_variant(events, bars, variant_name="confirm_2d", confirm_days=2)
        confirm_3d = build_confirmation_variant(events, bars, variant_name="confirm_3d", confirm_days=3)

        self.assertEqual(baseline["confirmation_passed"].tolist(), [True, True])
        self.assertEqual(confirm_2d["confirmation_passed"].tolist(), [True, False])
        self.assertEqual(confirm_3d["confirmation_passed"].tolist(), [False, False])
        self.assertEqual(confirm_2d.loc[0, "candidate_entry_date"], "20240105")
        self.assertEqual(confirm_3d.loc[0, "candidate_entry_date"], "20240108")
        self.assertAlmostEqual(confirm_2d.loc[0, "entry_open"], 10.5)
        self.assertTrue(pd.isna(confirm_3d.loc[0, "entry_open"]))

    def test_summarize_variant_years_uses_candidate_rows_as_denominator(self) -> None:
        events = _events()
        bars = _bars()
        combined = pd.concat(
            [
                build_confirmation_variant(events, bars, variant_name="baseline_first_break", confirm_days=0),
                build_confirmation_variant(events, bars, variant_name="confirm_2d", confirm_days=2),
                build_confirmation_variant(events, bars, variant_name="confirm_3d", confirm_days=3),
            ],
            ignore_index=True,
        )

        summary, density = summarize_variant_years(combined)

        confirm_2d_2024 = summary[(summary["variant"] == "confirm_2d") & (summary["year"] == 2024)].iloc[0]
        confirm_3d_2024 = summary[(summary["variant"] == "confirm_3d") & (summary["year"] == 2024)].iloc[0]
        density_2d = density[(density["variant"] == "confirm_2d") & (density["year"] == 2024)].iloc[0]

        self.assertAlmostEqual(confirm_2d_2024["confirmation_pass_rate"], 0.5)
        self.assertEqual(int(confirm_2d_2024["events"]), 1)
        self.assertAlmostEqual(confirm_3d_2024["confirmation_pass_rate"], 0.0)
        self.assertEqual(int(confirm_3d_2024["events"]), 0)
        self.assertEqual(int(density_2d["signal_days"]), 1)
        self.assertAlmostEqual(density_2d["avg_per_day"], 1.0)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the new test file and verify it fails because the module does not exist yet**

Run: `PYTHONPATH=src python3 -m unittest tests.test_vshape_first_break_confirmation -v`

Expected: `FAILED` with `ModuleNotFoundError: No module named 'alpha_find_v2.vshape_first_break_confirmation'`

- [ ] **Step 3: Write the minimal module that makes the unit tests pass**

```python
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


SUMMARY_COLUMNS = [
    "variant",
    "year",
    "events",
    "candidate_rows",
    "confirmation_pass_rate",
    "avg_close_ret30",
    "median_close_ret30",
    "avg_max_ret30",
    "avg_min_ret30",
    "up10_rate",
    "up20_rate",
    "up30_rate",
    "loss10_rate",
    "up10_first",
    "dn10_first",
    "unresolved",
]


def _future_window(bar_frame: pd.DataFrame, entry_index: int, horizon: int = 30) -> pd.DataFrame:
    return bar_frame.iloc[entry_index : entry_index + horizon].reset_index(drop=True)


def _first_hit_state(future: pd.DataFrame, entry_open: float) -> str:
    for row in future.itertuples(index=False):
        hit_up = float(row.high_adj) >= entry_open * 1.10
        hit_dn = float(row.low_adj) <= entry_open * 0.90
        if hit_up and hit_dn:
            return "both_same_day"
        if hit_up:
            return "up10_first"
        if hit_dn:
            return "dn10_first"
    return "unresolved"


def build_confirmation_variant(
    events: pd.DataFrame,
    bars_by_security: dict[str, pd.DataFrame],
    *,
    variant_name: str,
    confirm_days: int,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for event in events.itertuples(index=False):
        bars = bars_by_security[event.security_id].reset_index(drop=True)
        dates = bars["trade_date"].astype(str).tolist()
        signal_index = dates.index(str(event.signal_date))
        window_start = signal_index + 1
        window_end = window_start + confirm_days
        candidate_entry_index = signal_index + confirm_days + 1
        candidate_entry_date = (
            str(bars.iloc[candidate_entry_index]["trade_date"])
            if candidate_entry_index < len(bars)
            else None
        )

        passed = True
        if confirm_days > 0:
            if window_end > len(bars):
                passed = False
            else:
                lows = bars.iloc[window_start:window_end]["low_adj"].astype(float)
                passed = bool((lows > float(event.start_high)).all())

        entry_open = None
        close_ret30 = None
        max_ret30 = None
        min_ret30 = None
        up10 = None
        up20 = None
        up30 = None
        loss10 = None
        first_hit = None
        if passed and candidate_entry_date is not None:
            entry_row = bars.iloc[candidate_entry_index]
            entry_open = float(entry_row["open_adj"])
            future = _future_window(bars, candidate_entry_index)
            max_high = float(future["high_adj"].max())
            min_low = float(future["low_adj"].min())
            close30 = float(future.iloc[-1]["close_adj"])
            close_ret30 = close30 / entry_open - 1.0
            max_ret30 = max_high / entry_open - 1.0
            min_ret30 = min_low / entry_open - 1.0
            up10 = float(max_high >= entry_open * 1.10)
            up20 = float(max_high >= entry_open * 1.20)
            up30 = float(max_high >= entry_open * 1.30)
            loss10 = float(min_low <= entry_open * 0.90)
            first_hit = _first_hit_state(future, entry_open)
        else:
            passed = False

        rows.append(
            {
                "variant": variant_name,
                "confirm_days": confirm_days,
                "security_id": event.security_id,
                "signal_date": str(event.signal_date),
                "year": int(str(event.signal_date)[:4]),
                "start_high": float(event.start_high),
                "candidate_entry_date": candidate_entry_date,
                "confirmation_passed": passed,
                "entry_open": entry_open,
                "close_ret30": close_ret30,
                "max_ret30": max_ret30,
                "min_ret30": min_ret30,
                "up10": up10,
                "up20": up20,
                "up30": up30,
                "loss10": loss10,
                "first_hit": first_hit,
            }
        )
    return pd.DataFrame(rows)


def summarize_variant_years(variant_rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_rows: list[dict[str, object]] = []
    density_rows: list[dict[str, object]] = []
    for (variant, year), group in variant_rows.groupby(["variant", "year"], dropna=False):
        passed = group[group["confirmation_passed"]].copy()
        summary_rows.append(
            {
                "variant": variant,
                "year": int(year),
                "events": int(len(passed)),
                "candidate_rows": int(len(group)),
                "confirmation_pass_rate": float(len(passed) / len(group)) if len(group) else 0.0,
                "avg_close_ret30": float(passed["close_ret30"].mean()) if len(passed) else 0.0,
                "median_close_ret30": float(passed["close_ret30"].median()) if len(passed) else 0.0,
                "avg_max_ret30": float(passed["max_ret30"].mean()) if len(passed) else 0.0,
                "avg_min_ret30": float(passed["min_ret30"].mean()) if len(passed) else 0.0,
                "up10_rate": float(passed["up10"].mean()) if len(passed) else 0.0,
                "up20_rate": float(passed["up20"].mean()) if len(passed) else 0.0,
                "up30_rate": float(passed["up30"].mean()) if len(passed) else 0.0,
                "loss10_rate": float(passed["loss10"].mean()) if len(passed) else 0.0,
                "up10_first": float((passed["first_hit"] == "up10_first").mean()) if len(passed) else 0.0,
                "dn10_first": float((passed["first_hit"] == "dn10_first").mean()) if len(passed) else 0.0,
                "unresolved": float((passed["first_hit"] == "unresolved").mean()) if len(passed) else 0.0,
            }
        )
        if len(passed):
            counts = passed.groupby("candidate_entry_date").size()
            density_rows.append(
                {
                    "variant": variant,
                    "year": int(year),
                    "signal_days": int(len(counts)),
                    "avg_per_day": float(counts.mean()),
                    "median_per_day": float(counts.median()),
                    "max_per_day": int(counts.max()),
                }
            )
        else:
            density_rows.append(
                {
                    "variant": variant,
                    "year": int(year),
                    "signal_days": 0,
                    "avg_per_day": 0.0,
                    "median_per_day": 0.0,
                    "max_per_day": 0,
                }
            )
    return pd.DataFrame(summary_rows, columns=SUMMARY_COLUMNS), pd.DataFrame(density_rows)
```

- [ ] **Step 4: Run the focused unit test file again and verify it passes**

Run: `PYTHONPATH=src python3 -m unittest tests.test_vshape_first_break_confirmation -v`

Expected: `OK`

- [ ] **Step 5: Commit the unit-test-backed confirmation semantics**

```bash
git add tests/test_vshape_first_break_confirmation.py src/alpha_find_v2/vshape_first_break_confirmation.py
git commit -m "Lock V-shape first-break confirmation semantics"
```

### Task 2: Add DuckDB Loading, Output Writers, And A Non-Product Module Entry Point

**Files:**
- Modify: `src/alpha_find_v2/vshape_first_break_confirmation.py`
- Modify: `tests/test_vshape_first_break_confirmation.py`
- Test: `tests/test_vshape_first_break_confirmation.py`

- [ ] **Step 1: Extend the test file with an integration case that exercises DuckDB loading and file outputs**

```python
from pathlib import Path
import json
import tempfile

import duckdb

from alpha_find_v2.vshape_first_break_confirmation import main, run_first_break_confirmation_study


def _write_source_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
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
    conn.executemany(
        "INSERT INTO daily_bar_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("000001.SZ", "20240102", "SZ", "main_board", False, 10.2, 10.7, 10.1, 10.6),
            ("000001.SZ", "20240103", "SZ", "main_board", False, 10.3, 10.8, 10.1, 10.7),
            ("000001.SZ", "20240104", "SZ", "main_board", False, 10.4, 10.9, 10.05, 10.8),
            ("000001.SZ", "20240105", "SZ", "main_board", False, 10.5, 11.1, 9.95, 10.7),
            ("000001.SZ", "20240108", "SZ", "main_board", False, 10.7, 11.6, 10.4, 11.3),
            ("000001.SZ", "20240109", "SZ", "main_board", False, 11.2, 12.4, 10.9, 12.1),
            ("000002.SZ", "20240102", "SZ", "main_board", False, 20.2, 20.6, 19.8, 20.3),
            ("000002.SZ", "20240103", "SZ", "main_board", False, 20.1, 20.4, 19.9, 20.0),
            ("000002.SZ", "20240104", "SZ", "main_board", False, 19.8, 20.1, 19.7, 19.9),
            ("000002.SZ", "20240105", "SZ", "main_board", False, 19.7, 20.0, 19.6, 19.8),
        ],
    )
    conn.close()


def _write_events_csv(path: Path) -> None:
    _events().to_csv(path, index=False)


class VShapeFirstBreakConfirmationIntegrationTest(unittest.TestCase):
    def test_run_study_writes_summary_density_events_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source_db = root / "research_source.duckdb"
            events_csv = root / "events.csv"
            summary_csv = root / "summary.csv"
            density_csv = root / "density.csv"
            events_out_csv = root / "events_out.csv"
            report_md = root / "report.md"

            _write_source_db(source_db)
            _write_events_csv(events_csv)

            result = run_first_break_confirmation_study(
                events_csv_path=events_csv,
                source_db_path=source_db,
                summary_csv_path=summary_csv,
                density_csv_path=density_csv,
                events_output_csv_path=events_out_csv,
                report_markdown_path=report_md,
            )

            self.assertEqual(set(result.keys()), {"summary", "density", "events"})
            self.assertTrue(summary_csv.exists())
            self.assertTrue(density_csv.exists())
            self.assertTrue(events_out_csv.exists())
            self.assertTrue(report_md.exists())

            summary = pd.read_csv(summary_csv)
            self.assertEqual(set(summary["variant"]), {"baseline_first_break", "confirm_2d", "confirm_3d"})
            self.assertIn("confirmation_pass_rate", summary.columns)
            report_text = report_md.read_text(encoding="utf-8")
            self.assertIn("# V Shape First Break Confirmation Study - 2026-05-12", report_text)
            self.assertIn("confirm_2d", report_text)

    def test_main_accepts_file_paths_and_exits_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source_db = root / "research_source.duckdb"
            events_csv = root / "events.csv"
            summary_csv = root / "summary.csv"
            density_csv = root / "density.csv"
            events_out_csv = root / "events_out.csv"
            report_md = root / "report.md"

            _write_source_db(source_db)
            _write_events_csv(events_csv)

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
                    str(events_out_csv),
                    "--report-markdown",
                    str(report_md),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(summary_csv.exists())
```

- [ ] **Step 2: Run the single integration test and verify it fails because loading and writing code is missing**

Run: `PYTHONPATH=src python3 -m unittest tests.test_vshape_first_break_confirmation.VShapeFirstBreakConfirmationIntegrationTest.test_run_study_writes_summary_density_events_and_report -v`

Expected: `FAILED` with `AttributeError` or `NameError` for `run_first_break_confirmation_study` / `main`

- [ ] **Step 3: Extend the module with DuckDB readers, CSV writers, markdown rendering, and an argparse main**

```python
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


def load_first_break_events(events_csv_path: Path) -> pd.DataFrame:
    events = pd.read_csv(events_csv_path)
    required = {"security_id", "signal_date", "start_high"}
    missing = required.difference(events.columns)
    if missing:
        raise ValueError(f"events CSV missing required columns: {sorted(missing)}")
    events["signal_date"] = events["signal_date"].astype(str)
    return events.sort_values(["security_id", "signal_date"]).reset_index(drop=True)


def load_bar_history(source_db_path: Path, security_ids: list[str], min_signal_date: str) -> dict[str, pd.DataFrame]:
    quoted = ", ".join(f"'{security_id}'" for security_id in sorted(set(security_ids)))
    query = f"""
        SELECT security_id, trade_date, open_adj, high_adj, low_adj, close_adj
        FROM daily_bar_pit
        WHERE security_id IN ({quoted})
          AND trade_date >= '{min_signal_date}'
          AND exchange IN ('SH', 'SZ')
          AND board = 'main_board'
          AND coalesce(is_st, false) = false
        ORDER BY security_id, trade_date
    """
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        bars = conn.execute(query).fetchdf()
    finally:
        conn.close()
    bars["trade_date"] = bars["trade_date"].astype(str)
    return {
        security_id: frame.reset_index(drop=True)
        for security_id, frame in bars.groupby("security_id", sort=False)
    }


def _markdown_table(frame: pd.DataFrame) -> str:
    columns = list(frame.columns)
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join(["---"] * len(columns)) + " |"
    rows = [
        "| " + " | ".join(str(row[column]) for column in columns) + " |"
        for _, row in frame.iterrows()
    ]
    return "\n".join([header, divider, *rows])


def write_markdown_report(
    report_markdown_path: Path,
    *,
    events_csv_path: Path,
    source_db_path: Path,
    summary: pd.DataFrame,
    density: pd.DataFrame,
) -> None:
    report_lines = [
        "# V Shape First Break Confirmation Study - 2026-05-12",
        "",
        "## Inputs",
        "",
        f"- Source events: `{events_csv_path}`",
        f"- Source DB: `{source_db_path}`",
        "- Variants: `baseline_first_break`, `confirm_2d`, `confirm_3d`",
        "",
        "## Summary",
        "",
        _markdown_table(summary.round(4)),
        "",
        "## Signal Density",
        "",
        _markdown_table(density.round(4)),
        "",
        "## Judgment",
        "",
        "- Use the summary and density tables above to decide whether confirmation improved follow-through enough to justify the sample loss.",
        "- Do not move to portfolio construction unless at least one confirmation variant shows a clear edge with tolerable density loss.",
    ]
    report_markdown_path.write_text("\n".join(report_lines), encoding="utf-8")


def run_first_break_confirmation_study(
    *,
    events_csv_path: Path,
    source_db_path: Path,
    summary_csv_path: Path,
    density_csv_path: Path,
    events_output_csv_path: Path,
    report_markdown_path: Path | None = None,
) -> dict[str, pd.DataFrame]:
    events = load_first_break_events(events_csv_path)
    bars_by_security = load_bar_history(
        source_db_path,
        security_ids=events["security_id"].astype(str).tolist(),
        min_signal_date=events["signal_date"].min(),
    )
    combined = pd.concat(
        [
            build_confirmation_variant(events, bars_by_security, variant_name="baseline_first_break", confirm_days=0),
            build_confirmation_variant(events, bars_by_security, variant_name="confirm_2d", confirm_days=2),
            build_confirmation_variant(events, bars_by_security, variant_name="confirm_3d", confirm_days=3),
        ],
        ignore_index=True,
    )
    summary, density = summarize_variant_years(combined)
    summary.to_csv(summary_csv_path, index=False)
    density.to_csv(density_csv_path, index=False)
    combined.to_csv(events_output_csv_path, index=False)
    if report_markdown_path is not None:
        write_markdown_report(
            report_markdown_path,
            events_csv_path=events_csv_path,
            source_db_path=source_db_path,
            summary=summary,
            density=density,
        )
    return {"summary": summary, "density": density, "events": combined}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the V-shape first-break confirmation study.")
    parser.add_argument("--events-csv", required=True)
    parser.add_argument("--source-db", required=True)
    parser.add_argument("--summary-csv", required=True)
    parser.add_argument("--density-csv", required=True)
    parser.add_argument("--events-output-csv", required=True)
    parser.add_argument("--report-markdown")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_first_break_confirmation_study(
        events_csv_path=Path(args.events_csv),
        source_db_path=Path(args.source_db),
        summary_csv_path=Path(args.summary_csv),
        density_csv_path=Path(args.density_csv),
        events_output_csv_path=Path(args.events_output_csv),
        report_markdown_path=Path(args.report_markdown) if args.report_markdown else None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the whole test file and verify both unit and integration coverage pass**

Run: `PYTHONPATH=src python3 -m unittest tests.test_vshape_first_break_confirmation -v`

Expected: `OK`

- [ ] **Step 5: Commit the standalone study runner**

```bash
git add tests/test_vshape_first_break_confirmation.py src/alpha_find_v2/vshape_first_break_confirmation.py
git commit -m "Add standalone first-break confirmation study runner"
```

### Task 3: Execute The Real Study, Validate The Outputs, And Persist The Judgment

**Files:**
- Modify: `docs/research/vshape-first-break-confirmation-2026-05-12.md`
- Verify runtime outputs: `/tmp/vshape_first_break_confirmation_summary.csv`
- Verify runtime outputs: `/tmp/vshape_first_break_confirmation_density.csv`
- Verify runtime outputs: `/tmp/vshape_first_break_confirmation_events.csv`

- [ ] **Step 1: Run the study against the existing first-break artifact and research DB**

Run:

```bash
PYTHONPATH=src python3 -m alpha_find_v2.vshape_first_break_confirmation \
  --events-csv /tmp/vshape_first_break_events.csv \
  --source-db output/research_source.duckdb \
  --summary-csv /tmp/vshape_first_break_confirmation_summary.csv \
  --density-csv /tmp/vshape_first_break_confirmation_density.csv \
  --events-output-csv /tmp/vshape_first_break_confirmation_events.csv \
  --report-markdown docs/research/vshape-first-break-confirmation-2026-05-12.md
```

Expected:

- `/tmp/vshape_first_break_confirmation_summary.csv` exists
- `/tmp/vshape_first_break_confirmation_density.csv` exists
- `/tmp/vshape_first_break_confirmation_events.csv` exists
- `docs/research/vshape-first-break-confirmation-2026-05-12.md` exists

- [ ] **Step 2: Run output validation checks before trusting the conclusions**

Run:

```bash
python3 - <<'PY'
import pandas as pd

summary = pd.read_csv('/tmp/vshape_first_break_confirmation_summary.csv')
density = pd.read_csv('/tmp/vshape_first_break_confirmation_density.csv')
events = pd.read_csv('/tmp/vshape_first_break_confirmation_events.csv')

for year in sorted(summary['year'].dropna().unique()):
    y = summary[summary['year'] == year].set_index('variant')
    assert y.loc['confirm_2d', 'events'] <= y.loc['baseline_first_break', 'events']
    assert y.loc['confirm_3d', 'events'] <= y.loc['baseline_first_break', 'events']
    assert y.loc['confirm_3d', 'events'] <= y.loc['confirm_2d', 'events']

failed = events[events['confirmation_passed'] == False]
assert failed['entry_open'].isna().all()

print(summary.round(4).to_string(index=False))
print()
print(density.round(4).to_string(index=False))
PY
```

Expected:

- Python exits cleanly
- Printed summary shows monotonic event counts by variant
- Failed rows have no `entry_open`

- [ ] **Step 3: Review the generated research note and tighten the judgment paragraph**

Use this exact target structure inside `docs/research/vshape-first-break-confirmation-2026-05-12.md`:

```markdown
# V Shape First Break Confirmation Study - 2026-05-12

## Object

- Source events: `/tmp/vshape_first_break_events.csv`
- Source DB: `output/research_source.duckdb`
- Variants: `baseline_first_break`, `confirm_2d`, `confirm_3d`

## Summary

[Paste the generated markdown summary table unchanged]

## Signal Density

[Paste the generated markdown density table unchanged]

## Judgment

- State whether `confirm_2d` improved the 30-day distribution enough to justify the sample loss.
- State whether `confirm_3d` improved the 30-day distribution enough to justify the sample loss.
- State which variant, if any, should advance to portfolio-level testing next.
- If both variants fail, say the experiment should stop at event level.
```

The file is already generated by the module. Only tighten the final four judgment bullets after reading the real metrics; do not rewrite the tables by hand.

- [ ] **Step 4: Run the focused test file one more time and compile the modified module**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_vshape_first_break_confirmation -v
python3 -m compileall src/alpha_find_v2/vshape_first_break_confirmation.py tests/test_vshape_first_break_confirmation.py
```

Expected:

- `OK`
- `Compiling 'src/alpha_find_v2/vshape_first_break_confirmation.py'...`

- [ ] **Step 5: Commit the executed study and research conclusion**

```bash
git add \
  src/alpha_find_v2/vshape_first_break_confirmation.py \
  tests/test_vshape_first_break_confirmation.py \
  docs/research/vshape-first-break-confirmation-2026-05-12.md
git commit -m "Study delayed confirmation on V-shape first breaks"
```
