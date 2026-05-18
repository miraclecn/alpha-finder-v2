from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from alpha_find_v2.price_volume_regime_validation import (
    _compute_forward_hit_stats,
    build_candidate_feature_rows,
    classify_regime_dates,
    fit_regime_entry_evaluators,
    fit_regime_prototypes,
    run_price_volume_regime_validation_study,
    score_candidate_rows,
)


def _synthetic_candidate_rows() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    year_specs = {
        2022: {
            "regime_hint": "attention_transition",
            "early_mean_ret": 0.0001,
            "late_mean_ret": -0.0002,
            "launch_pad_mean_ret": -0.0001,
            "early_mean_turnover_ratio": 1.34,
            "late_mean_turnover_ratio": 1.36,
            "launch_pad_mean_turnover_ratio": 1.37,
            "late_expand_up_rate": 0.142,
            "launch_pad_expand_up_rate": 0.146,
            "late_contract_down_rate": 0.130,
            "launch_pad_contract_down_rate": 0.128,
            "late_contract_flat_rate": 0.150,
            "launch_pad_contract_flat_rate": 0.145,
            "late_up_mean_turnover_ratio": 1.84,
            "launch_pad_up_mean_turnover_ratio": 1.86,
            "launch_pad_down_mean_turnover_ratio": 1.33,
        },
        2023: {
            "regime_hint": "clean_breakout",
            "early_mean_ret": 0.0008,
            "late_mean_ret": 0.0010,
            "launch_pad_mean_ret": 0.0013,
            "early_mean_turnover_ratio": 1.36,
            "late_mean_turnover_ratio": 1.42,
            "launch_pad_mean_turnover_ratio": 1.52,
            "late_expand_up_rate": 0.150,
            "launch_pad_expand_up_rate": 0.178,
            "late_contract_down_rate": 0.122,
            "launch_pad_contract_down_rate": 0.110,
            "late_contract_flat_rate": 0.142,
            "launch_pad_contract_flat_rate": 0.120,
            "late_up_mean_turnover_ratio": 1.90,
            "launch_pad_up_mean_turnover_ratio": 2.06,
            "launch_pad_down_mean_turnover_ratio": 1.36,
        },
        2024: {
            "regime_hint": "repair_retake",
            "early_mean_ret": -0.0009,
            "late_mean_ret": 0.0004,
            "launch_pad_mean_ret": 0.0008,
            "early_mean_turnover_ratio": 1.28,
            "late_mean_turnover_ratio": 1.46,
            "launch_pad_mean_turnover_ratio": 1.58,
            "late_expand_up_rate": 0.155,
            "launch_pad_expand_up_rate": 0.182,
            "late_contract_down_rate": 0.140,
            "launch_pad_contract_down_rate": 0.136,
            "late_contract_flat_rate": 0.150,
            "launch_pad_contract_flat_rate": 0.130,
            "late_up_mean_turnover_ratio": 1.99,
            "launch_pad_up_mean_turnover_ratio": 2.18,
            "launch_pad_down_mean_turnover_ratio": 1.47,
        },
        2025: {
            "regime_hint": "trend_continuation",
            "early_mean_ret": 0.0021,
            "late_mean_ret": 0.0011,
            "launch_pad_mean_ret": 0.0012,
            "early_mean_turnover_ratio": 1.38,
            "late_mean_turnover_ratio": 1.34,
            "launch_pad_mean_turnover_ratio": 1.33,
            "late_expand_up_rate": 0.149,
            "launch_pad_expand_up_rate": 0.151,
            "late_contract_down_rate": 0.118,
            "launch_pad_contract_down_rate": 0.115,
            "late_contract_flat_rate": 0.170,
            "launch_pad_contract_flat_rate": 0.166,
            "late_up_mean_turnover_ratio": 1.83,
            "launch_pad_up_mean_turnover_ratio": 1.82,
            "launch_pad_down_mean_turnover_ratio": 1.22,
        },
        2021: {
            "regime_hint": "clean_breakout_like_validation",
            "early_mean_ret": 0.0009,
            "late_mean_ret": 0.0010,
            "launch_pad_mean_ret": 0.0014,
            "early_mean_turnover_ratio": 1.35,
            "late_mean_turnover_ratio": 1.43,
            "launch_pad_mean_turnover_ratio": 1.55,
            "late_expand_up_rate": 0.151,
            "launch_pad_expand_up_rate": 0.181,
            "late_contract_down_rate": 0.121,
            "launch_pad_contract_down_rate": 0.109,
            "late_contract_flat_rate": 0.141,
            "launch_pad_contract_flat_rate": 0.118,
            "late_up_mean_turnover_ratio": 1.92,
            "launch_pad_up_mean_turnover_ratio": 2.08,
            "launch_pad_down_mean_turnover_ratio": 1.34,
        },
        2026: {
            "regime_hint": "trend_continuation_like_validation",
            "early_mean_ret": 0.0020,
            "late_mean_ret": 0.0010,
            "launch_pad_mean_ret": 0.0011,
            "early_mean_turnover_ratio": 1.39,
            "late_mean_turnover_ratio": 1.35,
            "launch_pad_mean_turnover_ratio": 1.34,
            "late_expand_up_rate": 0.148,
            "launch_pad_expand_up_rate": 0.152,
            "late_contract_down_rate": 0.117,
            "launch_pad_contract_down_rate": 0.114,
            "late_contract_flat_rate": 0.168,
            "launch_pad_contract_flat_rate": 0.165,
            "late_up_mean_turnover_ratio": 1.82,
            "launch_pad_up_mean_turnover_ratio": 1.81,
            "launch_pad_down_mean_turnover_ratio": 1.21,
        },
    }

    for year, spec in year_specs.items():
        dates = pd.date_range(f"{year}-01-04", periods=12, freq="B")
        for idx, day in enumerate(dates):
            for bucket in range(2):
                success_like = bucket == 0
                adjust = 0.03 if success_like else -0.03
                row = {
                    "security_id": f"{year}_{idx}_{bucket}.SZ",
                    "trade_date": day.strftime("%Y%m%d"),
                    "year": year,
                    "board": "main_board",
                    "success_label": success_like if year in {2022, 2023, 2024, 2025} else False,
                    "close_ret30": 0.18 + adjust if success_like else -0.04 + adjust,
                    "max_ret30": 0.28 + adjust if success_like else 0.08 + adjust,
                    "min_ret30": -0.05 + adjust if success_like else -0.15 + adjust,
                    "days_to_up20": 5.0 if success_like else float("nan"),
                    "days_to_loss10": float("nan") if success_like else 4.0,
                    "first_hit": "up20_first" if success_like else "loss10_first",
                    "regime_hint": spec["regime_hint"],
                }
                for key, value in spec.items():
                    if key == "regime_hint":
                        continue
                    if success_like:
                        row[key] = float(value) + 0.02
                    else:
                        row[key] = float(value) - 0.02
                rows.append(row)
    return pd.DataFrame(rows)


def _write_candidate_source_db(path: Path) -> None:
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
                low_adj DOUBLE,
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

        dates = pd.date_range("2020-01-02", periods=150, freq="B")
        bar_rows: list[tuple[object, ...]] = []
        trade_rows: list[tuple[object, ...]] = []
        close_value = 10.0
        for idx, day in enumerate(dates):
            trade_date = day.strftime("%Y%m%d")
            if idx < 90:
                close_value *= 1.001
            else:
                close_value *= 1.008
            high_adj = close_value * (1.03 if idx >= 95 else 1.01)
            low_adj = close_value * 0.985
            turnover = 100_000_000.0 + (idx % 10) * 5_000_000.0
            bar_rows.append(
                (
                    "AAA.SZ",
                    trade_date,
                    "main_board",
                    False,
                    "unadjusted",
                    close_value,
                    high_adj,
                    low_adj,
                    turnover,
                )
            )
            trade_rows.append(("AAA.SZ", trade_date, False))

        conn.executemany("INSERT INTO daily_bar_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", bar_rows)
        conn.executemany("INSERT INTO tradeability_state_daily VALUES (?, ?, ?)", trade_rows)
    finally:
        conn.close()


class PriceVolumeRegimeValidationTest(unittest.TestCase):
    def test_compute_forward_hit_stats_tracks_first_hit_days(self) -> None:
        stats = _compute_forward_hit_stats(
            entry_price=10.0,
            forward_high=np.array([10.5, 11.8, 12.1, 11.4, 10.2], dtype=float),
            forward_low=np.array([9.7, 9.6, 9.5, 8.8, 9.4], dtype=float),
        )

        self.assertEqual(stats["days_to_up20"], 3.0)
        self.assertEqual(stats["days_to_loss10"], 4.0)
        self.assertTrue(stats["success_label"])
        self.assertEqual(stats["first_hit"], "up20_first")

    def test_build_candidate_feature_rows_reads_db_and_emits_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            source_db = Path(tmp_dir) / "source.duckdb"
            _write_candidate_source_db(source_db)

            rows = build_candidate_feature_rows(
                source_db_path=source_db,
                entry_start="20200401",
                entry_end="20200731",
                query_start="20200101",
                query_end="20201231",
            )

            self.assertFalse(rows.empty)
            self.assertIn("success_label", rows.columns)
            self.assertIn("days_to_up20", rows.columns)
            self.assertIn("days_to_loss10", rows.columns)
            self.assertIn("launch_pad_mean_turnover_ratio", rows.columns)
            self.assertTrue(rows["success_label"].any())
            self.assertGreater(rows["days_to_up20"].notna().sum(), 0)

    def test_classify_regime_dates_assigns_nearest_prototype(self) -> None:
        candidate_rows = _synthetic_candidate_rows()
        year_to_regime = {
            2022: "attention_transition",
            2023: "clean_breakout",
            2024: "repair_retake",
            2025: "trend_continuation",
        }
        feature_names = [
            "early_mean_ret",
            "late_mean_turnover_ratio",
            "launch_pad_mean_turnover_ratio",
        ]

        prototypes, scales = fit_regime_prototypes(
            candidate_rows=candidate_rows[candidate_rows["year"].between(2022, 2025)],
            year_to_regime=year_to_regime,
            feature_names=feature_names,
            regime_rolling_days=2,
        )

        validation_daily = pd.DataFrame(
            [
                {
                    "trade_date": "20260120",
                    "year": 2026,
                    "early_mean_ret": 0.0021,
                    "late_mean_turnover_ratio": 1.34,
                    "launch_pad_mean_turnover_ratio": 1.33,
                },
                {
                    "trade_date": "20260220",
                    "year": 2026,
                    "early_mean_ret": 0.0009,
                    "late_mean_turnover_ratio": 1.43,
                    "launch_pad_mean_turnover_ratio": 1.55,
                },
            ]
        )

        classified = classify_regime_dates(
            daily_market_features=validation_daily,
            prototypes=prototypes,
            feature_scales=scales,
            feature_names=feature_names,
        )

        self.assertEqual(
            classified["predicted_regime"].tolist(),
            ["trend_continuation", "clean_breakout"],
        )

    def test_fit_regime_entry_evaluators_and_score_candidates_prefer_success_like_rows(self) -> None:
        candidate_rows = _synthetic_candidate_rows()
        year_to_regime = {
            2022: "attention_transition",
            2023: "clean_breakout",
            2024: "repair_retake",
            2025: "trend_continuation",
        }
        feature_names = [
            "late_mean_turnover_ratio",
            "launch_pad_mean_turnover_ratio",
            "launch_pad_expand_up_rate",
            "launch_pad_contract_flat_rate",
            "launch_pad_up_mean_turnover_ratio",
        ]

        evaluators = fit_regime_entry_evaluators(
            candidate_rows=candidate_rows[candidate_rows["year"].between(2022, 2025)],
            year_to_regime=year_to_regime,
            feature_names=feature_names,
        )

        validation = pd.DataFrame(
            [
                {
                    "security_id": "good.SZ",
                    "trade_date": "20260120",
                    "year": 2026,
                    "board": "main_board",
                    "late_mean_turnover_ratio": 1.36,
                    "launch_pad_mean_turnover_ratio": 1.36,
                    "launch_pad_expand_up_rate": 0.17,
                    "launch_pad_contract_flat_rate": 0.14,
                    "launch_pad_up_mean_turnover_ratio": 1.90,
                },
                {
                    "security_id": "weak.SZ",
                    "trade_date": "20260120",
                    "year": 2026,
                    "board": "main_board",
                    "late_mean_turnover_ratio": 1.29,
                    "launch_pad_mean_turnover_ratio": 1.28,
                    "launch_pad_expand_up_rate": 0.11,
                    "launch_pad_contract_flat_rate": 0.20,
                    "launch_pad_up_mean_turnover_ratio": 1.62,
                },
            ]
        )
        regimes = pd.DataFrame(
            [
                {
                    "trade_date": "20260120",
                    "predicted_regime": "trend_continuation",
                }
            ]
        )

        scored = score_candidate_rows(
            candidate_rows=validation,
            regime_by_date=regimes,
            evaluators=evaluators,
            feature_names=feature_names,
        ).sort_values("score", ascending=False)

        self.assertEqual(scored.iloc[0]["security_id"], "good.SZ")
        self.assertGreater(float(scored.iloc[0]["score"]), float(scored.iloc[1]["score"]))

    def test_run_study_writes_outputs_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            summary_csv = base / "summary.csv"
            scored_csv = base / "scored.csv"
            regimes_csv = base / "regimes.csv"
            report_markdown = base / "report.md"

            outputs = run_price_volume_regime_validation_study(
                candidate_rows=_synthetic_candidate_rows(),
                summary_csv_path=summary_csv,
                scored_csv_path=scored_csv,
                regimes_csv_path=regimes_csv,
                report_markdown_path=report_markdown,
                training_years=(2022, 2023, 2024, 2025),
                validation_years=(2021, 2026),
                top_n_per_day=1,
                regime_rolling_days=2,
            )

            self.assertEqual(set(outputs.keys()), {"summary", "scored", "regimes"})
            self.assertTrue(summary_csv.exists())
            self.assertTrue(scored_csv.exists())
            self.assertTrue(regimes_csv.exists())
            self.assertTrue(report_markdown.exists())

            summary = pd.read_csv(summary_csv)
            self.assertEqual(set(summary["year"].tolist()), {2021, 2026})
            self.assertIn("selected_event_rate", summary.columns)
            self.assertTrue(summary["predicted_regime_majority"].notna().all())

            scored = pd.read_csv(scored_csv)
            self.assertIn("days_to_up20", scored.columns)
            self.assertIn("days_to_loss10", scored.columns)
            self.assertTrue(scored["days_to_up20"].notna().any())
            self.assertTrue(scored["days_to_loss10"].notna().any())

            markdown = report_markdown.read_text(encoding="utf-8")
            self.assertIn("# Price-Volume Regime Validation Study", markdown)
            self.assertIn("2021", markdown)
            self.assertIn("2026", markdown)


if __name__ == "__main__":
    unittest.main()
