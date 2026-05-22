from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


class MarketDataQualityTest(unittest.TestCase):
    def test_summary_counts_quality_flags_and_unresolved_factor_jumps(self) -> None:
        from alpha_find_v2.market_data_quality import summarize_market_data_quality

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "research_source.duckdb"
            self._create_quality_db(db_path)

            summary = summarize_market_data_quality(db_path)

            self.assertEqual(summary.daily_bar_rows, 14)
            self.assertEqual(summary.qfq_fallback_rows, 1)
            self.assertEqual(summary.missing_price_rows, 1)
            self.assertEqual(summary.zero_or_missing_adj_factor_rows, 2)
            self.assertEqual(summary.corporate_action_rows, 2)
            self.assertEqual(summary.tradeability_rows, 3)
            self.assertEqual(summary.tradeability_official_rows, 2)
            self.assertEqual(summary.tradeability_ohlc_fallback_rows, 1)
            self.assertTrue(summary.adj_factor_jump_assessable)
            self.assertEqual(summary.missing_quality_tables, ())
            self.assertEqual(
                summary.promotion_blocking_quality_state,
                "blocked_unresolved_adj_factor_jumps",
            )
            self.assertEqual(summary.adj_factor_jump_rows, 5)
            self.assertEqual(summary.explained_adj_factor_jump_rows, 2)
            self.assertEqual(summary.unresolved_adj_factor_jump_rows, 3)
            self.assertEqual(summary.promotion_blocking_unresolved_adj_factor_jump_rows, 3)
            self.assertEqual(
                summary.unresolved_adj_factor_jump_triage,
                (
                    {
                        "triage_class": "implemented_dividend_outside_factor_window",
                        "rows": 1,
                        "securities": 1,
                    },
                    {
                        "triage_class": "nonimplemented_dividend_same_date",
                        "rows": 1,
                        "securities": 1,
                    },
                    {
                        "triage_class": "provider_factor_jump_without_event_evidence",
                        "rows": 1,
                        "securities": 1,
                    },
                ),
            )
            self.assertEqual(
                summary.unresolved_adj_factor_jump_years,
                (
                    {
                        "year": "2024",
                        "rows": 3,
                        "securities": 3,
                        "earliest_date": "20240103",
                        "latest_date": "20240103",
                    },
                ),
            )
            self.assertEqual(
                summary.unresolved_adj_factor_jump_examples,
                (
                    {
                        "security_id": "AAA",
                        "previous_trade_date": "20240102",
                        "trade_date": "20240103",
                        "factor_ratio": 2.0,
                        "magnitude_bucket": ">10pct",
                        "dividend_proximity_bucket": "implemented_within_5d",
                        "nearest_implemented_dividend_ex_date": "20240106",
                        "nearest_implemented_dividend_days": 3,
                        "has_suspend_window": False,
                        "factor_pre_close_basis_diff": 1.0,
                        "triage_class": "implemented_dividend_outside_factor_window",
                        "recommended_action": "quarantine_security_window_from_promotion",
                    },
                    {
                        "security_id": "HHH",
                        "previous_trade_date": "20240102",
                        "trade_date": "20240103",
                        "factor_ratio": 1.2,
                        "magnitude_bucket": ">10pct",
                        "dividend_proximity_bucket": "no_implemented_within_30d",
                        "nearest_implemented_dividend_ex_date": None,
                        "nearest_implemented_dividend_days": None,
                        "has_suspend_window": False,
                        "factor_pre_close_basis_diff": 0.2,
                        "triage_class": "provider_factor_jump_without_event_evidence",
                        "recommended_action": "quarantine_security_window_from_promotion",
                    },
                    {
                        "security_id": "GGG",
                        "previous_trade_date": "20240102",
                        "trade_date": "20240103",
                        "factor_ratio": 1.05,
                        "magnitude_bucket": "<=10pct",
                        "dividend_proximity_bucket": "same_date_nonimplemented_only",
                        "nearest_implemented_dividend_ex_date": None,
                        "nearest_implemented_dividend_days": None,
                        "has_suspend_window": False,
                        "factor_pre_close_basis_diff": 0.05,
                        "triage_class": "nonimplemented_dividend_same_date",
                        "recommended_action": "quarantine_security_window_from_promotion",
                    },
                ),
            )
            self.assertEqual(
                summary.unresolved_adj_factor_jump_magnitude_buckets,
                (
                    {"bucket": "<=10pct", "rows": 1, "securities": 1},
                    {"bucket": ">10pct", "rows": 2, "securities": 2},
                ),
            )
            self.assertEqual(
                summary.unresolved_adj_factor_jump_top_securities,
                (
                    {
                        "security_id": "AAA",
                        "rows": 1,
                        "earliest_date": "20240103",
                        "latest_date": "20240103",
                        "min_factor_ratio": 2.0,
                        "max_factor_ratio": 2.0,
                    },
                    {
                        "security_id": "GGG",
                        "rows": 1,
                        "earliest_date": "20240103",
                        "latest_date": "20240103",
                        "min_factor_ratio": 1.05,
                        "max_factor_ratio": 1.05,
                    },
                    {
                        "security_id": "HHH",
                        "rows": 1,
                        "earliest_date": "20240103",
                        "latest_date": "20240103",
                        "min_factor_ratio": 1.2,
                        "max_factor_ratio": 1.2,
                    },
                ),
            )
            self.assertEqual(
                summary.unresolved_adj_factor_jump_dividend_proximity,
                (
                    {"bucket": "same_date_nonimplemented_only", "rows": 1, "securities": 1},
                    {"bucket": "implemented_within_5d", "rows": 1, "securities": 1},
                    {"bucket": "no_implemented_within_30d", "rows": 1, "securities": 1},
                ),
            )

    def test_missing_quality_tables_are_unassessable_not_green(self) -> None:
        from alpha_find_v2.market_data_quality import summarize_market_data_quality

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "minimal.duckdb"
            conn = duckdb.connect(str(db_path))
            try:
                conn.execute(
                    """
                    CREATE TABLE daily_bar_pit (
                        security_id VARCHAR,
                        trade_date VARCHAR,
                        price_basis VARCHAR,
                        open DOUBLE,
                        high DOUBLE,
                        low DOUBLE,
                        close DOUBLE,
                        adj_factor DOUBLE
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO daily_bar_pit VALUES
                    ('AAA', '20240102', 'unadjusted', 10.0, 10.0, 10.0, 10.0, 1.0)
                    """
                )
            finally:
                conn.close()

            summary = summarize_market_data_quality(db_path)

            self.assertEqual(summary.daily_bar_rows, 1)
            self.assertEqual(summary.corporate_action_rows, 0)
            self.assertEqual(summary.tradeability_rows, 0)
            self.assertEqual(summary.tradeability_official_rows, 0)
            self.assertEqual(summary.tradeability_ohlc_fallback_rows, 0)
            self.assertFalse(summary.adj_factor_jump_assessable)
            self.assertEqual(
                summary.missing_quality_tables,
                ("corporate_action_ledger", "tradeability_state_daily"),
            )
            self.assertEqual(
                summary.promotion_blocking_quality_state,
                "blocked_unassessable",
            )
            self.assertEqual(summary.adj_factor_jump_rows, 0)
            self.assertEqual(summary.explained_adj_factor_jump_rows, 0)
            self.assertEqual(summary.unresolved_adj_factor_jump_rows, 0)
            self.assertEqual(summary.unresolved_adj_factor_jump_years, ())
            self.assertEqual(summary.unresolved_adj_factor_jump_magnitude_buckets, ())
            self.assertEqual(summary.unresolved_adj_factor_jump_top_securities, ())
            self.assertEqual(summary.unresolved_adj_factor_jump_dividend_proximity, ())
            self.assertEqual(summary.unresolved_adj_factor_jump_triage, ())
            self.assertEqual(summary.promotion_blocking_unresolved_adj_factor_jump_rows, 0)
            self.assertEqual(summary.unresolved_adj_factor_jump_examples, ())

    def test_cli_audit_market_data_quality_writes_json_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            db_path = temp_root / "research_source.duckdb"
            output_path = temp_root / "audits" / "market_data_quality.json"
            self._create_quality_db(db_path)

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "alpha_find_v2",
                    "audit-market-data-quality",
                    "--source-db",
                    str(db_path),
                    "--output",
                    str(output_path),
                ],
                cwd=Path(__file__).resolve().parents[1],
                env={**os.environ, "PYTHONPATH": "src"},
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(output_path.exists())
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            # Compare resolved paths to handle Windows short-name vs long-name variation
            self.assertEqual(
                Path(payload["source_db"]).resolve(), db_path.resolve()
            )
            self.assertEqual(payload["summary"]["daily_bar_rows"], 14)
            self.assertEqual(payload["summary"]["tradeability_official_rows"], 2)
            self.assertEqual(payload["summary"]["tradeability_ohlc_fallback_rows"], 1)
            self.assertTrue(payload["summary"]["adj_factor_jump_assessable"])
            self.assertEqual(payload["summary"]["missing_quality_tables"], [])
            self.assertEqual(
                payload["summary"]["promotion_blocking_quality_state"],
                "blocked_unresolved_adj_factor_jumps",
            )
            self.assertEqual(payload["summary"]["adj_factor_jump_rows"], 5)
            self.assertEqual(payload["summary"]["explained_adj_factor_jump_rows"], 2)
            self.assertEqual(payload["summary"]["unresolved_adj_factor_jump_rows"], 3)
            self.assertEqual(
                payload["summary"]["promotion_blocking_unresolved_adj_factor_jump_rows"],
                3,
            )
            self.assertEqual(
                payload["summary"]["unresolved_adj_factor_jump_triage"],
                [
                    {
                        "triage_class": "implemented_dividend_outside_factor_window",
                        "rows": 1,
                        "securities": 1,
                    },
                    {
                        "triage_class": "nonimplemented_dividend_same_date",
                        "rows": 1,
                        "securities": 1,
                    },
                    {
                        "triage_class": "provider_factor_jump_without_event_evidence",
                        "rows": 1,
                        "securities": 1,
                    },
                ],
            )
            self.assertEqual(
                payload["summary"]["unresolved_adj_factor_jump_years"],
                [
                    {
                        "year": "2024",
                        "rows": 3,
                        "securities": 3,
                        "earliest_date": "20240103",
                        "latest_date": "20240103",
                    },
                ],
            )
            self.assertEqual(
                payload["summary"]["unresolved_adj_factor_jump_dividend_proximity"],
                [
                    {"bucket": "same_date_nonimplemented_only", "rows": 1, "securities": 1},
                    {"bucket": "implemented_within_5d", "rows": 1, "securities": 1},
                    {"bucket": "no_implemented_within_30d", "rows": 1, "securities": 1},
                ],
            )
            self.assertEqual(
                payload["summary"]["unresolved_adj_factor_jump_examples"][0],
                {
                    "security_id": "AAA",
                    "previous_trade_date": "20240102",
                    "trade_date": "20240103",
                    "factor_ratio": 2.0,
                    "magnitude_bucket": ">10pct",
                    "dividend_proximity_bucket": "implemented_within_5d",
                    "nearest_implemented_dividend_ex_date": "20240106",
                    "nearest_implemented_dividend_days": 3,
                    "has_suspend_window": False,
                    "factor_pre_close_basis_diff": 1.0,
                    "triage_class": "implemented_dividend_outside_factor_window",
                    "recommended_action": "quarantine_security_window_from_promotion",
                },
            )
            stdout_payload = json.loads(result.stdout)
            self.assertEqual(stdout_payload["output_path"], str(output_path))

    def _create_quality_db(self, db_path: Path) -> None:
        conn = duckdb.connect(str(db_path))
        try:
            conn.execute(
                """
                CREATE TABLE daily_bar_pit (
                    security_id VARCHAR,
                    trade_date VARCHAR,
                    price_basis VARCHAR,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    pre_close DOUBLE,
                    adj_factor DOUBLE
                )
                """
            )
            conn.execute(
                """
                INSERT INTO daily_bar_pit VALUES
                ('AAA', '20240102', 'unadjusted', 10.0, 10.0, 10.0, 10.0, 10.0, 1.0),
                ('AAA', '20240103', 'unadjusted', 10.0, 10.0, 10.0, 10.0, 10.0, 2.0),
                ('BBB', '20240102', 'unadjusted', 20.0, 20.0, 20.0, 20.0, 20.0, 1.0),
                ('BBB', '20240103', 'unadjusted', 20.0, 20.0, 20.0, 20.0, 20.0, 1.5),
                ('CCC', '20240102', 'qfq_fallback', 9.0, 9.0, 9.0, 9.0, 9.0, 1.0),
                ('DDD', '20240102', 'unadjusted', NULL, 8.0, 8.0, 8.0, 8.0, 1.0),
                ('EEE', '20240102', 'unadjusted', 7.0, 7.0, 7.0, 7.0, 7.0, NULL),
                ('FFF', '20240102', 'unadjusted', 6.0, 6.0, 6.0, 6.0, 6.0, 0.0),
                ('GGG', '20240102', 'unadjusted', 5.0, 5.0, 5.0, 5.0, 5.0, 1.0),
                ('GGG', '20240103', 'unadjusted', 5.0, 5.0, 5.0, 5.0, 5.0, 1.05),
                ('HHH', '20240102', 'unadjusted', 4.0, 4.0, 4.0, 4.0, 4.0, 1.0),
                ('HHH', '20240103', 'unadjusted', 4.0, 4.0, 4.0, 4.0, 4.0, 1.2),
                ('JJJ', '20240102', 'unadjusted', 3.0, 3.0, 3.0, 3.0, 3.0, 1.0),
                ('JJJ', '20240110', 'unadjusted', 3.0, 3.0, 3.0, 3.0, 3.0, 2.0)
                """
            )
            conn.execute(
                """
                CREATE TABLE corporate_action_ledger (
                    action_id VARCHAR,
                    security_id VARCHAR,
                    action_type VARCHAR,
                    record_date VARCHAR,
                    book_date VARCHAR,
                    ex_date VARCHAR,
                    cash_per_share DOUBLE,
                    share_ratio DOUBLE,
                    source_table VARCHAR
                )
                """
            )
            conn.execute(
                """
                INSERT INTO corporate_action_ledger VALUES
                ('BBB:20240103:share', 'BBB', 'share_dividend', '20240102', '20240103',
                 '20240103', 0.0, 0.5, 'test'),
                ('JJJ:20240105:share', 'JJJ', 'share_dividend', '20240102', '20240105',
                 '20240105', 0.0, 1.0, 'test')
                """
            )
            conn.execute(
                """
                CREATE TABLE tradeability_state_daily (
                    security_id VARCHAR,
                    trade_date VARCHAR,
                    is_suspended BOOLEAN,
                    up_limit DOUBLE,
                    down_limit DOUBLE,
                    is_limit_up_open_lock BOOLEAN,
                    is_limit_down_open_lock BOOLEAN,
                    source_priority VARCHAR
                )
                """
            )
            conn.execute(
                """
                INSERT INTO tradeability_state_daily VALUES
                ('AAA', '20240102', FALSE, 11.0, 9.0, FALSE, FALSE, 'official'),
                ('BBB', '20240102', TRUE, NULL, NULL, FALSE, FALSE, 'official'),
                ('CCC', '20240102', FALSE, NULL, NULL, FALSE, FALSE, 'ohlc_fallback')
                """
            )
            conn.execute(
                """
                CREATE TABLE raw_dividend (
                    ts_code VARCHAR,
                    div_proc VARCHAR,
                    ex_date VARCHAR
                )
                """
            )
            conn.execute(
                """
                INSERT INTO raw_dividend VALUES
                ('AAA', '瀹炴柦', '20240106'),
                ('GGG', '棰勬', '20240103')
                """
            )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
