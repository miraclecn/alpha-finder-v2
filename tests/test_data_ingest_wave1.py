"""
Tests for data_ingest Wave 1:
  - schemas.py (DDL constants)
  - config_models.py (data_sources.toml parsing)
  - rate_limiter.py (token bucket)
"""
from __future__ import annotations

import sys
import tempfile
import threading
from pathlib import Path
import unittest

import duckdb

from alpha_find_v2.data_ingest.schemas import (
    ALL_DATASET_IDS,
    DATASET_INCREMENTAL_AXIS,
    DATASET_PRIMARY_KEYS,
    DATASET_TABLE_NAME,
    META_DDL,
    RAW_TABLE_DDL,
)
from alpha_find_v2.data_ingest.rate_limiter import RateLimitTimeout, TokenBucket


# ---------------------------------------------------------------------------
# schemas.py tests
# ---------------------------------------------------------------------------


class RawTableDDLTest(unittest.TestCase):
    def test_all_dataset_ids_present_in_raw_table_ddl(self) -> None:
        for dataset_id in ALL_DATASET_IDS:
            self.assertIn(
                dataset_id,
                RAW_TABLE_DDL,
                msg=f"RAW_TABLE_DDL missing dataset_id '{dataset_id}'",
            )

    def test_all_ddls_parse_in_duckdb(self) -> None:
        conn = duckdb.connect(":memory:")
        for dataset_id, ddl in RAW_TABLE_DDL.items():
            with self.subTest(dataset_id=dataset_id):
                try:
                    conn.execute(ddl)
                except Exception as exc:  # pragma: no cover
                    self.fail(f"DDL for '{dataset_id}' failed to parse: {exc}\n{ddl}")
        conn.close()

    def test_all_dataset_ids_present_in_primary_keys(self) -> None:
        for dataset_id in ALL_DATASET_IDS:
            self.assertIn(dataset_id, DATASET_PRIMARY_KEYS)

    def test_all_dataset_ids_present_in_incremental_axis(self) -> None:
        for dataset_id in ALL_DATASET_IDS:
            self.assertIn(dataset_id, DATASET_INCREMENTAL_AXIS)
            self.assertIn(
                DATASET_INCREMENTAL_AXIS[dataset_id],
                {"trade_date", "period_end", "static"},
            )

    def test_all_dataset_ids_present_in_table_name(self) -> None:
        for dataset_id in ALL_DATASET_IDS:
            self.assertIn(dataset_id, DATASET_TABLE_NAME)

    def test_primary_keys_are_columns_in_ddl(self) -> None:
        """Every primary-key column must appear in the corresponding DDL."""
        conn = duckdb.connect(":memory:")
        for dataset_id, pks in DATASET_PRIMARY_KEYS.items():
            ddl = RAW_TABLE_DDL[dataset_id]
            conn.execute(ddl)
            table = DATASET_TABLE_NAME[dataset_id]
            cols_result = conn.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
            ).fetchall()
            col_names = {row[0] for row in cols_result}
            for pk in pks:
                self.assertIn(
                    pk,
                    col_names,
                    msg=f"Primary key '{pk}' not in DDL columns for '{dataset_id}'",
                )
        conn.close()

    def test_meta_ddl_parses_in_duckdb(self) -> None:
        conn = duckdb.connect(":memory:")
        for name, ddl in META_DDL.items():
            with self.subTest(name=name):
                try:
                    conn.execute(ddl)
                except Exception as exc:  # pragma: no cover
                    self.fail(f"META_DDL '{name}' failed: {exc}\n{ddl}")
        conn.close()

    def test_existing_raw_table_schemas_compatible_with_fixture(self) -> None:
        """
        Verify that schemas required by market_data_bootstrap match fixture
        column names used in test_market_data_bootstrap.py::_create_source_db.
        """
        expected_columns = {
            "stock_basic": {
                "ts_code", "symbol", "name", "area", "industry",
                "list_date", "delist_date", "is_hs", "ingested_at",
            },
            "namechange": {
                "ts_code", "name", "start_date", "end_date", "ann_date",
                "change_reason", "source_table", "ingested_at",
            },
            "daily": {
                "ts_code", "trade_date", "open", "high", "low", "close",
                "pre_close", "change", "pct_chg", "vol", "amount",
                "source_table", "ingested_at",
            },
            "daily_basic": {
                "ts_code", "trade_date", "close", "turnover_rate",
                "turnover_rate_f", "volume_ratio", "pe", "pe_ttm", "pb",
                "ps", "ps_ttm", "dv_ratio", "dv_ttm", "total_share",
                "float_share", "free_share", "total_mv", "circ_mv",
                "source_table", "ingested_at",
            },
            "adj_factor": {
                "ts_code", "trade_date", "adj_factor",
                "source_table", "ingested_at",
            },
            "daily_qfq": {
                "ts_code", "trade_date", "open", "high", "low", "close",
                "pre_close", "change", "pct_chg", "vol", "amount",
                "source_table", "ingested_at",
            },
            "fina_indicator": {
                "ts_code", "ann_date", "end_date", "eps", "roe", "roa",
                "gross_margin", "netprofit_margin", "current_ratio",
                "debt_to_assets", "revenue_ps", "netprofit_yoy",
                "dt_netprofit_yoy", "or_yoy", "q_sales_yoy",
                "assets_yoy", "equity_yoy",
            },
        }
        conn = duckdb.connect(":memory:")
        for dataset_id, ddl in RAW_TABLE_DDL.items():
            conn.execute(ddl)
        for dataset_id, required_cols in expected_columns.items():
            table = DATASET_TABLE_NAME[dataset_id]
            actual_cols = {
                row[0]
                for row in conn.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
                ).fetchall()
            }
            missing = required_cols - actual_cols
            self.assertFalse(
                missing,
                msg=f"Dataset '{dataset_id}': columns {missing} required by "
                f"market_data_bootstrap are missing from DDL",
            )
        conn.close()

    def test_18_datasets_declared(self) -> None:
        self.assertEqual(len(ALL_DATASET_IDS), 18)


# ---------------------------------------------------------------------------
# config_models.py tests
# ---------------------------------------------------------------------------


class ConfigModelsTest(unittest.TestCase):
    def _template_path(self) -> Path:
        from alpha_find_v2.data_ingest import templates as _tmpl_pkg  # noqa: F401
        import alpha_find_v2.data_ingest.templates as tmpl_mod
        return Path(tmpl_mod.__file__).parent / "data_sources.toml.template"

    def test_template_path_exists(self) -> None:
        self.assertTrue(self._template_path().exists())

    def test_template_loads_successfully(self) -> None:
        from alpha_find_v2.data_ingest.config_models import load_data_sources_config
        config = load_data_sources_config(self._template_path())
        self.assertEqual(config.schema_version, 1)

    def test_template_has_18_datasets(self) -> None:
        from alpha_find_v2.data_ingest.config_models import load_data_sources_config
        config = load_data_sources_config(self._template_path())
        self.assertEqual(len(config.datasets), 18)

    def test_template_has_3_adapters(self) -> None:
        from alpha_find_v2.data_ingest.config_models import load_data_sources_config
        config = load_data_sources_config(self._template_path())
        self.assertEqual(len(config.adapters), 3)
        self.assertIn("tushare", config.adapters)
        self.assertIn("akshare", config.adapters)
        self.assertIn("baostock", config.adapters)

    def test_5000_credit_datasets_default_disabled(self) -> None:
        from alpha_find_v2.data_ingest.config_models import load_data_sources_config
        config = load_data_sources_config(self._template_path())
        gated = ["fina_indicator", "income", "balancesheet", "cashflow", "forecast", "express"]
        for ds_id in gated:
            self.assertFalse(
                config.datasets[ds_id].enabled,
                msg=f"5000-credit dataset '{ds_id}' should default to enabled=false",
            )

    def test_priority_returns_enabled_adapters_in_order(self) -> None:
        from alpha_find_v2.data_ingest.config_models import load_data_sources_config
        config = load_data_sources_config(self._template_path())
        # daily has priority = ["tushare", "akshare", "baostock"] but baostock disabled
        prio = config.priority("daily")
        self.assertEqual(prio[0], "tushare")
        self.assertIn("akshare", prio)
        self.assertNotIn("baostock", prio)  # disabled by default

    def test_priority_for_disabled_dataset_is_empty(self) -> None:
        from alpha_find_v2.data_ingest.config_models import load_data_sources_config
        config = load_data_sources_config(self._template_path())
        self.assertEqual(config.priority("fina_indicator"), ())

    def test_malformed_config_unknown_adapter_raises(self) -> None:
        from alpha_find_v2.data_ingest.config_models import load_data_sources_config
        bad_toml = (
            "schema_version = 1\n"
            "[adapter.tushare]\nenabled = true\ncalls_per_minute = 490\ncalls_per_day = 0\n"
            "[datasets.daily]\nenabled = true\ncredit_tier = 120\npriority = [\"ghost_adapter\"]\n"
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".toml", delete=False, encoding="utf-8"
        ) as fh:
            fh.write(bad_toml)
            bad_path = Path(fh.name)
        try:
            with self.assertRaises(ValueError) as ctx:
                load_data_sources_config(bad_path)
            self.assertIn("ghost_adapter", str(ctx.exception))
        finally:
            bad_path.unlink(missing_ok=True)

    def test_malformed_config_invalid_schema_version_raises(self) -> None:
        from alpha_find_v2.data_ingest.config_models import load_data_sources_config
        bad_toml = "schema_version = 99\n"
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".toml", delete=False, encoding="utf-8"
        ) as fh:
            fh.write(bad_toml)
            bad_path = Path(fh.name)
        try:
            with self.assertRaises(ValueError) as ctx:
                load_data_sources_config(bad_path)
            self.assertIn("schema_version", str(ctx.exception))
        finally:
            bad_path.unlink(missing_ok=True)

    def test_malformed_config_invalid_credit_tier_raises(self) -> None:
        from alpha_find_v2.data_ingest.config_models import load_data_sources_config
        bad_toml = (
            "schema_version = 1\n"
            "[adapter.tushare]\nenabled = true\ncalls_per_minute = 490\ncalls_per_day = 0\n"
            "[datasets.daily]\nenabled = true\ncredit_tier = 9999\npriority = [\"tushare\"]\n"
        )
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".toml", delete=False, encoding="utf-8"
        ) as fh:
            fh.write(bad_toml)
            bad_path = Path(fh.name)
        try:
            with self.assertRaises(ValueError) as ctx:
                load_data_sources_config(bad_path)
            self.assertIn("credit_tier", str(ctx.exception))
        finally:
            bad_path.unlink(missing_ok=True)

    def test_enabled_datasets_returns_only_enabled_ids(self) -> None:
        from alpha_find_v2.data_ingest.config_models import load_data_sources_config
        config = load_data_sources_config(self._template_path())
        enabled = config.enabled_datasets()
        self.assertIn("daily", enabled)
        self.assertNotIn("fina_indicator", enabled)


# ---------------------------------------------------------------------------
# rate_limiter.py tests
# ---------------------------------------------------------------------------


class TokenBucketTest(unittest.TestCase):
    def _make_fake_clock_and_sleep(self) -> tuple[list[float], object, object]:
        """Return (time_list, clock_fn, sleep_fn) backed by a mutable float."""
        times: list[float] = [0.0]

        def clock() -> float:
            return times[0]

        def sleep(secs: float) -> None:
            # Advance the fake clock by the requested sleep duration
            times[0] += secs

        return times, clock, sleep  # type: ignore[return-value]

    def test_acquire_succeeds_immediately_when_tokens_available(self) -> None:
        times, clock, sleep = self._make_fake_clock_and_sleep()
        bucket = TokenBucket(60, _clock=clock, _sleep=sleep)
        # 60 tokens available at construction; should succeed without sleeping
        bucket.acquire(timeout=0.001)

    def test_acquire_blocks_when_no_tokens(self) -> None:
        """Drain all tokens then verify next acquire must wait."""
        times, clock, sleep = self._make_fake_clock_and_sleep()
        bucket = TokenBucket(2, _clock=clock, _sleep=sleep)  # 2 tokens/min
        # Drain both initial tokens instantly
        bucket.acquire()
        bucket.acquire()
        # Now no tokens; with timeout=0.0 should raise immediately
        with self.assertRaises(RateLimitTimeout):
            bucket.acquire(timeout=0.0)

    def test_tokens_refill_over_time(self) -> None:
        times, clock, sleep = self._make_fake_clock_and_sleep()
        bucket = TokenBucket(60, _clock=clock, _sleep=sleep)  # 1 token/s
        # Drain all 60 tokens
        for _ in range(60):
            bucket.acquire()
        # Advance fake clock by 1 second → 1 new token
        times[0] += 1.0  # type: ignore[index]
        # acquire() should succeed (fake sleep advances clock, allowing refill)
        bucket.acquire(timeout=2.0)

    def test_daily_cap_blocks_after_limit(self) -> None:
        times, clock, sleep = self._make_fake_clock_and_sleep()
        bucket = TokenBucket(1000, daily_cap=3, _clock=clock, _sleep=sleep)
        bucket.acquire()
        bucket.acquire()
        bucket.acquire()
        from alpha_find_v2.data_ingest.rate_limiter import DailyCapExhausted
        with self.assertRaises(DailyCapExhausted):
            bucket.acquire(timeout=0.001)

    def test_daily_exhausted_returns_true_after_cap(self) -> None:
        times, clock, sleep = self._make_fake_clock_and_sleep()
        bucket = TokenBucket(1000, daily_cap=2, _clock=clock, _sleep=sleep)
        self.assertFalse(bucket.daily_exhausted())
        bucket.acquire()
        bucket.acquire()
        self.assertTrue(bucket.daily_exhausted())

    def test_rate_per_minute_zero_raises(self) -> None:
        with self.assertRaises(ValueError):
            TokenBucket(0)

    def test_daily_cap_negative_raises(self) -> None:
        with self.assertRaises(ValueError):
            TokenBucket(60, daily_cap=-1)

    def test_thread_safety_concurrent_acquires(self) -> None:
        """Multiple threads acquiring from same bucket should not exceed tokens."""
        acquired: list[bool] = []
        lock = threading.Lock()
        bucket = TokenBucket(1000)  # plenty of tokens; uses real clock/sleep

        def _worker() -> None:
            try:
                bucket.acquire(timeout=1.0)
                with lock:
                    acquired.append(True)
            except RateLimitTimeout:
                with lock:
                    acquired.append(False)

        threads = [threading.Thread(target=_worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # All 20 should have acquired (bucket starts with 1000 tokens)
        self.assertEqual(acquired.count(True), 20)


# ---------------------------------------------------------------------------
# templates __init__.py needed for import to work in test
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    unittest.main()
