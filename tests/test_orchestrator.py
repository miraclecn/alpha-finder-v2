"""
Tests for orchestrator.py — tasks 9, 10, 11, 12.

All tests use in-memory DuckDB and canned adapters; no network calls.
"""
from __future__ import annotations

import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import MagicMock

import duckdb
import pytest

from alpha_find_v2.data_ingest.adapters.base import (
    AdapterPermissionError,
    AdapterRateLimitError,
)
from alpha_find_v2.data_ingest.orchestrator import (
    DatasetSyncState,
    SyncReport,
    _ensure_meta_schema,
    _load_state,
    _next_day,
    _pick_adapter,
    _record_state,
    _with_retries,
    _write_dataset,
    sync,
)
from alpha_find_v2.data_ingest.schemas import META_DDL, RAW_TABLE_DDL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_conn() -> Any:
    """Return a fresh in-memory DuckDB connection with meta schema ready."""
    conn = duckdb.connect(":memory:")
    _ensure_meta_schema(conn)
    return conn


def _make_state(
    dataset_id: str = "daily",
    adapter: str = "tushare",
    last_trade_date: str | None = None,
    last_period_end: str | None = None,
    last_status: str = "success",
    last_row_count: int = 0,
    error_message: str | None = None,
) -> DatasetSyncState:
    return DatasetSyncState(
        dataset_id=dataset_id,
        adapter=adapter,
        last_trade_date=last_trade_date,
        last_period_end=last_period_end,
        last_run_at=datetime.now(UTC),
        last_status=last_status,
        last_row_count=last_row_count,
        error_message=error_message,
    )


def _minimal_config(datasets: dict[str, Any] | None = None) -> Any:
    """Build a minimal DataSourcesConfig for testing."""
    from alpha_find_v2.data_ingest.config_models import (
        AdapterConfig,
        DatasetConfig,
        DataSourcesConfig,
    )
    adapters = {
        "tushare": AdapterConfig(name="tushare", enabled=True, calls_per_minute=490, calls_per_day=0),
        "akshare": AdapterConfig(name="akshare", enabled=True, calls_per_minute=60, calls_per_day=0),
    }
    _datasets: dict[str, DatasetConfig] = {}
    if datasets:
        for ds_id, kwargs in datasets.items():
            _datasets[ds_id] = DatasetConfig(
                dataset_id=ds_id,
                enabled=kwargs.get("enabled", True),
                credit_tier=kwargs.get("credit_tier", 120),
                priority=tuple(kwargs.get("priority", ["tushare"])),
            )
    return DataSourcesConfig(schema_version=1, adapters=adapters, datasets=_datasets)


def _stock_basic_rows(n: int = 3) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    return [
        {
            "ts_code": f"00000{i}.SZ",
            "symbol": f"00000{i}",
            "name": f"Stock{i}",
            "area": "上海",
            "industry": "银行",
            "list_date": "20100101",
            "delist_date": None,
            "is_hs": "H",
            "source_table": "tushare.stock_basic",
            "ingested_at": now,
        }
        for i in range(n)
    ]


def _daily_rows(trade_dates: list[str], ts_code: str = "000001.SZ") -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    return [
        {
            "ts_code": ts_code,
            "trade_date": td,
            "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5,
            "pre_close": 10.0, "change": 0.5, "pct_chg": 5.0,
            "vol": 100000.0, "amount": 1050000.0,
            "source_table": "tushare.daily",
            "ingested_at": now,
        }
        for td in trade_dates
    ]


class CannedAdapter:
    """Simple canned adapter for testing. Returns fixed rows for one dataset."""

    def __init__(
        self,
        name: str,
        dataset_id: str,
        rows: list[dict[str, Any]],
        captured_since: list[str | None] | None = None,
    ) -> None:
        self.name = name
        self._dataset_id = dataset_id
        self._rows = rows
        self._captured_since = captured_since  # will record the 'since' passed to fetch

    def supports(self, dataset_id: str) -> bool:
        return dataset_id == self._dataset_id

    def fetch(
        self, dataset_id: str, *, since: str | None, until: str | None, full: bool
    ) -> Iterator[dict[str, Any]]:
        if self._captured_since is not None:
            self._captured_since.append(since)
        yield from self._rows


class MultiCannedAdapter:
    """Canned adapter supporting multiple datasets. Maps dataset_id → rows."""

    def __init__(self, name: str, dataset_rows: dict[str, list[dict[str, Any]]]) -> None:
        self.name = name
        self._dataset_rows = dataset_rows

    def supports(self, dataset_id: str) -> bool:
        return dataset_id in self._dataset_rows

    def fetch(
        self, dataset_id: str, *, since: str | None, until: str | None, full: bool
    ) -> Iterator[dict[str, Any]]:
        yield from self._dataset_rows.get(dataset_id, [])


class FailingAdapter:
    """Adapter that always raises the given exception."""

    def __init__(self, name: str, exc: Exception, dataset_id: str = "stock_basic") -> None:
        self.name = name
        self._exc = exc
        self._dataset_id = dataset_id
        self.call_count = 0

    def supports(self, dataset_id: str) -> bool:
        return dataset_id == self._dataset_id

    def fetch(
        self, dataset_id: str, *, since: str | None, until: str | None, full: bool
    ) -> Iterator[dict[str, Any]]:
        self.call_count += 1
        raise self._exc
        yield  # make it a generator


class OnceFailingAdapter:
    """Raises on first call, succeeds on subsequent calls."""

    def __init__(
        self,
        name: str,
        exc: Exception,
        dataset_id: str,
        rows: list[dict[str, Any]],
    ) -> None:
        self.name = name
        self._exc = exc
        self._dataset_id = dataset_id
        self._rows = rows
        self.call_count = 0

    def supports(self, dataset_id: str) -> bool:
        return dataset_id == self._dataset_id

    def fetch(
        self, dataset_id: str, *, since: str | None, until: str | None, full: bool
    ) -> Iterator[dict[str, Any]]:
        self.call_count += 1
        if self.call_count == 1:
            raise self._exc
        yield from self._rows


class PartialAdapter:
    """Yields n rows then raises."""

    def __init__(self, name: str, dataset_id: str, rows: list[dict[str, Any]], fail_after: int) -> None:
        self.name = name
        self._dataset_id = dataset_id
        self._rows = rows
        self._fail_after = fail_after

    def supports(self, dataset_id: str) -> bool:
        return dataset_id == self._dataset_id

    def fetch(
        self, dataset_id: str, *, since: str | None, until: str | None, full: bool
    ) -> Iterator[dict[str, Any]]:
        for i, row in enumerate(self._rows):
            if i >= self._fail_after:
                raise RuntimeError("mid-stream failure")
            yield row


# ---------------------------------------------------------------------------
# Test 1: Sync state I/O round-trip (Task 9)
# ---------------------------------------------------------------------------


class TestSyncStateIO:
    def test_round_trip_all_fields(self) -> None:
        conn = _make_conn()
        ts = datetime(2024, 3, 15, 10, 30, 0, tzinfo=UTC)
        state = DatasetSyncState(
            dataset_id="daily",
            adapter="tushare",
            last_trade_date="20240314",
            last_period_end="20231231",
            last_run_at=ts,
            last_status="success",
            last_row_count=42,
            error_message=None,
            schema_version=1,
        )
        _record_state(conn, state)
        loaded = _load_state(conn)

        assert "daily" in loaded
        s = loaded["daily"]
        assert s.dataset_id == "daily"
        assert s.adapter == "tushare"
        assert s.last_trade_date == "20240314"
        assert s.last_period_end == "20231231"
        assert s.last_status == "success"
        assert s.last_row_count == 42
        assert s.error_message is None
        assert s.schema_version == 1

    def test_upsert_overwrites_existing(self) -> None:
        conn = _make_conn()
        state1 = _make_state("daily", last_status="failed", last_row_count=0)
        _record_state(conn, state1)

        state2 = _make_state("daily", last_status="success", last_row_count=99)
        _record_state(conn, state2)

        loaded = _load_state(conn)
        assert loaded["daily"].last_status == "success"
        assert loaded["daily"].last_row_count == 99

    def test_multiple_datasets_stored_independently(self) -> None:
        conn = _make_conn()
        _record_state(conn, _make_state("daily"))
        _record_state(conn, _make_state("stock_basic"))
        loaded = _load_state(conn)
        assert "daily" in loaded
        assert "stock_basic" in loaded
        assert loaded["daily"].dataset_id == "daily"
        assert loaded["stock_basic"].dataset_id == "stock_basic"

    def test_load_empty_returns_empty_dict(self) -> None:
        conn = _make_conn()
        loaded = _load_state(conn)
        assert loaded == {}

    def test_error_message_stored_and_retrieved(self) -> None:
        conn = _make_conn()
        state = _make_state("daily", last_status="failed", error_message="API timeout")
        _record_state(conn, state)
        loaded = _load_state(conn)
        assert loaded["daily"].error_message == "API timeout"


# ---------------------------------------------------------------------------
# Test 2: Full sync with canned adapter (Task 10)
# ---------------------------------------------------------------------------


class TestFullSync:
    def test_sync_writes_rows_to_raw_table(self, tmp_path: Path) -> None:
        rows = _stock_basic_rows(3)
        adapter = CannedAdapter("tushare", "stock_basic", rows)
        config = _minimal_config({"stock_basic": {"priority": ["tushare"]}})

        raw_db = tmp_path / "raw.duckdb"
        report = sync(
            raw_db_path=raw_db,
            config=config,
            adapters=[adapter],
            only={"stock_basic"},
        )

        assert len(report.results) == 1
        result = report.results[0]
        assert result.dataset_id == "stock_basic"
        assert result.rows_added == 3
        assert result.status == "success"

        # Verify rows landed in the database
        conn = duckdb.connect(str(raw_db))
        count = conn.execute("SELECT COUNT(*) FROM stock_basic_ref").fetchone()[0]
        conn.close()
        assert count == 3

    def test_sync_in_memory_writes_rows(self) -> None:
        """Use :memory: path directly."""
        rows = _stock_basic_rows(3)
        adapter = CannedAdapter("tushare", "stock_basic", rows)
        config = _minimal_config({"stock_basic": {"priority": ["tushare"]}})

        # With :memory: path we pass a Path object; sync handles it
        report = sync(
            raw_db_path=Path(":memory:"),
            config=config,
            adapters=[adapter],
            only={"stock_basic"},
        )
        assert report.results[0].rows_added == 3
        assert report.results[0].status == "success"

    def test_sync_sets_success_state(self, tmp_path: Path) -> None:
        rows = _stock_basic_rows(2)
        adapter = CannedAdapter("tushare", "stock_basic", rows)
        config = _minimal_config({"stock_basic": {"priority": ["tushare"]}})

        raw_db = tmp_path / "raw.duckdb"
        sync(raw_db_path=raw_db, config=config, adapters=[adapter], only={"stock_basic"})

        conn = duckdb.connect(str(raw_db))
        state_rows = conn.execute(
            "SELECT last_status, last_row_count FROM meta.dataset_sync_state WHERE dataset_id='stock_basic'"
        ).fetchone()
        conn.close()
        assert state_rows is not None
        assert state_rows[0] == "success"
        assert state_rows[1] == 2

    def test_dry_run_makes_no_api_calls(self, tmp_path: Path) -> None:
        called = []

        class TrackingAdapter:
            name = "tushare"

            def supports(self, _: str) -> bool:
                return True

            def fetch(self, *a: Any, **kw: Any) -> Iterator[dict[str, Any]]:
                called.append(True)
                return iter([])

        config = _minimal_config({"stock_basic": {"priority": ["tushare"]}})
        raw_db = tmp_path / "raw.duckdb"
        report = sync(
            raw_db_path=raw_db,
            config=config,
            adapters=[TrackingAdapter()],
            only={"stock_basic"},
            dry_run=True,
        )
        assert called == []
        assert report.results[0].status == "skipped"


# ---------------------------------------------------------------------------
# Test 3: Incremental (Task 11)
# ---------------------------------------------------------------------------


class TestIncremental:
    def test_second_sync_uses_next_day_as_since(self, tmp_path: Path) -> None:
        """After first sync, second sync calls adapter with since=last_trade_date+1."""
        trade_dates_1 = ["20240101", "20240102", "20240103"]
        captured_since: list[str | None] = []

        adapter = CannedAdapter("tushare", "daily", _daily_rows(trade_dates_1), captured_since)
        config = _minimal_config({"daily": {"priority": ["tushare"]}})
        raw_db = tmp_path / "raw.duckdb"

        # First sync — full pull
        sync(raw_db_path=raw_db, config=config, adapters=[adapter], only={"daily"})

        # Second sync — incremental; adapter now returns one new row
        trade_dates_2 = ["20240104"]
        adapter2 = CannedAdapter("tushare", "daily", _daily_rows(trade_dates_2), captured_since)
        sync(raw_db_path=raw_db, config=config, adapters=[adapter2], only={"daily"})

        # The second call's since should be last_trade_date + 1 day
        # captured_since[0] is from first sync (None), captured_since[1] from second
        assert len(captured_since) == 2
        assert captured_since[0] is None  # first sync has no prior state
        assert captured_since[1] == "20240104"  # 20240103 + 1

    def test_incremental_does_not_duplicate_rows(self, tmp_path: Path) -> None:
        """Running sync twice with the same rows should not duplicate."""
        rows = _daily_rows(["20240101", "20240102"])
        config = _minimal_config({"daily": {"priority": ["tushare"]}})
        raw_db = tmp_path / "raw.duckdb"

        sync(raw_db_path=raw_db, config=config, adapters=[CannedAdapter("tushare", "daily", rows)], only={"daily"})
        sync(raw_db_path=raw_db, config=config, adapters=[CannedAdapter("tushare", "daily", rows)], only={"daily"})

        conn = duckdb.connect(str(raw_db))
        count = conn.execute("SELECT COUNT(*) FROM raw_kline_unadj").fetchone()[0]
        conn.close()
        assert count == 2  # no duplicates

    def test_state_last_trade_date_set_after_sync(self, tmp_path: Path) -> None:
        rows = _daily_rows(["20240101", "20240115", "20240110"])
        adapter = CannedAdapter("tushare", "daily", rows)
        config = _minimal_config({"daily": {"priority": ["tushare"]}})
        raw_db = tmp_path / "raw.duckdb"

        sync(raw_db_path=raw_db, config=config, adapters=[adapter], only={"daily"})

        conn = duckdb.connect(str(raw_db))
        row = conn.execute(
            "SELECT last_trade_date FROM meta.dataset_sync_state WHERE dataset_id='daily'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "20240115"  # max trade_date


# ---------------------------------------------------------------------------
# Test 4: --reset clears state and table (Task 11)
# ---------------------------------------------------------------------------


class TestReset:
    def test_reset_clears_state_row_and_recreates_table(self, tmp_path: Path) -> None:
        rows = _daily_rows(["20240101", "20240102"])
        config = _minimal_config({
            "daily": {"priority": ["tushare"]},
            "stock_basic": {"priority": ["tushare"]},
        })
        raw_db = tmp_path / "raw.duckdb"

        # First sync — use multi-dataset adapter to avoid name collision
        first_adapter = MultiCannedAdapter("tushare", {
            "daily": rows,
            "stock_basic": _stock_basic_rows(2),
        })
        sync(
            raw_db_path=raw_db,
            config=config,
            adapters=[first_adapter],
            only={"daily", "stock_basic"},
        )

        # Verify rows exist
        conn = duckdb.connect(str(raw_db))
        pre_daily = conn.execute("SELECT COUNT(*) FROM raw_kline_unadj").fetchone()[0]
        pre_sb = conn.execute("SELECT COUNT(*) FROM stock_basic_ref").fetchone()[0]
        conn.close()
        assert pre_daily == 2
        assert pre_sb == 2

        # Reset only "daily"
        reset_adapter = MultiCannedAdapter("tushare", {"daily": [], "stock_basic": []})
        sync(
            raw_db_path=raw_db,
            config=config,
            adapters=[reset_adapter],
            only={"daily"},
            reset={"daily"},
        )

        conn = duckdb.connect(str(raw_db))
        post_daily = conn.execute("SELECT COUNT(*) FROM raw_kline_unadj").fetchone()[0]
        post_sb = conn.execute("SELECT COUNT(*) FROM stock_basic_ref").fetchone()[0]
        daily_state = conn.execute(
            "SELECT * FROM meta.dataset_sync_state WHERE dataset_id='daily'"
        ).fetchone()
        conn.close()

        # daily table was reset (0 rows after reset + empty adapter)
        assert post_daily == 0
        # stock_basic untouched
        assert post_sb == 2
        # daily state was written fresh (status success with 0 rows)
        assert daily_state is not None

    def test_reset_removes_prior_state_before_new_sync(self, tmp_path: Path) -> None:
        rows = _daily_rows(["20240101"])
        config = _minimal_config({"daily": {"priority": ["tushare"]}})
        raw_db = tmp_path / "raw.duckdb"

        # First sync
        sync(raw_db_path=raw_db, config=config, adapters=[CannedAdapter("tushare", "daily", rows)], only={"daily"})

        # Check state set
        conn = duckdb.connect(str(raw_db))
        before = conn.execute("SELECT last_trade_date FROM meta.dataset_sync_state WHERE dataset_id='daily'").fetchone()
        conn.close()
        assert before[0] == "20240101"

        # Reset and resync
        new_rows = _daily_rows(["20240201"])
        sync(
            raw_db_path=raw_db,
            config=config,
            adapters=[CannedAdapter("tushare", "daily", new_rows)],
            only={"daily"},
            reset={"daily"},
        )

        conn = duckdb.connect(str(raw_db))
        count = conn.execute("SELECT COUNT(*) FROM raw_kline_unadj").fetchone()[0]
        after = conn.execute("SELECT last_trade_date FROM meta.dataset_sync_state WHERE dataset_id='daily'").fetchone()
        conn.close()
        assert count == 1
        assert after[0] == "20240201"


# ---------------------------------------------------------------------------
# Test 5: AdapterPermissionError → permission_denied, no retry (Task 10)
# ---------------------------------------------------------------------------


class TestPermissionDenied:
    def test_permission_error_gives_permission_denied_status(self, tmp_path: Path) -> None:
        adapter = FailingAdapter("tushare", AdapterPermissionError("no credits"), "stock_basic")
        config = _minimal_config({"stock_basic": {"priority": ["tushare"]}})
        raw_db = tmp_path / "raw.duckdb"

        report = sync(
            raw_db_path=raw_db,
            config=config,
            adapters=[adapter],
            only={"stock_basic"},
        )

        result = report.results[0]
        # When all adapters deny, status is permission_denied
        assert result.status == "permission_denied"
        assert result.rows_added == 0

    def test_permission_error_not_retried(self, tmp_path: Path) -> None:
        adapter = FailingAdapter("tushare", AdapterPermissionError("no credits"), "stock_basic")
        config = _minimal_config({"stock_basic": {"priority": ["tushare"]}})
        raw_db = tmp_path / "raw.duckdb"

        sync(
            raw_db_path=raw_db,
            config=config,
            adapters=[adapter],
            only={"stock_basic"},
        )

        # Should only be called once (no retries on permission error)
        assert adapter.call_count == 1

    def test_with_retries_does_not_retry_permission_error(self) -> None:
        calls = []

        def _call() -> None:
            calls.append(1)
            raise AdapterPermissionError("denied")

        sleep_calls = []
        with pytest.raises(AdapterPermissionError):
            _with_retries(_call, max_attempts=3, base_delay=1.0, _sleep=lambda s: sleep_calls.append(s))

        assert len(calls) == 1
        assert sleep_calls == []


# ---------------------------------------------------------------------------
# Test 6: AdapterRateLimitError → retry succeeds (Task 10)
# ---------------------------------------------------------------------------


class TestRetry:
    def test_rate_limit_error_retried_and_succeeds(self, tmp_path: Path) -> None:
        rows = _stock_basic_rows(3)
        adapter = OnceFailingAdapter("tushare", AdapterRateLimitError("rate limited"), "stock_basic", rows)
        config = _minimal_config({"stock_basic": {"priority": ["tushare"]}})
        raw_db = tmp_path / "raw.duckdb"

        sleep_calls: list[float] = []
        report = sync(
            raw_db_path=raw_db,
            config=config,
            adapters=[adapter],
            only={"stock_basic"},
            _sleep=lambda s: sleep_calls.append(s),
        )

        assert report.results[0].status == "success"
        assert report.results[0].rows_added == 3
        assert adapter.call_count == 2  # one failure + one success
        assert len(sleep_calls) == 1    # slept once between attempts

    def test_with_retries_retries_rate_limit_error(self) -> None:
        calls = []
        sleep_calls: list[float] = []

        def _call() -> str:
            calls.append(1)
            if len(calls) < 2:
                raise AdapterRateLimitError("rate limited")
            return "ok"

        result = _with_retries(
            _call, max_attempts=3, base_delay=1.0, _sleep=lambda s: sleep_calls.append(s)
        )
        assert result == "ok"
        assert len(calls) == 2
        assert len(sleep_calls) == 1

    def test_with_retries_retries_oserror(self) -> None:
        calls = []
        sleep_calls: list[float] = []

        def _call() -> str:
            calls.append(1)
            if len(calls) < 3:
                raise OSError("connection reset")
            return "ok"

        result = _with_retries(
            _call, max_attempts=3, base_delay=1.0, _sleep=lambda s: sleep_calls.append(s)
        )
        assert result == "ok"
        assert len(calls) == 3
        assert len(sleep_calls) == 2

    def test_with_retries_exhausted_raises_last_exception(self) -> None:
        sleep_calls: list[float] = []

        def _call() -> None:
            raise AdapterRateLimitError("always rate limited")

        with pytest.raises(AdapterRateLimitError):
            _with_retries(_call, max_attempts=3, base_delay=1.0, _sleep=lambda s: sleep_calls.append(s))
        assert len(sleep_calls) == 2  # slept before attempt 2 and 3

    def test_with_retries_does_not_retry_generic_exception(self) -> None:
        calls = []

        def _call() -> None:
            calls.append(1)
            raise ValueError("not retriable")

        with pytest.raises(ValueError):
            _with_retries(_call, max_attempts=3, base_delay=1.0, _sleep=lambda _: None)
        assert len(calls) == 1


# ---------------------------------------------------------------------------
# Test 7: Mid-stream exception rolls back (Task 10)
# ---------------------------------------------------------------------------


class TestAtomicity:
    def test_midstream_exception_rolls_back(self, tmp_path: Path) -> None:
        """Adapter yields 2 rows then raises; raw table must remain unchanged."""
        # First sync puts 2 known rows in
        initial_rows = _daily_rows(["20240101", "20240102"])
        config = _minimal_config({"daily": {"priority": ["tushare"]}})
        raw_db = tmp_path / "raw.duckdb"

        sync(
            raw_db_path=raw_db,
            config=config,
            adapters=[CannedAdapter("tushare", "daily", initial_rows)],
            only={"daily"},
        )

        # Second sync: adapter yields new rows then fails mid-stream
        partial_rows = _daily_rows(["20240103", "20240104", "20240105"])
        failing_adapter = PartialAdapter("tushare", "daily", partial_rows, fail_after=2)

        report = sync(
            raw_db_path=raw_db,
            config=config,
            adapters=[failing_adapter],
            only={"daily"},
        )

        result = report.results[0]
        assert result.status == "failed"

        # Table should still have original 2 rows only (rollback worked)
        conn = duckdb.connect(str(raw_db))
        count = conn.execute("SELECT COUNT(*) FROM raw_kline_unadj").fetchone()[0]
        conn.close()
        assert count == 2

    def test_write_dataset_transactional_delete_insert(self) -> None:
        """_write_dataset does DELETE+INSERT in one transaction."""
        conn = duckdb.connect(":memory:")
        conn.execute(RAW_TABLE_DDL["daily"])

        # Insert existing row
        now = datetime.now(UTC)
        existing = {
            "ts_code": "000001.SZ", "trade_date": "20240101",
            "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5,
            "pre_close": 10.0, "change": 0.5, "pct_chg": 5.0,
            "vol": 100000.0, "amount": 1050000.0,
            "source_table": "tushare.daily", "ingested_at": now,
        }
        conn.execute(
            "INSERT INTO raw_kline_unadj VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            list(existing.values()),
        )

        # Write same PK with updated close
        updated = dict(existing, close=12.0)
        count = _write_dataset(conn, "daily", [updated])
        assert count == 1

        # Only 1 row (no duplicate)
        rows = conn.execute("SELECT close FROM raw_kline_unadj").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 12.0


# ---------------------------------------------------------------------------
# Test 8: Fallback chain (Task 12)
# ---------------------------------------------------------------------------


class TestFallback:
    def test_tushare_permission_error_falls_back_to_akshare(self, tmp_path: Path) -> None:
        """Priority=[tushare,akshare]; tushare raises PermissionError; akshare yields rows."""
        from alpha_find_v2.data_ingest.config_models import (
            AdapterConfig, DatasetConfig, DataSourcesConfig,
        )

        rows = _daily_rows(["20240101", "20240102"])
        tushare_adapter = FailingAdapter("tushare", AdapterPermissionError("no token"), "daily")
        akshare_adapter = CannedAdapter("akshare", "daily", rows)

        # Config with priority [tushare, akshare] for daily
        config = DataSourcesConfig(
            schema_version=1,
            adapters={
                "tushare": AdapterConfig(name="tushare", enabled=True, calls_per_minute=490, calls_per_day=0),
                "akshare": AdapterConfig(name="akshare", enabled=True, calls_per_minute=60, calls_per_day=0),
            },
            datasets={
                "daily": DatasetConfig(
                    dataset_id="daily", enabled=True, credit_tier=120,
                    priority=("tushare", "akshare"),
                ),
            },
        )

        raw_db = tmp_path / "raw.duckdb"
        report = sync(
            raw_db_path=raw_db,
            config=config,
            adapters=[tushare_adapter, akshare_adapter],
            only={"daily"},
        )

        result = report.results[0]
        assert result.status == "success"
        assert result.adapter == "akshare"
        assert result.rows_added == 2

        # Verify state records akshare
        conn = duckdb.connect(str(raw_db))
        adapter_name = conn.execute(
            "SELECT adapter FROM meta.dataset_sync_state WHERE dataset_id='daily'"
        ).fetchone()[0]
        conn.close()
        assert adapter_name == "akshare"

    def test_pick_adapter_returns_first_supporting(self) -> None:
        from alpha_find_v2.data_ingest.config_models import (
            AdapterConfig, DatasetConfig, DataSourcesConfig,
        )

        adapter_a = CannedAdapter("tushare", "daily", [])
        adapter_b = CannedAdapter("akshare", "daily", [])

        config = DataSourcesConfig(
            schema_version=1,
            adapters={
                "tushare": AdapterConfig(name="tushare", enabled=True, calls_per_minute=490, calls_per_day=0),
                "akshare": AdapterConfig(name="akshare", enabled=True, calls_per_minute=60, calls_per_day=0),
            },
            datasets={
                "daily": DatasetConfig(
                    dataset_id="daily", enabled=True, credit_tier=120,
                    priority=("tushare", "akshare"),
                ),
            },
        )

        result = _pick_adapter([adapter_a, adapter_b], "daily", config)
        assert result is not None
        picked_adapter, picked_name = result
        assert picked_name == "tushare"

    def test_pick_adapter_skips_non_supporting(self) -> None:
        from alpha_find_v2.data_ingest.config_models import (
            AdapterConfig, DatasetConfig, DataSourcesConfig,
        )

        # tushare only supports stock_basic, not daily
        adapter_a = CannedAdapter("tushare", "stock_basic", [])
        adapter_b = CannedAdapter("akshare", "daily", [])

        config = DataSourcesConfig(
            schema_version=1,
            adapters={
                "tushare": AdapterConfig(name="tushare", enabled=True, calls_per_minute=490, calls_per_day=0),
                "akshare": AdapterConfig(name="akshare", enabled=True, calls_per_minute=60, calls_per_day=0),
            },
            datasets={
                "daily": DatasetConfig(
                    dataset_id="daily", enabled=True, credit_tier=120,
                    priority=("tushare", "akshare"),
                ),
            },
        )

        result = _pick_adapter([adapter_a, adapter_b], "daily", config)
        assert result is not None
        _, name = result
        assert name == "akshare"


# ---------------------------------------------------------------------------
# Test 9: All adapters fail (Task 12)
# ---------------------------------------------------------------------------


class TestAllAdaptersFail:
    def test_all_adapters_permission_denied_gives_failed_with_message(self, tmp_path: Path) -> None:
        from alpha_find_v2.data_ingest.config_models import (
            AdapterConfig, DatasetConfig, DataSourcesConfig,
        )

        tushare_adapter = FailingAdapter("tushare", AdapterPermissionError("no tushare"), "daily")
        akshare_adapter = FailingAdapter("akshare", AdapterPermissionError("no akshare"), "daily")

        config = DataSourcesConfig(
            schema_version=1,
            adapters={
                "tushare": AdapterConfig(name="tushare", enabled=True, calls_per_minute=490, calls_per_day=0),
                "akshare": AdapterConfig(name="akshare", enabled=True, calls_per_minute=60, calls_per_day=0),
            },
            datasets={
                "daily": DatasetConfig(
                    dataset_id="daily", enabled=True, credit_tier=120,
                    priority=("tushare", "akshare"),
                ),
            },
        )

        raw_db = tmp_path / "raw.duckdb"
        report = sync(
            raw_db_path=raw_db,
            config=config,
            adapters=[tushare_adapter, akshare_adapter],
            only={"daily"},
        )

        result = report.results[0]
        assert result.status == "permission_denied"
        assert result.rows_added == 0
        # Error message should name the failed attempts
        assert result.error_message is not None
        assert "tushare" in result.error_message or "akshare" in result.error_message

    def test_no_adapter_available_gives_failed(self, tmp_path: Path) -> None:
        """Dataset with empty priority list → failed."""
        from alpha_find_v2.data_ingest.config_models import (
            AdapterConfig, DatasetConfig, DataSourcesConfig,
        )

        config = DataSourcesConfig(
            schema_version=1,
            adapters={
                "tushare": AdapterConfig(name="tushare", enabled=True, calls_per_minute=490, calls_per_day=0),
            },
            datasets={
                "stock_basic": DatasetConfig(
                    dataset_id="stock_basic", enabled=True, credit_tier=120,
                    priority=(),  # empty — no adapter
                ),
            },
        )

        raw_db = tmp_path / "raw.duckdb"
        report = sync(
            raw_db_path=raw_db,
            config=config,
            adapters=[],
            only={"stock_basic"},
        )

        result = report.results[0]
        assert result.status == "failed"
        assert result.rows_added == 0


# ---------------------------------------------------------------------------
# Helpers tests
# ---------------------------------------------------------------------------


class TestNextDay:
    def test_next_day_basic(self) -> None:
        assert _next_day("20240101") == "20240102"

    def test_next_day_end_of_month(self) -> None:
        assert _next_day("20240131") == "20240201"

    def test_next_day_end_of_year(self) -> None:
        assert _next_day("20231231") == "20240101"

    def test_next_day_leap_year(self) -> None:
        assert _next_day("20240228") == "20240229"


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
