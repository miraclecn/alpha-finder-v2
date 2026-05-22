"""
End-to-end smoke test for the data ingestion pipeline.

Exercises the full pipeline using an in-memory SmokeAdapter:
  1. init_workspace
  2. sync (full) — 3 securities, 30 trading days
  3. sync (incremental) — assert new rows added on second run
  4. build_research_source_db — assert dataset_registry rows
  5. run_audit — assert no blocking failures

All tests must complete in under 10 seconds.

**Validates: Requirements R2.6, R3.2, R5.1**
"""
from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

import duckdb
import pytest

from alpha_find_v2.data_ingest.config_models import (
    AdapterConfig,
    DatasetConfig,
    DataSourcesConfig,
)
from alpha_find_v2.data_ingest.init_workspace import init_workspace
from alpha_find_v2.data_ingest.orchestrator import sync
from alpha_find_v2.data_ingest.audit import run_audit
from alpha_find_v2.market_data_bootstrap import build_research_source_db


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SECURITIES = ["000001.SZ", "000002.SZ", "600001.SH"]
_SYMBOLS = {"000001.SZ": "000001", "000002.SZ": "000002", "600001.SH": "600001"}

_START_DATE = datetime(2024, 1, 2)
_NUM_DAYS = 30

# 30 consecutive weekday dates starting from 2024-01-02
_WEEKDAYS: list[str] = []
_d = _START_DATE
while len(_WEEKDAYS) < _NUM_DAYS:
    if _d.weekday() < 5:  # Mon–Fri
        _WEEKDAYS.append(_d.strftime("%Y%m%d"))
    _d += timedelta(days=1)

_LAST_DATE = _WEEKDAYS[-1]  # "20240214" (30th weekday from 2024-01-02)
_NEXT_DATE = (datetime.strptime(_LAST_DATE, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# SmokeAdapter
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(UTC)


def _make_stock_basic_rows(securities: list[str]) -> list[dict[str, Any]]:
    now = _now()
    return [
        {
            "ts_code": ts,
            "symbol": _SYMBOLS.get(ts, ts.split(".")[0]),
            "name": f"TestStock_{ts}",
            "area": "上海",
            "industry": "银行",
            "list_date": "20100101",
            "delist_date": None,
            "is_hs": "H",
            "source_table": "smoke.fake",
            "ingested_at": now,
        }
        for ts in securities
    ]


def _make_trade_cal_rows(dates: list[str]) -> list[dict[str, Any]]:
    now = _now()
    rows = []
    for i, cal_date in enumerate(dates):
        pretrade_date = dates[i - 1] if i > 0 else cal_date
        rows.append({
            "exchange": "SSE",
            "cal_date": cal_date,
            "is_open": 1,
            "pretrade_date": pretrade_date,
            "source_table": "smoke.fake",
            "ingested_at": now,
        })
    return rows


def _make_namechange_rows(securities: list[str]) -> list[dict[str, Any]]:
    now = _now()
    return [
        {
            "ts_code": ts,
            "name": f"TestStock_{ts}",
            "start_date": "20100101",
            "end_date": None,
            "ann_date": "20100101",
            "change_reason": "IPO",
            "source_table": "smoke.fake",
            "ingested_at": now,
        }
        for ts in securities
    ]


def _make_daily_rows(securities: list[str], dates: list[str]) -> list[dict[str, Any]]:
    now = _now()
    rows = []
    for ts in securities:
        for td in dates:
            rows.append({
                "ts_code": ts,
                "trade_date": td,
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "pre_close": 10.0,
                "change": 0.5,
                "pct_chg": 5.0,
                "vol": 100000.0,
                "amount": 1050000.0,
                "source_table": "smoke.fake",
                "ingested_at": now,
            })
    return rows


def _make_daily_basic_rows(securities: list[str], dates: list[str]) -> list[dict[str, Any]]:
    now = _now()
    rows = []
    for ts in securities:
        for td in dates:
            rows.append({
                "ts_code": ts,
                "trade_date": td,
                "close": 10.5,
                "turnover_rate": 1.0,
                "turnover_rate_f": 0.9,
                "volume_ratio": 1.0,
                "pe": 15.0,
                "pe_ttm": 14.5,
                "pb": 1.2,
                "ps": 2.0,
                "ps_ttm": 1.9,
                "dv_ratio": 0.5,
                "dv_ttm": 0.5,
                "total_share": 100000.0,
                "float_share": 80000.0,
                "free_share": 70000.0,
                "total_mv": 1050000.0,
                "circ_mv": 840000.0,
                "source_table": "smoke.fake",
                "ingested_at": now,
            })
    return rows


def _make_adj_factor_rows(securities: list[str], dates: list[str]) -> list[dict[str, Any]]:
    now = _now()
    rows = []
    for ts in securities:
        for td in dates:
            rows.append({
                "ts_code": ts,
                "trade_date": td,
                "adj_factor": 1.0,
                "source_table": "smoke.fake",
                "ingested_at": now,
            })
    return rows


def _make_stk_limit_rows(securities: list[str], dates: list[str]) -> list[dict[str, Any]]:
    now = _now()
    rows = []
    for ts in securities:
        for td in dates:
            rows.append({
                "trade_date": td,
                "ts_code": ts,
                "up_limit": 11.5,
                "down_limit": 9.5,
                "pre_close": 10.0,
                "source_table": "smoke.fake",
                "ingested_at": now,
            })
    return rows


def _make_index_daily_rows(dates: list[str]) -> list[dict[str, Any]]:
    now = _now()
    return [
        {
            "ts_code": "000001.SH",
            "trade_date": td,
            "close": 3000.0,
            "open": 2990.0,
            "high": 3020.0,
            "low": 2980.0,
            "pre_close": 2995.0,
            "change": 5.0,
            "pct_chg": 0.17,
            "vol": 5000000.0,
            "amount": 50000000.0,
            "source_table": "smoke.fake",
            "ingested_at": now,
        }
        for td in dates
    ]


def _build_dataset_rows(
    securities: list[str], dates: list[str]
) -> dict[str, list[dict[str, Any]]]:
    """Build all 12 Stage-1 dataset rows."""
    daily = _make_daily_rows(securities, dates)
    return {
        "stock_basic": _make_stock_basic_rows(securities),
        "trade_cal": _make_trade_cal_rows(dates),
        "namechange": _make_namechange_rows(securities),
        "daily": daily,
        "daily_basic": _make_daily_basic_rows(securities, dates),
        "adj_factor": _make_adj_factor_rows(securities, dates),
        "daily_qfq": daily,  # same shape as daily
        "suspend_d": [],
        "stk_limit": _make_stk_limit_rows(securities, dates),
        "index_daily": _make_index_daily_rows(dates),
        "index_weight": [],
        "index_member_all": [],
    }


class SmokeAdapter:
    """Fake adapter yielding canned rows for all 12 Stage-1 datasets."""

    name = "smoke"

    _STAGE1_DATASETS = frozenset({
        "stock_basic", "trade_cal", "namechange",
        "daily", "daily_basic", "adj_factor", "daily_qfq",
        "suspend_d", "stk_limit", "index_daily",
        "index_weight", "index_member_all",
    })

    def __init__(
        self,
        securities: list[str],
        dates: list[str],
    ) -> None:
        self._rows = _build_dataset_rows(securities, dates)

    def supports(self, dataset_id: str) -> bool:
        return dataset_id in self._STAGE1_DATASETS

    def fetch(
        self,
        dataset_id: str,
        *,
        since: str | None = None,
        until: str | None = None,
        full: bool = True,
    ) -> Iterator[dict[str, Any]]:
        yield from self._rows.get(dataset_id, [])


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

_STAGE1_DATASETS = [
    "stock_basic", "trade_cal", "namechange",
    "daily", "daily_basic", "adj_factor", "daily_qfq",
    "suspend_d", "stk_limit", "index_daily",
    "index_weight", "index_member_all",
]


def _make_smoke_config() -> DataSourcesConfig:
    adapters = {
        "smoke": AdapterConfig(
            name="smoke", enabled=True, calls_per_minute=10000, calls_per_day=0
        )
    }
    datasets = {
        ds_id: DatasetConfig(
            dataset_id=ds_id,
            enabled=True,
            credit_tier=120,
            priority=("smoke",),
        )
        for ds_id in _STAGE1_DATASETS
    }
    return DataSourcesConfig(schema_version=1, adapters=adapters, datasets=datasets)


# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------


@pytest.mark.smoke
def test_full_pipeline_smoke(tmp_path: Path) -> None:
    """
    Exercises the full pipeline from init through audit.

    Steps:
      1. init_workspace
      2. sync (full) with SmokeAdapter
      3. build_research_source_db
      4. run_audit — no blocking failures
    """
    # Step 1: init_workspace
    report = init_workspace(tmp_path)
    assert len(report.actions) == 3
    assert all(a.action == "created" for a in report.actions)

    # Step 2: sync full
    raw_db = tmp_path / "output" / "raw.duckdb"
    adapter = SmokeAdapter(_SECURITIES, _WEEKDAYS)
    config = _make_smoke_config()

    sync_report = sync(
        raw_db_path=raw_db,
        config=config,
        adapters=[adapter],
    )

    # All datasets should succeed
    failed = [r for r in sync_report.results if r.status not in ("success", "skipped")]
    assert failed == [], f"Failed datasets: {[(r.dataset_id, r.status, r.error_message) for r in failed]}"

    # Verify row counts in raw.duckdb
    conn = duckdb.connect(str(raw_db))
    daily_count = conn.execute("SELECT COUNT(*) FROM raw_kline_unadj").fetchone()[0]
    assert daily_count == 90, f"Expected 90 daily rows, got {daily_count}"

    # Verify sync state: last_trade_date matches the last weekday
    last_td = conn.execute(
        "SELECT last_trade_date FROM meta.dataset_sync_state WHERE dataset_id='daily'"
    ).fetchone()
    assert last_td is not None and last_td[0] == _LAST_DATE, (
        f"Expected last_trade_date={_LAST_DATE!r}, got {last_td}"
    )
    conn.close()

    # Step 3: build_research_source_db
    target_db = tmp_path / "output" / "research_source.duckdb"
    result = build_research_source_db(raw_db, target_db)
    assert target_db.exists()

    tgt_conn = duckdb.connect(str(target_db))
    tables = {t for (t,) in tgt_conn.execute("SHOW TABLES").fetchall()}
    assert "daily_bar_pit" in tables, f"daily_bar_pit missing from {tables}"
    assert "security_master_ref" in tables, f"security_master_ref missing from {tables}"

    registry_ids = {
        row[0]
        for row in tgt_conn.execute("SELECT dataset_id FROM dataset_registry").fetchall()
    }
    assert "daily_bar_pit" in registry_ids
    assert "security_master_ref" in registry_ids
    assert "market_trade_calendar" in registry_ids
    tgt_conn.close()

    # Step 4: run_audit
    audit_out = tmp_path / "output" / "audit"
    audit_report = run_audit(raw_db_path=raw_db, out_dir=audit_out)

    assert not audit_report.has_blocking_failure(), (
        "Audit blocking failures: "
        + str(audit_report.blocking_failures())
    )


@pytest.mark.smoke
def test_incremental_adds_only_new_rows(tmp_path: Path) -> None:
    """
    Full sync followed by incremental sync.

    Second sync uses a new date (one day after _LAST_DATE) with 1 row per
    security. Asserts rows_added > 0 for daily and total count = 90 + 3 = 93.
    """
    raw_db = tmp_path / "raw.duckdb"
    config = _make_smoke_config()

    # --- First sync: 3 securities × 30 dates ---
    adapter_full = SmokeAdapter(_SECURITIES, _WEEKDAYS)
    sync(raw_db_path=raw_db, config=config, adapters=[adapter_full])

    conn = duckdb.connect(str(raw_db))
    count_after_full = conn.execute("SELECT COUNT(*) FROM raw_kline_unadj").fetchone()[0]
    assert count_after_full == 90

    last_td_after_full = conn.execute(
        "SELECT last_trade_date FROM meta.dataset_sync_state WHERE dataset_id='daily'"
    ).fetchone()[0]
    assert last_td_after_full == _LAST_DATE
    conn.close()

    # --- Second sync: one new date (_NEXT_DATE) ---
    incremental_dates = [_NEXT_DATE]
    adapter_incr = SmokeAdapter(_SECURITIES, incremental_dates)
    report2 = sync(raw_db_path=raw_db, config=config, adapters=[adapter_incr])

    # Find daily result
    daily_result = next(
        (r for r in report2.results if r.dataset_id == "daily"), None
    )
    assert daily_result is not None
    assert daily_result.rows_added == 3, (
        f"Expected 3 new daily rows (1 per security), got {daily_result.rows_added}"
    )

    # Total rows in raw_kline_unadj = 90 (full) + 3 (incremental)
    conn = duckdb.connect(str(raw_db))
    total = conn.execute("SELECT COUNT(*) FROM raw_kline_unadj").fetchone()[0]
    conn.close()
    assert total == 93, f"Expected 93 total rows, got {total}"


# ---------------------------------------------------------------------------
# PBT: SmokeAdapter rows always have source_table and ingested_at
# ---------------------------------------------------------------------------


def test_pbt_smoke_adapter_rows_have_required_columns(tmp_path: Path) -> None:
    """
    Property: for any valid (securities, dates) input, every row yielded by
    SmokeAdapter has non-None source_table and ingested_at fields.

    **Validates: Requirements R2.5**
    """
    rng = random.Random(42)

    # Run 30 random (securities_count, dates_count) combinations
    all_securities = [
        "000001.SZ", "000002.SZ", "600001.SH", "600002.SH", "300001.SZ"
    ]
    for _ in range(30):
        n_sec = rng.randint(1, 5)
        n_days = rng.randint(5, 20)
        securities = all_securities[:n_sec]

        # Build consecutive weekday dates
        dates: list[str] = []
        d = datetime(2024, 1, 2)
        while len(dates) < n_days:
            if d.weekday() < 5:
                dates.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)

        adapter = SmokeAdapter(securities, dates)
        for ds_id in SmokeAdapter._STAGE1_DATASETS:
            rows = list(adapter.fetch(ds_id, since=None, until=None, full=True))
            for row in rows:
                assert "source_table" in row, (
                    f"Row from {ds_id} missing 'source_table': {row!r}"
                )
                assert row["source_table"] is not None, (
                    f"Row from {ds_id} has None 'source_table': {row!r}"
                )
                assert "ingested_at" in row, (
                    f"Row from {ds_id} missing 'ingested_at': {row!r}"
                )
                assert row["ingested_at"] is not None, (
                    f"Row from {ds_id} has None 'ingested_at': {row!r}"
                )
