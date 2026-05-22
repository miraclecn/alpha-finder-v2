"""
Tests for TushareAdapter using a fake Tushare pro_api client.

All tests are fully offline; no real Tushare token is required.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import pytest

from alpha_find_v2.data_ingest.adapters.base import (
    AdapterPermissionError,
    AdapterSchemaMismatchError,
)
from alpha_find_v2.data_ingest.adapters.tushare_adapter import TushareAdapter


# ---------------------------------------------------------------------------
# Fake Tushare client
# ---------------------------------------------------------------------------


class FakeTushareClient:
    """Minimal fake that returns canned DataFrames for each API call."""

    def stock_basic(self, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SZ",
            "symbol": "000001",
            "name": "平安银行",
            "area": "深圳",
            "industry": "银行",
            "list_date": "19910403",
            "delist_date": None,
            "is_hs": "N",
        }])

    def trade_cal(self, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "exchange": "SSE",
            "cal_date": "20240102",
            "is_open": 1,
            "pretrade_date": "20231229",
        }])

    def namechange(self, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SZ",
            "name": "平安银行",
            "start_date": "20030101",
            "end_date": None,
            "ann_date": None,
            "change_reason": None,
        }])

    def daily(self, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SZ",
            "trade_date": "20240102",
            "open": 10.0,
            "high": 10.5,
            "low": 9.9,
            "close": 10.2,
            "pre_close": 10.0,
            "change": 0.2,
            "pct_chg": 2.0,
            "vol": 100.0,
            "amount": 200.0,
        }])

    def daily_basic(self, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SZ",
            "trade_date": "20240102",
            "close": 10.2,
            "turnover_rate": 0.5,
            "turnover_rate_f": 0.6,
            "volume_ratio": 1.1,
            "pe": 10.0,
            "pe_ttm": 9.8,
            "pb": 1.2,
            "ps": 0.9,
            "ps_ttm": 0.85,
            "dv_ratio": 2.0,
            "dv_ttm": 2.1,
            "total_share": 1000000.0,
            "float_share": 800000.0,
            "free_share": 750000.0,
            "total_mv": 10200000.0,
            "circ_mv": 8160000.0,
        }])

    def adj_factor(self, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SZ",
            "trade_date": "20240102",
            "adj_factor": 1.5,
        }])

    def suspend_d(self, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SZ",
            "trade_date": "20240102",
            "suspend_timing": "全天",
            "suspend_type": "S",
        }])

    def stk_limit(self, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "trade_date": "20240102",
            "ts_code": "000001.SZ",
            "up_limit": 11.0,
            "down_limit": 9.1,
            "pre_close": 10.0,
        }])

    def index_daily(self, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SH",
            "trade_date": "20240102",
            "close": 3000.0,
            "open": 2990.0,
            "high": 3010.0,
            "low": 2985.0,
            "pre_close": 2995.0,
            "change": 5.0,
            "pct_chg": 0.17,
            "vol": 500000.0,
            "amount": 600000.0,
        }])

    def pro_bar(self, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SZ",
            "trade_date": "20240102",
            "open": 10.0,
            "high": 10.5,
            "low": 9.9,
            "close": 10.2,
            "pre_close": 10.0,
            "change": 0.2,
            "pct_chg": 2.0,
            "vol": 100.0,
            "amount": 200.0,
        }])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def adapter() -> TushareAdapter:
    return TushareAdapter(_client=FakeTushareClient())


# ---------------------------------------------------------------------------
# Test 1 — stock_basic yields correct source_table and all required columns
# ---------------------------------------------------------------------------


def test_stock_basic_source_table_and_columns(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("stock_basic", since=None, until=None, full=True))
    assert len(rows) == 1
    row = rows[0]
    assert row["source_table"] == "tushare.stock_basic"
    # All required schema columns present
    for col in ("ts_code", "symbol", "name", "area", "industry", "list_date", "delist_date", "is_hs"):
        assert col in row, f"Missing column: {col}"
    assert row["ts_code"] == "000001.SZ"
    assert row["name"] == "平安银行"


# ---------------------------------------------------------------------------
# Test 2 — daily fetch yields rows with correct source_table and ingested_at
# ---------------------------------------------------------------------------


def test_daily_fetch_source_table_and_ingested_at(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("daily", since="20240101", until="20240131", full=False))
    assert len(rows) == 1
    row = rows[0]
    assert row["source_table"] == "tushare.daily"
    assert isinstance(row["ingested_at"], datetime)
    assert row["ts_code"] == "000001.SZ"
    assert row["trade_date"] == "20240102"


# ---------------------------------------------------------------------------
# Test 3 — adj_factor yields rows with adj_factor column
# ---------------------------------------------------------------------------


def test_adj_factor_yields_adj_factor_column(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("adj_factor", since="20240101", until="20240131", full=False))
    assert len(rows) == 1
    row = rows[0]
    assert "adj_factor" in row
    assert row["adj_factor"] == 1.5
    assert row["ts_code"] == "000001.SZ"
    assert row["trade_date"] == "20240102"


# ---------------------------------------------------------------------------
# Test 4 — supports() returns True for known datasets, False for unknown
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dataset_id", [
    "stock_basic", "trade_cal", "namechange", "daily", "daily_basic",
    "adj_factor", "daily_qfq", "suspend_d", "stk_limit", "index_daily",
    "index_weight", "index_member_all",
    "fina_indicator", "income", "balancesheet", "cashflow", "forecast", "express",
])
def test_supports_returns_true_for_supported(adapter: TushareAdapter, dataset_id: str) -> None:
    assert adapter.supports(dataset_id) is True


@pytest.mark.parametrize("dataset_id", [
    "unknown_dataset",
])
def test_supports_returns_false_for_unsupported(adapter: TushareAdapter, dataset_id: str) -> None:
    assert adapter.supports(dataset_id) is False


# ---------------------------------------------------------------------------
# Test 5 — permission error from client wraps to AdapterPermissionError
# ---------------------------------------------------------------------------


class _PermissionErrorClient:
    def stock_basic(self, **kwargs: Any) -> pd.DataFrame:
        raise Exception("permission denied 40203")


def test_permission_error_wraps_to_adapter_permission_error() -> None:
    adapter = TushareAdapter(_client=_PermissionErrorClient())
    with pytest.raises(AdapterPermissionError) as exc_info:
        list(adapter.fetch("stock_basic", since=None, until=None, full=True))
    assert "40203" in str(exc_info.value) or "permission" in str(exc_info.value).lower()


class _PermissionCodeErrorClient:
    """Simulates a Tushare error code string like '40203'."""

    def daily(self, **kwargs: Any) -> pd.DataFrame:
        raise Exception("抱歉，您没有访问权限，错误代码：40203")


def test_permission_code_error_wraps_to_adapter_permission_error() -> None:
    adapter = TushareAdapter(_client=_PermissionCodeErrorClient())
    with pytest.raises(AdapterPermissionError):
        list(adapter.fetch("daily", since="20240101", until="20240131", full=False))


# ---------------------------------------------------------------------------
# Test 6 — missing primary key column raises AdapterSchemaMismatchError
# ---------------------------------------------------------------------------


class _MissingTsCodeClient:
    """Returns a DataFrame without the ts_code primary key column."""

    def stock_basic(self, **kwargs: Any) -> pd.DataFrame:
        # Missing ts_code
        return pd.DataFrame([{"symbol": "000001", "name": "平安银行"}])


def test_missing_primary_key_raises_schema_mismatch() -> None:
    adapter = TushareAdapter(_client=_MissingTsCodeClient())
    with pytest.raises(AdapterSchemaMismatchError) as exc_info:
        list(adapter.fetch("stock_basic", since=None, until=None, full=True))
    assert "ts_code" in str(exc_info.value)


class _MissingTradeDateClient:
    """Returns a DataFrame with ts_code but without trade_date."""

    def daily(self, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{"ts_code": "000001.SZ", "open": 10.0}])


def test_missing_trade_date_raises_schema_mismatch() -> None:
    adapter = TushareAdapter(_client=_MissingTradeDateClient())
    with pytest.raises(AdapterSchemaMismatchError) as exc_info:
        list(adapter.fetch("daily", since="20240101", until="20240131", full=False))
    assert "trade_date" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Additional — index_weight and index_member_all yield empty (delegated)
# ---------------------------------------------------------------------------


def test_index_weight_yields_empty(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("index_weight", since="20240101", until="20240131", full=False))
    assert rows == []


def test_index_member_all_yields_empty(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("index_member_all", since=None, until=None, full=True))
    assert rows == []


# ---------------------------------------------------------------------------
# Additional — trade_cal, namechange, suspend_d, stk_limit basic smoke
# ---------------------------------------------------------------------------


def test_trade_cal_fetch(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("trade_cal", since="20240101", until="20240131", full=False))
    assert len(rows) == 1
    row = rows[0]
    assert row["source_table"] == "tushare.trade_cal"
    assert row["cal_date"] == "20240102"
    assert row["exchange"] == "SSE"


def test_suspend_d_fetch(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("suspend_d", since="20240101", until="20240131", full=False))
    assert len(rows) == 1
    assert rows[0]["source_table"] == "tushare.suspend_d"
    assert rows[0]["ts_code"] == "000001.SZ"


def test_stk_limit_fetch(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("stk_limit", since="20240101", until="20240131", full=False))
    assert len(rows) == 1
    row = rows[0]
    assert row["source_table"] == "tushare.stk_limit"
    assert row["up_limit"] == 11.0
