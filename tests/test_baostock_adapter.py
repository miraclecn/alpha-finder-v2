"""Tests for BaostockAdapter."""

from __future__ import annotations

import sys
import types
from datetime import datetime
from unittest.mock import MagicMock, call

import pytest

from alpha_find_v2.data_ingest.adapters.base import AdapterUnavailable
from alpha_find_v2.data_ingest.adapters.baostock_adapter import BaostockAdapter


# ---------------------------------------------------------------------------
# Fake baostock module helpers
# ---------------------------------------------------------------------------

def _make_fake_result(rows: list[list[str]]) -> MagicMock:
    result = MagicMock()
    result.data = rows
    return result


def _make_fake_baostock(rows: list[list[str]] | None = None) -> MagicMock:
    """Return a fake baostock module with login/logout/query_history_k_data_plus."""
    bs = MagicMock(spec=["login", "logout", "query_history_k_data_plus"])
    bs.login.return_value = None
    bs.logout.return_value = None
    bs.query_history_k_data_plus.return_value = _make_fake_result(rows or [])
    return bs


def _inject_fake_baostock(fake_bs: MagicMock) -> None:
    """Install fake_bs into sys.modules so the adapter picks it up."""
    sys.modules["baostock"] = fake_bs


def _remove_fake_baostock() -> None:
    sys.modules.pop("baostock", None)


# Canonical row matching _K_DATA_FIELDS = "date,open,high,low,close,preclose,volume,amount,pctChg"
_SAMPLE_ROW = ["2024-01-15", "10.50", "11.00", "10.30", "10.80", "10.40", "1000000", "10800000", "1.92"]


# ---------------------------------------------------------------------------
# supports()
# ---------------------------------------------------------------------------

class TestSupports:
    def test_supports_daily(self):
        adapter = BaostockAdapter()
        assert adapter.supports("daily") is True

    def test_supports_index_daily(self):
        adapter = BaostockAdapter()
        assert adapter.supports("index_daily") is True

    def test_does_not_support_adj_factor(self):
        adapter = BaostockAdapter()
        assert adapter.supports("adj_factor") is False

    def test_does_not_support_stock_basic(self):
        adapter = BaostockAdapter()
        assert adapter.supports("stock_basic") is False

    def test_does_not_support_empty_string(self):
        adapter = BaostockAdapter()
        assert adapter.supports("") is False


# ---------------------------------------------------------------------------
# fetch("daily") — happy path
# ---------------------------------------------------------------------------

class TestFetchDaily:
    def setup_method(self):
        self._fake_bs = _make_fake_baostock(rows=[_SAMPLE_ROW])
        _inject_fake_baostock(self._fake_bs)

    def teardown_method(self):
        _remove_fake_baostock()

    def test_yields_tushare_shaped_dict(self):
        adapter = BaostockAdapter(stock_codes=["600001.SH"])
        rows = list(adapter.fetch("daily", since="2024-01-01", until="2024-01-31", full=False))

        assert len(rows) == 1
        row = rows[0]
        assert row["ts_code"] == "600001.SH"
        assert row["trade_date"] == "20240115"
        assert row["open"] == pytest.approx(10.50)
        assert row["high"] == pytest.approx(11.00)
        assert row["low"] == pytest.approx(10.30)
        assert row["close"] == pytest.approx(10.80)
        assert row["pre_close"] == pytest.approx(10.40)
        assert row["vol"] == pytest.approx(1_000_000)
        assert row["amount"] == pytest.approx(10_800_000)
        assert row["pct_chg"] == pytest.approx(1.92)
        assert row["source_table"] == "baostock.k_data"
        assert isinstance(row["ingested_at"], datetime)

    def test_converts_ts_code_to_baostock_format(self):
        adapter = BaostockAdapter(stock_codes=["600001.SH"])
        list(adapter.fetch("daily", since="2024-01-01", until="2024-01-31", full=False))

        self._fake_bs.query_history_k_data_plus.assert_called_once_with(
            code="sh.600001",
            fields="date,open,high,low,close,preclose,volume,amount,pctChg",
            start_date="2024-01-01",
            end_date="2024-01-31",
            frequency="d",
            adjustflag="3",
        )

    def test_sz_code_conversion(self):
        adapter = BaostockAdapter(stock_codes=["000001.SZ"])
        list(adapter.fetch("daily", since=None, until=None, full=False))

        self._fake_bs.query_history_k_data_plus.assert_called_once_with(
            code="sz.000001",
            fields="date,open,high,low,close,preclose,volume,amount,pctChg",
            start_date=None,
            end_date=None,
            frequency="d",
            adjustflag="3",
        )

    def test_multiple_codes_yields_rows_for_each(self):
        self._fake_bs.query_history_k_data_plus.return_value = _make_fake_result([_SAMPLE_ROW])
        adapter = BaostockAdapter(stock_codes=["600001.SH", "000001.SZ"])
        rows = list(adapter.fetch("daily", since=None, until=None, full=False))

        assert len(rows) == 2
        assert self._fake_bs.query_history_k_data_plus.call_count == 2

    def test_empty_stock_codes_yields_no_rows(self):
        adapter = BaostockAdapter(stock_codes=[])
        rows = list(adapter.fetch("daily", since=None, until=None, full=False))
        assert rows == []
        self._fake_bs.query_history_k_data_plus.assert_not_called()

    def test_no_stock_codes_arg_yields_no_rows(self):
        adapter = BaostockAdapter()
        rows = list(adapter.fetch("daily", since=None, until=None, full=False))
        assert rows == []


# ---------------------------------------------------------------------------
# fetch("index_daily") — happy path
# ---------------------------------------------------------------------------

class TestFetchIndexDaily:
    def setup_method(self):
        self._fake_bs = _make_fake_baostock(rows=[_SAMPLE_ROW])
        _inject_fake_baostock(self._fake_bs)

    def teardown_method(self):
        _remove_fake_baostock()

    def test_yields_tushare_shaped_dict_for_index(self):
        adapter = BaostockAdapter(stock_codes=["000300.SH"])
        rows = list(adapter.fetch("index_daily", since=None, until=None, full=False))

        assert len(rows) == 1
        row = rows[0]
        assert row["ts_code"] == "000300.SH"
        assert row["source_table"] == "baostock.k_data"

    def test_uses_sh_prefix_for_sh_index(self):
        adapter = BaostockAdapter(stock_codes=["000300.SH"])
        list(adapter.fetch("index_daily", since=None, until=None, full=False))

        self._fake_bs.query_history_k_data_plus.assert_called_once_with(
            code="sh.000300",
            fields="date,open,high,low,close,preclose,volume,amount,pctChg",
            start_date=None,
            end_date=None,
            frequency="d",
            adjustflag="3",
        )


# ---------------------------------------------------------------------------
# Session management: login / logout
# ---------------------------------------------------------------------------

class TestSessionManagement:
    def setup_method(self):
        self._fake_bs = _make_fake_baostock(rows=[_SAMPLE_ROW])
        _inject_fake_baostock(self._fake_bs)

    def teardown_method(self):
        _remove_fake_baostock()

    def test_login_called_once_on_first_fetch(self):
        adapter = BaostockAdapter(stock_codes=["600001.SH"])
        list(adapter.fetch("daily", since=None, until=None, full=False))

        self._fake_bs.login.assert_called_once()

    def test_login_not_called_again_on_second_fetch(self):
        adapter = BaostockAdapter(stock_codes=["600001.SH"])
        list(adapter.fetch("daily", since=None, until=None, full=False))
        list(adapter.fetch("daily", since=None, until=None, full=False))

        # login should still only have been called once
        self._fake_bs.login.assert_called_once()

    def test_logout_called_on_close(self):
        adapter = BaostockAdapter(stock_codes=["600001.SH"])
        list(adapter.fetch("daily", since=None, until=None, full=False))

        self._fake_bs.logout.assert_not_called()
        adapter.close()
        self._fake_bs.logout.assert_called_once()

    def test_logout_not_called_if_never_logged_in(self):
        adapter = BaostockAdapter()
        adapter.close()
        self._fake_bs.logout.assert_not_called()

    def test_context_manager_calls_logout_on_exit(self):
        adapter = BaostockAdapter(stock_codes=["600001.SH"])
        with adapter:
            list(adapter.fetch("daily", since=None, until=None, full=False))

        self._fake_bs.logout.assert_called_once()

    def test_close_is_idempotent(self):
        adapter = BaostockAdapter(stock_codes=["600001.SH"])
        list(adapter.fetch("daily", since=None, until=None, full=False))
        adapter.close()
        adapter.close()  # second close should not raise or call logout again

        self._fake_bs.logout.assert_called_once()


# ---------------------------------------------------------------------------
# Missing baostock package → AdapterUnavailable
# ---------------------------------------------------------------------------

class TestMissingPackage:
    def setup_method(self):
        # Make sure baostock is NOT in sys.modules
        sys.modules.pop("baostock", None)

    def test_fetch_raises_adapter_unavailable_not_import_error(self):
        adapter = BaostockAdapter(stock_codes=["600001.SH"])
        with pytest.raises(AdapterUnavailable, match="baostock package not installed"):
            list(adapter.fetch("daily", since=None, until=None, full=False))

    def test_raised_exception_is_not_import_error(self):
        adapter = BaostockAdapter(stock_codes=["600001.SH"])
        try:
            list(adapter.fetch("daily", since=None, until=None, full=False))
        except AdapterUnavailable:
            pass  # expected
        except ImportError:
            pytest.fail("Should raise AdapterUnavailable, not ImportError")

    def test_supports_works_without_baostock_installed(self):
        # supports() must not import baostock
        adapter = BaostockAdapter()
        assert adapter.supports("daily") is True
        assert adapter.supports("adj_factor") is False


# ---------------------------------------------------------------------------
# name attribute
# ---------------------------------------------------------------------------

class TestAdapterName:
    def test_name_is_baostock(self):
        assert BaostockAdapter.name == "baostock"

    def test_instance_name_is_baostock(self):
        assert BaostockAdapter().name == "baostock"
