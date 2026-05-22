"""
Tests for TushareAdapter fundamentals (5000-credit datasets).

All tests are fully offline; no real Tushare token is required.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import pytest

from alpha_find_v2.data_ingest.adapters.base import AdapterPermissionError
from alpha_find_v2.data_ingest.adapters.tushare_adapter import TushareAdapter


# ---------------------------------------------------------------------------
# Fake Tushare client with fundamentals support
# ---------------------------------------------------------------------------


class FakeTushareClient:
    """Fake pro_api returning canned DataFrames for fundamental endpoints."""

    def fina_indicator(self, ts_code: str = "", **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SZ",
            "ann_date": "20240430",
            "end_date": "20231231",
            "eps": 1.5,
            "roe": 12.3,
            "roa": 1.8,
            "gross_margin": 35.0,
            "netprofit_margin": 25.0,
            "current_ratio": 2.1,
            "debt_to_assets": 0.45,
            "revenue_ps": 10.5,
            "netprofit_yoy": 8.0,
            "dt_netprofit_yoy": 7.5,
            "or_yoy": 5.0,
            "q_sales_yoy": 6.0,
            "assets_yoy": 4.0,
            "equity_yoy": 9.0,
        }])

    def income(self, ts_code: str = "", **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SZ",
            "ann_date": "20240430",
            "f_ann_date": "20240428",
            "end_date": "20231231",
            "report_type": "1",
            "comp_type": "1",
            "total_revenue": 1000000.0,
            "revenue": 950000.0,
            "operate_profit": 200000.0,
            "total_profit": 210000.0,
            "income_tax": 50000.0,
            "n_income": 160000.0,
            "n_income_attr_p": 155000.0,
        }])

    def balancesheet(self, ts_code: str = "", **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SZ",
            "ann_date": "20240430",
            "f_ann_date": "20240428",
            "end_date": "20231231",
            "report_type": "1",
            "comp_type": "1",
            "total_assets": 5000000.0,
            "total_liab": 2000000.0,
            "total_hldr_eqy_exc_min_int": 2900000.0,
            "total_hldr_eqy_inc_min_int": 3000000.0,
            "money_cap": 300000.0,
            "accounts_receiv": 150000.0,
            "inventories": 80000.0,
        }])

    def cashflow(self, ts_code: str = "", **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SZ",
            "ann_date": "20240430",
            "f_ann_date": "20240428",
            "end_date": "20231231",
            "report_type": "1",
            "comp_type": "1",
            "net_profit": 160000.0,
            "n_cashflow_act": 180000.0,
            "n_cashflow_inv_act": -50000.0,
            "n_cash_flows_fnc_act": -30000.0,
            "free_cashflow": 130000.0,
        }])

    def forecast(self, **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SZ",
            "ann_date": "20240115",
            "end_date": "20231231",
            "type": "预增",
            "p_change_min": 10.0,
            "p_change_max": 30.0,
            "net_profit_min": 176000.0,
            "net_profit_max": 208000.0,
            "last_parent_net": 160000.0,
            "first_ann_date": "20240115",
            "summary": "业绩预增",
            "change_reason": "主营业务增长",
        }])

    def express(self, ts_code: str = "", **kwargs: Any) -> pd.DataFrame:
        return pd.DataFrame([{
            "ts_code": "000001.SZ",
            "ann_date": "20240128",
            "end_date": "20231231",
            "revenue": 950000.0,
            "operate_profit": 200000.0,
            "total_profit": 210000.0,
            "n_income": 160000.0,
            "total_assets": 5000000.0,
            "total_hldr_eqy_exc_min_int": 2900000.0,
            "diluted_eps": 1.5,
            "diluted_roe": 12.3,
            "yoy_net_profit": 8.0,
        }])


@pytest.fixture()
def adapter() -> TushareAdapter:
    return TushareAdapter(_client=FakeTushareClient())


# ---------------------------------------------------------------------------
# supports() tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dataset_id", [
    "fina_indicator", "income", "balancesheet", "cashflow", "forecast", "express",
])
def test_supports_fundamentals(adapter: TushareAdapter, dataset_id: str) -> None:
    assert adapter.supports(dataset_id) is True


# ---------------------------------------------------------------------------
# fina_indicator — source_table and column coverage
# ---------------------------------------------------------------------------


def test_fina_indicator_source_table(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("fina_indicator", since=None, until="20231231", full=True))
    assert len(rows) == 1
    row = rows[0]
    assert row["source_table"] == "tushare.fina_indicator"
    assert isinstance(row["ingested_at"], datetime)


def test_fina_indicator_columns(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("fina_indicator", since=None, until="20231231", full=True))
    row = rows[0]
    expected_cols = (
        "ts_code", "ann_date", "end_date", "eps", "roe", "roa",
        "gross_margin", "netprofit_margin", "current_ratio", "debt_to_assets",
        "revenue_ps", "netprofit_yoy", "dt_netprofit_yoy", "or_yoy",
        "q_sales_yoy", "assets_yoy", "equity_yoy",
    )
    for col in expected_cols:
        assert col in row, f"Missing column: {col}"
    assert row["ts_code"] == "000001.SZ"
    assert row["end_date"] == "20231231"


# ---------------------------------------------------------------------------
# income, balancesheet, cashflow, forecast, express — basic smoke
# ---------------------------------------------------------------------------


def test_income_fetch(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("income", since=None, until="20231231", full=True))
    assert len(rows) == 1
    row = rows[0]
    assert row["source_table"] == "tushare.income"
    assert isinstance(row["ingested_at"], datetime)
    for col in ("ts_code", "ann_date", "f_ann_date", "end_date", "report_type",
                "comp_type", "total_revenue", "revenue", "operate_profit",
                "total_profit", "income_tax", "n_income", "n_income_attr_p"):
        assert col in row, f"Missing column: {col}"


def test_balancesheet_fetch(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("balancesheet", since=None, until="20231231", full=True))
    assert len(rows) == 1
    row = rows[0]
    assert row["source_table"] == "tushare.balancesheet"
    assert isinstance(row["ingested_at"], datetime)
    for col in ("ts_code", "ann_date", "f_ann_date", "end_date", "report_type",
                "comp_type", "total_assets", "total_liab",
                "total_hldr_eqy_exc_min_int", "total_hldr_eqy_inc_min_int",
                "money_cap", "accounts_receiv", "inventories"):
        assert col in row, f"Missing column: {col}"


def test_cashflow_fetch(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("cashflow", since=None, until="20231231", full=True))
    assert len(rows) == 1
    row = rows[0]
    assert row["source_table"] == "tushare.cashflow"
    assert isinstance(row["ingested_at"], datetime)
    for col in ("ts_code", "ann_date", "f_ann_date", "end_date", "report_type",
                "comp_type", "net_profit", "n_cashflow_act",
                "n_cashflow_inv_act", "n_cash_flows_fnc_act", "free_cashflow"):
        assert col in row, f"Missing column: {col}"


def test_forecast_fetch(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("forecast", since=None, until="20231231", full=True))
    assert len(rows) == 1
    row = rows[0]
    assert row["source_table"] == "tushare.forecast"
    assert isinstance(row["ingested_at"], datetime)
    for col in ("ts_code", "ann_date", "end_date", "type", "p_change_min",
                "p_change_max", "net_profit_min", "net_profit_max",
                "last_parent_net", "first_ann_date", "summary", "change_reason"):
        assert col in row, f"Missing column: {col}"


def test_express_fetch(adapter: TushareAdapter) -> None:
    rows = list(adapter.fetch("express", since=None, until="20231231", full=True))
    assert len(rows) == 1
    row = rows[0]
    assert row["source_table"] == "tushare.express"
    assert isinstance(row["ingested_at"], datetime)
    for col in ("ts_code", "ann_date", "end_date", "revenue", "operate_profit",
                "total_profit", "n_income", "total_assets",
                "total_hldr_eqy_exc_min_int", "diluted_eps", "diluted_roe",
                "yoy_net_profit"):
        assert col in row, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# Permission error wraps to AdapterPermissionError for fundamentals
# ---------------------------------------------------------------------------


class _PermissionClient:
    def fina_indicator(self, **kwargs: Any) -> pd.DataFrame:
        raise Exception("permission denied 40203")

    def income(self, **kwargs: Any) -> pd.DataFrame:
        raise Exception("抱歉，您没有访问权限，错误代码：40203")

    def balancesheet(self, **kwargs: Any) -> pd.DataFrame:
        raise Exception("permission denied")

    def cashflow(self, **kwargs: Any) -> pd.DataFrame:
        raise Exception("permission 403")

    def forecast(self, **kwargs: Any) -> pd.DataFrame:
        raise Exception("40 permission error")

    def express(self, **kwargs: Any) -> pd.DataFrame:
        raise Exception("permission denied 5000 credits required")


@pytest.mark.parametrize("dataset_id", [
    "fina_indicator", "income", "balancesheet", "cashflow", "forecast", "express",
])
def test_permission_error_wraps_for_fundamentals(dataset_id: str) -> None:
    adapter = TushareAdapter(_client=_PermissionClient())
    with pytest.raises(AdapterPermissionError):
        list(adapter.fetch(dataset_id, since=None, until="20231231", full=True))
