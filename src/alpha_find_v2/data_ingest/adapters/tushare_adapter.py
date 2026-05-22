"""
TushareAdapter — primary data source adapter for A-share price and reference datasets.

Wraps the Tushare pro_api to yield rows conforming to RAW_TABLE_DDL schemas.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Iterator

from alpha_find_v2.data_ingest.adapters.base import (
    AdapterPermissionError,
    AdapterSchemaMismatchError,
)
from alpha_find_v2.data_ingest.schemas import DATASET_PRIMARY_KEYS
from alpha_find_v2.reference_data_staging import (
    _build_tushare_client,
    load_tushare_token,
)

logger = logging.getLogger(__name__)

# Indices fetched for index_daily
_DEFAULT_INDEX_CODES = [
    "000001.SH",  # Shanghai Composite
    "000300.SH",  # CSI 300
    "000905.SH",  # CSI 500
    "000852.SH",  # CSI 1000
    "399001.SZ",  # Shenzhen Component
    "399006.SZ",  # ChiNext
]

# Datasets supported by this adapter
_SUPPORTED = frozenset({
    "stock_basic",
    "trade_cal",
    "namechange",
    "daily",
    "daily_basic",
    "adj_factor",
    "daily_qfq",
    "suspend_d",
    "stk_limit",
    "index_daily",
    "index_weight",
    "index_member_all",
    # 5000-credit fundamentals
    "fina_indicator",
    "income",
    "balancesheet",
    "cashflow",
    "forecast",
    "express",
})

# Fields expected in the RAW_TABLE_DDL schema for each dataset (minus ingested_at/source_table)
_SCHEMA_FIELDS: dict[str, tuple[str, ...]] = {
    "stock_basic":   ("ts_code", "symbol", "name", "area", "industry", "list_date", "delist_date", "is_hs"),
    "trade_cal":     ("exchange", "cal_date", "is_open", "pretrade_date"),
    "namechange":    ("ts_code", "name", "start_date", "end_date", "ann_date", "change_reason"),
    "daily":         ("ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"),
    "daily_basic":   ("ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f", "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm", "total_share", "float_share", "free_share", "total_mv", "circ_mv"),
    "adj_factor":    ("ts_code", "trade_date", "adj_factor"),
    "daily_qfq":     ("ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"),
    "suspend_d":     ("ts_code", "trade_date", "suspend_timing", "suspend_type"),
    "stk_limit":     ("trade_date", "ts_code", "up_limit", "down_limit", "pre_close"),
    "index_daily":   ("ts_code", "trade_date", "close", "open", "high", "low", "pre_close", "change", "pct_chg", "vol", "amount"),
    "index_weight":  ("index_code", "con_code", "trade_date", "weight"),
    "index_member_all": ("l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name", "ts_code", "name", "in_date", "out_date", "is_new"),
    # 5000-credit fundamentals
    "fina_indicator": ("ts_code", "ann_date", "end_date", "eps", "roe", "roa", "gross_margin", "netprofit_margin", "current_ratio", "debt_to_assets", "revenue_ps", "netprofit_yoy", "dt_netprofit_yoy", "or_yoy", "q_sales_yoy", "assets_yoy", "equity_yoy"),
    "income":        ("ts_code", "ann_date", "f_ann_date", "end_date", "report_type", "comp_type", "total_revenue", "revenue", "operate_profit", "total_profit", "income_tax", "n_income", "n_income_attr_p"),
    "balancesheet":  ("ts_code", "ann_date", "f_ann_date", "end_date", "report_type", "comp_type", "total_assets", "total_liab", "total_hldr_eqy_exc_min_int", "total_hldr_eqy_inc_min_int", "money_cap", "accounts_receiv", "inventories"),
    "cashflow":      ("ts_code", "ann_date", "f_ann_date", "end_date", "report_type", "comp_type", "net_profit", "n_cashflow_act", "n_cashflow_inv_act", "n_cash_flows_fnc_act", "free_cashflow"),
    "forecast":      ("ts_code", "ann_date", "end_date", "type", "p_change_min", "p_change_max", "net_profit_min", "net_profit_max", "last_parent_net", "first_ann_date", "summary", "change_reason"),
    "express":       ("ts_code", "ann_date", "end_date", "revenue", "operate_profit", "total_profit", "n_income", "total_assets", "total_hldr_eqy_exc_min_int", "diluted_eps", "diluted_roe", "yoy_net_profit"),
}


def _check_primary_keys(dataset_id: str, row: dict[str, Any]) -> None:
    """Raise AdapterSchemaMismatchError if any primary key is missing from the row."""
    for pk in DATASET_PRIMARY_KEYS[dataset_id]:
        if pk not in row:
            raise AdapterSchemaMismatchError(
                f"Dataset '{dataset_id}': primary key column '{pk}' missing from response"
            )


def _df_to_rows(df: Any, dataset_id: str, source_table: str) -> list[dict[str, Any]]:
    """Convert a DataFrame to a list of dicts, filling missing schema fields with None.

    Raises AdapterSchemaMismatchError if any primary key column is absent.
    """
    if df is None or df.empty:
        return []

    ingested_at = datetime.utcnow()
    expected_fields = _SCHEMA_FIELDS.get(dataset_id, ())
    primary_keys = DATASET_PRIMARY_KEYS.get(dataset_id, ())
    df_cols = set(df.columns)

    # Check primary keys present in the DataFrame before iterating
    for pk in primary_keys:
        if pk not in df_cols:
            raise AdapterSchemaMismatchError(
                f"Dataset '{dataset_id}': primary key column '{pk}' missing from response"
            )

    rows = []
    for record in df.to_dict("records"):
        row: dict[str, Any] = {}
        for field in expected_fields:
            row[field] = record.get(field)
        row["source_table"] = source_table
        row["ingested_at"] = ingested_at
        rows.append(row)
    return rows


def _call_api(func: Any, *args: Any, **kwargs: Any) -> Any:
    """Call a Tushare API function, translating permission errors."""
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        msg = str(exc)
        if "permission" in msg.lower() or any(code in msg for code in ("40", "403", "401")):
            raise AdapterPermissionError(msg) from exc
        raise


class TushareAdapter:
    """DataSourceAdapter wrapping the Tushare pro_api for price and reference data."""

    name = "tushare"

    def __init__(
        self,
        token: str | None = None,
        *,
        _client: Any | None = None,
    ) -> None:
        if _client is not None:
            self._pro = _client
        else:
            tok = load_tushare_token(token)
            self._pro = _build_tushare_client(tok)

    # ------------------------------------------------------------------
    # Protocol methods
    # ------------------------------------------------------------------

    def supports(self, dataset_id: str) -> bool:
        return dataset_id in _SUPPORTED

    def fetch(
        self,
        dataset_id: str,
        *,
        since: str | None,
        until: str | None,
        full: bool,
    ) -> Iterator[dict[str, Any]]:
        """Yield rows conforming to RAW_TABLE_DDL[dataset_id]."""
        handler = _FETCH_HANDLERS.get(dataset_id)
        if handler is None:
            raise ValueError(f"TushareAdapter does not support dataset_id={dataset_id!r}")
        yield from handler(self, since=since, until=until, full=full)

    # ------------------------------------------------------------------
    # Per-dataset fetch handlers
    # ------------------------------------------------------------------

    def _fetch_stock_basic(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(
            self._pro.stock_basic,
            fields="ts_code,symbol,name,area,industry,list_date,delist_date,is_hs",
        )
        yield from _df_to_rows(df, "stock_basic", "tushare.stock_basic")

    def _fetch_trade_cal(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(
            self._pro.trade_cal,
            start_date=since,
            end_date=until,
        )
        yield from _df_to_rows(df, "trade_cal", "tushare.trade_cal")

    def _fetch_namechange(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(self._pro.namechange)
        yield from _df_to_rows(df, "namechange", "tushare.namechange")

    def _fetch_daily(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(
            self._pro.daily,
            start_date=since,
            end_date=until,
        )
        yield from _df_to_rows(df, "daily", "tushare.daily")

    def _fetch_daily_basic(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(
            self._pro.daily_basic,
            ts_code="",
            start_date=since,
            end_date=until,
        )
        yield from _df_to_rows(df, "daily_basic", "tushare.daily_basic")

    def _fetch_adj_factor(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(
            self._pro.adj_factor,
            start_date=since,
            end_date=until,
        )
        yield from _df_to_rows(df, "adj_factor", "tushare.adj_factor")

    def _fetch_daily_qfq(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        # Fetch all stocks in batches via pro_bar per stock.
        # First get the stock list.
        try:
            df_stocks = _call_api(
                self._pro.stock_basic,
                fields="ts_code",
            )
        except AdapterPermissionError:
            raise
        except Exception as exc:
            raise RuntimeError(f"daily_qfq: failed to fetch stock list: {exc}") from exc

        if df_stocks is None or df_stocks.empty:
            return

        codes = df_stocks["ts_code"].tolist()
        ingested_at = datetime.utcnow()
        source_table = "tushare.pro_bar_qfq"
        primary_keys = DATASET_PRIMARY_KEYS["daily_qfq"]
        expected_fields = _SCHEMA_FIELDS["daily_qfq"]

        for ts_code in codes:
            try:
                df = _call_api(
                    self._pro.pro_bar,
                    ts_code=ts_code,
                    adj="qfq",
                    start_date=since,
                    end_date=until,
                )
            except AdapterPermissionError:
                raise
            except Exception as exc:
                logger.warning("daily_qfq: skipping %s due to error: %s", ts_code, exc)
                continue

            if df is None or df.empty:
                continue

            df_cols = set(df.columns)
            for pk in primary_keys:
                if pk not in df_cols:
                    raise AdapterSchemaMismatchError(
                        f"Dataset 'daily_qfq': primary key column '{pk}' missing from response for {ts_code}"
                    )

            for record in df.to_dict("records"):
                row: dict[str, Any] = {}
                for field in expected_fields:
                    row[field] = record.get(field)
                row["source_table"] = source_table
                row["ingested_at"] = ingested_at
                yield row

    def _fetch_suspend_d(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(
            self._pro.suspend_d,
            start_date=since,
            end_date=until,
        )
        yield from _df_to_rows(df, "suspend_d", "tushare.suspend_d")

    def _fetch_stk_limit(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(
            self._pro.stk_limit,
            start_date=since,
            end_date=until,
        )
        yield from _df_to_rows(df, "stk_limit", "tushare.stk_limit")

    def _fetch_index_daily(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        ingested_at = datetime.utcnow()
        source_table = "tushare.index_daily"
        primary_keys = DATASET_PRIMARY_KEYS["index_daily"]
        expected_fields = _SCHEMA_FIELDS["index_daily"]

        for index_code in _DEFAULT_INDEX_CODES:
            try:
                df = _call_api(
                    self._pro.index_daily,
                    ts_code=index_code,
                    start_date=since,
                    end_date=until,
                )
            except AdapterPermissionError:
                raise
            except Exception as exc:
                logger.warning("index_daily: skipping %s due to error: %s", index_code, exc)
                continue

            if df is None or df.empty:
                continue

            df_cols = set(df.columns)
            for pk in primary_keys:
                if pk not in df_cols:
                    raise AdapterSchemaMismatchError(
                        f"Dataset 'index_daily': primary key column '{pk}' missing from response"
                    )

            for record in df.to_dict("records"):
                row: dict[str, Any] = {}
                for field in expected_fields:
                    row[field] = record.get(field)
                row["source_table"] = source_table
                row["ingested_at"] = ingested_at
                yield row

    def _fetch_index_weight(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        # Handled by existing reference_data_staging; orchestrator calls build_tushare_reference_db directly.
        return iter([])

    def _fetch_index_member_all(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        # Handled by existing reference_data_staging; orchestrator calls build_tushare_reference_db directly.
        return iter([])

    # ------------------------------------------------------------------
    # 5000-credit fundamental handlers (period_end axis)
    # ------------------------------------------------------------------

    def _fetch_fina_indicator(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(
            self._pro.fina_indicator,
            ts_code="",
            period=until,
        )
        yield from _df_to_rows(df, "fina_indicator", "tushare.fina_indicator")

    def _fetch_income(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(
            self._pro.income,
            ts_code="",
            period=until,
        )
        yield from _df_to_rows(df, "income", "tushare.income")

    def _fetch_balancesheet(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(
            self._pro.balancesheet,
            ts_code="",
            period=until,
        )
        yield from _df_to_rows(df, "balancesheet", "tushare.balancesheet")

    def _fetch_cashflow(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(
            self._pro.cashflow,
            ts_code="",
            period=until,
        )
        yield from _df_to_rows(df, "cashflow", "tushare.cashflow")

    def _fetch_forecast(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(
            self._pro.forecast,
            period=until,
        )
        yield from _df_to_rows(df, "forecast", "tushare.forecast")

    def _fetch_express(self, *, since: str | None, until: str | None, full: bool) -> Iterator[dict[str, Any]]:
        df = _call_api(
            self._pro.express,
            ts_code="",
            period=until,
        )
        yield from _df_to_rows(df, "express", "tushare.express")


# Dispatch table mapping dataset_id → bound method name
_FETCH_HANDLERS: dict[str, Any] = {
    "stock_basic":      TushareAdapter._fetch_stock_basic,
    "trade_cal":        TushareAdapter._fetch_trade_cal,
    "namechange":       TushareAdapter._fetch_namechange,
    "daily":            TushareAdapter._fetch_daily,
    "daily_basic":      TushareAdapter._fetch_daily_basic,
    "adj_factor":       TushareAdapter._fetch_adj_factor,
    "daily_qfq":        TushareAdapter._fetch_daily_qfq,
    "suspend_d":        TushareAdapter._fetch_suspend_d,
    "stk_limit":        TushareAdapter._fetch_stk_limit,
    "index_daily":      TushareAdapter._fetch_index_daily,
    "index_weight":     TushareAdapter._fetch_index_weight,
    "index_member_all": TushareAdapter._fetch_index_member_all,
    # 5000-credit fundamentals
    "fina_indicator":   TushareAdapter._fetch_fina_indicator,
    "income":           TushareAdapter._fetch_income,
    "balancesheet":     TushareAdapter._fetch_balancesheet,
    "cashflow":         TushareAdapter._fetch_cashflow,
    "forecast":         TushareAdapter._fetch_forecast,
    "express":          TushareAdapter._fetch_express,
}
