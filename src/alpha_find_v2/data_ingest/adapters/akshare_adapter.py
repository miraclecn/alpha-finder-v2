"""
AKShareAdapter — free fallback adapter for daily A-share and index prices.

Supports `daily` and `index_daily` only.  Emits Tushare-shaped rows so the
orchestrator can treat AKShare and Tushare rows uniformly.

Usage:
    adapter = AKShareAdapter(symbols=["600000", "000001"])
    for row in adapter.fetch("daily", since="20240101", until="20240131", full=False):
        ...
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Iterator

from alpha_find_v2.data_ingest.adapters.base import AdapterUnavailable

logger = logging.getLogger(__name__)

# Default 10 common A-share symbols (bare, no exchange suffix) for demo/fallback
_DEFAULT_STOCK_SYMBOLS: list[str] = [
    "600000",  # 浦发银行   SH
    "600519",  # 贵州茅台   SH
    "601318",  # 中国平安   SH
    "601857",  # 中国石油   SH
    "600028",  # 中国石化   SH
    "000001",  # 平安银行   SZ
    "000651",  # 格力电器   SZ
    "000858",  # 五粮液     SZ
    "002594",  # 比亚迪     SZ
    "300750",  # 宁德时代   SZ
]

# AKShare stock_zh_a_hist(adjust='') columns → Tushare field names.
# Actual columns returned: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
# pre_close is not available from the unadjusted endpoint → always injected as None.
# 振幅 (amplitude) and 换手率 (turnover rate) are not in raw_kline_unadj schema → ignored.
_DAILY_COL_MAP: dict[str, str] = {
    "日期":  "trade_date",
    "开盘":  "open",
    "收盘":  "close",
    "最高":  "high",
    "最低":  "low",
    "成交量": "vol",
    "成交额": "amount",
    "涨跌幅": "pct_chg",
    "涨跌额": "change",
}

# Index history uses the same column names
_INDEX_COL_MAP = _DAILY_COL_MAP


def _load_akshare() -> Any:
    """Lazy import of akshare; raises AdapterUnavailable if not installed."""
    try:
        import akshare as ak  # noqa: PLC0415

        return ak
    except ImportError:
        raise AdapterUnavailable("akshare package not installed")


def _symbol_suffix(symbol: str) -> str:
    """Return exchange suffix for a bare A-share symbol.

    Heuristic: starts with '6' → SH (Shanghai), else → SZ (Shenzhen).
    Good enough for demo fallback; not expected to cover BJ-listed stocks.
    """
    return "SH" if symbol.startswith("6") else "SZ"


def _strip_suffix(code: str) -> str:
    """'600000.SH' → '600000', '000001' → '000001'."""
    return code.split(".")[0]


def _format_date(date_val: Any) -> str:
    """Convert various date representations to YYYYMMDD string.

    AKShare returns dates as strings like '2024-01-01' or pandas Timestamps.
    """
    if isinstance(date_val, str):
        # Already YYYYMMDD
        if len(date_val) == 8 and date_val.isdigit():
            return date_val
        # ISO format: '2024-01-01'
        return date_val.replace("-", "")
    # pandas Timestamp or datetime
    try:
        return date_val.strftime("%Y%m%d")
    except AttributeError:
        return str(date_val).replace("-", "")[:8]


def _map_row(
    ak_row: dict[str, Any],
    col_map: dict[str, str],
    ts_code: str,
    source_table: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    """Map a single AKShare row dict to a Tushare-shaped row dict."""
    row: dict[str, Any] = {
        "ts_code": ts_code,
        "pre_close": None,  # not available from unadjusted AKShare endpoint
        "source_table": source_table,
        "ingested_at": ingested_at,
    }
    for ak_col, ts_col in col_map.items():
        if ak_col not in ak_row:
            continue
        val = ak_row[ak_col]
        if ts_col == "trade_date":
            val = _format_date(val)
        row[ts_col] = val
    return row


class AKShareAdapter:
    """DataSourceAdapter wrapping AKShare for daily A-share and index prices."""

    name = "akshare"

    def __init__(
        self,
        symbols: list[str] | None = None,
        index_codes: list[str] | None = None,
        *,
        _akshare: Any | None = None,  # injectable fake module for tests
    ) -> None:
        # symbols: bare A-share codes without exchange suffix (e.g. "600000").
        # When None, _DEFAULT_STOCK_SYMBOLS is used at fetch time.
        self._symbols = symbols
        self._index_codes = index_codes
        self._akshare = _akshare

    def _get_akshare(self) -> Any:
        """Return the akshare module (injected or lazily imported)."""
        if self._akshare is not None:
            return self._akshare
        return _load_akshare()

    # ------------------------------------------------------------------
    # Protocol methods
    # ------------------------------------------------------------------

    def supports(self, dataset_id: str) -> bool:
        return dataset_id in ("daily", "index_daily")

    def fetch(
        self,
        dataset_id: str,
        *,
        since: str | None,
        until: str | None,
        full: bool,
    ) -> Iterator[dict[str, Any]]:
        """Yield Tushare-shaped rows for `dataset_id`."""
        if dataset_id == "daily":
            yield from self._fetch_daily(since=since, until=until)
        elif dataset_id == "index_daily":
            yield from self._fetch_index_daily(since=since, until=until)
        else:
            raise ValueError(
                f"AKShareAdapter does not support dataset_id={dataset_id!r}"
            )

    # ------------------------------------------------------------------
    # Internal fetch helpers
    # ------------------------------------------------------------------

    def _fetch_daily(
        self,
        *,
        since: str | None,
        until: str | None,
    ) -> Iterator[dict[str, Any]]:
        """Yield unadjusted daily bars for each symbol.

        When symbols were not provided, falls back to _DEFAULT_STOCK_SYMBOLS.
        """
        symbols = (
            self._symbols if self._symbols is not None else _DEFAULT_STOCK_SYMBOLS
        )
        ak = self._get_akshare()
        ingested_at = datetime.utcnow()

        for symbol in symbols:
            suffix = _symbol_suffix(symbol)
            ts_code = f"{symbol}.{suffix}"
            try:
                df = ak.stock_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=since or "19900101",
                    end_date=until or datetime.utcnow().strftime("%Y%m%d"),
                    adjust="",
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "AKShareAdapter: failed fetching %s: %s", symbol, exc
                )
                continue

            if df is None or df.empty:
                continue

            for _, ak_row in df.iterrows():
                yield _map_row(
                    ak_row.to_dict(),
                    _DAILY_COL_MAP,
                    ts_code=ts_code,
                    source_table="akshare.stock_zh_a_hist",
                    ingested_at=ingested_at,
                )

    def _fetch_index_daily(
        self,
        *,
        since: str | None,
        until: str | None,
    ) -> Iterator[dict[str, Any]]:
        """Yield daily bars for each index in self._index_codes."""
        if not self._index_codes:
            logger.warning(
                "AKShareAdapter: no index_codes configured; "
                "fetch('index_daily') yields nothing."
            )
            return

        ak = self._get_akshare()
        ingested_at = datetime.utcnow()

        for ts_code in self._index_codes:
            symbol = _strip_suffix(ts_code)
            try:
                df = ak.index_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date=since or "19900101",
                    end_date=until or datetime.utcnow().strftime("%Y%m%d"),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "AKShareAdapter: failed fetching index %s: %s", ts_code, exc
                )
                continue

            if df is None or df.empty:
                continue

            for _, ak_row in df.iterrows():
                yield _map_row(
                    ak_row.to_dict(),
                    _INDEX_COL_MAP,
                    ts_code=ts_code,
                    source_table="akshare.index_zh_a_hist",
                    ingested_at=ingested_at,
                )
