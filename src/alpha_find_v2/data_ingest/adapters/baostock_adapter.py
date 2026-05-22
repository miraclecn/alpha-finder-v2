"""
BaostockAdapter — free fallback adapter for daily A-share price data.

Supports: "daily", "index_daily"
Output: Tushare-shaped row dicts (same columns as raw_kline_unadj / raw_index_daily).

Session management:
  - `bs.login()` is called lazily on the first `fetch()` call.
  - `bs.logout()` is called on `close()` or when used as a context manager.
"""

from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from typing import Any, Iterator

from .base import AdapterUnavailable

_SUPPORTED = frozenset({"daily", "index_daily"})

# Baostock fields requested in each query
_K_DATA_FIELDS = "date,open,high,low,close,preclose,volume,amount,pctChg"


def _to_baostock_code(ts_code: str) -> str:
    """Convert Tushare format to baostock format.

    '600001.SH' -> 'sh.600001'
    '000001.SZ' -> 'sz.000001'
    '000300.SH' -> 'sh.000300'  (index on SH)
    '399001.SZ' -> 'sz.399001'  (index on SZ)
    """
    symbol, exchange = ts_code.split(".")
    prefix = exchange.lower()
    return f"{prefix}.{symbol}"


def _to_tushare_code(baostock_code: str) -> str:
    """Convert baostock format back to Tushare format.

    'sh.600001' -> '600001.SH'
    """
    prefix, symbol = baostock_code.split(".", 1)
    return f"{symbol}.{prefix.upper()}"


def _parse_float(value: str) -> float | None:
    """Parse a string to float; return None if empty or unparseable."""
    if not value or value.strip() == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


class BaostockAdapter:
    """Adapter that fetches daily price data from the baostock library."""

    name = "baostock"

    def __init__(self, stock_codes: list[str] | None = None) -> None:
        self._stock_codes = stock_codes
        self._bs: types.ModuleType | None = None  # lazy-loaded baostock module
        self._logged_in = False

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
        """Yield Tushare-shaped dicts for `dataset_id`."""
        bs = self._require_baostock()
        self._ensure_login(bs)

        if dataset_id == "daily":
            yield from self._fetch_daily(bs, since=since, until=until)
        elif dataset_id == "index_daily":
            yield from self._fetch_index_daily(bs, since=since, until=until)
        else:
            raise ValueError(f"BaostockAdapter does not support dataset_id={dataset_id!r}")

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "BaostockAdapter":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._logged_in and self._bs is not None:
            self._bs.logout()
            self._logged_in = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_baostock(self) -> types.ModuleType:
        """Lazy-import baostock; raise AdapterUnavailable if not installed."""
        if self._bs is not None:
            return self._bs
        # Try to import from sys.modules first (allows test injection)
        bs = sys.modules.get("baostock")
        if bs is None:
            try:
                import importlib
                bs = importlib.import_module("baostock")
            except ImportError:
                raise AdapterUnavailable("baostock package not installed")
        self._bs = bs
        return bs

    def _ensure_login(self, bs: types.ModuleType) -> None:
        if not self._logged_in:
            bs.login()
            self._logged_in = True

    def _fetch_daily(
        self,
        bs: types.ModuleType,
        *,
        since: str | None,
        until: str | None,
    ) -> Iterator[dict[str, Any]]:
        """Fetch unadjusted daily bars (adjustflag='3') for each stock code."""
        codes = self._resolve_codes(bs)
        now = datetime.now(timezone.utc)
        for ts_code in codes:
            baostock_code = _to_baostock_code(ts_code)
            result = bs.query_history_k_data_plus(
                code=baostock_code,
                fields=_K_DATA_FIELDS,
                start_date=since,
                end_date=until,
                frequency="d",
                adjustflag="3",
            )
            for row in result.data:
                yield _map_k_data_row(row, ts_code=ts_code, ingested_at=now)

    def _fetch_index_daily(
        self,
        bs: types.ModuleType,
        *,
        since: str | None,
        until: str | None,
    ) -> Iterator[dict[str, Any]]:
        """Fetch daily bars for index codes."""
        codes = self._resolve_codes(bs)
        now = datetime.now(timezone.utc)
        for ts_code in codes:
            baostock_code = _to_baostock_code(ts_code)
            result = bs.query_history_k_data_plus(
                code=baostock_code,
                fields=_K_DATA_FIELDS,
                start_date=since,
                end_date=until,
                frequency="d",
                adjustflag="3",
            )
            for row in result.data:
                yield _map_k_data_row(row, ts_code=ts_code, ingested_at=now)

    def _resolve_codes(self, bs: types.ModuleType) -> list[str]:
        """Return stock codes to fetch. Falls back to empty list if none set."""
        if self._stock_codes is not None:
            return self._stock_codes
        return []


def _map_k_data_row(
    row: list[str],
    *,
    ts_code: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    """Map a baostock k_data row (list of strings) to a Tushare-shaped dict.

    Baostock column order for fields 'date,open,high,low,close,preclose,volume,amount,pctChg':
      index 0: date       (YYYY-MM-DD)
      index 1: open
      index 2: high
      index 3: low
      index 4: close
      index 5: preclose
      index 6: volume
      index 7: amount
      index 8: pctChg
    """
    date_str = row[0]  # YYYY-MM-DD
    trade_date = date_str.replace("-", "")  # YYYYMMDD

    return {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "open": _parse_float(row[1]),
        "high": _parse_float(row[2]),
        "low": _parse_float(row[3]),
        "close": _parse_float(row[4]),
        "pre_close": _parse_float(row[5]),
        "change": None,  # baostock does not provide absolute change
        "pct_chg": _parse_float(row[8]),
        "vol": _parse_float(row[6]),
        "amount": _parse_float(row[7]),
        "source_table": "baostock.k_data",
        "ingested_at": ingested_at,
    }
