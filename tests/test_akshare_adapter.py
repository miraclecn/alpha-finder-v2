"""Tests for AKShareAdapter.

Covers:
- supports() True/False
- fetch("daily") happy path: AKShare columns → Tushare-shaped row
- fetch("index_daily") happy path
- ts_code generated from bare symbol + SH/SZ heuristic
- injectable _akshare module used instead of real akshare
- missing akshare module → AdapterUnavailable (not ImportError)
- pre_close is always None (not available from unadjusted endpoint)
- PBT: random AKShare-shaped DataFrames always map to required Tushare keys
  with trade_date in YYYYMMDD format (8 digits, no dashes)

**Validates: Requirements R4.2, R4.3**
"""

from __future__ import annotations

import sys
from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from alpha_find_v2.data_ingest.adapters.base import AdapterUnavailable

# ---------------------------------------------------------------------------
# Required Tushare keys for raw_kline_unadj
# ---------------------------------------------------------------------------

_REQUIRED_DAILY_KEYS = {
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
    "source_table",
    "ingested_at",
}


# ---------------------------------------------------------------------------
# Helper: build a minimal AKShare-shaped DataFrame
# (stock_zh_a_hist with adjust='' returns these columns)
# ---------------------------------------------------------------------------


def _make_akshare_daily_df(
    dates: list[str] | None = None,
    n: int | None = None,
) -> pd.DataFrame:
    """Return a DataFrame with AKShare unadjusted daily column names.

    The real AKShare endpoint does NOT include 前收盘 for adjust=''.
    """
    if dates is None:
        dates = ["2024-01-02", "2024-01-03"] if n is None else [f"2024-01-{i+2:02d}" for i in range(n)]
    k = len(dates)
    opens = [10.0 + i for i in range(k)]
    return pd.DataFrame(
        {
            "日期":  dates,
            "开盘":  opens,
            "收盘":  [o + 0.5 for o in opens],
            "最高":  [o + 1.0 for o in opens],
            "最低":  [o - 0.5 for o in opens],
            "成交量": [100_000.0 + i * 1000 for i in range(k)],
            "成交额": [1_050_000.0 + i * 10_000 for i in range(k)],
            "振幅":  [2.0] * k,
            "涨跌幅": [1.0 + i * 0.1 for i in range(k)],
            "涨跌额": [0.5 + i * 0.05 for i in range(k)],
            "换手率": [0.5] * k,
        }
    )


def _make_fake_akshare(
    daily_df: pd.DataFrame,
    index_df: pd.DataFrame | None = None,
) -> MagicMock:
    """Return a MagicMock that behaves like the akshare module."""
    fake_ak = MagicMock()
    fake_ak.stock_zh_a_hist.return_value = daily_df
    if index_df is not None:
        fake_ak.index_zh_a_hist.return_value = index_df
    return fake_ak


# ---------------------------------------------------------------------------
# supports()
# ---------------------------------------------------------------------------


def test_supports_daily_true():
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    assert AKShareAdapter().supports("daily") is True


def test_supports_index_daily_true():
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    assert AKShareAdapter().supports("index_daily") is True


def test_supports_stock_basic_false():
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    assert AKShareAdapter().supports("stock_basic") is False


def test_supports_fina_indicator_false():
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    assert AKShareAdapter().supports("fina_indicator") is False


# ---------------------------------------------------------------------------
# fetch("daily") with injectable _akshare
# ---------------------------------------------------------------------------


def test_fetch_daily_returns_tushare_shaped_rows():
    """Fake ak.stock_zh_a_hist returns a DataFrame → rows have all required keys."""
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    fake_ak = _make_fake_akshare(_make_akshare_daily_df())
    adapter = AKShareAdapter(symbols=["000001"], _akshare=fake_ak)
    rows = list(adapter.fetch("daily", since="20240101", until="20240131", full=False))

    assert len(rows) == 2
    for row in rows:
        missing = _REQUIRED_DAILY_KEYS - row.keys()
        assert not missing, f"Missing keys: {missing}"


def test_fetch_daily_trade_date_is_yyyymmdd():
    """'2024-01-02' must map to '20240102'."""
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    fake_ak = _make_fake_akshare(_make_akshare_daily_df(["2024-01-02"]))
    adapter = AKShareAdapter(symbols=["000001"], _akshare=fake_ak)
    rows = list(adapter.fetch("daily", since="20240101", until="20240131", full=False))

    assert rows[0]["trade_date"] == "20240102"


def test_fetch_daily_ts_code_sh_heuristic():
    """Symbol starting with '6' → ts_code ends with .SH."""
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    fake_ak = _make_fake_akshare(_make_akshare_daily_df(["2024-01-02"]))
    adapter = AKShareAdapter(symbols=["600000"], _akshare=fake_ak)
    rows = list(adapter.fetch("daily", since="20240101", until="20240131", full=False))

    assert rows[0]["ts_code"] == "600000.SH"


def test_fetch_daily_ts_code_sz_heuristic():
    """Symbol not starting with '6' → ts_code ends with .SZ."""
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    fake_ak = _make_fake_akshare(_make_akshare_daily_df(["2024-01-02"]))
    adapter = AKShareAdapter(symbols=["000001"], _akshare=fake_ak)
    rows = list(adapter.fetch("daily", since="20240101", until="20240131", full=False))

    assert rows[0]["ts_code"] == "000001.SZ"


def test_fetch_daily_pre_close_is_none():
    """pre_close is always None (not in unadjusted AKShare endpoint)."""
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    fake_ak = _make_fake_akshare(_make_akshare_daily_df(["2024-01-02"]))
    adapter = AKShareAdapter(symbols=["600000"], _akshare=fake_ak)
    rows = list(adapter.fetch("daily", since="20240101", until="20240131", full=False))

    assert rows[0]["pre_close"] is None


def test_fetch_daily_source_table():
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    fake_ak = _make_fake_akshare(_make_akshare_daily_df(["2024-01-02"]))
    adapter = AKShareAdapter(symbols=["600000"], _akshare=fake_ak)
    rows = list(adapter.fetch("daily", since="20240101", until="20240131", full=False))

    assert rows[0]["source_table"] == "akshare.stock_zh_a_hist"


def test_fetch_daily_ingested_at_is_datetime():
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    fake_ak = _make_fake_akshare(_make_akshare_daily_df(["2024-01-02"]))
    adapter = AKShareAdapter(symbols=["600000"], _akshare=fake_ak)
    rows = list(adapter.fetch("daily", since="20240101", until="20240131", full=False))

    assert isinstance(rows[0]["ingested_at"], datetime)


def test_fetch_daily_strips_suffix_before_calling_akshare():
    """AKShare is called with bare symbol, not '600000.SH'."""
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    fake_ak = _make_fake_akshare(_make_akshare_daily_df(["2024-01-02"]))
    adapter = AKShareAdapter(symbols=["600000"], _akshare=fake_ak)
    list(adapter.fetch("daily", since="20240101", until="20240131", full=False))

    call_kwargs = fake_ak.stock_zh_a_hist.call_args
    assert call_kwargs.kwargs["symbol"] == "600000"


# ---------------------------------------------------------------------------
# Default symbol list — no symbols → uses hardcoded 10 symbols
# ---------------------------------------------------------------------------


def test_fetch_daily_uses_default_symbols_when_none():
    """When symbols=None, adapter falls back to 10 default symbols."""
    from alpha_find_v2.data_ingest.adapters import akshare_adapter as _mod

    # Return a 1-row DF for every call
    fake_ak = MagicMock()
    fake_ak.stock_zh_a_hist.return_value = _make_akshare_daily_df(["2024-01-02"])

    adapter = _mod.AKShareAdapter(symbols=None, _akshare=fake_ak)
    rows = list(adapter.fetch("daily", since="20240101", until="20240131", full=False))

    # 10 symbols × 1 row each
    assert len(rows) == len(_mod._DEFAULT_STOCK_SYMBOLS)
    assert fake_ak.stock_zh_a_hist.call_count == len(_mod._DEFAULT_STOCK_SYMBOLS)


# ---------------------------------------------------------------------------
# fetch("index_daily") with injectable _akshare
# ---------------------------------------------------------------------------


def test_fetch_index_daily_returns_tushare_shaped_rows():
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    index_df = _make_akshare_daily_df(["2024-01-02", "2024-01-03"])
    fake_ak = MagicMock()
    fake_ak.index_zh_a_hist.return_value = index_df
    adapter = AKShareAdapter(index_codes=["000300.SH"], _akshare=fake_ak)
    rows = list(adapter.fetch("index_daily", since="20240101", until="20240131", full=False))

    assert len(rows) == 2
    for row in rows:
        missing = _REQUIRED_DAILY_KEYS - row.keys()
        assert not missing, f"Missing keys: {missing}"


def test_fetch_index_daily_source_table():
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    index_df = _make_akshare_daily_df(["2024-01-02"])
    fake_ak = MagicMock()
    fake_ak.index_zh_a_hist.return_value = index_df
    adapter = AKShareAdapter(index_codes=["000300.SH"], _akshare=fake_ak)
    rows = list(adapter.fetch("index_daily", since="20240101", until="20240131", full=False))

    assert rows[0]["source_table"] == "akshare.index_zh_a_hist"


def test_fetch_index_daily_no_codes_yields_nothing():
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    fake_ak = MagicMock()
    adapter = AKShareAdapter(index_codes=None, _akshare=fake_ak)
    rows = list(adapter.fetch("index_daily", since="20240101", until="20240131", full=False))
    assert rows == []


# ---------------------------------------------------------------------------
# Missing akshare → AdapterUnavailable (not ImportError)
# ---------------------------------------------------------------------------


def test_missing_akshare_raises_adapter_unavailable():
    """When _akshare not injected and akshare not installed, raises AdapterUnavailable."""
    from alpha_find_v2.data_ingest.adapters import akshare_adapter as _mod

    original = sys.modules.get("akshare", None)
    sys.modules["akshare"] = None  # type: ignore[assignment]
    try:
        with pytest.raises(AdapterUnavailable, match="akshare package not installed"):
            _mod._load_akshare()
    finally:
        if original is None:
            sys.modules.pop("akshare", None)
        else:
            sys.modules["akshare"] = original


def test_adapter_instantiation_with_fake_module_does_not_import_real_akshare():
    """AKShareAdapter(symbols=[...], _akshare=fake) never tries to import akshare."""
    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    fake_ak = _make_fake_akshare(_make_akshare_daily_df(["2024-01-02"]))
    # Remove akshare from sys.modules to confirm no real import occurs
    original = sys.modules.pop("akshare", None)
    try:
        adapter = AKShareAdapter(symbols=["000001"], _akshare=fake_ak)
        rows = list(adapter.fetch("daily", since="20240101", until="20240131", full=False))
        assert len(rows) == 1
    finally:
        if original is not None:
            sys.modules["akshare"] = original


# ---------------------------------------------------------------------------
# PBT: random AKShare-shaped DataFrames always map to required Tushare keys
# ---------------------------------------------------------------------------


def test_pbt_random_akshare_frames_map_to_tushare_shape():
    """**Validates: Requirements R4.2, R4.3**

    Property: for any AKShare-shaped DataFrame (unadjusted columns) with
    valid dates and positive prices, all yielded rows contain the required
    Tushare keys and trade_date is always an 8-digit YYYYMMDD string with no
    dashes.
    """
    import random

    from alpha_find_v2.data_ingest.adapters.akshare_adapter import AKShareAdapter

    rng = random.Random(42)

    def random_date_str() -> str:
        year = rng.randint(2010, 2024)
        month = rng.randint(1, 12)
        day = rng.randint(1, 28)
        return f"{year:04d}-{month:02d}-{day:02d}"

    def random_positive() -> float:
        return round(rng.uniform(0.01, 9999.99), 2)

    def make_random_df(n: int) -> pd.DataFrame:
        dates = [random_date_str() for _ in range(n)]
        opens = [random_positive() for _ in range(n)]
        return pd.DataFrame(
            {
                "日期":  dates,
                "开盘":  opens,
                "收盘":  [o + rng.uniform(-1, 1) for o in opens],
                "最高":  [o + rng.uniform(0, 5) for o in opens],
                "最低":  [max(0.01, o - rng.uniform(0, 5)) for o in opens],
                "成交量": [rng.uniform(1_000, 1_000_000) for _ in range(n)],
                "成交额": [rng.uniform(10_000, 100_000_000) for _ in range(n)],
                "振幅":  [rng.uniform(0, 10) for _ in range(n)],
                "涨跌幅": [rng.uniform(-10, 10) for _ in range(n)],
                "涨跌额": [rng.uniform(-10, 10) for _ in range(n)],
                "换手率": [rng.uniform(0, 5) for _ in range(n)],
            }
        )

    NUM_TRIALS = 50
    for trial in range(NUM_TRIALS):
        n_rows = rng.randint(1, 20)
        df = make_random_df(n_rows)

        # Vary between SH (starts with 6) and SZ (starts with 0/3) symbols
        if rng.random() > 0.5:
            symbol = f"6{rng.randint(0, 99999):05d}"
        else:
            prefix = rng.choice(["0", "3"])
            symbol = f"{prefix}{rng.randint(0, 99999):05d}"

        fake_ak = _make_fake_akshare(df)
        adapter = AKShareAdapter(symbols=[symbol], _akshare=fake_ak)
        rows = list(adapter.fetch("daily", since="20100101", until="20241231", full=True))

        assert len(rows) == n_rows, (
            f"trial={trial}: expected {n_rows} rows, got {len(rows)}"
        )

        expected_ts_code = f"{symbol}.{'SH' if symbol.startswith('6') else 'SZ'}"

        for row in rows:
            # All required keys present
            missing = _REQUIRED_DAILY_KEYS - row.keys()
            assert not missing, f"trial={trial}: Missing keys: {missing}"

            # trade_date is 8-digit YYYYMMDD
            td = row["trade_date"]
            assert isinstance(td, str) and len(td) == 8 and td.isdigit(), (
                f"trial={trial}: trade_date={td!r} is not YYYYMMDD"
            )
            assert "-" not in td, f"trial={trial}: trade_date={td!r} contains dashes"

            # ts_code is symbol + correct suffix
            assert row["ts_code"] == expected_ts_code, (
                f"trial={trial}: ts_code={row['ts_code']!r} != {expected_ts_code!r}"
            )

            # pre_close is always None
            assert row["pre_close"] is None, (
                f"trial={trial}: pre_close={row['pre_close']!r} should be None"
            )

            # source_table
            assert row["source_table"] == "akshare.stock_zh_a_hist"

            # ingested_at is datetime
            assert isinstance(row["ingested_at"], datetime)
