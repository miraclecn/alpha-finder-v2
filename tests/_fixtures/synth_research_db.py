"""
Synthetic research-source DuckDB fixture for factor_evaluation tests.

Builds a tiny but structurally correct `research_source.duckdb` containing
tables that match what `market_data_bootstrap.build_research_source_db` produces:

    daily_bar_pit       — OHLCV + close_adj + turnover_value_cny
    raw_daily_basic     — pb, pe, free_share (for sector_relative_valuation)
    raw_adj_factor      — adj_factor per (security, date)
    industry_classification_pit  — PIT industry (sw2021_l1)
    benchmark_membership_pit     — CSI 800 mock membership
    market_trade_calendar        — trade date list
    security_master_ref          — listing info

Prices are deterministic monotone (constant daily growth per stock) so IC
tests can assert directional correctness without randomness.

Additional helper: `build_synth_raw_db` writes `raw_suspend_d` and
`raw_stk_limit` tables to a separate raw.duckdb for tradeability tests.
"""
from __future__ import annotations

import math
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb


_SECURITIES = [
    # (security_id, industry_code, growth_factor, initial_pb, initial_pe, free_share_mn)
    ("600001.SH", "bank",       1.0050, 0.80, 8.0,  5000.0),
    ("600002.SH", "bank",       1.0040, 1.20, 10.0, 3000.0),
    ("600003.SH", "tech",       1.0120, 3.50, 40.0, 1500.0),
    ("600004.SH", "tech",       1.0080, 2.80, 30.0, 2000.0),
    ("600005.SH", "industrial", 1.0060, 1.50, 15.0, 4000.0),
]

_BENCHMARK_ID = "CSI 800"
_INDUSTRY_SCHEMA = "sw2021_l1"


def _trading_days(start: date, n: int) -> list[date]:
    days: list[date] = []
    d = start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def build_synth_research_db(
    path: Path,
    *,
    n_securities: int = 5,
    n_dates: int = 250,
    start: date = date(2022, 1, 3),
    adj_factor_base: float = 1.0,
) -> None:
    """
    Build a deterministic synthetic research_source.duckdb.

    Prices grow at a constant daily rate per stock. The growth rate is ordered
    so that security ranks are stable (useful for IC monotonicity checks).

    Args:
        path: Destination path (created if absent; overwritten if present).
        n_securities: Number of securities (max 5 with default _SECURITIES list).
        n_dates: Number of trading days to generate.
        start: First trade date.
        adj_factor_base: Base adj_factor (constant for simplicity).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()

    securities = _SECURITIES[:n_securities]
    trade_dates = _trading_days(start, n_dates)
    date_strs = [d.strftime("%Y%m%d") for d in trade_dates]

    conn = duckdb.connect(str(path))

    # ------------------------------------------------------------------ market_trade_calendar
    conn.execute(
        "CREATE TABLE market_trade_calendar (trade_date VARCHAR PRIMARY KEY)"
    )
    conn.executemany(
        "INSERT INTO market_trade_calendar VALUES (?)",
        [(d,) for d in date_strs],
    )

    # ------------------------------------------------------------------ security_master_ref
    conn.execute(
        """
        CREATE TABLE security_master_ref (
            security_id VARCHAR PRIMARY KEY,
            symbol VARCHAR,
            current_name VARCHAR,
            exchange VARCHAR,
            board VARCHAR,
            area VARCHAR,
            list_date VARCHAR,
            delist_date VARCHAR,
            is_hs VARCHAR,
            is_a_share BOOLEAN,
            ingested_at TIMESTAMP
        )
        """
    )
    for sec_id, _, _, _, _, _ in securities:
        symbol = sec_id.split(".")[0]
        conn.execute(
            """
            INSERT INTO security_master_ref VALUES
            (?, ?, ?, 'SH', 'main_board', '上海', '20100101', NULL, 'N', TRUE, CURRENT_TIMESTAMP)
            """,
            [sec_id, symbol, f"Stock_{symbol}"],
        )

    # ------------------------------------------------------------------ daily_bar_pit
    conn.execute(
        """
        CREATE TABLE daily_bar_pit (
            security_id       VARCHAR,
            trade_date        VARCHAR,
            exchange          VARCHAR,
            board             VARCHAR,
            is_st             BOOLEAN,
            pre_close         DOUBLE,
            open              DOUBLE,
            high              DOUBLE,
            low               DOUBLE,
            close             DOUBLE,
            close_adj         DOUBLE,
            turnover_value_cny DOUBLE,
            volume_shares     DOUBLE,
            price_basis       VARCHAR,
            open_adj          DOUBLE,
            float_mcap_cny    DOUBLE,
            free_float_shares DOUBLE,
            PRIMARY KEY (security_id, trade_date)
        )
        """
    )

    bar_rows: list[tuple[Any, ...]] = []
    for sec_id, _, growth, _, _, free_share_mn in securities:
        base_price = 10.0
        for i, trade_date in enumerate(date_strs):
            price = base_price * (growth ** i)
            adj = adj_factor_base
            bar_rows.append((
                sec_id,
                trade_date,
                "SH",
                "main_board",
                False,
                price / growth,          # pre_close (previous day's close)
                price * 0.999,           # open (slight gap below close)
                price * 1.01,
                price * 0.99,
                price,
                price * adj,             # close_adj
                price * free_share_mn * 1e4 * 0.002,  # turnover_value_cny
                free_share_mn * 1e4 * 0.002,           # volume_shares
                "standard",
                price * 0.999 * adj,     # open_adj
                price * free_share_mn * 1e4,            # float_mcap_cny
                free_share_mn * 1e4,                    # free_float_shares
            ))

    conn.executemany(
        """
        INSERT INTO daily_bar_pit VALUES
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        bar_rows,
    )

    # ------------------------------------------------------------------ raw_adj_factor
    conn.execute(
        """
        CREATE TABLE raw_adj_factor (
            ts_code      VARCHAR,
            trade_date   VARCHAR,
            adj_factor   DOUBLE,
            source_table VARCHAR,
            ingested_at  TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date)
        )
        """
    )
    adj_rows = [
        (sec_id, d, adj_factor_base, "synth", None)
        for sec_id, _, _, _, _, _ in securities
        for d in date_strs
    ]
    conn.executemany(
        "INSERT INTO raw_adj_factor VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)",
        [(r[0], r[1], r[2], r[3]) for r in adj_rows],
    )

    # ------------------------------------------------------------------ raw_daily_basic
    conn.execute(
        """
        CREATE TABLE raw_daily_basic (
            ts_code        VARCHAR,
            trade_date     VARCHAR,
            close          DOUBLE,
            turnover_rate  DOUBLE,
            turnover_rate_f DOUBLE,
            volume_ratio   DOUBLE,
            pe             DOUBLE,
            pe_ttm         DOUBLE,
            pb             DOUBLE,
            ps             DOUBLE,
            ps_ttm         DOUBLE,
            dv_ratio       DOUBLE,
            dv_ttm         DOUBLE,
            total_share    DOUBLE,
            float_share    DOUBLE,
            free_share     DOUBLE,
            total_mv       DOUBLE,
            circ_mv        DOUBLE,
            source_table   VARCHAR,
            ingested_at    TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date)
        )
        """
    )
    basic_rows: list[tuple[Any, ...]] = []
    for sec_id, _, growth, initial_pb, initial_pe, free_share_mn in securities:
        for i, trade_date in enumerate(date_strs):
            price = 10.0 * (growth ** i)
            pb = initial_pb * (growth ** (i * 0.5))   # PB drifts slower
            pe = initial_pe
            basic_rows.append((
                sec_id,
                trade_date,
                price,
                0.002,  # turnover_rate
                0.002,
                1.0,    # volume_ratio
                pe, pe, pb, 0.0, 0.0, 0.0, 0.0,
                free_share_mn * 1.5 * 1e4,  # total_share
                free_share_mn * 1.2 * 1e4,  # float_share
                free_share_mn * 1e4,         # free_share
                price * free_share_mn * 1.5 * 1e4,
                price * free_share_mn * 1e4,
                "synth",
            ))
    conn.executemany(
        """
        INSERT INTO raw_daily_basic VALUES
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
        """,
        basic_rows,
    )

    # ------------------------------------------------------------------ industry_classification_pit
    conn.execute(
        """
        CREATE TABLE industry_classification_pit (
            security_id    VARCHAR,
            industry_schema VARCHAR,
            industry_code  VARCHAR,
            effective_at   VARCHAR,
            removed_at     VARCHAR,
            PRIMARY KEY (security_id, industry_schema, effective_at)
        )
        """
    )
    for sec_id, industry_code, _, _, _, _ in securities:
        conn.execute(
            """
            INSERT INTO industry_classification_pit VALUES (?, ?, ?, '20100101', NULL)
            """,
            [sec_id, _INDUSTRY_SCHEMA, industry_code],
        )

    # ------------------------------------------------------------------ benchmark_membership_pit
    conn.execute(
        """
        CREATE TABLE benchmark_membership_pit (
            benchmark_id VARCHAR,
            security_id  VARCHAR,
            effective_at VARCHAR,
            removed_at   VARCHAR,
            PRIMARY KEY (benchmark_id, security_id, effective_at)
        )
        """
    )
    for sec_id, _, _, _, _, _ in securities:
        conn.execute(
            """
            INSERT INTO benchmark_membership_pit VALUES (?, ?, '20100101', NULL)
            """,
            [_BENCHMARK_ID, sec_id],
        )

    conn.close()


def build_synth_raw_db(
    path: Path,
    *,
    research_db_path: Path,
    suspended_rows: set[tuple[str, str]] | None = None,
    limit_locked_rows: set[tuple[str, str]] | None = None,
) -> None:
    """
    Build a tiny raw.duckdb with raw_suspend_d and raw_stk_limit tables
    for tradeability-filter tests.

    Pulls the trade_date list from the given research_db.

    Args:
        path: Destination for raw.duckdb.
        research_db_path: Existing synth research_source.duckdb to read dates from.
        suspended_rows: Set of (security_id, trade_date) to mark suspended.
        limit_locked_rows: Set of (security_id, trade_date) to mark limit-locked.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()

    # Read dates and securities from research DB
    src_conn = duckdb.connect(str(research_db_path), read_only=True)
    dates = [r[0] for r in src_conn.execute(
        "SELECT trade_date FROM market_trade_calendar ORDER BY trade_date"
    ).fetchall()]
    sec_ids = [r[0] for r in src_conn.execute(
        "SELECT security_id FROM security_master_ref ORDER BY security_id"
    ).fetchall()]
    src_conn.close()

    conn = duckdb.connect(str(path))

    conn.execute(
        """
        CREATE TABLE raw_suspend_d (
            ts_code        VARCHAR,
            trade_date     VARCHAR,
            suspend_timing VARCHAR,
            suspend_type   VARCHAR,
            ingested_at    TIMESTAMP,
            source_table   VARCHAR,
            PRIMARY KEY (ts_code, trade_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE raw_stk_limit (
            trade_date   VARCHAR,
            ts_code      VARCHAR,
            up_limit     DOUBLE,
            down_limit   DOUBLE,
            pre_close    DOUBLE,
            ingested_at  TIMESTAMP,
            source_table VARCHAR,
            PRIMARY KEY (ts_code, trade_date)
        )
        """
    )

    for sec_id, trade_date in (suspended_rows or set()):
        conn.execute(
            "INSERT INTO raw_suspend_d VALUES (?, ?, 'all_day', 'S', CURRENT_TIMESTAMP, 'synth')",
            [sec_id, trade_date],
        )

    for sec_id, trade_date in (limit_locked_rows or set()):
        conn.execute(
            "INSERT INTO raw_stk_limit VALUES (?, ?, 11.0, 9.0, 10.0, CURRENT_TIMESTAMP, 'synth')",
            [trade_date, sec_id],
        )

    conn.close()
