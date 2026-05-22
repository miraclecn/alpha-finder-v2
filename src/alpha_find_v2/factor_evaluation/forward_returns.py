"""
Forward-return calculator for the descriptor evaluation harness.

Uses LEAD() OVER (PARTITION BY security_id ORDER BY trade_date) to compute
open-to-open returns H trade-calendar days forward.

Entry price: open[t+1]
Exit price:  open[t+1+H]
Return:      open[t+1+H] / open[t+1] - 1.0

Adj-factor is applied so returns are on a comparable, split-adjusted basis.
"""
from __future__ import annotations

from typing import Any

import pandas as pd


def compute_forward_returns(
    conn: Any,
    *,
    start_date: str,
    end_date: str,
    horizons: tuple[int, ...],
) -> dict[int, pd.DataFrame]:
    """
    Compute forward open-to-open returns for each horizon.

    Args:
        conn: Open DuckDB connection to research_source.duckdb.
        start_date: Signal date window start (YYYYMMDD).
        end_date:   Signal date window end (YYYYMMDD).
        horizons:   Tuple of trade-calendar-day horizons (e.g. (5, 20, 60)).

    Returns:
        dict mapping horizon -> DataFrame with columns:
            (security_id, trade_date, forward_return, open_t1, open_t1_h)
        trade_date = signal date t.
        open_t1    = open at t+1 (entry).
        open_t1_h  = open at t+1+H (exit).
        forward_return = open_t1_h / open_t1 - 1.0.
        Rows where entry or exit open is NULL are dropped.
    """
    results: dict[int, pd.DataFrame] = {}
    for H in horizons:
        df = _forward_return_for_horizon(conn, start_date=start_date, end_date=end_date, H=H)
        results[H] = df
    return results


def _forward_return_for_horizon(
    conn: Any,
    *,
    start_date: str,
    end_date: str,
    H: int,
) -> pd.DataFrame:
    """
    Single-horizon forward return computation.

    LEAD(open_adj, 1)   → entry open (t+1)
    LEAD(open_adj, 1+H) → exit open (t+1+H)
    """
    # Use open * adj_factor for the adjusted open price.
    # The adj_factor may not be 1.0 after corporate actions.
    sql = f"""
    WITH bars AS (
        SELECT
            b.security_id,
            b.trade_date,
            b.open * COALESCE(a.adj_factor, 1.0) AS open_adj
        FROM daily_bar_pit b
        LEFT JOIN raw_adj_factor a
          ON  a.ts_code    = b.security_id
          AND a.trade_date = b.trade_date
        WHERE b.open IS NOT NULL
    ),
    with_lead AS (
        SELECT
            security_id,
            trade_date,
            open_adj,
            LEAD(open_adj, 1)     OVER (PARTITION BY security_id ORDER BY trade_date) AS open_t1,
            LEAD(open_adj, {1 + H}) OVER (PARTITION BY security_id ORDER BY trade_date) AS open_t1_h
        FROM bars
    )
    SELECT
        security_id,
        trade_date,
        open_t1,
        open_t1_h,
        (open_t1_h / NULLIF(open_t1, 0)) - 1.0 AS forward_return
    FROM with_lead
    WHERE trade_date BETWEEN ? AND ?
      AND open_t1   IS NOT NULL
      AND open_t1_h IS NOT NULL
      AND open_t1   > 0
    ORDER BY security_id, trade_date
    """
    df = conn.execute(sql, [start_date, end_date]).df()
    return df
