"""
Descriptor compute registry for alpha-find-v2 Stage 2.

Usage:
    from alpha_find_v2.factor_evaluation.descriptor_compute import (
        REGISTRY, ComputeContext, get, list_registered,
    )

    spec = get("medium_term_relative_strength")
    df = spec.fn(ctx)   # returns DataFrame(trade_date, security_id, descriptor_value)
"""
from __future__ import annotations

import hashlib
import inspect
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .exceptions import DescriptorNotImplemented


# ---------------------------------------------------------------------------
# Core data types
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ComputeContext:
    """
    Everything a descriptor compute function needs.

    Attributes:
        conn: Open read-only connection to research_source.duckdb.
        start_date: First trade date to produce values for (YYYYMMDD).
        end_date: Last trade date to produce values for (YYYYMMDD).
        universe: Optional set of security_ids to keep; None = no filter at compute time.
    """
    conn: Any            # duckdb.DuckDBPyConnection (typed as Any to avoid import)
    start_date: str
    end_date: str
    universe: set[str] | None = None


@dataclass(frozen=True, slots=True)
class DescriptorComputeSpec:
    """
    Registry entry for one descriptor.

    Attributes:
        descriptor_id: Matches config/descriptors/<id>.toml.
        fn: Compute function returning DataFrame(trade_date, security_id, descriptor_value).
             None marks a stub (calls will raise DescriptorNotImplemented).
        requires: Input table ids required for compute (subset of V2 PIT tables).
        notes: Free-text documentation.
    """
    descriptor_id: str
    fn: Callable[[ComputeContext], pd.DataFrame] | None
    requires: tuple[str, ...]
    notes: str = ""


# ---------------------------------------------------------------------------
# Registry singleton
# ---------------------------------------------------------------------------

REGISTRY: dict[str, DescriptorComputeSpec] = {}


def register(spec: DescriptorComputeSpec) -> None:
    """Add a DescriptorComputeSpec to the global registry."""
    REGISTRY[spec.descriptor_id] = spec


def get(descriptor_id: str) -> DescriptorComputeSpec:
    """
    Retrieve a spec by id.

    Raises:
        KeyError: if id is not in the registry, with helpful message listing known ids.
    """
    if descriptor_id not in REGISTRY:
        known = sorted(REGISTRY.keys())
        raise KeyError(
            f"Descriptor '{descriptor_id}' is not registered. "
            f"Registered ids: {known}"
        )
    return REGISTRY[descriptor_id]


def list_registered() -> list[str]:
    """Return sorted list of all registered descriptor ids."""
    return sorted(REGISTRY.keys())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _frame_columns() -> tuple[str, str, str]:
    return "trade_date", "security_id", "descriptor_value"


def _validate_output(df: pd.DataFrame, descriptor_id: str) -> pd.DataFrame:
    """Assert the output frame has exactly the required columns."""
    required = set(_frame_columns())
    actual = set(df.columns.tolist())
    if required != actual:
        raise ValueError(
            f"Compute function for '{descriptor_id}' returned columns {actual}; "
            f"expected {required}"
        )
    return df


def descriptor_version(descriptor_id: str, toml_path: Path | None = None) -> str:
    """
    Compute a stable version string for a descriptor.

    Hashes:
    - The source of the registered compute function (if implemented).
    - The content of the TOML config file (if path is provided).

    Returns: "sha256:<hex>" string.
    """
    spec = get(descriptor_id)
    h = hashlib.sha256()
    if spec.fn is not None:
        try:
            src = inspect.getsource(spec.fn)
            h.update(src.encode("utf-8"))
        except (OSError, TypeError):
            h.update(b"<source_unavailable>")
    else:
        h.update(b"<stub>")
    if toml_path is not None and toml_path.exists():
        h.update(toml_path.read_bytes())
    return f"sha256:{h.hexdigest()}"


# ---------------------------------------------------------------------------
# In-scope descriptor implementations
# Imported and registered at the bottom of this module.
# ---------------------------------------------------------------------------


def _compute_medium_term_relative_strength(ctx: ComputeContext) -> pd.DataFrame:
    """
    60-day log return minus 5-day log return.

    Signal = log(close_adj[t] / close_adj[t-60]) - log(close_adj[t] / close_adj[t-5])
           = log(close_adj[t-5]) - log(close_adj[t-60])

    Higher is better (stock rose more in medium term than in very short term).
    """
    sql = """
    WITH lagged AS (
        SELECT
            security_id,
            trade_date,
            close_adj,
            LAG(close_adj, 60) OVER (PARTITION BY security_id ORDER BY trade_date) AS lag60,
            LAG(close_adj, 5)  OVER (PARTITION BY security_id ORDER BY trade_date) AS lag5
        FROM daily_bar_pit
        WHERE trade_date BETWEEN ? AND ?
           OR trade_date < ?   -- need history for lags
    ),
    filtered AS (
        SELECT
            security_id,
            trade_date,
            LN(close_adj / NULLIF(lag60, 0)) - LN(close_adj / NULLIF(lag5, 0))
                AS descriptor_value
        FROM lagged
        WHERE trade_date BETWEEN ? AND ?
          AND lag60 IS NOT NULL
          AND lag5  IS NOT NULL
          AND lag60 > 0
          AND lag5  > 0
          AND close_adj > 0
    )
    SELECT trade_date, security_id, descriptor_value
    FROM filtered
    ORDER BY trade_date, security_id
    """
    # We need some history before start_date for lags. Pull a wide window:
    import duckdb as _duckdb
    df = ctx.conn.execute(sql, [
        ctx.start_date, ctx.end_date,  # first WHERE clause (generous window)
        ctx.start_date,
        ctx.start_date, ctx.end_date,   # filtered WHERE clause
    ]).df()
    df = df.rename(columns={"trade_date": "trade_date",
                             "security_id": "security_id",
                             "descriptor_value": "descriptor_value"})
    if ctx.universe is not None:
        df = df[df["security_id"].isin(ctx.universe)]
    return _validate_output(df[list(_frame_columns())], "medium_term_relative_strength")


def _compute_trend_stability(ctx: ComputeContext) -> pd.DataFrame:
    """
    60-day rolling Sharpe of daily log returns: mean / std.

    Higher values indicate orderly, stable uptrends.
    """
    sql = """
    WITH returns AS (
        SELECT
            security_id,
            trade_date,
            LN(close_adj / NULLIF(LAG(close_adj, 1) OVER (PARTITION BY security_id ORDER BY trade_date), 0))
                AS daily_ret
        FROM daily_bar_pit
    ),
    rolling AS (
        SELECT
            security_id,
            trade_date,
            AVG(daily_ret) OVER (
                PARTITION BY security_id
                ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS mean_ret,
            STDDEV_SAMP(daily_ret) OVER (
                PARTITION BY security_id
                ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS std_ret,
            COUNT(daily_ret) OVER (
                PARTITION BY security_id
                ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS n_ret
        FROM returns
    )
    SELECT
        trade_date,
        security_id,
        mean_ret / NULLIF(std_ret, 0) AS descriptor_value
    FROM rolling
    WHERE trade_date BETWEEN ? AND ?
      AND n_ret >= 60
      AND std_ret > 0
    ORDER BY trade_date, security_id
    """
    df = ctx.conn.execute(sql, [ctx.start_date, ctx.end_date]).df()
    if ctx.universe is not None:
        df = df[df["security_id"].isin(ctx.universe)]
    return _validate_output(df[list(_frame_columns())], "trend_stability")


def _compute_turnover_confirmation(ctx: ComputeContext) -> pd.DataFrame:
    """
    Recent 5-day mean turnover / prior 55-day mean turnover.

    Detects volume confirmation alongside trend: ratio > 1 means rising participation.
    """
    sql = """
    WITH recent AS (
        SELECT
            security_id,
            trade_date,
            AVG(turnover_value_cny) OVER (
                PARTITION BY security_id
                ORDER BY trade_date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS mean_recent,
            AVG(turnover_value_cny) OVER (
                PARTITION BY security_id
                ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND 5 PRECEDING
            ) AS mean_prior,
            COUNT(*) OVER (
                PARTITION BY security_id
                ORDER BY trade_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS window_count
        FROM daily_bar_pit
    )
    SELECT
        trade_date,
        security_id,
        mean_recent / NULLIF(mean_prior, 0) AS descriptor_value
    FROM recent
    WHERE trade_date BETWEEN ? AND ?
      AND window_count >= 60
      AND mean_prior > 0
    ORDER BY trade_date, security_id
    """
    df = ctx.conn.execute(sql, [ctx.start_date, ctx.end_date]).df()
    if ctx.universe is not None:
        df = df[df["security_id"].isin(ctx.universe)]
    return _validate_output(df[list(_frame_columns())], "turnover_confirmation")


def _compute_industry_relative_strength(ctx: ComputeContext) -> pd.DataFrame:
    """
    Stock 60-day log return minus PIT industry mean 60-day log return.

    Positive = stock leads its industry. Uses sw2021_l1 PIT industry labels.
    """
    sql = """
    WITH lagged AS (
        SELECT
            b.security_id,
            b.trade_date,
            LN(b.close_adj / NULLIF(
                LAG(b.close_adj, 60) OVER (PARTITION BY b.security_id ORDER BY b.trade_date),
                0
            )) AS ret60,
            i.industry_code
        FROM daily_bar_pit b
        JOIN industry_classification_pit i
          ON  i.security_id    = b.security_id
          AND i.industry_schema = 'sw2021_l1'
          AND i.effective_at   <= b.trade_date
          AND (i.removed_at IS NULL OR i.removed_at > b.trade_date)
    ),
    industry_means AS (
        SELECT
            trade_date,
            industry_code,
            AVG(ret60) AS industry_ret60
        FROM lagged
        WHERE ret60 IS NOT NULL
        GROUP BY trade_date, industry_code
    )
    SELECT
        l.trade_date,
        l.security_id,
        l.ret60 - im.industry_ret60 AS descriptor_value
    FROM lagged l
    JOIN industry_means im
      ON im.trade_date    = l.trade_date
     AND im.industry_code = l.industry_code
    WHERE l.trade_date BETWEEN ? AND ?
      AND l.ret60 IS NOT NULL
    ORDER BY l.trade_date, l.security_id
    """
    df = ctx.conn.execute(sql, [ctx.start_date, ctx.end_date]).df()
    if ctx.universe is not None:
        df = df[df["security_id"].isin(ctx.universe)]
    return _validate_output(df[list(_frame_columns())], "industry_relative_strength")


def _compute_sector_relative_valuation(ctx: ComputeContext) -> pd.DataFrame:
    """
    Industry-relative cheapness: z-score of 1/PB within each PIT industry bucket.

    Cheaper-is-better: higher score = more attractive relative valuation.
    Uses daily_basic.pb and sw2021_l1 PIT industry labels.
    """
    df = ctx.conn.execute(
        """
        WITH val AS (
            SELECT
                d.ts_code                 AS security_id,
                d.trade_date,
                1.0 / NULLIF(d.pb, 0)     AS inv_pb,
                i.industry_code
            FROM raw_daily_basic d
            JOIN industry_classification_pit i
              ON  i.security_id    = d.ts_code
              AND i.industry_schema = 'sw2021_l1'
              AND i.effective_at   <= d.trade_date
              AND (i.removed_at IS NULL OR i.removed_at > d.trade_date)
            WHERE d.trade_date BETWEEN ? AND ?
              AND d.pb > 0
        ),
        with_stats AS (
            SELECT
                security_id,
                trade_date,
                inv_pb,
                AVG(inv_pb) OVER (PARTITION BY trade_date, industry_code) AS ind_mean,
                STDDEV_SAMP(inv_pb) OVER (PARTITION BY trade_date, industry_code) AS ind_std
            FROM val
        )
        SELECT
            trade_date,
            security_id,
            (inv_pb - ind_mean) / NULLIF(ind_std, 0) AS descriptor_value
        FROM with_stats
        WHERE ind_std > 0
        ORDER BY trade_date, security_id
        """,
        [ctx.start_date, ctx.end_date],
    ).df()
    if ctx.universe is not None:
        df = df[df["security_id"].isin(ctx.universe)]
    return _validate_output(df[list(_frame_columns())], "sector_relative_valuation")


# ---------------------------------------------------------------------------
# Register all in-scope descriptors
# ---------------------------------------------------------------------------

register(DescriptorComputeSpec(
    descriptor_id="medium_term_relative_strength",
    fn=_compute_medium_term_relative_strength,
    requires=("daily_bar_pit", "raw_adj_factor"),
    notes="60d log return minus 5d log return.",
))

register(DescriptorComputeSpec(
    descriptor_id="trend_stability",
    fn=_compute_trend_stability,
    requires=("daily_bar_pit", "raw_adj_factor"),
    notes="60d rolling Sharpe of daily log returns.",
))

register(DescriptorComputeSpec(
    descriptor_id="turnover_confirmation",
    fn=_compute_turnover_confirmation,
    requires=("daily_bar_pit",),
    notes="Recent 5d mean turnover / prior 55d mean turnover.",
))

register(DescriptorComputeSpec(
    descriptor_id="industry_relative_strength",
    fn=_compute_industry_relative_strength,
    requires=("daily_bar_pit", "raw_adj_factor", "industry_classification_pit"),
    notes="60d stock log return minus PIT industry mean log return (sw2021_l1).",
))

register(DescriptorComputeSpec(
    descriptor_id="sector_relative_valuation",
    fn=_compute_sector_relative_valuation,
    requires=("raw_daily_basic", "industry_classification_pit"),
    notes="Industry-relative z-score of 1/PB (cheaper-is-better).",
))
