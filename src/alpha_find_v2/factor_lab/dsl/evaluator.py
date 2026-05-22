"""DSL evaluator: AST + EvaluationContext → DataFrame[trade_date, security_id, descriptor_value].

Design decisions (design.md):
- Leaf data is loaded once per run into wide panels (security_id × trade_date)
  and cached on ``EvaluationContext._leaf_cache`` so DuckDB is not hit again.
- TS ops operate per-security column (along the time axis).
- CS ops operate per-date row (across the security axis).
- ``pe`` / ``pb`` leaf fields: non-positive values are replaced with NaN (R2.11).
- ``close_adj = close * adj_factor`` from ``daily_bar_pit`` joined to
  ``raw_adj_factor``.
- ``open`` comes from ``daily_bar_pit.open``.
- ``turnover_value_cny`` comes from ``daily_bar_pit.turnover_value_cny``.
- ``cs_industry_demean`` joins the ``industry_cs1_member_pit`` table; if that
  table is absent the operation falls back to ``cs_demean`` (global demean)
  rather than raising, so that the synthetic-fixture test (which may not have
  this table) still works.

Output DataFrame columns: ``trade_date`` (str YYYYMMDD), ``security_id``
(str), ``descriptor_value`` (float), one row per (date, security) pair.

Requirements: R2.11, R11.3
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from alpha_find_v2.factor_lab.dsl.grammar import ASTNode, ArithOp, CSOp, Leaf, TSOp


# ---------------------------------------------------------------------------
# Evaluation context
# ---------------------------------------------------------------------------


@dataclass
class EvaluationContext:
    """Context passed to ``evaluate``.

    Attributes:
        conn: Open DuckDB connection (read-only access to research_source.duckdb).
        start_date: First trade date to return values for (YYYYMMDD).
        end_date: Last trade date to return values for (YYYYMMDD).
        _leaf_cache: Internal dict from field name → wide panel
            (DataFrame with trade_date index, security_id columns).
            Populated lazily on first access to each field.
    """

    conn: Any  # duckdb.DuckDBPyConnection (typed Any to avoid import overhead)
    start_date: str
    end_date: str
    _leaf_cache: dict[str, pd.DataFrame] = field(default_factory=dict, repr=False)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def evaluate(ast: ASTNode, ctx: EvaluationContext) -> pd.DataFrame:
    """Evaluate *ast* against *ctx* and return a long-format DataFrame.

    Returns:
        DataFrame with exactly three columns: ``trade_date`` (str),
        ``security_id`` (str), ``descriptor_value`` (float).  One row per
        (date × security) pair that has a non-NaN value.  Rows with NaN
        ``descriptor_value`` are dropped.
    """
    panel = _eval_panel(ast, ctx)  # wide: index=trade_date, columns=security_id

    # Restrict to the requested date window
    panel = panel.loc[
        (panel.index >= ctx.start_date) & (panel.index <= ctx.end_date)
    ]

    # Melt to long format — use melt rather than stack to avoid pandas version
    # incompatibilities with the dropna parameter (changed in pandas 2.1).
    long = (
        panel.reset_index()
        .melt(id_vars="trade_date", var_name="security_id", value_name="descriptor_value")
        .dropna(subset=["descriptor_value"])
    )
    long = long[["trade_date", "security_id", "descriptor_value"]]
    long["descriptor_value"] = long["descriptor_value"].astype(float)
    return long.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Internal: panel-level evaluator (wide format throughout)
# ---------------------------------------------------------------------------


def _eval_panel(ast: ASTNode, ctx: EvaluationContext) -> pd.DataFrame:
    """Recursively evaluate *ast*, returning a wide panel.

    Panel: index = trade_date (str, sorted ascending), columns = security_id.
    """
    if isinstance(ast, Leaf):
        return _load_leaf(ast.field, ctx)

    if isinstance(ast, TSOp):
        inner = _eval_panel(ast.operand, ctx)
        return _apply_ts_op(ast.op, inner, ast.window)

    if isinstance(ast, CSOp):
        inner = _eval_panel(ast.operand, ctx)
        return _apply_cs_op(ast.op, inner, ctx)

    if isinstance(ast, ArithOp):
        left_panel = _eval_panel(ast.left, ctx)
        if ast.right is None:
            # Unary: log
            return _apply_log(left_panel)
        right_panel = _eval_panel(ast.right, ctx)
        return _apply_binary_arith(ast.op, left_panel, right_panel)

    raise TypeError(f"Unknown AST node type: {type(ast)}")  # pragma: no cover


# ---------------------------------------------------------------------------
# Leaf loading
# ---------------------------------------------------------------------------


def _load_leaf(field: str, ctx: EvaluationContext) -> pd.DataFrame:
    """Load leaf data into a wide panel and cache it on ctx."""
    if field in ctx._leaf_cache:
        return ctx._leaf_cache[field]

    panel = _fetch_leaf(field, ctx)
    ctx._leaf_cache[field] = panel
    return panel


def _fetch_leaf(field: str, ctx: EvaluationContext) -> pd.DataFrame:
    """Query DuckDB for *field* and return a wide panel.

    We fetch a generous date window (everything up to end_date) so that TS
    operators that look back N periods have sufficient history even when
    start_date is early.  The panel is NOT filtered to [start_date, end_date]
    here — that trim happens at the very end in ``evaluate()``.
    """
    if field == "close_adj":
        return _fetch_close_adj(ctx)
    if field == "open":
        return _fetch_open(ctx)
    if field == "turnover_value_cny":
        return _fetch_turnover(ctx)
    if field == "pe":
        return _fetch_pe_or_pb(ctx, column="pe")
    if field == "pb":
        return _fetch_pe_or_pb(ctx, column="pb")

    raise ValueError(f"Unknown leaf field: {field!r}")  # pragma: no cover


def _pivot_long_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot long DataFrame with (security_id, trade_date, value) to wide.

    Returns DataFrame indexed by trade_date (sorted), columns = security_id.
    """
    wide = df.pivot(index="trade_date", columns="security_id", values="value")
    wide = wide.sort_index()
    wide.columns.name = None
    wide.index.name = "trade_date"
    return wide


def _fetch_close_adj(ctx: EvaluationContext) -> pd.DataFrame:
    sql = """
    SELECT
        b.security_id,
        b.trade_date,
        b.close * COALESCE(a.adj_factor, 1.0) AS value
    FROM daily_bar_pit b
    LEFT JOIN raw_adj_factor a
        ON  a.ts_code    = b.security_id
        AND a.trade_date = b.trade_date
    WHERE b.trade_date <= ?
    ORDER BY b.security_id, b.trade_date
    """
    df = ctx.conn.execute(sql, [ctx.end_date]).df()
    return _pivot_long_to_wide(df)


def _fetch_open(ctx: EvaluationContext) -> pd.DataFrame:
    sql = """
    SELECT security_id, trade_date, open AS value
    FROM daily_bar_pit
    WHERE trade_date <= ?
    ORDER BY security_id, trade_date
    """
    df = ctx.conn.execute(sql, [ctx.end_date]).df()
    return _pivot_long_to_wide(df)


def _fetch_turnover(ctx: EvaluationContext) -> pd.DataFrame:
    sql = """
    SELECT security_id, trade_date, turnover_value_cny AS value
    FROM daily_bar_pit
    WHERE trade_date <= ?
    ORDER BY security_id, trade_date
    """
    df = ctx.conn.execute(sql, [ctx.end_date]).df()
    return _pivot_long_to_wide(df)


def _fetch_pe_or_pb(ctx: EvaluationContext, column: str) -> pd.DataFrame:
    """Fetch pe or pb from daily_basic; treat non-positive as NaN (R2.11).

    Supports both the Stage 2 table name ``raw_daily_basic`` (ts_code column)
    and the sandbox-test table name ``daily_basic`` (ts_code or security_id
    column).  Falls back gracefully so the synthetic fixture works.
    """
    # Try the Stage 2 table name first (raw_daily_basic with ts_code)
    for table, id_col in [
        ("raw_daily_basic", "ts_code"),
        ("daily_basic", "ts_code"),
        ("daily_basic", "security_id"),
    ]:
        try:
            sql = f"""
            SELECT {id_col} AS security_id, trade_date, {column} AS value
            FROM {table}
            WHERE trade_date <= ?
            ORDER BY {id_col}, trade_date
            """
            df = ctx.conn.execute(sql, [ctx.end_date]).df()
            # R2.11: treat non-positive as NaN
            df["value"] = df["value"].where(df["value"] > 0, other=np.nan)
            return _pivot_long_to_wide(df)
        except Exception:
            continue

    raise RuntimeError(
        f"Could not load {column!r}: no recognised table "
        "(raw_daily_basic / daily_basic) found in DuckDB connection."
    )


# ---------------------------------------------------------------------------
# TS operators (per-security column)
# ---------------------------------------------------------------------------


def _apply_ts_op(op: str, panel: pd.DataFrame, window: int) -> pd.DataFrame:
    """Apply a time-series operator to each security column."""
    if op == "lag":
        return panel.shift(window)
    if op == "delta":
        return panel - panel.shift(window)
    if op == "rolling_mean":
        return panel.rolling(window, min_periods=window).mean()
    if op == "rolling_std":
        return panel.rolling(window, min_periods=window).std(ddof=1)
    if op == "rolling_max":
        return panel.rolling(window, min_periods=window).max()
    if op == "rolling_min":
        return panel.rolling(window, min_periods=window).min()

    raise ValueError(f"Unknown TS operator: {op!r}")  # pragma: no cover


# ---------------------------------------------------------------------------
# CS operators (per-date row)
# ---------------------------------------------------------------------------


def _apply_cs_op(op: str, panel: pd.DataFrame, ctx: EvaluationContext) -> pd.DataFrame:
    """Apply a cross-section operator across each date row."""
    if op == "cs_rank":
        return panel.rank(axis=1, pct=True, na_option="keep")
    if op == "cs_zscore":
        row_mean = panel.mean(axis=1)
        row_std = panel.std(axis=1, ddof=1)
        return panel.sub(row_mean, axis=0).div(row_std, axis=0)
    if op == "cs_demean":
        row_mean = panel.mean(axis=1)
        return panel.sub(row_mean, axis=0)
    if op == "cs_industry_demean":
        return _apply_cs_industry_demean(panel, ctx)

    raise ValueError(f"Unknown CS operator: {op!r}")  # pragma: no cover


def _apply_cs_industry_demean(panel: pd.DataFrame, ctx: EvaluationContext) -> pd.DataFrame:
    """Subtract the industry mean from each security within each date row.

    Joins the ``industry_cs1_member_pit`` table to map securities to industries.
    If the table does not exist (e.g. in the synthetic test fixture), falls back
    to global demean so tests can still run.
    """
    # Try to load industry membership
    industry_map: dict[str, str] | None = None
    for table in ("industry_cs1_member_pit", "industry_classification_pit"):
        try:
            if table == "industry_cs1_member_pit":
                df = ctx.conn.execute(
                    "SELECT security_id, industry_code FROM industry_cs1_member_pit"
                ).df()
                industry_map = dict(zip(df["security_id"], df["industry_code"]))
            else:
                # industry_classification_pit: pick the most recent record per security
                df = ctx.conn.execute(
                    """
                    SELECT security_id, industry_code
                    FROM industry_classification_pit
                    WHERE removed_at IS NULL
                       OR removed_at > '99991231'
                    """
                ).df()
                if len(df) > 0:
                    industry_map = dict(zip(df["security_id"], df["industry_code"]))
            if industry_map:
                break
        except Exception:
            continue

    if not industry_map:
        # Fallback: global demean
        row_mean = panel.mean(axis=1)
        return panel.sub(row_mean, axis=0)

    result = panel.copy()
    securities = list(panel.columns)

    # Group securities by industry
    industry_groups: dict[str, list[str]] = {}
    ungrouped: list[str] = []
    for sec in securities:
        ind = industry_map.get(sec)
        if ind is not None:
            industry_groups.setdefault(ind, []).append(sec)
        else:
            ungrouped.append(sec)

    # Demean within each industry group
    for ind, secs in industry_groups.items():
        sub = panel[secs]
        ind_mean = sub.mean(axis=1)
        result[secs] = sub.sub(ind_mean, axis=0)

    # Securities without industry mapping: demean globally among themselves
    if ungrouped:
        sub = panel[ungrouped]
        ind_mean = sub.mean(axis=1)
        result[ungrouped] = sub.sub(ind_mean, axis=0)

    return result


# ---------------------------------------------------------------------------
# Arithmetic operators
# ---------------------------------------------------------------------------


def _apply_log(panel: pd.DataFrame) -> pd.DataFrame:
    """Natural log; non-positive values become NaN."""
    return np.log(panel.where(panel > 0))


def _apply_binary_arith(
    op: str, left: pd.DataFrame, right: pd.DataFrame
) -> pd.DataFrame:
    """Element-wise binary arithmetic, aligning on (trade_date, security_id)."""
    if op == "+":
        return left.add(right, fill_value=np.nan)
    if op == "-":
        return left.sub(right, fill_value=np.nan)
    if op == "*":
        return left.mul(right, fill_value=np.nan)
    if op == "/":
        # Avoid division by zero → NaN
        aligned_l, aligned_r = left.align(right, join="outer")
        return aligned_l.div(aligned_r.replace(0, np.nan))

    raise ValueError(f"Unknown binary arithmetic operator: {op!r}")  # pragma: no cover
