"""Unit tests for DSL canonical.py and evaluator.py.

Verification target: Task 4 (R2.11, R11.3)

Tests run against a minimal synthetic 5-stock × 60-date in-memory DuckDB so
they never touch real data and run in under a second.

**Validates: Requirements R2.11, R11.3**
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import pytest

from alpha_find_v2.factor_lab.dsl.canonical import canonical
from alpha_find_v2.factor_lab.dsl.evaluator import EvaluationContext, evaluate
from alpha_find_v2.factor_lab.dsl.grammar import ArithOp, CSOp, Leaf, TSOp
from alpha_find_v2.factor_lab.dsl.parser import parse


# ---------------------------------------------------------------------------
# Synthetic DuckDB fixture
# ---------------------------------------------------------------------------

_N_SECURITIES = 5
_N_DATES = 60

_SECURITIES = [f"S{i:03d}.SH" for i in range(1, _N_SECURITIES + 1)]
# Deterministic growth rates so we can predict outcomes
_GROWTH = [1.001, 1.002, 1.003, 1.004, 1.005]
_INDUSTRY = ["A", "A", "B", "B", "C"]  # 3 industry groups

# PE/PB: first security gets negative PE (should become NaN, R2.11)
_PE = [-5.0, 10.0, 20.0, 30.0, 40.0]
_PB = [0.0, 1.0, 2.0, 3.0, 4.0]   # first is zero (non-positive → NaN)


def _trading_days(n: int, start: date = date(2022, 1, 3)) -> list[str]:
    days: list[str] = []
    d = start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return days


def _build_synth_db() -> duckdb.DuckDBPyConnection:
    """Build an in-memory DuckDB with the tables the evaluator needs."""
    conn = duckdb.connect(":memory:")
    dates = _trading_days(_N_DATES)

    # daily_bar_pit
    conn.execute("""
        CREATE TABLE daily_bar_pit (
            security_id         VARCHAR,
            trade_date          VARCHAR,
            open                DOUBLE,
            close               DOUBLE,
            turnover_value_cny  DOUBLE,
            adj_factor          DOUBLE,
            is_st               BOOLEAN
        )
    """)
    bar_rows = []
    for i, sec in enumerate(_SECURITIES):
        g = _GROWTH[i]
        base = 10.0 * (i + 1)
        for j, td in enumerate(dates):
            price = base * (g ** j)
            bar_rows.append((
                sec,
                td,
                price * 0.999,   # open
                price,           # close
                price * 1e4,     # turnover_value_cny
                1.0,             # adj_factor (=1 so close_adj == close)
                False,
            ))
    conn.executemany(
        "INSERT INTO daily_bar_pit VALUES (?,?,?,?,?,?,?)",
        bar_rows,
    )

    # raw_adj_factor
    conn.execute("""
        CREATE TABLE raw_adj_factor (
            ts_code     VARCHAR,
            trade_date  VARCHAR,
            adj_factor  DOUBLE
        )
    """)
    adj_rows = [
        (sec, td, 1.0)
        for sec in _SECURITIES
        for td in dates
    ]
    conn.executemany("INSERT INTO raw_adj_factor VALUES (?,?,?)", adj_rows)

    # daily_basic  (pe/pb — used via fallback path in evaluator)
    conn.execute("""
        CREATE TABLE daily_basic (
            ts_code     VARCHAR,
            trade_date  VARCHAR,
            pe          DOUBLE,
            pb          DOUBLE
        )
    """)
    basic_rows = []
    for i, sec in enumerate(_SECURITIES):
        for td in dates:
            basic_rows.append((sec, td, _PE[i], _PB[i]))
    conn.executemany("INSERT INTO daily_basic VALUES (?,?,?,?)", basic_rows)

    # industry_classification_pit (for cs_industry_demean fallback)
    conn.execute("""
        CREATE TABLE industry_classification_pit (
            security_id     VARCHAR,
            industry_schema VARCHAR,
            industry_code   VARCHAR,
            effective_at    VARCHAR,
            removed_at      VARCHAR
        )
    """)
    for i, sec in enumerate(_SECURITIES):
        conn.execute(
            "INSERT INTO industry_classification_pit VALUES (?,?,?,?,?)",
            [sec, "sw2021_l1", _INDUSTRY[i], "20100101", None],
        )

    return conn


@pytest.fixture(scope="module")
def conn() -> duckdb.DuckDBPyConnection:  # type: ignore[misc]
    return _build_synth_db()


def _make_ctx(conn: Any, start: str | None = None, end: str | None = None) -> EvaluationContext:
    dates = _trading_days(_N_DATES)
    return EvaluationContext(
        conn=conn,
        start_date=start or dates[0],
        end_date=end or dates[-1],
    )


# ---------------------------------------------------------------------------
# canonical.py tests
# ---------------------------------------------------------------------------


class TestCanonical:
    """canonical() must produce a deterministic, round-trippable string."""

    def test_leaf(self) -> None:
        assert canonical(Leaf("close_adj")) == "close_adj"

    def test_ts_op(self) -> None:
        assert canonical(TSOp("lag", Leaf("close_adj"), 20)) == "lag(close_adj, 20)"

    def test_cs_op(self) -> None:
        assert canonical(CSOp("cs_rank", Leaf("pe"))) == "cs_rank(pe)"

    def test_binary_arith(self) -> None:
        node = ArithOp("+", Leaf("close_adj"), Leaf("open"))
        assert canonical(node) == "+(close_adj, open)"

    def test_unary_log(self) -> None:
        assert canonical(ArithOp("log", Leaf("close_adj"))) == "log(close_adj)"

    def test_nested(self) -> None:
        inner = TSOp("rolling_mean", Leaf("close_adj"), 20)
        outer = CSOp("cs_rank", inner)
        assert canonical(outer) == "cs_rank(rolling_mean(close_adj, 20))"

    def test_round_trip_via_parser(self) -> None:
        """parse(canonical(ast)) == ast for each tested expression."""
        nodes = [
            Leaf("close_adj"),
            TSOp("lag", Leaf("close_adj"), 20),
            CSOp("cs_rank", Leaf("pe")),
            CSOp("cs_zscore", TSOp("delta", Leaf("close_adj"), 5)),
            ArithOp("+", Leaf("open"), Leaf("turnover_value_cny")),
            ArithOp("log", Leaf("close_adj")),
        ]
        for ast in nodes:
            s = canonical(ast)
            reparsed = parse(s)
            assert reparsed == ast, f"Round-trip failed for {s!r}"

    def test_deterministic(self) -> None:
        """Same AST → same string on repeated calls."""
        ast = CSOp("cs_demean", TSOp("rolling_std", Leaf("close_adj"), 10))
        s1 = canonical(ast)
        s2 = canonical(ast)
        assert s1 == s2

    def test_distinct_asts_distinct_strings(self) -> None:
        a1 = TSOp("lag", Leaf("close_adj"), 5)
        a2 = TSOp("lag", Leaf("close_adj"), 10)
        assert canonical(a1) != canonical(a2)


# ---------------------------------------------------------------------------
# evaluator.py — output schema tests
# ---------------------------------------------------------------------------


class TestEvaluatorOutputSchema:
    """evaluate() must return exactly the right columns."""

    def test_output_columns(self, conn: Any) -> None:
        ctx = _make_ctx(conn)
        df = evaluate(Leaf("close_adj"), ctx)
        assert list(df.columns) == ["trade_date", "security_id", "descriptor_value"]

    def test_output_dtypes(self, conn: Any) -> None:
        ctx = _make_ctx(conn)
        df = evaluate(Leaf("close_adj"), ctx)
        assert df["descriptor_value"].dtype == float

    def test_no_nan_descriptor_value(self, conn: Any) -> None:
        """NaN values must be dropped from the output."""
        ctx = _make_ctx(conn)
        df = evaluate(Leaf("close_adj"), ctx)
        assert not df["descriptor_value"].isna().any()

    def test_date_range_respected(self, conn: Any) -> None:
        dates = _trading_days(_N_DATES)
        ctx = _make_ctx(conn, start=dates[5], end=dates[14])
        df = evaluate(Leaf("close_adj"), ctx)
        assert df["trade_date"].min() >= dates[5]
        assert df["trade_date"].max() <= dates[14]


# ---------------------------------------------------------------------------
# Leaf field tests
# ---------------------------------------------------------------------------


class TestLeafFields:
    def test_close_adj_equals_close_when_adj_is_one(self, conn: Any) -> None:
        """adj_factor=1 everywhere, so close_adj == close (bar table)."""
        ctx = _make_ctx(conn)
        df = evaluate(Leaf("close_adj"), ctx)
        # Spot-check: S001.SH first date should be base price * 1.001^0 = 10.0
        row = df[(df["security_id"] == "S001.SH") & (df["trade_date"] == _trading_days(_N_DATES)[0])]
        assert len(row) == 1
        assert abs(row["descriptor_value"].iloc[0] - 10.0) < 1e-9

    def test_open_field_loaded(self, conn: Any) -> None:
        ctx = _make_ctx(conn)
        df = evaluate(Leaf("open"), ctx)
        assert len(df) > 0
        # open = close * 0.999 in our fixture
        close_df = evaluate(Leaf("close_adj"), ctx)
        merged = close_df.merge(df, on=["trade_date", "security_id"], suffixes=("_close", "_open"))
        ratio = merged["descriptor_value_open"] / merged["descriptor_value_close"]
        assert np.allclose(ratio, 0.999, atol=1e-9)

    def test_turnover_field_loaded(self, conn: Any) -> None:
        ctx = _make_ctx(conn)
        df = evaluate(Leaf("turnover_value_cny"), ctx)
        assert len(df) > 0
        assert (df["descriptor_value"] > 0).all()

    def test_pe_non_positive_treated_as_missing(self, conn: Any) -> None:
        """R2.11: _PE[0] = -5.0 must become NaN → S001.SH absent from output."""
        ctx = _make_ctx(conn)
        df = evaluate(Leaf("pe"), ctx)
        assert "S001.SH" not in df["security_id"].values, (
            "S001.SH has PE=-5 (non-positive) and should be absent from output"
        )
        # Positive PE securities must be present
        for sec in _SECURITIES[1:]:
            assert sec in df["security_id"].values

    def test_pb_non_positive_treated_as_missing(self, conn: Any) -> None:
        """R2.11: _PB[0] = 0.0 must become NaN → S001.SH absent from output."""
        ctx = _make_ctx(conn)
        df = evaluate(Leaf("pb"), ctx)
        assert "S001.SH" not in df["security_id"].values, (
            "S001.SH has PB=0 (non-positive) and should be absent from output"
        )


# ---------------------------------------------------------------------------
# TS operator tests
# ---------------------------------------------------------------------------


class TestTSOperators:
    def test_lag_shifts_by_n(self, conn: Any) -> None:
        """lag(x, N)[t] == x[t-N] per security."""
        dates = _trading_days(_N_DATES)
        ctx = _make_ctx(conn)
        n = 5
        orig = evaluate(Leaf("close_adj"), ctx)
        lagged = evaluate(TSOp("lag", Leaf("close_adj"), n), ctx)

        # For security S001.SH at date index N: lag value should equal close_adj at date 0
        sec = "S001.SH"
        base = orig[(orig["security_id"] == sec) & (orig["trade_date"] == dates[0])]["descriptor_value"].iloc[0]
        lag_val = lagged[(lagged["security_id"] == sec) & (lagged["trade_date"] == dates[n])]["descriptor_value"].iloc[0]
        assert abs(lag_val - base) < 1e-9

    def test_delta_is_x_minus_lag(self, conn: Any) -> None:
        dates = _trading_days(_N_DATES)
        ctx = _make_ctx(conn)
        n = 5
        delta_df = evaluate(TSOp("delta", Leaf("close_adj"), n), ctx)
        orig = evaluate(Leaf("close_adj"), ctx)

        sec = "S001.SH"
        t_idx = 10
        val_now = orig[(orig["security_id"] == sec) & (orig["trade_date"] == dates[t_idx])]["descriptor_value"].iloc[0]
        val_lag = orig[(orig["security_id"] == sec) & (orig["trade_date"] == dates[t_idx - n])]["descriptor_value"].iloc[0]
        expected = val_now - val_lag
        actual = delta_df[(delta_df["security_id"] == sec) & (delta_df["trade_date"] == dates[t_idx])]["descriptor_value"].iloc[0]
        assert abs(actual - expected) < 1e-9

    def test_rolling_mean_window(self, conn: Any) -> None:
        """rolling_mean(x, N) at date t == mean of x[t-N+1..t]."""
        ctx = _make_ctx(conn)
        n = 5
        roll_df = evaluate(TSOp("rolling_mean", Leaf("close_adj"), n), ctx)
        orig = evaluate(Leaf("close_adj"), ctx)
        dates = sorted(orig["trade_date"].unique())

        sec = "S001.SH"
        t_idx = 10
        orig_sub = orig[orig["security_id"] == sec].sort_values("trade_date")
        window_vals = orig_sub["descriptor_value"].iloc[t_idx - n + 1: t_idx + 1].values
        expected = np.mean(window_vals)
        actual = roll_df[(roll_df["security_id"] == sec) & (roll_df["trade_date"] == dates[t_idx])]["descriptor_value"].iloc[0]
        assert abs(actual - expected) < 1e-9

    def test_rolling_std_gt_zero(self, conn: Any) -> None:
        ctx = _make_ctx(conn)
        df = evaluate(TSOp("rolling_std", Leaf("close_adj"), 10), ctx)
        assert (df["descriptor_value"] > 0).all()

    def test_rolling_max_ge_rolling_min(self, conn: Any) -> None:
        ctx = _make_ctx(conn)
        max_df = evaluate(TSOp("rolling_max", Leaf("close_adj"), 10), ctx)
        min_df = evaluate(TSOp("rolling_min", Leaf("close_adj"), 10), ctx)
        merged = max_df.merge(min_df, on=["trade_date", "security_id"], suffixes=("_max", "_min"))
        assert (merged["descriptor_value_max"] >= merged["descriptor_value_min"]).all()

    def test_lag_requires_full_window(self, conn: Any) -> None:
        """lag(x, N) must have no value in first N rows per security."""
        dates = _trading_days(_N_DATES)
        ctx = _make_ctx(conn)
        n = 5
        lagged = evaluate(TSOp("lag", Leaf("close_adj"), n), ctx)
        early_dates = set(dates[:n])
        # None of the first N dates should appear in results (they're NaN, dropped)
        rows_in_early = lagged[lagged["trade_date"].isin(early_dates)]
        assert len(rows_in_early) == 0, (
            f"lag({n}) should have no values for first {n} dates; got {len(rows_in_early)} rows"
        )


# ---------------------------------------------------------------------------
# CS operator tests
# ---------------------------------------------------------------------------


class TestCSOperators:
    def test_cs_rank_range(self, conn: Any) -> None:
        """cs_rank values must be in (0, 1]."""
        ctx = _make_ctx(conn)
        df = evaluate(CSOp("cs_rank", Leaf("close_adj")), ctx)
        assert (df["descriptor_value"] > 0).all()
        assert (df["descriptor_value"] <= 1.0).all()

    def test_cs_rank_per_date_sum(self, conn: Any) -> None:
        """Percentile ranks sum to ~(N+1)/2 per date when all N securities present."""
        ctx = _make_ctx(conn)
        df = evaluate(CSOp("cs_rank", Leaf("close_adj")), ctx)
        # With 5 securities: expected per-date mean of ranks ≈ 3/5 = 0.6
        per_date_means = df.groupby("trade_date")["descriptor_value"].mean()
        expected_mean = (sum(range(1, 6)) / 5) / 5  # 15/25 = 0.6
        assert np.allclose(per_date_means, expected_mean, atol=0.01)

    def test_cs_zscore_mean_near_zero(self, conn: Any) -> None:
        """cs_zscore: cross-sectional mean should be ~0 per date."""
        ctx = _make_ctx(conn)
        df = evaluate(CSOp("cs_zscore", Leaf("close_adj")), ctx)
        per_date_means = df.groupby("trade_date")["descriptor_value"].mean()
        assert np.allclose(per_date_means, 0.0, atol=1e-9)

    def test_cs_demean_mean_near_zero(self, conn: Any) -> None:
        """cs_demean: cross-sectional mean should be exactly 0 per date."""
        ctx = _make_ctx(conn)
        df = evaluate(CSOp("cs_demean", Leaf("close_adj")), ctx)
        per_date_means = df.groupby("trade_date")["descriptor_value"].mean()
        assert np.allclose(per_date_means, 0.0, atol=1e-9)

    def test_cs_industry_demean_group_means_near_zero(self, conn: Any) -> None:
        """cs_industry_demean: within-industry mean should be ~0."""
        ctx = _make_ctx(conn)
        df = evaluate(CSOp("cs_industry_demean", Leaf("close_adj")), ctx)
        # Map industry back
        industry_map = dict(zip(_SECURITIES, _INDUSTRY))
        df["industry"] = df["security_id"].map(industry_map)
        group_means = df.groupby(["trade_date", "industry"])["descriptor_value"].mean()
        assert np.allclose(group_means.values, 0.0, atol=1e-9), (
            f"Industry group means not near zero: {group_means.describe()}"
        )


# ---------------------------------------------------------------------------
# Arithmetic operator tests
# ---------------------------------------------------------------------------


class TestArithOperators:
    def test_add(self, conn: Any) -> None:
        ctx = _make_ctx(conn)
        df_sum = evaluate(ArithOp("+", Leaf("close_adj"), Leaf("open")), ctx)
        df_close = evaluate(Leaf("close_adj"), ctx)
        df_open = evaluate(Leaf("open"), ctx)
        merged = df_close.merge(df_open, on=["trade_date", "security_id"], suffixes=("_c", "_o"))
        merged = merged.merge(df_sum, on=["trade_date", "security_id"])
        expected = merged["descriptor_value_c"] + merged["descriptor_value_o"]
        assert np.allclose(merged["descriptor_value"], expected, atol=1e-9)

    def test_subtract(self, conn: Any) -> None:
        ctx = _make_ctx(conn)
        df_diff = evaluate(ArithOp("-", Leaf("close_adj"), Leaf("open")), ctx)
        # close_adj - open = close * 1.0 - close * 0.999 = close * 0.001
        assert (df_diff["descriptor_value"] > 0).all()

    def test_multiply(self, conn: Any) -> None:
        ctx = _make_ctx(conn)
        df_prod = evaluate(ArithOp("*", Leaf("close_adj"), Leaf("close_adj")), ctx)
        df_close = evaluate(Leaf("close_adj"), ctx)
        merged = df_close.merge(df_prod, on=["trade_date", "security_id"], suffixes=("_c", "_sq"))
        expected = merged["descriptor_value_c"] ** 2
        assert np.allclose(merged["descriptor_value_sq"], expected, atol=1e-9)

    def test_divide(self, conn: Any) -> None:
        ctx = _make_ctx(conn)
        df_ratio = evaluate(ArithOp("/", Leaf("close_adj"), Leaf("open")), ctx)
        # close_adj / open = close / (close * 0.999) ≈ 1.001001…
        assert np.allclose(df_ratio["descriptor_value"], 1.0 / 0.999, atol=1e-9)

    def test_log_positive_output(self, conn: Any) -> None:
        ctx = _make_ctx(conn)
        df = evaluate(ArithOp("log", Leaf("close_adj")), ctx)
        # All prices > 0 → log should produce real finite values
        assert np.all(np.isfinite(df["descriptor_value"]))

    def test_log_non_positive_becomes_nan(self, conn: Any) -> None:
        """log(pe) for S001.SH (pe=-5) must not appear in output."""
        ctx = _make_ctx(conn)
        df = evaluate(ArithOp("log", Leaf("pe")), ctx)
        # S001.SH has pe=-5 which becomes NaN after R2.11 treatment; log(NaN)=NaN → dropped
        assert "S001.SH" not in df["security_id"].values


# ---------------------------------------------------------------------------
# Cache hit test (R11.3)
# ---------------------------------------------------------------------------


class TestCacheHit:
    def test_same_canonical_returns_identical_frame(self, conn: Any) -> None:
        """Evaluating the same AST twice on the same ctx returns identical DataFrames
        and hits the leaf cache (no re-query to DuckDB)."""
        ctx = _make_ctx(conn)
        ast = TSOp("rolling_mean", Leaf("close_adj"), 20)

        df1 = evaluate(ast, ctx)
        # Leaf should now be cached
        assert "close_adj" in ctx._leaf_cache

        df2 = evaluate(ast, ctx)
        # Results should be byte-identical
        pd.testing.assert_frame_equal(df1, df2)

    def test_leaf_cache_populated_after_eval(self, conn: Any) -> None:
        ctx = _make_ctx(conn)
        assert len(ctx._leaf_cache) == 0
        evaluate(Leaf("open"), ctx)
        assert "open" in ctx._leaf_cache

    def test_different_canonical_strings_different_results(self, conn: Any) -> None:
        ctx = _make_ctx(conn)
        ast1 = TSOp("lag", Leaf("close_adj"), 5)
        ast2 = TSOp("lag", Leaf("close_adj"), 10)
        assert canonical(ast1) != canonical(ast2)
        df1 = evaluate(ast1, ctx)
        df2 = evaluate(ast2, ctx)
        # They should differ (different lag window → different values)
        merged = df1.merge(df2, on=["trade_date", "security_id"], suffixes=("_5", "_10"))
        assert not np.allclose(merged["descriptor_value_5"], merged["descriptor_value_10"])


# ---------------------------------------------------------------------------
# Composite expression tests
# ---------------------------------------------------------------------------


class TestCompositeExpressions:
    def test_cs_rank_of_lag(self, conn: Any) -> None:
        """cs_rank(lag(close_adj, 5)) should produce valid percentile ranks."""
        ctx = _make_ctx(conn)
        ast = CSOp("cs_rank", TSOp("lag", Leaf("close_adj"), 5))
        df = evaluate(ast, ctx)
        assert len(df) > 0
        assert (df["descriptor_value"] > 0).all()
        assert (df["descriptor_value"] <= 1.0).all()

    def test_delta_then_cs_zscore(self, conn: Any) -> None:
        """cs_zscore(delta(close_adj, 5)) should have ~zero per-date mean."""
        ctx = _make_ctx(conn)
        ast = CSOp("cs_zscore", TSOp("delta", Leaf("close_adj"), 5))
        df = evaluate(ast, ctx)
        per_date_means = df.groupby("trade_date")["descriptor_value"].mean()
        assert np.allclose(per_date_means, 0.0, atol=1e-9)

    def test_arith_combines_ts_and_leaf(self, conn: Any) -> None:
        """+(lag(close_adj,5), open) produces valid finite values."""
        ctx = _make_ctx(conn)
        ast = ArithOp("+", TSOp("lag", Leaf("close_adj"), 5), Leaf("open"))
        df = evaluate(ast, ctx)
        assert len(df) > 0
        assert np.all(np.isfinite(df["descriptor_value"]))
