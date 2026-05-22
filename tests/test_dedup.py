"""Unit tests for factor_lab.dedup.run_correlation_dedup.

Verification target: Task 14 (R6.4, R6.5, R6.6, R6.7, R6.8, R6.9)

Uses a synthetic in-memory DuckDB (same pattern as test_beam_search.py).

Covers:
1. Two correlated candidates → lower-fitness rejected with correct reference id.
2. One candidate vs a registered descriptor with high correlation → rejected.
3. Insufficient overlap (< min_obs) → undefined cell, candidate not rejected.
4. Full matrix contains every evaluated candidate (including rejected).
5. Rejected candidate carries correct reference id and rounded r value.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, timedelta
from unittest.mock import patch

import duckdb
import numpy as np
import pandas as pd
import pytest

from alpha_find_v2.factor_evaluation.descriptor_compute import (
    ComputeContext,
    DescriptorComputeSpec,
    register,
)
from alpha_find_v2.factor_lab.dedup import DedupResult, run_correlation_dedup
from alpha_find_v2.factor_lab.dsl.evaluator import EvaluationContext
from alpha_find_v2.factor_lab.dsl.grammar import CSOp, Leaf, TSOp
from alpha_find_v2.factor_lab.search.beam import Candidate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EXPR_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")
_N_DATES = 80
_N_SEC = 5
_SECURITIES = [f"S{i:03d}.SH" for i in range(1, _N_SEC + 1)]


def _trading_days(n: int, start: date = date(2022, 1, 3)) -> list[str]:
    days: list[str] = []
    d = start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return days


def _build_synth_db() -> duckdb.DuckDBPyConnection:
    """Minimal in-memory DuckDB for dedup tests."""
    conn = duckdb.connect(":memory:")
    dates = _trading_days(_N_DATES)

    conn.execute("""
        CREATE TABLE daily_bar_pit (
            security_id        VARCHAR,
            trade_date         VARCHAR,
            open               DOUBLE,
            close              DOUBLE,
            turnover_value_cny DOUBLE,
            adj_factor         DOUBLE,
            close_adj          DOUBLE,
            is_st              BOOLEAN
        )
    """)
    rows = []
    for i, sec in enumerate(_SECURITIES):
        base = 10.0 * (i + 1)
        growth = 1.001 + i * 0.0003
        for j, td in enumerate(dates):
            p = base * (growth ** j)
            rows.append((sec, td, p * 0.999, p, p * 1e4, 1.0, p, False))
    conn.executemany(
        "INSERT INTO daily_bar_pit VALUES (?,?,?,?,?,?,?,?)", rows
    )

    conn.execute("""
        CREATE TABLE raw_adj_factor (
            ts_code    VARCHAR,
            trade_date VARCHAR,
            adj_factor DOUBLE
        )
    """)
    conn.executemany(
        "INSERT INTO raw_adj_factor VALUES (?,?,?)",
        [(sec, td, 1.0) for sec in _SECURITIES for td in _trading_days(_N_DATES)],
    )

    return conn


@pytest.fixture(scope="module")
def synth_conn() -> duckdb.DuckDBPyConnection:
    return _build_synth_db()


def _make_ctx(conn: duckdb.DuckDBPyConnection) -> EvaluationContext:
    dates = _trading_days(_N_DATES)
    return EvaluationContext(conn=conn, start_date=dates[0], end_date=dates[-1])


def _make_candidate(
    canonical_str: str,
    ast,
    fitness: float,
    node_count: int = 1,
) -> Candidate:
    cid = uuid.uuid5(_EXPR_NAMESPACE, canonical_str).hex
    return Candidate(
        expr_id=cid,
        canonical=canonical_str,
        ast=ast,
        node_count=node_count,
        family="trend",
        sources=["beam"],
        train_ic_ir=fitness,
        fitness=fitness,
        status="accepted_oos",
        oos_segments=[],
    )


# ---------------------------------------------------------------------------
# Test 1: Two correlated candidates → lower-fitness rejected
# ---------------------------------------------------------------------------


class TestCorrelatedCandidates:
    """Two candidates where one is a trivial transform → |r|=1 → lower-fitness rejected."""

    def test_lower_fitness_rejected(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)

        # close_adj leaf: series A
        ast_a = Leaf(field="close_adj")
        # cs_demean(close_adj): this is a linear transform, highly correlated with close_adj
        # Use rolling_mean(close_adj,5) instead for near-perfect correlation
        ast_b = TSOp(op="rolling_mean", operand=Leaf(field="close_adj"), window=5)

        # Candidate A has higher fitness → survives
        cand_a = _make_candidate("close_adj", ast_a, fitness=0.8, node_count=1)
        # Candidate B has lower fitness → should be rejected if |r| > rho
        cand_b = _make_candidate("rolling_mean(close_adj,5)", ast_b, fitness=0.3, node_count=2)

        result = run_correlation_dedup(
            candidates=[cand_a, cand_b],
            registered_descriptor_ids=[],
            ctx=ctx,
            dedup_rho=0.85,
            dedup_min_obs=5,
        )

        assert isinstance(result, DedupResult)
        assert cand_a in result.admitted, "Higher-fitness candidate should be admitted"
        assert cand_b in result.rejected_correlation, "Lower-fitness candidate should be rejected"
        assert cand_b.corr_ref_id == cand_a.canonical
        assert cand_b.status == "rejected_correlation"

    def test_rejected_candidate_has_rounded_r(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        ast_a = Leaf(field="close_adj")
        ast_b = TSOp(op="rolling_mean", operand=Leaf(field="close_adj"), window=5)
        cand_a = _make_candidate("close_adj", ast_a, fitness=0.8, node_count=1)
        cand_b = _make_candidate("rolling_mean(close_adj,5)", ast_b, fitness=0.3, node_count=2)

        result = run_correlation_dedup(
            candidates=[cand_a, cand_b],
            registered_descriptor_ids=[],
            ctx=ctx,
            dedup_rho=0.85,
            dedup_min_obs=5,
        )

        if cand_b in result.rejected_correlation:
            assert cand_b.corr_r is not None
            # Rounded to 6 decimal places
            assert cand_b.corr_r == round(cand_b.corr_r, 6)


# ---------------------------------------------------------------------------
# Test 2: Candidate vs registered descriptor with high correlation
# ---------------------------------------------------------------------------


class TestVsRegisteredDescriptor:
    """Candidate highly correlated to a registered descriptor → rejected with descriptor id."""

    def test_rejected_with_descriptor_ref_id(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)

        # Register a mock descriptor that returns close_adj score series
        desc_id = "__test_dedup_close_adj__"
        ast_leaf = Leaf(field="close_adj")

        def _fake_fn(compute_ctx: ComputeContext) -> pd.DataFrame:
            from alpha_find_v2.factor_lab.dsl.evaluator import evaluate
            eval_ctx = EvaluationContext(
                conn=compute_ctx.conn,
                start_date=compute_ctx.start_date,
                end_date=compute_ctx.end_date,
            )
            return evaluate(ast_leaf, eval_ctx)

        spec = DescriptorComputeSpec(
            descriptor_id=desc_id,
            fn=_fake_fn,
            requires=("daily_bar_pit",),
        )
        register(spec)

        # Candidate uses rolling_mean of close_adj → highly correlated with close_adj
        ast_cand = TSOp(op="rolling_mean", operand=Leaf(field="close_adj"), window=5)
        cand = _make_candidate("rolling_mean(close_adj,5)_desc_test", ast_cand, fitness=0.5)

        result = run_correlation_dedup(
            candidates=[cand],
            registered_descriptor_ids=[desc_id],
            ctx=ctx,
            dedup_rho=0.85,
            dedup_min_obs=5,
        )

        assert cand in result.rejected_correlation, (
            "Candidate highly correlated with registered descriptor should be rejected"
        )
        assert cand.corr_ref_id == desc_id

    def test_low_corr_descriptor_not_rejected(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)

        desc_id = "__test_dedup_uncorrelated__"

        def _constant_fn(compute_ctx: ComputeContext) -> pd.DataFrame:
            """Returns a score series that is orthogonal to any monotone signal."""
            dates = _trading_days(_N_DATES)
            rows = []
            rng = np.random.default_rng(42)
            for td in dates:
                for sec in _SECURITIES:
                    # Random noise — low correlation with any monotone signal
                    rows.append({"trade_date": td, "security_id": sec,
                                 "descriptor_value": float(rng.standard_normal())})
            return pd.DataFrame(rows)

        spec = DescriptorComputeSpec(
            descriptor_id=desc_id,
            fn=_constant_fn,
            requires=(),
        )
        register(spec)

        ast_cand = Leaf(field="close_adj")
        cand = _make_candidate("close_adj_uncorr_test", ast_cand, fitness=0.5)

        result = run_correlation_dedup(
            candidates=[cand],
            registered_descriptor_ids=[desc_id],
            ctx=ctx,
            dedup_rho=0.85,
            dedup_min_obs=5,
        )

        # With random noise vs monotone, |r| should be well below 0.85
        assert cand in result.admitted


# ---------------------------------------------------------------------------
# Test 3: Insufficient overlap → undefined cell, no rejection
# ---------------------------------------------------------------------------


class TestInsufficientOverlap:
    """When overlap < min_obs, cell is None and candidate is not rejected."""

    def test_undefined_cell_no_rejection(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)

        desc_id = "__test_dedup_sparse__"

        def _sparse_fn(compute_ctx: ComputeContext) -> pd.DataFrame:
            """Returns only 2 rows — far fewer than min_obs=60."""
            dates = _trading_days(_N_DATES)
            return pd.DataFrame([
                {"trade_date": dates[0], "security_id": _SECURITIES[0], "descriptor_value": 1.0},
                {"trade_date": dates[1], "security_id": _SECURITIES[0], "descriptor_value": 2.0},
            ])

        spec = DescriptorComputeSpec(
            descriptor_id=desc_id,
            fn=_sparse_fn,
            requires=(),
        )
        register(spec)

        ast_cand = Leaf(field="close_adj")
        cand = _make_candidate("close_adj_sparse_test", ast_cand, fitness=0.5)

        result = run_correlation_dedup(
            candidates=[cand],
            registered_descriptor_ids=[desc_id],
            ctx=ctx,
            dedup_rho=0.85,
            dedup_min_obs=60,
        )

        assert cand in result.admitted, "Candidate should not be rejected on undefined pair"
        assert result.matrix[cand.canonical][desc_id] is None, (
            "Cell with insufficient overlap should be None"
        )


# ---------------------------------------------------------------------------
# Test 4 & 5: Matrix completeness and rejected candidate fields
# ---------------------------------------------------------------------------


class TestMatrixCompleteness:
    """Full matrix contains every evaluated candidate including rejected ones."""

    def test_matrix_contains_all_candidates(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        ast_a = Leaf(field="close_adj")
        ast_b = TSOp(op="rolling_mean", operand=Leaf(field="close_adj"), window=5)
        cand_a = _make_candidate("close_adj_matrix_a", ast_a, fitness=0.9)
        cand_b = _make_candidate("rolling_mean_close_adj_matrix_b", ast_b, fitness=0.1, node_count=2)

        result = run_correlation_dedup(
            candidates=[cand_a, cand_b],
            registered_descriptor_ids=[],
            ctx=ctx,
            dedup_rho=0.85,
            dedup_min_obs=5,
        )

        # Both candidates must appear in matrix regardless of admission/rejection
        assert cand_a.canonical in result.matrix
        assert cand_b.canonical in result.matrix

    def test_rejected_candidate_in_matrix(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        """Rejected candidates must still appear as rows in the matrix (R6.8)."""
        ctx = _make_ctx(synth_conn)
        ast_a = Leaf(field="close_adj")
        ast_b = TSOp(op="rolling_mean", operand=Leaf(field="close_adj"), window=5)
        cand_a = _make_candidate("close_adj_rejected_matrix", ast_a, fitness=0.9)
        cand_b = _make_candidate("rolling_mean_rejected_matrix", ast_b, fitness=0.1, node_count=2)

        result = run_correlation_dedup(
            candidates=[cand_a, cand_b],
            registered_descriptor_ids=[],
            ctx=ctx,
            dedup_rho=0.85,
            dedup_min_obs=5,
        )

        rejected_canonicals = {c.canonical for c in result.rejected_correlation}
        for canon in rejected_canonicals:
            assert canon in result.matrix, (
                f"Rejected candidate {canon!r} must appear in correlation matrix"
            )

    def test_matrix_cell_is_float_or_none(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        """All matrix cells are float or None (never NaN, never string)."""
        ctx = _make_ctx(synth_conn)
        ast_a = Leaf(field="close_adj")
        ast_b = TSOp(op="rolling_mean", operand=Leaf(field="close_adj"), window=5)
        cand_a = _make_candidate("close_adj_cell_type", ast_a, fitness=0.9)
        cand_b = _make_candidate("rolling_mean_cell_type", ast_b, fitness=0.1, node_count=2)

        result = run_correlation_dedup(
            candidates=[cand_a, cand_b],
            registered_descriptor_ids=[],
            ctx=ctx,
            dedup_rho=0.85,
            dedup_min_obs=5,
        )

        for row in result.matrix.values():
            for val in row.values():
                assert val is None or isinstance(val, float)


# ---------------------------------------------------------------------------
# Test: Processing order (fitness descending R6.7)
# ---------------------------------------------------------------------------


class TestProcessingOrder:
    """Higher-fitness candidate admitted first, lower-fitness rejected."""

    def test_higher_fitness_wins(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        ast_a = Leaf(field="close_adj")
        ast_b = TSOp(op="rolling_mean", operand=Leaf(field="close_adj"), window=5)

        # Pass candidates in reverse fitness order to ensure sort happens internally
        cand_low = _make_candidate("rolling_mean_order_low", ast_b, fitness=0.1, node_count=2)
        cand_high = _make_candidate("close_adj_order_high", ast_a, fitness=0.9, node_count=1)

        result = run_correlation_dedup(
            candidates=[cand_low, cand_high],  # low-fitness first in input
            registered_descriptor_ids=[],
            ctx=ctx,
            dedup_rho=0.85,
            dedup_min_obs=5,
        )

        assert cand_high in result.admitted
        assert cand_low in result.rejected_correlation
