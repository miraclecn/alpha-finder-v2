"""Focused dedup-stage scenario tests for factor_lab.

Higher-level verification of the three scenarios from Task 25:
  1. Two correlated candidates → lower-fitness rejected (status, corr_ref_id, corr_r)
  2. Candidate vs a registered descriptor → rejected with descriptor id
  3. Insufficient overlap → None cell, candidate NOT rejected on that pair alone

Matrix completeness invariant: all evaluated candidates appear in matrix rows.

Comprehensive unit coverage lives in tests/test_dedup.py; this file is
focused on the three specific pipeline scenarios described in tasks.md.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta

import duckdb
import numpy as np
import pandas as pd
import pytest

from alpha_find_v2.factor_evaluation.descriptor_compute import (
    REGISTRY,
    ComputeContext,
    DescriptorComputeSpec,
    register,
)
from alpha_find_v2.factor_lab.dedup import run_correlation_dedup
from alpha_find_v2.factor_lab.dsl.evaluator import EvaluationContext
from alpha_find_v2.factor_lab.dsl.grammar import Leaf, TSOp
from alpha_find_v2.factor_lab.search.beam import Candidate

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_EXPR_NS = uuid.UUID("12345678-1234-5678-1234-567812345678")
_N_DATES = 80
_SECURITIES = [f"S{i:03d}.SH" for i in range(1, 6)]


def _trading_days(n: int) -> list[str]:
    days, d = [], date(2022, 1, 3)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return days


@pytest.fixture(scope="module")
def conn() -> duckdb.DuckDBPyConnection:
    db = duckdb.connect(":memory:")
    dates = _trading_days(_N_DATES)
    db.execute("""
        CREATE TABLE daily_bar_pit (
            security_id VARCHAR, trade_date VARCHAR,
            open DOUBLE, close DOUBLE, turnover_value_cny DOUBLE,
            adj_factor DOUBLE, close_adj DOUBLE, is_st BOOLEAN
        )
    """)
    rows = []
    for i, sec in enumerate(_SECURITIES):
        base, growth = 10.0 * (i + 1), 1.001 + i * 0.0003
        for j, td in enumerate(dates):
            p = base * (growth ** j)
            rows.append((sec, td, p * 0.999, p, p * 1e4, 1.0, p, False))
    db.executemany("INSERT INTO daily_bar_pit VALUES (?,?,?,?,?,?,?,?)", rows)

    db.execute("""
        CREATE TABLE raw_adj_factor (
            ts_code VARCHAR, trade_date VARCHAR, adj_factor DOUBLE
        )
    """)
    db.executemany(
        "INSERT INTO raw_adj_factor VALUES (?,?,?)",
        [(sec, td, 1.0) for sec in _SECURITIES for td in dates],
    )
    return db


@pytest.fixture(scope="module")
def ctx(conn: duckdb.DuckDBPyConnection) -> EvaluationContext:
    dates = _trading_days(_N_DATES)
    return EvaluationContext(conn=conn, start_date=dates[0], end_date=dates[-1])


def _cand(canonical_str: str, ast, fitness: float, node_count: int = 1) -> Candidate:
    return Candidate(
        expr_id=uuid.uuid5(_EXPR_NS, canonical_str).hex,
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
# Scenario 1: trivial transform → |r|≈1 → lower-fitness rejected
# ---------------------------------------------------------------------------


def test_trivial_transform_lower_fitness_rejected(ctx: EvaluationContext) -> None:
    """close_adj vs rolling_mean(close_adj,5) are highly correlated; lower fitness rejected."""
    cand_a = _cand("close_adj_s1", Leaf(field="close_adj"), fitness=0.8)
    cand_b = _cand("rolling_mean_close_adj_5_s1", TSOp("rolling_mean", Leaf("close_adj"), 5),
                   fitness=0.3, node_count=2)

    result = run_correlation_dedup(
        candidates=[cand_a, cand_b],
        registered_descriptor_ids=[],
        ctx=ctx,
        dedup_rho=0.85,
        dedup_min_obs=5,
    )

    assert cand_a in result.admitted
    assert cand_b in result.rejected_correlation
    assert cand_b.status == "rejected_correlation"
    assert cand_b.corr_ref_id == cand_a.canonical
    assert cand_b.corr_r is not None
    assert cand_b.corr_r == round(cand_b.corr_r, 6)
    # Matrix completeness: both candidates appear
    assert cand_a.canonical in result.matrix
    assert cand_b.canonical in result.matrix


# ---------------------------------------------------------------------------
# Scenario 2: candidate vs registered descriptor → rejected with descriptor id
# ---------------------------------------------------------------------------


def test_rejected_references_registered_descriptor(ctx: EvaluationContext) -> None:
    """Candidate correlated with a registered mock descriptor → rejected with that descriptor id."""
    desc_id = "__test_dedup_stage_desc__"
    leaf_ast = Leaf(field="close_adj")

    def _mock_fn(compute_ctx: ComputeContext) -> pd.DataFrame:
        eval_ctx = EvaluationContext(
            conn=compute_ctx.conn,
            start_date=compute_ctx.start_date,
            end_date=compute_ctx.end_date,
        )
        from alpha_find_v2.factor_lab.dsl.evaluator import evaluate
        return evaluate(leaf_ast, eval_ctx)

    register(DescriptorComputeSpec(descriptor_id=desc_id, fn=_mock_fn, requires=("daily_bar_pit",)))
    try:
        cand = _cand("rolling_mean_close_adj_5_s2",
                     TSOp("rolling_mean", Leaf("close_adj"), 5),
                     fitness=0.5, node_count=2)

        result = run_correlation_dedup(
            candidates=[cand],
            registered_descriptor_ids=[desc_id],
            ctx=ctx,
            dedup_rho=0.85,
            dedup_min_obs=5,
        )

        assert cand in result.rejected_correlation
        assert cand.corr_ref_id == desc_id
        assert cand.canonical in result.matrix
    finally:
        REGISTRY.pop(desc_id, None)


# ---------------------------------------------------------------------------
# Scenario 3: insufficient overlap → None cell, candidate not rejected
# ---------------------------------------------------------------------------


def test_insufficient_overlap_cell_none_not_rejected(ctx: EvaluationContext) -> None:
    """Sparse descriptor with < min_obs rows → matrix cell is None; candidate admitted."""
    desc_id = "__test_dedup_stage_sparse__"
    dates = _trading_days(_N_DATES)

    def _sparse_fn(compute_ctx: ComputeContext) -> pd.DataFrame:
        return pd.DataFrame([
            {"trade_date": dates[0], "security_id": _SECURITIES[0], "descriptor_value": 1.0},
            {"trade_date": dates[1], "security_id": _SECURITIES[0], "descriptor_value": 2.0},
        ])

    register(DescriptorComputeSpec(descriptor_id=desc_id, fn=_sparse_fn, requires=()))
    try:
        cand = _cand("close_adj_s3", Leaf(field="close_adj"), fitness=0.5)

        result = run_correlation_dedup(
            candidates=[cand],
            registered_descriptor_ids=[desc_id],
            ctx=ctx,
            dedup_rho=0.85,
            dedup_min_obs=60,
        )

        assert cand in result.admitted, "Candidate must not be rejected on undefined pair alone"
        assert result.matrix[cand.canonical][desc_id] is None, "Insufficient-overlap cell must be None"
    finally:
        REGISTRY.pop(desc_id, None)
