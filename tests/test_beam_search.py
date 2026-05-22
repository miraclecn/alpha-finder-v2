"""Unit tests for factor_lab.search.beam — beam search engine.

Verification target: Task 9 (R3.1, R3.2, R3.8, R6.1, R6.2, R6.3)

Uses a minimal synthetic 5-securities × 60-dates in-memory DuckDB (same
fixture as test_dsl_canonical_evaluator.py) so tests run without real data.

Covers:
- Layer-1 beam retains at most beam_width leaves.
- 2-layer beam retains at most beam_width per layer.
- Candidates with NaN IC_IR are ineligible (R3.2).
- Fitness tie-breaking: ascending node_count, then lex canonical (R6.3).
- Determinism: two calls with the same config produce identical results (R3.8).
- Candidate dataclass fields: expr_id, canonical, ast, node_count, family,
  sources, train_ic_ir, fitness, status.
"""

from __future__ import annotations

import math
from datetime import date, timedelta

import duckdb
import pytest

from alpha_find_v2.factor_lab.config import FitnessConfig, SearchConfig
from alpha_find_v2.factor_lab.dsl.canonical import canonical
from alpha_find_v2.factor_lab.dsl.evaluator import EvaluationContext
from alpha_find_v2.factor_lab.dsl.grammar import node_count
from alpha_find_v2.factor_lab.search.beam import Candidate, run_beam_search


# ---------------------------------------------------------------------------
# Synthetic fixture (5 securities × 60 trading dates)
# ---------------------------------------------------------------------------

_N_SECURITIES = 5
_N_DATES = 60

_SECURITIES = [f"S{i:03d}.SH" for i in range(1, _N_SECURITIES + 1)]
# Distinct growth rates so cross-sectional IC is non-trivial.
_GROWTH = [1.001, 1.002, 1.003, 1.004, 1.005]


def _trading_days(n: int, start: date = date(2022, 1, 3)) -> list[str]:
    days: list[str] = []
    d = start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return days


def _build_synth_db() -> duckdb.DuckDBPyConnection:
    """Build a minimal in-memory DuckDB with the tables beam search needs."""
    conn = duckdb.connect(":memory:")
    dates = _trading_days(_N_DATES)

    # daily_bar_pit — prices monotonically increasing per security
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
    rows = []
    for i, sec in enumerate(_SECURITIES):
        g = _GROWTH[i]
        base = 10.0 * (i + 1)
        for j, td in enumerate(dates):
            price = base * (g ** j)
            rows.append((sec, td, price * 0.999, price, price * 1e4, 1.0, False))
    conn.executemany("INSERT INTO daily_bar_pit VALUES (?,?,?,?,?,?,?)", rows)

    # raw_adj_factor — all 1.0
    conn.execute("""
        CREATE TABLE raw_adj_factor (
            ts_code    VARCHAR,
            trade_date VARCHAR,
            adj_factor DOUBLE
        )
    """)
    adj_rows = [(sec, td, 1.0) for sec in _SECURITIES for td in dates]
    conn.executemany("INSERT INTO raw_adj_factor VALUES (?,?,?)", adj_rows)

    # raw_daily_basic — positive pe/pb so they evaluate to real values
    conn.execute("""
        CREATE TABLE raw_daily_basic (
            ts_code    VARCHAR,
            trade_date VARCHAR,
            pe         DOUBLE,
            pb         DOUBLE
        )
    """)
    basic_rows = [
        (sec, td, 10.0 + i, 1.0 + i * 0.5)
        for i, sec in enumerate(_SECURITIES)
        for td in dates
    ]
    conn.executemany("INSERT INTO raw_daily_basic VALUES (?,?,?,?)", basic_rows)

    return conn


@pytest.fixture(scope="module")
def synth_conn() -> duckdb.DuckDBPyConnection:
    return _build_synth_db()


def _make_ctx(conn: duckdb.DuckDBPyConnection, n_dates: int = _N_DATES) -> EvaluationContext:
    dates = _trading_days(n_dates)
    return EvaluationContext(conn=conn, start_date=dates[0], end_date=dates[-1])


def _default_search_config(**kwargs) -> SearchConfig:
    defaults = dict(beam_width=3, max_depth=2, random_sample_size=0, seed=42)
    defaults.update(kwargs)
    return SearchConfig(**defaults)


def _default_fitness_config() -> FitnessConfig:
    return FitnessConfig(complexity_lambda=0.05)


# ---------------------------------------------------------------------------
# Basic structural tests
# ---------------------------------------------------------------------------


class TestBeamSearchStructure:
    """run_beam_search returns a list of Candidate objects."""

    def test_returns_list(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=2, max_depth=1)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        assert isinstance(result, list)

    def test_all_elements_are_candidates(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=2, max_depth=1)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        for c in result:
            assert isinstance(c, Candidate)

    def test_candidate_fields_present(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=2, max_depth=1)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        assert result, "Expected at least one candidate"
        c = result[0]
        assert isinstance(c.expr_id, str) and len(c.expr_id) > 0
        assert isinstance(c.canonical, str) and len(c.canonical) > 0
        assert c.ast is not None
        assert isinstance(c.node_count, int) and c.node_count >= 1
        # family may be None for unclassifiable
        assert isinstance(c.sources, list) and "beam" in c.sources
        assert c.status is not None
        assert isinstance(c.oos_segments, list)

    def test_sources_contains_beam(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=3, max_depth=1)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        for c in result:
            assert "beam" in c.sources

    def test_canonical_matches_ast(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=3, max_depth=2)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        for c in result:
            assert canonical(c.ast) == c.canonical

    def test_node_count_matches_ast(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=3, max_depth=2)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        for c in result:
            assert node_count(c.ast) == c.node_count


# ---------------------------------------------------------------------------
# Beam width enforcement
# ---------------------------------------------------------------------------


class TestBeamWidthEnforcement:
    """At each layer, at most beam_width candidates are retained."""

    def test_layer1_at_most_beam_width(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        """Layer 1 has 5 leaves; beam_width=3 → at most 3 retained."""
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=3, max_depth=1)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        # All results come from layer 1 (max_depth=1)
        assert len(result) <= 3, f"Expected ≤3 candidates, got {len(result)}"

    def test_2layer_total_at_most_2x_beam_width(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        """2-layer beam: up to beam_width per layer → at most 2×beam_width total."""
        beam_width = 3
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=beam_width, max_depth=2)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        assert len(result) <= 2 * beam_width, (
            f"Expected ≤{2 * beam_width} candidates across 2 layers, got {len(result)}"
        )

    def test_exactly_beam_width_when_enough_eligible(
        self, synth_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Layer 1 has 5 eligible leaves, beam_width=3 → exactly 3 retained."""
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=3, max_depth=1)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        eligible_count = sum(1 for c in result if c.fitness is not None)
        # There should be exactly beam_width=3 eligible candidates
        assert eligible_count == 3

    def test_beam_width_1_retains_single_best(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        """beam_width=1 at layer 1 retains exactly 1 candidate."""
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=1, max_depth=1)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        assert len(result) == 1

    def test_2layer_each_layer_at_most_beam_width(
        self, synth_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """With max_depth=2, beam_width=2: layer 1 has ≤2, layer 2 has ≤2."""
        beam_width = 2
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=beam_width, max_depth=2)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        # Layer 1: node_count==1 (leaves). Layer 2: node_count>1.
        layer1 = [c for c in result if c.node_count == 1]
        layer2 = [c for c in result if c.node_count > 1]
        assert len(layer1) <= beam_width
        assert len(layer2) <= beam_width


# ---------------------------------------------------------------------------
# Ineligibility of NaN IC_IR (R3.2)
# ---------------------------------------------------------------------------


class TestIneligibilityRule:
    """Candidates with None fitness are excluded from retention."""

    def test_retained_candidates_have_finite_fitness(
        self, synth_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """All retained candidates must have non-None, finite fitness."""
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=5, max_depth=2)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        for c in result:
            assert c.fitness is not None, f"Retained candidate {c.canonical} has None fitness"
            assert math.isfinite(c.fitness), (
                f"Retained candidate {c.canonical} has non-finite fitness {c.fitness}"
            )

    def test_retained_candidates_have_finite_ic_ir(
        self, synth_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """All retained candidates must have non-None, finite train_ic_ir."""
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=5, max_depth=2)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        for c in result:
            assert c.train_ic_ir is not None
            assert math.isfinite(c.train_ic_ir)


# ---------------------------------------------------------------------------
# Tie-breaking order (R6.3)
# ---------------------------------------------------------------------------


class TestTieBreaking:
    """Retained layer is sorted by (-fitness, node_count, canonical)."""

    def test_retained_ordered_by_fitness_descending(
        self, synth_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Within a layer, retained candidates are ordered fitness desc."""
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=5, max_depth=1)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        # Layer 1 results (all node_count == 1)
        layer1 = [c for c in result if c.node_count == 1]
        fitnesses = [c.fitness for c in layer1]
        assert fitnesses == sorted(fitnesses, reverse=True), (
            f"Layer 1 not sorted by fitness desc: {fitnesses}"
        )


# ---------------------------------------------------------------------------
# Determinism (R3.8)
# ---------------------------------------------------------------------------


class TestDeterminism:
    """Same config + ctx → identical results across two calls."""

    def test_two_runs_produce_identical_canonicals(
        self, synth_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """Running beam search twice on the same config returns the same canonical strings."""
        ctx1 = _make_ctx(synth_conn)
        ctx2 = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=3, max_depth=2)
        fc = _default_fitness_config()

        result1 = run_beam_search(config, fc, ctx1)
        result2 = run_beam_search(config, fc, ctx2)

        canonicals1 = [c.canonical for c in result1]
        canonicals2 = [c.canonical for c in result2]
        assert canonicals1 == canonicals2

    def test_two_runs_produce_identical_fitness(
        self, synth_conn: duckdb.DuckDBPyConnection
    ) -> None:
        ctx1 = _make_ctx(synth_conn)
        ctx2 = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=3, max_depth=2)
        fc = _default_fitness_config()

        result1 = run_beam_search(config, fc, ctx1)
        result2 = run_beam_search(config, fc, ctx2)

        for c1, c2 in zip(result1, result2):
            if c1.fitness is None:
                assert c2.fitness is None
            else:
                assert abs(c1.fitness - c2.fitness) < 1e-12


# ---------------------------------------------------------------------------
# expr_id determinism
# ---------------------------------------------------------------------------


class TestExprId:
    """expr_id must be deterministic given the canonical string."""

    def test_expr_id_deterministic_for_same_canonical(
        self, synth_conn: duckdb.DuckDBPyConnection
    ) -> None:
        ctx1 = _make_ctx(synth_conn)
        ctx2 = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=3, max_depth=1)
        fc = _default_fitness_config()

        r1 = run_beam_search(config, fc, ctx1)
        r2 = run_beam_search(config, fc, ctx2)

        ids1 = {c.canonical: c.expr_id for c in r1}
        ids2 = {c.canonical: c.expr_id for c in r2}
        for can, eid in ids1.items():
            assert ids2[can] == eid, f"expr_id differs for {can!r}: {eid} vs {ids2[can]}"

    def test_distinct_canonicals_have_distinct_expr_ids(
        self, synth_conn: duckdb.DuckDBPyConnection
    ) -> None:
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=5, max_depth=2)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        ids = [c.expr_id for c in result]
        assert len(ids) == len(set(ids)), "Duplicate expr_ids found"


# ---------------------------------------------------------------------------
# No-duplicate canonicals in results
# ---------------------------------------------------------------------------


class TestNoDuplicates:
    """Each canonical string should appear at most once in results."""

    def test_no_duplicate_canonicals(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=5, max_depth=2)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        canonicals = [c.canonical for c in result]
        assert len(canonicals) == len(set(canonicals)), (
            f"Duplicate canonicals in results: {[c for c in canonicals if canonicals.count(c) > 1]}"
        )


# ---------------------------------------------------------------------------
# max_depth=1 only returns leaves
# ---------------------------------------------------------------------------


class TestMaxDepth1:
    """With max_depth=1, only depth-1 expressions (leaves) are produced."""

    def test_only_leaves_at_depth1(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _default_search_config(beam_width=5, max_depth=1)
        result = run_beam_search(config, _default_fitness_config(), ctx)
        for c in result:
            assert c.node_count == 1, f"Non-leaf in depth-1 run: {c.canonical}"
