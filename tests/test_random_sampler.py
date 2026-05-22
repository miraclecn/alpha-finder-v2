"""Unit tests for factor_lab.search.random_sampler.

Verification target: Task 10 (R3.3, R3.4, R3.8)

Uses the same synthetic 5-securities × 60-dates in-memory DuckDB fixture
as test_beam_search.py.

Covers:
- Returns at most random_sample_size candidates (may be fewer due to
  within-run dedup of duplicate canonical strings).
- All returned candidates have sources=["random"] (R3.4).
- Same seed → identical canonical string set (R3.8).
- All candidates have required fields: expr_id, canonical, ast, node_count,
  family, sources, status, oos_segments.
- node_count ≤ max_depth for all candidates.
- Coverage spans multiple depths and operator families at 1000 draws.
"""

from __future__ import annotations

from datetime import date, timedelta

import duckdb
import numpy as np
import pytest

from alpha_find_v2.factor_lab.config import FitnessConfig, SearchConfig
from alpha_find_v2.factor_lab.dsl.canonical import canonical
from alpha_find_v2.factor_lab.dsl.evaluator import EvaluationContext
from alpha_find_v2.factor_lab.dsl.grammar import MAX_DEPTH, node_count
from alpha_find_v2.factor_lab.search.beam import Candidate
from alpha_find_v2.factor_lab.search.random_sampler import run_random_sampling


# ---------------------------------------------------------------------------
# Synthetic fixture (5 securities × 60 trading dates) — same as beam tests
# ---------------------------------------------------------------------------

_N_SECURITIES = 5
_N_DATES = 60

_SECURITIES = [f"S{i:03d}.SH" for i in range(1, _N_SECURITIES + 1)]
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
    conn = duckdb.connect(":memory:")
    dates = _trading_days(_N_DATES)

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

    conn.execute("""
        CREATE TABLE raw_adj_factor (
            ts_code    VARCHAR,
            trade_date VARCHAR,
            adj_factor DOUBLE
        )
    """)
    adj_rows = [(sec, td, 1.0) for sec in _SECURITIES for td in dates]
    conn.executemany("INSERT INTO raw_adj_factor VALUES (?,?,?)", adj_rows)

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


def _make_ctx(conn: duckdb.DuckDBPyConnection) -> EvaluationContext:
    dates = _trading_days(_N_DATES)
    return EvaluationContext(conn=conn, start_date=dates[0], end_date=dates[-1])


def _search_config(sample_size: int = 50, max_depth: int = MAX_DEPTH, seed: int = 42) -> SearchConfig:
    return SearchConfig(
        beam_width=20,
        max_depth=max_depth,
        random_sample_size=sample_size,
        seed=seed,
    )


def _fitness_config() -> FitnessConfig:
    return FitnessConfig(complexity_lambda=0.05)


# ---------------------------------------------------------------------------
# Count and type
# ---------------------------------------------------------------------------


class TestReturnCount:
    """Result length respects random_sample_size (may be shorter due to dedup)."""

    def test_at_most_sample_size(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _search_config(sample_size=30)
        result = run_random_sampling(config, _fitness_config(), ctx)
        assert len(result) <= 30

    def test_returns_list_of_candidates(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _search_config(sample_size=10)
        result = run_random_sampling(config, _fitness_config(), ctx)
        assert isinstance(result, list)
        for c in result:
            assert isinstance(c, Candidate)

    def test_zero_sample_size_returns_empty(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _search_config(sample_size=0)
        result = run_random_sampling(config, _fitness_config(), ctx)
        assert result == []


# ---------------------------------------------------------------------------
# Sources field (R3.4)
# ---------------------------------------------------------------------------


class TestSources:
    """Every returned candidate has sources=["random"]."""

    def test_all_sources_contain_random(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _search_config(sample_size=50)
        result = run_random_sampling(config, _fitness_config(), ctx)
        assert result, "Expected at least one candidate"
        for c in result:
            assert c.sources == ["random"], f"Expected sources=['random'], got {c.sources}"

    def test_sources_does_not_contain_beam(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _search_config(sample_size=50)
        result = run_random_sampling(config, _fitness_config(), ctx)
        for c in result:
            assert "beam" not in c.sources


# ---------------------------------------------------------------------------
# Candidate field validity
# ---------------------------------------------------------------------------


class TestCandidateFields:
    """All returned candidates have valid required fields."""

    def test_all_fields_present(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _search_config(sample_size=20)
        result = run_random_sampling(config, _fitness_config(), ctx)
        assert result, "Expected at least one candidate"
        for c in result:
            assert isinstance(c.expr_id, str) and c.expr_id
            assert isinstance(c.canonical, str) and c.canonical
            assert c.ast is not None
            assert isinstance(c.node_count, int) and c.node_count >= 1
            assert isinstance(c.sources, list)
            assert c.status is not None
            assert isinstance(c.oos_segments, list)

    def test_canonical_matches_ast(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _search_config(sample_size=30)
        result = run_random_sampling(config, _fitness_config(), ctx)
        for c in result:
            assert canonical(c.ast) == c.canonical

    def test_node_count_matches_ast(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx = _make_ctx(synth_conn)
        config = _search_config(sample_size=30)
        result = run_random_sampling(config, _fitness_config(), ctx)
        for c in result:
            assert node_count(c.ast) == c.node_count

    def test_node_count_within_max_depth(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        """node_count ≤ max_depth for every candidate (R3.3 / R2.6)."""
        max_depth = 4
        ctx = _make_ctx(synth_conn)
        config = _search_config(sample_size=100, max_depth=max_depth)
        result = run_random_sampling(config, _fitness_config(), ctx)
        for c in result:
            assert c.node_count <= max_depth, (
                f"{c.canonical} has node_count={c.node_count} > max_depth={max_depth}"
            )

    def test_no_duplicate_canonicals(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        """Within-run dedup: each canonical appears at most once."""
        ctx = _make_ctx(synth_conn)
        config = _search_config(sample_size=100)
        result = run_random_sampling(config, _fitness_config(), ctx)
        canonicals = [c.canonical for c in result]
        assert len(canonicals) == len(set(canonicals))


# ---------------------------------------------------------------------------
# Determinism (R3.8)
# ---------------------------------------------------------------------------


class TestDeterminism:
    """Same seed → same canonical string set."""

    def test_same_seed_same_canonicals(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        ctx1 = _make_ctx(synth_conn)
        ctx2 = _make_ctx(synth_conn)
        config = _search_config(sample_size=50, seed=99)

        r1 = run_random_sampling(config, _fitness_config(), ctx1)
        r2 = run_random_sampling(config, _fitness_config(), ctx2)

        assert sorted(c.canonical for c in r1) == sorted(c.canonical for c in r2)

    def test_different_seeds_produce_different_sets(
        self, synth_conn: duckdb.DuckDBPyConnection
    ) -> None:
        ctx1 = _make_ctx(synth_conn)
        ctx2 = _make_ctx(synth_conn)
        config1 = _search_config(sample_size=50, seed=1)
        config2 = _search_config(sample_size=50, seed=2)

        r1 = run_random_sampling(config1, _fitness_config(), ctx1)
        r2 = run_random_sampling(config2, _fitness_config(), ctx2)

        # With 50 draws and a large grammar, different seeds should differ.
        set1 = set(c.canonical for c in r1)
        set2 = set(c.canonical for c in r2)
        assert set1 != set2, "Different seeds produced identical canonical sets"

    def test_injected_rng_used(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        """Passing an explicit RNG produces reproducible results."""
        ctx1 = _make_ctx(synth_conn)
        ctx2 = _make_ctx(synth_conn)
        config = _search_config(sample_size=30)

        rng1 = np.random.default_rng(77)
        rng2 = np.random.default_rng(77)

        r1 = run_random_sampling(config, _fitness_config(), ctx1, rng=rng1)
        r2 = run_random_sampling(config, _fitness_config(), ctx2, rng=rng2)

        assert sorted(c.canonical for c in r1) == sorted(c.canonical for c in r2)


# ---------------------------------------------------------------------------
# Coverage at 1000 draws (R3.3)
# ---------------------------------------------------------------------------


class TestCoverageAt1000:
    """1000 draws span all depths and multiple grammar families."""

    def test_coverage_spans_all_depths(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        """With 1000 draws at max_depth=5, every depth 1..5 should appear."""
        ctx = _make_ctx(synth_conn)
        config = _search_config(sample_size=1000, max_depth=5)
        result = run_random_sampling(config, _fitness_config(), ctx)

        observed_depths = {c.node_count for c in result}
        # At 1000 draws, we expect all depths 1..5 to appear with very high probability.
        for d in range(1, 6):
            assert d in observed_depths, (
                f"Depth {d} not represented in 1000 draws. Observed: {sorted(observed_depths)}"
            )

    def test_coverage_spans_multiple_families(self, synth_conn: duckdb.DuckDBPyConnection) -> None:
        """With 1000 draws, more than one family type should appear."""
        ctx = _make_ctx(synth_conn)
        config = _search_config(sample_size=1000, max_depth=5)
        result = run_random_sampling(config, _fitness_config(), ctx)

        # Collect non-None families
        families = {c.family for c in result if c.family is not None}
        assert len(families) > 1, (
            f"Expected multiple families from 1000 draws, got: {families}"
        )
