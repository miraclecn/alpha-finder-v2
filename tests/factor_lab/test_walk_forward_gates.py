"""Walk-forward OOS gate tests for factor_lab.

Higher-level gate contract tests (R5.4, R5.5, R9.4):
  - 4-year fixture; 3-segment split produces exactly 3 segments
  - Candidate passing all segments -> status "accepted_oos"
  - Candidate failing a segment -> status "rejected_oos" with first_failing_segment
  - Precondition violation raises ValueError
  - Ad-hoc spec absent from descriptor registry after run

Uses 10 securities x ~1000 trading days (~4 years).
oos_window_months=2 is used for integration tests to keep run time manageable
(matches the pattern in tests/test_walk_forward.py).
"""
from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from alpha_find_v2.factor_evaluation.descriptor_compute import list_registered
from alpha_find_v2.factor_lab.config import WalkForwardConfig
from alpha_find_v2.factor_lab.dsl.grammar import ArithOp, Leaf, TSOp
from alpha_find_v2.factor_lab.walk_forward import run_walk_forward


# ---------------------------------------------------------------------------
# Minimal candidate stand-in
# ---------------------------------------------------------------------------


@dataclass
class _Candidate:
    ast: object
    canonical: str = ""
    expr_id: str = "test"
    node_count: int = 1
    family: str | None = "trend"
    sources: list = field(default_factory=lambda: ["beam"])
    train_ic_ir: float | None = None
    fitness: float | None = None
    status: str = "pending"
    oos_segments: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Synthetic DB -- 10 securities x ~1000 trading days (~4 years)
# ---------------------------------------------------------------------------

_N_SECURITIES = 10
_N_DATES = 1000   # ~4 trading years (252 days/year x 4 ~= 1008)


def _trading_days(n: int, start: date = date(2018, 1, 2)) -> list[str]:
    days: list[str] = []
    d = start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return days


def _build_synth_db(tmp_dir: Path) -> Path:
    db_path = tmp_dir / "research_gates.duckdb"
    dates = _trading_days(_N_DATES)
    securities = [f"{600000 + i:06d}.SH" for i in range(_N_SECURITIES)]

    conn = duckdb.connect(str(db_path))

    conn.execute("CREATE TABLE market_trade_calendar (trade_date VARCHAR PRIMARY KEY)")
    conn.executemany("INSERT INTO market_trade_calendar VALUES (?)", [(d,) for d in dates])

    conn.execute("""
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
    """)
    for sec in securities:
        sym = sec.split(".")[0]
        conn.execute(
            "INSERT INTO security_master_ref VALUES"
            " (?,?,?,'SH','main_board','SH','20100101',NULL,'N',TRUE,CURRENT_TIMESTAMP)",
            [sec, sym, f"Stock_{sym}"],
        )

    conn.execute("""
        CREATE TABLE daily_bar_pit (
            security_id VARCHAR,
            trade_date VARCHAR,
            exchange VARCHAR,
            board VARCHAR,
            is_st BOOLEAN,
            pre_close DOUBLE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            close_adj DOUBLE,
            turnover_value_cny DOUBLE,
            volume_shares DOUBLE,
            price_basis VARCHAR,
            open_adj DOUBLE,
            float_mcap_cny DOUBLE,
            free_float_shares DOUBLE,
            PRIMARY KEY (security_id, trade_date)
        )
    """)
    rows = []
    for i, sec in enumerate(securities):
        growth = 1.001 + i * 0.0002
        base = 10.0 * (i + 1)
        free_mn = 3000.0
        for j, td in enumerate(dates):
            p = base * (growth ** j)
            rows.append((
                sec, td, "SH", "main_board", False,
                p / growth,
                p * 0.999,
                p * 1.01,
                p * 0.99,
                p,
                p,
                p * free_mn * 1e4 * 0.002,
                free_mn * 1e4 * 0.002,
                "standard",
                p * 0.999,
                p * free_mn * 1e4,
                free_mn * 1e4,
            ))
    conn.executemany(
        "INSERT INTO daily_bar_pit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )

    conn.execute("""
        CREATE TABLE raw_adj_factor (
            ts_code VARCHAR,
            trade_date VARCHAR,
            adj_factor DOUBLE,
            source_table VARCHAR,
            ingested_at TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    adj_rows = [(sec, td, 1.0, "synth") for sec in securities for td in dates]
    conn.executemany(
        "INSERT INTO raw_adj_factor VALUES (?,?,?,?,CURRENT_TIMESTAMP)",
        adj_rows,
    )

    conn.execute("""
        CREATE TABLE benchmark_membership_pit (
            benchmark_id VARCHAR,
            security_id VARCHAR,
            effective_at VARCHAR,
            removed_at VARCHAR,
            PRIMARY KEY (benchmark_id, security_id, effective_at)
        )
    """)
    for sec in securities:
        conn.execute(
            "INSERT INTO benchmark_membership_pit VALUES ('CSI 800',?,'20100101',NULL)",
            [sec],
        )

    conn.close()
    return db_path


@pytest.fixture(scope="module")
def synth_db() -> Path:
    with tempfile.TemporaryDirectory() as tmp:
        yield _build_synth_db(Path(tmp))


def _dates():
    return _trading_days(_N_DATES)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_three_segment_split_produces_three_segments(synth_db):
    """R5.1: 3-segment config over a 4-year date range produces exactly 3 OOS segments."""
    ast = Leaf(field="close_adj")
    candidate = _Candidate(ast=ast)
    dates = _dates()

    cfg = WalkForwardConfig(
        segments=3,
        oos_window_months=2,
        min_train_months=1,
        oos_ic_ir_threshold=0.0,
        primary_horizon_days=5,
    )
    result = run_walk_forward(
        candidate=candidate,
        walk_fwd_config=cfg,
        research_db=synth_db,
        start_date=dates[0],
        end_date=dates[-1],
        universe_id="csi800",
    )
    assert len(result.oos_segments) == 3


def test_candidate_passing_threshold_accepted(synth_db):
    """R5.4: candidate with positive IC in all segments -> accepted_oos.

    lag(close_adj, 5) on a monotonically rising synthetic DB produces positive
    rank correlations with future returns in every segment.
    """
    ast = TSOp(op="lag", operand=Leaf(field="close_adj"), window=5)
    candidate = _Candidate(ast=ast)
    dates = _dates()

    cfg = WalkForwardConfig(
        segments=3,
        oos_window_months=2,
        min_train_months=1,
        oos_ic_ir_threshold=0.0,
        primary_horizon_days=5,
    )
    result = run_walk_forward(
        candidate=candidate,
        walk_fwd_config=cfg,
        research_db=synth_db,
        start_date=dates[0],
        end_date=dates[-1],
        universe_id="csi800",
    )
    assert result.status == "accepted_oos"
    assert result.first_failing_segment is None


def test_candidate_failing_threshold_rejected(synth_db):
    """R5.5: negative-IC signal -> rejected_oos with first_failing_segment set.

    close_adj - turnover_value_cny is dominated by the turnover term which is
    proportional to price * volume.  Higher-growth securities have larger turnover
    so the signal is strongly negative for the securities with the highest future
    returns, producing oos_ic_mean ~= -1.  The gate requires oos_ic_mean > 0,
    so segment 1 fails immediately.
    """
    ast = ArithOp(
        op="-", left=Leaf(field="close_adj"), right=Leaf(field="turnover_value_cny")
    )
    candidate = _Candidate(ast=ast)
    dates = _dates()

    cfg = WalkForwardConfig(
        segments=1,
        oos_window_months=2,
        min_train_months=1,
        oos_ic_ir_threshold=0.0,
        primary_horizon_days=5,
    )
    result = run_walk_forward(
        candidate=candidate,
        walk_fwd_config=cfg,
        research_db=synth_db,
        start_date=dates[0],
        end_date=dates[-1],
        universe_id="csi800",
    )
    assert result.status == "rejected_oos"
    assert result.first_failing_segment is not None


def test_precondition_violation_raises(synth_db):
    """R5.2: min_train_months=24 with only ~1 month of data -> ValueError with 'segment'."""
    ast = Leaf(field="close_adj")
    candidate = _Candidate(ast=ast)
    dates = _dates()

    # Restrict to ~21 trading days (~1 calendar month)
    tiny_end = dates[20]
    cfg = WalkForwardConfig(
        segments=1,
        oos_window_months=1,
        min_train_months=24,
        oos_ic_ir_threshold=0.0,
        primary_horizon_days=5,
    )
    with pytest.raises(ValueError, match="segment"):
        run_walk_forward(
            candidate=candidate,
            walk_fwd_config=cfg,
            research_db=synth_db,
            start_date=dates[0],
            end_date=tiny_end,
            universe_id="csi800",
        )


def test_adhoc_spec_not_in_registry_after_run(synth_db):
    """R9.4: ad-hoc spec must never remain in the descriptor registry after run completes."""
    ast = TSOp(op="lag", operand=Leaf(field="close_adj"), window=5)
    candidate = _Candidate(ast=ast)
    dates = _dates()

    cfg = WalkForwardConfig(
        segments=1,
        oos_window_months=2,
        min_train_months=1,
        oos_ic_ir_threshold=0.0,
        primary_horizon_days=5,
    )

    registered_before = set(list_registered())

    try:
        run_walk_forward(
            candidate=candidate,
            walk_fwd_config=cfg,
            research_db=synth_db,
            start_date=dates[0],
            end_date=dates[-1],
            universe_id="csi800",
        )
    except Exception:
        pass  # only care about registry state

    registered_after = set(list_registered())
    adhoc_new = [
        e for e in (registered_after - registered_before) if e.startswith("__adhoc__")
    ]
    assert not adhoc_new, f"Ad-hoc spec(s) left in registry: {adhoc_new}"
