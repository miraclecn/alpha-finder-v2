"""Unit tests for factor_lab.walk_forward.

Verification target: Task 13
  - 3-segment split is computed correctly
  - Precondition violations raise ValueError with offending segment info
  - Ad-hoc spec never appears in descriptor_compute.list_registered() after call

Uses a larger synthetic DB: 250 securities × 500 trading days so that
realistic multi-segment splits can be exercised.
"""
from __future__ import annotations

import math
import tempfile
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from alpha_find_v2.factor_evaluation.descriptor_compute import list_registered
from alpha_find_v2.factor_lab.config import WalkForwardConfig
from alpha_find_v2.factor_lab.dsl.grammar import Leaf
from alpha_find_v2.factor_lab.walk_forward import (
    WalkForwardResult,
    _add_months,
    _build_segments,
    _count_months_between,
    _snap_to_prior_trade_date,
    _validate_preconditions,
    run_walk_forward,
)


# ---------------------------------------------------------------------------
# Minimal Candidate stand-in (only .ast is needed)
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
# Synthetic DB builder (250 securities × 500 trading days)
# ---------------------------------------------------------------------------

_N_SECURITIES = 10   # enough for universe resolver but keep tests fast
_N_DATES = 500


def _trading_days(n: int, start: date = date(2018, 1, 2)) -> list[str]:
    days: list[str] = []
    d = start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return days


def _build_large_synth_db(tmp_dir: Path) -> Path:
    """Build a synthetic research DB with enough dates for 3-segment walk-forward."""
    db_path = tmp_dir / "research_wf.duckdb"
    dates = _trading_days(_N_DATES)
    securities = [f"{600000 + i:06d}.SH" for i in range(_N_SECURITIES)]

    conn = duckdb.connect(str(db_path))

    # market_trade_calendar
    conn.execute("CREATE TABLE market_trade_calendar (trade_date VARCHAR PRIMARY KEY)")
    conn.executemany("INSERT INTO market_trade_calendar VALUES (?)", [(d,) for d in dates])

    # security_master_ref
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
            "INSERT INTO security_master_ref VALUES (?,?,?,'SH','main_board','上海','20100101',NULL,'N',TRUE,CURRENT_TIMESTAMP)",
            [sec, sym, f"Stock_{sym}"],
        )

    # daily_bar_pit — monotonically increasing price per security
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
                p / growth,    # pre_close
                p * 0.999,     # open
                p * 1.01,      # high
                p * 0.99,      # low
                p,             # close
                p,             # close_adj
                p * free_mn * 1e4 * 0.002,   # turnover_value_cny
                free_mn * 1e4 * 0.002,       # volume_shares
                "standard",
                p * 0.999,     # open_adj
                p * free_mn * 1e4,
                free_mn * 1e4,
            ))
    conn.executemany(
        "INSERT INTO daily_bar_pit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )

    # raw_adj_factor
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

    # benchmark_membership_pit
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
def large_db() -> Path:
    with tempfile.TemporaryDirectory() as tmp:
        yield _build_large_synth_db(Path(tmp))


# ---------------------------------------------------------------------------
# Unit tests for segment date arithmetic (no DB needed)
# ---------------------------------------------------------------------------


class TestAddMonths:
    def test_basic(self):
        assert _add_months(date(2020, 1, 31), 1) == date(2020, 2, 29)  # 2020 leap year
        assert _add_months(date(2021, 1, 31), 1) == date(2021, 2, 28)  # 2021 not leap
        assert _add_months(date(2020, 3, 15), 3) == date(2020, 6, 15)
        assert _add_months(date(2020, 1, 1), 12) == date(2021, 1, 1)


class TestSnapToPriorTradeDate:
    def test_exact_match(self):
        dates = ["20200101", "20200102", "20200106"]
        assert _snap_to_prior_trade_date(date(2020, 1, 2), dates) == "20200102"

    def test_weekend_snaps_to_friday(self):
        dates = ["20200103", "20200106", "20200107"]  # Mon, Fri, Sat (fake)
        assert _snap_to_prior_trade_date(date(2020, 1, 5), dates) == "20200103"

    def test_before_all_raises(self):
        dates = ["20200103", "20200106"]
        with pytest.raises(ValueError):
            _snap_to_prior_trade_date(date(2020, 1, 1), dates)


# ---------------------------------------------------------------------------
# Segment structure tests
# ---------------------------------------------------------------------------


class TestBuildSegments:
    """3-segment split is computed with correct anchored structure."""

    # Use oos_window_months=2 so 3 segments fit within ~300 trading days (~14 months).
    # Segment k train_end = start + k*2 months; last OOS end = start + 8 months ≈ 168 days.

    def _make_dates(self):
        """300 trading days starting 2018-01-02 (~14 calendar months)."""
        return _trading_days(300, start=date(2018, 1, 2))

    def test_three_segments_count(self):
        dates = self._make_dates()
        cfg = WalkForwardConfig(segments=3, oos_window_months=2, min_train_months=1)
        segs = _build_segments(dates[0], dates[-1], cfg, dates)
        assert len(segs) == 3

    def test_all_segments_anchored_to_start(self):
        dates = self._make_dates()
        cfg = WalkForwardConfig(segments=3, oos_window_months=2, min_train_months=1)
        segs = _build_segments(dates[0], dates[-1], cfg, dates)
        for seg in segs:
            assert seg["train_start"] == dates[0]

    def test_train_ends_increasing(self):
        dates = self._make_dates()
        cfg = WalkForwardConfig(segments=3, oos_window_months=2, min_train_months=1)
        segs = _build_segments(dates[0], dates[-1], cfg, dates)
        ends = [seg["train_end"] for seg in segs]
        assert ends[0] < ends[1] < ends[2]

    def test_oos_start_after_train_end(self):
        dates = self._make_dates()
        cfg = WalkForwardConfig(segments=3, oos_window_months=2, min_train_months=1)
        segs = _build_segments(dates[0], dates[-1], cfg, dates)
        for seg in segs:
            assert seg["oos_start"] > seg["train_end"]

    def test_oos_end_after_oos_start(self):
        dates = self._make_dates()
        cfg = WalkForwardConfig(segments=3, oos_window_months=2, min_train_months=1)
        segs = _build_segments(dates[0], dates[-1], cfg, dates)
        for seg in segs:
            assert seg["oos_end"] > seg["oos_start"]

    def test_segment_k_labels(self):
        dates = self._make_dates()
        cfg = WalkForwardConfig(segments=3, oos_window_months=2, min_train_months=1)
        segs = _build_segments(dates[0], dates[-1], cfg, dates)
        assert [s["k"] for s in segs] == [1, 2, 3]


# ---------------------------------------------------------------------------
# Precondition violation tests
# ---------------------------------------------------------------------------


class TestPreconditions:
    def _make_dates(self, n=300):
        return _trading_days(n, start=date(2018, 1, 2))

    def test_min_train_months_violation_raises(self):
        """If train window < min_train_months, raise ValueError naming segment."""
        dates = self._make_dates(60)  # only 60 trading days ≈ ~3 months
        cfg = WalkForwardConfig(segments=1, oos_window_months=1, min_train_months=24)
        segs = _build_segments(dates[0], dates[-1], cfg, dates)
        with pytest.raises(ValueError, match="segment 1"):
            _validate_preconditions(segs, cfg, dates[-1], dates)

    def test_oos_end_exceeds_end_date_raises(self):
        """If last segment OOS end > end_date, raise ValueError naming segment."""
        dates = self._make_dates(300)
        # Use oos_window_months=2 so 3 segments fit in the trade date list.
        # Then tell _validate_preconditions the end_date is only 3 months in
        # so the last segment's OOS end exceeds it.
        cfg = WalkForwardConfig(segments=3, oos_window_months=2, min_train_months=1)
        segs = _build_segments(dates[0], dates[-1], cfg, dates)
        # Artificially shrink end_date so last OOS overflows
        fake_end = "20180601"  # well before last OOS end (~start + 8 months)
        with pytest.raises(ValueError, match=r"segment \d+"):
            _validate_preconditions(segs, cfg, fake_end, dates)

    def test_valid_config_does_not_raise(self):
        """Reasonable config with enough dates passes precondition check."""
        dates = self._make_dates(300)
        cfg = WalkForwardConfig(segments=2, oos_window_months=2, min_train_months=1)
        segs = _build_segments(dates[0], dates[-1], cfg, dates)
        # Should not raise
        _validate_preconditions(segs, cfg, dates[-1], dates)


# ---------------------------------------------------------------------------
# Ad-hoc spec NOT in registry after run
# ---------------------------------------------------------------------------


class TestAdhocSpecNotRegistered:
    """After run_walk_forward completes, the ad-hoc spec must not be in list_registered()."""

    def test_adhoc_spec_absent_after_run(self, large_db: Path):
        """R9.4: ad-hoc spec never permanently registered."""
        ast = Leaf(field="close_adj")  # simplest possible expression
        candidate = _Candidate(ast=ast)

        # Use a date range with enough data for at least 1 segment with low thresholds
        dates = _trading_days(_N_DATES)
        start = dates[0]
        end = dates[-1]

        cfg = WalkForwardConfig(
            segments=1,
            oos_window_months=2,
            min_train_months=1,
            oos_ic_ir_threshold=0.0,   # low threshold so we don't need real signal
            primary_horizon_days=5,
        )

        registered_before = set(list_registered())

        try:
            run_walk_forward(
                candidate=candidate,
                walk_fwd_config=cfg,
                research_db=large_db,
                start_date=start,
                end_date=end,
                universe_id="csi800",
            )
        except Exception:
            pass  # we only care about registry state, not success

        registered_after = set(list_registered())
        new_entries = registered_after - registered_before
        adhoc_entries = [e for e in new_entries if e.startswith("__adhoc__")]
        assert not adhoc_entries, (
            f"Ad-hoc spec(s) left in registry: {adhoc_entries}"
        )


# ---------------------------------------------------------------------------
# Integration: full 3-segment run
# ---------------------------------------------------------------------------


class TestRunWalkForward:
    """Integration: run_walk_forward returns correct structure."""

    def test_returns_walk_forward_result(self, large_db: Path):
        ast = Leaf(field="close_adj")
        candidate = _Candidate(ast=ast)
        dates = _trading_days(_N_DATES)

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
            research_db=large_db,
            start_date=dates[0],
            end_date=dates[-1],
            universe_id="csi800",
        )
        assert isinstance(result, WalkForwardResult)

    def test_three_segments_in_result(self, large_db: Path):
        ast = Leaf(field="close_adj")
        candidate = _Candidate(ast=ast)
        dates = _trading_days(_N_DATES)

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
            research_db=large_db,
            start_date=dates[0],
            end_date=dates[-1],
            universe_id="csi800",
        )
        assert len(result.oos_segments) == 3

    def test_status_is_accepted_or_rejected(self, large_db: Path):
        ast = Leaf(field="close_adj")
        candidate = _Candidate(ast=ast)
        dates = _trading_days(_N_DATES)

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
            research_db=large_db,
            start_date=dates[0],
            end_date=dates[-1],
            universe_id="csi800",
        )
        assert result.status in ("accepted_oos", "rejected_oos")

    def test_rejected_has_first_failing_segment(self, large_db: Path):
        """With threshold=0 but OOS window too small for rolling_std, expect rejected."""
        from alpha_find_v2.factor_lab.dsl.grammar import TSOp

        # rolling_std(close_adj, 250) requires 250 prior dates to produce any value.
        # An OOS window of 2 months (~42 dates) will yield all NaN → IC is NaN
        # → math.isfinite(nan) = False → passes=False → rejected_oos.
        ast = TSOp(op="rolling_std", operand=Leaf(field="close_adj"), window=250)
        candidate = _Candidate(ast=ast)
        dates = _trading_days(_N_DATES)

        cfg = WalkForwardConfig(
            segments=1,
            oos_window_months=2,
            min_train_months=1,
            oos_ic_ir_threshold=0.0,   # even threshold=0 won't help if IC is NaN
            primary_horizon_days=5,
        )
        result = run_walk_forward(
            candidate=candidate,
            walk_fwd_config=cfg,
            research_db=large_db,
            start_date=dates[0],
            end_date=dates[-1],
            universe_id="csi800",
        )
        assert result.status == "rejected_oos"
        assert result.first_failing_segment == 1
        assert result.failing_oos_ic_ir is not None
        assert result.failing_oos_ic_mean is not None

    def test_accepted_has_none_failing_fields(self, large_db: Path):
        """With threshold=0 and positive mean, a synthetic monotone signal should pass."""
        from alpha_find_v2.factor_lab.dsl.grammar import TSOp

        # lag(close_adj, 5) — produces a non-trivial signal
        ast = TSOp(op="lag", operand=Leaf(field="close_adj"), window=5)
        candidate = _Candidate(ast=ast)
        dates = _trading_days(_N_DATES)

        cfg = WalkForwardConfig(
            segments=1,
            oos_window_months=2,
            min_train_months=1,
            oos_ic_ir_threshold=0.0,   # pass if IC mean > 0 or IC IR >= 0
            primary_horizon_days=5,
        )
        result = run_walk_forward(
            candidate=candidate,
            walk_fwd_config=cfg,
            research_db=large_db,
            start_date=dates[0],
            end_date=dates[-1],
            universe_id="csi800",
        )
        # If accepted, failing fields must be None
        if result.status == "accepted_oos":
            assert result.first_failing_segment is None
            assert result.failing_oos_ic_ir is None
            assert result.failing_oos_ic_mean is None

    def test_precondition_violation_raises(self, large_db: Path):
        """min_train_months too large for the available date range → ValueError."""
        ast = Leaf(field="close_adj")
        candidate = _Candidate(ast=ast)
        dates = _trading_days(_N_DATES)

        # Only use a tiny slice (30 days) but require 24 months of train data
        tiny_end = dates[29]
        cfg = WalkForwardConfig(
            segments=1,
            oos_window_months=1,
            min_train_months=24,
            oos_ic_ir_threshold=0.0,
            primary_horizon_days=5,
        )
        with pytest.raises(ValueError, match="Precondition failed"):
            run_walk_forward(
                candidate=candidate,
                walk_fwd_config=cfg,
                research_db=large_db,
                start_date=dates[0],
                end_date=tiny_end,
                universe_id="csi800",
            )

    def test_segment_records_have_required_keys(self, large_db: Path):
        ast = Leaf(field="close_adj")
        candidate = _Candidate(ast=ast)
        dates = _trading_days(_N_DATES)

        cfg = WalkForwardConfig(
            segments=2,
            oos_window_months=2,
            min_train_months=1,
            oos_ic_ir_threshold=0.0,
            primary_horizon_days=5,
        )
        result = run_walk_forward(
            candidate=candidate,
            walk_fwd_config=cfg,
            research_db=large_db,
            start_date=dates[0],
            end_date=dates[-1],
            universe_id="csi800",
        )
        for seg in result.oos_segments:
            for key in ("segment", "train_start", "train_end", "oos_start", "oos_end",
                        "train_ic_ir", "oos_ic_ir", "oos_ic_mean", "oos_coverage"):
                assert key in seg, f"Missing key '{key}' in segment record"
