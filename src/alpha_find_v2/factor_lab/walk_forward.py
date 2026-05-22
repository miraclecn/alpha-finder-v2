"""Walk-forward evaluator for factor_lab candidates.

Splits [start, end] into anchored segments, evaluates each segment via the
Stage 2 evaluate_descriptor pipeline using an ad-hoc (never-registered)
DescriptorComputeSpec, and applies the acceptance gate.

Requirements: R5.1–R5.8, R9.3, R9.4, R9.6
"""
from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pandas as pd

from alpha_find_v2.factor_evaluation.descriptor_compute import (
    REGISTRY,
    ComputeContext,
    DescriptorComputeSpec,
)
from alpha_find_v2.factor_evaluation.descriptor_evaluator import evaluate_descriptor
from alpha_find_v2.factor_lab.config import WalkForwardConfig
from alpha_find_v2.factor_lab.dsl.canonical import canonical
from alpha_find_v2.factor_lab.dsl.evaluator import EvaluationContext, evaluate
from alpha_find_v2.factor_lab.dsl.grammar import ASTNode

if TYPE_CHECKING:
    from alpha_find_v2.factor_lab.search.beam import Candidate

# Default cost model path (R9.6)
_DEFAULT_COST_MODEL = Path(__file__).parents[4] / "config" / "cost_models" / "base_a_share_cash.toml"


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class WalkForwardResult:
    status: str                      # "accepted_oos" or "rejected_oos"
    oos_segments: list[dict]         # per-segment metrics (one entry per segment)
    first_failing_segment: int | None  # 1-indexed; None when accepted
    failing_oos_ic_ir: float | None
    failing_oos_ic_mean: float | None


# ---------------------------------------------------------------------------
# Segment date arithmetic
# ---------------------------------------------------------------------------


def _add_months(d: date, months: int) -> date:
    """Add calendar months to a date (day clamped to month-end if needed)."""
    month = d.month - 1 + months
    year = d.year + month // 12
    month = month % 12 + 1
    # Clamp day to valid range for the new month
    import calendar
    max_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(d.day, max_day))


def _snap_to_prior_trade_date(d: date, trade_dates: list[str]) -> str:
    """Return the latest trade date that is <= d (YYYYMMDD string)."""
    target = d.strftime("%Y%m%d")
    # Binary search for the last date <= target
    lo, hi = 0, len(trade_dates) - 1
    result = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if trade_dates[mid] <= target:
            result = trade_dates[mid]
            lo = mid + 1
        else:
            hi = mid - 1
    if result is None:
        raise ValueError(f"No trade date on or before {target}")
    return result


def _snap_to_next_trade_date(after: str, trade_dates: list[str]) -> str:
    """Return the earliest trade date strictly after *after* (YYYYMMDD string)."""
    lo, hi = 0, len(trade_dates) - 1
    result = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if trade_dates[mid] > after:
            result = trade_dates[mid]
            hi = mid - 1
        else:
            lo = mid + 1
    if result is None:
        raise ValueError(f"No trade date after {after}")
    return result


def _count_months_between(start_str: str, end_str: str, trade_dates: list[str]) -> int:
    """Approximate months in [start_str, end_str] as count_of_trade_days / 21."""
    dates_in_range = [d for d in trade_dates if start_str <= d <= end_str]
    return len(dates_in_range) // 21


def _build_segments(
    start_date: str,
    end_date: str,
    config: WalkForwardConfig,
    trade_dates: list[str],
) -> list[dict]:
    """Compute segment date boundaries.

    Returns list of dicts with keys:
        k, train_start, train_end, oos_start, oos_end

    Raises ValueError with offending segment index on precondition failure.
    """
    start = date.fromisoformat(start_date[:4] + "-" + start_date[4:6] + "-" + start_date[6:8])

    segments = []
    for k in range(1, config.segments + 1):
        # train_end = start + k * oos_window_months (calendar months, snapped)
        train_end_cal = _add_months(start, k * config.oos_window_months)
        train_end = _snap_to_prior_trade_date(train_end_cal, trade_dates)

        # oos_start = next trade day after train_end
        try:
            oos_start = _snap_to_next_trade_date(train_end, trade_dates)
        except ValueError:
            raise ValueError(
                f"Precondition failed: segment {k}: no trade date after train_end {train_end}"
            )

        # oos_end = train_end + oos_window_months (calendar months, snapped)
        oos_end_cal = _add_months(date.fromisoformat(
            train_end[:4] + "-" + train_end[4:6] + "-" + train_end[6:8]
        ), config.oos_window_months)
        oos_end = _snap_to_prior_trade_date(oos_end_cal, trade_dates)

        segments.append({
            "k": k,
            "train_start": start_date,
            "train_end": train_end,
            "oos_start": oos_start,
            "oos_end": oos_end,
        })

    return segments


def _validate_preconditions(
    segments: list[dict],
    config: WalkForwardConfig,
    end_date: str,
    trade_dates: list[str],
) -> None:
    """Raise ValueError for the first failing precondition (R5.2)."""
    for seg in segments:
        k = seg["k"]
        train_months = _count_months_between(seg["train_start"], seg["train_end"], trade_dates)
        if train_months < config.min_train_months:
            raise ValueError(
                f"Precondition failed: segment {k}: train window has ~{train_months} months "
                f"(< min_train_months={config.min_train_months})"
            )

    # Last segment OOS end must not exceed end_date
    last = segments[-1]
    if last["oos_end"] > end_date:
        k = last["k"]
        raise ValueError(
            f"Precondition failed: segment {k}: OOS end {last['oos_end']} "
            f"exceeds --end {end_date}"
        )


# ---------------------------------------------------------------------------
# Ad-hoc spec (R9.3, R9.4)
# ---------------------------------------------------------------------------


def _make_adhoc_spec(ast: ASTNode) -> DescriptorComputeSpec:
    """Construct an ad-hoc DescriptorComputeSpec that wraps dsl.evaluator.evaluate.

    The spec id starts with '__adhoc__' so it never collides with registered
    descriptor ids (which follow the slug naming convention).
    """
    spec_id = f"__adhoc__{canonical(ast)}"

    def fn(ctx: ComputeContext) -> pd.DataFrame:
        eval_ctx = EvaluationContext(
            conn=ctx.conn,
            start_date=ctx.start_date,
            end_date=ctx.end_date,
        )
        return evaluate(ast, eval_ctx)  # returns [trade_date, security_id, descriptor_value]

    return DescriptorComputeSpec(
        descriptor_id=spec_id,
        fn=fn,
        requires=(),
        notes="ad-hoc sandbox candidate",
    )


@contextlib.contextmanager
def _temporary_registration(spec: DescriptorComputeSpec):
    """Context manager that registers *spec* for the duration, then removes it.

    Satisfies R9.4: the spec is never permanently in the global registry.
    Asserts the id is not already registered before entering.
    """
    assert spec.descriptor_id not in REGISTRY, (
        f"Ad-hoc spec id '{spec.descriptor_id}' is already in registry"
    )
    REGISTRY[spec.descriptor_id] = spec
    try:
        yield
    finally:
        REGISTRY.pop(spec.descriptor_id, None)


# ---------------------------------------------------------------------------
# IC extraction helpers
# ---------------------------------------------------------------------------


def _extract_oos_metrics(report, horizon: int) -> dict:
    """Extract OOS IC IR and IC mean from an evaluation report."""
    hm = report.horizon_metrics.get(horizon)
    if hm is None:
        return {"oos_ic_ir": float("nan"), "oos_ic_mean": float("nan"), "oos_coverage": 0.0}
    return {
        "oos_ic_ir": hm.ic_pearson.ir,
        "oos_ic_mean": hm.ic_pearson.mean,
        "oos_coverage": report.coverage.rows_used_over_possible,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_walk_forward(
    candidate: "Candidate",
    walk_fwd_config: WalkForwardConfig,
    research_db: Path,
    start_date: str,
    end_date: str,
    universe_id: str = "investable_a_share_core",
    cost_model_path: Path | None = None,
) -> WalkForwardResult:
    """Run anchored walk-forward evaluation for one candidate.

    Args:
        candidate: Beam/random-search Candidate with an .ast attribute.
        walk_fwd_config: WalkForwardConfig (segments, oos_window_months, etc.).
        research_db: Path to research_source.duckdb.
        start_date: Run start date (YYYYMMDD).
        end_date: Run end date (YYYYMMDD).
        universe_id: Universe identifier passed to evaluate_descriptor.
        cost_model_path: Optional cost model TOML; defaults to base_a_share_cash.

    Returns:
        WalkForwardResult with status, per-segment metrics, and failure info.

    Raises:
        ValueError: If preconditions (R5.2) are violated.
    """
    if cost_model_path is None:
        cost_model_path = _DEFAULT_COST_MODEL if _DEFAULT_COST_MODEL.exists() else None

    # Load trade calendar from the DB
    conn = duckdb.connect(str(research_db), read_only=True)
    try:
        rows = conn.execute(
            "SELECT trade_date FROM market_trade_calendar ORDER BY trade_date"
        ).fetchall()
        trade_dates = [r[0] for r in rows]
    finally:
        conn.close()

    if not trade_dates:
        raise ValueError("No trade dates found in market_trade_calendar")

    # Build segments (R5.1)
    segments = _build_segments(start_date, end_date, walk_fwd_config, trade_dates)

    # Validate preconditions (R5.2)
    _validate_preconditions(segments, walk_fwd_config, end_date, trade_dates)

    # Build the ad-hoc spec (R9.3, R9.4)
    spec = _make_adhoc_spec(candidate.ast)

    horizon = walk_fwd_config.primary_horizon_days
    oos_segments: list[dict] = []
    first_failing: int | None = None
    failing_ic_ir: float | None = None
    failing_ic_mean: float | None = None

    # Evaluate each segment (R5.3, R5.6, R5.7, R5.8)
    for seg in segments:
        k = seg["k"]

        with _temporary_registration(spec):
            # Train window evaluation (for train IC_IR)
            try:
                train_report = evaluate_descriptor(
                    descriptor_id=spec.descriptor_id,
                    research_db=research_db,
                    universe=universe_id,
                    start_date=seg["train_start"],
                    end_date=seg["train_end"],
                    horizons=(horizon,),
                    primary_horizon=horizon,
                    cost_model_path=cost_model_path,
                )
                train_hm = train_report.horizon_metrics.get(horizon)
                train_ic_ir = train_hm.ic_pearson.ir if train_hm else float("nan")
            except Exception:
                train_ic_ir = float("nan")

        with _temporary_registration(spec):
            # OOS window evaluation (R5.3)
            try:
                oos_report = evaluate_descriptor(
                    descriptor_id=spec.descriptor_id,
                    research_db=research_db,
                    universe=universe_id,
                    start_date=seg["oos_start"],
                    end_date=seg["oos_end"],
                    horizons=(horizon,),
                    primary_horizon=horizon,
                    cost_model_path=cost_model_path,
                )
                oos_metrics = _extract_oos_metrics(oos_report, horizon)
            except Exception:
                oos_metrics = {
                    "oos_ic_ir": float("nan"),
                    "oos_ic_mean": float("nan"),
                    "oos_coverage": 0.0,
                }

        seg_record = {
            "segment": k,
            "train_start": seg["train_start"],
            "train_end": seg["train_end"],
            "oos_start": seg["oos_start"],
            "oos_end": seg["oos_end"],
            "train_ic_ir": train_ic_ir,
            **oos_metrics,
        }
        oos_segments.append(seg_record)

        # Check acceptance gate (R5.4, R5.5) — record first failure
        if first_failing is None:
            oos_ic_ir = oos_metrics["oos_ic_ir"]
            oos_ic_mean = oos_metrics["oos_ic_mean"]
            import math
            passes = (
                math.isfinite(oos_ic_ir)
                and oos_ic_ir >= walk_fwd_config.oos_ic_ir_threshold
                and math.isfinite(oos_ic_mean)
                and oos_ic_mean > 0
            )
            if not passes:
                first_failing = k
                failing_ic_ir = oos_ic_ir
                failing_ic_mean = oos_ic_mean

    # Determine status
    if first_failing is None:
        status = "accepted_oos"
    else:
        status = "rejected_oos"

    return WalkForwardResult(
        status=status,
        oos_segments=oos_segments,
        first_failing_segment=first_failing,
        failing_oos_ic_ir=failing_ic_ir,
        failing_oos_ic_mean=failing_ic_mean,
    )
