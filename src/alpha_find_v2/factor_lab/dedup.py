"""Correlation deduplication stage for factor mining candidates.

After walk-forward acceptance, computes pairwise absolute Pearson correlation
between each accepted candidate's score series and:
  (a) every registered descriptor's score series (loaded once, cached)
  (b) every already-admitted candidate's score series

Candidates are processed in fitness-descending order (R6.7).
A candidate is rejected if any defined |r| > dedup_rho (R6.6).
Undefined cells (insufficient overlap or zero variance) are None / empty string (R6.5).
The full correlation matrix is returned for all evaluated candidates (R6.8).

Requirements: R6.4, R6.5, R6.6, R6.7, R6.8, R6.9
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from alpha_find_v2.factor_evaluation.descriptor_compute import ComputeContext, get
from alpha_find_v2.factor_lab.dsl.evaluator import EvaluationContext, evaluate
from alpha_find_v2.factor_lab.search.beam import Candidate
from alpha_find_v2.factor_lab.search.fitness import sort_key


@dataclass
class DedupResult:
    admitted: list[Candidate]
    rejected_correlation: list[Candidate]
    # None = undefined (empty string in CSV)
    matrix: dict[str, dict[str, float | None]]


def _pearson_or_none(
    left: pd.DataFrame,
    right: pd.DataFrame,
    min_obs: int,
) -> float | None:
    """Return absolute Pearson r for overlapping (trade_date, security_id) pairs.

    Returns None if overlap < min_obs or either side has zero variance (R6.5).
    """
    merged = left.merge(right, on=["trade_date", "security_id"], suffixes=("_l", "_r"))
    both_valid = merged.dropna(subset=["descriptor_value_l", "descriptor_value_r"])
    if len(both_valid) < min_obs:
        return None
    x = both_valid["descriptor_value_l"].values
    y = both_valid["descriptor_value_r"].values
    if np.std(x) == 0 or np.std(y) == 0:
        return None
    r = float(np.corrcoef(x, y)[0, 1])
    return abs(r) if math.isfinite(r) else None


def run_correlation_dedup(
    candidates: list[Candidate],
    registered_descriptor_ids: list[str],
    ctx: EvaluationContext,
    dedup_rho: float = 0.85,
    dedup_min_obs: int = 60,
) -> DedupResult:
    """Run correlation dedup on walk-forward-accepted candidates.

    Args:
        candidates: All walk-forward accepted candidates.
        registered_descriptor_ids: Descriptor ids to compare against (R6.4, R6.9).
        ctx: EvaluationContext covering the train window.
        dedup_rho: Rejection threshold for absolute Pearson r (default 0.85).
        dedup_min_obs: Minimum overlapping observations for a defined cell (default 60).

    Returns:
        DedupResult with admitted, rejected_correlation, and full correlation matrix.
    """
    # Sort candidates: fitness descending, then node_count asc, then canonical asc (R6.7)
    ordered = sorted(
        candidates,
        key=lambda c: sort_key((c.fitness, c.node_count, c.canonical)),
    )

    # Cache registered descriptor score series once (R6.9)
    compute_ctx = ComputeContext(
        conn=ctx.conn,
        start_date=ctx.start_date,
        end_date=ctx.end_date,
    )
    desc_series: dict[str, pd.DataFrame] = {}
    for desc_id in registered_descriptor_ids:
        spec = get(desc_id)
        desc_series[desc_id] = spec.fn(compute_ctx)

    admitted: list[Candidate] = []
    rejected_correlation: list[Candidate] = []
    # admitted_series: score series for already-admitted candidates (in acceptance order)
    admitted_series: list[tuple[str, pd.DataFrame]] = []  # (canonical, series)

    # matrix rows: every evaluated candidate; cols: registered_ids then admitted canonicals
    matrix: dict[str, dict[str, float | None]] = {}

    for cand in ordered:
        cand_series = evaluate(cand.ast, ctx)
        row: dict[str, float | None] = {}

        max_r: float | None = None
        max_ref_id: str | None = None

        # Compare against registered descriptors (R6.4)
        for desc_id in registered_descriptor_ids:
            r = _pearson_or_none(cand_series, desc_series[desc_id], dedup_min_obs)
            row[desc_id] = r
            if r is not None and (max_r is None or r > max_r):
                max_r = r
                max_ref_id = desc_id

        # Compare against already-admitted candidates (R6.4)
        for adm_canonical, adm_series in admitted_series:
            r = _pearson_or_none(cand_series, adm_series, dedup_min_obs)
            row[adm_canonical] = r
            if r is not None and (max_r is None or r > max_r):
                max_r = r
                max_ref_id = adm_canonical

        matrix[cand.canonical] = row

        # Reject if any defined |r| > dedup_rho (R6.6)
        if max_r is not None and max_r > dedup_rho:
            cand.status = "rejected_correlation"
            cand.corr_ref_id = max_ref_id
            cand.corr_r = round(max_r, 6)
            rejected_correlation.append(cand)
        else:
            admitted.append(cand)
            admitted_series.append((cand.canonical, cand_series))

    return DedupResult(
        admitted=admitted,
        rejected_correlation=rejected_correlation,
        matrix=matrix,
    )
