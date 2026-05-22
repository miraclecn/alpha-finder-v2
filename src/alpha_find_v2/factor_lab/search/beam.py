"""Beam search engine for factor mining.

Layer-by-layer expression growth from depth 1 to ``max_depth``.  At the end
of each layer, retain at most ``beam_width`` candidates ranked by fitness
(train IC_IR minus complexity penalty).

Tie-breaking for retention: ascending ``node_count``, then ascending
lexicographic canonical string (R3.1, R6.3).

Candidates whose IC_IR is NaN, non-finite, or whose evaluation raised an error
are ineligible for retention (R3.2).

All stochastic choices derive from the RNG seeded from ``config.search.seed``;
beam search itself is deterministic (R3.8).

Requirements: R3.1, R3.2, R3.5, R3.8, R6.1, R6.2, R6.3
"""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from alpha_find_v2.factor_evaluation.forward_returns import compute_forward_returns
from alpha_find_v2.factor_lab.config import FitnessConfig, SearchConfig
from alpha_find_v2.factor_lab.dsl.canonical import canonical
from alpha_find_v2.factor_lab.dsl.evaluator import EvaluationContext, evaluate
from alpha_find_v2.factor_lab.dsl.grammar import ASTNode, node_count
from alpha_find_v2.factor_lab.family import classify
from alpha_find_v2.factor_lab.search.expression_generator import all_leaves, expand_layer
from alpha_find_v2.factor_lab.search.fitness import fitness, sort_key


# ---------------------------------------------------------------------------
# Candidate dataclass
# ---------------------------------------------------------------------------


@dataclass
class Candidate:
    """A single expression candidate produced by the beam search.

    Attributes:
        expr_id: Deterministic UUID5 hex string based on canonical string.
        canonical: Canonical string form of the AST.
        ast: The AST itself.
        node_count: Number of DSL nodes.
        family: Family classification (or None if unclassifiable).
        sources: Origin tags, e.g. ``["beam"]``.
        train_ic_ir: Train-set IC_IR (or None if not computable).
        fitness: Fitness score (or None if not computable).
        status: Lifecycle status string.
        oos_segments: Populated later by walk-forward; empty list at beam time.
    """

    expr_id: str
    canonical: str
    ast: ASTNode
    node_count: int
    family: str | None
    sources: list[str]
    train_ic_ir: float | None
    fitness: float | None
    status: str
    oos_segments: list[dict] = field(default_factory=list)
    # Populated by correlation dedup stage when status == "rejected_correlation"
    corr_ref_id: str | None = None
    corr_r: float | None = None


# ---------------------------------------------------------------------------
# IC_IR helper
# ---------------------------------------------------------------------------

# Stable UUID5 namespace for deterministic expr_id generation.
_EXPR_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")


def _expr_id(canonical_str: str) -> str:
    """Return a deterministic UUID5 hex string based on the canonical string."""
    return uuid.uuid5(_EXPR_NAMESPACE, canonical_str).hex


def _compute_ic_ir(
    scores_df: pd.DataFrame,
    fwd_returns_df: pd.DataFrame,
) -> float | None:
    """Compute Pearson IC per date; return IC_IR = mean(IC) / std(IC).

    Args:
        scores_df: DataFrame with columns [trade_date, security_id, descriptor_value].
        fwd_returns_df: DataFrame with columns [security_id, trade_date, forward_return].

    Returns:
        IC_IR as a float, or None if std(IC) is 0 or fewer than 2 valid dates.
    """
    merged = scores_df.merge(
        fwd_returns_df[["security_id", "trade_date", "forward_return"]],
        on=["security_id", "trade_date"],
        how="inner",
    )
    if merged.empty:
        return None

    # Per-date Pearson IC
    def _pearson_corr(group: pd.DataFrame) -> float | None:
        x = group["descriptor_value"].values
        y = group["forward_return"].values
        # Need at least 2 observations
        if len(x) < 2:
            return None
        with np.errstate(invalid="ignore"):
            corr = np.corrcoef(x, y)[0, 1]
        if not math.isfinite(corr):
            return None
        return float(corr)

    ic_by_date = (
        merged.groupby("trade_date", sort=True)
        .apply(_pearson_corr, include_groups=False)
        .dropna()
    )

    if len(ic_by_date) < 2:
        return None

    ic_values = ic_by_date.values.astype(float)
    ic_mean = float(np.mean(ic_values))
    ic_std = float(np.std(ic_values, ddof=1))

    if ic_std == 0.0 or not math.isfinite(ic_std):
        return None

    ic_ir = ic_mean / ic_std
    if not math.isfinite(ic_ir):
        return None
    return ic_ir


# ---------------------------------------------------------------------------
# Layer evaluation
# ---------------------------------------------------------------------------


def _evaluate_candidate(
    ast: ASTNode,
    ctx: EvaluationContext,
    fwd_returns_df: pd.DataFrame,
    fitness_config: FitnessConfig,
) -> Candidate:
    """Evaluate one AST and return a Candidate.

    Evaluation errors produce a Candidate with None train_ic_ir and fitness.
    """
    can = canonical(ast)
    nc = node_count(ast)
    fam = classify(ast)
    cid = _expr_id(can)

    try:
        scores_df = evaluate(ast, ctx)
        train_ic_ir = _compute_ic_ir(scores_df, fwd_returns_df)
    except Exception:
        return Candidate(
            expr_id=cid,
            canonical=can,
            ast=ast,
            node_count=nc,
            family=fam,
            sources=["beam"],
            train_ic_ir=None,
            fitness=None,
            status="rejected_oos",
            oos_segments=[],
        )

    fit = fitness(train_ic_ir, nc, fitness_config.complexity_lambda)

    return Candidate(
        expr_id=cid,
        canonical=can,
        ast=ast,
        node_count=nc,
        family=fam,
        sources=["beam"],
        train_ic_ir=train_ic_ir,
        fitness=fit,
        status="pending",
        oos_segments=[],
    )


def _retain_top(
    candidates: list[Candidate],
    beam_width: int,
) -> list[Candidate]:
    """Retain up to ``beam_width`` eligible candidates by fitness descending.

    Eligibility: finite, non-None fitness (equivalently non-None train_ic_ir).
    Tie-breaking: ascending node_count, then ascending canonical string (R6.3).
    """
    eligible = [c for c in candidates if c.fitness is not None]
    eligible.sort(key=lambda c: sort_key((c.fitness, c.node_count, c.canonical)))
    return eligible[:beam_width]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_beam_search(
    config: SearchConfig,
    fitness_config: FitnessConfig,
    ctx: EvaluationContext,
    primary_horizon_days: int = 20,
) -> list[Candidate]:
    """Run beam search over the DSL grammar and return all retained candidates.

    Layer-by-layer growth from depth 1 to ``config.max_depth``.  At each
    layer, all new expressions are evaluated; the top ``config.beam_width``
    by fitness are retained for the next layer.  The final result is the
    union of retained candidates from every layer.

    Args:
        config: SearchConfig with beam_width, max_depth, seed.
        fitness_config: FitnessConfig with complexity_lambda.
        ctx: EvaluationContext pointing at the TRAIN window connection,
             start_date, and end_date.
        primary_horizon_days: Forward-return horizon for IC_IR computation.

    Returns:
        List of Candidate objects, one per retained expression across all
        layers, ordered by layer then descending fitness within each layer.
    """
    # Pre-compute forward returns once for the entire train window.
    fwd_map = compute_forward_returns(
        ctx.conn,
        start_date=ctx.start_date,
        end_date=ctx.end_date,
        horizons=(primary_horizon_days,),
    )
    fwd_returns_df = fwd_map[primary_horizon_days]

    # Layer 1: all 5 leaf nodes
    layer1_asts: list[ASTNode] = all_leaves()
    all_retained: list[Candidate] = []
    seen_canonicals: set[str] = set()

    # Evaluate layer 1
    layer1_candidates: list[Candidate] = []
    for ast in layer1_asts:
        can = canonical(ast)
        if can in seen_canonicals:
            continue
        seen_canonicals.add(can)
        cand = _evaluate_candidate(ast, ctx, fwd_returns_df, fitness_config)
        layer1_candidates.append(cand)


    retained1 = _retain_top(layer1_candidates, config.beam_width)
    all_retained.extend(retained1)

    if config.max_depth <= 1:
        return all_retained

    # Layers 2..max_depth
    beam_asts: list[ASTNode] = [c.ast for c in retained1]

    for _depth in range(2, config.max_depth + 1):
        if not beam_asts:
            break

        layer_candidates: list[Candidate] = []
        for ast in expand_layer(beam_asts):
            can = canonical(ast)
            if can in seen_canonicals:
                continue
            seen_canonicals.add(can)
            cand = _evaluate_candidate(ast, ctx, fwd_returns_df, fitness_config)
            layer_candidates.append(cand)

        retained = _retain_top(layer_candidates, config.beam_width)
        all_retained.extend(retained)
        beam_asts = [c.ast for c in retained]

    return all_retained
