"""Random sampler for factor mining — uniform draw from the DSL grammar.

Draws ``random_sample_size`` syntactically valid expressions uniformly
across depths 1..max_depth, evaluates each on the same train window as beam
search, and returns a list of Candidates with ``sources=["random"]``.

Deduplication is performed within the run: if the same canonical string is
drawn more than once (sampling is with replacement), only the first occurrence
is evaluated and returned.

Merge with beam results (R3.6) is handled downstream by merge.py (Task 11).
The R3.7 ``beam_underperforms_random`` warning requires beam acceptance rate
for comparison; it is therefore computed by the caller (run.py/merge.py) after
both streams are complete.

Requirements: R3.3, R3.4, R3.8
"""

from __future__ import annotations

import numpy as np

from alpha_find_v2.factor_evaluation.forward_returns import compute_forward_returns
from alpha_find_v2.factor_lab.config import FitnessConfig, SearchConfig
from alpha_find_v2.factor_lab.dsl.canonical import canonical
from alpha_find_v2.factor_lab.dsl.evaluator import EvaluationContext
from alpha_find_v2.factor_lab.search.beam import Candidate, _evaluate_candidate
from alpha_find_v2.factor_lab.search.expression_generator import random_tree


def run_random_sampling(
    config: SearchConfig,
    fitness_config: FitnessConfig,
    ctx: EvaluationContext,
    primary_horizon_days: int = 20,
    rng: np.random.Generator | None = None,
) -> list[Candidate]:
    """Draw random expressions from the grammar and evaluate each.

    Draws ``config.random_sample_size`` trees uniformly (with replacement)
    from depths 1..config.max_depth.  Duplicate canonical strings within
    the run are skipped (evaluated at most once).

    Args:
        config: SearchConfig — uses ``random_sample_size``, ``max_depth``,
                and ``seed`` (seed used only when ``rng`` is None).
        fitness_config: FitnessConfig with ``complexity_lambda``.
        ctx: EvaluationContext pointing at the train-window DuckDB connection.
        primary_horizon_days: Forward-return horizon for IC_IR.
        rng: Optional pre-seeded numpy Generator.  If None, a fresh Generator
             is created from ``config.seed`` (R3.8).  Pass the caller's RNG
             to ensure draws are independent from beam search draws.

    Returns:
        List of evaluated Candidate objects with ``sources=["random"]``.
        Length is at most ``config.random_sample_size``; may be shorter if
        duplicate canonical strings were drawn.
    """
    if rng is None:
        rng = np.random.default_rng(config.seed)

    # Pre-compute forward returns once for the entire window (R3.3).
    fwd_map = compute_forward_returns(
        ctx.conn,
        start_date=ctx.start_date,
        end_date=ctx.end_date,
        horizons=(primary_horizon_days,),
    )
    fwd_returns_df = fwd_map[primary_horizon_days]

    seen_canonicals: set[str] = set()
    results: list[Candidate] = []

    for _ in range(config.random_sample_size):
        ast = random_tree(rng, config.max_depth)
        can = canonical(ast)

        # Within-run dedup: skip if already seen.
        if can in seen_canonicals:
            continue
        seen_canonicals.add(can)

        cand = _evaluate_candidate(ast, ctx, fwd_returns_df, fitness_config)
        # Override sources — beam.py always stamps "beam"; we want "random" (R3.4).
        cand.sources = ["random"]
        results.append(cand)

    return results
