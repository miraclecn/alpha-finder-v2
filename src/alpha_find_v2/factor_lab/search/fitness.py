"""Fitness function with complexity penalty for factor mining search.

fitness = train_IC_IR − λ × node_count  (R6.1)

NaN / infinite train_ic_ir → returns None (R6.2).
Tie-breaking sort key: (-fitness, node_count, canonical_string) (R6.3).

Requirements: R6.1, R6.2, R6.3
"""

from __future__ import annotations

import math


def fitness(
    train_ic_ir: float | None,
    node_count: int,
    lambda_: float = 0.05,
) -> float | None:
    """Return fitness score or None if train_ic_ir is invalid.

    Parameters
    ----------
    train_ic_ir:
        Train-set IC_IR. None, NaN, or infinite → returns None (R6.2).
    node_count:
        Number of DSL nodes in the expression tree. Bounded 1–5 by grammar.
    lambda_:
        Complexity penalty coefficient. Non-negative real in [0.0, 1.0].
        Defaults to 0.05.

    Returns
    -------
    float | None
        ``train_ic_ir - lambda_ * node_count``, or ``None`` when
        ``train_ic_ir`` is None, NaN, or infinite.
    """
    if train_ic_ir is None:
        return None
    if not math.isfinite(train_ic_ir):
        return None
    return train_ic_ir - lambda_ * node_count


def sort_key(candidate: tuple[float | None, int, str]) -> tuple:
    """Return sort key for (fitness, node_count, canonical_string).

    None fitness sorts last (worst).

    Ordering: higher fitness first, then ascending node_count,
    then ascending lexicographic canonical_string (R6.3).
    """
    fit, nc, cs = candidate
    if fit is None:
        # Place after all finite fitness values.
        return (1, 0, nc, cs)
    return (0, -fit, nc, cs)
