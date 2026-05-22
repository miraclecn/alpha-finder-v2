"""Expression generator for factor mining beam search and random sampler.

Provides two public functions:
- ``expand_layer``: enumerate valid one-operator extensions of parent ASTs
  (used by beam search to grow each layer).
- ``random_tree``: draw a complete syntactically valid AST uniformly at
  random from the grammar (used by the random sampler).

All stochastic choices in ``random_tree`` derive from an injected
``numpy.random.Generator`` (seeded from ``config.search.seed``) so that
results are reproducible (R3.8).

Grammar constraints enforced:
- R2.6: node_count ≤ MAX_DEPTH (5)
- R2.7: no TS operator wrapping a CS node (no TS-wraps-CS)
- R2.5: window values from WINDOW_WHITELIST only

Requirements: R3.8
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import numpy as np

from alpha_find_v2.factor_lab.dsl.grammar import (
    CROSS_SECTION_OPS,
    LEAF_FIELDS,
    MAX_DEPTH,
    TIME_SERIES_OPS,
    WINDOW_WHITELIST,
    ArithOp,
    ASTNode,
    CSOp,
    Leaf,
    TSOp,
    node_count,
)

if TYPE_CHECKING:
    pass

# Pre-sorted tuples for deterministic enumeration order
_SORTED_LEAF_FIELDS: tuple[str, ...] = tuple(sorted(LEAF_FIELDS))
_SORTED_TS_OPS: tuple[str, ...] = tuple(sorted(TIME_SERIES_OPS))
_SORTED_CS_OPS: tuple[str, ...] = tuple(sorted(CROSS_SECTION_OPS))
_SORTED_WINDOWS: tuple[int, ...] = tuple(sorted(WINDOW_WHITELIST))
_SORTED_BIN_OPS: tuple[str, ...] = tuple(sorted({"+", "-", "*", "/"}))


def all_leaves() -> list[Leaf]:
    """Return all 5 leaf nodes, in sorted field order."""
    return [Leaf(f) for f in _SORTED_LEAF_FIELDS]


def _contains_cs_op(ast: ASTNode) -> bool:
    """Return True if *ast* transitively contains any CS operator.

    This implements the R2.7 guard: a TS op must not wrap (directly or
    transitively) any CS op.
    """
    if isinstance(ast, Leaf):
        return False
    if isinstance(ast, TSOp):
        return _contains_cs_op(ast.operand)
    if isinstance(ast, CSOp):
        return True
    if isinstance(ast, ArithOp):
        if _contains_cs_op(ast.left):
            return True
        if ast.right is not None and _contains_cs_op(ast.right):
            return True
        return False
    return False  # pragma: no cover


def expand_layer(parents: list[ASTNode]) -> Iterator[ASTNode]:
    """Enumerate all valid one-operator extensions of each parent AST.

    For each parent, yields every tree formed by:
    1. Wrapping the parent in a TS op (all ops × all windows) — only if the
       parent contains no CS node (R2.7).
    2. Wrapping the parent in a CS op (always allowed per R2.8).
    3. Wrapping the parent in the unary ArithOp ``log``.
    4. Combining two parents with a binary ArithOp (+, -, *, /).

    Only expressions with ``node_count ≤ MAX_DEPTH`` are yielded (R2.6).

    Note: callers are responsible for deduplication; this function may yield
    structurally identical trees when two parents are equal.

    Args:
        parents: List of existing valid ASTNodes (the current beam layer).

    Yields:
        ASTNode instances that are valid one-op extensions.
    """
    for parent in parents:
        # 1. TS wraps (only allowed if parent contains no CS op — R2.7)
        if not _contains_cs_op(parent):
            for op in _SORTED_TS_OPS:
                for window in _SORTED_WINDOWS:
                    candidate = TSOp(op, parent, window)
                    if node_count(candidate) <= MAX_DEPTH:
                        yield candidate

        # 2. CS wraps (always valid — R2.8)
        for op in _SORTED_CS_OPS:
            candidate = CSOp(op, parent)
            if node_count(candidate) <= MAX_DEPTH:
                yield candidate

        # 3. Unary log
        candidate = ArithOp("log", parent)
        if node_count(candidate) <= MAX_DEPTH:
            yield candidate

    # 4. Binary ArithOps: all ordered pairs of parents (left × right)
    for left in parents:
        for right in parents:
            for op in _SORTED_BIN_OPS:
                candidate = ArithOp(op, left, right)
                if node_count(candidate) <= MAX_DEPTH:
                    yield candidate


# ---------------------------------------------------------------------------
# Random tree helpers
# ---------------------------------------------------------------------------

def _random_leaf(rng: np.random.Generator) -> Leaf:
    """Draw a uniformly random leaf."""
    idx = rng.integers(0, len(_SORTED_LEAF_FIELDS))
    return Leaf(_SORTED_LEAF_FIELDS[idx])


def _random_window(rng: np.random.Generator) -> int:
    """Draw a uniformly random window value."""
    idx = rng.integers(0, len(_SORTED_WINDOWS))
    return _SORTED_WINDOWS[idx]


def _random_ts_op(rng: np.random.Generator) -> str:
    """Draw a uniformly random TS operator name."""
    idx = rng.integers(0, len(_SORTED_TS_OPS))
    return _SORTED_TS_OPS[idx]


def _random_cs_op(rng: np.random.Generator) -> str:
    """Draw a uniformly random CS operator name."""
    idx = rng.integers(0, len(_SORTED_CS_OPS))
    return _SORTED_CS_OPS[idx]


def _random_bin_op(rng: np.random.Generator) -> str:
    """Draw a uniformly random binary arithmetic operator."""
    idx = rng.integers(0, len(_SORTED_BIN_OPS))
    return _SORTED_BIN_OPS[idx]


def _build_ts_safe_tree(rng: np.random.Generator, budget: int) -> ASTNode:
    """Build a random tree with node_count ≤ *budget* that contains no CS ops.

    Used for constructing the sub-tree that a TS op wraps (R2.7: TS must not
    wrap CS).

    budget=1 → always a leaf.
    budget≥2 → choose from {leaf, TSOp(sub), log(sub), binary_arith(sub, sub)},
               where each sub-tree is built with a smaller budget so the
               total node_count stays ≤ budget.
    """
    if budget <= 1:
        return _random_leaf(rng)

    # Available choices depend on budget
    choices: list[int] = [0]  # leaf always available
    if budget >= 2:
        choices += [1, 2]  # TSOp(sub), log(sub): cost 1 + sub_cost ≤ budget
    if budget >= 3:
        choices.append(3)  # binary: cost 1 + left + right ≤ budget

    choice = choices[int(rng.integers(0, len(choices)))]
    if choice == 0:
        return _random_leaf(rng)
    elif choice == 1:
        sub = _build_ts_safe_tree(rng, budget - 1)
        return TSOp(_random_ts_op(rng), sub, _random_window(rng))
    elif choice == 2:
        sub = _build_ts_safe_tree(rng, budget - 1)
        return ArithOp("log", sub)
    else:
        # Split remaining budget-1 nodes between left and right
        # left gets k nodes (1 ≤ k ≤ budget-2), right gets budget-1-k
        k = int(rng.integers(1, budget - 1))
        left = _build_ts_safe_tree(rng, k)
        right = _build_ts_safe_tree(rng, budget - 1 - k)
        return ArithOp(_random_bin_op(rng), left, right)


def _build_any_tree(rng: np.random.Generator, budget: int) -> ASTNode:
    """Build a random tree with node_count ≤ *budget*, obeying R2.7.

    budget=1 → always a leaf.
    budget≥2 → choose from {leaf, TSOp(ts_safe_sub), CSOp(any_sub),
                             log(any_sub), binary_arith(any_sub, any_sub)},
               where each sub-tree is built with a smaller budget so the
               total node_count stays ≤ budget.

    The TS branch uses ``_build_ts_safe_tree`` for its operand to guarantee
    no CS op appears inside the TS operand (R2.7).
    """
    if budget <= 1:
        return _random_leaf(rng)

    choices: list[int] = [0]  # leaf always available
    if budget >= 2:
        choices += [1, 2, 3]  # TSOp, CSOp, log — each adds 1 op
    if budget >= 3:
        choices.append(4)  # binary: needs at least 3 nodes (1 op + 2 leaves)

    choice = choices[int(rng.integers(0, len(choices)))]
    if choice == 0:
        return _random_leaf(rng)
    elif choice == 1:  # TSOp — operand must be CS-free
        sub = _build_ts_safe_tree(rng, budget - 1)
        return TSOp(_random_ts_op(rng), sub, _random_window(rng))
    elif choice == 2:  # CSOp
        sub = _build_any_tree(rng, budget - 1)
        return CSOp(_random_cs_op(rng), sub)
    elif choice == 3:  # log (unary)
        sub = _build_any_tree(rng, budget - 1)
        return ArithOp("log", sub)
    else:  # binary ArithOp
        k = int(rng.integers(1, budget - 1))
        left = _build_any_tree(rng, k)
        right = _build_any_tree(rng, budget - 1 - k)
        return ArithOp(_random_bin_op(rng), left, right)


def random_tree(rng: np.random.Generator, max_depth: int) -> ASTNode:
    """Draw a complete, syntactically valid AST uniformly from the grammar.

    Strategy: uniformly pick a node budget ``n`` in ``[1, max_depth]``, then
    recursively build a tree whose ``node_count ≤ n``.

    The ``max_depth`` parameter follows the R2.6 convention where "depth" is
    the maximum number of DSL nodes (operators + leaf references), not the
    tree height.  Callers should pass ``MAX_DEPTH`` (= 5) or a smaller value.

    The returned tree:
    - Has ``node_count ≤ max_depth``.
    - Satisfies R2.7 (no TS wrapping CS).
    - Uses only window values from WINDOW_WHITELIST (R2.5).

    Args:
        rng: A ``numpy.random.Generator`` instance (seeded externally, R3.8).
        max_depth: Maximum node count (inclusive). Must be ≥ 1.

    Returns:
        A valid ASTNode.
    """
    if max_depth < 1:
        raise ValueError(f"max_depth must be ≥ 1, got {max_depth}")
    budget = int(rng.integers(1, max_depth + 1))
    return _build_any_tree(rng, budget)
