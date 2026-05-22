"""DSL grammar constants and AST node definitions.

Whitelists are frozen so callers cannot accidentally mutate them.
AST nodes are frozen dataclasses (hashable, cacheable — D4).

Requirements: R2.1–R2.6, R2.12
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Union

# ---------------------------------------------------------------------------
# Operator / field whitelists (R2.1–R2.5)
# ---------------------------------------------------------------------------

TIME_SERIES_OPS: frozenset[str] = frozenset(
    {"lag", "delta", "rolling_mean", "rolling_std", "rolling_max", "rolling_min"}
)

CROSS_SECTION_OPS: frozenset[str] = frozenset(
    {"cs_rank", "cs_zscore", "cs_demean", "cs_industry_demean"}
)

ARITHMETIC_OPS: frozenset[str] = frozenset({"+", "-", "*", "/", "log"})

LEAF_FIELDS: frozenset[str] = frozenset(
    {"close_adj", "open", "turnover_value_cny", "pe", "pb"}
)

WINDOW_WHITELIST: frozenset[int] = frozenset({5, 10, 20, 60, 120, 250})

# Maximum expression-tree depth (R2.6)
MAX_DEPTH: int = 5

# ---------------------------------------------------------------------------
# AST node types (R2.12, D4)
# ---------------------------------------------------------------------------

# Forward reference for recursive type alias
ASTNode = Union["Leaf", "TSOp", "CSOp", "ArithOp"]


@dataclass(frozen=True)
class Leaf:
    """A leaf field reference (e.g. close_adj).

    field must be one of LEAF_FIELDS.
    """

    field: str


@dataclass(frozen=True)
class TSOp:
    """A time-series operator application: op(operand, window).

    op must be one of TIME_SERIES_OPS.
    window must be one of WINDOW_WHITELIST.
    """

    op: str
    operand: ASTNode
    window: int


@dataclass(frozen=True)
class CSOp:
    """A cross-section operator application: op(operand).

    op must be one of CROSS_SECTION_OPS.
    """

    op: str
    operand: ASTNode


@dataclass(frozen=True)
class ArithOp:
    """An arithmetic operator application.

    Binary: op(left, right) for op in {+, -, *, /}
    Unary:  op(left)         for op == log
    right is None for log.
    """

    op: str
    left: ASTNode
    right: ASTNode | None = None


# ---------------------------------------------------------------------------
# node_count (R2.6, R6.1)
# ---------------------------------------------------------------------------


def node_count(ast: ASTNode) -> int:
    """Count operator applications and leaf field references.

    Window-size literals inside TSOp are NOT counted (R2.6).
    """
    if isinstance(ast, Leaf):
        return 1
    if isinstance(ast, TSOp):
        return 1 + node_count(ast.operand)
    if isinstance(ast, CSOp):
        return 1 + node_count(ast.operand)
    if isinstance(ast, ArithOp):
        right_count = node_count(ast.right) if ast.right is not None else 0
        return 1 + node_count(ast.left) + right_count
    raise TypeError(f"Unknown AST node type: {type(ast)}")  # pragma: no cover
