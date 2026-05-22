"""Family classifier for DSL expression ASTs.

Implements the deterministic rule cascade defined in R4 clauses 2–7.

Returns one of {trend, volatility, volume, value, cross_momentum} or None
(rejected / unclassifiable).

Pure function of the AST — no side effects, no state.

Requirements: R4.1, R4.2, R4.3, R4.4, R4.5, R4.6, R4.7, R4.9, R4.11, R4.12
"""

from __future__ import annotations

from alpha_find_v2.factor_lab.dsl.grammar import ArithOp, ASTNode, CSOp, Leaf, TSOp

# The only valid return values (R4.1, R4.9 — "quality" is deliberately absent)
_VALID_FAMILIES: frozenset[str] = frozenset(
    {"trend", "volatility", "volume", "value", "cross_momentum"}
)


def _leaf_fields(ast: ASTNode) -> set[str]:
    """Recursively collect all leaf field names from the AST."""
    if isinstance(ast, Leaf):
        return {ast.field}
    if isinstance(ast, TSOp):
        return _leaf_fields(ast.operand)
    if isinstance(ast, CSOp):
        return _leaf_fields(ast.operand)
    if isinstance(ast, ArithOp):
        result = _leaf_fields(ast.left)
        if ast.right is not None:
            result = result | _leaf_fields(ast.right)
        return result
    return set()  # pragma: no cover


def _contains_op(ast: ASTNode, op_name: str) -> bool:
    """Recursively check if any node in the AST has op == op_name."""
    if isinstance(ast, Leaf):
        return False
    if isinstance(ast, TSOp):
        return ast.op == op_name or _contains_op(ast.operand, op_name)
    if isinstance(ast, CSOp):
        return ast.op == op_name or _contains_op(ast.operand, op_name)
    if isinstance(ast, ArithOp):
        if ast.op == op_name:
            return True
        if _contains_op(ast.left, op_name):
            return True
        if ast.right is not None and _contains_op(ast.right, op_name):
            return True
        return False
    return False  # pragma: no cover


def classify(ast: object) -> str | None:
    """Assign a family to the expression AST using the R4 rule cascade.

    Args:
        ast: A parsed ASTNode (Leaf, TSOp, CSOp, or ArithOp).  If the value
             is not a recognised AST node type (e.g. a bare string or a
             RejectionRecord was passed), returns None per R4.12.

    Returns:
        One of "trend", "volatility", "volume", "value", "cross_momentum",
        or None if no clause matches or the input is not a valid ASTNode.
        Never returns "quality" (R4.9).
    """
    # R4.12: not a valid ASTNode → reject
    if not isinstance(ast, (Leaf, TSOp, CSOp, ArithOp)):
        return None

    # R4.2: cs_rank or cs_zscore present → cross_momentum
    if _contains_op(ast, "cs_rank") or _contains_op(ast, "cs_zscore"):
        return "cross_momentum"

    # R4.3: leaf set is non-empty subset of {pe, pb} OR cs_industry_demean present → value
    leaves = _leaf_fields(ast)
    if (leaves and leaves.issubset({"pe", "pb"})) or _contains_op(ast, "cs_industry_demean"):
        return "value"

    # R4.4: turnover_value_cny in leaf set → volume
    if "turnover_value_cny" in leaves:
        return "volume"

    # R4.5: rolling_std present → volatility
    if _contains_op(ast, "rolling_std"):
        return "volatility"

    # R4.6: (delta or lag) present AND close_adj in leaf set → trend
    if (_contains_op(ast, "delta") or _contains_op(ast, "lag")) and "close_adj" in leaves:
        return "trend"

    # R4.7: none of the above → unclassifiable
    return None
