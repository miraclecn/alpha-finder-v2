"""DSL canonical string: AST → deterministic, normalized string.

Used as dict key for score-series caching across beam, random, walk-forward,
and dedup stages (design.md: canonical.py).

The canonical form is identical to the parse-round-trip string, i.e. it
re-serialises the AST in prefix/functional notation that the parser can
re-parse to the same AST.  Two ASTs are semantically equivalent iff their
canonical strings are equal.

Requirements: R11.3 (expression cache keyed by canonical string)
"""

from __future__ import annotations

from alpha_find_v2.factor_lab.dsl.grammar import ASTNode, ArithOp, CSOp, Leaf, TSOp


def canonical(ast: ASTNode) -> str:
    """Return a deterministic canonical string for *ast*.

    The string is in the same prefix/functional notation accepted by
    ``dsl.parser.parse``, so ``parse(canonical(ast)) == ast`` holds for all
    valid ASTs.

    Args:
        ast: A parsed, valid DSL AST node (``Leaf``, ``TSOp``, ``CSOp``, or
             ``ArithOp``).

    Returns:
        A non-empty string uniquely identifying the expression.

    Raises:
        TypeError: if *ast* is not a recognised node type.
    """
    if isinstance(ast, Leaf):
        return ast.field

    if isinstance(ast, TSOp):
        return f"{ast.op}({canonical(ast.operand)}, {ast.window})"

    if isinstance(ast, CSOp):
        return f"{ast.op}({canonical(ast.operand)})"

    if isinstance(ast, ArithOp):
        if ast.right is None:
            # Unary: only "log"
            return f"log({canonical(ast.left)})"
        return f"{ast.op}({canonical(ast.left)}, {canonical(ast.right)})"

    raise TypeError(f"Unknown AST node type: {type(ast)}")  # pragma: no cover
