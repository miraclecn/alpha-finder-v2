"""DSL parser: expression string → AST.

Parses the prefix/functional notation described in design.md:
  Leaf:    close_adj
  TSOp:    lag(close_adj, 20)
  CSOp:    cs_rank(close_adj)
  ArithOp: +(close_adj, open)   or  log(close_adj)

Returns either an AST node or a RejectionRecord on any violation.

Requirements: R2.5, R2.6, R2.7, R2.8, R2.9, R2.10, R2.12
"""

from __future__ import annotations

import re
from typing import Union

from alpha_find_v2.factor_lab.dsl.grammar import (
    ARITHMETIC_OPS,
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
from alpha_find_v2.factor_lab.dsl.validator import RejectionRecord

ParseResult = Union[ASTNode, RejectionRecord]

# All operator names that the grammar accepts.
_ALL_OPS: frozenset[str] = TIME_SERIES_OPS | CROSS_SECTION_OPS | ARITHMETIC_OPS

# Operators that look like identifiers (not punctuation like +, -, *, /)
_IDENT_OPS: frozenset[str] = frozenset(
    op for op in _ALL_OPS if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", op)
)


# ---------------------------------------------------------------------------
# Tokeniser
# ---------------------------------------------------------------------------

_TOKEN_RE = re.compile(
    r"""
    (?P<NUMBER>-?\d+)          |   # integer (possibly negative)
    (?P<IDENT>[a-zA-Z_][a-zA-Z0-9_]*)  |  # identifier or keyword
    (?P<OP>[+\-*/])            |   # arithmetic punctuation operators
    (?P<LPAREN>\()             |
    (?P<RPAREN>\))             |
    (?P<COMMA>,)               |
    (?P<WS>\s+)                    # whitespace (skipped)
""",
    re.VERBOSE,
)


def _tokenise(expr: str) -> list[tuple[str, str]]:
    """Return a list of (type, value) tokens, skipping whitespace."""
    tokens: list[tuple[str, str]] = []
    pos = 0
    while pos < len(expr):
        m = _TOKEN_RE.match(expr, pos)
        if m is None:
            # Unknown character — we will report this as an unknown-operator
            # when the parser fails to recognise it.
            tokens.append(("UNKNOWN", expr[pos]))
            pos += 1
        else:
            kind = m.lastgroup
            if kind != "WS":
                tokens.append((kind, m.group()))
            pos = m.end()
    return tokens


# ---------------------------------------------------------------------------
# Recursive-descent parser
# ---------------------------------------------------------------------------


class _Parser:
    """Stateful parser; create a new instance per expression string."""

    def __init__(self, tokens: list[tuple[str, str]], original: str) -> None:
        self._tokens = tokens
        self._pos = 0
        self._original = original

    # ------------------------------------------------------------------
    # Token helpers
    # ------------------------------------------------------------------

    def _peek(self) -> tuple[str, str] | None:
        if self._pos < len(self._tokens):
            return self._tokens[self._pos]
        return None

    def _consume(self) -> tuple[str, str]:
        tok = self._tokens[self._pos]
        self._pos += 1
        return tok

    def _at_end(self) -> bool:
        return self._pos >= len(self._tokens)

    # ------------------------------------------------------------------
    # Position helper: reconstruct a token position string for RejectionRecord
    # ------------------------------------------------------------------

    def _position_str(self, value: str) -> str:
        return value

    # ------------------------------------------------------------------
    # Main entry: parse one complete expression
    # ------------------------------------------------------------------

    def parse(self) -> ParseResult:
        result = self._parse_expr()
        if isinstance(result, RejectionRecord):
            return result
        if not self._at_end():
            # Trailing garbage
            leftover = "".join(v for _, v in self._tokens[self._pos :])
            return RejectionRecord(
                clause_number="R2.9",
                position=leftover,
                reason=f"Unexpected trailing tokens: {leftover!r}",
            )
        return result

    def _parse_expr(self) -> ParseResult:
        tok = self._peek()
        if tok is None:
            return RejectionRecord(
                clause_number="R2.9",
                position="<empty>",
                reason="Empty expression",
            )

        kind, value = tok

        # ----------------------------------------------------------------
        # UNKNOWN character → reject as outside whitelist
        # ----------------------------------------------------------------
        if kind == "UNKNOWN":
            self._consume()
            return RejectionRecord(
                clause_number="R2.9",
                position=value,
                reason=f"Unknown character {value!r}; not in operator or field whitelist",
            )

        # ----------------------------------------------------------------
        # Punctuation operator: +, -, *, /  (prefix form: op(left, right))
        # ----------------------------------------------------------------
        if kind == "OP":
            self._consume()
            op = value
            return self._parse_call(op, op_position=op)

        # ----------------------------------------------------------------
        # Identifier: operator or leaf field
        # ----------------------------------------------------------------
        if kind == "IDENT":
            self._consume()
            name = value

            # Check if followed by '(' — it's a function call
            next_tok = self._peek()
            if next_tok is not None and next_tok[0] == "LPAREN":
                return self._parse_call(name, op_position=name)

            # Otherwise it must be a leaf field
            if name in LEAF_FIELDS:
                return Leaf(name)

            # Unknown identifier not followed by '(' — reject
            return RejectionRecord(
                clause_number="R2.9",
                position=name,
                reason=f"Unknown identifier {name!r}; not in leaf fields whitelist and not a function call",
            )

        # ----------------------------------------------------------------
        # Number at top level — not a valid expression
        # ----------------------------------------------------------------
        if kind == "NUMBER":
            self._consume()
            return RejectionRecord(
                clause_number="R2.9",
                position=value,
                reason=f"Bare numeric literal {value!r} is not a valid expression",
            )

        # ----------------------------------------------------------------
        # Any other token type
        # ----------------------------------------------------------------
        self._consume()
        return RejectionRecord(
            clause_number="R2.9",
            position=value,
            reason=f"Unexpected token {value!r}",
        )

    def _parse_call(self, op: str, op_position: str) -> ParseResult:
        """Parse: op LPAREN arg [, arg ...] RPAREN."""
        # Consume '('
        tok = self._peek()
        if tok is None or tok[0] != "LPAREN":
            got = tok[1] if tok else "<end>"
            return RejectionRecord(
                clause_number="R2.9",
                position=op_position,
                reason=f"Expected '(' after {op!r}, got {got!r}",
            )
        self._consume()  # '('

        # Parse arguments
        args: list[ParseResult] = []
        while True:
            tok = self._peek()
            if tok is None:
                return RejectionRecord(
                    clause_number="R2.9",
                    position=op_position,
                    reason=f"Unclosed parenthesis for {op!r}",
                )
            if tok[0] == "RPAREN":
                self._consume()
                break
            if args:
                # Expect comma between args
                if tok[0] != "COMMA":
                    return RejectionRecord(
                        clause_number="R2.9",
                        position=op_position,
                        reason=f"Expected ',' between arguments of {op!r}, got {tok[1]!r}",
                    )
                self._consume()  # ','

            # Time-series ops: second arg is always a window integer literal
            if op in TIME_SERIES_OPS and len(args) == 1:
                win_result = self._parse_window(op)
                if isinstance(win_result, RejectionRecord):
                    return win_result
                args.append(win_result)
            else:
                arg = self._parse_expr()
                args.append(arg)

        # Check for RejectionRecord in args
        for arg in args:
            if isinstance(arg, RejectionRecord):
                return arg

        # ----------------------------------------------------------------
        # Validate op is in whitelist
        # ----------------------------------------------------------------
        if op in TIME_SERIES_OPS:
            return self._build_ts_op(op, op_position, args)  # type: ignore[arg-type]
        if op in CROSS_SECTION_OPS:
            return self._build_cs_op(op, op_position, args)  # type: ignore[arg-type]
        if op in ARITHMETIC_OPS:
            return self._build_arith_op(op, op_position, args)  # type: ignore[arg-type]

        # Unknown operator
        return RejectionRecord(
            clause_number="R2.9",
            position=op_position,
            reason=f"Operator {op!r} is not in the operator whitelist",
        )

    def _parse_window(self, op: str) -> int | RejectionRecord:
        """Parse a window size literal for a TS op (second argument)."""
        tok = self._peek()
        if tok is None:
            return RejectionRecord(
                clause_number="R2.5",
                position=f"{op}(..., <end>)",
                reason=f"Expected integer window literal as second argument of {op!r}",
            )
        kind, value = tok
        if kind != "NUMBER":
            # Could be an expression (sub-expression as window) → reject R2.5
            return RejectionRecord(
                clause_number="R2.5",
                position=value,
                reason=(
                    f"Window argument of {op!r} must be an integer literal, "
                    f"got {value!r}"
                ),
            )
        self._consume()
        try:
            n = int(value)
        except ValueError:
            return RejectionRecord(
                clause_number="R2.5",
                position=value,
                reason=f"Window argument {value!r} is not a valid integer",
            )
        if n <= 0:
            return RejectionRecord(
                clause_number="R2.5",
                position=value,
                reason=f"Window argument must be positive; got {n}",
            )
        if n not in WINDOW_WHITELIST:
            return RejectionRecord(
                clause_number="R2.5",
                position=value,
                reason=(
                    f"Window argument {n} is not in whitelist {sorted(WINDOW_WHITELIST)}"
                ),
            )
        return n

    # ------------------------------------------------------------------
    # AST construction with validation
    # ------------------------------------------------------------------

    def _build_ts_op(
        self, op: str, op_position: str, args: list[ASTNode | int]
    ) -> ParseResult:
        # TS ops require exactly 2 args: (operand, window)
        if len(args) != 2:  # noqa: PLR2004
            return RejectionRecord(
                clause_number="R2.12",
                position=op_position,
                reason=(
                    f"Time-series operator {op!r} requires exactly 2 arguments "
                    f"(operand + window); got {len(args)}"
                ),
            )
        operand = args[0]
        window = args[1]
        # operand must be ASTNode (window parse already validated window)
        if not isinstance(operand, (Leaf, TSOp, CSOp, ArithOp)):
            return RejectionRecord(
                clause_number="R2.12",
                position=op_position,
                reason=f"First argument of {op!r} must be an expression, got {operand!r}",
            )
        if not isinstance(window, int):
            return RejectionRecord(
                clause_number="R2.5",
                position=op_position,
                reason=f"Second argument of {op!r} must be an integer window, got {window!r}",
            )
        # R2.7: TS must not wrap CS (directly or transitively)
        rejection = _check_ts_wraps_cs(op, operand)
        if rejection is not None:
            return rejection
        return TSOp(op, operand, window)

    def _build_cs_op(
        self, op: str, op_position: str, args: list[ASTNode | int]
    ) -> ParseResult:
        # CS ops require exactly 1 arg
        if len(args) != 1:
            return RejectionRecord(
                clause_number="R2.12",
                position=op_position,
                reason=(
                    f"Cross-section operator {op!r} requires exactly 1 argument; "
                    f"got {len(args)}"
                ),
            )
        operand = args[0]
        if not isinstance(operand, (Leaf, TSOp, CSOp, ArithOp)):
            return RejectionRecord(
                clause_number="R2.12",
                position=op_position,
                reason=f"Argument of {op!r} must be an expression, got {operand!r}",
            )
        # CS wrapping TS is explicitly allowed (R2.8) — no check needed
        return CSOp(op, operand)

    def _build_arith_op(
        self, op: str, op_position: str, args: list[ASTNode | int]
    ) -> ParseResult:
        if op == "log":
            # Unary
            if len(args) != 1:
                return RejectionRecord(
                    clause_number="R2.12",
                    position=op_position,
                    reason=(
                        f"Operator 'log' requires exactly 1 argument; got {len(args)}"
                    ),
                )
            operand = args[0]
            if not isinstance(operand, (Leaf, TSOp, CSOp, ArithOp)):
                return RejectionRecord(
                    clause_number="R2.12",
                    position=op_position,
                    reason=f"Argument of 'log' must be an expression, got {operand!r}",
                )
            return ArithOp("log", operand)
        else:
            # Binary: +, -, *, /
            if len(args) != 2:  # noqa: PLR2004
                return RejectionRecord(
                    clause_number="R2.12",
                    position=op_position,
                    reason=(
                        f"Binary arithmetic operator {op!r} requires exactly 2 "
                        f"operands; got {len(args)}"
                    ),
                )
            left, right = args[0], args[1]
            if not isinstance(left, (Leaf, TSOp, CSOp, ArithOp)):
                return RejectionRecord(
                    clause_number="R2.12",
                    position=op_position,
                    reason=f"Left operand of {op!r} must be an expression, got {left!r}",
                )
            if not isinstance(right, (Leaf, TSOp, CSOp, ArithOp)):
                return RejectionRecord(
                    clause_number="R2.12",
                    position=op_position,
                    reason=f"Right operand of {op!r} must be an expression, got {right!r}",
                )
            return ArithOp(op, left, right)


# ---------------------------------------------------------------------------
# R2.7: TS-wraps-CS check (recursive)
# ---------------------------------------------------------------------------


def _contains_cs(ast: ASTNode) -> bool:
    """Return True if ast contains any cross-section operator node."""
    if isinstance(ast, Leaf):
        return False
    if isinstance(ast, CSOp):
        return True
    if isinstance(ast, TSOp):
        return _contains_cs(ast.operand)
    if isinstance(ast, ArithOp):
        has_right = ast.right is not None and _contains_cs(ast.right)
        return _contains_cs(ast.left) or has_right
    return False  # pragma: no cover


def _check_ts_wraps_cs(ts_op: str, operand: ASTNode) -> RejectionRecord | None:
    """Return RejectionRecord if operand transitively contains a CS op (R2.7)."""
    if _contains_cs(operand):
        return RejectionRecord(
            clause_number="R2.7",
            position=ts_op,
            reason=(
                f"Time-series operator {ts_op!r} wraps a cross-section operator, "
                "which would leak cross-date information"
            ),
        )
    return None


# ---------------------------------------------------------------------------
# R2.6: depth check
# ---------------------------------------------------------------------------


def _check_depth(ast: ASTNode) -> RejectionRecord | None:
    """Return RejectionRecord if node_count exceeds MAX_DEPTH (R2.6)."""
    count = node_count(ast)
    if count > MAX_DEPTH:
        return RejectionRecord(
            clause_number="R2.6",
            position=str(ast),
            reason=(
                f"Expression has {count} nodes, exceeding maximum of {MAX_DEPTH}"
            ),
        )
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse(expr: str) -> ParseResult:
    """Parse *expr* into an AST node, or return a RejectionRecord on error.

    Validates:
    - Operator and field whitelist (R2.9)
    - Arity (R2.12)
    - Window values (R2.5)
    - TS-wraps-CS composition (R2.7)
    - Depth limit (R2.6)

    Allows:
    - CS-wraps-TS composition (R2.8)
    """
    tokens = _tokenise(expr.strip())
    if not tokens:
        return RejectionRecord(
            clause_number="R2.9",
            position="<empty>",
            reason="Empty expression",
        )

    parser = _Parser(tokens, expr)
    result = parser.parse()
    if isinstance(result, RejectionRecord):
        return result

    # Post-parse depth check (R2.6)
    depth_rejection = _check_depth(result)
    if depth_rejection is not None:
        return depth_rejection

    return result
