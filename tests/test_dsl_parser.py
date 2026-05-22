"""Unit + property-based tests for the DSL parser and validator.

Verification target: Task 3 (R2.5, R2.6, R2.7, R2.8, R2.9, R2.10, R2.12)

**Validates: Requirements R2.5, R2.6, R2.7, R2.8, R2.9, R2.10, R2.12**
"""

from __future__ import annotations

import unittest

from hypothesis import given, settings
from hypothesis import strategies as st

from alpha_find_v2.factor_lab.dsl.grammar import (
    ARITHMETIC_OPS,
    CROSS_SECTION_OPS,
    LEAF_FIELDS,
    MAX_DEPTH,
    TIME_SERIES_OPS,
    WINDOW_WHITELIST,
    ArithOp,
    CSOp,
    Leaf,
    TSOp,
    node_count,
)
from alpha_find_v2.factor_lab.dsl.parser import parse
from alpha_find_v2.factor_lab.dsl.validator import RejectionRecord

# Concrete tuple for isinstance checks — ASTNode is a Union type alias
_AST_TYPES = (Leaf, TSOp, CSOp, ArithOp)

# ---------------------------------------------------------------------------
# Helpers: build canonical string from an AST (minimal inline version for
# round-trip verification — canonical.py may not exist yet in this task)
# ---------------------------------------------------------------------------

_BINARY_ARITH = frozenset({"+", "-", "*", "/"})


def _ast_to_str(ast: ASTNode) -> str:
    """Minimal AST → string in the same notation the parser accepts."""
    if isinstance(ast, Leaf):
        return ast.field
    if isinstance(ast, TSOp):
        return f"{ast.op}({_ast_to_str(ast.operand)}, {ast.window})"
    if isinstance(ast, CSOp):
        return f"{ast.op}({_ast_to_str(ast.operand)})"
    if isinstance(ast, ArithOp):
        if ast.right is None:
            return f"log({_ast_to_str(ast.left)})"
        return f"{ast.op}({_ast_to_str(ast.left)}, {_ast_to_str(ast.right)})"
    raise TypeError(f"Unknown node type: {type(ast)}")  # pragma: no cover


# ---------------------------------------------------------------------------
# Unit tests: valid expressions
# ---------------------------------------------------------------------------


class TestValidExpressions(unittest.TestCase):
    """Expressions that must parse successfully without rejection."""

    def _assert_valid(self, expr: str) -> ASTNode:
        result = parse(expr)
        self.assertNotIsInstance(
            result,
            RejectionRecord,
            msg=f"Expected valid parse of {expr!r}, got rejection: {result}",
        )
        return result  # type: ignore[return-value]

    def test_leaf(self) -> None:
        node = self._assert_valid("close_adj")
        self.assertEqual(node, Leaf("close_adj"))

    def test_all_leaves(self) -> None:
        for field in LEAF_FIELDS:
            self._assert_valid(field)

    def test_ts_op(self) -> None:
        node = self._assert_valid("lag(close_adj, 20)")
        self.assertEqual(node, TSOp("lag", Leaf("close_adj"), 20))

    def test_ts_op_all_windows(self) -> None:
        for w in WINDOW_WHITELIST:
            self._assert_valid(f"lag(close_adj, {w})")

    def test_cs_op(self) -> None:
        node = self._assert_valid("cs_rank(close_adj)")
        self.assertEqual(node, CSOp("cs_rank", Leaf("close_adj")))

    def test_arith_binary(self) -> None:
        node = self._assert_valid("+(close_adj, open)")
        self.assertEqual(node, ArithOp("+", Leaf("close_adj"), Leaf("open")))

    def test_arith_log(self) -> None:
        node = self._assert_valid("log(close_adj)")
        self.assertEqual(node, ArithOp("log", Leaf("close_adj")))

    def test_cs_wraps_ts_allowed(self) -> None:
        """R2.8: CS wrapping TS must be accepted."""
        node = self._assert_valid("cs_rank(lag(close_adj, 20))")
        self.assertEqual(node, CSOp("cs_rank", TSOp("lag", Leaf("close_adj"), 20)))

    def test_cs_wraps_ts_deep(self) -> None:
        # cs_rank(rolling_mean(close_adj, 60)) — allowed
        self._assert_valid("cs_rank(rolling_mean(close_adj, 60))")

    def test_arith_wraps_cs(self) -> None:
        # +(cs_rank(close_adj), open) — allowed, depth = 3
        self._assert_valid("+(cs_rank(close_adj), open)")

    def test_whitespace_tolerance(self) -> None:
        self._assert_valid("lag( close_adj , 20 )")

    def test_depth_exactly_5(self) -> None:
        # +(lag(close_adj, 5), +(rolling_mean(open, 10), pb))
        # nodes: + lag close_adj + rolling_mean open pb = 7 — too many
        # Build a tree with exactly 5 nodes:
        # +(+(close_adj, open), +(pe, pb)) → but that's 5:
        # ArithOp, ArithOp, Leaf, Leaf, ArithOp → wait, that's 7 total
        # Use: cs_rank(lag(close_adj, 20)) → 3 nodes
        # Use: +(lag(close_adj,5), cs_rank(rolling_mean(open, 10)))
        #   ArithOp(1) + TSOp(1) + Leaf(1) + CSOp(1) + TSOp(1) + Leaf(1) = 6 > 5
        # Use a depth-5 node count tree:
        # +(close_adj, +(open, +(pe, pb)))
        #   ArithOp + Leaf + ArithOp + Leaf + ArithOp + Leaf + Leaf = 7 > 5
        # 5-node: +(close_adj, +(open, log(pe)))
        #   ArithOp + Leaf + ArithOp + Leaf + ArithOp + Leaf = 6 > 5
        # Exactly 5: +(close_adj, +(open, pe))
        #   = ArithOp(1) Leaf(1) ArithOp(1) Leaf(1) Leaf(1) = 5
        node = self._assert_valid("+(close_adj, +(open, pe))")
        self.assertEqual(node_count(node), 5)


# ---------------------------------------------------------------------------
# Unit tests: rejection cases
# ---------------------------------------------------------------------------


class TestRejectionArityViolations(unittest.TestCase):
    """R2.12: arity violations → clause_number 'R2.12'."""

    def _assert_rejection(self, expr: str, clause: str) -> RejectionRecord:
        result = parse(expr)
        self.assertIsInstance(
            result,
            RejectionRecord,
            msg=f"Expected rejection for {expr!r}",
        )
        assert isinstance(result, RejectionRecord)
        self.assertEqual(
            result.clause_number,
            clause,
            msg=f"Expected clause {clause}, got {result.clause_number} for {expr!r}",
        )
        return result

    def test_ts_op_missing_window(self) -> None:
        # lag takes 2 args; giving 1 should violate arity
        # lag(close_adj) — only 1 arg
        result = parse("lag(close_adj)")
        self.assertIsInstance(result, RejectionRecord)
        assert isinstance(result, RejectionRecord)
        # Missing window arg: either R2.12 (arity) or R2.5 (no window provided)
        self.assertIn(result.clause_number, {"R2.12", "R2.5"})

    def test_ts_op_extra_arg(self) -> None:
        # lag(close_adj, 20, open) — 3 args for TS op
        self._assert_rejection("lag(close_adj, 20, open)", "R2.12")

    def test_cs_op_zero_args(self) -> None:
        # cs_rank() — 0 args
        self._assert_rejection("cs_rank()", "R2.12")

    def test_cs_op_two_args(self) -> None:
        # cs_rank(close_adj, open) — 2 args
        self._assert_rejection("cs_rank(close_adj, open)", "R2.12")

    def test_log_zero_args(self) -> None:
        self._assert_rejection("log()", "R2.12")

    def test_log_two_args(self) -> None:
        self._assert_rejection("log(close_adj, open)", "R2.12")

    def test_binary_arith_one_arg(self) -> None:
        self._assert_rejection("+(close_adj)", "R2.12")

    def test_binary_arith_three_args(self) -> None:
        self._assert_rejection("+(close_adj, open, pe)", "R2.12")


class TestRejectionUnknownOperators(unittest.TestCase):
    """R2.9: unknown operators/fields → clause_number 'R2.9'."""

    def _assert_r29(self, expr: str) -> None:
        result = parse(expr)
        self.assertIsInstance(result, RejectionRecord, msg=f"Expected rejection for {expr!r}")
        assert isinstance(result, RejectionRecord)
        self.assertEqual(result.clause_number, "R2.9", msg=f"Wrong clause for {expr!r}")

    def test_unknown_function(self) -> None:
        self._assert_r29("my_udf(close_adj)")

    def test_conditional(self) -> None:
        # Not in the grammar
        self._assert_r29("if(close_adj, open, pe)")

    def test_unknown_leaf(self) -> None:
        self._assert_r29("volume")

    def test_loop_syntax(self) -> None:
        self._assert_r29("for(close_adj)")

    def test_unknown_operator_typo(self) -> None:
        self._assert_r29("lag_(close_adj, 20)")


class TestRejectionBadWindow(unittest.TestCase):
    """R2.5: bad window values → clause_number 'R2.5'."""

    def _assert_r25(self, expr: str) -> None:
        result = parse(expr)
        self.assertIsInstance(result, RejectionRecord, msg=f"Expected rejection for {expr!r}")
        assert isinstance(result, RejectionRecord)
        self.assertEqual(result.clause_number, "R2.5", msg=f"Wrong clause for {expr!r}: {result}")

    def test_window_not_in_whitelist(self) -> None:
        self._assert_r25("lag(close_adj, 7)")

    def test_window_zero(self) -> None:
        self._assert_r25("lag(close_adj, 0)")

    def test_window_negative(self) -> None:
        self._assert_r25("lag(close_adj, -5)")

    def test_window_non_integer_expression(self) -> None:
        # Using a field name as the window argument
        self._assert_r25("lag(close_adj, open)")

    def test_window_not_in_whitelist_large(self) -> None:
        self._assert_r25("rolling_mean(close_adj, 100)")

    def test_window_not_in_whitelist_1(self) -> None:
        self._assert_r25("delta(open, 1)")


class TestRejectionDepthExceeded(unittest.TestCase):
    """R2.6: depth > 5 nodes → clause_number 'R2.6'."""

    def test_six_nodes(self) -> None:
        # +(close_adj, +(open, +(pe, pb))) = 3 ArithOp + 4 Leaf = 7 nodes
        result = parse("+(close_adj, +(open, +(pe, pb)))")
        self.assertIsInstance(result, RejectionRecord)
        assert isinstance(result, RejectionRecord)
        self.assertEqual(result.clause_number, "R2.6")

    def test_deep_nested(self) -> None:
        # log(log(log(log(log(close_adj))))) = 6 nodes
        result = parse("log(log(log(log(log(close_adj)))))")
        self.assertIsInstance(result, RejectionRecord)
        assert isinstance(result, RejectionRecord)
        self.assertEqual(result.clause_number, "R2.6")


class TestRejectionTSWrapsCS(unittest.TestCase):
    """R2.7: TS wrapping CS → clause_number 'R2.7'."""

    def test_ts_direct_wraps_cs(self) -> None:
        # lag(cs_rank(close_adj), 20) — TS directly wrapping CS
        result = parse("lag(cs_rank(close_adj), 20)")
        self.assertIsInstance(result, RejectionRecord)
        assert isinstance(result, RejectionRecord)
        self.assertEqual(result.clause_number, "R2.7")

    def test_ts_transitively_wraps_cs(self) -> None:
        # rolling_mean(+(cs_rank(close_adj), open), 60)
        # TS wraps ArithOp which contains CS → also forbidden
        result = parse("rolling_mean(+(cs_rank(close_adj), open), 60)")
        self.assertIsInstance(result, RejectionRecord)
        assert isinstance(result, RejectionRecord)
        self.assertEqual(result.clause_number, "R2.7")

    def test_cs_wraps_ts_allowed(self) -> None:
        """R2.8: this direction is explicitly allowed."""
        result = parse("cs_rank(lag(close_adj, 20))")
        self.assertNotIsInstance(result, RejectionRecord)


# ---------------------------------------------------------------------------
# Round-trip unit tests
# ---------------------------------------------------------------------------


class TestRoundTrip(unittest.TestCase):
    """Valid expressions parse and serialise back to equivalent form."""

    def _round_trip(self, expr: str) -> None:
        result = parse(expr)
        self.assertNotIsInstance(result, RejectionRecord, msg=f"Expected valid parse of {expr!r}")
        assert isinstance(result, _AST_TYPES)
        serialised = _ast_to_str(result)
        result2 = parse(serialised)
        self.assertNotIsInstance(
            result2, RejectionRecord,
            msg=f"Re-parse of serialised {serialised!r} failed: {result2}",
        )
        self.assertEqual(result, result2, msg=f"Round-trip mismatch for {expr!r}")

    def test_leaf_round_trip(self) -> None:
        self._round_trip("close_adj")

    def test_ts_op_round_trip(self) -> None:
        self._round_trip("lag(close_adj, 20)")

    def test_cs_op_round_trip(self) -> None:
        self._round_trip("cs_rank(open)")

    def test_arith_binary_round_trip(self) -> None:
        self._round_trip("+(close_adj, open)")

    def test_log_round_trip(self) -> None:
        self._round_trip("log(pe)")

    def test_cs_wraps_ts_round_trip(self) -> None:
        self._round_trip("cs_rank(lag(close_adj, 20))")

    def test_nested_arith_round_trip(self) -> None:
        self._round_trip("*(lag(close_adj, 5), cs_zscore(open))")


# ---------------------------------------------------------------------------
# Property-based tests (Hypothesis)
# ---------------------------------------------------------------------------

# Strategies for building valid expression strings

_leaves = st.sampled_from(sorted(LEAF_FIELDS))
_windows = st.sampled_from(sorted(WINDOW_WHITELIST))
_ts_ops = st.sampled_from(sorted(TIME_SERIES_OPS))
_cs_ops = st.sampled_from(sorted(CROSS_SECTION_OPS))
_bin_arith_ops = st.sampled_from(sorted(_BINARY_ARITH))


def _valid_expr_strategy(max_depth: int = 3) -> st.SearchStrategy[str]:
    """Build a valid DSL expression string, obeying all grammar constraints.

    Note: expressions built here are always valid by construction, so the
    parser must never return a RejectionRecord for them.

    CS-wraps-TS is allowed; TS-wraps-CS is prohibited by construction.
    """
    # A "TS-safe" sub-expression: contains no CS ops at any level.
    # This is needed because a TS op's operand must not contain CS.
    def ts_safe_expr(depth: int) -> st.SearchStrategy[str]:
        if depth <= 1:
            return _leaves
        return st.one_of(
            _leaves,
            # TS wrapping TS-safe is OK
            st.builds(
                lambda op, operand, w: f"{op}({operand}, {w})",
                _ts_ops,
                ts_safe_expr(depth - 1),
                _windows,
            ),
            # ArithOp over TS-safe operands
            st.builds(
                lambda op, l, r: f"{op}({l}, {r})",
                _bin_arith_ops,
                ts_safe_expr(depth - 1),
                ts_safe_expr(depth - 1),
            ),
            st.builds(
                lambda l: f"log({l})",
                ts_safe_expr(depth - 1),
            ),
        )

    def any_expr(depth: int) -> st.SearchStrategy[str]:
        if depth <= 1:
            return _leaves
        ts_safe = ts_safe_expr(depth - 1)
        any_sub = any_expr(depth - 1)
        return st.one_of(
            _leaves,
            # TS op: operand must be TS-safe (no CS inside)
            st.builds(
                lambda op, operand, w: f"{op}({operand}, {w})",
                _ts_ops,
                ts_safe,
                _windows,
            ),
            # CS op: operand can be anything (including TS)
            st.builds(
                lambda op, operand: f"{op}({operand})",
                _cs_ops,
                any_sub,
            ),
            # ArithOp binary
            st.builds(
                lambda op, l, r: f"{op}({l}, {r})",
                _bin_arith_ops,
                any_sub,
                any_sub,
            ),
            # log
            st.builds(
                lambda l: f"log({l})",
                any_sub,
            ),
        )

    return any_expr(max_depth)


def _invalid_window_expr_strategy() -> st.SearchStrategy[str]:
    """Build expressions that have an invalid window (not in whitelist)."""
    bad_windows = st.integers().filter(lambda n: n not in WINDOW_WHITELIST)
    return st.builds(
        lambda op, field, w: f"{op}({field}, {w})",
        _ts_ops,
        _leaves,
        bad_windows,
    )


def _ts_wraps_cs_expr_strategy() -> st.SearchStrategy[str]:
    """Build expressions where TS directly wraps CS."""
    return st.builds(
        lambda ts_op, cs_op, field, w: f"{ts_op}({cs_op}({field}), {w})",
        _ts_ops,
        _cs_ops,
        _leaves,
        _windows,
    )


def _unknown_operator_expr_strategy() -> st.SearchStrategy[str]:
    """Build expressions with an unknown function name."""
    known = TIME_SERIES_OPS | CROSS_SECTION_OPS | ARITHMETIC_OPS
    # Simple identifiers that start with a letter but are not in any whitelist
    unknown_names = st.text(
        alphabet="abcdefghijklmnopqrstuvwxyz_",
        min_size=2,
        max_size=12,
    ).filter(lambda s: s not in known and s not in LEAF_FIELDS and s[0].isalpha())
    return st.builds(
        lambda name, field: f"{name}({field})",
        unknown_names,
        _leaves,
    )


class TestPBTValidExpressions(unittest.TestCase):
    """PBT: randomly generated valid expressions must always parse successfully."""

    @given(_valid_expr_strategy(max_depth=3))
    @settings(max_examples=200)
    def test_valid_expr_never_rejected(self, expr: str) -> None:
        """**Validates: Requirements R2.5, R2.6, R2.7, R2.8, R2.9, R2.12**

        Any expression built by the valid strategy must parse without rejection,
        UNLESS the expression exceeds the depth limit (which the strategy may
        occasionally produce at depth 3 due to binary composition — such
        expressions are legitimately rejected with R2.6 and are skipped here).
        """
        result = parse(expr)
        if isinstance(result, RejectionRecord):
            # The only legitimate rejection for a structurally-valid expression
            # is a depth violation (R2.6) caused by combining two deep subtrees.
            self.assertEqual(
                result.clause_number,
                "R2.6",
                msg=(
                    f"Valid-strategy expression {expr!r} was rejected with "
                    f"unexpected clause {result.clause_number!r}: {result.reason}"
                ),
            )

    @given(_valid_expr_strategy(max_depth=2))
    @settings(max_examples=200)
    def test_valid_expr_round_trips(self, expr: str) -> None:
        """**Validates: Requirements R2.9, R2.10**

        Valid expressions (depth ≤ 2, guaranteed ≤ 5 nodes) parse, serialise,
        and re-parse to the identical AST.
        """
        result = parse(expr)
        if isinstance(result, RejectionRecord):
            # depth-2 strategy might still hit 5-node limit in rare binary cases;
            # only R2.6 is acceptable here
            self.assertEqual(result.clause_number, "R2.6")
            return
        serialised = _ast_to_str(result)
        result2 = parse(serialised)
        self.assertNotIsInstance(
            result2,
            RejectionRecord,
            msg=f"Re-parse of {serialised!r} failed: {result2}",
        )
        self.assertEqual(
            result,
            result2,
            msg=f"Round-trip mismatch: original={expr!r} → {result}, "
            f"serialised={serialised!r} → {result2}",
        )


class TestPBTInvalidWindowRejection(unittest.TestCase):
    """PBT: expressions with bad windows must always emit a RejectionRecord with R2.5."""

    @given(_invalid_window_expr_strategy())
    @settings(max_examples=200)
    def test_bad_window_rejected_with_r25(self, expr: str) -> None:
        """**Validates: Requirements R2.5, R2.10**

        Every expression with a window outside the whitelist must be rejected
        with clause_number 'R2.5', and the result must be a RejectionRecord.
        """
        result = parse(expr)
        self.assertIsInstance(
            result,
            RejectionRecord,
            msg=f"Expression with bad window {expr!r} was not rejected",
        )
        assert isinstance(result, RejectionRecord)
        self.assertEqual(
            result.clause_number,
            "R2.5",
            msg=f"Expected R2.5, got {result.clause_number} for {expr!r}",
        )
        # Sanity: RejectionRecord must have non-empty reason
        self.assertTrue(result.reason, msg="RejectionRecord.reason must be non-empty")
        self.assertTrue(result.position, msg="RejectionRecord.position must be non-empty")


class TestPBTTSWrapsCSRejection(unittest.TestCase):
    """PBT: expressions where TS directly wraps CS must always emit R2.7."""

    @given(_ts_wraps_cs_expr_strategy())
    @settings(max_examples=200)
    def test_ts_wraps_cs_rejected_with_r27(self, expr: str) -> None:
        """**Validates: Requirements R2.7, R2.10**

        Every expression where a TS op directly wraps a CS op must be
        rejected with clause_number 'R2.7'.
        """
        result = parse(expr)
        self.assertIsInstance(
            result,
            RejectionRecord,
            msg=f"TS-wraps-CS expression {expr!r} was not rejected",
        )
        assert isinstance(result, RejectionRecord)
        self.assertEqual(
            result.clause_number,
            "R2.7",
            msg=f"Expected R2.7, got {result.clause_number} for {expr!r}",
        )


class TestPBTUnknownOperatorRejection(unittest.TestCase):
    """PBT: expressions with unknown function names must always emit R2.9."""

    @given(_unknown_operator_expr_strategy())
    @settings(max_examples=200)
    def test_unknown_operator_rejected_with_r29(self, expr: str) -> None:
        """**Validates: Requirements R2.9, R2.10**

        Every expression with an unknown function name must be rejected
        with clause_number 'R2.9'.
        """
        result = parse(expr)
        self.assertIsInstance(
            result,
            RejectionRecord,
            msg=f"Unknown-operator expression {expr!r} was not rejected",
        )
        assert isinstance(result, RejectionRecord)
        self.assertEqual(
            result.clause_number,
            "R2.9",
            msg=f"Expected R2.9, got {result.clause_number} for {expr!r}",
        )


class TestPBTRejectionRecordStructure(unittest.TestCase):
    """PBT: any rejection must produce a structurally complete RejectionRecord."""

    @given(
        st.one_of(
            _invalid_window_expr_strategy(),
            _ts_wraps_cs_expr_strategy(),
            _unknown_operator_expr_strategy(),
        )
    )
    @settings(max_examples=300)
    def test_rejection_has_all_fields(self, expr: str) -> None:
        """**Validates: Requirements R2.10**

        Any rejection must emit a RejectionRecord with non-empty clause_number,
        position, and reason.
        """
        result = parse(expr)
        self.assertIsInstance(result, RejectionRecord)
        assert isinstance(result, RejectionRecord)
        self.assertIsInstance(result.clause_number, str)
        self.assertTrue(result.clause_number.startswith("R2."), msg=f"clause_number must start with R2., got {result.clause_number!r}")
        self.assertIsInstance(result.position, str)
        self.assertIsInstance(result.reason, str)
        self.assertTrue(result.reason, "reason must be non-empty")


if __name__ == "__main__":
    unittest.main()
