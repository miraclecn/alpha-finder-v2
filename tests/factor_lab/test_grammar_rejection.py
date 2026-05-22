"""Grammar rejection tests for the DSL parser.

Covers every banned composition in R2 clauses 5, 6, 7, 9, 12 with explicit
string inputs and asserted RejectionRecord clause numbers.  Also verifies that
valid expressions (one per family) parse without rejection, and uses PBT to
confirm that randomly generated invalid expressions always produce a structured
RejectionRecord.

**Validates: Requirements R2.5, R2.6, R2.7, R2.9, R2.10, R2.12**
"""

from __future__ import annotations

import unittest

from hypothesis import given, settings
from hypothesis import strategies as st

from alpha_find_v2.factor_lab.dsl.grammar import (
    ARITHMETIC_OPS,
    CROSS_SECTION_OPS,
    LEAF_FIELDS,
    TIME_SERIES_OPS,
    WINDOW_WHITELIST,
)
from alpha_find_v2.factor_lab.dsl.parser import parse
from alpha_find_v2.factor_lab.dsl.validator import RejectionRecord

_ALL_OPS = TIME_SERIES_OPS | CROSS_SECTION_OPS | ARITHMETIC_OPS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _rejected(tc: unittest.TestCase, expr: str, clause: str | None = None) -> RejectionRecord:
    result = parse(expr)
    tc.assertIsInstance(result, RejectionRecord, msg=f"{expr!r} should be rejected")
    assert isinstance(result, RejectionRecord)
    if clause is not None:
        tc.assertEqual(
            result.clause_number, clause, msg=f"Wrong clause for {expr!r}: {result}"
        )
    return result


def _accepted(tc: unittest.TestCase, expr: str) -> None:
    result = parse(expr)
    tc.assertNotIsInstance(
        result, RejectionRecord, msg=f"{expr!r} should be accepted, got {result!r}"
    )


# ---------------------------------------------------------------------------
# R2.5 — bad window values
# ---------------------------------------------------------------------------


class TestR25BadWindow(unittest.TestCase):
    """R2.5: window not in whitelist, zero, negative, or non-literal."""

    def test_window_7_not_in_whitelist(self) -> None:
        _rejected(self, "lag(close_adj, 7)", "R2.5")

    def test_window_0(self) -> None:
        _rejected(self, "lag(close_adj, 0)", "R2.5")

    def test_window_negative(self) -> None:
        _rejected(self, "lag(close_adj, -5)", "R2.5")

    def test_window_100_not_in_whitelist(self) -> None:
        _rejected(self, "rolling_mean(close_adj, 100)", "R2.5")

    def test_window_1_not_in_whitelist(self) -> None:
        _rejected(self, "delta(open, 1)", "R2.5")

    def test_window_is_field_expression(self) -> None:
        _rejected(self, "lag(close_adj, open)", "R2.5")


# ---------------------------------------------------------------------------
# R2.6 — depth > 5 nodes
# ---------------------------------------------------------------------------


class TestR26DepthExceeded(unittest.TestCase):
    """R2.6: more than 5 nodes."""

    def test_7_nodes_binary_nesting(self) -> None:
        # +(close_adj, +(open, +(pe, pb))) = 3 ArithOp + 4 Leaf = 7 nodes
        _rejected(self, "+(close_adj, +(open, +(pe, pb)))", "R2.6")

    def test_6_nodes_nested_log(self) -> None:
        # log(log(log(log(log(close_adj))))) = 5 ArithOp + 1 Leaf = 6 nodes
        _rejected(self, "log(log(log(log(log(close_adj)))))", "R2.6")


# ---------------------------------------------------------------------------
# R2.7 — TS wraps CS
# ---------------------------------------------------------------------------


class TestR27TSWrapsCS(unittest.TestCase):
    """R2.7: time-series operator wrapping cross-section operator."""

    def test_ts_directly_wraps_cs(self) -> None:
        _rejected(self, "lag(cs_rank(close_adj), 20)", "R2.7")

    def test_ts_wraps_cs_zscore(self) -> None:
        _rejected(self, "rolling_mean(cs_zscore(open), 5)", "R2.7")

    def test_ts_wraps_arith_containing_cs(self) -> None:
        # TS wraps ArithOp that transitively contains CS — still forbidden
        _rejected(self, "lag(+(cs_rank(close_adj), open), 5)", "R2.7")


# ---------------------------------------------------------------------------
# R2.8 — CS wraps TS (ALLOWED — no rejection)
# ---------------------------------------------------------------------------


class TestR28CSWrapsTSAllowed(unittest.TestCase):
    """R2.8: cross-section wrapping time-series must NOT be rejected."""

    def test_cs_rank_wraps_lag(self) -> None:
        _accepted(self, "cs_rank(lag(close_adj, 20))")

    def test_cs_zscore_wraps_rolling_mean(self) -> None:
        _accepted(self, "cs_zscore(rolling_mean(open, 5))")


# ---------------------------------------------------------------------------
# R2.9 — unknown operators / fields
# ---------------------------------------------------------------------------


class TestR29UnknownOps(unittest.TestCase):
    """R2.9: unknown operators, conditionals, user-defined functions, unknown fields."""

    def test_udf_rejected(self) -> None:
        _rejected(self, "my_udf(close_adj)", "R2.9")

    def test_conditional_rejected(self) -> None:
        _rejected(self, "if(close_adj, open, pe)", "R2.9")

    def test_unknown_leaf_rejected(self) -> None:
        _rejected(self, "volume", "R2.9")

    def test_typo_operator_rejected(self) -> None:
        _rejected(self, "lag_(close_adj, 20)", "R2.9")


# ---------------------------------------------------------------------------
# R2.12 — arity violations
# ---------------------------------------------------------------------------


class TestR212Arity(unittest.TestCase):
    """R2.12: arity violations for all operator families."""

    def test_ts_op_missing_window(self) -> None:
        # lag(close_adj) — 1 arg instead of 2; may surface as R2.12 or R2.5
        r = _rejected(self, "lag(close_adj)")
        self.assertIn(r.clause_number, {"R2.12", "R2.5"})

    def test_ts_op_extra_arg(self) -> None:
        _rejected(self, "lag(close_adj, 20, open)", "R2.12")

    def test_cs_op_zero_args(self) -> None:
        _rejected(self, "cs_rank()", "R2.12")

    def test_cs_op_two_args(self) -> None:
        _rejected(self, "cs_rank(close_adj, open)", "R2.12")

    def test_log_zero_args(self) -> None:
        _rejected(self, "log()", "R2.12")

    def test_log_two_args(self) -> None:
        _rejected(self, "log(close_adj, open)", "R2.12")

    def test_binary_arith_one_arg(self) -> None:
        _rejected(self, "+(close_adj)", "R2.12")

    def test_binary_arith_three_args(self) -> None:
        _rejected(self, "+(close_adj, open, pe)", "R2.12")


# ---------------------------------------------------------------------------
# Valid expressions — one per family, must NOT be rejected
# ---------------------------------------------------------------------------


class TestValidExpressions(unittest.TestCase):
    """One valid expression per expression family — must parse without rejection."""

    def test_leaf(self) -> None:
        _accepted(self, "close_adj")  # trend-ish bare leaf

    def test_ts_op(self) -> None:
        _accepted(self, "lag(close_adj, 20)")  # trend

    def test_cs_op(self) -> None:
        _accepted(self, "cs_rank(close_adj)")  # cross_momentum

    def test_arith_binary(self) -> None:
        _accepted(self, "+(close_adj, open)")  # trend/arith

    def test_log_value_family(self) -> None:
        _accepted(self, "log(pe)")  # value

    def test_cs_wraps_ts(self) -> None:
        # R2.8: CS wrapping TS is explicitly allowed
        _accepted(self, "cs_rank(lag(close_adj, 20))")


# ---------------------------------------------------------------------------
# PBT strategies
# ---------------------------------------------------------------------------

_ts_ops_st = st.sampled_from(sorted(TIME_SERIES_OPS))
_cs_ops_st = st.sampled_from(sorted(CROSS_SECTION_OPS))
_leaves_st = st.sampled_from(sorted(LEAF_FIELDS))
_windows_st = st.sampled_from(sorted(WINDOW_WHITELIST))
_bad_windows_st = st.integers().filter(lambda n: n not in WINDOW_WHITELIST)


@st.composite
def _invalid_expr_st(draw: st.DrawFn) -> str:
    """Build an expression string that violates at least one R2 rule."""
    choice = draw(st.integers(min_value=0, max_value=3))
    if choice == 0:
        # R2.5: window not in whitelist
        op = draw(_ts_ops_st)
        field = draw(_leaves_st)
        w = draw(_bad_windows_st)
        return f"{op}({field}, {w})"
    elif choice == 1:
        # R2.7: TS directly wraps CS
        ts_op = draw(_ts_ops_st)
        cs_op = draw(_cs_ops_st)
        field = draw(_leaves_st)
        w = draw(_windows_st)
        return f"{ts_op}({cs_op}({field}), {w})"
    elif choice == 2:
        # R2.9: unknown operator (identifier not in any whitelist)
        name = draw(
            st.text(
                alphabet="abcdefghijklmnopqrstuvwxyz_",
                min_size=2,
                max_size=12,
            ).filter(
                lambda s: s not in _ALL_OPS
                and s not in LEAF_FIELDS
                and s[0].isalpha()
            )
        )
        field = draw(_leaves_st)
        return f"{name}({field})"
    else:
        # R2.6: 6 nested log applications = 7 nodes > 5
        field = draw(_leaves_st)
        expr = field
        for _ in range(6):
            expr = f"log({expr})"
        return expr


# ---------------------------------------------------------------------------
# PBT tests
# ---------------------------------------------------------------------------


class TestPBTInvalidExpressions(unittest.TestCase):
    """PBT: randomly generated invalid expressions always produce a structured RejectionRecord."""

    @given(_invalid_expr_st())
    @settings(max_examples=300)
    def test_rejection_has_structured_record(self, expr: str) -> None:
        """**Validates: Requirements R2.5, R2.6, R2.7, R2.9, R2.10, R2.12**

        Every invalid expression must produce a RejectionRecord with a non-empty
        clause_number (starting with 'R2.'), position, and reason.
        """
        result = parse(expr)
        self.assertIsInstance(
            result,
            RejectionRecord,
            msg=f"Expected RejectionRecord for invalid {expr!r}, got {result!r}",
        )
        assert isinstance(result, RejectionRecord)
        self.assertTrue(
            result.clause_number.startswith("R2."),
            msg=f"clause_number must start with 'R2.', got {result.clause_number!r}",
        )
        self.assertTrue(result.position, msg="position must be non-empty")
        self.assertTrue(result.reason, msg="reason must be non-empty")


if __name__ == "__main__":
    unittest.main()
