"""Unit + property-based tests for the family classifier.

Verification target: Task 5 (R4.1–R4.7, R4.9, R4.11, R4.12)

**Validates: Requirements R4.1, R4.2, R4.3, R4.4, R4.5, R4.6, R4.7, R4.9, R4.11, R4.12**
"""

from __future__ import annotations

import unittest

from hypothesis import given, settings
from hypothesis import strategies as st

from alpha_find_v2.factor_lab.dsl.grammar import (
    LEAF_FIELDS,
    TIME_SERIES_OPS,
    WINDOW_WHITELIST,
    ArithOp,
    CSOp,
    Leaf,
    TSOp,
)
from alpha_find_v2.factor_lab.family import classify, _leaf_fields, _contains_op


# ---------------------------------------------------------------------------
# Unit tests: all 5 families
# ---------------------------------------------------------------------------


class TestCrossMomentumFamily(unittest.TestCase):
    """R4.2: cs_rank or cs_zscore → cross_momentum."""

    def test_cs_rank_leaf(self) -> None:
        ast = CSOp("cs_rank", Leaf("close_adj"))
        self.assertEqual(classify(ast), "cross_momentum")

    def test_cs_zscore_leaf(self) -> None:
        ast = CSOp("cs_zscore", Leaf("open"))
        self.assertEqual(classify(ast), "cross_momentum")

    def test_cs_rank_wrapping_ts(self) -> None:
        ast = CSOp("cs_rank", TSOp("lag", Leaf("close_adj"), 20))
        self.assertEqual(classify(ast), "cross_momentum")

    def test_cs_zscore_wrapping_ts(self) -> None:
        ast = CSOp("cs_zscore", TSOp("rolling_mean", Leaf("close_adj"), 60))
        self.assertEqual(classify(ast), "cross_momentum")

    def test_cs_rank_inside_arith(self) -> None:
        # +(cs_rank(close_adj), open)
        ast = ArithOp("+", CSOp("cs_rank", Leaf("close_adj")), Leaf("open"))
        self.assertEqual(classify(ast), "cross_momentum")

    def test_cs_zscore_takes_priority_over_value_leaves(self) -> None:
        # cs_zscore with pe leaf — R4.2 wins over R4.3
        ast = CSOp("cs_zscore", Leaf("pe"))
        self.assertEqual(classify(ast), "cross_momentum")


class TestValueFamily(unittest.TestCase):
    """R4.3: leaf ⊆ {pe, pb} (non-empty) OR cs_industry_demean → value."""

    def test_pe_only(self) -> None:
        ast = Leaf("pe")
        self.assertEqual(classify(ast), "value")

    def test_pb_only(self) -> None:
        ast = Leaf("pb")
        self.assertEqual(classify(ast), "value")

    def test_pe_and_pb(self) -> None:
        ast = ArithOp("/", Leaf("pe"), Leaf("pb"))
        self.assertEqual(classify(ast), "value")

    def test_cs_industry_demean_with_close(self) -> None:
        # cs_industry_demean is present → value even with close_adj leaf
        ast = CSOp("cs_industry_demean", Leaf("close_adj"))
        self.assertEqual(classify(ast), "value")

    def test_cs_industry_demean_with_pe(self) -> None:
        ast = CSOp("cs_industry_demean", Leaf("pe"))
        self.assertEqual(classify(ast), "value")

    def test_cs_industry_demean_inside_arith(self) -> None:
        ast = ArithOp("+", CSOp("cs_industry_demean", Leaf("close_adj")), Leaf("open"))
        self.assertEqual(classify(ast), "value")

    def test_value_not_triggered_by_empty_leaves(self) -> None:
        # R4.3 requires non-empty leaf set that is a subset of {pe, pb}.
        # A tree with only pe qualifies; mixed with other leaves does not.
        ast = ArithOp("+", Leaf("pe"), Leaf("open"))
        # leaf set = {pe, open} which is NOT a subset of {pe, pb}
        # cs_industry_demean also absent → falls through to later clauses
        # open alone doesn't match volume/volatility/trend → None
        self.assertIsNone(classify(ast))

    def test_value_takes_priority_over_volume(self) -> None:
        # pe with cs_industry_demean: value wins over turnover_value_cny check
        ast = CSOp("cs_industry_demean", Leaf("turnover_value_cny"))
        self.assertEqual(classify(ast), "value")


class TestVolumeFamily(unittest.TestCase):
    """R4.4: turnover_value_cny in leaf set AND clauses 2/3 don't apply → volume."""

    def test_turnover_leaf_direct(self) -> None:
        ast = Leaf("turnover_value_cny")
        self.assertEqual(classify(ast), "volume")

    def test_turnover_in_ts_op(self) -> None:
        ast = TSOp("rolling_mean", Leaf("turnover_value_cny"), 20)
        self.assertEqual(classify(ast), "volume")

    def test_turnover_in_arith(self) -> None:
        ast = ArithOp("/", Leaf("turnover_value_cny"), Leaf("close_adj"))
        self.assertEqual(classify(ast), "volume")

    def test_turnover_wrapped_in_cs_demean(self) -> None:
        # cs_demean is not cs_rank, cs_zscore, or cs_industry_demean
        # → clause 2 does not apply; clause 3 does not apply;
        # turnover_value_cny is in leaves → volume
        ast = CSOp("cs_demean", Leaf("turnover_value_cny"))
        self.assertEqual(classify(ast), "volume")


class TestVolatilityFamily(unittest.TestCase):
    """R4.5: rolling_std present AND clauses 2/3/4 don't apply → volatility."""

    def test_rolling_std_close_adj(self) -> None:
        ast = TSOp("rolling_std", Leaf("close_adj"), 20)
        self.assertEqual(classify(ast), "volatility")

    def test_rolling_std_open(self) -> None:
        ast = TSOp("rolling_std", Leaf("open"), 60)
        self.assertEqual(classify(ast), "volatility")

    def test_rolling_std_inside_arith(self) -> None:
        ast = ArithOp("/", TSOp("rolling_std", Leaf("close_adj"), 20), Leaf("open"))
        self.assertEqual(classify(ast), "volatility")

    def test_rolling_std_with_cs_demean_no_turnover_no_pe_pb(self) -> None:
        # cs_demean(rolling_std(close_adj, 20)) — neither cs_rank nor cs_zscore
        # leaf = {close_adj} not ⊆ {pe, pb}, no cs_industry_demean
        # no turnover_value_cny → volume doesn't apply
        # rolling_std present → volatility
        ast = CSOp("cs_demean", TSOp("rolling_std", Leaf("close_adj"), 20))
        self.assertEqual(classify(ast), "volatility")


class TestTrendFamily(unittest.TestCase):
    """R4.6: (delta or lag) AND close_adj in leaves AND clauses 2–5 don't apply → trend."""

    def test_lag_close_adj(self) -> None:
        ast = TSOp("lag", Leaf("close_adj"), 20)
        self.assertEqual(classify(ast), "trend")

    def test_delta_close_adj(self) -> None:
        ast = TSOp("delta", Leaf("close_adj"), 5)
        self.assertEqual(classify(ast), "trend")

    def test_lag_close_adj_arith(self) -> None:
        ast = ArithOp("/", TSOp("lag", Leaf("close_adj"), 5), Leaf("close_adj"))
        self.assertEqual(classify(ast), "trend")

    def test_delta_inside_arith_with_close(self) -> None:
        ast = ArithOp("-", TSOp("delta", Leaf("close_adj"), 10), Leaf("open"))
        self.assertEqual(classify(ast), "trend")

    def test_lag_without_close_adj_returns_none(self) -> None:
        # lag(open, 20) — lag present but close_adj absent → reject
        ast = TSOp("lag", Leaf("open"), 20)
        self.assertIsNone(classify(ast))

    def test_delta_without_close_adj_returns_none(self) -> None:
        ast = TSOp("delta", Leaf("open"), 5)
        self.assertIsNone(classify(ast))


class TestRejectCase(unittest.TestCase):
    """R4.7: no clause matches → None."""

    def test_rolling_mean_open_returns_none(self) -> None:
        # rolling_mean(open, 20): no cs_rank/zscore, leaf={open} not ⊆ {pe,pb},
        # no cs_industry_demean, no turnover, no rolling_std, no delta/lag
        ast = TSOp("rolling_mean", Leaf("open"), 20)
        self.assertIsNone(classify(ast))

    def test_rolling_max_open_returns_none(self) -> None:
        ast = TSOp("rolling_max", Leaf("open"), 60)
        self.assertIsNone(classify(ast))

    def test_cs_demean_open_returns_none(self) -> None:
        ast = CSOp("cs_demean", Leaf("open"))
        self.assertIsNone(classify(ast))

    def test_log_open_returns_none(self) -> None:
        ast = ArithOp("log", Leaf("open"))
        self.assertIsNone(classify(ast))

    def test_arith_open_close_returns_none(self) -> None:
        # +(open, close_adj): no operator triggers trend (no delta/lag), etc.
        ast = ArithOp("+", Leaf("open"), Leaf("close_adj"))
        self.assertIsNone(classify(ast))


class TestInvalidInput(unittest.TestCase):
    """R4.12: non-ASTNode input → None."""

    def test_string_input_returns_none(self) -> None:
        self.assertIsNone(classify("cs_rank(close_adj)"))

    def test_none_input_returns_none(self) -> None:
        self.assertIsNone(classify(None))

    def test_int_input_returns_none(self) -> None:
        self.assertIsNone(classify(42))

    def test_dict_input_returns_none(self) -> None:
        self.assertIsNone(classify({"op": "cs_rank"}))


class TestQualityNeverReturned(unittest.TestCase):
    """R4.9: 'quality' must never be returned under any input."""

    def test_quality_never_for_leaf_fields(self) -> None:
        for field in LEAF_FIELDS:
            result = classify(Leaf(field))
            self.assertNotEqual(result, "quality", msg=f"Got 'quality' for Leaf({field!r})")

    def test_quality_never_for_cs_ops(self) -> None:
        for op in ("cs_rank", "cs_zscore", "cs_demean", "cs_industry_demean"):
            result = classify(CSOp(op, Leaf("close_adj")))
            self.assertNotEqual(result, "quality", msg=f"Got 'quality' for CSOp({op!r})")

    def test_quality_never_for_ts_ops(self) -> None:
        for op in ("lag", "delta", "rolling_mean", "rolling_std", "rolling_max", "rolling_min"):
            result = classify(TSOp(op, Leaf("close_adj"), 20))
            self.assertNotEqual(result, "quality")


# ---------------------------------------------------------------------------
# Helpers for PBT: strategies to build random valid ASTNodes
# ---------------------------------------------------------------------------

_leaf_strat = st.sampled_from(sorted(LEAF_FIELDS))
_window_strat = st.sampled_from(sorted(WINDOW_WHITELIST))
_ts_op_strat = st.sampled_from(sorted(TIME_SERIES_OPS))
_cs_op_strat = st.sampled_from(
    sorted({"cs_rank", "cs_zscore", "cs_demean", "cs_industry_demean"})
)
_bin_arith_strat = st.sampled_from(sorted({"+", "-", "*", "/"}))

_VALID_FAMILIES_SET = {"trend", "volatility", "volume", "value", "cross_momentum"}


def _ast_strategy(max_depth: int = 3) -> st.SearchStrategy[object]:
    """Build random valid ASTNode objects (TS-wraps-CS is avoided)."""

    def ts_safe(depth: int) -> st.SearchStrategy[object]:
        if depth <= 1:
            return _leaf_strat.map(Leaf)
        sub = ts_safe(depth - 1)
        return st.one_of(
            _leaf_strat.map(Leaf),
            st.builds(TSOp, _ts_op_strat, sub, _window_strat),
            st.builds(ArithOp, _bin_arith_strat, sub, sub),
            st.builds(lambda l: ArithOp("log", l), sub),
        )

    def any_ast(depth: int) -> st.SearchStrategy[object]:
        if depth <= 1:
            return _leaf_strat.map(Leaf)
        ts_safe_sub = ts_safe(depth - 1)
        any_sub = any_ast(depth - 1)
        return st.one_of(
            _leaf_strat.map(Leaf),
            st.builds(TSOp, _ts_op_strat, ts_safe_sub, _window_strat),
            st.builds(CSOp, _cs_op_strat, any_sub),
            st.builds(ArithOp, _bin_arith_strat, any_sub, any_sub),
            st.builds(lambda l: ArithOp("log", l), any_sub),
        )

    return any_ast(max_depth)


# ---------------------------------------------------------------------------
# Property-based tests
# ---------------------------------------------------------------------------


class TestPBTIdempotency(unittest.TestCase):
    """R4.11: same AST → same family every call (idempotency)."""

    @given(_ast_strategy(max_depth=3))
    @settings(max_examples=1000)
    def test_classify_idempotent(self, ast: object) -> None:
        """**Validates: Requirements R4.11**

        Calling classify() twice on the same AST must return the same result,
        independent of call order or any external state.
        """
        first = classify(ast)
        second = classify(ast)
        self.assertEqual(
            first,
            second,
            msg=f"classify returned {first!r} then {second!r} for {ast!r}",
        )


class TestPBTReturnValueConstraints(unittest.TestCase):
    """R4.1, R4.9: result is always in the valid set or None; never 'quality'."""

    @given(_ast_strategy(max_depth=3))
    @settings(max_examples=500)
    def test_result_is_valid_family_or_none(self, ast: object) -> None:
        """**Validates: Requirements R4.1, R4.9**

        classify() must return one of the 5 valid family strings or None.
        It must never return 'quality' or any other unexpected string.
        """
        result = classify(ast)
        if result is not None:
            self.assertIn(
                result,
                _VALID_FAMILIES_SET,
                msg=f"classify returned unexpected value {result!r} for {ast!r}",
            )
        self.assertNotEqual(result, "quality", msg="classify must never return 'quality'")


class TestPBTLeafHelperCorrectness(unittest.TestCase):
    """_leaf_fields always returns a subset of LEAF_FIELDS."""

    @given(_ast_strategy(max_depth=3))
    @settings(max_examples=300)
    def test_leaf_fields_are_valid(self, ast: object) -> None:
        """**Validates: Requirements R4.1**

        _leaf_fields must only return field names from the grammar whitelist.
        """
        if not isinstance(ast, (Leaf, TSOp, CSOp, ArithOp)):
            return
        leaves = _leaf_fields(ast)
        self.assertTrue(
            leaves.issubset(LEAF_FIELDS),
            msg=f"_leaf_fields returned {leaves!r} which contains unknown fields",
        )


class TestPBTContainsOpNeverRaisesOnValidAST(unittest.TestCase):
    """_contains_op must not raise on any valid AST."""

    @given(_ast_strategy(max_depth=3), st.sampled_from(["cs_rank", "lag", "rolling_std", "unknown_op"]))
    @settings(max_examples=300)
    def test_contains_op_does_not_raise(self, ast: object, op: str) -> None:
        """**Validates: Requirements R4.2, R4.5, R4.6**"""
        if not isinstance(ast, (Leaf, TSOp, CSOp, ArithOp)):
            return
        try:
            _contains_op(ast, op)
        except Exception as exc:  # noqa: BLE001
            self.fail(f"_contains_op raised {exc!r} for ast={ast!r}, op={op!r}")


if __name__ == "__main__":
    unittest.main()
