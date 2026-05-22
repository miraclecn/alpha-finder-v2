"""Unit + property-based tests for expression_generator.

Verification target: Task 8 (R3.8)

Tests:
- Unit: all_leaves() returns exactly 5 leaves.
- Unit: expand_layer on 5 leaves produces the expected 305 depth-2 trees.
- Unit: _contains_cs_op returns correct results.
- Unit: random_tree respects grammar rules.
- PBT: same seed produces same random_tree sequence (R3.8).
- PBT: random_tree never violates R2.7 (no TS-wraps-CS).

**Validates: Requirements R3.8**
"""

from __future__ import annotations

import unittest

import numpy as np
from hypothesis import given, settings
from hypothesis import strategies as st

from alpha_find_v2.factor_lab.dsl.grammar import (
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
from alpha_find_v2.factor_lab.search.expression_generator import (
    _contains_cs_op,
    all_leaves,
    expand_layer,
    random_tree,
)

# ---------------------------------------------------------------------------
# Unit tests: all_leaves
# ---------------------------------------------------------------------------


class TestAllLeaves(unittest.TestCase):
    def test_returns_five_leaves(self) -> None:
        leaves = all_leaves()
        self.assertEqual(len(leaves), 5)

    def test_all_are_leaf_nodes(self) -> None:
        for leaf in all_leaves():
            self.assertIsInstance(leaf, Leaf)

    def test_fields_match_whitelist(self) -> None:
        fields = {leaf.field for leaf in all_leaves()}
        self.assertEqual(fields, LEAF_FIELDS)

    def test_result_is_sorted(self) -> None:
        leaves = all_leaves()
        fields = [leaf.field for leaf in leaves]
        self.assertEqual(fields, sorted(fields))


# ---------------------------------------------------------------------------
# Unit tests: _contains_cs_op
# ---------------------------------------------------------------------------


class TestContainsCsOp(unittest.TestCase):
    def test_leaf_returns_false(self) -> None:
        self.assertFalse(_contains_cs_op(Leaf("close_adj")))

    def test_cs_op_returns_true(self) -> None:
        self.assertTrue(_contains_cs_op(CSOp("cs_rank", Leaf("close_adj"))))

    def test_ts_op_wrapping_leaf_returns_false(self) -> None:
        self.assertFalse(_contains_cs_op(TSOp("lag", Leaf("close_adj"), 20)))

    def test_ts_op_wrapping_cs_returns_true(self) -> None:
        inner = CSOp("cs_rank", Leaf("close_adj"))
        outer = TSOp("lag", inner, 20)
        self.assertTrue(_contains_cs_op(outer))

    def test_arith_with_cs_on_left(self) -> None:
        self.assertTrue(
            _contains_cs_op(ArithOp("+", CSOp("cs_rank", Leaf("pe")), Leaf("open")))
        )

    def test_arith_with_cs_on_right(self) -> None:
        self.assertTrue(
            _contains_cs_op(ArithOp("+", Leaf("open"), CSOp("cs_rank", Leaf("pe"))))
        )

    def test_arith_no_cs_returns_false(self) -> None:
        self.assertFalse(
            _contains_cs_op(ArithOp("+", Leaf("close_adj"), Leaf("open")))
        )

    def test_log_wrapping_cs_returns_true(self) -> None:
        self.assertTrue(_contains_cs_op(ArithOp("log", CSOp("cs_zscore", Leaf("pe")))))

    def test_log_wrapping_leaf_returns_false(self) -> None:
        self.assertFalse(_contains_cs_op(ArithOp("log", Leaf("close_adj"))))


# ---------------------------------------------------------------------------
# Unit tests: expand_layer on depth-1 leaves → expected depth-2 count
# ---------------------------------------------------------------------------


class TestExpandLayerDepth1(unittest.TestCase):
    """Verify expand_layer produces the correct number of depth-2 trees
    when seeded with the 5 leaf nodes.

    Expected count (all node_count ≤ MAX_DEPTH=5, depth-2 from 5 leaves):
    - TS ops wrapping a leaf (all leaves have no CS op):
        6 ops × 5 leaves × 6 windows = 180
    - CS ops wrapping a leaf:
        4 ops × 5 leaves = 20
    - log wrapping a leaf:
        1 × 5 leaves = 5
    Subtotal single-wrap: 205

    - Binary ArithOps (ordered pairs of parents):
        4 ops × 5 leaves × 5 leaves = 100
    Total: 305
    """

    def setUp(self) -> None:
        self.leaves = all_leaves()
        self.expanded = list(expand_layer(self.leaves))

    def test_total_count(self) -> None:
        self.assertEqual(len(self.expanded), 305)

    def test_ts_wrap_count(self) -> None:
        ts_wraps = [e for e in self.expanded if isinstance(e, TSOp)]
        # 6 ops × 5 leaves × 6 windows = 180
        self.assertEqual(len(ts_wraps), 180)

    def test_cs_wrap_count(self) -> None:
        cs_wraps = [e for e in self.expanded if isinstance(e, CSOp)]
        # 4 ops × 5 leaves = 20
        self.assertEqual(len(cs_wraps), 20)

    def test_log_wrap_count(self) -> None:
        log_wraps = [
            e for e in self.expanded
            if isinstance(e, ArithOp) and e.right is None
        ]
        # 5 leaves
        self.assertEqual(len(log_wraps), 5)

    def test_binary_arith_count(self) -> None:
        binary_arith = [
            e for e in self.expanded
            if isinstance(e, ArithOp) and e.right is not None
        ]
        # 4 ops × 5 × 5 = 100
        self.assertEqual(len(binary_arith), 100)

    def test_all_node_counts_are_within_limit(self) -> None:
        for tree in self.expanded:
            nc = node_count(tree)
            self.assertLessEqual(
                nc, MAX_DEPTH, msg=f"node_count={nc} exceeds MAX_DEPTH for {tree}"
            )

    def test_no_ts_wraps_cs(self) -> None:
        """R2.7: no TS op should wrap a CS sub-expression."""
        for tree in self.expanded:
            if isinstance(tree, TSOp):
                self.assertFalse(
                    _contains_cs_op(tree.operand),
                    msg=f"TS op wraps CS in {tree}",
                )


class TestExpandLayerEmptyParents(unittest.TestCase):
    def test_empty_list_yields_nothing(self) -> None:
        result = list(expand_layer([]))
        self.assertEqual(result, [])


class TestExpandLayerNodeCountFilter(unittest.TestCase):
    """When parents already have node_count=4, only some extensions fit in MAX_DEPTH=5."""

    def test_depth4_parent_only_unary_extensions_fit(self) -> None:
        # A tree with node_count=4: lag(rolling_mean(lag(close_adj,5),10),20)
        inner = TSOp("lag", Leaf("close_adj"), 5)
        mid = TSOp("rolling_mean", inner, 10)
        parent = TSOp("lag", mid, 20)
        self.assertEqual(node_count(parent), 4)

        expanded = list(expand_layer([parent]))
        for tree in expanded:
            self.assertLessEqual(node_count(tree), MAX_DEPTH)

        # All expanded should have node_count exactly 5 (parent 4 + 1 op)
        for tree in expanded:
            self.assertEqual(node_count(tree), 5)

    def test_depth5_parent_yields_nothing(self) -> None:
        # A tree with node_count=5 (already at max): no extensions can fit
        l1 = TSOp("lag", Leaf("close_adj"), 5)
        l2 = TSOp("lag", Leaf("open"), 5)
        parent = ArithOp("+", l1, l2)
        # node_count = 1 + 2 + 2 = 5
        self.assertEqual(node_count(parent), 5)

        expanded = list(expand_layer([parent]))
        self.assertEqual(
            expanded, [],
            msg="A depth-5 parent should yield no extensions (all would exceed MAX_DEPTH)"
        )


# ---------------------------------------------------------------------------
# Unit tests: random_tree
# ---------------------------------------------------------------------------


class TestRandomTree(unittest.TestCase):
    def _make_rng(self, seed: int = 42) -> np.random.Generator:
        return np.random.default_rng(seed)

    def test_returns_ast_node(self) -> None:
        rng = self._make_rng()
        tree = random_tree(rng, max_depth=3)
        self.assertIsInstance(tree, (Leaf, TSOp, CSOp, ArithOp))

    def test_node_count_bounded_by_max_depth(self) -> None:
        rng = self._make_rng()
        for _ in range(50):
            tree = random_tree(rng, max_depth=MAX_DEPTH)
            self.assertLessEqual(node_count(tree), MAX_DEPTH)

    def test_no_ts_wraps_cs(self) -> None:
        """R2.7 must be satisfied for all random trees."""
        rng = self._make_rng(0)
        for _ in range(100):
            tree = random_tree(rng, max_depth=MAX_DEPTH)
            self._assert_no_ts_wraps_cs(tree)

    def _assert_no_ts_wraps_cs(self, ast: object) -> None:
        if isinstance(ast, TSOp):
            self.assertFalse(
                _contains_cs_op(ast.operand),
                msg=f"TS op wraps CS sub-expression in {ast}",
            )
            self._assert_no_ts_wraps_cs(ast.operand)
        elif isinstance(ast, CSOp):
            self._assert_no_ts_wraps_cs(ast.operand)
        elif isinstance(ast, ArithOp):
            self._assert_no_ts_wraps_cs(ast.left)
            if ast.right is not None:
                self._assert_no_ts_wraps_cs(ast.right)

    def test_uses_only_whitelisted_windows(self) -> None:
        rng = self._make_rng(1)
        for _ in range(100):
            tree = random_tree(rng, max_depth=MAX_DEPTH)
            self._assert_whitelisted_windows(tree)

    def _assert_whitelisted_windows(self, ast: object) -> None:
        if isinstance(ast, TSOp):
            self.assertIn(ast.window, WINDOW_WHITELIST)
            self._assert_whitelisted_windows(ast.operand)
        elif isinstance(ast, CSOp):
            self._assert_whitelisted_windows(ast.operand)
        elif isinstance(ast, ArithOp):
            self._assert_whitelisted_windows(ast.left)
            if ast.right is not None:
                self._assert_whitelisted_windows(ast.right)

    def test_uses_only_valid_operators(self) -> None:
        valid_ops = TIME_SERIES_OPS | CROSS_SECTION_OPS | {"+", "-", "*", "/", "log"}
        rng = self._make_rng(2)
        for _ in range(50):
            tree = random_tree(rng, max_depth=MAX_DEPTH)
            self._assert_valid_ops(tree, valid_ops)

    def _assert_valid_ops(self, ast: object, valid_ops: frozenset[str]) -> None:
        if isinstance(ast, TSOp):
            self.assertIn(ast.op, valid_ops)
            self._assert_valid_ops(ast.operand, valid_ops)
        elif isinstance(ast, CSOp):
            self.assertIn(ast.op, valid_ops)
            self._assert_valid_ops(ast.operand, valid_ops)
        elif isinstance(ast, ArithOp):
            self.assertIn(ast.op, valid_ops)
            self._assert_valid_ops(ast.left, valid_ops)
            if ast.right is not None:
                self._assert_valid_ops(ast.right, valid_ops)

    def test_uses_only_whitelisted_leaf_fields(self) -> None:
        rng = self._make_rng(3)
        for _ in range(50):
            tree = random_tree(rng, max_depth=MAX_DEPTH)
            self._assert_valid_leaves(tree)

    def _assert_valid_leaves(self, ast: object) -> None:
        if isinstance(ast, Leaf):
            self.assertIn(ast.field, LEAF_FIELDS)
        elif isinstance(ast, TSOp):
            self._assert_valid_leaves(ast.operand)
        elif isinstance(ast, CSOp):
            self._assert_valid_leaves(ast.operand)
        elif isinstance(ast, ArithOp):
            self._assert_valid_leaves(ast.left)
            if ast.right is not None:
                self._assert_valid_leaves(ast.right)

    def test_max_depth_1_always_returns_leaf(self) -> None:
        rng = self._make_rng(7)
        for _ in range(20):
            tree = random_tree(rng, max_depth=1)
            self.assertIsInstance(tree, Leaf)

    def test_invalid_max_depth_raises(self) -> None:
        rng = self._make_rng()
        with self.assertRaises(ValueError):
            random_tree(rng, max_depth=0)


# ---------------------------------------------------------------------------
# PBT: same seed → same random_tree sequence (R3.8)
# ---------------------------------------------------------------------------


class TestPBTReproducibility(unittest.TestCase):
    """R3.8: All stochastic choices derive from injected RNG seed."""

    @given(st.integers(min_value=0, max_value=2**32 - 1), st.integers(min_value=1, max_value=5))
    @settings(max_examples=200)
    def test_same_seed_same_sequence(self, seed: int, max_depth: int) -> None:
        """**Validates: Requirements R3.8**

        Two RNGs initialised with the same seed must produce the same sequence
        of random trees.
        """
        rng1 = np.random.default_rng(seed)
        rng2 = np.random.default_rng(seed)
        n_draws = 5
        trees1 = [random_tree(rng1, max_depth) for _ in range(n_draws)]
        trees2 = [random_tree(rng2, max_depth) for _ in range(n_draws)]
        self.assertEqual(
            trees1,
            trees2,
            msg=f"Different trees for seed={seed}, max_depth={max_depth}",
        )


class TestPBTRandomTreeGrammarCompliance(unittest.TestCase):
    """random_tree must always produce grammar-compliant expressions."""

    @given(
        st.integers(min_value=0, max_value=2**31 - 1),
        st.integers(min_value=1, max_value=MAX_DEPTH),
    )
    @settings(max_examples=300)
    def test_no_ts_wraps_cs_property(self, seed: int, max_depth: int) -> None:
        """**Validates: Requirements R3.8**

        random_tree must never produce a tree where a TS op wraps a CS op
        (R2.7).
        """
        rng = np.random.default_rng(seed)
        tree = random_tree(rng, max_depth)
        self._check_no_ts_wraps_cs(tree)

    def _check_no_ts_wraps_cs(self, ast: object) -> None:
        if isinstance(ast, TSOp):
            self.assertFalse(
                _contains_cs_op(ast.operand),
                msg=f"R2.7 violated: TSOp wraps CS in {ast}",
            )
            self._check_no_ts_wraps_cs(ast.operand)
        elif isinstance(ast, CSOp):
            self._check_no_ts_wraps_cs(ast.operand)
        elif isinstance(ast, ArithOp):
            self._check_no_ts_wraps_cs(ast.left)
            if ast.right is not None:
                self._check_no_ts_wraps_cs(ast.right)

    @given(
        st.integers(min_value=0, max_value=2**31 - 1),
        st.integers(min_value=1, max_value=MAX_DEPTH),
    )
    @settings(max_examples=300)
    def test_only_valid_leaf_fields(self, seed: int, max_depth: int) -> None:
        """**Validates: Requirements R3.8**

        All leaf fields in a random tree must be in LEAF_FIELDS (R2.4).
        """
        rng = np.random.default_rng(seed)
        tree = random_tree(rng, max_depth)
        self._check_leaves(tree)

    def _check_leaves(self, ast: object) -> None:
        if isinstance(ast, Leaf):
            self.assertIn(ast.field, LEAF_FIELDS)
        elif isinstance(ast, TSOp):
            self._check_leaves(ast.operand)
        elif isinstance(ast, CSOp):
            self._check_leaves(ast.operand)
        elif isinstance(ast, ArithOp):
            self._check_leaves(ast.left)
            if ast.right is not None:
                self._check_leaves(ast.right)

    @given(
        st.integers(min_value=0, max_value=2**31 - 1),
        st.integers(min_value=1, max_value=MAX_DEPTH),
    )
    @settings(max_examples=300)
    def test_only_whitelisted_windows(self, seed: int, max_depth: int) -> None:
        """**Validates: Requirements R3.8**

        All window values in a random tree must be from WINDOW_WHITELIST (R2.5).
        """
        rng = np.random.default_rng(seed)
        tree = random_tree(rng, max_depth)
        self._check_windows(tree)

    def _check_windows(self, ast: object) -> None:
        if isinstance(ast, TSOp):
            self.assertIn(
                ast.window, WINDOW_WHITELIST,
                msg=f"Window {ast.window} not in whitelist for {ast}",
            )
            self._check_windows(ast.operand)
        elif isinstance(ast, CSOp):
            self._check_windows(ast.operand)
        elif isinstance(ast, ArithOp):
            self._check_windows(ast.left)
            if ast.right is not None:
                self._check_windows(ast.right)


if __name__ == "__main__":
    unittest.main()
