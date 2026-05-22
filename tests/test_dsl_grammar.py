"""Unit tests for factor_lab DSL grammar constants and AST nodes.

Verification target: Task 2 (R2.1–R2.6, R2.12)
"""

import unittest

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


class TestWhitelistSizes(unittest.TestCase):
    """Exact sizes required by R2.1–R2.5."""

    def test_time_series_ops_size(self) -> None:
        self.assertEqual(len(TIME_SERIES_OPS), 6)  # R2.1

    def test_cross_section_ops_size(self) -> None:
        self.assertEqual(len(CROSS_SECTION_OPS), 4)  # R2.2

    def test_arithmetic_ops_size(self) -> None:
        self.assertEqual(len(ARITHMETIC_OPS), 5)  # R2.3

    def test_leaf_fields_size(self) -> None:
        self.assertEqual(len(LEAF_FIELDS), 5)  # R2.4

    def test_window_whitelist_size(self) -> None:
        self.assertEqual(len(WINDOW_WHITELIST), 6)  # R2.5

    def test_max_depth(self) -> None:
        self.assertEqual(MAX_DEPTH, 5)


class TestWhitelistContents(unittest.TestCase):
    """Exact members required by R2.1–R2.5."""

    def test_time_series_ops_members(self) -> None:
        self.assertEqual(
            TIME_SERIES_OPS,
            frozenset(
                {"lag", "delta", "rolling_mean", "rolling_std", "rolling_max", "rolling_min"}
            ),
        )

    def test_cross_section_ops_members(self) -> None:
        self.assertEqual(
            CROSS_SECTION_OPS,
            frozenset({"cs_rank", "cs_zscore", "cs_demean", "cs_industry_demean"}),
        )

    def test_arithmetic_ops_members(self) -> None:
        self.assertEqual(ARITHMETIC_OPS, frozenset({"+", "-", "*", "/", "log"}))

    def test_leaf_fields_members(self) -> None:
        self.assertEqual(
            LEAF_FIELDS,
            frozenset({"close_adj", "open", "turnover_value_cny", "pe", "pb"}),
        )

    def test_window_whitelist_members(self) -> None:
        self.assertEqual(WINDOW_WHITELIST, frozenset({5, 10, 20, 60, 120, 250}))


class TestWhitelistsAreFrozen(unittest.TestCase):
    def test_time_series_ops_is_frozenset(self) -> None:
        self.assertIsInstance(TIME_SERIES_OPS, frozenset)

    def test_cross_section_ops_is_frozenset(self) -> None:
        self.assertIsInstance(CROSS_SECTION_OPS, frozenset)

    def test_arithmetic_ops_is_frozenset(self) -> None:
        self.assertIsInstance(ARITHMETIC_OPS, frozenset)

    def test_leaf_fields_is_frozenset(self) -> None:
        self.assertIsInstance(LEAF_FIELDS, frozenset)

    def test_window_whitelist_is_frozenset(self) -> None:
        self.assertIsInstance(WINDOW_WHITELIST, frozenset)


class TestASTNodeHashability(unittest.TestCase):
    """Frozen dataclasses must be hashable (D4)."""

    def test_leaf_hashable(self) -> None:
        node = Leaf("close_adj")
        self.assertIsInstance(hash(node), int)

    def test_ts_op_hashable(self) -> None:
        node = TSOp("lag", Leaf("close_adj"), 20)
        self.assertIsInstance(hash(node), int)

    def test_cs_op_hashable(self) -> None:
        node = CSOp("cs_rank", Leaf("pe"))
        self.assertIsInstance(hash(node), int)

    def test_arith_op_binary_hashable(self) -> None:
        node = ArithOp("+", Leaf("close_adj"), Leaf("open"))
        self.assertIsInstance(hash(node), int)

    def test_arith_op_unary_log_hashable(self) -> None:
        node = ArithOp("log", Leaf("close_adj"))
        self.assertIsInstance(hash(node), int)


class TestNodeCount(unittest.TestCase):
    """node_count counts operator applications + leaf refs; NOT window literals (R2.6)."""

    def test_leaf_is_1(self) -> None:
        self.assertEqual(node_count(Leaf("close_adj")), 1)

    def test_ts_op_excludes_window(self) -> None:
        # lag(close_adj, 20): 1 op + 1 leaf = 2, window 20 is NOT counted
        self.assertEqual(node_count(TSOp("lag", Leaf("close_adj"), 20)), 2)

    def test_cs_op(self) -> None:
        # cs_rank(pe): 1 op + 1 leaf = 2
        self.assertEqual(node_count(CSOp("cs_rank", Leaf("pe"))), 2)

    def test_arith_binary(self) -> None:
        # close_adj + open: 1 op + 1 leaf + 1 leaf = 3
        self.assertEqual(node_count(ArithOp("+", Leaf("close_adj"), Leaf("open"))), 3)

    def test_arith_unary_log(self) -> None:
        # log(close_adj): 1 op + 1 leaf = 2
        self.assertEqual(node_count(ArithOp("log", Leaf("close_adj"))), 2)

    def test_nested_excludes_window(self) -> None:
        # cs_rank(rolling_mean(close_adj, 20)):
        #   cs_rank=1, rolling_mean=1, close_adj=1 → total 3
        # window 20 must NOT be counted
        inner = TSOp("rolling_mean", Leaf("close_adj"), 20)
        outer = CSOp("cs_rank", inner)
        self.assertEqual(node_count(outer), 3)

    def test_deep_tree(self) -> None:
        # (lag(close_adj,5) + rolling_std(open,10))
        # = 1 (ArithOp) + 1 (TSOp lag) + 1 (Leaf close_adj)
        #                + 1 (TSOp rolling_std) + 1 (Leaf open) = 5
        left = TSOp("lag", Leaf("close_adj"), 5)
        right = TSOp("rolling_std", Leaf("open"), 10)
        tree = ArithOp("+", left, right)
        self.assertEqual(node_count(tree), 5)


if __name__ == "__main__":
    unittest.main()
