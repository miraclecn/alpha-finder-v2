"""Unit + property-based tests for fitness function with complexity penalty.

Verification target: Task 7 (R6.1, R6.2, R6.3)

**Validates: Requirements R6.1, R6.2, R6.3**
"""

from __future__ import annotations

import math
import unittest

from hypothesis import assume, given, settings
from hypothesis import strategies as st

from alpha_find_v2.factor_lab.search.fitness import fitness, sort_key


# ---------------------------------------------------------------------------
# Unit tests: boundary values and core formula
# ---------------------------------------------------------------------------


class TestFitnessFormula(unittest.TestCase):
    """R6.1: fitness = train_IC_IR − λ × node_count."""

    def test_zero_ic_ir_zero_lambda(self) -> None:
        # fitness(0, 1, 0.0) = 0 - 0 * 1 = 0.0
        self.assertEqual(fitness(0.0, 1, 0.0), 0.0)

    def test_positive_ic_ir_default_lambda(self) -> None:
        # fitness(1.0, 1, 0.05) = 1.0 - 0.05 * 1 = 0.95
        self.assertAlmostEqual(fitness(1.0, 1, 0.05), 0.95)

    def test_positive_ic_ir_max_nodes(self) -> None:
        # fitness(1.0, 5, 0.05) = 1.0 - 0.05 * 5 = 0.75
        self.assertAlmostEqual(fitness(1.0, 5, 0.05), 0.75)

    def test_negative_ic_ir(self) -> None:
        # fitness(-0.5, 3, 0.05) = -0.5 - 0.05 * 3 = -0.65
        self.assertAlmostEqual(fitness(-0.5, 3, 0.05), -0.65)

    def test_zero_lambda_no_penalty(self) -> None:
        # When lambda_ = 0, fitness = train_ic_ir regardless of node_count
        self.assertAlmostEqual(fitness(0.8, 5, 0.0), 0.8)

    def test_high_lambda_strong_penalty(self) -> None:
        # fitness(1.0, 5, 1.0) = 1.0 - 1.0 * 5 = -4.0
        self.assertAlmostEqual(fitness(1.0, 5, 1.0), -4.0)

    def test_ic_ir_one_single_node(self) -> None:
        # fitness(1.0, 1, 0.05) = 0.95
        self.assertAlmostEqual(fitness(1.0, 1, 0.05), 0.95)


class TestFitnessInvalidInputs(unittest.TestCase):
    """R6.2: None / NaN / infinite train_ic_ir → None."""

    def test_none_returns_none(self) -> None:
        self.assertIsNone(fitness(None, 1, 0.05))

    def test_nan_returns_none(self) -> None:
        self.assertIsNone(fitness(float("nan"), 1, 0.05))

    def test_positive_inf_returns_none(self) -> None:
        self.assertIsNone(fitness(float("inf"), 1, 0.05))

    def test_negative_inf_returns_none(self) -> None:
        self.assertIsNone(fitness(float("-inf"), 1, 0.05))

    def test_none_with_zero_node_count(self) -> None:
        # node_count value doesn't matter when train_ic_ir is invalid
        self.assertIsNone(fitness(None, 0, 0.0))

    def test_nan_with_zero_lambda(self) -> None:
        self.assertIsNone(fitness(float("nan"), 1, 0.0))


# ---------------------------------------------------------------------------
# Unit tests: sort_key ordering invariants
# ---------------------------------------------------------------------------


class TestSortKey(unittest.TestCase):
    """R6.3: ordering — higher fitness first; ties by node_count asc; ties by lex asc."""

    def test_higher_fitness_ranked_first(self) -> None:
        k_high = sort_key((1.0, 1, "a"))
        k_low = sort_key((0.5, 1, "a"))
        self.assertLess(k_high, k_low)

    def test_equal_fitness_ascending_node_count(self) -> None:
        k_fewer = sort_key((0.9, 1, "a"))
        k_more = sort_key((0.9, 3, "a"))
        self.assertLess(k_fewer, k_more)

    def test_equal_fitness_equal_nodes_ascending_lex(self) -> None:
        k_a = sort_key((0.9, 2, "aaa"))
        k_b = sort_key((0.9, 2, "bbb"))
        self.assertLess(k_a, k_b)

    def test_none_fitness_sorts_after_finite(self) -> None:
        k_none = sort_key((None, 1, "a"))
        k_any = sort_key((-999.0, 5, "zzz"))
        self.assertGreater(k_none, k_any)

    def test_none_fitness_equal_to_none(self) -> None:
        # Two None-fitness candidates compare only by node_count then lex
        k1 = sort_key((None, 1, "a"))
        k2 = sort_key((None, 2, "z"))
        self.assertLess(k1, k2)

    def test_sort_list_by_sort_key(self) -> None:
        candidates = [
            (0.5, 3, "c"),
            (1.0, 1, "a"),
            (1.0, 1, "b"),
            (1.0, 2, "a"),
            (None, 1, "a"),
        ]
        ordered = sorted(candidates, key=sort_key)
        # Expected order:
        # (1.0, 1, 'a') → key (0, -1.0, 1, 'a')
        # (1.0, 1, 'b') → key (0, -1.0, 1, 'b')
        # (1.0, 2, 'a') → key (0, -1.0, 2, 'a')
        # (0.5, 3, 'c') → key (0, -0.5, 3, 'c')
        # (None, 1, 'a') → key (1, 0, 1, 'a')
        expected = [
            (1.0, 1, "a"),
            (1.0, 1, "b"),
            (1.0, 2, "a"),
            (0.5, 3, "c"),
            (None, 1, "a"),
        ]
        self.assertEqual(ordered, expected)


# ---------------------------------------------------------------------------
# Property-based tests: ordering invariants
# ---------------------------------------------------------------------------

_finite_ic_ir = st.floats(min_value=-10.0, max_value=10.0, allow_nan=False, allow_infinity=False)
_node_count_st = st.integers(min_value=1, max_value=5)
_lambda_st = st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)
_canonical_st = st.text(alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), min_codepoint=32, max_codepoint=127), min_size=1, max_size=30)


class TestPBTFitnessMonotonicity(unittest.TestCase):
    """R6.1: higher train_ic_ir with same node_count and lambda_ → higher fitness."""

    @given(
        ic1=_finite_ic_ir,
        ic2=_finite_ic_ir,
        nc=_node_count_st,
        lam=_lambda_st,
    )
    @settings(max_examples=500)
    def test_higher_ic_ir_gives_higher_fitness(
        self, ic1: float, ic2: float, nc: int, lam: float
    ) -> None:
        """**Validates: Requirements R6.1**

        If ic1 > ic2 (same node_count and lambda_), then
        fitness(ic1, nc, lam) > fitness(ic2, nc, lam).
        """
        # Require a difference large enough to survive float64 subtraction
        assume(ic1 > ic2 and (ic1 - ic2) > 1e-10)
        f1 = fitness(ic1, nc, lam)
        f2 = fitness(ic2, nc, lam)
        assert f1 is not None and f2 is not None
        self.assertGreater(
            f1,
            f2,
            msg=f"fitness({ic1}, {nc}, {lam})={f1} not > fitness({ic2}, {nc}, {lam})={f2}",
        )

    @given(
        ic=_finite_ic_ir,
        nc1=_node_count_st,
        nc2=_node_count_st,
        lam=_lambda_st,
    )
    @settings(max_examples=500)
    def test_fewer_nodes_higher_fitness_for_positive_lambda(
        self, ic: float, nc1: int, nc2: int, lam: float
    ) -> None:
        """**Validates: Requirements R6.1**

        If nc1 < nc2 and lambda_ > 0, then
        fitness(ic, nc1, lam) > fitness(ic, nc2, lam).
        """
        # Require lambda large enough that the penalty difference survives float64
        assume(nc1 < nc2 and lam > 1e-10)
        f1 = fitness(ic, nc1, lam)
        f2 = fitness(ic, nc2, lam)
        assert f1 is not None and f2 is not None
        self.assertGreater(
            f1,
            f2,
            msg=f"fitness({ic}, {nc1}, {lam})={f1} not > fitness({ic}, {nc2}, {lam})={f2}",
        )


class TestPBTSortKeyOrdering(unittest.TestCase):
    """R6.3: sort_key ordering invariants across random candidates."""

    @given(
        ic1=_finite_ic_ir,
        ic2=_finite_ic_ir,
        nc=_node_count_st,
        cs=_canonical_st,
        lam=_lambda_st,
    )
    @settings(max_examples=500)
    def test_higher_fitness_sorts_first(
        self, ic1: float, ic2: float, nc: int, cs: str, lam: float
    ) -> None:
        """**Validates: Requirements R6.3**

        Candidate with strictly higher fitness has a strictly smaller sort_key.
        """
        f1 = fitness(ic1, nc, lam)
        f2 = fitness(ic2, nc, lam)
        if f1 is None or f2 is None or f1 <= f2:
            return
        k1 = sort_key((f1, nc, cs))
        k2 = sort_key((f2, nc, cs))
        self.assertLess(
            k1, k2,
            msg=f"sort_key for fitness={f1} not < sort_key for fitness={f2}",
        )

    @given(
        f=_finite_ic_ir,
        nc1=_node_count_st,
        nc2=_node_count_st,
        cs=_canonical_st,
    )
    @settings(max_examples=500)
    def test_fewer_nodes_sorts_first_on_tied_fitness(
        self, f: float, nc1: int, nc2: int, cs: str
    ) -> None:
        """**Validates: Requirements R6.3**

        When fitness is identical, candidate with fewer nodes sorts first.
        """
        if nc1 >= nc2:
            return
        k1 = sort_key((f, nc1, cs))
        k2 = sort_key((f, nc2, cs))
        self.assertLess(
            k1, k2,
            msg=f"sort_key(nc={nc1}) not < sort_key(nc={nc2}) for same fitness={f}",
        )

    @given(
        f=_finite_ic_ir,
        nc=_node_count_st,
        cs1=_canonical_st,
        cs2=_canonical_st,
    )
    @settings(max_examples=500)
    def test_lex_order_on_tied_fitness_and_nodes(
        self, f: float, nc: int, cs1: str, cs2: str
    ) -> None:
        """**Validates: Requirements R6.3**

        When fitness and node_count are identical, lexicographically smaller
        canonical string sorts first.
        """
        if cs1 >= cs2:
            return
        k1 = sort_key((f, nc, cs1))
        k2 = sort_key((f, nc, cs2))
        self.assertLess(
            k1, k2,
            msg=f"sort_key(cs={cs1!r}) not < sort_key(cs={cs2!r}) for same fitness and nc",
        )

    @given(
        f=_finite_ic_ir,
        nc=_node_count_st,
        cs=_canonical_st,
    )
    @settings(max_examples=300)
    def test_none_sorts_after_any_finite_fitness(
        self, f: float, nc: int, cs: str
    ) -> None:
        """**Validates: Requirements R6.3**

        None fitness always sorts after any finite fitness value.
        """
        k_finite = sort_key((f, nc, cs))
        k_none = sort_key((None, nc, cs))
        self.assertLess(
            k_finite, k_none,
            msg=f"sort_key(fitness={f}) not < sort_key(None)",
        )


if __name__ == "__main__":
    unittest.main()
