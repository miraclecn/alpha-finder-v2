"""Unit tests for factor_lab.quota.apply_family_quota.

Verification target: Task 12 (R4.8, R4.9, R4.10)

Covers:
1. 30 candidates across 5 families (6 per family) with quota=5 → 25 admitted, 5 rejected.
2. Admitted candidates are the top-fitness ones per family.
3. Rejected candidates have status="rejected_quota".
4. Candidates with family=None pass through without quota.
5. Tie-breaking: tied fitness → lex ascending canonical wins.
6. quota=1 → only 1 admitted per family.
"""

from __future__ import annotations

import uuid

import pytest

from alpha_find_v2.factor_lab.dsl.grammar import Leaf
from alpha_find_v2.factor_lab.quota import apply_family_quota
from alpha_find_v2.factor_lab.search.beam import Candidate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EXPR_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")


def _expr_id(canonical_str: str) -> str:
    return uuid.uuid5(_EXPR_NAMESPACE, canonical_str).hex


def _make_candidate(
    canonical_str: str,
    family: str | None,
    fitness: float | None,
    node_count: int = 1,
    status: str = "pending",
) -> Candidate:
    ast = Leaf(field="close_adj")
    return Candidate(
        expr_id=_expr_id(canonical_str),
        canonical=canonical_str,
        ast=ast,
        node_count=node_count,
        family=family,
        sources=["beam"],
        train_ic_ir=fitness,
        fitness=fitness,
        status=status,
        oos_segments=[],
    )


_FAMILIES = ["trend", "volatility", "volume", "value", "cross_momentum"]


def _make_family_batch(
    family: str,
    count: int,
    fitness_values: list[float | None],
) -> list[Candidate]:
    """Create ``count`` candidates for ``family`` with given fitness values."""
    assert len(fitness_values) == count
    return [
        _make_candidate(f"{family}_expr_{i}", family, fitness_values[i])
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# Test 1: 30 candidates across 5 families, quota=5 → 25 admitted, 5 rejected
# ---------------------------------------------------------------------------


class TestBasicQuota:
    def _build_30_candidates(self) -> list[Candidate]:
        """6 candidates per family, fitness 0.1..0.6 (all distinct)."""
        all_cands: list[Candidate] = []
        for fam in _FAMILIES:
            for i in range(6):
                fitness_val = (i + 1) * 0.1
                all_cands.append(_make_candidate(f"{fam}_expr_{i}", fam, fitness_val))
        return all_cands

    def test_total_admitted_is_25(self) -> None:
        candidates = self._build_30_candidates()
        admitted, rejected = apply_family_quota(candidates, quota_per_family=5)
        assert len(admitted) == 25

    def test_total_rejected_is_5(self) -> None:
        candidates = self._build_30_candidates()
        admitted, rejected = apply_family_quota(candidates, quota_per_family=5)
        assert len(rejected) == 5

    def test_exactly_5_admitted_per_family(self) -> None:
        candidates = self._build_30_candidates()
        admitted, _ = apply_family_quota(candidates, quota_per_family=5)
        for fam in _FAMILIES:
            fam_admitted = [c for c in admitted if c.family == fam]
            assert len(fam_admitted) == 5, f"Expected 5 admitted for {fam}, got {len(fam_admitted)}"

    def test_exactly_1_rejected_per_family(self) -> None:
        candidates = self._build_30_candidates()
        _, rejected = apply_family_quota(candidates, quota_per_family=5)
        for fam in _FAMILIES:
            fam_rejected = [c for c in rejected if c.family == fam]
            assert len(fam_rejected) == 1, f"Expected 1 rejected for {fam}, got {len(fam_rejected)}"


# ---------------------------------------------------------------------------
# Test 2: Admitted candidates are the top-fitness ones per family
# ---------------------------------------------------------------------------


class TestTopFitnessAdmitted:
    def test_highest_fitness_candidates_admitted(self) -> None:
        """Quota=2: top-2 fitness per family are admitted."""
        candidates = [
            _make_candidate("trend_a", "trend", 0.9),
            _make_candidate("trend_b", "trend", 0.7),
            _make_candidate("trend_c", "trend", 0.3),  # should be rejected
        ]
        admitted, rejected = apply_family_quota(candidates, quota_per_family=2)
        admitted_canonicals = {c.canonical for c in admitted}
        assert "trend_a" in admitted_canonicals
        assert "trend_b" in admitted_canonicals
        assert "trend_c" not in admitted_canonicals

    def test_lowest_fitness_candidate_rejected(self) -> None:
        """The candidate with the lowest fitness is the one rejected."""
        candidates = [
            _make_candidate("a_expr_0", "trend", 0.5),
            _make_candidate("a_expr_1", "trend", 0.8),
            _make_candidate("a_expr_2", "trend", 0.1),
        ]
        _, rejected = apply_family_quota(candidates, quota_per_family=2)
        assert len(rejected) == 1
        assert rejected[0].canonical == "a_expr_2"


# ---------------------------------------------------------------------------
# Test 3: Rejected candidates have status="rejected_quota"
# ---------------------------------------------------------------------------


class TestRejectedStatus:
    def test_rejected_candidates_have_correct_status(self) -> None:
        candidates = [
            _make_candidate("fam_a", "trend", 0.9),
            _make_candidate("fam_b", "trend", 0.5),
            _make_candidate("fam_c", "trend", 0.1),
        ]
        _, rejected = apply_family_quota(candidates, quota_per_family=2)
        for c in rejected:
            assert c.status == "rejected_quota"

    def test_admitted_candidates_retain_original_status(self) -> None:
        candidates = [
            _make_candidate("fam_a", "trend", 0.9, status="pending"),
            _make_candidate("fam_b", "trend", 0.5, status="pending"),
            _make_candidate("fam_c", "trend", 0.1, status="pending"),
        ]
        admitted, _ = apply_family_quota(candidates, quota_per_family=2)
        for c in admitted:
            assert c.status == "pending"


# ---------------------------------------------------------------------------
# Test 4: Candidates with family=None pass through without quota
# ---------------------------------------------------------------------------


class TestNoneFamilyPassThrough:
    def test_none_family_candidates_in_admitted(self) -> None:
        candidates = [
            _make_candidate("unclassifiable_1", None, 0.9, status="rejected_family_unclassifiable"),
            _make_candidate("unclassifiable_2", None, 0.5, status="rejected_family_unclassifiable"),
        ]
        admitted, rejected = apply_family_quota(candidates, quota_per_family=1)
        assert len(admitted) == 2
        assert len(rejected) == 0

    def test_none_family_candidates_not_in_rejected(self) -> None:
        candidates = [
            _make_candidate("unclassifiable", None, 0.9, status="rejected_family_unclassifiable"),
            _make_candidate("trend_x", "trend", 0.9),
            _make_candidate("trend_y", "trend", 0.5),
        ]
        _, rejected = apply_family_quota(candidates, quota_per_family=1)
        rejected_canonicals = {c.canonical for c in rejected}
        assert "unclassifiable" not in rejected_canonicals

    def test_none_family_status_unchanged(self) -> None:
        c = _make_candidate("unclassifiable", None, 0.5, status="rejected_family_unclassifiable")
        admitted, _ = apply_family_quota([c], quota_per_family=5)
        assert admitted[0].status == "rejected_family_unclassifiable"

    def test_mixed_none_and_family_candidates(self) -> None:
        candidates = [
            _make_candidate("unclass_1", None, 0.9, status="rejected_family_unclassifiable"),
            _make_candidate("trend_a", "trend", 0.8),
            _make_candidate("trend_b", "trend", 0.6),
            _make_candidate("trend_c", "trend", 0.2),
        ]
        admitted, rejected = apply_family_quota(candidates, quota_per_family=2)
        assert len(admitted) == 3  # 1 unclassifiable + 2 trend
        assert len(rejected) == 1


# ---------------------------------------------------------------------------
# Test 5: Tie-breaking — tied fitness → lex ascending canonical wins
# ---------------------------------------------------------------------------


class TestTieBreaking:
    def test_lex_smaller_canonical_admitted_on_tied_fitness(self) -> None:
        """When fitness is equal, the lex-smaller canonical string is admitted."""
        candidates = [
            _make_candidate("zzz_expr", "trend", 0.5),
            _make_candidate("aaa_expr", "trend", 0.5),
            _make_candidate("mmm_expr", "trend", 0.5),
        ]
        admitted, rejected = apply_family_quota(candidates, quota_per_family=2)
        admitted_canonicals = {c.canonical for c in admitted}
        assert "aaa_expr" in admitted_canonicals
        assert "mmm_expr" in admitted_canonicals
        assert "zzz_expr" not in admitted_canonicals

    def test_lex_larger_canonical_rejected_on_tied_fitness(self) -> None:
        """The lex-largest canonical is rejected when all fitness values are tied."""
        candidates = [
            _make_candidate("aaa", "volatility", 0.5),
            _make_candidate("bbb", "volatility", 0.5),
            _make_candidate("ccc", "volatility", 0.5),
        ]
        _, rejected = apply_family_quota(candidates, quota_per_family=2)
        assert len(rejected) == 1
        assert rejected[0].canonical == "ccc"

    def test_fitness_beats_lex_order(self) -> None:
        """Higher fitness beats lex order: 'zzz' with fitness=1.0 > 'aaa' with fitness=0.1."""
        candidates = [
            _make_candidate("aaa", "trend", 0.1),
            _make_candidate("zzz", "trend", 1.0),
        ]
        admitted, rejected = apply_family_quota(candidates, quota_per_family=1)
        assert admitted[0].canonical == "zzz"
        assert rejected[0].canonical == "aaa"


# ---------------------------------------------------------------------------
# Test 6: quota=1 → only 1 admitted per family
# ---------------------------------------------------------------------------


class TestQuotaOne:
    def test_quota_1_admits_exactly_one_per_family(self) -> None:
        candidates: list[Candidate] = []
        for fam in _FAMILIES:
            for i in range(4):
                candidates.append(_make_candidate(f"{fam}_{i}", fam, float(i)))
        admitted, rejected = apply_family_quota(candidates, quota_per_family=1)
        assert len(admitted) == 5
        assert len(rejected) == 15

    def test_quota_1_best_candidate_admitted(self) -> None:
        candidates = [
            _make_candidate("trend_low", "trend", 0.1),
            _make_candidate("trend_high", "trend", 0.9),
            _make_candidate("trend_mid", "trend", 0.5),
        ]
        admitted, _ = apply_family_quota(candidates, quota_per_family=1)
        assert len(admitted) == 1
        assert admitted[0].canonical == "trend_high"


# ---------------------------------------------------------------------------
# Test: quota >= count → all admitted, none rejected
# ---------------------------------------------------------------------------


class TestQuotaNotExceeded:
    def test_quota_not_reached_no_rejections(self) -> None:
        candidates = [
            _make_candidate("a", "trend", 0.9),
            _make_candidate("b", "trend", 0.5),
        ]
        admitted, rejected = apply_family_quota(candidates, quota_per_family=5)
        assert len(admitted) == 2
        assert len(rejected) == 0

    def test_empty_input_returns_empty(self) -> None:
        admitted, rejected = apply_family_quota([], quota_per_family=5)
        assert admitted == []
        assert rejected == []
