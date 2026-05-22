"""Unit tests for factor_lab.search.merge.

Verification target: Task 11 (R3.4, R3.6, R3.7)

Covers:
1. Overlapping canonical → merged candidate with sources=["beam","random"].
2. Non-overlapping → beam sources=["beam"], random sources=["random"].
3. All beam candidates appear in result.
4. All random-only candidates appear in result.
5. No duplicates in result (unique canonicals).
6. beam_underperforms_random=True when random_accepted_rate > beam_accepted_rate.
7. beam_underperforms_random=False when beam_accepted_rate > random_accepted_rate.
8. beam_underperforms_random=True when rates are equal.
"""

from __future__ import annotations

import uuid

import pytest

from alpha_find_v2.factor_lab.dsl.grammar import Leaf
from alpha_find_v2.factor_lab.search.beam import Candidate
from alpha_find_v2.factor_lab.search.merge import MergeResult, merge_streams

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EXPR_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")


def _expr_id(canonical_str: str) -> str:
    return uuid.uuid5(_EXPR_NAMESPACE, canonical_str).hex


def _make_candidate(
    canonical_str: str,
    sources: list[str],
    fitness: float | None = 1.0,
) -> Candidate:
    """Build a minimal Candidate with a real Leaf AST so canonical matches."""
    ast = Leaf(field="close_adj")  # AST doesn't matter for merge tests; canonical is set directly
    cand = Candidate(
        expr_id=_expr_id(canonical_str),
        canonical=canonical_str,
        ast=ast,
        node_count=1,
        family="trend",
        sources=list(sources),
        train_ic_ir=fitness,
        fitness=fitness,
        status="pending",
        oos_segments=[],
    )
    return cand


# ---------------------------------------------------------------------------
# R3.6: Dedup by canonical string
# ---------------------------------------------------------------------------


class TestDeduplication:
    def test_overlapping_canonical_merges_sources(self) -> None:
        """Same canonical in both streams → one candidate with sources=['beam','random']."""
        beam = [_make_candidate("close_adj", ["beam"])]
        random = [_make_candidate("close_adj", ["random"])]

        result = merge_streams(beam, random)

        assert len(result.candidates) == 1
        merged = result.candidates[0]
        assert "beam" in merged.sources
        assert "random" in merged.sources

    def test_overlapping_canonical_keeps_beam_fields(self) -> None:
        """Merged candidate retains beam candidate's fitness and other fields."""
        beam_cand = _make_candidate("close_adj", ["beam"], fitness=0.8)
        random_cand = _make_candidate("close_adj", ["random"], fitness=0.3)

        result = merge_streams([beam_cand], [random_cand])

        merged = result.candidates[0]
        assert merged.fitness == 0.8  # beam field retained

    def test_non_overlapping_beam_keeps_beam_source(self) -> None:
        """Beam-only canonical retains sources=['beam']."""
        beam = [_make_candidate("close_adj", ["beam"])]
        random = [_make_candidate("open", ["random"])]

        result = merge_streams(beam, random)

        beam_cands = [c for c in result.candidates if c.canonical == "close_adj"]
        assert len(beam_cands) == 1
        assert beam_cands[0].sources == ["beam"]

    def test_non_overlapping_random_keeps_random_source(self) -> None:
        """Random-only canonical retains sources=['random']."""
        beam = [_make_candidate("close_adj", ["beam"])]
        random = [_make_candidate("open", ["random"])]

        result = merge_streams(beam, random)

        rand_cands = [c for c in result.candidates if c.canonical == "open"]
        assert len(rand_cands) == 1
        assert rand_cands[0].sources == ["random"]

    def test_all_beam_candidates_in_result(self) -> None:
        """Every beam candidate appears in the merged result."""
        beam = [
            _make_candidate("close_adj", ["beam"]),
            _make_candidate("open", ["beam"]),
            _make_candidate("pe", ["beam"]),
        ]
        random = [_make_candidate("pb", ["random"])]

        result = merge_streams(beam, random)

        result_canonicals = {c.canonical for c in result.candidates}
        for bc in beam:
            assert bc.canonical in result_canonicals

    def test_all_random_only_candidates_in_result(self) -> None:
        """Random-only candidates (no beam overlap) appear in result."""
        beam = [_make_candidate("close_adj", ["beam"])]
        random = [
            _make_candidate("open", ["random"]),
            _make_candidate("pe", ["random"]),
        ]

        result = merge_streams(beam, random)

        result_canonicals = {c.canonical for c in result.candidates}
        assert "open" in result_canonicals
        assert "pe" in result_canonicals

    def test_no_duplicates_in_result(self) -> None:
        """No canonical string appears more than once in the merged list."""
        beam = [
            _make_candidate("close_adj", ["beam"]),
            _make_candidate("open", ["beam"]),
        ]
        random = [
            _make_candidate("close_adj", ["random"]),  # overlaps beam
            _make_candidate("pe", ["random"]),
        ]

        result = merge_streams(beam, random)

        canonicals = [c.canonical for c in result.candidates]
        assert len(canonicals) == len(set(canonicals))

    def test_total_count_correct_with_full_overlap(self) -> None:
        """When all random canonicals overlap beam, total equals beam count."""
        beam = [
            _make_candidate("close_adj", ["beam"]),
            _make_candidate("open", ["beam"]),
        ]
        random = [
            _make_candidate("close_adj", ["random"]),
            _make_candidate("open", ["random"]),
        ]

        result = merge_streams(beam, random)

        assert len(result.candidates) == 2

    def test_total_count_correct_with_no_overlap(self) -> None:
        """When no canonicals overlap, total equals beam + random count."""
        beam = [_make_candidate("close_adj", ["beam"]), _make_candidate("open", ["beam"])]
        random = [_make_candidate("pe", ["random"]), _make_candidate("pb", ["random"])]

        result = merge_streams(beam, random)

        assert len(result.candidates) == 4


# ---------------------------------------------------------------------------
# R3.7: beam_underperforms_random warning
# ---------------------------------------------------------------------------


class TestBeamUnderperformsWarning:
    def test_warning_true_when_random_rate_greater(self) -> None:
        """beam_underperforms_random=True when random_accepted_rate > beam_accepted_rate."""
        # beam: 1/2 accepted
        beam = [
            _make_candidate("close_adj", ["beam"], fitness=1.0),
            _make_candidate("open", ["beam"], fitness=None),
        ]
        # random (no overlap): 2/2 accepted
        random = [
            _make_candidate("pe", ["random"], fitness=0.5),
            _make_candidate("pb", ["random"], fitness=0.3),
        ]

        result = merge_streams(beam, random)

        assert result.beam_underperforms_random is True

    def test_warning_false_when_beam_rate_greater(self) -> None:
        """beam_underperforms_random=False when beam_accepted_rate > random_accepted_rate."""
        # beam: 2/2 accepted
        beam = [
            _make_candidate("close_adj", ["beam"], fitness=1.0),
            _make_candidate("open", ["beam"], fitness=0.5),
        ]
        # random (no overlap): 1/2 accepted
        random = [
            _make_candidate("pe", ["random"], fitness=0.5),
            _make_candidate("pb", ["random"], fitness=None),
        ]

        result = merge_streams(beam, random)

        assert result.beam_underperforms_random is False

    def test_warning_true_when_rates_equal(self) -> None:
        """beam_underperforms_random=True when random_accepted_rate == beam_accepted_rate."""
        # beam: 1/2 = 0.5
        beam = [
            _make_candidate("close_adj", ["beam"], fitness=1.0),
            _make_candidate("open", ["beam"], fitness=None),
        ]
        # random (no overlap): 1/2 = 0.5
        random = [
            _make_candidate("pe", ["random"], fitness=0.5),
            _make_candidate("pb", ["random"], fitness=None),
        ]

        result = merge_streams(beam, random)

        assert result.beam_underperforms_random is True

    def test_accepted_rates_computed_correctly(self) -> None:
        """beam_accepted_rate and random_accepted_rate match manual counts."""
        beam = [
            _make_candidate("close_adj", ["beam"], fitness=1.0),
            _make_candidate("open", ["beam"], fitness=None),
            _make_candidate("pe", ["beam"], fitness=0.3),
        ]
        # no overlap with random
        random = [
            _make_candidate("pb", ["random"], fitness=0.2),
            _make_candidate("turnover_value_cny", ["random"], fitness=None),
        ]

        result = merge_streams(beam, random)

        assert result.beam_accepted_rate == pytest.approx(2 / 3)
        assert result.random_accepted_rate == pytest.approx(1 / 2)

    def test_accepted_rate_none_when_beam_empty(self) -> None:
        """beam_accepted_rate is None when beam is empty."""
        random = [_make_candidate("close_adj", ["random"], fitness=1.0)]

        result = merge_streams([], random)

        assert result.beam_accepted_rate is None
        assert result.beam_underperforms_random is False

    def test_accepted_rate_none_when_random_empty(self) -> None:
        """random_accepted_rate is None when random is empty."""
        beam = [_make_candidate("close_adj", ["beam"], fitness=1.0)]

        result = merge_streams(beam, [])

        assert result.random_accepted_rate is None
        assert result.beam_underperforms_random is False

    def test_both_empty_returns_empty_no_warning(self) -> None:
        """Both empty streams → empty result, no warning."""
        result = merge_streams([], [])

        assert result.candidates == []
        assert result.beam_underperforms_random is False
        assert result.beam_accepted_rate is None
        assert result.random_accepted_rate is None

    def test_return_type_is_merge_result(self) -> None:
        """merge_streams returns a MergeResult."""
        result = merge_streams([], [])
        assert isinstance(result, MergeResult)
