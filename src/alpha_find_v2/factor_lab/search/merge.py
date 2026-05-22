"""Candidate merge and dedup-by-canonical for factor mining.

Merges beam-search and random-sampling streams into a single deduplicated
list, combining candidates that share a canonical string into one record
whose ``sources`` list contains both ``"beam"`` and ``"random"``.

Also computes accepted-rates for each stream and flags
``beam_underperforms_random`` when the random stream's accepted-rate is
greater than or equal to the beam stream's accepted-rate (R3.7).

"Accepted" proxy at this stage: ``fitness is not None`` (i.e. train IC_IR
was computable).  The OOS acceptance gate is applied later.

Requirements: R3.4, R3.6, R3.7
"""

from __future__ import annotations

from dataclasses import dataclass

from alpha_find_v2.factor_lab.search.beam import Candidate


@dataclass
class MergeResult:
    """Result of merging beam and random candidate streams.

    Attributes:
        candidates: Deduplicated merged list.
        beam_underperforms_random: True when random accepted-rate ≥ beam
            accepted-rate (R3.7).
        beam_accepted_rate: count(beam candidates with fitness is not None)
            / len(beam), or None when beam is empty.
        random_accepted_rate: count(random candidates with fitness is not None)
            / len(random), or None when random is empty.
    """

    candidates: list[Candidate]
    beam_underperforms_random: bool
    beam_accepted_rate: float | None
    random_accepted_rate: float | None


def merge_streams(
    beam: list[Candidate],
    random: list[Candidate],
) -> MergeResult:
    """Merge beam and random candidate streams, deduplicating by canonical string.

    Algorithm:
    1. Build a dict canonical_str → Candidate from the beam stream.
    2. For each random candidate:
       - If its canonical is already in the beam dict, add ``"random"`` to
         the existing beam candidate's sources list (R3.6).
       - Otherwise, include the random candidate in the result as-is.
    3. Final result = all (possibly-updated) beam candidates +
       random-only candidates.
    4. Compute accepted rates and the ``beam_underperforms_random`` flag (R3.7).

    Args:
        beam: Candidates produced by beam search (sources=["beam"]).
        random: Candidates produced by random sampling (sources=["random"]).

    Returns:
        MergeResult with the merged candidate list and warning flag.
    """
    # Step 1: index beam candidates by canonical string.
    beam_by_canonical: dict[str, Candidate] = {c.canonical: c for c in beam}

    # Step 2: iterate random stream; merge overlapping canonicals.
    random_only: list[Candidate] = []
    for rc in random:
        if rc.canonical in beam_by_canonical:
            # Merge: add "random" to the existing beam candidate's sources.
            bc = beam_by_canonical[rc.canonical]
            if "random" not in bc.sources:
                bc.sources.append("random")
        else:
            random_only.append(rc)

    # Step 3: final list — beam candidates (possibly updated) + random-only.
    merged: list[Candidate] = list(beam) + random_only

    # Step 4: compute accepted rates.
    beam_accepted_rate: float | None
    if len(beam) > 0:
        beam_accepted_rate = sum(1 for c in beam if c.fitness is not None) / len(beam)
    else:
        beam_accepted_rate = None

    random_accepted_rate: float | None
    if len(random) > 0:
        random_accepted_rate = sum(1 for c in random if c.fitness is not None) / len(random)
    else:
        random_accepted_rate = None

    beam_underperforms_random = (
        random_accepted_rate is not None
        and beam_accepted_rate is not None
        and random_accepted_rate >= beam_accepted_rate
    )

    return MergeResult(
        candidates=merged,
        beam_underperforms_random=beam_underperforms_random,
        beam_accepted_rate=beam_accepted_rate,
        random_accepted_rate=random_accepted_rate,
    )
