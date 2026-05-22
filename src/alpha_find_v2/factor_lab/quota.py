"""Family quota enforcement for factor mining sandbox.

Selects the top ``quota_per_family`` candidates per family by fitness
(descending), ties broken by ascending node_count then ascending lex
canonical string (R4.8, R6.3).  Excess candidates are marked
``status="rejected_quota"`` (R4.10).  Candidates with ``family=None``
pass through without quota check (R4.7, R4.9).

Requirements: R4.8, R4.9, R4.10
"""

from __future__ import annotations

from collections import defaultdict

from alpha_find_v2.factor_lab.search.beam import Candidate
from alpha_find_v2.factor_lab.search.fitness import sort_key


def apply_family_quota(
    candidates: list[Candidate],
    quota_per_family: int,
) -> tuple[list[Candidate], list[Candidate]]:
    """Apply per-family quota.

    Groups candidates by family, retains the top ``quota_per_family`` per
    family by fitness descending (ties: node_count ascending, then
    lexicographic canonical ascending), and marks the remainder
    ``status="rejected_quota"``.

    Candidates with ``family=None`` are passed through unchanged without
    counting against any quota slot (they are already rejected upstream).

    Args:
        candidates: All candidates to process.
        quota_per_family: Maximum candidates admitted per family.

    Returns:
        (admitted, rejected_quota)
        admitted: Candidates that pass the quota check, including
                  ``family=None`` pass-throughs.
        rejected_quota: Candidates excluded by quota with
                        ``status="rejected_quota"`` set on them.
    """
    admitted: list[Candidate] = []
    rejected_quota: list[Candidate] = []

    family_groups: dict[str, list[Candidate]] = defaultdict(list)

    for c in candidates:
        if c.family is None:
            # Already rejected by family classifier; pass through unchanged.
            admitted.append(c)
        else:
            family_groups[c.family].append(c)

    for group in family_groups.values():
        sorted_group = sorted(
            group,
            key=lambda c: sort_key((c.fitness, c.node_count, c.canonical)),
        )
        for i, candidate in enumerate(sorted_group):
            if i < quota_per_family:
                admitted.append(candidate)
            else:
                candidate.status = "rejected_quota"
                rejected_quota.append(candidate)

    return admitted, rejected_quota
