"""Structured rejection records for DSL expression validation.

Requirements: R2.5, R2.6, R2.7, R2.9, R2.10, R2.12
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RejectionRecord:
    """Emitted whenever an expression violates a DSL rule (R2.10).

    Attributes:
        clause_number: The R2 clause number that was violated (e.g. "R2.5").
        position: The offending token/sub-expression in the input string.
        reason: A human-readable description of what rule was violated.
    """

    clause_number: str
    position: str
    reason: str
