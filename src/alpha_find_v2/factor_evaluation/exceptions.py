"""
Custom exceptions for the factor_evaluation package.
"""
from __future__ import annotations


class DescriptorNotImplemented(Exception):
    """
    Raised when a stub descriptor is called before its data prerequisites
    (typically 5000-credit Tushare datasets) are available.

    Attributes:
        descriptor_id: The id of the unimplemented descriptor.
        requires: Tuple of dataset ids needed for the implementation.
        message: Human-readable explanation.
    """

    def __init__(
        self,
        descriptor_id: str,
        requires: tuple[str, ...],
        message: str = "",
    ) -> None:
        self.descriptor_id = descriptor_id
        self.requires = requires
        self.message = message or (
            f"Descriptor '{descriptor_id}' is not yet implemented. "
            f"Required datasets: {', '.join(requires)}"
        )
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message


class UniverseEmpty(Exception):
    """
    Raised when the resolved universe is empty for the entire evaluation window.
    The evaluation cannot proceed without at least one valid trade date.
    """

    def __init__(self, universe_id: str, start_date: str, end_date: str) -> None:
        self.universe_id = universe_id
        self.start_date = start_date
        self.end_date = end_date
        super().__init__(
            f"Universe '{universe_id}' resolved to zero securities "
            f"over the window {start_date}–{end_date}."
        )


class EvaluationError(Exception):
    """
    Generic evaluation-level error (e.g. SQL failure, missing research DB).
    Carries an exit-code hint that the CLI can translate directly.
    """

    def __init__(self, message: str, exit_code: int = 6) -> None:
        self.exit_code = exit_code
        super().__init__(message)
