"""Isolation guard for the factor_lab sandbox.

Ensures all output is written exclusively under output/factor_lab/,
as required by R8.5. Call assert_output_root_safe() at startup.
"""

import sys
from pathlib import Path

OUTPUT_ROOT: Path = Path("output/factor_lab")


def assert_output_root_safe(path: Path) -> None:
    """Exit with code 6 if *path* is not under output/factor_lab/.

    Resolves symbolic links and ``..`` segments before the check so that
    tricks like ``output/factor_lab/../../etc`` are caught.
    """
    resolved = path.resolve()
    allowed = OUTPUT_ROOT.resolve()
    try:
        resolved.relative_to(allowed)
    except ValueError:
        print(
            f"Isolation violation: resolved path '{resolved}' is not under "
            f"'{allowed}'. Refusing to proceed.",
            file=sys.stderr,
        )
        sys.exit(6)
