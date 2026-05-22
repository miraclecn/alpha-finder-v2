"""Git metadata helpers for reproducibility tracking (R12.4, R12.5, R12.6)."""

import subprocess
import sys
from pathlib import Path


class GitMetadataError(Exception):
    """Raised when git SHA cannot be resolved (R12.6)."""


def resolve_git_sha(repo_root: Path) -> tuple[str, bool]:
    """Return (sha, is_dirty) for the HEAD commit of *repo_root*.

    Args:
        repo_root: Path passed as ``-C`` to git.

    Returns:
        sha: 40-char lowercase hex string of HEAD.
        is_dirty: True if working tree has uncommitted changes.

    Raises:
        GitMetadataError: if *repo_root* is not inside a git repository.

    Side effects:
        If is_dirty, prints a ``dirty_working_tree`` warning to stderr.
    """
    try:
        sha = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except subprocess.CalledProcessError as exc:
        raise GitMetadataError(
            f"Cannot resolve git SHA for reproducibility metadata: {exc}"
        ) from exc

    status = subprocess.run(
        ["git", "-C", str(repo_root), "status", "--porcelain"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    is_dirty = bool(status.strip())
    if is_dirty:
        print(
            "dirty_working_tree warning: working tree has uncommitted changes; "
            "git_sha will be recorded as '<sha>-dirty'.",
            file=sys.stderr,
        )

    return sha, is_dirty
