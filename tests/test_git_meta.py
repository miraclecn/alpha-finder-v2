import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from alpha_find_v2.factor_lab.git_meta import GitMetadataError, resolve_git_sha

_SHA = "a" * 40  # valid 40-char hex SHA


def _make_result(stdout: str) -> MagicMock:
    result = MagicMock()
    result.stdout = stdout
    return result


class TestResolveGitSha(unittest.TestCase):
    def test_clean_repo(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                _make_result(_SHA + "\n"),  # rev-parse HEAD
                _make_result(""),            # status --porcelain (clean)
            ]
            sha, is_dirty = resolve_git_sha(Path("/repo"))

        self.assertEqual(sha, _SHA)
        self.assertFalse(is_dirty)

    def test_dirty_repo_returns_true_and_warns(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                _make_result(_SHA + "\n"),
                _make_result(" M src/foo.py\n"),
            ]
            with patch("sys.stderr") as mock_stderr:
                sha, is_dirty = resolve_git_sha(Path("/repo"))

        self.assertEqual(sha, _SHA)
        self.assertTrue(is_dirty)
        # warning must reach stderr
        mock_stderr.write.assert_called()
        written = "".join(call.args[0] for call in mock_stderr.write.call_args_list)
        self.assertIn("dirty_working_tree", written)

    def test_no_git_repo_raises(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(128, "git")
            with self.assertRaises(GitMetadataError):
                resolve_git_sha(Path("/not-a-repo"))

    def test_sha_is_exactly_40_chars(self) -> None:
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                _make_result(_SHA + "\n"),
                _make_result(""),
            ]
            sha, _ = resolve_git_sha(Path("/repo"))

        self.assertEqual(len(sha), 40)
        # caller would form the dirty variant like this:
        self.assertEqual(sha + "-dirty", _SHA + "-dirty")


if __name__ == "__main__":
    unittest.main()
