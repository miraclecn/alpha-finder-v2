import unittest
from pathlib import Path

from alpha_find_v2.factor_lab.isolation import OUTPUT_ROOT, assert_output_root_safe


class TestAssertOutputRootSafe(unittest.TestCase):
    def test_exits_6_for_tmp_path(self) -> None:
        with self.assertRaises(SystemExit) as ctx:
            assert_output_root_safe(Path("/tmp/foo"))
        self.assertEqual(ctx.exception.code, 6)

    def test_passes_for_valid_subpath(self) -> None:
        # Should not raise or exit.
        assert_output_root_safe(OUTPUT_ROOT / "runs" / "abc")

    def test_output_root_constant(self) -> None:
        self.assertEqual(OUTPUT_ROOT, Path("output/factor_lab"))


if __name__ == "__main__":
    unittest.main()
