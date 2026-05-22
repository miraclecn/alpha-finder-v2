"""Config validation integration checks — R10 and R13 key contracts.

Thin complement to tests/test_mining_config.py.  Each test maps directly to
one acceptance criterion so spec traceability is unambiguous.

**Validates: Requirements R10.7, R10.8, R10.9, R10.10, R10.11, R10.13,
             R13.1, R13.2, R13.3, R13.4**
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from alpha_find_v2.factor_lab.config import load_mining_config


def _toml(content: str) -> Path:
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".toml", delete=False, encoding="utf-8"
    )
    tmp.write(content)
    tmp.flush()
    tmp.close()
    return Path(tmp.name)


def _dict_to_toml(d: dict) -> str:
    lines = []
    for section, contents in d.items():
        lines.append(f"[{section}]")
        for k, v in contents.items():
            lines.append(f'{k} = "{v}"' if isinstance(v, str) else f"{k} = {v}")
    return "\n".join(lines) + "\n"


class TestR107DefaultApplied(unittest.TestCase):
    """R10.7 — omitted key gets default; path appears in defaults_applied."""

    def test_missing_beam_width_defaulted(self) -> None:
        path = _toml("")
        try:
            config, defaults = load_mining_config(path)
            self.assertEqual(config.search.beam_width, 20)
            self.assertIn("search.beam_width", defaults)
        finally:
            path.unlink(missing_ok=True)


class TestR108UnknownKeyRejected(unittest.TestCase):
    """R10.8 — unknown key in a known section → SystemExit(2)."""

    def test_unknown_key_exits_2(self) -> None:
        path = _toml("[search]\nunknown_param = 99\n")
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)


class TestR109OutOfRangeRejected(unittest.TestCase):
    """R10.9 — out-of-range value → SystemExit(2)."""

    def test_beam_width_zero_exits_2(self) -> None:
        path = _toml("[search]\nbeam_width = 0\n")
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)


class TestR1010TypeMismatchRejected(unittest.TestCase):
    """R10.10 — type mismatch → SystemExit(2)."""

    def test_string_for_int_exits_2(self) -> None:
        path = _toml('[search]\nbeam_width = "twenty"\n')
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)


class TestR1011R131QualityRejected(unittest.TestCase):
    """R10.11 / R13.1 — 'quality' identifier rejected (case-insensitive)."""

    def _assert_exits_2(self, value: str) -> None:
        path = _toml(f'[universe]\nid = "{value}"\n')
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)

    def test_quality_lowercase(self) -> None:
        self._assert_exits_2("quality")

    def test_quality_uppercase(self) -> None:
        self._assert_exits_2("QUALITY")

    def test_quality_with_whitespace(self) -> None:
        self._assert_exits_2("  quality  ")


class TestR132GpRlRejected(unittest.TestCase):
    """R13.2 — GP/RL search method identifiers rejected (case-insensitive)."""

    def _assert_exits_2(self, value: str) -> None:
        path = _toml(f'[universe]\nid = "{value}"\n')
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)

    def test_gp_rejected(self) -> None:
        self._assert_exits_2("gp")

    def test_rl_rejected(self) -> None:
        self._assert_exits_2("rl")

    def test_genetic_programming_rejected(self) -> None:
        self._assert_exits_2("genetic_programming")

    def test_reinforcement_learning_rejected(self) -> None:
        self._assert_exits_2("reinforcement_learning")

    def test_gp_case_insensitive(self) -> None:
        self._assert_exits_2("GP")


class TestR133UnregisteredSourceRejected(unittest.TestCase):
    """R13.3 — non-registered universe id → SystemExit(2)."""

    def test_unknown_universe_id_exits_2(self) -> None:
        path = _toml('[universe]\nid = "intraday_bars"\n')
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)

    def test_registered_ids_accepted(self) -> None:
        for uid in ("investable_a_share_core", "csi800"):
            path = _toml(f'[universe]\nid = "{uid}"\n')
            try:
                config, _ = load_mining_config(path)
                self.assertEqual(config.universe.id, uid)
            finally:
                path.unlink(missing_ok=True)


class TestR134UnknownOutputKeyRejected(unittest.TestCase):
    """R13.4 — unknown output key rejected via R10.8 (unknown key in known section)."""

    def test_unknown_section_exits_2(self) -> None:
        path = _toml("[output]\npath = '/tmp/results'\n")
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)


class TestR1013SnapshotRoundTrip(unittest.TestCase):
    """R10.13 — config_snapshot round-trip preserves exact resolved config."""

    def test_round_trip_preserves_config(self) -> None:
        original_toml = '[universe]\nid = "csi800"\n[search]\nbeam_width = 30\n'
        path = _toml(original_toml)
        try:
            config, _ = load_mining_config(path)
            snapshot = config.to_snapshot_dict()
            path2 = _toml(_dict_to_toml(snapshot))
            try:
                config2, _ = load_mining_config(path2)
            finally:
                path2.unlink(missing_ok=True)
            self.assertEqual(config, config2)
        finally:
            path.unlink(missing_ok=True)

    def test_snapshot_covers_all_sections(self) -> None:
        path = _toml("")
        try:
            config, _ = load_mining_config(path)
            snap = config.to_snapshot_dict()
            self.assertEqual(
                set(snap),
                {"search", "fitness", "family", "walk_forward", "dedup", "universe"},
            )
        finally:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
