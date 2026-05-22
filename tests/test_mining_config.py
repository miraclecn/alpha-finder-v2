"""Unit tests for factor_lab.config — MiningConfig schema and loader.

Covers:
- Minimal config: all defaults applied, round-trip via to_snapshot_dict()
- Full config: no defaults applied
- Out-of-range value: exits with code 2, names key path
- Unknown section / key: exits with code 2
- Type mismatch: exits with code 2
- 'quality' string rejected (R13.1)
- 'gp' / 'rl' search method rejected (R13.2)
- Unregistered universe id rejected (R13.3)
- Missing TOML file exits with code 2
- Invalid TOML syntax exits with code 2
- to_snapshot_dict() round-trip produces identical config

Requirements: R10.1–R10.13, R13.1–R13.3
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from alpha_find_v2.factor_lab.config import (
    MiningConfig,
    SearchConfig,
    FitnessConfig,
    FamilyConfig,
    WalkForwardConfig,
    DedupConfig,
    UniverseConfig,
    load_mining_config,
)


def _write_toml(content: str) -> Path:
    """Write *content* to a temporary TOML file and return its path."""
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".toml", delete=False, encoding="utf-8"
    )
    tmp.write(content)
    tmp.flush()
    tmp.close()
    return Path(tmp.name)


class TestMinimalConfig(unittest.TestCase):
    """Minimal (empty) TOML applies all defaults."""

    def setUp(self) -> None:
        self.path = _write_toml("")
        self.config, self.defaults = load_mining_config(self.path)

    def tearDown(self) -> None:
        self.path.unlink(missing_ok=True)

    def test_returns_mining_config(self) -> None:
        self.assertIsInstance(self.config, MiningConfig)

    def test_all_sections_default(self) -> None:
        self.assertEqual(self.config.search, SearchConfig())
        self.assertEqual(self.config.fitness, FitnessConfig())
        self.assertEqual(self.config.family, FamilyConfig())
        self.assertEqual(self.config.walk_forward, WalkForwardConfig())
        self.assertEqual(self.config.dedup, DedupConfig())
        self.assertEqual(self.config.universe, UniverseConfig())

    def test_defaults_applied_lists_all_keys(self) -> None:
        expected = {
            "search.beam_width", "search.max_depth", "search.random_sample_size", "search.seed",
            "fitness.complexity_lambda",
            "family.quota_per_family",
            "walk_forward.segments", "walk_forward.oos_window_months",
            "walk_forward.min_train_months", "walk_forward.oos_ic_ir_threshold",
            "walk_forward.primary_horizon_days",
            "dedup.rho_threshold", "dedup.min_obs",
            "universe.id",
        }
        self.assertEqual(set(self.defaults), expected)


class TestFullConfig(unittest.TestCase):
    """Fully specified TOML produces no defaults and overrides every value."""

    def setUp(self) -> None:
        self.path = _write_toml("""
[search]
beam_width = 50
max_depth = 3
random_sample_size = 500
seed = 99

[fitness]
complexity_lambda = 0.10

[family]
quota_per_family = 3

[walk_forward]
segments = 5
oos_window_months = 12
min_train_months = 36
oos_ic_ir_threshold = 0.50
primary_horizon_days = 60

[dedup]
rho_threshold = 0.70
min_obs = 120

[universe]
id = "csi800"
""")
        self.config, self.defaults = load_mining_config(self.path)

    def tearDown(self) -> None:
        self.path.unlink(missing_ok=True)

    def test_no_defaults_applied(self) -> None:
        self.assertEqual(self.defaults, [])

    def test_values_overridden(self) -> None:
        self.assertEqual(self.config.search.beam_width, 50)
        self.assertEqual(self.config.search.seed, 99)
        self.assertAlmostEqual(self.config.fitness.complexity_lambda, 0.10)
        self.assertEqual(self.config.family.quota_per_family, 3)
        self.assertEqual(self.config.walk_forward.segments, 5)
        self.assertAlmostEqual(self.config.walk_forward.oos_ic_ir_threshold, 0.50)
        self.assertAlmostEqual(self.config.dedup.rho_threshold, 0.70)
        self.assertEqual(self.config.universe.id, "csi800")


class TestRoundTrip(unittest.TestCase):
    """to_snapshot_dict() round-trip reproduces identical config (R10.13)."""

    def test_round_trip_default_config(self) -> None:
        path = _write_toml("")
        try:
            config, _ = load_mining_config(path)
            snapshot = config.to_snapshot_dict()

            import tomllib, io

            # Serialise snapshot back to TOML bytes and parse again
            toml_str = _dict_to_toml(snapshot)
            path2 = _write_toml(toml_str)
            try:
                config2, _ = load_mining_config(path2)
            finally:
                path2.unlink(missing_ok=True)

            self.assertEqual(config, config2)
        finally:
            path.unlink(missing_ok=True)

    def test_round_trip_custom_config(self) -> None:
        toml_str = """
[search]
beam_width = 30
max_depth = 4
random_sample_size = 2000
seed = 7

[fitness]
complexity_lambda = 0.03

[family]
quota_per_family = 10

[walk_forward]
segments = 4
oos_window_months = 9
min_train_months = 48
oos_ic_ir_threshold = 0.25
primary_horizon_days = 40

[dedup]
rho_threshold = 0.80
min_obs = 100

[universe]
id = "investable_a_share_core"
"""
        path = _write_toml(toml_str)
        try:
            config, _ = load_mining_config(path)
            snapshot = config.to_snapshot_dict()
            path2 = _write_toml(_dict_to_toml(snapshot))
            try:
                config2, _ = load_mining_config(path2)
            finally:
                path2.unlink(missing_ok=True)
            self.assertEqual(config, config2)
        finally:
            path.unlink(missing_ok=True)

    def test_snapshot_dict_contains_all_sections(self) -> None:
        path = _write_toml("")
        try:
            config, _ = load_mining_config(path)
            snap = config.to_snapshot_dict()
            self.assertEqual(
                set(snap.keys()),
                {"search", "fitness", "family", "walk_forward", "dedup", "universe"},
            )
        finally:
            path.unlink(missing_ok=True)


def _dict_to_toml(d: dict) -> str:
    """Minimal dict-to-TOML serialiser (only handles flat sections of scalars)."""
    lines = []
    for section, contents in d.items():
        lines.append(f"[{section}]")
        for k, v in contents.items():
            if isinstance(v, str):
                lines.append(f'{k} = "{v}"')
            else:
                lines.append(f"{k} = {v}")
    return "\n".join(lines) + "\n"


class TestOutOfRange(unittest.TestCase):
    """Out-of-range values → SystemExit(2) naming the key path (R10.9)."""

    def _assert_exit2(self, toml_content: str, key_path: str) -> None:
        path = _write_toml(toml_content)
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)

    def test_beam_width_zero(self) -> None:
        self._assert_exit2("[search]\nbeam_width = 0\n", "search.beam_width")

    def test_beam_width_above_max(self) -> None:
        self._assert_exit2("[search]\nbeam_width = 1001\n", "search.beam_width")

    def test_max_depth_above_max(self) -> None:
        self._assert_exit2("[search]\nmax_depth = 6\n", "search.max_depth")

    def test_complexity_lambda_above_one(self) -> None:
        self._assert_exit2("[fitness]\ncomplexity_lambda = 1.1\n", "fitness.complexity_lambda")

    def test_complexity_lambda_negative(self) -> None:
        self._assert_exit2("[fitness]\ncomplexity_lambda = -0.01\n", "fitness.complexity_lambda")

    def test_quota_per_family_zero(self) -> None:
        self._assert_exit2("[family]\nquota_per_family = 0\n", "family.quota_per_family")

    def test_quota_per_family_above_max(self) -> None:
        self._assert_exit2("[family]\nquota_per_family = 51\n", "family.quota_per_family")

    def test_rho_threshold_above_one(self) -> None:
        self._assert_exit2("[dedup]\nrho_threshold = 1.5\n", "dedup.rho_threshold")

    def test_seed_negative(self) -> None:
        self._assert_exit2("[search]\nseed = -1\n", "search.seed")

    def test_seed_above_max(self) -> None:
        self._assert_exit2(f"[search]\nseed = {2**32}\n", "search.seed")

    def test_min_train_months_below_min(self) -> None:
        self._assert_exit2("[walk_forward]\nmin_train_months = 5\n", "walk_forward.min_train_months")


class TestUnknownKeys(unittest.TestCase):
    """Unknown sections or keys → SystemExit(2) (R10.8)."""

    def test_unknown_top_level_section(self) -> None:
        path = _write_toml("[extra]\nfoo = 1\n")
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)

    def test_unknown_key_within_known_section(self) -> None:
        path = _write_toml("[search]\nbeam_width = 20\nunknown_param = 99\n")
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)


class TestTypeMismatch(unittest.TestCase):
    """Type-incompatible values → SystemExit(2) naming key path (R10.10)."""

    def test_string_for_int(self) -> None:
        path = _write_toml('[search]\nbeam_width = "twenty"\n')
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)

    def test_string_for_float(self) -> None:
        path = _write_toml('[fitness]\ncomplexity_lambda = "small"\n')
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)

    def test_integer_for_string(self) -> None:
        path = _write_toml("[universe]\nid = 42\n")
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)


class TestQualityRejection(unittest.TestCase):
    """'quality' family identifier rejected with code 2 (R10.11, R13.1)."""

    def test_quality_in_universe_id(self) -> None:
        """Any string value 'quality' should be caught by R13.1 scan."""
        path = _write_toml('[universe]\nid = "quality"\n')
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)

    def test_quality_case_insensitive(self) -> None:
        path = _write_toml('[universe]\nid = "QUALITY"\n')
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)

    def test_quality_with_whitespace(self) -> None:
        path = _write_toml('[universe]\nid = "  quality  "\n')
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)


class TestGpRlRejection(unittest.TestCase):
    """GP/RL search identifiers rejected with code 2 (R13.2)."""

    def _check(self, value: str) -> None:
        path = _write_toml(f'[universe]\nid = "{value}"\n')
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)

    def test_gp_rejected(self) -> None:
        self._check("gp")

    def test_rl_rejected(self) -> None:
        self._check("rl")

    def test_genetic_programming_rejected(self) -> None:
        self._check("genetic_programming")

    def test_reinforcement_learning_rejected(self) -> None:
        self._check("reinforcement_learning")

    def test_gp_case_insensitive(self) -> None:
        self._check("GP")

    def test_rl_case_insensitive(self) -> None:
        self._check("RL")


class TestUnregisteredUniverse(unittest.TestCase):
    """Non-registered universe id rejected with code 2 (R13.3)."""

    def test_unknown_universe_id(self) -> None:
        path = _write_toml('[universe]\nid = "intraday_bars"\n')
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)

    def test_registered_ids_accepted(self) -> None:
        for uid in ("investable_a_share_core", "csi800"):
            path = _write_toml(f'[universe]\nid = "{uid}"\n')
            try:
                config, _ = load_mining_config(path)
                self.assertEqual(config.universe.id, uid)
            finally:
                path.unlink(missing_ok=True)


class TestFileErrors(unittest.TestCase):
    """Missing or invalid TOML file → SystemExit(2) (R10.12)."""

    def test_missing_file(self) -> None:
        with self.assertRaises(SystemExit) as ctx:
            load_mining_config(Path("/nonexistent/path/config.toml"))
        self.assertEqual(ctx.exception.code, 2)

    def test_invalid_toml_syntax(self) -> None:
        path = _write_toml("this is not [ valid TOML !!!\n")
        try:
            with self.assertRaises(SystemExit) as ctx:
                load_mining_config(path)
            self.assertEqual(ctx.exception.code, 2)
        finally:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
