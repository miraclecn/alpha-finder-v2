"""Mining config TOML schema, validation, and loader.

Defines frozen dataclasses mirroring all six config tables, plus the
``load_mining_config`` loader that validates and returns the resolved
config with a list of defaulted key paths.

Requirements: R10.1–R10.13, R13.1–R13.3
"""

from __future__ import annotations

import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


# ── Sub-config dataclasses ────────────────────────────────────────────────────


@dataclass(frozen=True)
class SearchConfig:
    """[search] table (R10.1)."""

    beam_width: int = 20
    max_depth: int = 5
    random_sample_size: int = 1000
    seed: int = 42


@dataclass(frozen=True)
class FitnessConfig:
    """[fitness] table (R10.2)."""

    complexity_lambda: float = 0.05


@dataclass(frozen=True)
class FamilyConfig:
    """[family] table (R10.3)."""

    quota_per_family: int = 5


@dataclass(frozen=True)
class WalkForwardConfig:
    """[walk_forward] table (R10.4)."""

    segments: int = 3
    oos_window_months: int = 6
    min_train_months: int = 24
    oos_ic_ir_threshold: float = 0.30
    primary_horizon_days: int = 20


@dataclass(frozen=True)
class DedupConfig:
    """[dedup] table (R10.5)."""

    rho_threshold: float = 0.85
    min_obs: int = 60


@dataclass(frozen=True)
class UniverseConfig:
    """[universe] table (R10.6)."""

    id: str = "investable_a_share_core"


@dataclass(frozen=True)
class MiningConfig:
    """Top-level resolved mining configuration."""

    search: SearchConfig = field(default_factory=SearchConfig)
    fitness: FitnessConfig = field(default_factory=FitnessConfig)
    family: FamilyConfig = field(default_factory=FamilyConfig)
    walk_forward: WalkForwardConfig = field(default_factory=WalkForwardConfig)
    dedup: DedupConfig = field(default_factory=DedupConfig)
    universe: UniverseConfig = field(default_factory=UniverseConfig)

    def to_snapshot_dict(self) -> dict:
        """Return the resolved config as a plain nested dict (R10.13).

        Embedding this dict in ``manifest.json`` under ``config_snapshot``
        lets callers reproduce the identical ``MiningConfig`` by re-parsing
        the snapshot as a TOML document.
        """
        return {
            "search": {
                "beam_width": self.search.beam_width,
                "max_depth": self.search.max_depth,
                "random_sample_size": self.search.random_sample_size,
                "seed": self.search.seed,
            },
            "fitness": {
                "complexity_lambda": self.fitness.complexity_lambda,
            },
            "family": {
                "quota_per_family": self.family.quota_per_family,
            },
            "walk_forward": {
                "segments": self.walk_forward.segments,
                "oos_window_months": self.walk_forward.oos_window_months,
                "min_train_months": self.walk_forward.min_train_months,
                "oos_ic_ir_threshold": self.walk_forward.oos_ic_ir_threshold,
                "primary_horizon_days": self.walk_forward.primary_horizon_days,
            },
            "dedup": {
                "rho_threshold": self.dedup.rho_threshold,
                "min_obs": self.dedup.min_obs,
            },
            "universe": {
                "id": self.universe.id,
            },
        }


# ── Schema descriptor ─────────────────────────────────────────────────────────

# Maps section → key → (expected_python_type, default, min_inclusive, max_inclusive).
# For str fields, min/max are None (no numeric range check).
_SCHEMA: dict[str, dict[str, tuple]] = {
    "search": {
        "beam_width": (int, 20, 1, 1000),
        "max_depth": (int, 5, 1, 5),
        "random_sample_size": (int, 1000, 0, 100_000),
        "seed": (int, 42, 0, 2**32 - 1),
    },
    "fitness": {
        "complexity_lambda": (float, 0.05, 0.0, 1.0),
    },
    "family": {
        "quota_per_family": (int, 5, 1, 50),
    },
    "walk_forward": {
        "segments": (int, 3, 1, 32),
        "oos_window_months": (int, 6, 1, 60),
        "min_train_months": (int, 24, 6, 120),
        "oos_ic_ir_threshold": (float, 0.30, 0.0, 5.0),
        "primary_horizon_days": (int, 20, 1, 250),
    },
    "dedup": {
        "rho_threshold": (float, 0.85, 0.0, 1.0),
        "min_obs": (int, 60, 1, 5000),
    },
    "universe": {
        "id": (str, "investable_a_share_core", None, None),
    },
}

_REGISTERED_UNIVERSE_IDS: frozenset[str] = frozenset(
    {"investable_a_share_core", "csi800"}
)

_GP_RL_PATTERNS: frozenset[str] = frozenset(
    {"gp", "rl", "genetic_programming", "reinforcement_learning"}
)


# ── Internal helpers ──────────────────────────────────────────────────────────


def _fail(msg: str) -> None:
    """Print *msg* to stderr and exit with code 2."""
    print(msg, file=sys.stderr)
    sys.exit(2)


def _scan_r13_strings(raw: dict) -> None:
    """Scan all string values for R13.1 (quality) and R13.2 (GP/RL) before
    any schema validation so the error message is specific rather than generic.
    """
    for section, content in raw.items():
        if not isinstance(content, dict):
            continue
        for key, value in content.items():
            if not isinstance(value, str):
                continue
            normalized = value.strip().lower()
            # R13.1 — quality family deferred
            if normalized == "quality":
                _fail(
                    f"Config error: [{section}].{key} = {value!r}: the 'quality' family "
                    "requires pit_fina_indicator; this family is deferred until the "
                    "Tushare credit prerequisite is met (R13.1)."
                )
            # R13.2 — GP/RL not supported
            if normalized in _GP_RL_PATTERNS:
                _fail(
                    f"Config error: [{section}].{key} = {value!r}: genetic programming "
                    "and reinforcement learning search methods are not supported (R13.2)."
                )


# ── Public API ────────────────────────────────────────────────────────────────


def load_mining_config(path: Path) -> tuple[MiningConfig, list[str]]:
    """Parse and validate a mining-config TOML file.

    Args:
        path: Filesystem path to the ``.toml`` config file.

    Returns:
        ``(MiningConfig, defaults_applied)`` where *defaults_applied* is a
        list of ``"section.key"`` strings for every key that was absent from
        the file and received its schema default (R10.7).

    Side-effects:
        Calls ``sys.exit(2)`` on any validation error, printing a message to
        *stderr* naming the offending key path and the nature of the error
        (R10.8–R10.12, R13.1–R13.3).  Callers never receive an exception;
        they receive either a valid result or no return at all.
    """
    # R10.12 — open and parse
    try:
        with open(path, "rb") as fh:
            raw: dict = tomllib.load(fh)
    except FileNotFoundError:
        _fail(f"Config error: file not found: {path}")
    except tomllib.TOMLDecodeError as exc:
        _fail(f"Config error: invalid TOML in {path}: {exc}")

    # R13.1 / R13.2 — scan string values before schema checks
    _scan_r13_strings(raw)

    # R10.8 — reject unknown top-level sections
    for section in raw:
        if section not in _SCHEMA:
            _fail(f"Config error: unknown section [{section}]")
        if not isinstance(raw[section], dict):
            _fail(
                f"Config error: [{section}] must be a TOML table, "
                f"got {type(raw[section]).__name__}"
            )
        # R10.8 — reject unknown keys within known sections
        for key in raw[section]:
            if key not in _SCHEMA[section]:
                _fail(f"Config error: unknown key [{section}].{key}")

    defaults_applied: list[str] = []
    resolved: dict[str, dict] = {}

    for section, key_specs in _SCHEMA.items():
        section_raw = raw.get(section, {})
        resolved[section] = {}

        for key, (expected_type, default, lo, hi) in key_specs.items():
            key_path = f"{section}.{key}"

            if key not in section_raw:
                # R10.7 — apply default, record key path
                resolved[section][key] = default
                defaults_applied.append(key_path)
                continue

            value = section_raw[key]

            # R10.10 — type check (booleans are int subclass; reject them)
            if expected_type is int:
                if isinstance(value, bool) or not isinstance(value, int):
                    _fail(
                        f"Config error: [{section}].{key} = {value!r}: "
                        f"expected int, got {type(value).__name__}"
                    )
            elif expected_type is float:
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    _fail(
                        f"Config error: [{section}].{key} = {value!r}: "
                        f"expected float, got {type(value).__name__}"
                    )
                value = float(value)  # coerce integer literals to float
            elif expected_type is str:
                if not isinstance(value, str):
                    _fail(
                        f"Config error: [{section}].{key} = {value!r}: "
                        f"expected str, got {type(value).__name__}"
                    )

            # R10.9 — range check
            if lo is not None and value < lo:
                _fail(
                    f"Config error: [{section}].{key} = {value!r}: "
                    f"value {value} is below minimum {lo}"
                )
            if hi is not None and value > hi:
                _fail(
                    f"Config error: [{section}].{key} = {value!r}: "
                    f"value {value} is above maximum {hi}"
                )

            resolved[section][key] = value

    # R13.3 — validate universe id against registered sources
    universe_id: str = resolved["universe"]["id"]
    if universe_id not in _REGISTERED_UNIVERSE_IDS:
        _fail(
            f"Config error: [universe].id = {universe_id!r}: only Stage 1 audited "
            f"research database sources are permitted; registered ids are "
            f"{sorted(_REGISTERED_UNIVERSE_IDS)} (R13.3)."
        )

    config = MiningConfig(
        search=SearchConfig(**resolved["search"]),
        fitness=FitnessConfig(**resolved["fitness"]),
        family=FamilyConfig(**resolved["family"]),
        walk_forward=WalkForwardConfig(**resolved["walk_forward"]),
        dedup=DedupConfig(**resolved["dedup"]),
        universe=UniverseConfig(**resolved["universe"]),
    )
    return config, defaults_applied
