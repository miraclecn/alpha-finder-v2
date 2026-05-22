"""
Configuration models for config/data_sources.toml.

Parses the TOML configuration file and provides typed access to adapter
settings and per-dataset enable/priority configuration.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomllib  # type: ignore[no-redef]
    except ImportError:
        import tomli as tomllib  # type: ignore[no-redef]

from .schemas import ALL_DATASET_IDS

_VALID_CREDIT_TIERS: frozenset[int] = frozenset({120, 2000, 5000})
_SCHEMA_VERSION: int = 1


@dataclass(frozen=True, slots=True)
class AdapterConfig:
    name: str
    enabled: bool
    calls_per_minute: int
    calls_per_day: int  # 0 = unlimited


@dataclass(frozen=True, slots=True)
class DatasetConfig:
    dataset_id: str
    enabled: bool
    credit_tier: int
    priority: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DataSourcesConfig:
    schema_version: int
    adapters: dict[str, AdapterConfig]
    datasets: dict[str, DatasetConfig]

    def priority(self, dataset_id: str) -> tuple[str, ...]:
        """Return enabled adapter names for dataset_id in declared priority order."""
        cfg = self.datasets.get(dataset_id)
        if cfg is None or not cfg.enabled:
            return ()
        return tuple(
            name
            for name in cfg.priority
            if name in self.adapters and self.adapters[name].enabled
        )

    def enabled_datasets(self) -> tuple[str, ...]:
        """Return enabled dataset ids in canonical execution order."""
        return tuple(d for d in ALL_DATASET_IDS if self.datasets.get(d, _disabled_dataset(d)).enabled)


def _disabled_dataset(dataset_id: str) -> DatasetConfig:
    return DatasetConfig(dataset_id=dataset_id, enabled=False, credit_tier=120, priority=())


def load_data_sources_config(path: Path) -> DataSourcesConfig:
    """Parse config/data_sources.toml and return a validated DataSourcesConfig."""
    with open(path, "rb") as fh:
        raw: dict[str, Any] = tomllib.load(fh)

    # Schema version check
    version = raw.get("schema_version")
    if version != _SCHEMA_VERSION:
        raise ValueError(
            f"data_sources.toml schema_version must be {_SCHEMA_VERSION}, got {version!r}"
        )

    # Parse adapters
    adapters_raw = raw.get("adapter", {})
    adapters: dict[str, AdapterConfig] = {}
    for name, cfg in adapters_raw.items():
        adapters[name] = AdapterConfig(
            name=name,
            enabled=bool(cfg.get("enabled", True)),
            calls_per_minute=int(cfg.get("calls_per_minute", 60)),
            calls_per_day=int(cfg.get("calls_per_day", 0)),
        )

    # Parse datasets
    datasets_raw = raw.get("datasets", {})
    datasets: dict[str, DatasetConfig] = {}
    for ds_id, cfg in datasets_raw.items():
        priority_list: list[str] = cfg.get("priority", [])
        # Validate: every priority adapter must be declared in [adapter.*]
        for adapter_name in priority_list:
            if adapter_name not in adapters:
                raise ValueError(
                    f"Dataset '{ds_id}' references unknown adapter '{adapter_name}' "
                    f"in priority list. Declared adapters: {sorted(adapters)}"
                )
        credit_tier = int(cfg.get("credit_tier", 120))
        if credit_tier not in _VALID_CREDIT_TIERS:
            raise ValueError(
                f"Dataset '{ds_id}' has invalid credit_tier {credit_tier}. "
                f"Must be one of {sorted(_VALID_CREDIT_TIERS)}."
            )
        datasets[ds_id] = DatasetConfig(
            dataset_id=ds_id,
            enabled=bool(cfg.get("enabled", True)),
            credit_tier=credit_tier,
            priority=tuple(priority_list),
        )

    return DataSourcesConfig(
        schema_version=version,
        adapters=adapters,
        datasets=datasets,
    )
