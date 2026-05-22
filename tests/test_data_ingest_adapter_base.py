"""
Tests for adapters/base.py — DataSourceAdapter protocol, DatasetSpec, STATIC_SPECS.

Validates:
- STATIC_SPECS covers exactly the 18 dataset ids from ALL_DATASET_IDS
- A no-op class satisfying name/supports()/fetch() passes isinstance check
- Every DatasetSpec has non-empty raw_table and non-empty primary_keys
- All incremental_axis values are in {"trade_date", "period_end", "static"}
"""

from __future__ import annotations

from typing import Any, Iterator

from alpha_find_v2.data_ingest.adapters.base import (
    DataSourceAdapter,
    DatasetSpec,
    STATIC_SPECS,
)
from alpha_find_v2.data_ingest.schemas import ALL_DATASET_IDS

_VALID_AXES = {"trade_date", "period_end", "static"}


# ---------------------------------------------------------------------------
# STATIC_SPECS coverage
# ---------------------------------------------------------------------------


def test_static_specs_covers_exactly_18_dataset_ids():
    assert len(STATIC_SPECS) == 18


def test_static_specs_keys_match_all_dataset_ids():
    assert set(STATIC_SPECS.keys()) == set(ALL_DATASET_IDS)


# ---------------------------------------------------------------------------
# Protocol isinstance check
# ---------------------------------------------------------------------------


class _NoOpAdapter:
    name = "noop"

    def supports(self, dataset_id: str) -> bool:
        return False

    def fetch(
        self,
        dataset_id: str,
        *,
        since: str | None,
        until: str | None,
        full: bool,
    ) -> Iterator[dict[str, Any]]:
        return iter([])


def test_no_op_class_satisfies_protocol():
    assert isinstance(_NoOpAdapter(), DataSourceAdapter)


# ---------------------------------------------------------------------------
# DatasetSpec field validity
# ---------------------------------------------------------------------------


def test_every_spec_has_non_empty_raw_table():
    for dataset_id, spec in STATIC_SPECS.items():
        assert spec.raw_table, f"{dataset_id} has empty raw_table"


def test_every_spec_has_non_empty_primary_keys():
    for dataset_id, spec in STATIC_SPECS.items():
        assert len(spec.primary_keys) > 0, f"{dataset_id} has empty primary_keys"


def test_all_incremental_axis_values_are_valid():
    for dataset_id, spec in STATIC_SPECS.items():
        assert spec.incremental_axis in _VALID_AXES, (
            f"{dataset_id} has unexpected incremental_axis: {spec.incremental_axis!r}"
        )
