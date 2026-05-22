"""Tests for adapters/base.py — DataSourceAdapter protocol, DatasetSpec, STATIC_SPECS."""

from __future__ import annotations

from typing import Any, Iterator

import pytest

from alpha_find_v2.data_ingest.adapters.base import (
    AdapterPermissionError,
    AdapterRateLimitError,
    AdapterSchemaMismatchError,
    AdapterUnavailable,
    DataSourceAdapter,
    DatasetSpec,
    STATIC_SPECS,
)
from alpha_find_v2.data_ingest.schemas import DATASET_PRIMARY_KEYS


# ---------------------------------------------------------------------------
# STATIC_SPECS coverage
# ---------------------------------------------------------------------------


def test_static_specs_has_exactly_18_keys():
    assert len(STATIC_SPECS) == 18


def test_static_specs_keys_match_dataset_primary_keys():
    assert set(STATIC_SPECS.keys()) == set(DATASET_PRIMARY_KEYS.keys())


# ---------------------------------------------------------------------------
# DatasetSpec field validity
# ---------------------------------------------------------------------------


_VALID_AXES = {"trade_date", "period_end", "static"}


def test_every_spec_has_non_empty_primary_keys():
    for dataset_id, spec in STATIC_SPECS.items():
        assert len(spec.primary_keys) > 0, f"{dataset_id} has empty primary_keys"


def test_every_spec_has_valid_incremental_axis():
    for dataset_id, spec in STATIC_SPECS.items():
        assert spec.incremental_axis in _VALID_AXES, (
            f"{dataset_id} has unexpected incremental_axis: {spec.incremental_axis}"
        )


def test_every_spec_has_non_empty_raw_table():
    for dataset_id, spec in STATIC_SPECS.items():
        assert spec.raw_table, f"{dataset_id} has empty raw_table"


# ---------------------------------------------------------------------------
# runtime_checkable Protocol
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


def test_no_op_adapter_satisfies_protocol():
    adapter = _NoOpAdapter()
    assert isinstance(adapter, DataSourceAdapter)


def test_object_missing_fetch_does_not_satisfy_protocol():
    class _Bad:
        name = "bad"

        def supports(self, dataset_id: str) -> bool:
            return False

    assert not isinstance(_Bad(), DataSourceAdapter)


# ---------------------------------------------------------------------------
# Exception types are importable and subclass Exception
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc_class",
    [
        AdapterPermissionError,
        AdapterRateLimitError,
        AdapterSchemaMismatchError,
        AdapterUnavailable,
    ],
)
def test_exception_is_subclass_of_exception(exc_class):
    assert issubclass(exc_class, Exception)


@pytest.mark.parametrize(
    "exc_class",
    [
        AdapterPermissionError,
        AdapterRateLimitError,
        AdapterSchemaMismatchError,
        AdapterUnavailable,
    ],
)
def test_exception_can_be_raised_and_caught(exc_class):
    with pytest.raises(exc_class):
        raise exc_class("test message")
