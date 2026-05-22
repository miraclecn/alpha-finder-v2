"""
DataSourceAdapter protocol and supporting types for the data ingestion pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Literal, Protocol, runtime_checkable

from ..schemas import (
    ALL_DATASET_IDS,
    DATASET_INCREMENTAL_AXIS,
    DATASET_PRIMARY_KEYS,
    DATASET_TABLE_NAME,
)

# ---------------------------------------------------------------------------
# Exception types
# ---------------------------------------------------------------------------


class AdapterPermissionError(Exception):
    pass


class AdapterRateLimitError(Exception):
    pass


class AdapterSchemaMismatchError(Exception):
    pass


class AdapterUnavailable(Exception):
    pass


# ---------------------------------------------------------------------------
# DatasetSpec
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    dataset_id: str
    raw_table: str
    primary_keys: tuple[str, ...]
    incremental_axis: Literal["trade_date", "period_end", "static"]


# ---------------------------------------------------------------------------
# DataSourceAdapter protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class DataSourceAdapter(Protocol):
    name: str

    def supports(self, dataset_id: str) -> bool: ...

    def fetch(
        self,
        dataset_id: str,
        *,
        since: str | None,
        until: str | None,
        full: bool,
    ) -> Iterator[dict[str, Any]]: ...


# ---------------------------------------------------------------------------
# STATIC_SPECS — one entry per dataset_id
# ---------------------------------------------------------------------------

STATIC_SPECS: dict[str, DatasetSpec] = {
    ds_id: DatasetSpec(
        dataset_id=ds_id,
        raw_table=DATASET_TABLE_NAME[ds_id],
        primary_keys=DATASET_PRIMARY_KEYS[ds_id],
        incremental_axis=DATASET_INCREMENTAL_AXIS[ds_id],
    )
    for ds_id in ALL_DATASET_IDS
}
