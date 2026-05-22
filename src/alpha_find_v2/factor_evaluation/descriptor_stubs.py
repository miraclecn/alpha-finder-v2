"""
Stub registry entries for descriptors that require 5000-credit Tushare data.

Importing this module registers the stubs into REGISTRY so the CLI can
surface them with exit code 3 + missing-dataset message.
"""
from __future__ import annotations

import pandas as pd

from .descriptor_compute import ComputeContext, DescriptorComputeSpec, register
from .exceptions import DescriptorNotImplemented


def _make_stub(
    descriptor_id: str,
    requires: tuple[str, ...],
) -> DescriptorComputeSpec:
    def _stub(ctx: ComputeContext) -> pd.DataFrame:
        raise DescriptorNotImplemented(
            descriptor_id=descriptor_id,
            requires=requires,
            message=(
                f"Descriptor '{descriptor_id}' requires 5000 Tushare credits. "
                f"Enable the following datasets in config/data_sources.toml: "
                f"{', '.join(requires)}"
            ),
        )

    _stub.__name__ = f"_stub_{descriptor_id}"
    return DescriptorComputeSpec(
        descriptor_id=descriptor_id,
        fn=_stub,
        requires=requires,
        notes="STUB — requires 5000-credit datasets.",
    )


register(_make_stub(
    "accrual_quality",
    requires=("pit_fina_indicator", "raw_balancesheet", "raw_cashflow"),
))

register(_make_stub(
    "profitability_quality",
    requires=("pit_fina_indicator",),
))

register(_make_stub(
    "leverage_conservatism",
    requires=("pit_fina_indicator", "raw_balancesheet"),
))

register(_make_stub(
    "estimate_revision_breadth",
    requires=("raw_forecast", "raw_express"),
))

register(_make_stub(
    "post_earnings_drift_signal",
    requires=("raw_forecast", "raw_express", "pit_fina_indicator"),
))
