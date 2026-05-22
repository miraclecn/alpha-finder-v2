"""
Cross-descriptor Pearson correlation on raw descriptor values.
"""
from __future__ import annotations

import math

import pandas as pd


def compute_cross_correlation(
    primary_panel: pd.DataFrame,
    other_panels: dict[str, pd.DataFrame],
) -> dict[str, float]:
    """
    Compute pairwise Pearson correlation between primary_panel and each panel
    in other_panels, aligned on (trade_date, security_id).

    Args:
        primary_panel: (trade_date, security_id, descriptor_value)
        other_panels: dict of descriptor_id -> (trade_date, security_id, descriptor_value)

    Returns: dict of descriptor_id -> correlation float.
    """
    if not other_panels or primary_panel.empty:
        return {}

    results: dict[str, float] = {}
    for other_id, other_df in other_panels.items():
        if other_df.empty:
            results[other_id] = float("nan")
            continue
        merged = primary_panel[["trade_date", "security_id", "descriptor_value"]].merge(
            other_df[["trade_date", "security_id", "descriptor_value"]].rename(
                columns={"descriptor_value": "other_value"}
            ),
            on=["trade_date", "security_id"],
            how="inner",
        )
        merged = merged.dropna(subset=["descriptor_value", "other_value"])
        if len(merged) < 2:
            results[other_id] = float("nan")
            continue
        corr = merged["descriptor_value"].corr(merged["other_value"])
        results[other_id] = float(corr) if not math.isnan(corr) else float("nan")

    return results
