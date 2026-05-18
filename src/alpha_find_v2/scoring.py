from __future__ import annotations

import math


def zscore_map(values_by_asset: dict[str, float]) -> dict[str, float]:
    series = list(values_by_asset.values())
    if len(series) < 2:
        return {asset_id: 0.0 for asset_id in values_by_asset}

    mean = sum(series) / len(series)
    variance = sum((value - mean) ** 2 for value in series) / (len(series) - 1)
    if variance <= 0.0:
        return {asset_id: 0.0 for asset_id in values_by_asset}
    stdev = math.sqrt(variance)
    return {
        asset_id: (value - mean) / stdev
        for asset_id, value in values_by_asset.items()
    }


def group_neutral_zscore_map(
    *,
    values_by_asset: dict[str, float],
    group_by_asset: dict[str, str],
) -> dict[str, float]:
    missing_groups = sorted(
        asset_id
        for asset_id in values_by_asset
        if not group_by_asset.get(asset_id, "").strip()
    )
    if missing_groups:
        raise ValueError(
            "Group-neutral scoring requires a group for every asset: "
            + ", ".join(missing_groups)
        )

    grouped_values: dict[str, dict[str, float]] = {}
    for asset_id, value in values_by_asset.items():
        grouped_values.setdefault(group_by_asset[asset_id], {})[asset_id] = value

    zscores: dict[str, float] = {}
    for group_values in grouped_values.values():
        zscores.update(zscore_map(group_values))
    return zscores


def rank_then_cap_weights(
    asset_ids: list[str],
    *,
    weight_cap: float,
) -> dict[str, float]:
    if not asset_ids:
        return {}

    raw_weights = {
        asset_id: 1.0 / rank
        for rank, asset_id in enumerate(asset_ids, start=1)
    }
    if weight_cap <= 0.0 or weight_cap >= 1.0:
        return _normalize(raw_weights)

    allocated: dict[str, float] = {}
    remaining_raw = dict(raw_weights)
    remaining_weight = 1.0
    while remaining_raw and remaining_weight > 0.0:
        provisional = _normalize_to_budget(remaining_raw, remaining_weight)
        capped_assets = [
            asset_id
            for asset_id, weight in provisional.items()
            if weight > weight_cap
        ]
        if not capped_assets:
            allocated.update(provisional)
            break
        for asset_id in capped_assets:
            allocated[asset_id] = weight_cap
            remaining_weight -= weight_cap
            del remaining_raw[asset_id]

    return {
        asset_id: allocated.get(asset_id, 0.0)
        for asset_id in asset_ids
    }


def _normalize(weights: dict[str, float]) -> dict[str, float]:
    return _normalize_to_budget(weights, 1.0)


def _normalize_to_budget(weights: dict[str, float], budget: float) -> dict[str, float]:
    total = sum(weights.values())
    if total <= 0.0 or budget <= 0.0:
        return {asset_id: 0.0 for asset_id in weights}
    scale = budget / total
    return {
        asset_id: weight * scale
        for asset_id, weight in weights.items()
    }
