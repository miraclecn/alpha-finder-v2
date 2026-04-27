from __future__ import annotations

from dataclasses import dataclass, replace
import json
from pathlib import Path

from .fundamental_research_input_builder import (
    build_fundamental_research_observation_input,
    load_fundamental_research_input_build_case,
)
from .trend_research_input_builder import (
    build_trend_research_observation_input,
    load_trend_research_input_build_case,
    _resolve_project_path,
)


@dataclass(slots=True)
class ResidualSnapshotRequiredCoverageBuildResult:
    target_id: str
    fundamental_case_path: str
    trend_case_path: str
    source_db_path: str
    as_of_date: str
    trade_dates: list[str]
    fundamental_observation_count: int
    trend_observation_count: int
    required_union_observation_count: int
    required_union_asset_count: int
    records_by_trade_date: list[dict[str, object]]


def build_residual_snapshot_required_coverage(
    *,
    fundamental_case_path: Path | str,
    trend_case_path: Path | str,
    as_of_date: str,
) -> ResidualSnapshotRequiredCoverageBuildResult:
    loaded_fundamental_case = load_fundamental_research_input_build_case(
        fundamental_case_path
    )
    loaded_trend_case = load_trend_research_input_build_case(trend_case_path)

    if loaded_fundamental_case.target_id != loaded_trend_case.target_id:
        raise ValueError(
            "Residual snapshot required coverage cases must share the same target_id."
        )
    if loaded_fundamental_case.source_db_path != loaded_trend_case.source_db_path:
        raise ValueError(
            "Residual snapshot required coverage cases must share the same source_db_path."
        )

    # Coverage derives from which observations the two cases select, not from the
    # residual component values that will later be attached by the audited snapshot.
    fundamental_result = build_fundamental_research_observation_input(
        replace(
            loaded_fundamental_case,
            residual_components=[],
            residual_component_snapshot_path=None,
        )
    )
    trend_result = build_trend_research_observation_input(
        replace(
            loaded_trend_case,
            residual_components=[],
            residual_component_snapshot_path=None,
        )
    )

    records_by_trade_date: dict[str, set[str]] = {}
    for result in (fundamental_result, trend_result):
        for step in result.observation_input.steps:
            records_by_trade_date.setdefault(step.trade_date, set()).update(
                record.asset_id for record in step.records
            )

    sorted_trade_dates = sorted(records_by_trade_date)
    ordered_records_by_trade_date = [
        {
            "trade_date": trade_date,
            "required_union_asset_count": len(records_by_trade_date[trade_date]),
            "required_union_asset_ids": sorted(records_by_trade_date[trade_date]),
        }
        for trade_date in sorted_trade_dates
    ]
    required_union_asset_ids = {
        asset_id
        for asset_ids in records_by_trade_date.values()
        for asset_id in asset_ids
    }

    return ResidualSnapshotRequiredCoverageBuildResult(
        target_id=loaded_fundamental_case.target_id,
        fundamental_case_path=str(_resolve_project_path(fundamental_case_path)),
        trend_case_path=str(_resolve_project_path(trend_case_path)),
        source_db_path=str(loaded_fundamental_case.source_db_path),
        as_of_date=as_of_date,
        trade_dates=sorted_trade_dates,
        fundamental_observation_count=sum(
            len(step.records) for step in fundamental_result.observation_input.steps
        ),
        trend_observation_count=sum(
            len(step.records) for step in trend_result.observation_input.steps
        ),
        required_union_observation_count=sum(
            len(asset_ids) for asset_ids in records_by_trade_date.values()
        ),
        required_union_asset_count=len(required_union_asset_ids),
        records_by_trade_date=ordered_records_by_trade_date,
    )


def write_residual_snapshot_required_coverage(
    result: ResidualSnapshotRequiredCoverageBuildResult,
    path: Path | str,
) -> Path:
    resolved_path = _resolve_project_path(path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "artifact_type": "residual_snapshot_required_coverage",
        "target_id": result.target_id,
        "generated_from": {
            "fundamental_case_path": result.fundamental_case_path,
            "trend_case_path": result.trend_case_path,
            "source_db_path": result.source_db_path,
            "as_of_date": result.as_of_date,
        },
        "summary": {
            "decision_date_count": len(result.trade_dates),
            "required_union_observation_count": result.required_union_observation_count,
            "required_union_asset_count": result.required_union_asset_count,
        },
        "records_by_trade_date": result.records_by_trade_date,
    }
    resolved_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return resolved_path
