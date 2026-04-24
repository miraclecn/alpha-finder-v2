from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path

from .config_loader import PROJECT_ROOT
from .portfolio_promotion_replay import (
    SleeveResearchArtifact,
    SleeveResearchStep,
    SleeveSignalRecord,
)
from .portfolio_simulator import TradeConstraintState
from .research_artifact_loader import (
    LoadedSleeveArtifactBuildCase,
    SleeveResearchObservationRecord,
    SleeveResearchObservationStep,
)
from .risk_model import (
    AssetRiskObservation,
    ConfiguredRiskModelResidualizer,
    RiskModelSnapshot,
)
from .target_builder import ExecutableResidualTargetBuilder, TargetObservation


def build_sleeve_artifact(
    loaded_case: LoadedSleeveArtifactBuildCase,
) -> SleeveResearchArtifact:
    builders = {
        loaded_case.default_cost_model.id: ExecutableResidualTargetBuilder(
            loaded_case.target,
            loaded_case.default_cost_model,
        )
    }
    for cost_model in loaded_case.cost_models.values():
        builders[cost_model.id] = ExecutableResidualTargetBuilder(
            loaded_case.target,
            cost_model,
        )

    residualizer = (
        ConfiguredRiskModelResidualizer(loaded_case.risk_model)
        if loaded_case.risk_model is not None
        else None
    )

    return SleeveResearchArtifact(
        sleeve_id=loaded_case.sleeve.id,
        mandate_id=loaded_case.mandate.id,
        target_id=loaded_case.target.id,
        steps=[
            _build_step(
                step=step,
                default_builder=builders[loaded_case.default_cost_model.id],
                builders=builders,
                residualizer=residualizer,
            )
            for step in loaded_case.observation_input.steps
        ],
    )


def write_sleeve_artifact(
    artifact: SleeveResearchArtifact,
    path: Path | str,
) -> Path:
    target = _resolve_project_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = _json_ready(
        {
        "schema_version": 1,
        "artifact_type": "sleeve_research_artifact",
        "sleeve_id": artifact.sleeve_id,
        "mandate_id": artifact.mandate_id,
        "target_id": artifact.target_id,
        "steps": [asdict(step) for step in artifact.steps],
        }
    )
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return target


def _build_step(
    *,
    step: SleeveResearchObservationStep,
    default_builder: ExecutableResidualTargetBuilder,
    builders: dict[str, ExecutableResidualTargetBuilder],
    residualizer: ConfiguredRiskModelResidualizer | None,
) -> SleeveResearchStep:
    return SleeveResearchStep(
        trade_date=step.trade_date,
        records=[
            _build_record(
                record=record,
                factor_returns=step.factor_returns,
                default_builder=default_builder,
                builders=builders,
                residualizer=residualizer,
            )
            for record in sorted(step.records, key=lambda item: (item.rank, item.asset_id))
        ],
    )


def _build_record(
    *,
    record: SleeveResearchObservationRecord,
    factor_returns: dict[str, float],
    default_builder: ExecutableResidualTargetBuilder,
    builders: dict[str, ExecutableResidualTargetBuilder],
    residualizer: ConfiguredRiskModelResidualizer | None,
) -> SleeveSignalRecord:
    builder = default_builder
    if record.cost_model_id:
        builder = builders.get(record.cost_model_id)
        if builder is None:
            raise ValueError(f"Unknown cost model for artifact record: {record.cost_model_id}")

    gross_return = (record.exit_open / record.entry_open) - 1.0
    risk_decomposition = None
    if record.exposures and builder.target.residualization:
        if residualizer is None:
            raise ValueError("Artifact build record exposures require a configured risk model.")
        risk_decomposition = residualizer.decompose(
            observation=AssetRiskObservation(
                asset_id=record.asset_id,
                forward_return=gross_return,
                exposures=record.exposures,
            ),
            snapshot=RiskModelSnapshot(factor_returns=factor_returns),
        )
    elif builder.target.residualization and not record.residual_components:
        raise ValueError(
            "Artifact build record must define either exposures or residual_components."
        )

    observation = TargetObservation(
        entry_open=record.entry_open,
        exit_open=record.exit_open,
        entry_state=record.entry_state,
        exit_state=record.exit_state,
        residual_components=record.residual_components,
        risk_decomposition=risk_decomposition,
    )
    evaluation = builder.evaluate(observation)
    if evaluation.realized_return is None:
        raise ValueError(f"Missing realized return for asset {record.asset_id}")

    return SleeveSignalRecord(
        asset_id=record.asset_id,
        rank=record.rank,
        score=record.score,
        target_weight=record.target_weight,
        realized_return=evaluation.realized_return,
        cost_model_id=record.cost_model_id or builder.cost_model.id,
        industry=record.industry,
        trade_state=TradeConstraintState(
            can_enter=builder.can_enter(observation),
            can_exit=builder.can_exit(observation),
        ),
    )


def _resolve_project_path(path: Path | str) -> Path:
    target = Path(path)
    if target.is_absolute():
        return target
    return PROJECT_ROOT / target


def _json_ready(value: object) -> object:
    if isinstance(value, float):
        return round(value, 12)
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    return value
