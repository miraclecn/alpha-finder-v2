from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import tomllib
from typing import Any

from .config_loader import (
    CONFIG_ROOT,
    PROJECT_ROOT,
    load_cost_model,
    load_mandate,
    load_portfolio,
    load_portfolio_construction_model,
    load_promotion_gate,
    load_regime_overlay,
    load_risk_model,
    load_sleeve,
    load_target,
)
from .live_state import BenchmarkStateArtifact, load_benchmark_state_artifact
from .models import (
    CostModel,
    Mandate,
    PortfolioConstructionModel,
    PortfolioRecipe,
    PromotionGate,
    RegimeOverlay,
    ResidualTarget,
    RiskModel,
    Sleeve,
)
from .portfolio_promotion_replay import (
    PortfolioPromotionReplayInput,
    ReplayWalkForwardSplitDefinition,
    SleeveResearchArtifact,
    SleeveResearchStep,
    SleeveSignalRecord,
)
from .portfolio_simulator import TradeConstraintState
from .regime_overlay import (
    RegimeOverlayObservationArtifact,
    load_regime_overlay_observation_artifact,
)
from .target_builder import TradeLegState


JsonMap = dict[str, Any]


@dataclass(slots=True)
class BenchmarkIndustryWeightsArtifact:
    benchmark: str
    classification: str
    weights_by_date: dict[str, dict[str, float]] = field(default_factory=dict)


@dataclass(slots=True)
class PortfolioPromotionReplayCaseDefinition:
    case_id: str
    description: str
    baseline_portfolio_path: str
    candidate_portfolio_path: str
    default_cost_model_path: str
    artifact_paths: list[str] = field(default_factory=list)
    additional_cost_model_paths: list[str] = field(default_factory=list)
    benchmark_state_path: str = ""
    benchmark_industry_weights_path: str = ""
    regime_overlay_observation_path: str = ""
    periods_per_year: int = 52
    cost_scenario_pass: dict[str, bool] = field(default_factory=dict)
    regime_pass: dict[str, bool] = field(default_factory=dict)
    max_component_correlation: float = 0.0
    correlation_to_existing_portfolio: float = 0.0
    turnover_budget: float = 0.0
    walk_forward_splits: list[ReplayWalkForwardSplitDefinition] = field(
        default_factory=list
    )

    @classmethod
    def from_toml(cls, data: JsonMap) -> "PortfolioPromotionReplayCaseDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(
                f"Unsupported promotion replay case schema version: {schema_version}"
            )
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "portfolio_promotion_replay_case":
            raise ValueError(f"Unsupported promotion replay case type: {artifact_type}")

        return cls(
            case_id=str(data["case_id"]),
            description=str(data["description"]),
            baseline_portfolio_path=str(data["baseline_portfolio_path"]),
            candidate_portfolio_path=str(data["candidate_portfolio_path"]),
            default_cost_model_path=str(data["default_cost_model_path"]),
            artifact_paths=[str(path) for path in data.get("artifact_paths", [])],
            additional_cost_model_paths=[
                str(path) for path in data.get("additional_cost_model_paths", [])
            ],
            benchmark_state_path=str(data.get("benchmark_state_path", "")),
            benchmark_industry_weights_path=str(
                data.get("benchmark_industry_weights_path", "")
            ),
            regime_overlay_observation_path=str(
                data.get("regime_overlay_observation_path", "")
            ),
            periods_per_year=int(data.get("periods_per_year", 52)),
            cost_scenario_pass={
                str(key): bool(value)
                for key, value in dict(data.get("cost_scenario_pass", {})).items()
            },
            regime_pass={
                str(key): bool(value)
                for key, value in dict(data.get("regime_pass", {})).items()
            },
            max_component_correlation=float(data.get("max_component_correlation", 0.0)),
            correlation_to_existing_portfolio=float(
                data.get("correlation_to_existing_portfolio", 0.0)
            ),
            turnover_budget=float(data.get("turnover_budget", 0.0)),
            walk_forward_splits=[
                _load_walk_forward_split(item)
                for item in data.get("walk_forward_splits", [])
            ],
        )


@dataclass(slots=True)
class LoadedPortfolioPromotionReplayCase:
    definition: PortfolioPromotionReplayCaseDefinition
    mandate: Mandate
    construction_model: PortfolioConstructionModel
    default_cost_model: CostModel
    replay_input: PortfolioPromotionReplayInput
    gate: PromotionGate | None = None
    cost_models: dict[str, CostModel] = field(default_factory=dict)
    benchmark_state_artifact: BenchmarkStateArtifact | None = None
    regime_overlay: RegimeOverlay | None = None
    regime_overlay_observations: RegimeOverlayObservationArtifact | None = None


@dataclass(slots=True)
class SleeveResearchObservationRecord:
    asset_id: str
    rank: int
    score: float
    target_weight: float
    entry_open: float
    exit_open: float
    cost_model_id: str = ""
    industry: str = ""
    entry_state: TradeLegState = field(default_factory=TradeLegState)
    exit_state: TradeLegState = field(default_factory=TradeLegState)
    exposures: dict[str, float] = field(default_factory=dict)
    residual_components: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class SleeveResearchObservationStep:
    trade_date: str
    factor_returns: dict[str, float] = field(default_factory=dict)
    records: list[SleeveResearchObservationRecord] = field(default_factory=list)


@dataclass(slots=True)
class SleeveResearchObservationInput:
    steps: list[SleeveResearchObservationStep] = field(default_factory=list)


@dataclass(slots=True)
class SleeveArtifactBuildCaseDefinition:
    case_id: str
    description: str
    sleeve_path: str
    input_path: str
    output_path: str
    additional_cost_model_paths: list[str] = field(default_factory=list)

    @classmethod
    def from_toml(cls, data: JsonMap) -> "SleeveArtifactBuildCaseDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(
                f"Unsupported sleeve artifact build case schema version: {schema_version}"
            )
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "sleeve_artifact_build_case":
            raise ValueError(f"Unsupported sleeve artifact build case type: {artifact_type}")

        return cls(
            case_id=str(data["case_id"]),
            description=str(data["description"]),
            sleeve_path=str(data["sleeve_path"]),
            input_path=str(data["input_path"]),
            output_path=str(data["output_path"]),
            additional_cost_model_paths=[
                str(path) for path in data.get("additional_cost_model_paths", [])
            ],
        )


@dataclass(slots=True)
class LoadedSleeveArtifactBuildCase:
    definition: SleeveArtifactBuildCaseDefinition
    mandate: Mandate
    sleeve: Sleeve
    target: ResidualTarget
    default_cost_model: CostModel
    observation_input: SleeveResearchObservationInput
    risk_model: RiskModel | None = None
    cost_models: dict[str, CostModel] = field(default_factory=dict)


def load_sleeve_artifact_build_case(path: Path | str) -> LoadedSleeveArtifactBuildCase:
    definition = SleeveArtifactBuildCaseDefinition.from_toml(_read_toml(path))
    sleeve = load_sleeve(definition.sleeve_path)
    if not sleeve.target_id:
        raise ValueError("Sleeve artifact build case sleeve must define a target.")

    mandate = load_mandate(CONFIG_ROOT / "mandates" / f"{sleeve.mandate_id}.toml")
    target = load_target(CONFIG_ROOT / "targets" / f"{sleeve.target_id}.toml")
    if not target.cost_model:
        raise ValueError("Sleeve artifact build case target must define a default cost model.")

    default_cost_model = load_cost_model(
        CONFIG_ROOT / "cost_models" / f"{target.cost_model}.toml"
    )
    risk_model = None
    if target.risk_model_id:
        risk_model = load_risk_model(
            CONFIG_ROOT / "risk_models" / f"{target.risk_model_id}.toml"
        )
    cost_models = {
        model.id: model
        for model in (
            load_cost_model(item) for item in definition.additional_cost_model_paths
        )
        if model.id != default_cost_model.id
    }
    return LoadedSleeveArtifactBuildCase(
        definition=definition,
        mandate=mandate,
        sleeve=sleeve,
        target=target,
        default_cost_model=default_cost_model,
        observation_input=load_sleeve_research_observation_input(definition.input_path),
        risk_model=risk_model,
        cost_models=cost_models,
    )


def load_sleeve_research_observation_input(
    path: Path | str,
) -> SleeveResearchObservationInput:
    payload = _read_json(path)
    schema_version = int(payload.get("schema_version", 0))
    if schema_version != 1:
        raise ValueError(
            "Unsupported sleeve research observation input schema version: "
            f"{schema_version}"
        )
    artifact_type = str(payload.get("artifact_type", ""))
    if artifact_type != "sleeve_research_observation_input":
        raise ValueError(f"Unsupported sleeve research observation input type: {artifact_type}")

    return SleeveResearchObservationInput(
        steps=[_load_research_observation_step(item) for item in payload.get("steps", [])]
    )


def load_sleeve_artifact(path: Path | str) -> SleeveResearchArtifact:
    payload = _read_json(path)
    schema_version = int(payload.get("schema_version", 0))
    if schema_version != 1:
        raise ValueError(f"Unsupported sleeve artifact schema version: {schema_version}")
    artifact_type = str(payload.get("artifact_type", ""))
    if artifact_type != "sleeve_research_artifact":
        raise ValueError(f"Unsupported sleeve artifact type: {artifact_type}")

    return SleeveResearchArtifact(
        sleeve_id=str(payload["sleeve_id"]),
        mandate_id=str(payload["mandate_id"]),
        target_id=str(payload["target_id"]),
        steps=[_load_sleeve_step(item) for item in payload.get("steps", [])],
    )


def load_benchmark_industry_weights_artifact(
    path: Path | str,
) -> BenchmarkIndustryWeightsArtifact:
    artifact = load_benchmark_state_artifact(path)
    return BenchmarkIndustryWeightsArtifact(
        benchmark=artifact.benchmark_id,
        classification=artifact.classification,
        weights_by_date=artifact.weights_by_date(),
    )


def load_portfolio_promotion_replay_case(
    path: Path | str,
) -> LoadedPortfolioPromotionReplayCase:
    definition = PortfolioPromotionReplayCaseDefinition.from_toml(_read_toml(path))
    baseline_portfolio = load_portfolio(definition.baseline_portfolio_path)
    candidate_portfolio = load_portfolio(definition.candidate_portfolio_path)

    _validate_portfolio_pair(
        baseline_portfolio=baseline_portfolio,
        candidate_portfolio=candidate_portfolio,
    )

    artifacts = [load_sleeve_artifact(path) for path in definition.artifact_paths]
    _validate_case_artifacts(
        baseline_portfolio=baseline_portfolio,
        candidate_portfolio=candidate_portfolio,
        artifacts=artifacts,
    )

    mandate = load_mandate(CONFIG_ROOT / "mandates" / f"{candidate_portfolio.mandate_id}.toml")
    construction_model = load_portfolio_construction_model(
        CONFIG_ROOT
        / "portfolio_construction"
        / f"{candidate_portfolio.construction_model_id}.toml"
    )
    gate = _load_gate(candidate_portfolio)
    default_cost_model = load_cost_model(definition.default_cost_model_path)
    cost_models = {
        model.id: model
        for model in (
            load_cost_model(path) for path in definition.additional_cost_model_paths
        )
        if model.id != default_cost_model.id
    }

    if definition.benchmark_state_path and definition.benchmark_industry_weights_path:
        raise ValueError(
            "Replay case must define only one benchmark state source."
        )

    benchmark_artifact = None
    benchmark_weights_by_date: dict[str, dict[str, float]] = {}
    benchmark_state_path = (
        definition.benchmark_state_path or definition.benchmark_industry_weights_path
    )
    if benchmark_state_path:
        benchmark_artifact = load_benchmark_state_artifact(benchmark_state_path)
        benchmark_weights_by_date = benchmark_artifact.weights_by_date()
        if benchmark_artifact.benchmark_id != candidate_portfolio.benchmark:
            raise ValueError(
                "Replay case benchmark state must match the candidate portfolio benchmark."
            )

    regime_overlay = None
    regime_overlay_observations = None
    if candidate_portfolio.regime_overlay_id:
        regime_overlay = load_regime_overlay(
            CONFIG_ROOT
            / "regime_overlays"
            / f"{candidate_portfolio.regime_overlay_id}.toml"
        )
        if not definition.regime_overlay_observation_path:
            raise ValueError(
                "Replay case must define regime_overlay_observation_path when the candidate portfolio declares regime_overlay_id."
            )
        regime_overlay_observations = load_regime_overlay_observation_artifact(
            definition.regime_overlay_observation_path
        )
        if regime_overlay.mandate_id != candidate_portfolio.mandate_id:
            raise ValueError(
                "Replay case regime overlay mandate must match the candidate portfolio mandate."
            )
        if regime_overlay.benchmark != candidate_portfolio.benchmark:
            raise ValueError(
                "Replay case regime overlay benchmark must match the candidate portfolio benchmark."
            )
        if regime_overlay_observations.overlay_id != regime_overlay.id:
            raise ValueError(
                "Replay case regime overlay observations must match the configured regime_overlay_id."
            )
    elif definition.regime_overlay_observation_path:
        raise ValueError(
            "Replay case cannot define regime_overlay_observation_path without a candidate portfolio regime_overlay_id."
        )

    replay_input = PortfolioPromotionReplayInput(
        baseline_portfolio=baseline_portfolio,
        candidate_portfolio=candidate_portfolio,
        artifacts=artifacts,
        regime_overlay=regime_overlay,
        regime_overlay_observations=(
            regime_overlay_observations.steps
            if regime_overlay_observations is not None
            else []
        ),
        periods_per_year=definition.periods_per_year,
        benchmark_industry_weights_by_date=benchmark_weights_by_date,
        cost_scenario_pass=definition.cost_scenario_pass,
        regime_pass=definition.regime_pass,
        max_component_correlation=definition.max_component_correlation,
        correlation_to_existing_portfolio=definition.correlation_to_existing_portfolio,
        turnover_budget=definition.turnover_budget,
        walk_forward_splits=definition.walk_forward_splits,
    )
    return LoadedPortfolioPromotionReplayCase(
        definition=definition,
        mandate=mandate,
        construction_model=construction_model,
        default_cost_model=default_cost_model,
        replay_input=replay_input,
        gate=gate,
        cost_models=cost_models,
        benchmark_state_artifact=benchmark_artifact,
        regime_overlay=regime_overlay,
        regime_overlay_observations=regime_overlay_observations,
    )


def _resolve_project_path(path: Path | str) -> Path:
    target = Path(path)
    if target.is_absolute():
        return target
    return PROJECT_ROOT / target


def _read_json(path: Path | str) -> JsonMap:
    target = _resolve_project_path(path)
    with target.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _read_toml(path: Path | str) -> JsonMap:
    target = _resolve_project_path(path)
    with target.open("rb") as handle:
        return tomllib.load(handle)


def _load_sleeve_step(data: JsonMap) -> SleeveResearchStep:
    return SleeveResearchStep(
        trade_date=str(data["trade_date"]),
        records=[_load_signal_record(item) for item in data.get("records", [])],
    )


def _load_walk_forward_split(data: JsonMap) -> ReplayWalkForwardSplitDefinition:
    return ReplayWalkForwardSplitDefinition(
        split_id=str(data["split_id"]),
        start_trade_date=str(data["start_trade_date"]),
        end_trade_date=str(data.get("end_trade_date", "")),
    )


def _load_signal_record(data: JsonMap) -> SleeveSignalRecord:
    trade_state = dict(data.get("trade_state", {}))
    return SleeveSignalRecord(
        asset_id=str(data["asset_id"]),
        rank=int(data["rank"]),
        score=float(data.get("score", 0.0)),
        target_weight=float(data["target_weight"]),
        realized_return=float(data["realized_return"]),
        cost_model_id=str(data.get("cost_model_id", "")),
        industry=str(data.get("industry", "")),
        trade_state=TradeConstraintState(
            can_enter=bool(trade_state.get("can_enter", True)),
            can_exit=bool(trade_state.get("can_exit", True)),
        ),
    )


def _load_research_observation_step(data: JsonMap) -> SleeveResearchObservationStep:
    return SleeveResearchObservationStep(
        trade_date=str(data["trade_date"]),
        factor_returns={
            str(key): float(value)
            for key, value in dict(data.get("factor_returns", {})).items()
        },
        records=[_load_research_observation_record(item) for item in data.get("records", [])],
    )


def _load_research_observation_record(data: JsonMap) -> SleeveResearchObservationRecord:
    return SleeveResearchObservationRecord(
        asset_id=str(data["asset_id"]),
        rank=int(data["rank"]),
        score=float(data.get("score", 0.0)),
        target_weight=float(data["target_weight"]),
        entry_open=float(data["entry_open"]),
        exit_open=float(data["exit_open"]),
        cost_model_id=str(data.get("cost_model_id", "")),
        industry=str(data.get("industry", "")),
        entry_state=_load_trade_leg_state(data.get("entry_state", {})),
        exit_state=_load_trade_leg_state(data.get("exit_state", {})),
        exposures={
            str(key): float(value)
            for key, value in dict(data.get("exposures", {})).items()
        },
        residual_components={
            str(key): float(value)
            for key, value in dict(data.get("residual_components", {})).items()
        },
    )


def _load_trade_leg_state(data: Any) -> TradeLegState:
    state = dict(data or {})
    return TradeLegState(
        suspended=bool(state.get("suspended", False)),
        limit_locked=bool(state.get("limit_locked", False)),
        liquidity_pass=bool(state.get("liquidity_pass", True)),
    )


def _validate_portfolio_pair(
    baseline_portfolio: PortfolioRecipe,
    candidate_portfolio: PortfolioRecipe,
) -> None:
    if baseline_portfolio.mandate_id != candidate_portfolio.mandate_id:
        raise ValueError("Replay case baseline and candidate portfolios must share a mandate.")
    if baseline_portfolio.benchmark != candidate_portfolio.benchmark:
        raise ValueError("Replay case baseline and candidate portfolios must share a benchmark.")
    if (
        baseline_portfolio.construction_model_id
        != candidate_portfolio.construction_model_id
    ):
        raise ValueError(
            "Replay case baseline and candidate portfolios must share a construction model."
        )


def _validate_case_artifacts(
    baseline_portfolio: PortfolioRecipe,
    candidate_portfolio: PortfolioRecipe,
    artifacts: list[SleeveResearchArtifact],
) -> None:
    used_sleeves = set(baseline_portfolio.sleeves) | set(candidate_portfolio.sleeves)
    artifact_sleeves = {artifact.sleeve_id for artifact in artifacts}
    unused_sleeves = sorted(artifact_sleeves - used_sleeves)
    if unused_sleeves:
        raise ValueError(
            f"Replay case contains artifacts for unused sleeves: {', '.join(unused_sleeves)}"
        )

    used_target_ids = sorted(
        {
            artifact.target_id
            for artifact in artifacts
            if artifact.sleeve_id in used_sleeves
        }
    )
    if len(used_target_ids) > 1:
        raise ValueError(
            "Replay case sleeves must share the same target_id; found: "
            + ", ".join(used_target_ids)
        )


def _load_gate(candidate_portfolio: PortfolioRecipe) -> PromotionGate | None:
    if not candidate_portfolio.promotion_gate_id:
        return None
    return load_promotion_gate(
        CONFIG_ROOT / "promotion_gates" / f"{candidate_portfolio.promotion_gate_id}.toml"
    )
