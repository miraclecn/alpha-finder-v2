from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
import tomllib
from typing import Any

from .config_loader import (
    CONFIG_ROOT,
    PROJECT_ROOT,
    load_cost_model,
    load_decay_monitor,
    load_execution_policy,
    load_mandate,
    load_portfolio,
    load_portfolio_construction_model,
)
from .live_state import (
    AccountStateSnapshot,
    BenchmarkStateArtifact,
    account_state_to_portfolio_state,
    load_account_state_snapshot,
    load_benchmark_state_artifact,
)
from .models import (
    CostModel,
    DecayMonitor,
    ExecutionPolicy,
    Mandate,
    PortfolioConstructionModel,
    PortfolioRecipe,
)
from .deployment import PortfolioHolding, PortfolioState
from .portfolio_constructor import PortfolioConstructionInput
from .promotion_gate_evaluator import SleevePromotionSnapshot
from .research_artifact_loader import (
    load_sleeve_artifact,
)
from .research_evaluator import SimulationSummary


JsonMap = dict[str, Any]


@dataclass(slots=True)
class ExecutableSignalCaseDefinition:
    case_id: str
    description: str
    portfolio_path: str
    default_cost_model_path: str
    trade_date: str
    execution_date: str = ""
    artifact_paths: list[str] = field(default_factory=list)
    additional_cost_model_paths: list[str] = field(default_factory=list)
    benchmark_state_path: str = ""
    benchmark_industry_weights_path: str = ""
    account_state_path: str = ""
    portfolio_state_path: str = ""

    @classmethod
    def from_toml(cls, data: JsonMap) -> "ExecutableSignalCaseDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(f"Unsupported executable signal case schema version: {schema_version}")
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "executable_signal_case":
            raise ValueError(f"Unsupported executable signal case type: {artifact_type}")

        return cls(
            case_id=str(data["case_id"]),
            description=str(data["description"]),
            portfolio_path=str(data["portfolio_path"]),
            default_cost_model_path=str(data["default_cost_model_path"]),
            trade_date=str(data["trade_date"]),
            execution_date=str(data.get("execution_date", "")),
            artifact_paths=[str(path) for path in data.get("artifact_paths", [])],
            additional_cost_model_paths=[
                str(path) for path in data.get("additional_cost_model_paths", [])
            ],
            benchmark_state_path=str(data.get("benchmark_state_path", "")),
            benchmark_industry_weights_path=str(
                data.get("benchmark_industry_weights_path", "")
            ),
            account_state_path=str(data.get("account_state_path", "")),
            portfolio_state_path=str(data.get("portfolio_state_path", "")),
        )


@dataclass(slots=True)
class LoadedExecutableSignalCase:
    definition: ExecutableSignalCaseDefinition
    mandate: Mandate
    portfolio: PortfolioRecipe
    construction_model: PortfolioConstructionModel
    execution_policy: ExecutionPolicy
    default_cost_model: CostModel
    construction_input: PortfolioConstructionInput
    portfolio_state: PortfolioState
    cost_models: dict[str, CostModel]
    account_state_snapshot: AccountStateSnapshot | None = None
    benchmark_state_artifact: BenchmarkStateArtifact | None = None


@dataclass(slots=True)
class DecayWatchCaseDefinition:
    case_id: str
    description: str
    portfolio_path: str
    evaluation_date: str
    window_label: str
    promotion_snapshot: JsonMap
    realized_summary: JsonMap

    @classmethod
    def from_toml(cls, data: JsonMap) -> "DecayWatchCaseDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(f"Unsupported decay watch case schema version: {schema_version}")
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "decay_watch_case":
            raise ValueError(f"Unsupported decay watch case type: {artifact_type}")

        return cls(
            case_id=str(data["case_id"]),
            description=str(data["description"]),
            portfolio_path=str(data["portfolio_path"]),
            evaluation_date=str(data["evaluation_date"]),
            window_label=str(data["window_label"]),
            promotion_snapshot=dict(data.get("promotion_snapshot", {})),
            realized_summary=dict(data.get("realized_summary", {})),
        )


@dataclass(slots=True)
class LoadedDecayWatchCase:
    definition: DecayWatchCaseDefinition
    portfolio: PortfolioRecipe
    decay_monitor: DecayMonitor
    promotion_snapshot: SleevePromotionSnapshot
    realized_summary: SimulationSummary


def load_portfolio_state_snapshot(path: Path | str) -> PortfolioState:
    payload = _read_json(path)
    schema_version = int(payload.get("schema_version", 0))
    if schema_version != 1:
        raise ValueError(f"Unsupported portfolio state snapshot schema version: {schema_version}")
    artifact_type = str(payload.get("artifact_type", ""))
    if artifact_type != "portfolio_state_snapshot":
        raise ValueError(f"Unsupported portfolio state snapshot type: {artifact_type}")

    holdings = [
        PortfolioHolding(
            asset_id=str(item["asset_id"]),
            weight=float(item["weight"]),
        )
        for item in payload.get("holdings", [])
    ]
    _validate_portfolio_holdings(holdings)

    state = PortfolioState(
        portfolio_id=str(payload["portfolio_id"]),
        account_id=str(payload.get("account_id", "")),
        as_of_date=str(payload["as_of_date"]),
        cash_weight=float(payload.get("cash_weight", 0.0)),
        holdings=holdings,
        blocked_entry_assets=sorted(
            {str(asset_id) for asset_id in payload.get("blocked_entry_assets", [])}
        ),
        blocked_exit_assets=sorted(
            {str(asset_id) for asset_id in payload.get("blocked_exit_assets", [])}
        ),
    )
    _validate_portfolio_state(state)
    return state


def load_executable_signal_case(path: Path | str) -> LoadedExecutableSignalCase:
    definition = ExecutableSignalCaseDefinition.from_toml(_read_toml(path))
    portfolio = load_portfolio(definition.portfolio_path)
    if not portfolio.construction_model_id:
        raise ValueError("Executable signal case portfolio must define a construction model.")
    if not portfolio.execution_policy_id:
        raise ValueError("Executable signal case portfolio must define an execution policy.")
    if definition.benchmark_state_path and definition.benchmark_industry_weights_path:
        raise ValueError("Executable signal case must define only one benchmark state source.")
    if definition.account_state_path and definition.portfolio_state_path:
        raise ValueError("Executable signal case must define only one portfolio state source.")
    if not definition.account_state_path and not definition.portfolio_state_path:
        raise ValueError(
            "Executable signal case must define an account state snapshot path or a portfolio state snapshot path."
        )

    artifacts = {
        artifact.sleeve_id: artifact
        for artifact in (load_sleeve_artifact(path) for path in definition.artifact_paths)
    }
    for sleeve_id in portfolio.sleeves:
        if sleeve_id not in artifacts:
            raise ValueError(f"Executable signal case missing sleeve artifact: {sleeve_id}")

    benchmark_artifact = None
    benchmark_weights: dict[str, float] = {}
    benchmark_state_path = (
        definition.benchmark_state_path or definition.benchmark_industry_weights_path
    )
    if benchmark_state_path:
        benchmark_artifact = load_benchmark_state_artifact(benchmark_state_path)
        if benchmark_artifact.benchmark_id != portfolio.benchmark:
            raise ValueError(
                "Executable signal case benchmark state must match the portfolio benchmark."
            )
        benchmark_weights = benchmark_artifact.step_for_date(
            definition.trade_date
        ).industry_weights

    mandate = load_mandate(CONFIG_ROOT / "mandates" / f"{portfolio.mandate_id}.toml")
    construction_model = load_portfolio_construction_model(
        CONFIG_ROOT / "portfolio_construction" / f"{portfolio.construction_model_id}.toml"
    )
    execution_policy = load_execution_policy(
        CONFIG_ROOT / "execution_policies" / f"{portfolio.execution_policy_id}.toml"
    )
    account_state = None
    if definition.account_state_path:
        account_state = load_account_state_snapshot(definition.account_state_path)
        portfolio_state = account_state_to_portfolio_state(
            portfolio_id=portfolio.id,
            account_state=account_state,
        )
    else:
        portfolio_state = load_portfolio_state_snapshot(definition.portfolio_state_path)
    if portfolio_state.portfolio_id != portfolio.id:
        raise ValueError(
            "Portfolio state snapshot portfolio_id must match the executable signal case portfolio."
        )
    if portfolio_state.as_of_date != definition.trade_date:
        raise ValueError(
            "Portfolio state snapshot as_of_date must match the executable signal case trade_date."
        )
    default_cost_model = load_cost_model(definition.default_cost_model_path)
    cost_models = {
        model.id: model
        for model in (
            load_cost_model(item) for item in definition.additional_cost_model_paths
        )
        if model.id != default_cost_model.id
    }
    construction_input = PortfolioConstructionInput(
        trade_date=definition.trade_date,
        sleeves=[
            artifacts[sleeve_id].step_for_date(definition.trade_date).to_construction_input(sleeve_id)
            for sleeve_id in portfolio.sleeves
        ],
        benchmark_industry_weights=benchmark_weights,
    )
    return LoadedExecutableSignalCase(
        definition=definition,
        mandate=mandate,
        portfolio=portfolio,
        construction_model=construction_model,
        execution_policy=execution_policy,
        default_cost_model=default_cost_model,
        construction_input=construction_input,
        portfolio_state=portfolio_state,
        account_state_snapshot=account_state,
        cost_models=cost_models,
        benchmark_state_artifact=benchmark_artifact,
    )


def load_decay_watch_case(path: Path | str) -> LoadedDecayWatchCase:
    definition = DecayWatchCaseDefinition.from_toml(_read_toml(path))
    portfolio = load_portfolio(definition.portfolio_path)
    if not portfolio.decay_monitor_id:
        raise ValueError("Decay watch case portfolio must define a decay monitor.")
    decay_monitor = load_decay_monitor(
        CONFIG_ROOT / "decay_monitors" / f"{portfolio.decay_monitor_id}.toml"
    )
    return LoadedDecayWatchCase(
        definition=definition,
        portfolio=portfolio,
        decay_monitor=decay_monitor,
        promotion_snapshot=_load_promotion_snapshot(definition.promotion_snapshot),
        realized_summary=_load_simulation_summary(definition.realized_summary),
    )


def _load_promotion_snapshot(data: JsonMap) -> SleevePromotionSnapshot:
    return SleevePromotionSnapshot(
        oos_ir=float(data["oos_ir"]),
        oos_tstat=float(data["oos_tstat"]),
        breadth=int(data["breadth"]),
        peak_to_trough_drawdown=float(data["peak_to_trough_drawdown"]),
        turnover_budget=float(data.get("turnover_budget", 0.0)),
        realized_turnover_vs_budget=float(data.get("realized_turnover_vs_budget", 0.0)),
    )


def _load_simulation_summary(data: JsonMap) -> SimulationSummary:
    return SimulationSummary(
        periods=int(data["periods"]),
        average_return=float(data["average_return"]),
        volatility=float(data["volatility"]),
        ir=float(data["ir"]),
        tstat=float(data["tstat"]),
        peak_to_trough_drawdown=float(data["peak_to_trough_drawdown"]),
        breadth=int(data["breadth"]),
        average_turnover=float(data["average_turnover"]),
        blocked_name_share=float(data["blocked_name_share"]),
    )


def _resolve_project_path(path: Path | str) -> Path:
    target = Path(path)
    if target.is_absolute():
        return target
    return PROJECT_ROOT / target


def _read_toml(path: Path | str) -> JsonMap:
    target = _resolve_project_path(path)
    with target.open("rb") as handle:
        return tomllib.load(handle)


def _read_json(path: Path | str) -> JsonMap:
    target = _resolve_project_path(path)
    with target.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _validate_portfolio_holdings(holdings: list[PortfolioHolding]) -> None:
    seen_assets: set[str] = set()
    for holding in holdings:
        if holding.asset_id in seen_assets:
            raise ValueError(f"Duplicate portfolio holding: {holding.asset_id}")
        seen_assets.add(holding.asset_id)
        if holding.weight < 0.0:
            raise ValueError(f"Portfolio holding weight cannot be negative: {holding.asset_id}")


def _validate_portfolio_state(state: PortfolioState) -> None:
    total_weight = state.cash_weight + sum(holding.weight for holding in state.holdings)
    if state.cash_weight < 0.0:
        raise ValueError("Portfolio state cash_weight cannot be negative.")
    if total_weight > 1.000000001:
        raise ValueError("Portfolio state holdings plus cash cannot exceed 100%.")

    holding_assets = {holding.asset_id for holding in state.holdings if holding.weight > 0.0}
    missing_blocked_exits = sorted(set(state.blocked_exit_assets) - holding_assets)
    if missing_blocked_exits:
        raise ValueError(
            "Portfolio state blocked_exit_assets must be current holdings: "
            + ", ".join(missing_blocked_exits)
        )
