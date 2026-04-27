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
    load_regime_overlay,
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
    RegimeOverlay,
)
from .deployment import (
    ManualExecutionAttempt,
    ManualExecutionBlock,
    ManualExecutionOutcome,
    ManualExecutionOverride,
    PortfolioHolding,
    PortfolioState,
    RealizedPortfolioPoint,
    RealizedTradingCost,
    RealizedTradingWindow,
    RunManifest,
)
from .portfolio_constructor import PortfolioConstructionInput
from .promotion_gate_evaluator import SleevePromotionSnapshot
from .regime_overlay import (
    RegimeOverlayDecision,
    RegimeOverlayEvaluator,
    load_regime_overlay_observation_artifact,
)
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
    regime_overlay_observation_path: str = ""
    live_candidate_bundle_path: str = ""
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
            regime_overlay_observation_path=str(
                data.get("regime_overlay_observation_path", "")
            ),
            live_candidate_bundle_path=str(
                data.get("live_candidate_bundle_path", "")
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
    regime_overlay: RegimeOverlay | None = None
    regime_overlay_decision: RegimeOverlayDecision | None = None


@dataclass(slots=True)
class RunManifestCaseDefinition:
    case_id: str
    description: str
    executable_signal_case_path: str
    operator_id: str
    operator_timestamp: str
    data_version: str
    data_build_date: str
    output_path: str

    @classmethod
    def from_toml(cls, data: JsonMap) -> "RunManifestCaseDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(f"Unsupported run manifest case schema version: {schema_version}")
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "run_manifest_case":
            raise ValueError(f"Unsupported run manifest case type: {artifact_type}")

        return cls(
            case_id=str(data["case_id"]),
            description=str(data["description"]),
            executable_signal_case_path=str(data["executable_signal_case_path"]),
            operator_id=str(data["operator_id"]),
            operator_timestamp=str(data["operator_timestamp"]),
            data_version=str(data["data_version"]),
            data_build_date=str(data["data_build_date"]),
            output_path=str(data["output_path"]),
        )


@dataclass(slots=True)
class LoadedRunManifestCase:
    definition: RunManifestCaseDefinition
    executable_signal_case: LoadedExecutableSignalCase


@dataclass(slots=True)
class DecayWatchCaseDefinition:
    case_id: str
    description: str
    portfolio_path: str
    evaluation_date: str
    window_label: str
    promotion_snapshot: JsonMap
    realized_summary: JsonMap
    run_manifest_path: str = ""
    manual_execution_outcome_path: str = ""
    realized_trading_window_path: str = ""

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
            run_manifest_path=str(data.get("run_manifest_path", "")),
            manual_execution_outcome_path=str(data.get("manual_execution_outcome_path", "")),
            realized_trading_window_path=str(data.get("realized_trading_window_path", "")),
        )


@dataclass(slots=True)
class LoadedDecayWatchCase:
    definition: DecayWatchCaseDefinition
    portfolio: PortfolioRecipe
    decay_monitor: DecayMonitor
    promotion_snapshot: SleevePromotionSnapshot
    realized_summary: SimulationSummary
    run_manifest: RunManifest | None = None
    manual_execution_outcome: ManualExecutionOutcome | None = None
    realized_trading_window: RealizedTradingWindow | None = None


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


def load_run_manifest_case(path: Path | str) -> LoadedRunManifestCase:
    definition = RunManifestCaseDefinition.from_toml(_read_toml(path))
    return LoadedRunManifestCase(
        definition=definition,
        executable_signal_case=load_executable_signal_case(definition.executable_signal_case_path),
    )


def load_run_manifest(path: Path | str) -> RunManifest:
    payload = _read_json(path)
    _validate_artifact_header(
        payload=payload,
        expected_type="run_manifest",
        schema_label="run manifest",
    )
    return RunManifest(
        run_id=str(payload["run_id"]),
        package_id=str(payload["package_id"]),
        portfolio_id=str(payload["portfolio_id"]),
        mandate_id=str(payload["mandate_id"]),
        trade_date=str(payload["trade_date"]),
        execution_date=str(payload["execution_date"]),
        data_version=str(payload.get("data_version", "")),
        data_build_date=str(payload.get("data_build_date", "")),
        benchmark_state_path=str(payload.get("benchmark_state_path", "")),
        sleeve_artifact_paths=[str(path) for path in payload.get("sleeve_artifact_paths", [])],
        portfolio_path=str(payload.get("portfolio_path", "")),
        account_state_path=str(payload.get("account_state_path", "")),
        portfolio_state_path=str(payload.get("portfolio_state_path", "")),
        execution_policy_id=str(payload.get("execution_policy_id", "")),
        base_investable_budget=float(payload.get("base_investable_budget", 0.0)),
        investable_budget=float(payload.get("investable_budget", 0.0)),
        target_cash_weight=float(payload.get("target_cash_weight", 0.0)),
        cash_weight=float(payload.get("cash_weight", 0.0)),
        regime_overlay_status=str(payload.get("regime_overlay_status", "")),
        regime_overlay_state=str(payload.get("regime_overlay_state", "")),
        operator_id=str(payload.get("operator_id", "")),
        operator_timestamp=str(payload.get("operator_timestamp", "")),
    )


def load_manual_execution_outcome(path: Path | str) -> ManualExecutionOutcome:
    payload = _read_json(path)
    _validate_artifact_header(
        payload=payload,
        expected_type="manual_execution_outcome",
        schema_label="manual execution outcome",
    )
    holdings = [
        PortfolioHolding(
            asset_id=str(item["asset_id"]),
            weight=float(item["weight"]),
        )
        for item in payload.get("post_trade_holdings", [])
    ]
    _validate_portfolio_holdings(holdings)
    return ManualExecutionOutcome(
        run_id=str(payload["run_id"]),
        package_id=str(payload["package_id"]),
        portfolio_id=str(payload["portfolio_id"]),
        execution_date=str(payload["execution_date"]),
        attempted_orders=[
            ManualExecutionAttempt(
                asset_id=str(item["asset_id"]),
                action=str(item["action"]),
                requested_delta_weight=float(item["requested_delta_weight"]),
                executed_delta_weight=float(item["executed_delta_weight"]),
                status=str(item["status"]),
                reason=str(item.get("reason", "")),
            )
            for item in payload.get("attempted_orders", [])
        ],
        blocked_trades=[
            ManualExecutionBlock(
                asset_id=str(item["asset_id"]),
                action=str(item["action"]),
                requested_delta_weight=float(item["requested_delta_weight"]),
                reason=str(item.get("reason", "")),
            )
            for item in payload.get("blocked_trades", [])
        ],
        manual_overrides=[
            ManualExecutionOverride(
                asset_id=str(item["asset_id"]),
                override_type=str(item["override_type"]),
                from_target_weight=float(item["from_target_weight"]),
                to_target_weight=float(item["to_target_weight"]),
                reason=str(item.get("reason", "")),
            )
            for item in payload.get("manual_overrides", [])
        ],
        cash_drift_weight=float(payload.get("cash_drift_weight", 0.0)),
        post_trade_holdings=holdings,
        exception_reasons=[str(item) for item in payload.get("exception_reasons", [])],
        notes=[str(item) for item in payload.get("notes", [])],
    )


def load_realized_trading_window(path: Path | str) -> RealizedTradingWindow:
    payload = _read_json(path)
    _validate_artifact_header(
        payload=payload,
        expected_type="realized_trading_window",
        schema_label="realized trading window",
    )
    holdings = [
        PortfolioHolding(
            asset_id=str(item["asset_id"]),
            weight=float(item["weight"]),
        )
        for item in payload.get("realized_holdings", [])
    ]
    _validate_portfolio_holdings(holdings)
    return RealizedTradingWindow(
        run_id=str(payload["run_id"]),
        portfolio_id=str(payload["portfolio_id"]),
        evaluation_date=str(payload["evaluation_date"]),
        window_label=str(payload["window_label"]),
        window_start_date=str(payload["window_start_date"]),
        window_end_date=str(payload["window_end_date"]),
        realized_execution_basis=str(payload.get("realized_execution_basis", "")),
        realized_summary=_load_simulation_summary(dict(payload.get("realized_summary", {}))),
        realized_holdings=holdings,
        realized_cost=RealizedTradingCost(
            estimated_trading_cost=float(
                dict(payload.get("realized_cost", {})).get("estimated_trading_cost", 0.0)
            ),
            realized_trading_cost=float(
                dict(payload.get("realized_cost", {})).get("realized_trading_cost", 0.0)
            ),
            slippage_bps=float(dict(payload.get("realized_cost", {})).get("slippage_bps", 0.0)),
        ),
        realized_portfolio_path=[
            RealizedPortfolioPoint(
                trade_date=str(item["trade_date"]),
                nav=float(item["nav"]),
            )
            for item in payload.get("realized_portfolio_path", [])
        ],
    )


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

    regime_overlay = None
    regime_overlay_decision = None
    if portfolio.regime_overlay_id:
        regime_overlay = load_regime_overlay(
            CONFIG_ROOT / "regime_overlays" / f"{portfolio.regime_overlay_id}.toml"
        )
        if not definition.regime_overlay_observation_path:
            raise ValueError(
                "Executable signal case must define regime_overlay_observation_path when the portfolio declares regime_overlay_id."
            )
        observations = load_regime_overlay_observation_artifact(
            definition.regime_overlay_observation_path
        )
        if observations.overlay_id != regime_overlay.id:
            raise ValueError(
                "Executable signal case regime overlay observations must match the configured regime_overlay_id."
            )
        if regime_overlay.mandate_id != portfolio.mandate_id:
            raise ValueError(
                "Executable signal case regime overlay mandate must match the portfolio mandate."
            )
        if regime_overlay.benchmark != portfolio.benchmark:
            raise ValueError(
                "Executable signal case regime overlay benchmark must match the portfolio benchmark."
            )
        regime_overlay_decision = RegimeOverlayEvaluator(regime_overlay).evaluate_step(
            trade_date=definition.trade_date,
            observation=next(
                (
                    step
                    for step in observations.steps
                    if step.trade_date == definition.trade_date
                ),
                None,
            ),
        )
    elif definition.regime_overlay_observation_path:
        raise ValueError(
            "Executable signal case cannot define regime_overlay_observation_path without a portfolio regime_overlay_id."
        )

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
        regime_overlay=regime_overlay,
        regime_overlay_decision=regime_overlay_decision,
    )


def load_decay_watch_case(path: Path | str) -> LoadedDecayWatchCase:
    definition = DecayWatchCaseDefinition.from_toml(_read_toml(path))
    portfolio = load_portfolio(definition.portfolio_path)
    if not portfolio.decay_monitor_id:
        raise ValueError("Decay watch case portfolio must define a decay monitor.")
    decay_monitor = load_decay_monitor(
        CONFIG_ROOT / "decay_monitors" / f"{portfolio.decay_monitor_id}.toml"
    )
    run_manifest = None
    manual_execution_outcome = None
    realized_trading_window = None
    if (
        definition.run_manifest_path
        or definition.manual_execution_outcome_path
        or definition.realized_trading_window_path
    ):
        if not (
            definition.run_manifest_path
            and definition.manual_execution_outcome_path
            and definition.realized_trading_window_path
        ):
            raise ValueError(
                "Decay watch case must define run_manifest_path, manual_execution_outcome_path, and realized_trading_window_path together."
            )
        run_manifest = load_run_manifest(definition.run_manifest_path)
        manual_execution_outcome = load_manual_execution_outcome(
            definition.manual_execution_outcome_path
        )
        realized_trading_window = load_realized_trading_window(
            definition.realized_trading_window_path
        )
        _validate_decay_watch_trace(
            portfolio=portfolio,
            definition=definition,
            run_manifest=run_manifest,
            manual_execution_outcome=manual_execution_outcome,
            realized_trading_window=realized_trading_window,
        )
        realized_summary = realized_trading_window.realized_summary
    else:
        realized_summary = _load_simulation_summary(definition.realized_summary)
    return LoadedDecayWatchCase(
        definition=definition,
        portfolio=portfolio,
        decay_monitor=decay_monitor,
        promotion_snapshot=_load_promotion_snapshot(definition.promotion_snapshot),
        realized_summary=realized_summary,
        run_manifest=run_manifest,
        manual_execution_outcome=manual_execution_outcome,
        realized_trading_window=realized_trading_window,
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


def _validate_artifact_header(
    *,
    payload: JsonMap,
    expected_type: str,
    schema_label: str,
) -> None:
    schema_version = int(payload.get("schema_version", 0))
    if schema_version != 1:
        raise ValueError(f"Unsupported {schema_label} schema version: {schema_version}")
    artifact_type = str(payload.get("artifact_type", ""))
    if artifact_type != expected_type:
        raise ValueError(f"Unsupported {schema_label} type: {artifact_type}")


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


def _validate_decay_watch_trace(
    *,
    portfolio: PortfolioRecipe,
    definition: DecayWatchCaseDefinition,
    run_manifest: RunManifest,
    manual_execution_outcome: ManualExecutionOutcome,
    realized_trading_window: RealizedTradingWindow,
) -> None:
    if run_manifest.portfolio_id != portfolio.id:
        raise ValueError("Run manifest portfolio_id must match the decay watch portfolio.")
    if manual_execution_outcome.portfolio_id != portfolio.id:
        raise ValueError(
            "Manual execution outcome portfolio_id must match the decay watch portfolio."
        )
    if realized_trading_window.portfolio_id != portfolio.id:
        raise ValueError(
            "Realized trading window portfolio_id must match the decay watch portfolio."
        )
    run_ids = {
        run_manifest.run_id,
        manual_execution_outcome.run_id,
        realized_trading_window.run_id,
    }
    if len(run_ids) != 1:
        raise ValueError("Decay watch execution trace artifacts must share one run_id.")
    if manual_execution_outcome.package_id != run_manifest.package_id:
        raise ValueError("Manual execution outcome package_id must match the run manifest.")
    if manual_execution_outcome.execution_date != run_manifest.execution_date:
        raise ValueError("Manual execution outcome execution_date must match the run manifest.")
    if realized_trading_window.evaluation_date != definition.evaluation_date:
        raise ValueError(
            "Realized trading window evaluation_date must match the decay watch case."
        )
    if realized_trading_window.window_label != definition.window_label:
        raise ValueError(
            "Realized trading window window_label must match the decay watch case."
        )
