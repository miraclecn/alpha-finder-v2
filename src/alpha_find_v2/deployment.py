from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path

from .config_loader import PROJECT_ROOT
from .models import (
    CostModel,
    DecayMonitor,
    ExecutionPolicy,
    Mandate,
    PortfolioRecipe,
    RegimeOverlay,
)
from .portfolio_simulator import (
    PortfolioRebalanceInput,
    PortfolioSecuritySignal,
    PortfolioSimulator,
    TradeConstraintState,
)
from .promotion_gate_evaluator import SleevePromotionSnapshot
from .regime_overlay import RegimeOverlayDecision
from .research_evaluator import SimulationSummary


@dataclass(slots=True)
class ExecutableSignalInstruction:
    asset_id: str
    action: str
    current_weight: float
    proposed_target_weight: float
    executable_target_weight: float
    delta_weight: float
    cost_model_id: str = ""
    industry: str = ""


@dataclass(slots=True)
class PortfolioHolding:
    asset_id: str
    weight: float


@dataclass(slots=True)
class PortfolioState:
    portfolio_id: str
    account_id: str
    as_of_date: str
    cash_weight: float = 0.0
    holdings: list[PortfolioHolding] = field(default_factory=list)
    blocked_entry_assets: list[str] = field(default_factory=list)
    blocked_exit_assets: list[str] = field(default_factory=list)

    def current_weights(self) -> dict[str, float]:
        return {
            holding.asset_id: holding.weight
            for holding in self.holdings
            if holding.weight > 0.0
        }


@dataclass(slots=True)
class ExecutableSignalPackage:
    package_id: str
    portfolio_id: str
    mandate_id: str
    trade_date: str
    execution_date: str
    execution_policy_id: str
    rebalance_policy: str
    base_investable_budget: float
    investable_budget: float
    target_cash_weight: float
    estimated_turnover: float
    estimated_trading_cost: float
    cash_weight: float
    regime_overlay_status: str = ""
    regime_overlay_state: str = ""
    regime_overlay_missing_inputs: list[str] = field(default_factory=list)
    regime_overlay_invalid_inputs: list[str] = field(default_factory=list)
    blocked_entries: list[str] = field(default_factory=list)
    blocked_exits: list[str] = field(default_factory=list)
    instructions: list[ExecutableSignalInstruction] = field(default_factory=list)


@dataclass(slots=True)
class RunManifest:
    run_id: str
    package_id: str
    portfolio_id: str
    mandate_id: str
    trade_date: str
    execution_date: str
    data_version: str
    data_build_date: str
    benchmark_state_path: str = ""
    sleeve_artifact_paths: list[str] = field(default_factory=list)
    portfolio_path: str = ""
    account_state_path: str = ""
    portfolio_state_path: str = ""
    execution_policy_id: str = ""
    base_investable_budget: float = 0.0
    investable_budget: float = 0.0
    target_cash_weight: float = 0.0
    cash_weight: float = 0.0
    regime_overlay_status: str = ""
    regime_overlay_state: str = ""
    operator_id: str = ""
    operator_timestamp: str = ""


@dataclass(slots=True)
class ManualExecutionAttempt:
    asset_id: str
    action: str
    requested_delta_weight: float
    executed_delta_weight: float
    status: str
    reason: str = ""


@dataclass(slots=True)
class ManualExecutionBlock:
    asset_id: str
    action: str
    requested_delta_weight: float
    reason: str = ""


@dataclass(slots=True)
class ManualExecutionOverride:
    asset_id: str
    override_type: str
    from_target_weight: float
    to_target_weight: float
    reason: str = ""


@dataclass(slots=True)
class ManualExecutionOutcome:
    run_id: str
    package_id: str
    portfolio_id: str
    execution_date: str
    attempted_orders: list[ManualExecutionAttempt] = field(default_factory=list)
    blocked_trades: list[ManualExecutionBlock] = field(default_factory=list)
    manual_overrides: list[ManualExecutionOverride] = field(default_factory=list)
    cash_drift_weight: float = 0.0
    post_trade_holdings: list[PortfolioHolding] = field(default_factory=list)
    exception_reasons: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RealizedTradingCost:
    estimated_trading_cost: float = 0.0
    realized_trading_cost: float = 0.0
    slippage_bps: float = 0.0


@dataclass(slots=True)
class RealizedPortfolioPoint:
    trade_date: str
    nav: float


@dataclass(slots=True)
class RealizedTradingWindow:
    run_id: str
    portfolio_id: str
    evaluation_date: str
    window_label: str
    window_start_date: str
    window_end_date: str
    realized_execution_basis: str
    realized_summary: SimulationSummary
    realized_holdings: list[PortfolioHolding] = field(default_factory=list)
    realized_cost: RealizedTradingCost = field(default_factory=RealizedTradingCost)
    realized_portfolio_path: list[RealizedPortfolioPoint] = field(default_factory=list)


@dataclass(slots=True)
class DecayRecord:
    record_id: str
    portfolio_id: str
    mandate_id: str
    evaluation_date: str
    window_label: str
    decay_monitor_id: str
    status: str
    reference_ir: float
    realized_ir: float
    ir_ratio: float
    reference_drawdown: float
    realized_drawdown: float
    drawdown_multiple: float
    turnover_budget: float
    realized_turnover: float
    turnover_vs_budget: float
    reference_breadth: int
    realized_breadth: int
    breadth_ratio: float
    blocked_name_share: float
    run_id: str = ""
    realized_execution_basis: str = ""
    blocked_trade_count: int = 0
    manual_override_count: int = 0
    exception_count: int = 0
    cash_drift_weight: float = 0.0
    warning_breaches: list[str] = field(default_factory=list)
    retirement_breaches: list[str] = field(default_factory=list)


class ExecutableSignalBuilder:
    def __init__(
        self,
        mandate: Mandate,
        portfolio: PortfolioRecipe,
        execution_policy: ExecutionPolicy,
        default_cost_model: CostModel,
        cost_models: dict[str, CostModel] | None = None,
        portfolio_overlay: RegimeOverlay | None = None,
    ) -> None:
        self.mandate = mandate
        self.portfolio = portfolio
        self.execution_policy = execution_policy
        self.portfolio_overlay = portfolio_overlay
        self.simulator = PortfolioSimulator(
            mandate=mandate,
            portfolio=portfolio,
            default_cost_model=default_cost_model,
            cost_models=cost_models,
        )

    def build(
        self,
        *,
        trade_date: str,
        signals: list[PortfolioSecuritySignal],
        portfolio_state: PortfolioState,
        execution_date: str = "",
        regime_overlay_decision: RegimeOverlayDecision | None = None,
    ) -> ExecutableSignalPackage:
        current_weights = portfolio_state.current_weights()
        overlay_exposure = self._overlay_gross_exposure(regime_overlay_decision)
        base_investable_budget = self.simulator.investable_budget()
        requested_investable_budget = base_investable_budget * overlay_exposure
        adjusted_signals = self._signals_with_portfolio_state(
            signals=signals,
            portfolio_state=portfolio_state,
        )
        adjusted_signals = self._signals_with_overlay_budget(
            signals=adjusted_signals,
            overlay_exposure=overlay_exposure,
        )
        rebalance = PortfolioRebalanceInput(trade_date=trade_date, signals=adjusted_signals)
        step = self.simulator.plan_rebalance(rebalance, current_weights=current_weights)
        signals_by_asset = {signal.asset_id: signal for signal in adjusted_signals}

        instruction_assets = set(current_weights) | set(step.target_weights) | set(step.executed_weights)
        instructions = [
            self._instruction_for_asset(
                asset_id=asset_id,
                current_weight=current_weights.get(asset_id, 0.0),
                proposed_target_weight=step.target_weights.get(asset_id, 0.0),
                executable_target_weight=step.executed_weights.get(asset_id, 0.0),
                blocked_entries=step.blocked_entries,
                blocked_exits=step.blocked_exits,
                signals_by_asset=signals_by_asset,
            )
            for asset_id in instruction_assets
        ]
        instructions = [
            instruction
            for instruction in instructions
            if instruction is not None
        ]
        instructions.sort(key=lambda item: (-abs(item.delta_weight), item.asset_id))

        return ExecutableSignalPackage(
            package_id=f"{self.portfolio.id}:{trade_date}",
            portfolio_id=self.portfolio.id,
            mandate_id=self.mandate.id,
            trade_date=trade_date,
            execution_date=execution_date or trade_date,
            execution_policy_id=self.execution_policy.id,
            rebalance_policy=self.portfolio.rebalance_policy,
            base_investable_budget=base_investable_budget,
            investable_budget=requested_investable_budget,
            target_cash_weight=max(1.0 - requested_investable_budget, 0.0),
            estimated_turnover=step.turnover,
            estimated_trading_cost=step.trading_cost,
            cash_weight=max(1.0 - sum(step.executed_weights.values()), 0.0),
            regime_overlay_status=(
                regime_overlay_decision.status if regime_overlay_decision is not None else ""
            ),
            regime_overlay_state=(
                regime_overlay_decision.state if regime_overlay_decision is not None else ""
            ),
            regime_overlay_missing_inputs=(
                list(regime_overlay_decision.missing_inputs)
                if regime_overlay_decision is not None
                else []
            ),
            regime_overlay_invalid_inputs=(
                list(regime_overlay_decision.invalid_inputs)
                if regime_overlay_decision is not None
                else []
            ),
            blocked_entries=step.blocked_entries,
            blocked_exits=step.blocked_exits,
            instructions=instructions,
        )

    def _signals_with_portfolio_state(
        self,
        *,
        signals: list[PortfolioSecuritySignal],
        portfolio_state: PortfolioState,
    ) -> list[PortfolioSecuritySignal]:
        blocked_entries = set(portfolio_state.blocked_entry_assets)
        blocked_exits = set(portfolio_state.blocked_exit_assets)
        adjusted_signals = [
            PortfolioSecuritySignal(
                asset_id=signal.asset_id,
                target_weight=signal.target_weight,
                realized_return=signal.realized_return,
                cost_model_id=signal.cost_model_id,
                industry=signal.industry,
                trade_state=TradeConstraintState(
                    can_enter=signal.trade_state.can_enter and signal.asset_id not in blocked_entries,
                    can_exit=signal.trade_state.can_exit and signal.asset_id not in blocked_exits,
                ),
            )
            for signal in signals
        ]

        known_assets = {signal.asset_id for signal in adjusted_signals}
        current_weights = portfolio_state.current_weights()
        for asset_id in sorted(blocked_exits):
            if asset_id in known_assets or current_weights.get(asset_id, 0.0) <= 0.0:
                continue
            adjusted_signals.append(
                PortfolioSecuritySignal(
                    asset_id=asset_id,
                    target_weight=0.0,
                    realized_return=0.0,
                    trade_state=TradeConstraintState(can_enter=False, can_exit=False),
                )
            )

        return adjusted_signals

    def _signals_with_overlay_budget(
        self,
        *,
        signals: list[PortfolioSecuritySignal],
        overlay_exposure: float,
    ) -> list[PortfolioSecuritySignal]:
        return [
            PortfolioSecuritySignal(
                asset_id=signal.asset_id,
                target_weight=signal.target_weight * overlay_exposure,
                realized_return=signal.realized_return,
                cost_model_id=signal.cost_model_id,
                industry=signal.industry,
                trade_state=signal.trade_state,
            )
            for signal in signals
        ]

    def _overlay_gross_exposure(
        self,
        regime_overlay_decision: RegimeOverlayDecision | None,
    ) -> float:
        if regime_overlay_decision is None or self.portfolio_overlay is None:
            return 1.0
        if regime_overlay_decision.state == "cash_heavier":
            return self.portfolio_overlay.cash_heavier_gross_exposure
        if regime_overlay_decision.state == "de_risk":
            return self.portfolio_overlay.de_risk_gross_exposure
        return self.portfolio_overlay.normal_gross_exposure

    def _instruction_for_asset(
        self,
        *,
        asset_id: str,
        current_weight: float,
        proposed_target_weight: float,
        executable_target_weight: float,
        blocked_entries: list[str],
        blocked_exits: list[str],
        signals_by_asset: dict[str, PortfolioSecuritySignal],
    ) -> ExecutableSignalInstruction | None:
        delta_weight = executable_target_weight - current_weight
        if asset_id in blocked_entries:
            action = "skip_enter_blocked"
        elif asset_id in blocked_exits:
            action = "hold_exit_blocked"
        elif delta_weight > 0.0:
            action = "buy"
        elif delta_weight < 0.0:
            action = "sell"
        elif executable_target_weight > 0.0:
            action = "hold"
        else:
            return None

        signal = signals_by_asset.get(asset_id)
        return ExecutableSignalInstruction(
            asset_id=asset_id,
            action=action,
            current_weight=current_weight,
            proposed_target_weight=proposed_target_weight,
            executable_target_weight=executable_target_weight,
            delta_weight=delta_weight,
            cost_model_id=signal.cost_model_id if signal is not None else "",
            industry=signal.industry if signal is not None else "",
        )


class DecayMonitorEvaluator:
    def __init__(self, monitor: DecayMonitor) -> None:
        self.monitor = monitor

    def evaluate(
        self,
        *,
        portfolio: PortfolioRecipe,
        evaluation_date: str,
        window_label: str,
        promotion_snapshot: SleevePromotionSnapshot,
        realized_summary: SimulationSummary,
        run_manifest: RunManifest | None = None,
        manual_execution_outcome: ManualExecutionOutcome | None = None,
        realized_trading_window: RealizedTradingWindow | None = None,
    ) -> DecayRecord:
        ir_ratio = _safe_ratio(realized_summary.ir, promotion_snapshot.oos_ir)
        drawdown_multiple = _safe_multiple(
            realized_summary.peak_to_trough_drawdown,
            promotion_snapshot.peak_to_trough_drawdown,
        )
        turnover_vs_budget = _safe_ratio(
            realized_summary.average_turnover,
            promotion_snapshot.turnover_budget,
        )
        breadth_ratio = _safe_ratio(
            float(realized_summary.breadth),
            float(promotion_snapshot.breadth),
        )

        warning_breaches = self._breaches(
            thresholds=self.monitor.warning_thresholds,
            ir_ratio=ir_ratio,
            drawdown_multiple=drawdown_multiple,
            turnover_vs_budget=turnover_vs_budget,
            breadth_ratio=breadth_ratio,
            blocked_name_share=realized_summary.blocked_name_share,
        )
        retirement_breaches = self._breaches(
            thresholds=self.monitor.retirement_thresholds,
            ir_ratio=ir_ratio,
            drawdown_multiple=drawdown_multiple,
            turnover_vs_budget=turnover_vs_budget,
            breadth_ratio=breadth_ratio,
            blocked_name_share=realized_summary.blocked_name_share,
        )

        status = "healthy"
        if retirement_breaches:
            status = "retire"
        elif warning_breaches:
            status = "watch"

        return DecayRecord(
            record_id=f"{portfolio.id}:{window_label}:{evaluation_date}",
            portfolio_id=portfolio.id,
            mandate_id=portfolio.mandate_id,
            evaluation_date=evaluation_date,
            window_label=window_label,
            decay_monitor_id=self.monitor.id,
            status=status,
            reference_ir=promotion_snapshot.oos_ir,
            realized_ir=realized_summary.ir,
            ir_ratio=ir_ratio,
            reference_drawdown=promotion_snapshot.peak_to_trough_drawdown,
            realized_drawdown=realized_summary.peak_to_trough_drawdown,
            drawdown_multiple=drawdown_multiple,
            turnover_budget=promotion_snapshot.turnover_budget,
            realized_turnover=realized_summary.average_turnover,
            turnover_vs_budget=turnover_vs_budget,
            reference_breadth=promotion_snapshot.breadth,
            realized_breadth=realized_summary.breadth,
            breadth_ratio=breadth_ratio,
            blocked_name_share=realized_summary.blocked_name_share,
            run_id=_run_id(
                run_manifest=run_manifest,
                manual_execution_outcome=manual_execution_outcome,
                realized_trading_window=realized_trading_window,
            ),
            realized_execution_basis=(
                realized_trading_window.realized_execution_basis
                if realized_trading_window is not None
                else ""
            ),
            blocked_trade_count=(
                len(manual_execution_outcome.blocked_trades)
                if manual_execution_outcome is not None
                else 0
            ),
            manual_override_count=(
                len(manual_execution_outcome.manual_overrides)
                if manual_execution_outcome is not None
                else 0
            ),
            exception_count=(
                len(manual_execution_outcome.exception_reasons)
                if manual_execution_outcome is not None
                else 0
            ),
            cash_drift_weight=(
                manual_execution_outcome.cash_drift_weight
                if manual_execution_outcome is not None
                else 0.0
            ),
            warning_breaches=warning_breaches,
            retirement_breaches=retirement_breaches,
        )

    def _breaches(
        self,
        *,
        thresholds: dict[str, object],
        ir_ratio: float,
        drawdown_multiple: float,
        turnover_vs_budget: float,
        breadth_ratio: float,
        blocked_name_share: float,
    ) -> list[str]:
        breaches: list[str] = []

        min_ir_ratio = thresholds.get("min_ir_ratio")
        if min_ir_ratio is not None and ir_ratio < float(min_ir_ratio):
            breaches.append("min_ir_ratio")

        max_drawdown_multiple = thresholds.get("max_drawdown_multiple")
        if max_drawdown_multiple is not None and drawdown_multiple > float(
            max_drawdown_multiple
        ):
            breaches.append("max_drawdown_multiple")

        max_turnover_vs_budget = thresholds.get("max_turnover_vs_budget")
        if max_turnover_vs_budget is not None and turnover_vs_budget > float(
            max_turnover_vs_budget
        ):
            breaches.append("max_turnover_vs_budget")

        min_breadth_ratio = thresholds.get("min_breadth_ratio")
        if min_breadth_ratio is not None and breadth_ratio < float(min_breadth_ratio):
            breaches.append("min_breadth_ratio")

        max_blocked_name_share = thresholds.get("max_blocked_name_share")
        if max_blocked_name_share is not None and blocked_name_share > float(
            max_blocked_name_share
        ):
            breaches.append("max_blocked_name_share")

        return breaches


def build_run_manifest(
    *,
    run_id: str,
    package: ExecutableSignalPackage,
    benchmark_state_path: str,
    sleeve_artifact_paths: list[str],
    portfolio_path: str,
    account_state_path: str,
    portfolio_state_path: str,
    data_version: str,
    data_build_date: str,
    operator_id: str,
    operator_timestamp: str,
) -> RunManifest:
    return RunManifest(
        run_id=run_id,
        package_id=package.package_id,
        portfolio_id=package.portfolio_id,
        mandate_id=package.mandate_id,
        trade_date=package.trade_date,
        execution_date=package.execution_date,
        data_version=data_version,
        data_build_date=data_build_date,
        benchmark_state_path=benchmark_state_path,
        sleeve_artifact_paths=list(sleeve_artifact_paths),
        portfolio_path=portfolio_path,
        account_state_path=account_state_path,
        portfolio_state_path=portfolio_state_path,
        execution_policy_id=package.execution_policy_id,
        base_investable_budget=package.base_investable_budget,
        investable_budget=package.investable_budget,
        target_cash_weight=package.target_cash_weight,
        cash_weight=package.cash_weight,
        regime_overlay_status=package.regime_overlay_status,
        regime_overlay_state=package.regime_overlay_state,
        operator_id=operator_id,
        operator_timestamp=operator_timestamp,
    )


def write_run_manifest(
    manifest: RunManifest,
    path: Path | str,
) -> Path:
    target = _resolve_project_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = _json_ready(
        {
            "schema_version": 1,
            "artifact_type": "run_manifest",
            **asdict(manifest),
        }
    )
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return target


def _safe_ratio(actual: float, reference: float) -> float:
    if reference <= 0.0:
        return 0.0 if actual < 0.0 else 1.0
    return actual / reference


def _safe_multiple(actual: float, reference: float) -> float:
    if reference <= 0.0:
        return 0.0 if actual <= 0.0 else float("inf")
    return actual / reference


def _run_id(
    *,
    run_manifest: RunManifest | None,
    manual_execution_outcome: ManualExecutionOutcome | None,
    realized_trading_window: RealizedTradingWindow | None,
) -> str:
    for candidate in (realized_trading_window, manual_execution_outcome, run_manifest):
        if candidate is not None and getattr(candidate, "run_id", ""):
            return str(candidate.run_id)
    return ""


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
