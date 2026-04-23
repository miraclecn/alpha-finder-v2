from __future__ import annotations

from dataclasses import dataclass, field

from .models import CostModel, DecayMonitor, ExecutionPolicy, Mandate, PortfolioRecipe
from .portfolio_simulator import (
    PortfolioRebalanceInput,
    PortfolioSecuritySignal,
    PortfolioSimulator,
    TradeConstraintState,
)
from .promotion_gate_evaluator import SleevePromotionSnapshot
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
    investable_budget: float
    estimated_turnover: float
    estimated_trading_cost: float
    cash_weight: float
    blocked_entries: list[str] = field(default_factory=list)
    blocked_exits: list[str] = field(default_factory=list)
    instructions: list[ExecutableSignalInstruction] = field(default_factory=list)


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
    ) -> None:
        self.mandate = mandate
        self.portfolio = portfolio
        self.execution_policy = execution_policy
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
    ) -> ExecutableSignalPackage:
        current_weights = portfolio_state.current_weights()
        adjusted_signals = self._signals_with_portfolio_state(
            signals=signals,
            portfolio_state=portfolio_state,
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
            investable_budget=self.simulator.investable_budget(),
            estimated_turnover=step.turnover,
            estimated_trading_cost=step.trading_cost,
            cash_weight=max(1.0 - sum(step.executed_weights.values()), 0.0),
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


def _safe_ratio(actual: float, reference: float) -> float:
    if reference <= 0.0:
        return 0.0 if actual < 0.0 else 1.0
    return actual / reference


def _safe_multiple(actual: float, reference: float) -> float:
    if reference <= 0.0:
        return 0.0 if actual <= 0.0 else float("inf")
    return actual / reference
