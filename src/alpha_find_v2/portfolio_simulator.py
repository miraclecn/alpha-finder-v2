from __future__ import annotations

from dataclasses import dataclass, field

from .models import CostModel, Mandate, PortfolioRecipe


@dataclass(slots=True)
class TradeConstraintState:
    can_enter: bool = True
    can_exit: bool = True


@dataclass(slots=True)
class PortfolioSecuritySignal:
    asset_id: str
    target_weight: float
    realized_return: float
    cost_model_id: str = ""
    industry: str = ""
    trade_state: TradeConstraintState = field(default_factory=TradeConstraintState)


@dataclass(slots=True)
class PortfolioRebalanceInput:
    trade_date: str
    signals: list[PortfolioSecuritySignal] = field(default_factory=list)


@dataclass(slots=True)
class PortfolioStepResult:
    trade_date: str
    target_weights: dict[str, float] = field(default_factory=dict)
    executed_weights: dict[str, float] = field(default_factory=dict)
    buy_turnover: float = 0.0
    sell_turnover: float = 0.0
    turnover: float = 0.0
    trading_cost: float = 0.0
    gross_return: float = 0.0
    net_return: float = 0.0
    blocked_entries: list[str] = field(default_factory=list)
    blocked_exits: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PortfolioSimulationResult:
    steps: list[PortfolioStepResult] = field(default_factory=list)
    final_weights: dict[str, float] = field(default_factory=dict)


class PortfolioSimulator:
    def __init__(
        self,
        mandate: Mandate,
        portfolio: PortfolioRecipe,
        default_cost_model: CostModel,
        cost_models: dict[str, CostModel] | None = None,
    ) -> None:
        self.mandate = mandate
        self.portfolio = portfolio
        self.default_cost_model = default_cost_model
        self.cost_models = {default_cost_model.id: default_cost_model}
        if cost_models:
            self.cost_models.update(cost_models)

    def run(self, rebalances: list[PortfolioRebalanceInput]) -> PortfolioSimulationResult:
        current_weights: dict[str, float] = {}
        steps: list[PortfolioStepResult] = []

        for rebalance in rebalances:
            step = self.plan_rebalance(
                rebalance=rebalance,
                current_weights=current_weights,
            )
            steps.append(step)
            current_weights = step.executed_weights

        return PortfolioSimulationResult(
            steps=steps,
            final_weights=current_weights,
        )

    def plan_rebalance(
        self,
        rebalance: PortfolioRebalanceInput,
        current_weights: dict[str, float] | None = None,
    ) -> PortfolioStepResult:
        current = dict(current_weights or {})
        signals_by_asset = self._signals_by_asset(rebalance.signals)
        target_weights = self._target_weights(rebalance.signals)
        executed_weights, blocked_entries, blocked_exits = self._apply_trade_constraints(
            current_weights=current,
            target_weights=target_weights,
            signals_by_asset=signals_by_asset,
        )
        buy_turnover, sell_turnover, trading_cost = self._trading_costs(
            current_weights=current,
            executed_weights=executed_weights,
            signals_by_asset=signals_by_asset,
        )
        gross_return = self._gross_return(
            executed_weights=executed_weights,
            signals_by_asset=signals_by_asset,
        )
        net_return = gross_return - trading_cost
        return PortfolioStepResult(
            trade_date=rebalance.trade_date,
            target_weights=target_weights,
            executed_weights=executed_weights,
            buy_turnover=buy_turnover,
            sell_turnover=sell_turnover,
            turnover=max(buy_turnover, sell_turnover),
            trading_cost=trading_cost,
            gross_return=gross_return,
            net_return=net_return,
            blocked_entries=blocked_entries,
            blocked_exits=blocked_exits,
        )

    def _signals_by_asset(
        self,
        signals: list[PortfolioSecuritySignal],
    ) -> dict[str, PortfolioSecuritySignal]:
        signals_by_asset: dict[str, PortfolioSecuritySignal] = {}
        for signal in signals:
            if signal.asset_id in signals_by_asset:
                raise ValueError(f"Duplicate signal for asset: {signal.asset_id}")
            signals_by_asset[signal.asset_id] = signal
        return signals_by_asset

    def _target_weights(self, signals: list[PortfolioSecuritySignal]) -> dict[str, float]:
        investable_budget = self._investable_budget()
        positive_signals = [
            signal
            for signal in signals
            if signal.target_weight > 0.0
        ]
        total_target = sum(signal.target_weight for signal in positive_signals)
        if total_target <= 0.0:
            return {}

        scale = min(1.0, investable_budget / total_target)
        return {
            signal.asset_id: signal.target_weight * scale
            for signal in positive_signals
        }

    def _apply_trade_constraints(
        self,
        current_weights: dict[str, float],
        target_weights: dict[str, float],
        signals_by_asset: dict[str, PortfolioSecuritySignal],
    ) -> tuple[dict[str, float], list[str], list[str]]:
        investable_budget = self._investable_budget()
        blocked_entries: list[str] = []
        blocked_exits: list[str] = []
        fixed_weights: dict[str, float] = {}

        all_assets = set(current_weights) | set(target_weights)
        for asset_id in all_assets:
            current_weight = current_weights.get(asset_id, 0.0)
            target_weight = target_weights.get(asset_id, 0.0)
            signal = signals_by_asset.get(asset_id)

            if current_weight > target_weight and signal is not None and not signal.trade_state.can_exit:
                fixed_weights[asset_id] = current_weight
                blocked_exits.append(asset_id)
                continue

            if target_weight > current_weight and signal is not None and not signal.trade_state.can_enter:
                fixed_weights[asset_id] = current_weight
                blocked_entries.append(asset_id)

        remaining_budget = max(investable_budget - sum(fixed_weights.values()), 0.0)
        adjustable_targets = {
            asset_id: weight
            for asset_id, weight in target_weights.items()
            if asset_id not in fixed_weights and weight > 0.0
        }
        total_adjustable = sum(adjustable_targets.values())

        executed_weights = dict(fixed_weights)
        if total_adjustable > 0.0 and remaining_budget > 0.0:
            scale = min(1.0, remaining_budget / total_adjustable)
            for asset_id, weight in adjustable_targets.items():
                executed_weights[asset_id] = weight * scale

        return executed_weights, sorted(blocked_entries), sorted(blocked_exits)

    def _trading_costs(
        self,
        current_weights: dict[str, float],
        executed_weights: dict[str, float],
        signals_by_asset: dict[str, PortfolioSecuritySignal],
    ) -> tuple[float, float, float]:
        buy_turnover = 0.0
        sell_turnover = 0.0
        trading_cost = 0.0

        for asset_id in set(current_weights) | set(executed_weights):
            current_weight = current_weights.get(asset_id, 0.0)
            executed_weight = executed_weights.get(asset_id, 0.0)
            delta = executed_weight - current_weight
            if delta == 0.0:
                continue

            cost_model = self._cost_model_for_asset(asset_id, signals_by_asset)
            if delta > 0.0:
                buy_turnover += delta
                trading_cost += delta * (cost_model.buy_total_bps() / 10_000.0)
                continue

            sell_turnover += -delta
            trading_cost += (-delta) * (cost_model.sell_total_bps() / 10_000.0)

        return buy_turnover, sell_turnover, trading_cost

    def _gross_return(
        self,
        executed_weights: dict[str, float],
        signals_by_asset: dict[str, PortfolioSecuritySignal],
    ) -> float:
        gross_return = 0.0
        for asset_id, weight in executed_weights.items():
            signal = signals_by_asset.get(asset_id)
            if signal is None:
                raise ValueError(
                    f"Missing realized return for executed holding: {asset_id}"
                )
            gross_return += weight * signal.realized_return
        return gross_return

    def _cost_model_for_asset(
        self,
        asset_id: str,
        signals_by_asset: dict[str, PortfolioSecuritySignal],
    ) -> CostModel:
        signal = signals_by_asset.get(asset_id)
        if signal is None or not signal.cost_model_id:
            return self.default_cost_model

        if signal.cost_model_id not in self.cost_models:
            raise ValueError(f"Unknown cost model for asset {asset_id}: {signal.cost_model_id}")
        return self.cost_models[signal.cost_model_id]

    def _investable_budget(self) -> float:
        cash_buffer = self.portfolio.constraints.get(
            "cash_buffer",
            self.mandate.risk.get("cash_buffer", 0.0),
        )
        return max(1.0 - float(cash_buffer), 0.0)

    def investable_budget(self) -> float:
        return self._investable_budget()
