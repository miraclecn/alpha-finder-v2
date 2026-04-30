from __future__ import annotations

import math

from .portfolio_constructor import PortfolioConstructionResult, PortfolioConstructionStep
from .portfolio_simulator import PortfolioSecuritySignal, TradeConstraintState


_STAGGERED_POLICY_STRIDE_DAYS = {
    "staggered_twice_weekly": 2,
    "staggered_weekly": 5,
    "staggered_biweekly": 10,
    "staggered_monthly": 20,
}


def tranche_count_for_policy(
    *,
    rebalance_policy: str,
    horizon_days: int,
) -> int:
    stride_days = _STAGGERED_POLICY_STRIDE_DAYS.get(rebalance_policy)
    if stride_days is None:
        if rebalance_policy.startswith("staggered_"):
            raise ValueError(f"Unsupported staggered rebalance_policy: {rebalance_policy}")
        return 1
    if horizon_days <= 0:
        raise ValueError("Staggered horizon_days must be positive.")
    return max(1, math.ceil(horizon_days / stride_days))


def apply_staggered_construction(
    construction: PortfolioConstructionResult,
    *,
    tranche_count: int,
) -> PortfolioConstructionResult:
    if tranche_count <= 1:
        return construction

    adjusted_steps: list[PortfolioConstructionStep] = []
    for index, current_step in enumerate(construction.steps):
        start_index = max(0, index - tranche_count + 1)
        active_steps = construction.steps[start_index : index + 1]
        exiting_step = construction.steps[index - tranche_count] if index >= tranche_count else None
        signals = _aggregate_signals(
            active_steps=active_steps,
            current_step=current_step,
            exiting_step=exiting_step,
            tranche_count=tranche_count,
        )
        adjusted_steps.append(
            PortfolioConstructionStep(
                trade_date=current_step.trade_date,
                combined_weights={
                    signal.asset_id: signal.target_weight
                    for signal in signals
                },
                signals=signals,
                overlap_names=list(current_step.overlap_names),
                dropped_names=list(current_step.dropped_names),
                capped_names=list(current_step.capped_names),
                industry_scaled_names=list(current_step.industry_scaled_names),
                cash_weight=max(1.0 - sum(signal.target_weight for signal in signals), 0.0),
                selection_cash_weight=(
                    sum(step.selection_cash_weight for step in active_steps) / tranche_count
                ),
                single_name_cap_cash_weight=(
                    sum(step.single_name_cap_cash_weight for step in active_steps) / tranche_count
                ),
                industry_cap_cash_weight=(
                    sum(step.industry_cap_cash_weight for step in active_steps) / tranche_count
                ),
            )
        )
    return PortfolioConstructionResult(steps=adjusted_steps)


def _aggregate_signals(
    *,
    active_steps: list[PortfolioConstructionStep],
    current_step: PortfolioConstructionStep,
    exiting_step: PortfolioConstructionStep | None,
    tranche_count: int,
) -> list[PortfolioSecuritySignal]:
    current_by_asset = {signal.asset_id: signal for signal in current_step.signals}
    exiting_by_asset = (
        {signal.asset_id: signal for signal in exiting_step.signals}
        if exiting_step is not None
        else {}
    )
    aggregates: dict[str, dict[str, object]] = {}

    for step in active_steps:
        for signal in step.signals:
            if signal.target_weight <= 0.0:
                continue
            aggregate = aggregates.setdefault(
                signal.asset_id,
                {
                    "target_weight_sum": 0.0,
                    "target_return_sum": 0.0,
                    "cost_model_id": "",
                    "industry": "",
                },
            )
            aggregate["target_weight_sum"] = float(aggregate["target_weight_sum"]) + signal.target_weight
            aggregate["target_return_sum"] = (
                float(aggregate["target_return_sum"])
                + (signal.target_weight * signal.realized_return)
            )
            if signal.cost_model_id:
                aggregate["cost_model_id"] = signal.cost_model_id
            if signal.industry:
                aggregate["industry"] = signal.industry

    signals: list[PortfolioSecuritySignal] = []
    for asset_id, aggregate in aggregates.items():
        target_weight_sum = float(aggregate["target_weight_sum"])
        if target_weight_sum <= 0.0:
            continue
        realized_return = (
            float(aggregate["target_return_sum"]) / target_weight_sum / tranche_count
        )
        current_signal = current_by_asset.get(asset_id)
        exiting_signal = exiting_by_asset.get(asset_id)
        signals.append(
            PortfolioSecuritySignal(
                asset_id=asset_id,
                target_weight=target_weight_sum / tranche_count,
                realized_return=realized_return,
                cost_model_id=str(aggregate["cost_model_id"]),
                industry=str(aggregate["industry"]),
                trade_state=TradeConstraintState(
                    can_enter=(
                        current_signal.trade_state.can_enter
                        if current_signal is not None
                        else True
                    ),
                    can_exit=(
                        exiting_signal.trade_state.can_exit
                        if exiting_signal is not None
                        else True
                    ),
                ),
            )
        )

    signals.sort(key=lambda signal: (-signal.target_weight, signal.asset_id))
    return signals
