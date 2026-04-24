from __future__ import annotations

from dataclasses import dataclass, field

from .models import Mandate, PortfolioConstructionModel, PortfolioRecipe
from .portfolio_simulator import (
    PortfolioRebalanceInput,
    PortfolioSecuritySignal,
    TradeConstraintState,
)


@dataclass(slots=True)
class SleeveConstructionInput:
    sleeve_id: str
    signals: list[PortfolioSecuritySignal] = field(default_factory=list)


@dataclass(slots=True)
class PortfolioConstructionInput:
    trade_date: str
    sleeves: list[SleeveConstructionInput] = field(default_factory=list)
    benchmark_industry_weights: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class PortfolioConstructionStep:
    trade_date: str
    combined_weights: dict[str, float] = field(default_factory=dict)
    signals: list[PortfolioSecuritySignal] = field(default_factory=list)
    overlap_names: list[str] = field(default_factory=list)
    dropped_names: list[str] = field(default_factory=list)
    capped_names: list[str] = field(default_factory=list)
    industry_scaled_names: list[str] = field(default_factory=list)
    cash_weight: float = 0.0


@dataclass(slots=True)
class PortfolioConstructionResult:
    steps: list[PortfolioConstructionStep] = field(default_factory=list)

    def to_rebalance_inputs(self) -> list[PortfolioRebalanceInput]:
        return [
            PortfolioRebalanceInput(trade_date=step.trade_date, signals=step.signals)
            for step in self.steps
        ]


class PortfolioConstructor:
    def __init__(
        self,
        mandate: Mandate,
        portfolio: PortfolioRecipe,
        construction_model: PortfolioConstructionModel,
    ) -> None:
        self.mandate = mandate
        self.portfolio = portfolio
        self.construction_model = construction_model

    def build(self, inputs: list[PortfolioConstructionInput]) -> PortfolioConstructionResult:
        return PortfolioConstructionResult(
            steps=[self._build_step(construction_input) for construction_input in inputs]
        )

    def _build_step(self, construction_input: PortfolioConstructionInput) -> PortfolioConstructionStep:
        combined_weights: dict[str, float] = {}
        metadata_by_asset: dict[str, PortfolioSecuritySignal] = {}
        asset_sleeves: dict[str, set[str]] = {}

        for sleeve_input in construction_input.sleeves:
            if sleeve_input.sleeve_id not in self.portfolio.allocation:
                raise ValueError(f"Unknown sleeve allocation: {sleeve_input.sleeve_id}")

            budget = self.portfolio.allocation[sleeve_input.sleeve_id]
            signals = self._positive_signals(sleeve_input.signals)
            total_signal_weight = sum(signal.target_weight for signal in signals)
            if budget <= 0.0 or total_signal_weight <= 0.0:
                continue

            scale = budget / total_signal_weight
            for signal in signals:
                metadata_by_asset[signal.asset_id] = self._merge_signal_metadata(
                    metadata_by_asset.get(signal.asset_id),
                    signal,
                )
                combined_weights[signal.asset_id] = (
                    combined_weights.get(signal.asset_id, 0.0)
                    + signal.target_weight * scale
                )
                asset_sleeves.setdefault(signal.asset_id, set()).add(sleeve_input.sleeve_id)

        overlap_names = sorted(
            asset_id for asset_id, sleeve_ids in asset_sleeves.items() if len(sleeve_ids) > 1
        )
        combined_weights, dropped_names = self._apply_name_selection(combined_weights)
        combined_weights, capped_names = self._apply_single_name_caps(combined_weights)
        combined_weights, industry_scaled_names = self._apply_industry_caps(
            combined_weights,
            metadata_by_asset,
            construction_input.benchmark_industry_weights,
        )

        ordered_assets = sorted(
            combined_weights,
            key=lambda asset_id: (-combined_weights[asset_id], asset_id),
        )
        signals = [
            self._combined_signal(
                metadata_by_asset[asset_id],
                target_weight=combined_weights[asset_id],
            )
            for asset_id in ordered_assets
            if combined_weights[asset_id] > 0.0
        ]

        return PortfolioConstructionStep(
            trade_date=construction_input.trade_date,
            combined_weights={signal.asset_id: signal.target_weight for signal in signals},
            signals=signals,
            overlap_names=overlap_names,
            dropped_names=dropped_names,
            capped_names=capped_names,
            industry_scaled_names=industry_scaled_names,
            cash_weight=max(1.0 - sum(signal.target_weight for signal in signals), 0.0),
        )

    def _positive_signals(
        self,
        signals: list[PortfolioSecuritySignal],
    ) -> list[PortfolioSecuritySignal]:
        seen_assets: set[str] = set()
        positive: list[PortfolioSecuritySignal] = []
        for signal in signals:
            if signal.asset_id in seen_assets:
                raise ValueError(f"Duplicate sleeve signal for asset: {signal.asset_id}")
            seen_assets.add(signal.asset_id)
            if signal.target_weight > 0.0:
                positive.append(signal)
        return positive

    def _apply_name_selection(
        self,
        combined_weights: dict[str, float],
    ) -> tuple[dict[str, float], list[str]]:
        max_names = int(self.portfolio.constraints.get("max_names", 0))
        if max_names <= 0 or len(combined_weights) <= max_names:
            return combined_weights, []

        ordered = sorted(
            combined_weights.items(),
            key=lambda item: (-item[1], item[0]),
        )
        kept = dict(ordered[:max_names])
        dropped_names = sorted(asset_id for asset_id, _ in ordered[max_names:])
        return kept, dropped_names

    def _apply_single_name_caps(
        self,
        combined_weights: dict[str, float],
    ) -> tuple[dict[str, float], list[str]]:
        max_single_name_weight = float(
            self.portfolio.constraints.get(
                "max_single_name_weight",
                self.mandate.max_single_name_weight,
            )
        )
        if max_single_name_weight <= 0.0:
            return combined_weights, []

        capped_names: list[str] = []
        adjusted = dict(combined_weights)
        for asset_id, weight in combined_weights.items():
            if weight <= max_single_name_weight:
                continue
            adjusted[asset_id] = max_single_name_weight
            capped_names.append(asset_id)
        return adjusted, sorted(capped_names)

    def _apply_industry_caps(
        self,
        combined_weights: dict[str, float],
        metadata_by_asset: dict[str, PortfolioSecuritySignal],
        benchmark_industry_weights: dict[str, float],
    ) -> tuple[dict[str, float], list[str]]:
        if self.construction_model.industry_budget_mode != "benchmark_relative":
            return combined_weights, []

        max_industry_overweight = float(
            self.portfolio.constraints.get(
                "max_industry_overweight",
                self.mandate.risk.get("max_industry_overweight", 0.0),
            )
        )
        if max_industry_overweight <= 0.0 or not combined_weights:
            return combined_weights, []
        if not benchmark_industry_weights:
            raise ValueError(
                "Benchmark industry weights are required for benchmark-relative industry caps."
            )
        missing_industry_assets = sorted(
            asset_id
            for asset_id in combined_weights
            if not metadata_by_asset[asset_id].industry.strip()
        )
        if missing_industry_assets:
            raise ValueError(
                "Industry labels are required for benchmark-relative industry caps: "
                f"{', '.join(missing_industry_assets)}"
            )

        grouped_assets: dict[str, list[str]] = {}
        for asset_id in combined_weights:
            industry = metadata_by_asset[asset_id].industry
            grouped_assets.setdefault(industry, []).append(asset_id)

        adjusted = dict(combined_weights)
        scaled_names: list[str] = []
        for industry, asset_ids in grouped_assets.items():
            total_weight = sum(adjusted[asset_id] for asset_id in asset_ids)
            allowed_weight = benchmark_industry_weights.get(industry, 0.0) + max_industry_overweight
            if total_weight <= allowed_weight:
                continue

            scale = allowed_weight / total_weight if total_weight > 0.0 else 0.0
            for asset_id in asset_ids:
                adjusted[asset_id] = adjusted[asset_id] * scale
                scaled_names.append(asset_id)
        return adjusted, sorted(scaled_names)

    def _merge_signal_metadata(
        self,
        existing: PortfolioSecuritySignal | None,
        incoming: PortfolioSecuritySignal,
    ) -> PortfolioSecuritySignal:
        if existing is None:
            return self._combined_signal(incoming, target_weight=0.0)

        if abs(existing.realized_return - incoming.realized_return) > 1e-12:
            raise ValueError(f"Inconsistent realized return for asset: {incoming.asset_id}")
        if (
            existing.trade_state.can_enter != incoming.trade_state.can_enter
            or existing.trade_state.can_exit != incoming.trade_state.can_exit
        ):
            raise ValueError(f"Inconsistent trade state for asset: {incoming.asset_id}")
        if (
            existing.cost_model_id
            and incoming.cost_model_id
            and existing.cost_model_id != incoming.cost_model_id
        ):
            raise ValueError(f"Inconsistent cost model for asset: {incoming.asset_id}")
        if existing.industry and incoming.industry and existing.industry != incoming.industry:
            raise ValueError(f"Inconsistent industry for asset: {incoming.asset_id}")

        return PortfolioSecuritySignal(
            asset_id=existing.asset_id,
            target_weight=0.0,
            realized_return=existing.realized_return,
            cost_model_id=existing.cost_model_id or incoming.cost_model_id,
            industry=existing.industry or incoming.industry,
            trade_state=TradeConstraintState(
                can_enter=existing.trade_state.can_enter,
                can_exit=existing.trade_state.can_exit,
            ),
        )

    def _combined_signal(
        self,
        signal: PortfolioSecuritySignal,
        target_weight: float,
    ) -> PortfolioSecuritySignal:
        return PortfolioSecuritySignal(
            asset_id=signal.asset_id,
            target_weight=target_weight,
            realized_return=signal.realized_return,
            cost_model_id=signal.cost_model_id,
            industry=signal.industry,
            trade_state=TradeConstraintState(
                can_enter=signal.trade_state.can_enter,
                can_exit=signal.trade_state.can_exit,
            ),
        )
