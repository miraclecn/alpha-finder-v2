from __future__ import annotations

from dataclasses import dataclass
import math

from .portfolio_simulator import PortfolioSimulationResult
from .promotion_gate_evaluator import SleevePromotionSnapshot
from .regime_overlay import RegimeOverlaySummary


@dataclass(slots=True)
class SimulationSummary:
    periods: int
    average_return: float
    volatility: float
    ir: float
    tstat: float
    peak_to_trough_drawdown: float
    breadth: int
    average_turnover: float
    blocked_name_share: float


@dataclass(slots=True)
class MarginalContributionSummary:
    baseline_ir: float
    candidate_ir: float
    marginal_ir_delta: float
    baseline_drawdown: float
    candidate_drawdown: float
    marginal_drawdown_increase: float
    average_return_delta: float
    average_turnover_delta: float


class PortfolioResearchEvaluator:
    def summarize(
        self,
        result: PortfolioSimulationResult,
        periods_per_year: int,
    ) -> SimulationSummary:
        if not result.steps:
            raise ValueError("Simulation result must contain at least one step.")

        returns = [step.net_return for step in result.steps]
        average_return = sum(returns) / len(returns)
        volatility = self._sample_std(returns)
        ir = 0.0
        tstat = 0.0
        if volatility > 0.0:
            ir = (average_return / volatility) * math.sqrt(periods_per_year)
            tstat = (average_return / volatility) * math.sqrt(len(returns))

        breadth = len(
            {
                asset_id
                for step in result.steps
                for asset_id, weight in step.executed_weights.items()
                if weight > 0.0
            }
        )
        average_turnover = sum(step.turnover for step in result.steps) / len(result.steps)
        blocked_name_share = sum(
            (
                len(step.blocked_entries) + len(step.blocked_exits)
            ) / max(len(step.target_weights), 1)
            for step in result.steps
        ) / len(result.steps)

        return SimulationSummary(
            periods=len(returns),
            average_return=average_return,
            volatility=volatility,
            ir=ir,
            tstat=tstat,
            peak_to_trough_drawdown=self._peak_to_trough_drawdown(returns),
            breadth=breadth,
            average_turnover=average_turnover,
            blocked_name_share=blocked_name_share,
        )

    def marginal_contribution(
        self,
        baseline_result: PortfolioSimulationResult,
        candidate_result: PortfolioSimulationResult,
        periods_per_year: int,
    ) -> MarginalContributionSummary:
        baseline = self.summarize(baseline_result, periods_per_year=periods_per_year)
        candidate = self.summarize(candidate_result, periods_per_year=periods_per_year)
        return MarginalContributionSummary(
            baseline_ir=baseline.ir,
            candidate_ir=candidate.ir,
            marginal_ir_delta=candidate.ir - baseline.ir,
            baseline_drawdown=baseline.peak_to_trough_drawdown,
            candidate_drawdown=candidate.peak_to_trough_drawdown,
            marginal_drawdown_increase=(
                candidate.peak_to_trough_drawdown - baseline.peak_to_trough_drawdown
            ),
            average_return_delta=candidate.average_return - baseline.average_return,
            average_turnover_delta=candidate.average_turnover - baseline.average_turnover,
        )

    def to_promotion_snapshot(
        self,
        summary: SimulationSummary,
        turnover_budget: float,
        cost_scenario_pass: dict[str, bool],
        regime_pass: dict[str, bool],
        max_component_correlation: float,
        correlation_to_existing_portfolio: float,
        marginal_ir_delta: float,
        marginal_drawdown_increase: float,
        regime_overlay_summary: RegimeOverlaySummary | None = None,
    ) -> SleevePromotionSnapshot:
        realized_turnover_vs_budget = 0.0
        if turnover_budget > 0.0:
            realized_turnover_vs_budget = summary.average_turnover / turnover_budget

        return SleevePromotionSnapshot(
            oos_ir=summary.ir,
            oos_tstat=summary.tstat,
            breadth=summary.breadth,
            peak_to_trough_drawdown=summary.peak_to_trough_drawdown,
            turnover_budget=turnover_budget,
            cost_scenario_pass=cost_scenario_pass,
            regime_pass=regime_pass,
            max_component_correlation=max_component_correlation,
            correlation_to_existing_portfolio=correlation_to_existing_portfolio,
            realized_turnover_vs_budget=realized_turnover_vs_budget,
            limit_locked_name_share=summary.blocked_name_share,
            marginal_ir_delta=marginal_ir_delta,
            marginal_drawdown_increase=marginal_drawdown_increase,
            regime_overlay_id=(
                regime_overlay_summary.overlay_id
                if regime_overlay_summary is not None
                else ""
            ),
            regime_overlay_blocked_periods=(
                regime_overlay_summary.blocked_periods
                if regime_overlay_summary is not None
                else 0
            ),
            regime_overlay_missing_inputs=(
                list(regime_overlay_summary.missing_inputs)
                if regime_overlay_summary is not None
                else []
            ),
            regime_overlay_invalid_inputs=(
                list(regime_overlay_summary.invalid_inputs)
                if regime_overlay_summary is not None
                else []
            ),
        )

    def _peak_to_trough_drawdown(self, returns: list[float]) -> float:
        equity = 1.0
        peak = 1.0
        max_drawdown = 0.0
        for step_return in returns:
            equity *= 1.0 + step_return
            peak = max(peak, equity)
            drawdown = 1.0 - (equity / peak)
            max_drawdown = max(max_drawdown, drawdown)
        return max_drawdown

    def _sample_std(self, values: list[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
        return math.sqrt(variance)
