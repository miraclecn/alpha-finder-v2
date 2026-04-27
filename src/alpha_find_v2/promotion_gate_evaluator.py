from __future__ import annotations

from dataclasses import dataclass, field

from .models import PromotionGate


@dataclass(slots=True)
class SleevePromotionSnapshot:
    oos_ir: float
    oos_tstat: float
    breadth: int
    peak_to_trough_drawdown: float
    turnover_budget: float = 0.0
    cost_scenario_pass: dict[str, bool] = field(default_factory=dict)
    regime_pass: dict[str, bool] = field(default_factory=dict)
    max_component_correlation: float = 0.0
    correlation_to_existing_portfolio: float = 0.0
    realized_turnover_vs_budget: float = 0.0
    limit_locked_name_share: float = 0.0
    marginal_ir_delta: float = 0.0
    marginal_drawdown_increase: float = 0.0
    regime_overlay_id: str = ""
    regime_overlay_blocked_periods: int = 0
    regime_overlay_missing_inputs: list[str] = field(default_factory=list)
    regime_overlay_invalid_inputs: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PromotionDecision:
    passed: bool
    failed_checks: list[str] = field(default_factory=list)
    passed_checks: list[str] = field(default_factory=list)


class PortfolioPromotionGateEvaluator:
    def __init__(self, gate: PromotionGate) -> None:
        self.gate = gate

    def evaluate(self, snapshot: SleevePromotionSnapshot) -> PromotionDecision:
        failed: list[str] = []
        passed: list[str] = []

        self._threshold_check(
            name="minimum_oos_ir",
            actual=snapshot.oos_ir,
            threshold=self.gate.minimum_oos_ir,
            failures=failed,
            successes=passed,
        )
        self._threshold_check(
            name="minimum_oos_tstat",
            actual=snapshot.oos_tstat,
            threshold=self.gate.minimum_oos_tstat,
            failures=failed,
            successes=passed,
        )
        self._threshold_check(
            name="minimum_breadth",
            actual=float(snapshot.breadth),
            threshold=float(self.gate.minimum_breadth),
            failures=failed,
            successes=passed,
        )

        if snapshot.peak_to_trough_drawdown <= self.gate.max_peak_to_trough_drawdown:
            passed.append("max_peak_to_trough_drawdown")
        else:
            failed.append("max_peak_to_trough_drawdown")

        for scenario in self.gate.cost_scenarios:
            if scenario not in snapshot.cost_scenario_pass:
                failed.append(f"missing_cost_scenario:{scenario}")
            elif snapshot.cost_scenario_pass[scenario]:
                passed.append(f"cost_scenario:{scenario}")
            else:
                failed.append(f"cost_scenario:{scenario}")

        for regime in self.gate.regime_requirements:
            if regime not in snapshot.regime_pass:
                failed.append(f"missing_regime:{regime}")
            elif snapshot.regime_pass[regime]:
                passed.append(f"regime:{regime}")
            else:
                failed.append(f"regime:{regime}")

        correlation_limits = self.gate.correlation_limits
        if snapshot.max_component_correlation <= float(
            correlation_limits.get("max_component_correlation", 1.0)
        ):
            passed.append("max_component_correlation")
        else:
            failed.append("max_component_correlation")

        if snapshot.correlation_to_existing_portfolio <= float(
            correlation_limits.get("max_to_existing_portfolio", 1.0)
        ):
            passed.append("max_to_existing_portfolio")
        else:
            failed.append("max_to_existing_portfolio")

        turnover_limits = self.gate.turnover_limits
        if snapshot.realized_turnover_vs_budget <= float(
            turnover_limits.get("max_realized_vs_budget", float("inf"))
        ):
            passed.append("max_realized_vs_budget")
        else:
            failed.append("max_realized_vs_budget")

        if snapshot.limit_locked_name_share <= float(
            turnover_limits.get("max_names_with_limit_lock_share", 1.0)
        ):
            passed.append("max_names_with_limit_lock_share")
        else:
            failed.append("max_names_with_limit_lock_share")

        contribution = self.gate.portfolio_contribution
        minimum_marginal_ir_delta = contribution.get("minimum_marginal_ir_delta")
        if minimum_marginal_ir_delta is not None:
            self._threshold_check(
                name="minimum_marginal_ir_delta",
                actual=snapshot.marginal_ir_delta,
                threshold=float(minimum_marginal_ir_delta),
                failures=failed,
                successes=passed,
            )

        max_marginal_drawdown_increase = contribution.get("max_marginal_drawdown_increase")
        if max_marginal_drawdown_increase is not None:
            if snapshot.marginal_drawdown_increase <= float(max_marginal_drawdown_increase):
                passed.append("max_marginal_drawdown_increase")
            else:
                failed.append("max_marginal_drawdown_increase")

        if snapshot.regime_overlay_id:
            if snapshot.regime_overlay_blocked_periods > 0:
                failed.append("regime_overlay_blocked")
            elif not snapshot.regime_overlay_missing_inputs and not snapshot.regime_overlay_invalid_inputs:
                passed.append("regime_overlay_complete")

            if snapshot.regime_overlay_missing_inputs:
                failed.append("regime_overlay_missing_inputs")
            if snapshot.regime_overlay_invalid_inputs:
                failed.append("regime_overlay_invalid_inputs")

        return PromotionDecision(
            passed=not failed,
            failed_checks=failed,
            passed_checks=passed,
        )

    def _threshold_check(
        self,
        name: str,
        actual: float,
        threshold: float,
        failures: list[str],
        successes: list[str],
    ) -> None:
        if actual >= threshold:
            successes.append(name)
        else:
            failures.append(name)
