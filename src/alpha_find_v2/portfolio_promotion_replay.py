from __future__ import annotations

from dataclasses import dataclass, field

from .models import (
    CostModel,
    Mandate,
    PortfolioConstructionModel,
    PortfolioRecipe,
    PromotionGate,
)
from .portfolio_constructor import (
    PortfolioConstructionInput,
    PortfolioConstructionResult,
    PortfolioConstructor,
    SleeveConstructionInput,
)
from .portfolio_simulator import (
    PortfolioSecuritySignal,
    PortfolioSimulationResult,
    PortfolioSimulator,
    TradeConstraintState,
)
from .promotion_gate_evaluator import (
    PortfolioPromotionGateEvaluator,
    PromotionDecision,
    SleevePromotionSnapshot,
)
from .research_evaluator import (
    MarginalContributionSummary,
    PortfolioResearchEvaluator,
    SimulationSummary,
)


@dataclass(slots=True)
class SleeveSignalRecord:
    asset_id: str
    rank: int
    score: float
    target_weight: float
    realized_return: float
    cost_model_id: str = ""
    industry: str = ""
    trade_state: TradeConstraintState = field(default_factory=TradeConstraintState)

    def to_portfolio_signal(self) -> PortfolioSecuritySignal:
        return PortfolioSecuritySignal(
            asset_id=self.asset_id,
            target_weight=self.target_weight,
            realized_return=self.realized_return,
            cost_model_id=self.cost_model_id,
            industry=self.industry,
            trade_state=TradeConstraintState(
                can_enter=self.trade_state.can_enter,
                can_exit=self.trade_state.can_exit,
            ),
        )


@dataclass(slots=True)
class SleeveResearchStep:
    trade_date: str
    records: list[SleeveSignalRecord] = field(default_factory=list)

    def to_construction_input(self, sleeve_id: str) -> SleeveConstructionInput:
        ordered = sorted(self.records, key=lambda record: (record.rank, record.asset_id))
        return SleeveConstructionInput(
            sleeve_id=sleeve_id,
            signals=[record.to_portfolio_signal() for record in ordered],
        )


@dataclass(slots=True)
class SleeveResearchArtifact:
    sleeve_id: str
    mandate_id: str
    target_id: str
    steps: list[SleeveResearchStep] = field(default_factory=list)

    def trade_dates(self) -> list[str]:
        return [step.trade_date for step in self.steps]

    def step_for_date(self, trade_date: str) -> SleeveResearchStep:
        for step in self.steps:
            if step.trade_date == trade_date:
                return step
        raise ValueError(f"Sleeve artifact {self.sleeve_id} must cover trade date {trade_date}")


@dataclass(slots=True)
class PortfolioPromotionReplayInput:
    baseline_portfolio: PortfolioRecipe
    candidate_portfolio: PortfolioRecipe
    artifacts: list[SleeveResearchArtifact] = field(default_factory=list)
    periods_per_year: int = 52
    benchmark_industry_weights_by_date: dict[str, dict[str, float]] = field(default_factory=dict)
    cost_scenario_pass: dict[str, bool] = field(default_factory=dict)
    regime_pass: dict[str, bool] = field(default_factory=dict)
    max_component_correlation: float = 0.0
    correlation_to_existing_portfolio: float = 0.0
    turnover_budget: float = 0.0


@dataclass(slots=True)
class ReplayIncrementalityDiagnostics:
    average_signal_name_jaccard: float = 0.0
    average_signal_weight_overlap: float = 0.0
    average_portfolio_overlap_name_count: float = 0.0
    average_candidate_only_name_count: float = 0.0
    average_candidate_only_weight: float = 0.0
    average_candidate_only_return_contribution: float = 0.0
    average_shared_return_contribution: float = 0.0


@dataclass(slots=True)
class ReplayConcentrationDiagnostics:
    baseline_average_name_count: float = 0.0
    candidate_average_name_count: float = 0.0
    baseline_average_effective_names: float = 0.0
    candidate_average_effective_names: float = 0.0
    baseline_average_cash_weight: float = 0.0
    candidate_average_cash_weight: float = 0.0


@dataclass(slots=True)
class ReplayPeriodDelta:
    trade_date: str
    baseline_net_return: float
    candidate_net_return: float
    net_return_delta: float
    turnover_delta: float


@dataclass(slots=True)
class ReplayDiagnostics:
    incrementality: ReplayIncrementalityDiagnostics = field(
        default_factory=ReplayIncrementalityDiagnostics
    )
    concentration: ReplayConcentrationDiagnostics = field(
        default_factory=ReplayConcentrationDiagnostics
    )
    best_periods: list[ReplayPeriodDelta] = field(default_factory=list)
    worst_periods: list[ReplayPeriodDelta] = field(default_factory=list)


@dataclass(slots=True)
class PortfolioPromotionReplayResult:
    baseline_construction: PortfolioConstructionResult
    candidate_construction: PortfolioConstructionResult
    baseline_simulation: PortfolioSimulationResult
    candidate_simulation: PortfolioSimulationResult
    baseline_summary: SimulationSummary
    candidate_summary: SimulationSummary
    marginal: MarginalContributionSummary
    diagnostics: ReplayDiagnostics
    snapshot: SleevePromotionSnapshot
    decision: PromotionDecision | None = None


class PortfolioPromotionReplay:
    def __init__(
        self,
        mandate: Mandate,
        construction_model: PortfolioConstructionModel,
        default_cost_model: CostModel,
        gate: PromotionGate | None = None,
        cost_models: dict[str, CostModel] | None = None,
    ) -> None:
        self.mandate = mandate
        self.construction_model = construction_model
        self.default_cost_model = default_cost_model
        self.cost_models = cost_models
        self.evaluator = PortfolioResearchEvaluator()
        self.gate_evaluator = PortfolioPromotionGateEvaluator(gate) if gate else None

    def replay(self, replay_input: PortfolioPromotionReplayInput) -> PortfolioPromotionReplayResult:
        self._validate_portfolio(replay_input.baseline_portfolio)
        self._validate_portfolio(replay_input.candidate_portfolio)

        artifacts_by_sleeve = self._artifacts_by_sleeve(replay_input.artifacts)
        trade_dates = self._trade_dates_for_portfolio(
            replay_input.candidate_portfolio,
            artifacts_by_sleeve,
        )
        baseline_inputs = self._construction_inputs(
            portfolio=replay_input.baseline_portfolio,
            trade_dates=trade_dates,
            artifacts_by_sleeve=artifacts_by_sleeve,
            benchmark_industry_weights_by_date=replay_input.benchmark_industry_weights_by_date,
        )
        candidate_inputs = self._construction_inputs(
            portfolio=replay_input.candidate_portfolio,
            trade_dates=trade_dates,
            artifacts_by_sleeve=artifacts_by_sleeve,
            benchmark_industry_weights_by_date=replay_input.benchmark_industry_weights_by_date,
        )

        baseline_construction = PortfolioConstructor(
            mandate=self.mandate,
            portfolio=replay_input.baseline_portfolio,
            construction_model=self.construction_model,
        ).build(baseline_inputs)
        candidate_construction = PortfolioConstructor(
            mandate=self.mandate,
            portfolio=replay_input.candidate_portfolio,
            construction_model=self.construction_model,
        ).build(candidate_inputs)

        baseline_simulation = PortfolioSimulator(
            mandate=self.mandate,
            portfolio=replay_input.baseline_portfolio,
            default_cost_model=self.default_cost_model,
            cost_models=self.cost_models,
        ).run(baseline_construction.to_rebalance_inputs())
        candidate_simulation = PortfolioSimulator(
            mandate=self.mandate,
            portfolio=replay_input.candidate_portfolio,
            default_cost_model=self.default_cost_model,
            cost_models=self.cost_models,
        ).run(candidate_construction.to_rebalance_inputs())

        baseline_summary = self.evaluator.summarize(
            baseline_simulation,
            periods_per_year=replay_input.periods_per_year,
        )
        candidate_summary = self.evaluator.summarize(
            candidate_simulation,
            periods_per_year=replay_input.periods_per_year,
        )
        marginal = self.evaluator.marginal_contribution(
            baseline_result=baseline_simulation,
            candidate_result=candidate_simulation,
            periods_per_year=replay_input.periods_per_year,
        )
        diagnostics = self._build_diagnostics(
            replay_input=replay_input,
            artifacts_by_sleeve=artifacts_by_sleeve,
            trade_dates=trade_dates,
            baseline_construction=baseline_construction,
            candidate_construction=candidate_construction,
            baseline_simulation=baseline_simulation,
            candidate_simulation=candidate_simulation,
        )
        snapshot = self.evaluator.to_promotion_snapshot(
            summary=candidate_summary,
            turnover_budget=self._turnover_budget(replay_input),
            cost_scenario_pass=replay_input.cost_scenario_pass,
            regime_pass=replay_input.regime_pass,
            max_component_correlation=replay_input.max_component_correlation,
            correlation_to_existing_portfolio=replay_input.correlation_to_existing_portfolio,
            marginal_ir_delta=marginal.marginal_ir_delta,
            marginal_drawdown_increase=marginal.marginal_drawdown_increase,
        )
        decision = None
        if self.gate_evaluator is not None:
            decision = self.gate_evaluator.evaluate(snapshot)

        return PortfolioPromotionReplayResult(
            baseline_construction=baseline_construction,
            candidate_construction=candidate_construction,
            baseline_simulation=baseline_simulation,
            candidate_simulation=candidate_simulation,
            baseline_summary=baseline_summary,
            candidate_summary=candidate_summary,
            marginal=marginal,
            diagnostics=diagnostics,
            snapshot=snapshot,
            decision=decision,
        )

    def _artifacts_by_sleeve(
        self,
        artifacts: list[SleeveResearchArtifact],
    ) -> dict[str, SleeveResearchArtifact]:
        artifacts_by_sleeve: dict[str, SleeveResearchArtifact] = {}
        for artifact in artifacts:
            if artifact.sleeve_id in artifacts_by_sleeve:
                raise ValueError(f"Duplicate sleeve artifact: {artifact.sleeve_id}")
            if artifact.mandate_id != self.mandate.id:
                raise ValueError(
                    f"Sleeve artifact {artifact.sleeve_id} does not match mandate {self.mandate.id}"
                )
            self._validate_artifact(artifact)
            artifacts_by_sleeve[artifact.sleeve_id] = artifact
        return artifacts_by_sleeve

    def _validate_artifact(self, artifact: SleeveResearchArtifact) -> None:
        seen_dates: set[str] = set()
        for step in artifact.steps:
            if step.trade_date in seen_dates:
                raise ValueError(
                    f"Duplicate trade date in sleeve artifact {artifact.sleeve_id}: {step.trade_date}"
                )
            seen_dates.add(step.trade_date)
            self._validate_step(artifact.sleeve_id, step)

    def _validate_step(self, sleeve_id: str, step: SleeveResearchStep) -> None:
        seen_assets: set[str] = set()
        seen_ranks: set[int] = set()
        for record in step.records:
            if record.asset_id in seen_assets:
                raise ValueError(
                    f"Duplicate asset in sleeve artifact {sleeve_id} on {step.trade_date}: {record.asset_id}"
                )
            if record.rank in seen_ranks:
                raise ValueError(
                    f"Duplicate rank in sleeve artifact {sleeve_id} on {step.trade_date}: {record.rank}"
                )
            seen_assets.add(record.asset_id)
            seen_ranks.add(record.rank)

    def _validate_portfolio(self, portfolio: PortfolioRecipe) -> None:
        if portfolio.mandate_id != self.mandate.id:
            raise ValueError(f"Portfolio {portfolio.id} does not match mandate {self.mandate.id}")

    def _trade_dates_for_portfolio(
        self,
        portfolio: PortfolioRecipe,
        artifacts_by_sleeve: dict[str, SleeveResearchArtifact],
    ) -> list[str]:
        if not portfolio.sleeves:
            raise ValueError(f"Portfolio {portfolio.id} must contain at least one sleeve.")

        for sleeve_id in portfolio.sleeves:
            if sleeve_id not in artifacts_by_sleeve:
                raise ValueError(f"Missing sleeve artifact: {sleeve_id}")
        trade_dates = sorted(
            {
                trade_date
                for sleeve_id in portfolio.sleeves
                for trade_date in artifacts_by_sleeve[sleeve_id].trade_dates()
            }
        )
        for sleeve_id in portfolio.sleeves:
            artifact = artifacts_by_sleeve[sleeve_id]
            for trade_date in trade_dates:
                artifact.step_for_date(trade_date)
        return trade_dates

    def _construction_inputs(
        self,
        portfolio: PortfolioRecipe,
        trade_dates: list[str],
        artifacts_by_sleeve: dict[str, SleeveResearchArtifact],
        benchmark_industry_weights_by_date: dict[str, dict[str, float]],
    ) -> list[PortfolioConstructionInput]:
        inputs: list[PortfolioConstructionInput] = []
        for trade_date in trade_dates:
            sleeves = []
            for sleeve_id in portfolio.sleeves:
                artifact = artifacts_by_sleeve.get(sleeve_id)
                if artifact is None:
                    raise ValueError(f"Missing sleeve artifact: {sleeve_id}")
                sleeves.append(artifact.step_for_date(trade_date).to_construction_input(sleeve_id))
            inputs.append(
                PortfolioConstructionInput(
                    trade_date=trade_date,
                    sleeves=sleeves,
                    benchmark_industry_weights=benchmark_industry_weights_by_date.get(trade_date, {}),
                )
            )
        return inputs

    def _turnover_budget(self, replay_input: PortfolioPromotionReplayInput) -> float:
        if replay_input.turnover_budget > 0.0:
            return replay_input.turnover_budget
        return self.mandate.max_turnover_per_rebalance

    def _build_diagnostics(
        self,
        *,
        replay_input: PortfolioPromotionReplayInput,
        artifacts_by_sleeve: dict[str, SleeveResearchArtifact],
        trade_dates: list[str],
        baseline_construction: PortfolioConstructionResult,
        candidate_construction: PortfolioConstructionResult,
        baseline_simulation: PortfolioSimulationResult,
        candidate_simulation: PortfolioSimulationResult,
    ) -> ReplayDiagnostics:
        existing_sleeves = [
            sleeve_id
            for sleeve_id in replay_input.candidate_portfolio.sleeves
            if sleeve_id in replay_input.baseline_portfolio.sleeves
        ]
        candidate_only_sleeves = [
            sleeve_id
            for sleeve_id in replay_input.candidate_portfolio.sleeves
            if sleeve_id not in replay_input.baseline_portfolio.sleeves
        ]

        signal_name_jaccards: list[float] = []
        signal_weight_overlaps: list[float] = []
        for trade_date in trade_dates:
            existing_weights = self._sleeve_group_weights(
                sleeve_ids=existing_sleeves,
                portfolio=replay_input.candidate_portfolio,
                trade_date=trade_date,
                artifacts_by_sleeve=artifacts_by_sleeve,
            )
            candidate_only_weights = self._sleeve_group_weights(
                sleeve_ids=candidate_only_sleeves,
                portfolio=replay_input.candidate_portfolio,
                trade_date=trade_date,
                artifacts_by_sleeve=artifacts_by_sleeve,
            )
            signal_name_jaccards.append(
                self._name_jaccard(existing_weights, candidate_only_weights)
            )
            signal_weight_overlaps.append(
                self._weight_overlap(existing_weights, candidate_only_weights)
            )

        baseline_steps_by_date = {
            step.trade_date: step for step in baseline_construction.steps
        }
        candidate_steps_by_date = {
            step.trade_date: step for step in candidate_construction.steps
        }
        baseline_simulation_by_date = {
            step.trade_date: step for step in baseline_simulation.steps
        }
        candidate_simulation_by_date = {
            step.trade_date: step for step in candidate_simulation.steps
        }

        portfolio_overlap_name_counts: list[float] = []
        candidate_only_name_counts: list[float] = []
        candidate_only_weights: list[float] = []
        candidate_only_return_contributions: list[float] = []
        shared_return_contributions: list[float] = []
        baseline_name_counts: list[float] = []
        candidate_name_counts: list[float] = []
        baseline_effective_names: list[float] = []
        candidate_effective_names: list[float] = []
        baseline_cash_weights: list[float] = []
        candidate_cash_weights: list[float] = []
        period_deltas: list[ReplayPeriodDelta] = []

        for trade_date in trade_dates:
            candidate_step = candidate_steps_by_date[trade_date]
            baseline_result_step = baseline_simulation_by_date[trade_date]
            candidate_result_step = candidate_simulation_by_date[trade_date]

            baseline_names = set(baseline_result_step.executed_weights)
            candidate_names = set(candidate_result_step.executed_weights)
            shared_names = baseline_names & candidate_names
            candidate_only_names = candidate_names - baseline_names
            candidate_returns = {
                signal.asset_id: signal.realized_return for signal in candidate_step.signals
            }

            portfolio_overlap_name_counts.append(float(len(shared_names)))
            candidate_only_name_counts.append(float(len(candidate_only_names)))
            candidate_only_weights.append(
                sum(
                    candidate_result_step.executed_weights[asset_id]
                    for asset_id in candidate_only_names
                )
            )
            candidate_only_return_contributions.append(
                sum(
                    candidate_result_step.executed_weights[asset_id]
                    * candidate_returns[asset_id]
                    for asset_id in candidate_only_names
                )
            )
            shared_return_contributions.append(
                sum(
                    candidate_result_step.executed_weights[asset_id]
                    * candidate_returns[asset_id]
                    for asset_id in shared_names
                )
            )
            baseline_name_counts.append(float(len(baseline_result_step.executed_weights)))
            candidate_name_counts.append(float(len(candidate_result_step.executed_weights)))
            baseline_effective_names.append(
                self._effective_name_count(baseline_result_step.executed_weights)
            )
            candidate_effective_names.append(
                self._effective_name_count(candidate_result_step.executed_weights)
            )
            baseline_cash_weights.append(
                max(1.0 - sum(baseline_result_step.executed_weights.values()), 0.0)
            )
            candidate_cash_weights.append(
                max(1.0 - sum(candidate_result_step.executed_weights.values()), 0.0)
            )
            period_deltas.append(
                ReplayPeriodDelta(
                    trade_date=trade_date,
                    baseline_net_return=baseline_result_step.net_return,
                    candidate_net_return=candidate_result_step.net_return,
                    net_return_delta=(
                        candidate_result_step.net_return - baseline_result_step.net_return
                    ),
                    turnover_delta=(
                        candidate_result_step.turnover - baseline_result_step.turnover
                    ),
                )
            )

        return ReplayDiagnostics(
            incrementality=ReplayIncrementalityDiagnostics(
                average_signal_name_jaccard=self._average(signal_name_jaccards),
                average_signal_weight_overlap=self._average(signal_weight_overlaps),
                average_portfolio_overlap_name_count=self._average(
                    portfolio_overlap_name_counts
                ),
                average_candidate_only_name_count=self._average(candidate_only_name_counts),
                average_candidate_only_weight=self._average(candidate_only_weights),
                average_candidate_only_return_contribution=self._average(
                    candidate_only_return_contributions
                ),
                average_shared_return_contribution=self._average(
                    shared_return_contributions
                ),
            ),
            concentration=ReplayConcentrationDiagnostics(
                baseline_average_name_count=self._average(baseline_name_counts),
                candidate_average_name_count=self._average(candidate_name_counts),
                baseline_average_effective_names=self._average(baseline_effective_names),
                candidate_average_effective_names=self._average(candidate_effective_names),
                baseline_average_cash_weight=self._average(baseline_cash_weights),
                candidate_average_cash_weight=self._average(candidate_cash_weights),
            ),
            best_periods=sorted(
                period_deltas,
                key=lambda period: (-period.net_return_delta, period.trade_date),
            )[:3],
            worst_periods=sorted(
                period_deltas,
                key=lambda period: (period.net_return_delta, period.trade_date),
            )[:3],
        )

    def _sleeve_group_weights(
        self,
        *,
        sleeve_ids: list[str],
        portfolio: PortfolioRecipe,
        trade_date: str,
        artifacts_by_sleeve: dict[str, SleeveResearchArtifact],
    ) -> dict[str, float]:
        group_weights: dict[str, float] = {}
        for sleeve_id in sleeve_ids:
            budget = portfolio.allocation.get(sleeve_id, 0.0)
            if budget <= 0.0:
                continue
            step = artifacts_by_sleeve[sleeve_id].step_for_date(trade_date)
            total_target_weight = sum(
                record.target_weight for record in step.records if record.target_weight > 0.0
            )
            if total_target_weight <= 0.0:
                continue
            scale = budget / total_target_weight
            for record in step.records:
                if record.target_weight <= 0.0:
                    continue
                group_weights[record.asset_id] = (
                    group_weights.get(record.asset_id, 0.0)
                    + record.target_weight * scale
                )
        return group_weights

    def _name_jaccard(
        self,
        left_weights: dict[str, float],
        right_weights: dict[str, float],
    ) -> float:
        if not left_weights or not right_weights:
            return 0.0
        left_names = set(left_weights)
        right_names = set(right_weights)
        union = left_names | right_names
        if not union:
            return 0.0
        return len(left_names & right_names) / len(union)

    def _weight_overlap(
        self,
        left_weights: dict[str, float],
        right_weights: dict[str, float],
    ) -> float:
        if not left_weights or not right_weights:
            return 0.0
        return sum(
            min(left_weights[asset_id], right_weights[asset_id])
            for asset_id in set(left_weights) & set(right_weights)
        )

    def _effective_name_count(self, weights: dict[str, float]) -> float:
        invested_weight = sum(weights.values())
        if invested_weight <= 0.0:
            return 0.0
        normalized_weights = [weight / invested_weight for weight in weights.values() if weight > 0.0]
        denominator = sum(weight * weight for weight in normalized_weights)
        if denominator <= 0.0:
            return 0.0
        return 1.0 / denominator

    def _average(self, values: list[float]) -> float:
        if not values:
            return 0.0
        return sum(values) / len(values)
