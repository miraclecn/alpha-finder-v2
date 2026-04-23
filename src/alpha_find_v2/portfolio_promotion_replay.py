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
class PortfolioPromotionReplayResult:
    baseline_construction: PortfolioConstructionResult
    candidate_construction: PortfolioConstructionResult
    baseline_simulation: PortfolioSimulationResult
    candidate_simulation: PortfolioSimulationResult
    baseline_summary: SimulationSummary
    candidate_summary: SimulationSummary
    marginal: MarginalContributionSummary
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
