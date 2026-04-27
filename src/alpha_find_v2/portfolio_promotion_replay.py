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
    PortfolioConstructionStep,
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
from .regime_overlay import (
    RegimeOverlay,
    RegimeOverlayDecision,
    RegimeOverlayEvaluator,
    RegimeOverlayObservationStep,
    RegimeOverlaySummary,
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
    regime_overlay: RegimeOverlay | None = None
    regime_overlay_observations: list[RegimeOverlayObservationStep] = field(
        default_factory=list
    )
    periods_per_year: int = 52
    benchmark_industry_weights_by_date: dict[str, dict[str, float]] = field(default_factory=dict)
    cost_scenario_pass: dict[str, bool] = field(default_factory=dict)
    regime_pass: dict[str, bool] = field(default_factory=dict)
    max_component_correlation: float = 0.0
    correlation_to_existing_portfolio: float = 0.0
    turnover_budget: float = 0.0
    walk_forward_splits: list["ReplayWalkForwardSplitDefinition"] = field(
        default_factory=list
    )


@dataclass(slots=True)
class ReplayWalkForwardSplitDefinition:
    split_id: str
    start_trade_date: str
    end_trade_date: str = ""


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
class ReplayWalkForwardSplitSummary:
    split_id: str
    start_trade_date: str
    end_trade_date: str
    baseline_summary: SimulationSummary
    candidate_summary: SimulationSummary
    marginal: MarginalContributionSummary


@dataclass(slots=True)
class ReplayWalkForwardStability:
    split_count: int = 0
    average_candidate_ir: float = 0.0
    worst_candidate_ir: float = 0.0
    best_candidate_ir: float = 0.0
    average_marginal_ir_delta: float = 0.0
    worst_marginal_ir_delta: float = 0.0
    best_marginal_ir_delta: float = 0.0
    worst_candidate_drawdown: float = 0.0
    average_candidate_drawdown: float = 0.0
    average_candidate_breadth: float = 0.0
    weakest_candidate_breadth: int = 0
    positive_marginal_ir_delta_share: float = 0.0


@dataclass(slots=True)
class ReplayWalkForwardEvidence:
    splits: list[ReplayWalkForwardSplitSummary] = field(default_factory=list)
    stability: ReplayWalkForwardStability = field(
        default_factory=ReplayWalkForwardStability
    )


@dataclass(slots=True)
class ReplayRegimeBucketSummary:
    bucket_id: str
    period_count: int = 0
    trade_dates: list[str] = field(default_factory=list)
    start_trade_date: str = ""
    end_trade_date: str = ""
    baseline_summary: SimulationSummary | None = None
    candidate_summary: SimulationSummary | None = None
    marginal: MarginalContributionSummary | None = None
    diagnostics: ReplayDiagnostics | None = None


@dataclass(slots=True)
class ReplayRegimeStability:
    bucket_count: int = 0
    average_candidate_ir: float = 0.0
    worst_candidate_ir: float = 0.0
    best_candidate_ir: float = 0.0
    average_marginal_ir_delta: float = 0.0
    worst_marginal_ir_delta: float = 0.0
    best_marginal_ir_delta: float = 0.0
    worst_candidate_drawdown: float = 0.0
    average_candidate_drawdown: float = 0.0
    average_candidate_breadth: float = 0.0
    weakest_candidate_breadth: int = 0
    positive_marginal_ir_delta_share: float = 0.0


@dataclass(slots=True)
class ReplayWeakSubperiod:
    weakness_id: str
    period_count: int = 0
    trade_dates: list[str] = field(default_factory=list)
    start_trade_date: str = ""
    end_trade_date: str = ""
    baseline_summary: SimulationSummary | None = None
    candidate_summary: SimulationSummary | None = None
    marginal: MarginalContributionSummary | None = None
    diagnostics: ReplayDiagnostics | None = None


@dataclass(slots=True)
class ReplayRegimeEvidence:
    weak_breadth_threshold_name_count: float = 0.0
    buckets: list[ReplayRegimeBucketSummary] = field(default_factory=list)
    stability: ReplayRegimeStability = field(default_factory=ReplayRegimeStability)
    weak_subperiods: list[ReplayWeakSubperiod] = field(default_factory=list)


@dataclass(slots=True)
class ReplayRegimeOverlayEvidence:
    decisions: list[RegimeOverlayDecision] = field(default_factory=list)
    summary: RegimeOverlaySummary = field(
        default_factory=lambda: RegimeOverlaySummary(overlay_id="")
    )


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
    walk_forward: ReplayWalkForwardEvidence | None = None
    regime_breakdown: ReplayRegimeEvidence | None = None
    regime_overlay: ReplayRegimeOverlayEvidence | None = None


@dataclass(slots=True)
class ReplayPeriodObservation:
    trade_date: str
    signal_name_jaccard: float
    signal_weight_overlap: float
    baseline_net_return: float
    candidate_net_return: float
    net_return_delta: float
    turnover_delta: float
    portfolio_overlap_name_count: float
    candidate_only_name_count: float
    candidate_only_weight: float
    candidate_only_return_contribution: float
    shared_return_contribution: float
    baseline_name_count: float
    candidate_name_count: float
    baseline_effective_names: float
    candidate_effective_names: float
    baseline_cash_weight: float
    candidate_cash_weight: float
    candidate_drawdown: float = 0.0


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
        result = self._replay_single_path(replay_input)
        result.regime_breakdown = self._build_regime_breakdown(
            replay_input=replay_input,
            result=result,
        )
        if replay_input.walk_forward_splits:
            result.walk_forward = self._build_walk_forward_evidence(
                replay_input=replay_input,
            )
        return result

    def _replay_single_path(
        self,
        replay_input: PortfolioPromotionReplayInput,
    ) -> PortfolioPromotionReplayResult:
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
        baseline_summary = self.evaluator.summarize(
            baseline_simulation,
            periods_per_year=replay_input.periods_per_year,
        )
        regime_overlay_summary = None
        regime_overlay_evidence = None
        if replay_input.regime_overlay is not None:
            regime_overlay_evidence = self._build_regime_overlay_evidence(
                replay_input=replay_input,
                trade_dates=trade_dates,
            )
            candidate_construction = self._construction_with_overlay_budget(
                construction=candidate_construction,
                overlay=replay_input.regime_overlay,
                decisions=regime_overlay_evidence.decisions,
            )
            regime_overlay_summary = regime_overlay_evidence.summary
        candidate_simulation = PortfolioSimulator(
            mandate=self.mandate,
            portfolio=replay_input.candidate_portfolio,
            default_cost_model=self.default_cost_model,
            cost_models=self.cost_models,
        ).run(candidate_construction.to_rebalance_inputs())
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
            regime_overlay_summary=regime_overlay_summary,
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
            regime_overlay=regime_overlay_evidence,
        )

    def _build_regime_overlay_evidence(
        self,
        *,
        replay_input: PortfolioPromotionReplayInput,
        trade_dates: list[str],
    ) -> ReplayRegimeOverlayEvidence:
        assert replay_input.regime_overlay is not None
        evaluator = RegimeOverlayEvaluator(replay_input.regime_overlay)
        evidence = evaluator.evaluate_history(
            trade_dates=trade_dates,
            observations=replay_input.regime_overlay_observations,
        )
        return ReplayRegimeOverlayEvidence(
            decisions=evidence.decisions,
            summary=evidence.summary,
        )

    def _construction_with_overlay_budget(
        self,
        *,
        construction: PortfolioConstructionResult,
        overlay: RegimeOverlay,
        decisions: list[RegimeOverlayDecision],
    ) -> PortfolioConstructionResult:
        decisions_by_date = {
            decision.trade_date: decision
            for decision in decisions
        }
        adjusted_steps: list[PortfolioConstructionStep] = []
        for step in construction.steps:
            decision = decisions_by_date.get(step.trade_date)
            if decision is None:
                raise ValueError(
                    "Replay overlay evidence must cover every construction trade date: "
                    f"{step.trade_date}"
                )
            overlay_exposure = self._overlay_gross_exposure(
                overlay=overlay,
                decision=decision,
            )
            adjusted_signals = [
                PortfolioSecuritySignal(
                    asset_id=signal.asset_id,
                    target_weight=signal.target_weight * overlay_exposure,
                    realized_return=signal.realized_return,
                    cost_model_id=signal.cost_model_id,
                    industry=signal.industry,
                    trade_state=signal.trade_state,
                )
                for signal in step.signals
            ]
            adjusted_steps.append(
                PortfolioConstructionStep(
                    trade_date=step.trade_date,
                    combined_weights={
                        signal.asset_id: signal.target_weight
                        for signal in adjusted_signals
                    },
                    signals=adjusted_signals,
                    overlap_names=list(step.overlap_names),
                    dropped_names=list(step.dropped_names),
                    capped_names=list(step.capped_names),
                    industry_scaled_names=list(step.industry_scaled_names),
                    cash_weight=max(
                        1.0 - sum(signal.target_weight for signal in adjusted_signals),
                        0.0,
                    ),
                )
            )
        return PortfolioConstructionResult(steps=adjusted_steps)

    def _overlay_gross_exposure(
        self,
        *,
        overlay: RegimeOverlay,
        decision: RegimeOverlayDecision,
    ) -> float:
        if decision.state == "cash_heavier":
            return overlay.cash_heavier_gross_exposure
        if decision.state == "de_risk":
            return overlay.de_risk_gross_exposure
        return overlay.normal_gross_exposure

    def _build_walk_forward_evidence(
        self,
        *,
        replay_input: PortfolioPromotionReplayInput,
    ) -> ReplayWalkForwardEvidence:
        artifacts_by_sleeve = self._artifacts_by_sleeve(replay_input.artifacts)
        full_trade_dates = self._trade_dates_for_portfolio(
            replay_input.candidate_portfolio,
            artifacts_by_sleeve,
        )
        splits: list[ReplayWalkForwardSplitSummary] = []

        for split in replay_input.walk_forward_splits:
            split_trade_dates = self._split_trade_dates(
                split=split,
                trade_dates=full_trade_dates,
            )
            split_result = self._replay_single_path(
                self._slice_replay_input(
                    replay_input=replay_input,
                    trade_dates=split_trade_dates,
                )
            )
            splits.append(
                ReplayWalkForwardSplitSummary(
                    split_id=split.split_id,
                    start_trade_date=split_trade_dates[0],
                    end_trade_date=split_trade_dates[-1],
                    baseline_summary=split_result.baseline_summary,
                    candidate_summary=split_result.candidate_summary,
                    marginal=split_result.marginal,
                )
            )

        return ReplayWalkForwardEvidence(
            splits=splits,
            stability=self._walk_forward_stability(splits),
        )

    def _build_regime_breakdown(
        self,
        *,
        replay_input: PortfolioPromotionReplayInput,
        result: PortfolioPromotionReplayResult,
    ) -> ReplayRegimeEvidence:
        artifacts_by_sleeve = self._artifacts_by_sleeve(replay_input.artifacts)
        trade_dates = [step.trade_date for step in result.candidate_simulation.steps]
        observations = self._build_period_observations(
            replay_input=replay_input,
            artifacts_by_sleeve=artifacts_by_sleeve,
            trade_dates=trade_dates,
            baseline_construction=result.baseline_construction,
            candidate_construction=result.candidate_construction,
            baseline_simulation=result.baseline_simulation,
            candidate_simulation=result.candidate_simulation,
        )
        weak_breadth_threshold = self._average(
            [observation.candidate_name_count for observation in observations]
        )

        buckets = [
            self._regime_bucket_summary(
                replay_input=replay_input,
                bucket_id="trend_up",
                trade_dates=[
                    observation.trade_date
                    for observation in observations
                    if observation.candidate_net_return > 0.0
                ],
            ),
            self._regime_bucket_summary(
                replay_input=replay_input,
                bucket_id="trend_down",
                trade_dates=[
                    observation.trade_date
                    for observation in observations
                    if observation.candidate_net_return <= 0.0
                ],
            ),
            self._regime_bucket_summary(
                replay_input=replay_input,
                bucket_id="drawdown",
                trade_dates=[
                    observation.trade_date
                    for observation in observations
                    if observation.candidate_drawdown > 0.0
                ],
            ),
            self._regime_bucket_summary(
                replay_input=replay_input,
                bucket_id="weak_breadth",
                trade_dates=[
                    observation.trade_date
                    for observation in observations
                    if observation.candidate_name_count < weak_breadth_threshold
                ],
            ),
        ]

        weak_subperiods = [
            *self._weak_subperiod_summaries(
                replay_input=replay_input,
                observations=observations,
                weakness_id="negative_marginal_delta",
                predicate=lambda observation: observation.net_return_delta <= 0.0,
            ),
            *self._weak_subperiod_summaries(
                replay_input=replay_input,
                observations=observations,
                weakness_id="drawdown",
                predicate=lambda observation: observation.candidate_drawdown > 0.0,
            ),
            *self._weak_subperiod_summaries(
                replay_input=replay_input,
                observations=observations,
                weakness_id="weak_breadth",
                predicate=lambda observation: observation.candidate_name_count < weak_breadth_threshold,
            ),
        ]

        return ReplayRegimeEvidence(
            weak_breadth_threshold_name_count=weak_breadth_threshold,
            buckets=buckets,
            stability=self._regime_stability(buckets),
            weak_subperiods=weak_subperiods,
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

    def _split_trade_dates(
        self,
        *,
        split: ReplayWalkForwardSplitDefinition,
        trade_dates: list[str],
    ) -> list[str]:
        if split.start_trade_date not in trade_dates:
            raise ValueError(
                f"Walk-forward split {split.split_id} must start on a replay trade date: "
                f"{split.start_trade_date}"
            )

        end_trade_date = split.end_trade_date or trade_dates[-1]
        if end_trade_date not in trade_dates:
            raise ValueError(
                f"Walk-forward split {split.split_id} must end on a replay trade date: "
                f"{end_trade_date}"
            )
        if split.start_trade_date > end_trade_date:
            raise ValueError(
                f"Walk-forward split {split.split_id} must start on or before its end date."
            )

        selected = [
            trade_date
            for trade_date in trade_dates
            if split.start_trade_date <= trade_date <= end_trade_date
        ]
        if not selected:
            raise ValueError(
                f"Walk-forward split {split.split_id} produced no replay periods."
            )
        return selected

    def _slice_replay_input(
        self,
        *,
        replay_input: PortfolioPromotionReplayInput,
        trade_dates: list[str],
    ) -> PortfolioPromotionReplayInput:
        selected_trade_dates = set(trade_dates)
        return PortfolioPromotionReplayInput(
            baseline_portfolio=replay_input.baseline_portfolio,
            candidate_portfolio=replay_input.candidate_portfolio,
            artifacts=[
                SleeveResearchArtifact(
                    sleeve_id=artifact.sleeve_id,
                    mandate_id=artifact.mandate_id,
                    target_id=artifact.target_id,
                    steps=[
                        step
                        for step in artifact.steps
                        if step.trade_date in selected_trade_dates
                    ],
                )
                for artifact in replay_input.artifacts
            ],
            periods_per_year=replay_input.periods_per_year,
            benchmark_industry_weights_by_date={
                trade_date: weights
                for trade_date, weights in replay_input.benchmark_industry_weights_by_date.items()
                if trade_date in selected_trade_dates
            },
            regime_overlay=replay_input.regime_overlay,
            regime_overlay_observations=[
                step
                for step in replay_input.regime_overlay_observations
                if step.trade_date in selected_trade_dates
            ],
            cost_scenario_pass=replay_input.cost_scenario_pass,
            regime_pass=replay_input.regime_pass,
            max_component_correlation=replay_input.max_component_correlation,
            correlation_to_existing_portfolio=replay_input.correlation_to_existing_portfolio,
            turnover_budget=replay_input.turnover_budget,
        )

    def _walk_forward_stability(
        self,
        splits: list[ReplayWalkForwardSplitSummary],
    ) -> ReplayWalkForwardStability:
        if not splits:
            return ReplayWalkForwardStability()

        candidate_irs = [split.candidate_summary.ir for split in splits]
        marginal_ir_deltas = [split.marginal.marginal_ir_delta for split in splits]
        candidate_drawdowns = [
            split.candidate_summary.peak_to_trough_drawdown
            for split in splits
        ]
        candidate_breadths = [split.candidate_summary.breadth for split in splits]
        positive_marginal_ir_splits = sum(
            1 for value in marginal_ir_deltas if value > 0.0
        )

        return ReplayWalkForwardStability(
            split_count=len(splits),
            average_candidate_ir=self._average(candidate_irs),
            worst_candidate_ir=min(candidate_irs),
            best_candidate_ir=max(candidate_irs),
            average_marginal_ir_delta=self._average(marginal_ir_deltas),
            worst_marginal_ir_delta=min(marginal_ir_deltas),
            best_marginal_ir_delta=max(marginal_ir_deltas),
            worst_candidate_drawdown=max(candidate_drawdowns),
            average_candidate_drawdown=self._average(candidate_drawdowns),
            average_candidate_breadth=self._average(
                [float(value) for value in candidate_breadths]
            ),
            weakest_candidate_breadth=min(candidate_breadths),
            positive_marginal_ir_delta_share=(
                positive_marginal_ir_splits / len(splits)
            ),
        )

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
        observations = self._build_period_observations(
            replay_input=replay_input,
            artifacts_by_sleeve=artifacts_by_sleeve,
            trade_dates=trade_dates,
            baseline_construction=baseline_construction,
            candidate_construction=candidate_construction,
            baseline_simulation=baseline_simulation,
            candidate_simulation=candidate_simulation,
        )
        return self._aggregate_diagnostics(observations)

    def _build_period_observations(
        self,
        *,
        replay_input: PortfolioPromotionReplayInput,
        artifacts_by_sleeve: dict[str, SleeveResearchArtifact],
        trade_dates: list[str],
        baseline_construction: PortfolioConstructionResult,
        candidate_construction: PortfolioConstructionResult,
        baseline_simulation: PortfolioSimulationResult,
        candidate_simulation: PortfolioSimulationResult,
    ) -> list[ReplayPeriodObservation]:
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

        candidate_steps_by_date = {
            step.trade_date: step for step in candidate_construction.steps
        }
        baseline_simulation_by_date = {step.trade_date: step for step in baseline_simulation.steps}
        candidate_simulation_by_date = {step.trade_date: step for step in candidate_simulation.steps}
        observations: list[ReplayPeriodObservation] = []
        candidate_equity = 1.0
        candidate_peak = 1.0

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

            candidate_equity *= 1.0 + candidate_result_step.net_return
            candidate_peak = max(candidate_peak, candidate_equity)
            candidate_drawdown = 1.0 - (candidate_equity / candidate_peak)

            observations.append(
                ReplayPeriodObservation(
                    trade_date=trade_date,
                    signal_name_jaccard=self._name_jaccard(
                        existing_weights,
                        candidate_only_weights,
                    ),
                    signal_weight_overlap=self._weight_overlap(
                        existing_weights,
                        candidate_only_weights,
                    ),
                    baseline_net_return=baseline_result_step.net_return,
                    candidate_net_return=candidate_result_step.net_return,
                    net_return_delta=(
                        candidate_result_step.net_return - baseline_result_step.net_return
                    ),
                    turnover_delta=(
                        candidate_result_step.turnover - baseline_result_step.turnover
                    ),
                    portfolio_overlap_name_count=float(len(shared_names)),
                    candidate_only_name_count=float(len(candidate_only_names)),
                    candidate_only_weight=sum(
                        candidate_result_step.executed_weights[asset_id]
                        for asset_id in candidate_only_names
                    ),
                    candidate_only_return_contribution=sum(
                        candidate_result_step.executed_weights[asset_id]
                        * candidate_returns[asset_id]
                        for asset_id in candidate_only_names
                    ),
                    shared_return_contribution=sum(
                        candidate_result_step.executed_weights[asset_id]
                        * candidate_returns[asset_id]
                        for asset_id in shared_names
                    ),
                    baseline_name_count=float(len(baseline_result_step.executed_weights)),
                    candidate_name_count=float(len(candidate_result_step.executed_weights)),
                    baseline_effective_names=self._effective_name_count(
                        baseline_result_step.executed_weights
                    ),
                    candidate_effective_names=self._effective_name_count(
                        candidate_result_step.executed_weights
                    ),
                    baseline_cash_weight=max(
                        1.0 - sum(baseline_result_step.executed_weights.values()),
                        0.0,
                    ),
                    candidate_cash_weight=max(
                        1.0 - sum(candidate_result_step.executed_weights.values()),
                        0.0,
                    ),
                    candidate_drawdown=candidate_drawdown,
                )
            )

        return observations

    def _aggregate_diagnostics(
        self,
        observations: list[ReplayPeriodObservation],
    ) -> ReplayDiagnostics:
        period_deltas = [
            ReplayPeriodDelta(
                trade_date=observation.trade_date,
                baseline_net_return=observation.baseline_net_return,
                candidate_net_return=observation.candidate_net_return,
                net_return_delta=observation.net_return_delta,
                turnover_delta=observation.turnover_delta,
            )
            for observation in observations
        ]

        return ReplayDiagnostics(
            incrementality=ReplayIncrementalityDiagnostics(
                average_signal_name_jaccard=self._average(
                    [observation.signal_name_jaccard for observation in observations]
                ),
                average_signal_weight_overlap=self._average(
                    [observation.signal_weight_overlap for observation in observations]
                ),
                average_portfolio_overlap_name_count=self._average(
                    [
                        observation.portfolio_overlap_name_count
                        for observation in observations
                    ]
                ),
                average_candidate_only_name_count=self._average(
                    [observation.candidate_only_name_count for observation in observations]
                ),
                average_candidate_only_weight=self._average(
                    [observation.candidate_only_weight for observation in observations]
                ),
                average_candidate_only_return_contribution=self._average(
                    [
                        observation.candidate_only_return_contribution
                        for observation in observations
                    ]
                ),
                average_shared_return_contribution=self._average(
                    [
                        observation.shared_return_contribution
                        for observation in observations
                    ]
                ),
            ),
            concentration=ReplayConcentrationDiagnostics(
                baseline_average_name_count=self._average(
                    [observation.baseline_name_count for observation in observations]
                ),
                candidate_average_name_count=self._average(
                    [observation.candidate_name_count for observation in observations]
                ),
                baseline_average_effective_names=self._average(
                    [observation.baseline_effective_names for observation in observations]
                ),
                candidate_average_effective_names=self._average(
                    [observation.candidate_effective_names for observation in observations]
                ),
                baseline_average_cash_weight=self._average(
                    [observation.baseline_cash_weight for observation in observations]
                ),
                candidate_average_cash_weight=self._average(
                    [observation.candidate_cash_weight for observation in observations]
                ),
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

    def _regime_bucket_summary(
        self,
        *,
        replay_input: PortfolioPromotionReplayInput,
        bucket_id: str,
        trade_dates: list[str],
    ) -> ReplayRegimeBucketSummary:
        if not trade_dates:
            return ReplayRegimeBucketSummary(bucket_id=bucket_id)

        bucket_result = self._replay_single_path(
            self._slice_replay_input(
                replay_input=replay_input,
                trade_dates=trade_dates,
            )
        )
        return ReplayRegimeBucketSummary(
            bucket_id=bucket_id,
            period_count=len(trade_dates),
            trade_dates=trade_dates,
            start_trade_date=trade_dates[0],
            end_trade_date=trade_dates[-1],
            baseline_summary=bucket_result.baseline_summary,
            candidate_summary=bucket_result.candidate_summary,
            marginal=bucket_result.marginal,
            diagnostics=bucket_result.diagnostics,
        )

    def _weak_subperiod_summaries(
        self,
        *,
        replay_input: PortfolioPromotionReplayInput,
        observations: list[ReplayPeriodObservation],
        weakness_id: str,
        predicate,
    ) -> list[ReplayWeakSubperiod]:
        summaries: list[ReplayWeakSubperiod] = []
        current_trade_dates: list[str] = []

        for observation in observations:
            if predicate(observation):
                current_trade_dates.append(observation.trade_date)
                continue

            if current_trade_dates:
                summaries.append(
                    self._weak_subperiod_summary(
                        replay_input=replay_input,
                        weakness_id=weakness_id,
                        trade_dates=current_trade_dates,
                    )
                )
                current_trade_dates = []

        if current_trade_dates:
            summaries.append(
                self._weak_subperiod_summary(
                    replay_input=replay_input,
                    weakness_id=weakness_id,
                    trade_dates=current_trade_dates,
                )
            )

        return summaries

    def _weak_subperiod_summary(
        self,
        *,
        replay_input: PortfolioPromotionReplayInput,
        weakness_id: str,
        trade_dates: list[str],
    ) -> ReplayWeakSubperiod:
        segment_result = self._replay_single_path(
            self._slice_replay_input(
                replay_input=replay_input,
                trade_dates=trade_dates,
            )
        )
        return ReplayWeakSubperiod(
            weakness_id=weakness_id,
            period_count=len(trade_dates),
            trade_dates=trade_dates,
            start_trade_date=trade_dates[0],
            end_trade_date=trade_dates[-1],
            baseline_summary=segment_result.baseline_summary,
            candidate_summary=segment_result.candidate_summary,
            marginal=segment_result.marginal,
            diagnostics=segment_result.diagnostics,
        )

    def _regime_stability(
        self,
        buckets: list[ReplayRegimeBucketSummary],
    ) -> ReplayRegimeStability:
        populated_buckets = [
            bucket
            for bucket in buckets
            if bucket.candidate_summary is not None and bucket.marginal is not None
        ]
        if not populated_buckets:
            return ReplayRegimeStability()

        candidate_irs = [
            bucket.candidate_summary.ir
            for bucket in populated_buckets
            if bucket.candidate_summary is not None
        ]
        marginal_ir_deltas = [
            bucket.marginal.marginal_ir_delta
            for bucket in populated_buckets
            if bucket.marginal is not None
        ]
        candidate_drawdowns = [
            bucket.candidate_summary.peak_to_trough_drawdown
            for bucket in populated_buckets
            if bucket.candidate_summary is not None
        ]
        candidate_breadths = [
            bucket.candidate_summary.breadth
            for bucket in populated_buckets
            if bucket.candidate_summary is not None
        ]
        positive_marginal_ir_buckets = sum(
            1 for value in marginal_ir_deltas if value > 0.0
        )

        return ReplayRegimeStability(
            bucket_count=len(populated_buckets),
            average_candidate_ir=self._average(candidate_irs),
            worst_candidate_ir=min(candidate_irs),
            best_candidate_ir=max(candidate_irs),
            average_marginal_ir_delta=self._average(marginal_ir_deltas),
            worst_marginal_ir_delta=min(marginal_ir_deltas),
            best_marginal_ir_delta=max(marginal_ir_deltas),
            worst_candidate_drawdown=max(candidate_drawdowns),
            average_candidate_drawdown=self._average(candidate_drawdowns),
            average_candidate_breadth=self._average(
                [float(value) for value in candidate_breadths]
            ),
            weakest_candidate_breadth=min(candidate_breadths),
            positive_marginal_ir_delta_share=(
                positive_marginal_ir_buckets / len(populated_buckets)
            ),
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
