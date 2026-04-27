from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import json
from pathlib import Path
import tomllib
from typing import Any

from .config_loader import (
    CONFIG_ROOT,
    PROJECT_ROOT,
    load_cost_model,
    load_descriptor_set,
    load_execution_policy,
    load_portfolio,
    load_sleeve,
    load_target,
    load_thesis,
)
from .benchmark_state_builder import load_benchmark_state_build_case
from .deployment_loader import (
    load_manual_execution_outcome,
    load_realized_trading_window,
    load_run_manifest,
)
from .live_state import BenchmarkStateArtifact, load_benchmark_state_artifact
from .models import (
    CostModel,
    DescriptorSet,
    ExecutionPolicy,
    PortfolioRecipe,
    ResidualTarget,
    Sleeve,
    Thesis,
)
from .portfolio_promotion_replay import PortfolioPromotionReplay
from .research_artifact_loader import (
    LoadedPortfolioPromotionReplayCase,
    SleeveResearchObservationInput,
    load_portfolio_promotion_replay_case,
    load_sleeve_research_observation_input,
)
from .trend_research_input_builder import (
    LoadedTrendResearchInputBuildCase,
    load_trend_research_input_build_case,
)


JsonMap = dict[str, Any]
_WEIGHT_TOLERANCE = 1e-9
_ALLOWED_CANDIDATE_STATUS = {
    "research_ready",
    "shadow_live_eligible",
    "probation_eligible",
}


@dataclass(slots=True)
class LiveCandidateBundleDefinition:
    candidate_id: str
    version: str
    status: str
    description: str
    thesis_path: str
    descriptor_set_path: str
    sleeve_path: str
    target_path: str
    portfolio_path: str
    default_cost_model_path: str
    stressed_cost_model_paths: list[str] = field(default_factory=list)
    regime_overlay_id: str = ""
    expected_turnover_budget: float = 0.0
    expected_breadth_min: int = 0
    expected_breadth_max: int = 0
    max_drawdown_budget: float = 0.0
    weak_regime_behavior: str = ""
    current_validation_window_start: str = ""
    multi_year_validation_required: bool = True
    multi_year_validation_audit_path: str = ""
    probation_policy_path: str = ""
    paper_trade_policy_path: str = ""
    account_export_contract_path: str = ""

    @classmethod
    def from_toml(cls, data: JsonMap) -> "LiveCandidateBundleDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(
                f"Unsupported live candidate bundle schema version: {schema_version}"
            )
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "live_candidate_bundle":
            raise ValueError(f"Unsupported live candidate bundle type: {artifact_type}")
        definition = cls(
            candidate_id=str(data["candidate_id"]),
            version=str(data["version"]),
            status=str(data["status"]),
            description=str(data["description"]),
            thesis_path=str(data["thesis_path"]),
            descriptor_set_path=str(data["descriptor_set_path"]),
            sleeve_path=str(data["sleeve_path"]),
            target_path=str(data["target_path"]),
            portfolio_path=str(data["portfolio_path"]),
            default_cost_model_path=str(data["default_cost_model_path"]),
            stressed_cost_model_paths=[
                str(path) for path in data.get("stressed_cost_model_paths", [])
            ],
            regime_overlay_id=str(data.get("regime_overlay_id", "")),
            expected_turnover_budget=float(data["expected_turnover_budget"]),
            expected_breadth_min=int(data["expected_breadth_min"]),
            expected_breadth_max=int(data["expected_breadth_max"]),
            max_drawdown_budget=float(data["max_drawdown_budget"]),
            weak_regime_behavior=str(data["weak_regime_behavior"]),
            current_validation_window_start=str(data["current_validation_window_start"]),
            multi_year_validation_required=bool(
                data.get("multi_year_validation_required", True)
            ),
            multi_year_validation_audit_path=str(
                data.get("multi_year_validation_audit_path", "")
            ),
            probation_policy_path=str(data["probation_policy_path"]),
            paper_trade_policy_path=str(data.get("paper_trade_policy_path", "")),
            account_export_contract_path=str(data.get("account_export_contract_path", "")),
        )
        if definition.status not in _ALLOWED_CANDIDATE_STATUS:
            raise ValueError(
                "Live candidate bundle status must be one of: "
                + ", ".join(sorted(_ALLOWED_CANDIDATE_STATUS))
            )
        if definition.expected_breadth_min > definition.expected_breadth_max:
            raise ValueError(
                "Live candidate bundle expected breadth range must be ascending."
            )
        return definition


@dataclass(slots=True)
class LoadedLiveCandidateBundle:
    definition: LiveCandidateBundleDefinition
    thesis: Thesis
    descriptor_set: DescriptorSet
    sleeve: Sleeve
    target: ResidualTarget
    portfolio: PortfolioRecipe
    default_cost_model: CostModel
    stressed_cost_models: dict[str, CostModel]


@dataclass(slots=True)
class MultiYearValidationAuditBuildCaseDefinition:
    case_id: str
    description: str
    candidate_id: str
    portfolio_path: str
    benchmark_state_build_case_path: str
    trend_research_input_build_case_path: str
    output_path: str
    replay_case_path: str = ""
    minimum_calendar_years: float = 5.0
    as_of_date: str = ""
    notes: list[str] = field(default_factory=list)

    @classmethod
    def from_toml(cls, data: JsonMap) -> "MultiYearValidationAuditBuildCaseDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(
                "Unsupported multi-year validation audit build case schema version: "
                f"{schema_version}"
            )
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "multi_year_validation_audit_build_case":
            raise ValueError(
                f"Unsupported multi-year validation audit build case type: {artifact_type}"
            )
        return cls(
            case_id=str(data["case_id"]),
            description=str(data["description"]),
            candidate_id=str(data["candidate_id"]),
            portfolio_path=str(data["portfolio_path"]),
            benchmark_state_build_case_path=str(data["benchmark_state_build_case_path"]),
            trend_research_input_build_case_path=str(
                data["trend_research_input_build_case_path"]
            ),
            output_path=str(data["output_path"]),
            replay_case_path=str(data.get("replay_case_path", "")),
            minimum_calendar_years=float(data.get("minimum_calendar_years", 5.0)),
            as_of_date=str(data.get("as_of_date", "")),
            notes=[str(item) for item in data.get("notes", [])],
        )


@dataclass(slots=True)
class LoadedMultiYearValidationAuditBuildCase:
    definition: MultiYearValidationAuditBuildCaseDefinition
    portfolio: PortfolioRecipe
    execution_policy: ExecutionPolicy
    benchmark_state_artifact: BenchmarkStateArtifact
    trend_case: LoadedTrendResearchInputBuildCase
    trend_observation_input: SleeveResearchObservationInput
    trend_input_payload: JsonMap
    target: ResidualTarget
    default_cost_model: CostModel
    replay_case: LoadedPortfolioPromotionReplayCase | None = None


@dataclass(slots=True)
class TradeabilityAuditDefinition:
    t_plus_one: bool = False
    suspension: bool = False
    price_limits: bool = False
    lot_size: bool = False
    liquidity: bool = False
    slippage: bool = False

    @classmethod
    def from_json(cls, data: JsonMap) -> "TradeabilityAuditDefinition":
        return cls(
            t_plus_one=bool(data.get("t_plus_one", False)),
            suspension=bool(data.get("suspension", False)),
            price_limits=bool(data.get("price_limits", False)),
            lot_size=bool(data.get("lot_size", False)),
            liquidity=bool(data.get("liquidity", False)),
            slippage=bool(data.get("slippage", False)),
        )

    def failed_checks(self) -> list[str]:
        failed = []
        for check, passed in (
            ("t_plus_one", self.t_plus_one),
            ("suspension", self.suspension),
            ("price_limits", self.price_limits),
            ("lot_size", self.lot_size),
            ("liquidity", self.liquidity),
            ("slippage", self.slippage),
        ):
            if not passed:
                failed.append(check)
        return failed


@dataclass(slots=True)
class MarketStateCoverageDefinition:
    drawdown: bool = False
    rebound: bool = False
    broad_trend: bool = False
    fragile_leadership: bool = False

    @classmethod
    def from_json(cls, data: JsonMap) -> "MarketStateCoverageDefinition":
        return cls(
            drawdown=bool(data.get("drawdown", False)),
            rebound=bool(data.get("rebound", False)),
            broad_trend=bool(data.get("broad_trend", False)),
            fragile_leadership=bool(data.get("fragile_leadership", False)),
        )

    def missing_states(self) -> list[str]:
        return [
            state
            for state, covered in (
                ("drawdown", self.drawdown),
                ("rebound", self.rebound),
                ("broad_trend", self.broad_trend),
                ("fragile_leadership", self.fragile_leadership),
            )
            if not covered
        ]


@dataclass(slots=True)
class MultiYearValidationAuditDefinition:
    candidate_id: str
    as_of_date: str
    validation_window_start: str
    validation_window_end: str
    minimum_calendar_years: float
    benchmark_membership_pit: bool
    industry_classification_pit: bool
    anchored_walk_forward_complete: bool
    regime_split_complete: bool
    portfolio_evidence_complete: bool
    tradeability: TradeabilityAuditDefinition
    market_state_coverage: MarketStateCoverageDefinition
    notes: list[str] = field(default_factory=list)

    @classmethod
    def from_json(cls, data: JsonMap) -> "MultiYearValidationAuditDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(
                f"Unsupported multi-year validation audit schema version: {schema_version}"
            )
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "multi_year_validation_audit":
            raise ValueError(
                f"Unsupported multi-year validation audit type: {artifact_type}"
            )
        return cls(
            candidate_id=str(data["candidate_id"]),
            as_of_date=str(data["as_of_date"]),
            validation_window_start=str(data["validation_window_start"]),
            validation_window_end=str(data["validation_window_end"]),
            minimum_calendar_years=float(data.get("minimum_calendar_years", 5.0)),
            benchmark_membership_pit=bool(data.get("benchmark_membership_pit", False)),
            industry_classification_pit=bool(
                data.get("industry_classification_pit", False)
            ),
            anchored_walk_forward_complete=bool(
                data.get("anchored_walk_forward_complete", False)
            ),
            regime_split_complete=bool(data.get("regime_split_complete", False)),
            portfolio_evidence_complete=bool(
                data.get("portfolio_evidence_complete", False)
            ),
            tradeability=TradeabilityAuditDefinition.from_json(
                dict(data.get("tradeability", {}))
            ),
            market_state_coverage=MarketStateCoverageDefinition.from_json(
                dict(data.get("market_state_coverage", {}))
            ),
            notes=[str(item) for item in data.get("notes", [])],
        )


@dataclass(slots=True)
class MultiYearValidationAuditSummary:
    calendar_years_covered: float = 0.0
    minimum_calendar_years: float = 0.0
    failed_tradeability_checks: list[str] = field(default_factory=list)
    missing_market_states: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    signal_release_gate_met: bool = False


@dataclass(slots=True)
class EvaluatedMultiYearValidationAudit:
    definition: MultiYearValidationAuditDefinition
    summary: MultiYearValidationAuditSummary


def load_multi_year_validation_audit_build_case(
    path: Path | str,
) -> LoadedMultiYearValidationAuditBuildCase:
    definition = MultiYearValidationAuditBuildCaseDefinition.from_toml(_read_toml(path))
    portfolio = load_portfolio(definition.portfolio_path)
    execution_policy = load_execution_policy(
        CONFIG_ROOT / "execution_policies" / f"{portfolio.execution_policy_id}.toml"
    )
    benchmark_case = load_benchmark_state_build_case(
        definition.benchmark_state_build_case_path
    )
    benchmark_state_artifact = load_benchmark_state_artifact(
        benchmark_case.definition.output_path
    )
    trend_case = load_trend_research_input_build_case(
        definition.trend_research_input_build_case_path
    )
    if trend_case.sleeve_id not in portfolio.sleeves:
        raise ValueError(
            "Multi-year validation audit build case portfolio must include the "
            f"trend sleeve {trend_case.sleeve_id}."
        )
    target = load_target(CONFIG_ROOT / "targets" / f"{trend_case.target_id}.toml")
    default_cost_model = load_cost_model(
        CONFIG_ROOT / "cost_models" / f"{target.cost_model}.toml"
    )
    trend_input_payload = _read_json(trend_case.definition.output_path)
    trend_observation_input = load_sleeve_research_observation_input(
        trend_case.definition.output_path
    )
    replay_case = None
    if definition.replay_case_path.strip():
        replay_case = load_portfolio_promotion_replay_case(definition.replay_case_path)
        if _resolve_project_path(replay_case.definition.candidate_portfolio_path) != _resolve_project_path(
            definition.portfolio_path
        ):
            raise ValueError(
                "Multi-year validation audit replay case candidate_portfolio_path must "
                "match the audited portfolio_path."
            )
    if benchmark_state_artifact.benchmark_id != portfolio.benchmark:
        raise ValueError(
            "Multi-year validation audit benchmark state must match the audited "
            f"portfolio benchmark {portfolio.benchmark}."
        )
    return LoadedMultiYearValidationAuditBuildCase(
        definition=definition,
        portfolio=portfolio,
        execution_policy=execution_policy,
        benchmark_state_artifact=benchmark_state_artifact,
        trend_case=trend_case,
        trend_observation_input=trend_observation_input,
        trend_input_payload=trend_input_payload,
        target=target,
        default_cost_model=default_cost_model,
        replay_case=replay_case,
    )


@dataclass(slots=True)
class ShadowLiveCycleDefinition:
    run_manifest_path: str
    manual_execution_outcome_path: str
    realized_trading_window_path: str


@dataclass(slots=True)
class ShadowLiveJournalDefinition:
    candidate_bundle_path: str
    started_at: str
    cycles: list[ShadowLiveCycleDefinition] = field(default_factory=list)

    @classmethod
    def from_json(cls, data: JsonMap) -> "ShadowLiveJournalDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(
                f"Unsupported shadow live journal schema version: {schema_version}"
            )
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "shadow_live_journal":
            raise ValueError(f"Unsupported shadow live journal type: {artifact_type}")
        return cls(
            candidate_bundle_path=str(data["candidate_bundle_path"]),
            started_at=str(data["started_at"]),
            cycles=[
                ShadowLiveCycleDefinition(
                    run_manifest_path=str(item["run_manifest_path"]),
                    manual_execution_outcome_path=str(item["manual_execution_outcome_path"]),
                    realized_trading_window_path=str(item["realized_trading_window_path"]),
                )
                for item in data.get("cycles", [])
            ],
        )


@dataclass(slots=True)
class ShadowLiveCycleTrace:
    trade_date: str
    run_id: str
    overlay_state: str
    run_manifest_path: str
    manual_execution_outcome_path: str
    realized_trading_window_path: str
    blocked_trade_count: int
    manual_override_count: int
    exception_count: int
    cash_drift_weight: float
    realized_turnover: float


@dataclass(slots=True)
class ShadowLiveJournalSummary:
    cycle_count: int = 0
    calendar_month_count: int = 0
    consecutive_weekly_cycles: int = 0
    shadow_live_gate_met: bool = False
    probation_preferred_gate_met: bool = False
    overlay_state_counts: dict[str, int] = field(default_factory=dict)
    blocked_trade_count: int = 0
    manual_override_count: int = 0
    exception_count: int = 0
    max_cash_drift_weight: float = 0.0
    average_cash_drift_weight: float = 0.0
    average_realized_turnover: float = 0.0
    max_realized_turnover: float = 0.0


@dataclass(slots=True)
class EvaluatedShadowLiveJournal:
    definition: ShadowLiveJournalDefinition
    bundle: LoadedLiveCandidateBundle
    cycles: list[ShadowLiveCycleTrace]
    summary: ShadowLiveJournalSummary


def load_live_candidate_bundle(path: Path | str) -> LoadedLiveCandidateBundle:
    definition = LiveCandidateBundleDefinition.from_toml(_read_toml(path))
    thesis = load_thesis(definition.thesis_path)
    descriptor_set = load_descriptor_set(definition.descriptor_set_path)
    sleeve = load_sleeve(definition.sleeve_path)
    target = load_target(definition.target_path)
    portfolio = load_portfolio(definition.portfolio_path)
    default_cost_model = load_cost_model(definition.default_cost_model_path)
    stressed_cost_models = {
        model.id: model
        for model in (
            load_cost_model(item) for item in definition.stressed_cost_model_paths
        )
        if model.id != default_cost_model.id
    }
    _validate_live_candidate_bundle(
        definition=definition,
        thesis=thesis,
        descriptor_set=descriptor_set,
        sleeve=sleeve,
        target=target,
        portfolio=portfolio,
    )
    return LoadedLiveCandidateBundle(
        definition=definition,
        thesis=thesis,
        descriptor_set=descriptor_set,
        sleeve=sleeve,
        target=target,
        portfolio=portfolio,
        default_cost_model=default_cost_model,
        stressed_cost_models=stressed_cost_models,
    )


def evaluate_shadow_live_journal(path: Path | str) -> EvaluatedShadowLiveJournal:
    definition = ShadowLiveJournalDefinition.from_json(_read_json(path))
    bundle = load_live_candidate_bundle(definition.candidate_bundle_path)
    cycles = [
        _load_shadow_live_cycle(bundle=bundle, definition=cycle)
        for cycle in definition.cycles
    ]
    _validate_shadow_live_cycle_order(cycles)
    summary = _shadow_live_summary(cycles)
    return EvaluatedShadowLiveJournal(
        definition=definition,
        bundle=bundle,
        cycles=cycles,
        summary=summary,
    )


def evaluate_multi_year_validation_audit(
    path: Path | str,
) -> EvaluatedMultiYearValidationAudit:
    definition = MultiYearValidationAuditDefinition.from_json(_read_json(path))
    window_start = date.fromisoformat(definition.validation_window_start)
    window_end = date.fromisoformat(definition.validation_window_end)
    if window_end < window_start:
        raise ValueError(
            "Multi-year validation audit window_end must be on or after window_start."
        )
    summary = MultiYearValidationAuditSummary(
        calendar_years_covered=((window_end - window_start).days + 1) / 365.25,
        minimum_calendar_years=definition.minimum_calendar_years,
        failed_tradeability_checks=definition.tradeability.failed_checks(),
        missing_market_states=definition.market_state_coverage.missing_states(),
    )
    if summary.calendar_years_covered < definition.minimum_calendar_years:
        summary.blockers.append("minimum_history_years")
    if not definition.benchmark_membership_pit:
        summary.blockers.append("benchmark_membership_pit")
    if not definition.industry_classification_pit:
        summary.blockers.append("industry_classification_pit")
    if summary.failed_tradeability_checks:
        summary.blockers.append("tradeability_realism")
    if summary.missing_market_states:
        summary.blockers.append("market_state_coverage")
    if not definition.anchored_walk_forward_complete:
        summary.blockers.append("anchored_walk_forward")
    if not definition.regime_split_complete:
        summary.blockers.append("regime_split")
    if not definition.portfolio_evidence_complete:
        summary.blockers.append("portfolio_evidence")
    summary.signal_release_gate_met = not summary.blockers
    return EvaluatedMultiYearValidationAudit(
        definition=definition,
        summary=summary,
    )


def build_multi_year_validation_audit(
    loaded_case: LoadedMultiYearValidationAuditBuildCase,
) -> MultiYearValidationAuditDefinition:
    trend_dates = [step.trade_date for step in loaded_case.trend_observation_input.steps]
    if not trend_dates:
        raise ValueError(
            "Multi-year validation audit build case trend observation input must "
            "contain at least one trade date."
        )

    validation_dates = list(trend_dates)
    anchored_walk_forward_complete = False
    regime_split_complete = False
    portfolio_evidence_complete = False
    market_state_coverage = MarketStateCoverageDefinition()
    auto_notes = list(loaded_case.definition.notes)

    if loaded_case.replay_case is not None:
        replay_result = PortfolioPromotionReplay(
            mandate=loaded_case.replay_case.mandate,
            construction_model=loaded_case.replay_case.construction_model,
            default_cost_model=loaded_case.replay_case.default_cost_model,
            gate=loaded_case.replay_case.gate,
            cost_models=loaded_case.replay_case.cost_models,
        ).replay(loaded_case.replay_case.replay_input)
        validation_dates = [
            step.trade_date for step in replay_result.candidate_simulation.steps
        ]
        anchored_walk_forward_complete = (
            replay_result.walk_forward is not None
            and replay_result.walk_forward.stability.split_count
            == len(loaded_case.replay_case.definition.walk_forward_splits)
            and replay_result.walk_forward.stability.split_count > 0
        )
        regime_split_complete = (
            replay_result.regime_breakdown is not None
            and replay_result.regime_breakdown.stability.bucket_count > 0
        )
        market_state_coverage = _replay_market_state_coverage(replay_result)
        if loaded_case.portfolio.regime_overlay_id:
            portfolio_evidence_complete = (
                replay_result.regime_overlay is not None
                and len(replay_result.regime_overlay.decisions)
                == len(replay_result.candidate_simulation.steps)
            )
            if not portfolio_evidence_complete:
                auto_notes.append(
                    "Portfolio-level replay evidence remains incomplete because "
                    "regime_overlay decisions are missing for one or more replay "
                    "trade dates."
                )
        else:
            portfolio_evidence_complete = True
    else:
        auto_notes.append(
            "No replay case is attached; anchored walk-forward, regime split, and "
            "portfolio-level evidence remain incomplete."
        )

    normalized_dates = [_normalize_trade_date(value) for value in validation_dates]
    window_start = min(normalized_dates)
    window_end = max(normalized_dates)
    calendar_years_covered = (
        (date.fromisoformat(window_end) - date.fromisoformat(window_start)).days + 1
    ) / 365.25
    if calendar_years_covered < loaded_case.definition.minimum_calendar_years:
        auto_notes.append(
            "Current validation window covers only "
            f"{calendar_years_covered:.4f} calendar years from {window_start} to "
            f"{window_end}. Treat this artifact as a pipeline smoke check only, not "
            "as release-grade strategy validation evidence."
        )
    benchmark_membership_pit = _benchmark_membership_is_pit_safe(
        loaded_case=loaded_case,
        validation_dates=validation_dates,
        notes=auto_notes,
    )
    industry_classification_pit = _industry_classification_is_pit_safe(
        loaded_case=loaded_case,
        validation_dates=validation_dates,
        notes=auto_notes,
    )

    return MultiYearValidationAuditDefinition(
        candidate_id=loaded_case.definition.candidate_id,
        as_of_date=(
            loaded_case.definition.as_of_date or date.today().isoformat()
        ),
        validation_window_start=window_start,
        validation_window_end=window_end,
        minimum_calendar_years=loaded_case.definition.minimum_calendar_years,
        benchmark_membership_pit=benchmark_membership_pit,
        industry_classification_pit=industry_classification_pit,
        anchored_walk_forward_complete=anchored_walk_forward_complete,
        regime_split_complete=regime_split_complete,
        portfolio_evidence_complete=portfolio_evidence_complete,
        tradeability=_tradeability_audit(loaded_case),
        market_state_coverage=market_state_coverage,
        notes=auto_notes,
    )


def write_multi_year_validation_audit(
    definition: MultiYearValidationAuditDefinition,
    path: Path | str,
) -> Path:
    target = _resolve_project_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "artifact_type": "multi_year_validation_audit",
        "candidate_id": definition.candidate_id,
        "as_of_date": definition.as_of_date,
        "validation_window_start": definition.validation_window_start,
        "validation_window_end": definition.validation_window_end,
        "minimum_calendar_years": definition.minimum_calendar_years,
        "benchmark_membership_pit": definition.benchmark_membership_pit,
        "industry_classification_pit": definition.industry_classification_pit,
        "anchored_walk_forward_complete": definition.anchored_walk_forward_complete,
        "regime_split_complete": definition.regime_split_complete,
        "portfolio_evidence_complete": definition.portfolio_evidence_complete,
        "tradeability": {
            "t_plus_one": definition.tradeability.t_plus_one,
            "suspension": definition.tradeability.suspension,
            "price_limits": definition.tradeability.price_limits,
            "lot_size": definition.tradeability.lot_size,
            "liquidity": definition.tradeability.liquidity,
            "slippage": definition.tradeability.slippage,
        },
        "market_state_coverage": {
            "drawdown": definition.market_state_coverage.drawdown,
            "rebound": definition.market_state_coverage.rebound,
            "broad_trend": definition.market_state_coverage.broad_trend,
            "fragile_leadership": definition.market_state_coverage.fragile_leadership,
        },
        "notes": list(definition.notes),
    }
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return target


def _validate_live_candidate_bundle(
    *,
    definition: LiveCandidateBundleDefinition,
    thesis: Thesis,
    descriptor_set: DescriptorSet,
    sleeve: Sleeve,
    target: ResidualTarget,
    portfolio: PortfolioRecipe,
) -> None:
    for path_value in (
        definition.probation_policy_path,
        definition.paper_trade_policy_path,
    ):
        if not path_value.strip():
            raise ValueError("Live candidate bundle must define its required operating docs.")
        target_path = _resolve_project_path(path_value)
        if not target_path.exists():
            raise ValueError(f"Live candidate bundle path does not exist: {path_value}")
    if definition.account_export_contract_path.strip():
        account_contract_path = _resolve_project_path(
            definition.account_export_contract_path
        )
        if not account_contract_path.exists():
            raise ValueError(
                "Live candidate bundle path does not exist: "
                f"{definition.account_export_contract_path}"
            )
    audit = None
    if definition.multi_year_validation_required:
        if not definition.multi_year_validation_audit_path.strip():
            raise ValueError(
                "Live candidate bundle must define multi_year_validation_audit_path "
                "when multi_year_validation_required is true."
            )
        audit = evaluate_multi_year_validation_audit(
            definition.multi_year_validation_audit_path
        )
    elif definition.multi_year_validation_audit_path.strip():
        audit = evaluate_multi_year_validation_audit(
            definition.multi_year_validation_audit_path
        )
    if descriptor_set.thesis_id != thesis.id:
        raise ValueError("Live candidate descriptor set must match the thesis id.")
    if descriptor_set.target_id != target.id:
        raise ValueError("Live candidate descriptor set must match the target id.")
    if sleeve.thesis_id != thesis.id:
        raise ValueError("Live candidate sleeve must match the thesis id.")
    if sleeve.descriptor_set_id != descriptor_set.id:
        raise ValueError("Live candidate sleeve must match the descriptor set id.")
    if sleeve.target_id != target.id:
        raise ValueError("Live candidate sleeve must match the target id.")
    if portfolio.sleeves != [sleeve.id]:
        raise ValueError(
            "Live candidate portfolio must hold exactly one sleeve: "
            f"{sleeve.id}."
        )
    total_allocation = sum(portfolio.allocation.values())
    if abs(total_allocation - 1.0) > _WEIGHT_TOLERANCE:
        raise ValueError("Live candidate portfolio allocation must sum to 100%.")
    if abs(portfolio.allocation.get(sleeve.id, 0.0) - 1.0) > _WEIGHT_TOLERANCE:
        raise ValueError("Live candidate portfolio must allocate 100% to the frozen sleeve.")
    if definition.regime_overlay_id != portfolio.regime_overlay_id:
        raise ValueError(
            "Live candidate bundle regime_overlay_id must match the portfolio recipe."
        )
    if abs(definition.expected_turnover_budget - sleeve.turnover_budget) > _WEIGHT_TOLERANCE:
        raise ValueError(
            "Live candidate expected_turnover_budget must match the frozen sleeve turnover budget."
        )
    holding_count = int(sleeve.construction.get("holding_count", 0))
    if not (
        definition.expected_breadth_min
        <= holding_count
        <= definition.expected_breadth_max
    ):
        raise ValueError(
            "Live candidate breadth range must contain the frozen holding count."
        )
    if definition.max_drawdown_budget <= 0.0:
        raise ValueError("Live candidate max_drawdown_budget must be positive.")
    if not definition.current_validation_window_start:
        raise ValueError(
            "Live candidate bundle must record the current honest validation window start."
        )
    if not definition.weak_regime_behavior.strip():
        raise ValueError("Live candidate bundle must describe weak-regime behavior.")
    if audit is not None and audit.definition.candidate_id != definition.candidate_id:
        raise ValueError(
            "Live candidate multi-year validation audit must match the frozen candidate id."
        )


def _load_shadow_live_cycle(
    *,
    bundle: LoadedLiveCandidateBundle,
    definition: ShadowLiveCycleDefinition,
) -> ShadowLiveCycleTrace:
    run_manifest = load_run_manifest(definition.run_manifest_path)
    manual_outcome = load_manual_execution_outcome(
        definition.manual_execution_outcome_path
    )
    realized_window = load_realized_trading_window(
        definition.realized_trading_window_path
    )
    if run_manifest.portfolio_id != bundle.portfolio.id:
        raise ValueError(
            "Shadow live run manifest portfolio_id must match the frozen live candidate."
        )
    if manual_outcome.portfolio_id != bundle.portfolio.id:
        raise ValueError(
            "Shadow live manual execution outcome portfolio_id must match the frozen live candidate."
        )
    if realized_window.portfolio_id != bundle.portfolio.id:
        raise ValueError(
            "Shadow live realized trading window portfolio_id must match the frozen live candidate."
        )
    if run_manifest.portfolio_path:
        if _resolve_project_path(run_manifest.portfolio_path) != _resolve_project_path(
            bundle.definition.portfolio_path
        ):
            raise ValueError(
                "Shadow live run manifest portfolio_path must stay attached to the frozen live candidate."
            )
    if bundle.definition.regime_overlay_id and not run_manifest.regime_overlay_state:
        raise ValueError(
            "Shadow live cycle must persist a regime overlay state for the frozen candidate."
        )
    run_ids = {run_manifest.run_id, manual_outcome.run_id, realized_window.run_id}
    if len(run_ids) != 1:
        raise ValueError("Shadow live cycle artifacts must share one run_id.")
    if manual_outcome.package_id != run_manifest.package_id:
        raise ValueError("Shadow live manual execution outcome must match the run manifest package.")
    if manual_outcome.execution_date != run_manifest.execution_date:
        raise ValueError(
            "Shadow live manual execution outcome execution_date must match the run manifest."
        )
    return ShadowLiveCycleTrace(
        trade_date=run_manifest.trade_date,
        run_id=run_manifest.run_id,
        overlay_state=run_manifest.regime_overlay_state,
        run_manifest_path=definition.run_manifest_path,
        manual_execution_outcome_path=definition.manual_execution_outcome_path,
        realized_trading_window_path=definition.realized_trading_window_path,
        blocked_trade_count=len(manual_outcome.blocked_trades),
        manual_override_count=len(manual_outcome.manual_overrides),
        exception_count=len(manual_outcome.exception_reasons),
        cash_drift_weight=manual_outcome.cash_drift_weight,
        realized_turnover=realized_window.realized_summary.average_turnover,
    )


def _validate_shadow_live_cycle_order(cycles: list[ShadowLiveCycleTrace]) -> None:
    seen_run_ids: set[str] = set()
    seen_trade_dates: set[str] = set()
    previous_date: date | None = None
    for cycle in cycles:
        if cycle.run_id in seen_run_ids:
            raise ValueError(f"Duplicate shadow live run_id: {cycle.run_id}")
        if cycle.trade_date in seen_trade_dates:
            raise ValueError(f"Duplicate shadow live trade_date: {cycle.trade_date}")
        seen_run_ids.add(cycle.run_id)
        seen_trade_dates.add(cycle.trade_date)
        current_date = date.fromisoformat(cycle.trade_date)
        if previous_date is not None and current_date <= previous_date:
            raise ValueError("Shadow live cycles must stay in ascending trade_date order.")
        previous_date = current_date


def _shadow_live_summary(cycles: list[ShadowLiveCycleTrace]) -> ShadowLiveJournalSummary:
    summary = ShadowLiveJournalSummary(
        cycle_count=len(cycles),
        overlay_state_counts={"normal": 0, "de_risk": 0, "cash_heavier": 0},
    )
    if not cycles:
        return summary
    summary.calendar_month_count = len({cycle.trade_date[:7] for cycle in cycles})
    summary.consecutive_weekly_cycles = _longest_weekly_streak(
        [cycle.trade_date for cycle in cycles]
    )
    for cycle in cycles:
        if cycle.overlay_state in summary.overlay_state_counts:
            summary.overlay_state_counts[cycle.overlay_state] += 1
        summary.blocked_trade_count += cycle.blocked_trade_count
        summary.manual_override_count += cycle.manual_override_count
        summary.exception_count += cycle.exception_count
        summary.max_cash_drift_weight = max(
            summary.max_cash_drift_weight,
            abs(cycle.cash_drift_weight),
        )
        summary.max_realized_turnover = max(
            summary.max_realized_turnover,
            cycle.realized_turnover,
        )
    summary.average_cash_drift_weight = sum(
        abs(cycle.cash_drift_weight) for cycle in cycles
    ) / len(cycles)
    summary.average_realized_turnover = sum(
        cycle.realized_turnover for cycle in cycles
    ) / len(cycles)
    summary.shadow_live_gate_met = (
        summary.cycle_count >= 12
        and summary.consecutive_weekly_cycles >= 12
        and summary.calendar_month_count >= 3
    )
    summary.probation_preferred_gate_met = (
        summary.cycle_count >= 20
        and summary.consecutive_weekly_cycles >= 20
        and summary.calendar_month_count >= 6
    )
    return summary


def _longest_weekly_streak(trade_dates: list[str]) -> int:
    if not trade_dates:
        return 0
    parsed = [date.fromisoformat(value) for value in trade_dates]
    longest = 1
    current = 1
    for previous, current_date in zip(parsed, parsed[1:]):
        day_gap = (current_date - previous).days
        if 4 <= day_gap <= 10:
            current += 1
        else:
            current = 1
        longest = max(longest, current)
    return longest


def _tradeability_audit(
    loaded_case: LoadedMultiYearValidationAuditBuildCase,
) -> TradeabilityAuditDefinition:
    target = loaded_case.target
    eligibility = set(target.eligibility)
    payload_steps = list(loaded_case.trend_input_payload.get("steps", []))
    has_trade_state_keys = _all_records_have_trade_state_keys(payload_steps)
    return TradeabilityAuditDefinition(
        t_plus_one=(
            target.trade_entry == "next_day_open"
            and target.trade_exit == "open_on_horizon"
            and target.horizon_days >= 1
        ),
        suspension="not_suspended" in eligibility and has_trade_state_keys,
        price_limits=(
            loaded_case.trend_case.definition.limit_lock_mode
            == "cn_a_directional_open_lock"
            and "not_limit_locked" in eligibility
            and has_trade_state_keys
        ),
        lot_size=loaded_case.execution_policy.lot_size > 0,
        liquidity=(
            "liquidity_pass" in eligibility
            and loaded_case.trend_case.min_turnover_cny_mn > 0.0
            and has_trade_state_keys
        ),
        slippage=(
            loaded_case.default_cost_model.buy_slippage_bps > 0.0
            and loaded_case.default_cost_model.sell_slippage_bps > 0.0
        ),
    )


def _benchmark_membership_is_pit_safe(
    *,
    loaded_case: LoadedMultiYearValidationAuditBuildCase,
    validation_dates: list[str],
    notes: list[str],
) -> bool:
    artifact = loaded_case.benchmark_state_artifact
    if loaded_case.portfolio.benchmark != artifact.benchmark_id:
        notes.append(
            "Benchmark-state artifact benchmark_id does not match the audited "
            f"portfolio benchmark {loaded_case.portfolio.benchmark}."
        )
        return False
    if artifact.weighting_method != "provider_weight":
        notes.append(
            "Benchmark-state artifact is not using provider_weight; PIT benchmark "
            "membership remains unaudited."
        )
        return False

    by_date = {step.trade_date: step for step in artifact.steps}
    missing_dates = [
        trade_date
        for trade_date in validation_dates
        if trade_date not in by_date
    ]
    if missing_dates:
        notes.append(
            "Benchmark-state artifact is missing validation dates: "
            + ", ".join(sorted(missing_dates)[:5])
        )
        return False

    for trade_date in validation_dates:
        step = by_date[trade_date]
        if not step.effective_at or not step.available_at:
            notes.append(
                "Benchmark-state artifact is missing effective/available timestamps on "
                f"{trade_date}."
            )
            return False
        if not step.constituents:
            notes.append(
                "Benchmark-state artifact has no constituents on validation date "
                f"{trade_date}."
            )
            return False
        if any(not constituent.industry for constituent in step.constituents):
            notes.append(
                "Benchmark-state artifact has constituents without PIT industry labels on "
                f"{trade_date}."
            )
            return False
    return True


def _industry_classification_is_pit_safe(
    *,
    loaded_case: LoadedMultiYearValidationAuditBuildCase,
    validation_dates: list[str],
    notes: list[str],
) -> bool:
    trend_definition = loaded_case.trend_case.definition
    if trend_definition.industry_label_source != "industry_classification_pit":
        notes.append(
            "Trend observation builder is not using industry_classification_pit; "
            "industry PIT coverage remains unaudited."
        )
        return False
    if (
        loaded_case.benchmark_state_artifact.classification
        and trend_definition.industry_schema
        and loaded_case.benchmark_state_artifact.classification
        != trend_definition.industry_schema
    ):
        notes.append(
            "Trend observation input industry_schema does not match the audited "
            "benchmark-state classification."
        )
        return False

    records_by_date = _trend_records_by_date(loaded_case.trend_input_payload)
    missing_dates = [
        trade_date
        for trade_date in validation_dates
        if trade_date not in records_by_date
    ]
    if missing_dates:
        notes.append(
            "Trend observation input is missing validation dates: "
            + ", ".join(sorted(missing_dates)[:5])
        )
        return False
    for trade_date in validation_dates:
        records = records_by_date[trade_date]
        if not records:
            notes.append(
                "Trend observation input has no records on validation date "
                f"{trade_date}."
            )
            return False
        if any(not str(record.get("industry", "")).strip() for record in records):
            notes.append(
                "Trend observation input has records without PIT industry labels on "
                f"{trade_date}."
            )
            return False
    return True


def _all_records_have_trade_state_keys(steps: list[Any]) -> bool:
    for step in steps:
        for record in dict(step).get("records", []):
            if "entry_state" not in record or "exit_state" not in record:
                return False
    return True


def _trend_records_by_date(payload: JsonMap) -> dict[str, list[JsonMap]]:
    records_by_date: dict[str, list[JsonMap]] = {}
    for item in payload.get("steps", []):
        trade_date = str(item["trade_date"])
        records_by_date[trade_date] = [dict(record) for record in item.get("records", [])]
    return records_by_date


def _replay_market_state_coverage(result: Any) -> MarketStateCoverageDefinition:
    drawdowns = _candidate_drawdown_series(
        [step.net_return for step in result.candidate_simulation.steps]
    )
    weak_breadth_bucket = next(
        (
            bucket
            for bucket in result.regime_breakdown.buckets
            if bucket.bucket_id == "weak_breadth"
        ),
        None,
    )
    rebound = any(
        previous > current
        and previous > 0.0
        and result.candidate_simulation.steps[index].net_return > 0.0
        for index, (previous, current) in enumerate(zip(drawdowns, drawdowns[1:]), start=1)
    )
    overlay_fragile_leadership = (
        result.regime_overlay is not None
        and any(
            "market_breadth" in decision.risk_off_inputs
            for decision in result.regime_overlay.decisions
        )
    )
    fragile_leadership = (
        weak_breadth_bucket is not None
        and weak_breadth_bucket.period_count > 0
    ) or any(
        subperiod.weakness_id == "weak_breadth" and subperiod.period_count > 0
        for subperiod in result.regime_breakdown.weak_subperiods
    ) or overlay_fragile_leadership
    return MarketStateCoverageDefinition(
        drawdown=any(value > 0.0 for value in drawdowns),
        rebound=rebound,
        broad_trend=any(
            step.net_return > 0.0 for step in result.candidate_simulation.steps
        ),
        fragile_leadership=fragile_leadership,
    )


def _candidate_drawdown_series(returns: list[float]) -> list[float]:
    equity = 1.0
    peak = 1.0
    drawdowns: list[float] = []
    for step_return in returns:
        equity *= 1.0 + step_return
        peak = max(peak, equity)
        drawdowns.append(1.0 - (equity / peak))
    return drawdowns


def _normalize_trade_date(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return date.fromisoformat(value).isoformat()


def _resolve_project_path(path: Path | str) -> Path:
    target = Path(path)
    if target.is_absolute():
        return target
    return PROJECT_ROOT / target


def _read_toml(path: Path | str) -> JsonMap:
    target = _resolve_project_path(path)
    with target.open("rb") as handle:
        return tomllib.load(handle)


def _read_json(path: Path | str) -> JsonMap:
    target = _resolve_project_path(path)
    with target.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def validate_live_candidate_signal_release(
    *,
    bundle_path: Path | str,
    portfolio_path: Path | str,
) -> EvaluatedMultiYearValidationAudit:
    bundle = load_live_candidate_bundle(bundle_path)
    if bundle.definition.status not in {"shadow_live_eligible", "probation_eligible"}:
        raise ValueError(
            "Live candidate status must be shadow_live_eligible or probation_eligible "
            "before signal release."
        )
    if _resolve_project_path(portfolio_path) != _resolve_project_path(
        bundle.definition.portfolio_path
    ):
        raise ValueError(
            "Executable signal case portfolio_path must stay attached to the frozen live candidate."
        )
    audit = evaluate_multi_year_validation_audit(
        bundle.definition.multi_year_validation_audit_path
    )
    if not audit.summary.signal_release_gate_met:
        raise ValueError(
            "Live candidate multi-year validation audit gate is not met: "
            + ", ".join(audit.summary.blockers)
        )
    return audit
