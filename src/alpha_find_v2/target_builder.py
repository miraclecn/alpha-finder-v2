from __future__ import annotations

from dataclasses import dataclass, field

from .models import CostModel, ResidualTarget
from .risk_model import RiskDecomposition


@dataclass(slots=True)
class TradeLegState:
    suspended: bool = False
    limit_locked: bool = False
    liquidity_pass: bool = True


@dataclass(slots=True)
class TargetObservation:
    entry_open: float
    exit_open: float
    entry_state: TradeLegState = field(default_factory=TradeLegState)
    exit_state: TradeLegState = field(default_factory=TradeLegState)
    residual_components: dict[str, float] = field(default_factory=dict)
    risk_decomposition: RiskDecomposition | None = None


@dataclass(slots=True)
class ExecutableTargetDefinition:
    target_id: str
    signal_observation: str
    return_basis: str
    entry_offset_days: int
    exit_offset_days: int
    cost_model_id: str
    risk_model_id: str
    round_trip_cost_bps: float
    label_name: str
    required_residual_components: list[str] = field(default_factory=list)
    eligibility_rules: list[str] = field(default_factory=list)


@dataclass(slots=True)
class TargetEvaluation:
    eligible: bool
    excluded_reasons: list[str] = field(default_factory=list)
    gross_return: float | None = None
    net_return: float | None = None
    residual_return: float | None = None
    round_trip_cost_bps: float = 0.0
    common_return: float = 0.0


class ExecutableResidualTargetBuilder:
    def __init__(self, target: ResidualTarget, cost_model: CostModel) -> None:
        self.target = target
        self.cost_model = cost_model
        self._validate_supported_target()

    def definition(self) -> ExecutableTargetDefinition:
        return ExecutableTargetDefinition(
            target_id=self.target.id,
            signal_observation=self.target.signal_observation,
            return_basis=self.target.return_basis,
            entry_offset_days=self._entry_offset_days(),
            exit_offset_days=self._exit_offset_days(),
            cost_model_id=self.cost_model.id,
            risk_model_id=self.target.risk_model_id,
            round_trip_cost_bps=self.cost_model.round_trip_bps(),
            label_name=f"{self.target.id}__residual_net_return",
            required_residual_components=list(self.target.residualization),
            eligibility_rules=list(self.target.eligibility),
        )

    def evaluate(self, observation: TargetObservation) -> TargetEvaluation:
        if observation.entry_open <= 0 or observation.exit_open <= 0:
            raise ValueError("Entry and exit open prices must be strictly positive.")

        residual_components = self._resolved_residual_components(observation)
        missing_components = [
            component
            for component in self.target.residualization
            if component not in residual_components
        ]
        if missing_components:
            joined = ", ".join(missing_components)
            raise ValueError(f"Missing residual components for target evaluation: {joined}")

        excluded_reasons = self._check_eligibility(observation)
        gross_return = (observation.exit_open / observation.entry_open) - 1.0
        net_return = gross_return - (self.cost_model.round_trip_bps() / 10_000.0)
        common_return = sum(
            residual_components[component]
            for component in self.target.residualization
        )
        residual_return = net_return - common_return

        return TargetEvaluation(
            eligible=not excluded_reasons,
            excluded_reasons=excluded_reasons,
            gross_return=gross_return,
            net_return=net_return,
            residual_return=residual_return,
            round_trip_cost_bps=self.cost_model.round_trip_bps(),
            common_return=common_return,
        )

    def can_enter(self, observation: TargetObservation) -> bool:
        return not self._check_leg_eligibility(
            leg_state=observation.entry_state,
            leg_label="entry",
        )

    def can_exit(self, observation: TargetObservation) -> bool:
        return not self._check_leg_eligibility(
            leg_state=observation.exit_state,
            leg_label="exit",
        )

    def _validate_supported_target(self) -> None:
        if self.target.trade_entry != "next_day_open":
            raise ValueError(f"Unsupported trade_entry: {self.target.trade_entry}")
        if self.target.trade_exit != "open_on_horizon":
            raise ValueError(f"Unsupported trade_exit: {self.target.trade_exit}")
        if self.target.return_basis != "open_to_open":
            raise ValueError(f"Unsupported return_basis: {self.target.return_basis}")

    def _entry_offset_days(self) -> int:
        return 1

    def _exit_offset_days(self) -> int:
        return self.target.horizon_days

    def _resolved_residual_components(self, observation: TargetObservation) -> dict[str, float]:
        if observation.risk_decomposition is not None:
            return dict(observation.risk_decomposition.components)
        return dict(observation.residual_components)

    def _check_eligibility(self, observation: TargetObservation) -> list[str]:
        return self._check_leg_eligibility(
            leg_state=observation.entry_state,
            leg_label="entry",
        ) + self._check_leg_eligibility(
            leg_state=observation.exit_state,
            leg_label="exit",
        )

    def _check_leg_eligibility(
        self,
        *,
        leg_state: TradeLegState,
        leg_label: str,
    ) -> list[str]:
        reasons: list[str] = []

        for rule in self.target.eligibility:
            if rule == "not_suspended":
                if leg_state.suspended:
                    reasons.append(f"{leg_label}_suspended")
                continue

            if rule == "not_limit_locked":
                if leg_state.limit_locked:
                    reasons.append(f"{leg_label}_limit_locked")
                continue

            if rule == "liquidity_pass":
                if not leg_state.liquidity_pass:
                    reasons.append(f"{leg_label}_liquidity_fail")
                continue

            raise ValueError(f"Unsupported eligibility rule: {rule}")

        return reasons
