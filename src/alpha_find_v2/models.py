from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


JsonMap = dict[str, Any]


@dataclass(slots=True)
class Mandate:
    id: str
    name: str
    market: str
    benchmark: str
    account_type: str
    description: str
    allowed_instruments: list[str] = field(default_factory=list)
    rebalance_days_per_week: int = 1
    holding_count_min: int = 0
    holding_count_max: int = 0
    max_single_name_weight: float = 0.0
    max_turnover_per_rebalance: float = 0.0
    trade_timing: str = ""
    execution_participation_cap: float = 0.0
    filters: JsonMap = field(default_factory=dict)
    risk: JsonMap = field(default_factory=dict)

    @classmethod
    def from_toml(cls, data: JsonMap) -> "Mandate":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            market=str(data["market"]),
            benchmark=str(data["benchmark"]),
            account_type=str(data["account_type"]),
            description=str(data["description"]),
            allowed_instruments=list(data.get("allowed_instruments", [])),
            rebalance_days_per_week=int(data.get("rebalance_days_per_week", 1)),
            holding_count_min=int(data.get("holding_count_min", 0)),
            holding_count_max=int(data.get("holding_count_max", 0)),
            max_single_name_weight=float(data.get("max_single_name_weight", 0.0)),
            max_turnover_per_rebalance=float(data.get("max_turnover_per_rebalance", 0.0)),
            trade_timing=str(data.get("trade_timing", "")),
            execution_participation_cap=float(data.get("execution_participation_cap", 0.0)),
            filters=dict(data.get("filters", {})),
            risk=dict(data.get("risk", {})),
        )


@dataclass(slots=True)
class Thesis:
    id: str
    name: str
    family: str
    mechanism: str
    why_a_share: str
    expected_sign: str
    expected_horizon_days: list[int] = field(default_factory=list)
    required_data: list[str] = field(default_factory=list)
    regime_dependencies: list[str] = field(default_factory=list)
    failure_modes: list[str] = field(default_factory=list)
    falsification_rules: list[str] = field(default_factory=list)
    portfolio_role: str = ""
    validation: JsonMap = field(default_factory=dict)

    @classmethod
    def from_toml(cls, data: JsonMap) -> "Thesis":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            family=str(data["family"]),
            mechanism=str(data["mechanism"]),
            why_a_share=str(data["why_a_share"]),
            expected_sign=str(data["expected_sign"]),
            expected_horizon_days=[int(value) for value in data.get("expected_horizon_days", [])],
            required_data=list(data.get("required_data", [])),
            regime_dependencies=list(data.get("regime_dependencies", [])),
            failure_modes=list(data.get("failure_modes", [])),
            falsification_rules=list(data.get("falsification_rules", [])),
            portfolio_role=str(data.get("portfolio_role", "")),
            validation=dict(data.get("validation", {})),
        )


@dataclass(slots=True)
class Descriptor:
    id: str
    name: str
    category: str
    signal_direction: str
    description: str
    required_data: list[str] = field(default_factory=list)
    preferred_horizon_days: list[int] = field(default_factory=list)
    normalization: list[str] = field(default_factory=list)
    point_in_time_rule: str = ""
    failure_modes: list[str] = field(default_factory=list)

    @classmethod
    def from_toml(cls, data: JsonMap) -> "Descriptor":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            category=str(data["category"]),
            signal_direction=str(data["signal_direction"]),
            description=str(data["description"]),
            required_data=list(data.get("required_data", [])),
            preferred_horizon_days=[int(value) for value in data.get("preferred_horizon_days", [])],
            normalization=list(data.get("normalization", [])),
            point_in_time_rule=str(data.get("point_in_time_rule", "")),
            failure_modes=list(data.get("failure_modes", [])),
        )


@dataclass(slots=True)
class DescriptorComponent:
    descriptor_id: str
    role: str
    weight: float
    transform: str

    @classmethod
    def from_toml(cls, data: JsonMap) -> "DescriptorComponent":
        return cls(
            descriptor_id=str(data["descriptor_id"]),
            role=str(data["role"]),
            weight=float(data["weight"]),
            transform=str(data["transform"]),
        )


@dataclass(slots=True)
class DescriptorSet:
    id: str
    name: str
    thesis_id: str
    target_id: str
    required_data: list[str] = field(default_factory=list)
    selection_logic: str = ""
    components: list[DescriptorComponent] = field(default_factory=list)

    @classmethod
    def from_toml(cls, data: JsonMap) -> "DescriptorSet":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            thesis_id=str(data["thesis_id"]),
            target_id=str(data["target_id"]),
            required_data=list(data.get("required_data", [])),
            selection_logic=str(data.get("selection_logic", "")),
            components=[
                DescriptorComponent.from_toml(component)
                for component in data.get("components", [])
            ],
        )


@dataclass(slots=True)
class ResidualTarget:
    id: str
    name: str
    description: str
    horizon_days: int
    signal_observation: str
    trade_entry: str
    trade_exit: str
    return_basis: str
    cost_model: str
    label_kind: str = "residual_net_return"
    risk_model_id: str = ""
    residualization: list[str] = field(default_factory=list)
    eligibility: list[str] = field(default_factory=list)

    @classmethod
    def from_toml(cls, data: JsonMap) -> "ResidualTarget":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            description=str(data["description"]),
            horizon_days=int(data["horizon_days"]),
            signal_observation=str(data.get("signal_observation", "")),
            trade_entry=str(data["trade_entry"]),
            trade_exit=str(data["trade_exit"]),
            return_basis=str(data["return_basis"]),
            cost_model=str(data["cost_model"]),
            label_kind=str(data.get("label_kind", "residual_net_return")),
            risk_model_id=str(data.get("risk_model_id", "")),
            residualization=list(data.get("residualization", [])),
            eligibility=list(data.get("eligibility", [])),
        )


@dataclass(slots=True)
class CostModel:
    id: str
    name: str
    description: str
    buy_commission_bps: float
    sell_commission_bps: float
    buy_slippage_bps: float
    sell_slippage_bps: float
    sell_stamp_duty_bps: float = 0.0
    participation_cap: float = 0.0
    min_median_daily_turnover_cny_mn: float = 0.0

    @classmethod
    def from_toml(cls, data: JsonMap) -> "CostModel":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            description=str(data["description"]),
            buy_commission_bps=float(data["buy_commission_bps"]),
            sell_commission_bps=float(data["sell_commission_bps"]),
            buy_slippage_bps=float(data["buy_slippage_bps"]),
            sell_slippage_bps=float(data["sell_slippage_bps"]),
            sell_stamp_duty_bps=float(data.get("sell_stamp_duty_bps", 0.0)),
            participation_cap=float(data.get("participation_cap", 0.0)),
            min_median_daily_turnover_cny_mn=float(
                data.get("min_median_daily_turnover_cny_mn", 0.0)
            ),
        )

    def buy_total_bps(self) -> float:
        return self.buy_commission_bps + self.buy_slippage_bps

    def sell_total_bps(self) -> float:
        return self.sell_commission_bps + self.sell_slippage_bps + self.sell_stamp_duty_bps

    def round_trip_bps(self) -> float:
        return self.buy_total_bps() + self.sell_total_bps()


@dataclass(slots=True)
class ExecutionPolicy:
    id: str
    name: str
    description: str
    trade_timing: str
    order_basis: str
    blocked_trade_policy: str
    cash_policy: str
    participation_cap_source: str = ""
    lot_size: int = 0
    min_trade_weight: float = 0.0

    @classmethod
    def from_toml(cls, data: JsonMap) -> "ExecutionPolicy":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            description=str(data["description"]),
            trade_timing=str(data["trade_timing"]),
            order_basis=str(data["order_basis"]),
            blocked_trade_policy=str(data["blocked_trade_policy"]),
            cash_policy=str(data["cash_policy"]),
            participation_cap_source=str(data.get("participation_cap_source", "")),
            lot_size=int(data.get("lot_size", 0)),
            min_trade_weight=float(data.get("min_trade_weight", 0.0)),
        )


@dataclass(slots=True)
class RiskFactor:
    id: str
    kind: str
    component: str
    description: str
    dynamic: bool = False
    exposure_key_prefix: str = ""

    @classmethod
    def from_toml(cls, data: JsonMap) -> "RiskFactor":
        factor_id = str(data["id"])
        return cls(
            id=factor_id,
            kind=str(data["kind"]),
            component=str(data.get("component", factor_id)),
            description=str(data["description"]),
            dynamic=bool(data.get("dynamic", False)),
            exposure_key_prefix=str(data.get("exposure_key_prefix", factor_id)),
        )


@dataclass(slots=True)
class RiskModel:
    id: str
    name: str
    description: str
    benchmark: str
    factor_return_source: str
    minimum_coverage: int = 0
    residual_components: list[str] = field(default_factory=list)
    factors: list[RiskFactor] = field(default_factory=list)

    @classmethod
    def from_toml(cls, data: JsonMap) -> "RiskModel":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            description=str(data["description"]),
            benchmark=str(data["benchmark"]),
            factor_return_source=str(data.get("factor_return_source", "")),
            minimum_coverage=int(data.get("minimum_coverage", 0)),
            residual_components=list(data.get("residual_components", [])),
            factors=[RiskFactor.from_toml(item) for item in data.get("factors", [])],
        )

    def factor_for_key(self, exposure_key: str) -> RiskFactor | None:
        for factor in self.factors:
            prefix = factor.exposure_key_prefix or factor.id
            if factor.dynamic:
                if exposure_key == prefix or exposure_key.startswith(f"{prefix}:"):
                    return factor
                continue
            if exposure_key == prefix or exposure_key == factor.id:
                return factor
        return None


@dataclass(slots=True)
class PromotionGate:
    id: str
    name: str
    scope: str
    description: str
    minimum_oos_ir: float
    minimum_oos_tstat: float
    minimum_breadth: int
    max_peak_to_trough_drawdown: float
    cost_scenarios: list[str] = field(default_factory=list)
    regime_requirements: list[str] = field(default_factory=list)
    correlation_limits: JsonMap = field(default_factory=dict)
    turnover_limits: JsonMap = field(default_factory=dict)
    portfolio_contribution: JsonMap = field(default_factory=dict)

    @classmethod
    def from_toml(cls, data: JsonMap) -> "PromotionGate":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            scope=str(data["scope"]),
            description=str(data["description"]),
            minimum_oos_ir=float(data["minimum_oos_ir"]),
            minimum_oos_tstat=float(data["minimum_oos_tstat"]),
            minimum_breadth=int(data["minimum_breadth"]),
            max_peak_to_trough_drawdown=float(data["max_peak_to_trough_drawdown"]),
            cost_scenarios=list(data.get("cost_scenarios", [])),
            regime_requirements=list(data.get("regime_requirements", [])),
            correlation_limits=dict(data.get("correlation_limits", {})),
            turnover_limits=dict(data.get("turnover_limits", {})),
            portfolio_contribution=dict(data.get("portfolio_contribution", {})),
        )


@dataclass(slots=True)
class Sleeve:
    id: str
    name: str
    mandate_id: str
    thesis_id: str
    universe: str
    rebalance_frequency: str
    target_holding_days: int
    turnover_budget: float
    execution_rule: str
    descriptor_set_id: str = ""
    target_id: str = ""
    neutralization: list[str] = field(default_factory=list)
    construction: JsonMap = field(default_factory=dict)
    constraints: JsonMap = field(default_factory=dict)

    @classmethod
    def from_toml(cls, data: JsonMap) -> "Sleeve":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            mandate_id=str(data["mandate_id"]),
            thesis_id=str(data["thesis_id"]),
            descriptor_set_id=str(data.get("descriptor_set_id", "")),
            target_id=str(data.get("target_id", "")),
            universe=str(data["universe"]),
            rebalance_frequency=str(data["rebalance_frequency"]),
            target_holding_days=int(data["target_holding_days"]),
            turnover_budget=float(data["turnover_budget"]),
            execution_rule=str(data["execution_rule"]),
            neutralization=list(data.get("neutralization", [])),
            construction=dict(data.get("construction", {})),
            constraints=dict(data.get("constraints", {})),
        )


@dataclass(slots=True)
class PortfolioConstructionModel:
    id: str
    name: str
    description: str
    sleeve_weight_source: str
    overlap_mode: str
    name_selection: str
    excess_weight_policy: str
    industry_budget_mode: str = ""

    @classmethod
    def from_toml(cls, data: JsonMap) -> "PortfolioConstructionModel":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            description=str(data["description"]),
            sleeve_weight_source=str(data["sleeve_weight_source"]),
            overlap_mode=str(data["overlap_mode"]),
            name_selection=str(data["name_selection"]),
            excess_weight_policy=str(data["excess_weight_policy"]),
            industry_budget_mode=str(data.get("industry_budget_mode", "")),
        )


@dataclass(slots=True)
class PortfolioRecipe:
    id: str
    name: str
    mandate_id: str
    benchmark: str
    rebalance_policy: str
    description: str
    construction_model_id: str = ""
    promotion_gate_id: str = ""
    execution_policy_id: str = ""
    decay_monitor_id: str = ""
    sleeves: list[str] = field(default_factory=list)
    allocation: dict[str, float] = field(default_factory=dict)
    constraints: JsonMap = field(default_factory=dict)

    @classmethod
    def from_toml(cls, data: JsonMap) -> "PortfolioRecipe":
        allocation = {
            str(key): float(value)
            for key, value in dict(data.get("allocation", {})).items()
        }
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            mandate_id=str(data["mandate_id"]),
            benchmark=str(data["benchmark"]),
            rebalance_policy=str(data["rebalance_policy"]),
            description=str(data["description"]),
            construction_model_id=str(data.get("construction_model_id", "")),
            promotion_gate_id=str(data.get("promotion_gate_id", "")),
            execution_policy_id=str(data.get("execution_policy_id", "")),
            decay_monitor_id=str(data.get("decay_monitor_id", "")),
            sleeves=list(data.get("sleeves", [])),
            allocation=allocation,
            constraints=dict(data.get("constraints", {})),
        )


@dataclass(slots=True)
class DecayMonitor:
    id: str
    name: str
    description: str
    comparison_mode: str
    observation_windows: list[int] = field(default_factory=list)
    warning_thresholds: JsonMap = field(default_factory=dict)
    retirement_thresholds: JsonMap = field(default_factory=dict)

    @classmethod
    def from_toml(cls, data: JsonMap) -> "DecayMonitor":
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            description=str(data["description"]),
            comparison_mode=str(data["comparison_mode"]),
            observation_windows=[int(value) for value in data.get("observation_windows", [])],
            warning_thresholds=dict(data.get("warning_thresholds", {})),
            retirement_thresholds=dict(data.get("retirement_thresholds", {})),
        )
