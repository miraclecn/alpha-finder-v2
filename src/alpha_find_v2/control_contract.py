from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from .models import ExecutionPolicy, Mandate, PortfolioConstructionModel, PortfolioRecipe, Sleeve


SUPPORTED_STATUSES = {"enforced", "report_only", "planned", "unsupported"}


class ControlContractError(ValueError):
    pass


@dataclass(slots=True)
class ControlContractItem:
    control: str
    status: str
    implemented: bool
    source: str
    evidence: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ControlContractReport:
    items: list[ControlContractItem] = field(default_factory=list)

    def status_for(self, control: str) -> str:
        for item in self.items:
            if item.control == control:
                return item.status
        raise KeyError(f"Unknown control contract item: {control}")


def build_control_contract_report(
    *,
    mandate: Mandate,
    portfolio: PortfolioRecipe,
    construction_model: PortfolioConstructionModel,
    execution_policy: ExecutionPolicy,
    sleeves: Iterable[Sleeve],
) -> ControlContractReport:
    sleeve_list = list(sleeves)
    items = [
        _industry_contract(
            mandate=mandate,
            construction_model=construction_model,
            sleeves=sleeve_list,
        ),
        _unsupported_style_contract(mandate, "size", "size_control"),
        _unsupported_style_contract(mandate, "beta", "beta_band"),
        _turnover_penalty_contract(portfolio),
        _min_trade_weight_contract(execution_policy),
    ]
    return ControlContractReport(items=items)


def assert_no_false_enforced_controls(report: ControlContractReport) -> None:
    false_enforced = [
        item.control
        for item in report.items
        if item.status == "enforced" and not item.implemented
    ]
    if false_enforced:
        raise ControlContractError(
            "Controls declared enforced without an implementation path: "
            + ", ".join(false_enforced)
        )


def _industry_contract(
    *,
    mandate: Mandate,
    construction_model: PortfolioConstructionModel,
    sleeves: list[Sleeve],
) -> ControlContractItem:
    evidence: list[str] = []
    if construction_model.industry_budget_mode == "benchmark_relative":
        evidence.append("portfolio_construction:benchmark_relative_industry_caps")
    if any("industry" in sleeve.neutralization for sleeve in sleeves):
        evidence.append("sleeve:industry_neutralization_declared")
    status = _declared_status(
        mandate.risk.get("industry_neutral"),
        default_enabled_status="enforced",
        default_disabled_status="unsupported",
    )
    return ControlContractItem(
        control="industry",
        status=status,
        implemented=bool(evidence),
        source="mandate.risk.industry_neutral",
        evidence=evidence,
    )


def _unsupported_style_contract(
    mandate: Mandate,
    control: str,
    risk_key: str,
) -> ControlContractItem:
    value = mandate.risk.get(risk_key)
    status = _declared_status(
        value,
        default_enabled_status="enforced",
        default_disabled_status="unsupported",
    )
    return ControlContractItem(
        control=control,
        status=status,
        implemented=False,
        source=f"mandate.risk.{risk_key}",
        evidence=[],
    )


def _turnover_penalty_contract(portfolio: PortfolioRecipe) -> ControlContractItem:
    value = portfolio.constraints.get("turnover_penalty")
    if isinstance(value, dict):
        status = _declared_status(value, default_enabled_status="report_only")
    elif value == "enabled":
        status = "report_only"
    elif value:
        status = "unsupported"
    else:
        status = "unsupported"
    return ControlContractItem(
        control="turnover_penalty",
        status=status,
        implemented=False,
        source="portfolio.constraints.turnover_penalty",
        evidence=[],
    )


def _min_trade_weight_contract(execution_policy: ExecutionPolicy) -> ControlContractItem:
    status = "planned" if execution_policy.min_trade_weight > 0.0 else "unsupported"
    return ControlContractItem(
        control="min_trade_weight",
        status=status,
        implemented=False,
        source="execution_policy.min_trade_weight",
        evidence=[],
    )


def _declared_status(
    value: object,
    *,
    default_enabled_status: str,
    default_disabled_status: str = "unsupported",
) -> str:
    if isinstance(value, dict):
        status = str(value.get("status", default_disabled_status))
    elif isinstance(value, bool):
        status = default_enabled_status if value else default_disabled_status
    elif value:
        status = default_enabled_status
    else:
        status = default_disabled_status
    if status not in SUPPORTED_STATUSES:
        raise ControlContractError(f"Unsupported control status: {status}")
    return status
