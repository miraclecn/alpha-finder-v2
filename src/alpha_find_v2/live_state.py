from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any

from .config_loader import PROJECT_ROOT
from .deployment import PortfolioHolding, PortfolioState


JsonMap = dict[str, Any]
_WEIGHT_TOLERANCE = 1e-9


@dataclass(slots=True)
class BenchmarkConstituent:
    asset_id: str
    weight: float
    industry: str = ""


@dataclass(slots=True)
class BenchmarkStateStep:
    trade_date: str
    effective_at: str = ""
    available_at: str = ""
    industry_weights: dict[str, float] = field(default_factory=dict)
    constituents: list[BenchmarkConstituent] = field(default_factory=list)


@dataclass(slots=True)
class BenchmarkStateArtifact:
    benchmark_id: str
    classification: str
    steps: list[BenchmarkStateStep] = field(default_factory=list)

    def step_for_date(self, trade_date: str) -> BenchmarkStateStep:
        for step in self.steps:
            if step.trade_date == trade_date:
                return step
        raise ValueError(
            f"Benchmark state history {self.benchmark_id} must cover trade date {trade_date}"
        )

    def weights_by_date(self) -> dict[str, dict[str, float]]:
        return {
            step.trade_date: dict(step.industry_weights)
            for step in self.steps
        }


@dataclass(slots=True)
class AccountPosition:
    asset_id: str
    shares: float
    available_shares: float
    market_value_cny: float


@dataclass(slots=True)
class AccountTradeRestriction:
    asset_id: str
    can_enter: bool = True
    can_exit: bool = True
    reason: str = ""


@dataclass(slots=True)
class AccountStateSnapshot:
    account_id: str
    as_of_date: str
    total_equity_cny: float
    cash_balance_cny: float
    available_cash_cny: float
    positions: list[AccountPosition] = field(default_factory=list)
    trade_restrictions: list[AccountTradeRestriction] = field(default_factory=list)

    def restriction_map(self) -> dict[str, AccountTradeRestriction]:
        return {
            restriction.asset_id: restriction
            for restriction in self.trade_restrictions
        }


def load_benchmark_state_artifact(path: Path | str) -> BenchmarkStateArtifact:
    payload = _read_json(path)
    schema_version = int(payload.get("schema_version", 0))
    if schema_version != 1:
        raise ValueError(f"Unsupported benchmark state schema version: {schema_version}")

    artifact_type = str(payload.get("artifact_type", ""))
    if artifact_type == "benchmark_state_history":
        steps = [
            BenchmarkStateStep(
                trade_date=str(item["trade_date"]),
                effective_at=str(item.get("effective_at", "")),
                available_at=str(item.get("available_at", "")),
                industry_weights={
                    str(industry): float(weight)
                    for industry, weight in dict(item.get("industry_weights", {})).items()
                },
                constituents=[
                    BenchmarkConstituent(
                        asset_id=str(constituent["asset_id"]),
                        weight=float(constituent["weight"]),
                        industry=str(constituent.get("industry", "")),
                    )
                    for constituent in item.get("constituents", [])
                ],
            )
            for item in payload.get("steps", [])
        ]
        artifact = BenchmarkStateArtifact(
            benchmark_id=str(payload["benchmark_id"]),
            classification=str(payload["classification"]),
            steps=steps,
        )
        _validate_benchmark_state_artifact(artifact)
        return artifact

    if artifact_type == "benchmark_industry_weights":
        artifact = BenchmarkStateArtifact(
            benchmark_id=str(payload["benchmark"]),
            classification=str(payload["classification"]),
            steps=[
                BenchmarkStateStep(
                    trade_date=str(trade_date),
                    industry_weights={
                        str(industry): float(weight)
                        for industry, weight in dict(weights).items()
                    },
                )
                for trade_date, weights in dict(payload.get("weights_by_date", {})).items()
            ],
        )
        _validate_benchmark_state_artifact(artifact)
        return artifact

    raise ValueError(f"Unsupported benchmark state type: {artifact_type}")


def load_account_state_snapshot(path: Path | str) -> AccountStateSnapshot:
    payload = _read_json(path)
    schema_version = int(payload.get("schema_version", 0))
    if schema_version != 1:
        raise ValueError(f"Unsupported account state schema version: {schema_version}")

    artifact_type = str(payload.get("artifact_type", ""))
    if artifact_type != "account_state_snapshot":
        raise ValueError(f"Unsupported account state type: {artifact_type}")

    state = AccountStateSnapshot(
        account_id=str(payload["account_id"]),
        as_of_date=str(payload["as_of_date"]),
        total_equity_cny=float(payload["total_equity_cny"]),
        cash_balance_cny=float(payload.get("cash_balance_cny", 0.0)),
        available_cash_cny=float(payload.get("available_cash_cny", 0.0)),
        positions=[
            AccountPosition(
                asset_id=str(item["asset_id"]),
                shares=float(item.get("shares", 0.0)),
                available_shares=float(item.get("available_shares", item.get("shares", 0.0))),
                market_value_cny=float(item["market_value_cny"]),
            )
            for item in payload.get("positions", [])
        ],
        trade_restrictions=[
            AccountTradeRestriction(
                asset_id=str(item["asset_id"]),
                can_enter=bool(item.get("can_enter", True)),
                can_exit=bool(item.get("can_exit", True)),
                reason=str(item.get("reason", "")),
            )
            for item in payload.get("trade_restrictions", [])
        ],
    )
    _validate_account_state_snapshot(state)
    return state


def account_state_to_portfolio_state(
    *,
    portfolio_id: str,
    account_state: AccountStateSnapshot,
) -> PortfolioState:
    total_equity_cny = account_state.total_equity_cny
    holdings = [
        PortfolioHolding(
            asset_id=position.asset_id,
            weight=position.market_value_cny / total_equity_cny,
        )
        for position in account_state.positions
        if position.market_value_cny > 0.0
    ]

    restrictions = account_state.restriction_map()
    blocked_entries = sorted(
        asset_id
        for asset_id, restriction in restrictions.items()
        if not restriction.can_enter
    )
    blocked_exits = {
        asset_id
        for asset_id, restriction in restrictions.items()
        if not restriction.can_exit
    }
    for position in account_state.positions:
        if position.shares > 0.0 and position.available_shares + _WEIGHT_TOLERANCE < position.shares:
            blocked_exits.add(position.asset_id)

    state = PortfolioState(
        portfolio_id=portfolio_id,
        account_id=account_state.account_id,
        as_of_date=account_state.as_of_date,
        cash_weight=account_state.cash_balance_cny / total_equity_cny,
        holdings=holdings,
        blocked_entry_assets=blocked_entries,
        blocked_exit_assets=sorted(blocked_exits),
    )
    _validate_portfolio_state(state)
    return state


def _validate_benchmark_state_artifact(artifact: BenchmarkStateArtifact) -> None:
    seen_dates: set[str] = set()
    for step in artifact.steps:
        if step.trade_date in seen_dates:
            raise ValueError(
                f"Duplicate benchmark state trade date: {artifact.benchmark_id} {step.trade_date}"
            )
        seen_dates.add(step.trade_date)
        _validate_benchmark_state_step(step)


def _validate_benchmark_state_step(step: BenchmarkStateStep) -> None:
    total_industry_weight = sum(step.industry_weights.values())
    if any(weight < 0.0 for weight in step.industry_weights.values()):
        raise ValueError(
            f"Benchmark industry weights cannot be negative on {step.trade_date}"
        )
    if total_industry_weight > 1.0 + _WEIGHT_TOLERANCE:
        raise ValueError(
            f"Benchmark industry weights cannot exceed 100% on {step.trade_date}"
        )

    seen_assets: set[str] = set()
    total_constituent_weight = 0.0
    aggregated_industry_weights: dict[str, float] = {}
    for constituent in step.constituents:
        if constituent.asset_id in seen_assets:
            raise ValueError(
                f"Duplicate benchmark constituent on {step.trade_date}: {constituent.asset_id}"
            )
        if constituent.weight < 0.0:
            raise ValueError(
                f"Benchmark constituent weight cannot be negative: {constituent.asset_id}"
            )
        seen_assets.add(constituent.asset_id)
        total_constituent_weight += constituent.weight
        if constituent.industry:
            aggregated_industry_weights[constituent.industry] = (
                aggregated_industry_weights.get(constituent.industry, 0.0)
                + constituent.weight
            )

    if total_constituent_weight > 1.0 + _WEIGHT_TOLERANCE:
        raise ValueError(
            f"Benchmark constituent weights cannot exceed 100% on {step.trade_date}"
        )

    if step.industry_weights and aggregated_industry_weights:
        industries = set(step.industry_weights) | set(aggregated_industry_weights)
        for industry in industries:
            if abs(
                step.industry_weights.get(industry, 0.0)
                - aggregated_industry_weights.get(industry, 0.0)
            ) > 1e-6:
                raise ValueError(
                    f"Benchmark constituent industry weights do not match summary weights on {step.trade_date}"
                )


def _validate_account_state_snapshot(state: AccountStateSnapshot) -> None:
    if state.total_equity_cny <= 0.0:
        raise ValueError("Account state total_equity_cny must be positive.")
    if state.cash_balance_cny < 0.0:
        raise ValueError("Account state cash_balance_cny cannot be negative.")
    if state.available_cash_cny < 0.0:
        raise ValueError("Account state available_cash_cny cannot be negative.")
    if state.available_cash_cny > state.cash_balance_cny + _WEIGHT_TOLERANCE:
        raise ValueError("Account state available_cash_cny cannot exceed cash_balance_cny.")

    seen_assets: set[str] = set()
    total_position_value = 0.0
    for position in state.positions:
        if position.asset_id in seen_assets:
            raise ValueError(f"Duplicate account position: {position.asset_id}")
        if position.shares < 0.0:
            raise ValueError(f"Account position shares cannot be negative: {position.asset_id}")
        if position.available_shares < 0.0:
            raise ValueError(
                f"Account position available_shares cannot be negative: {position.asset_id}"
            )
        if position.available_shares > position.shares + _WEIGHT_TOLERANCE:
            raise ValueError(
                f"Account position available_shares cannot exceed shares: {position.asset_id}"
            )
        if position.market_value_cny < 0.0:
            raise ValueError(
                f"Account position market_value_cny cannot be negative: {position.asset_id}"
            )
        seen_assets.add(position.asset_id)
        total_position_value += position.market_value_cny

    restriction_assets: set[str] = set()
    held_assets = {position.asset_id for position in state.positions if position.market_value_cny > 0.0}
    for restriction in state.trade_restrictions:
        if restriction.asset_id in restriction_assets:
            raise ValueError(f"Duplicate account trade restriction: {restriction.asset_id}")
        restriction_assets.add(restriction.asset_id)
        if not restriction.can_exit and restriction.asset_id not in held_assets:
            raise ValueError(
                "Account trade restrictions cannot block exits for non-held assets: "
                + restriction.asset_id
            )

    if total_position_value + state.cash_balance_cny > state.total_equity_cny + _WEIGHT_TOLERANCE:
        raise ValueError("Account positions plus cash cannot exceed total_equity_cny.")


def _validate_portfolio_state(state: PortfolioState) -> None:
    total_weight = state.cash_weight + sum(holding.weight for holding in state.holdings)
    if state.cash_weight < 0.0:
        raise ValueError("Portfolio state cash_weight cannot be negative.")
    if total_weight > 1.0 + _WEIGHT_TOLERANCE:
        raise ValueError("Portfolio state holdings plus cash cannot exceed 100%.")

    seen_assets: set[str] = set()
    for holding in state.holdings:
        if holding.asset_id in seen_assets:
            raise ValueError(f"Duplicate portfolio holding: {holding.asset_id}")
        if holding.weight < 0.0:
            raise ValueError(f"Portfolio holding weight cannot be negative: {holding.asset_id}")
        seen_assets.add(holding.asset_id)

    holding_assets = {holding.asset_id for holding in state.holdings if holding.weight > 0.0}
    missing_blocked_exits = sorted(set(state.blocked_exit_assets) - holding_assets)
    if missing_blocked_exits:
        raise ValueError(
            "Portfolio state blocked_exit_assets must be current holdings: "
            + ", ".join(missing_blocked_exits)
        )


def _resolve_project_path(path: Path | str) -> Path:
    target = Path(path)
    if target.is_absolute():
        return target
    return PROJECT_ROOT / target


def _read_json(path: Path | str) -> JsonMap:
    target = _resolve_project_path(path)
    with target.open("r", encoding="utf-8") as handle:
        return json.load(handle)
