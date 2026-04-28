from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
import math
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP
import statistics
import tomllib
from typing import Any

from .config_loader import (
    CONFIG_ROOT,
    PROJECT_ROOT,
    load_cost_model,
    load_execution_policy,
    load_mandate,
    load_portfolio,
    load_portfolio_construction_model,
    load_regime_overlay,
)
from .live_state import BenchmarkStateArtifact, load_benchmark_state_artifact
from .models import (
    CostModel,
    ExecutionPolicy,
    Mandate,
    PortfolioConstructionModel,
    PortfolioRecipe,
    RegimeOverlay,
)
from .portfolio_constructor import (
    PortfolioConstructionInput,
    PortfolioConstructionResult,
    PortfolioConstructionStep,
    PortfolioConstructor,
)
from .portfolio_promotion_replay import (
    SleeveResearchArtifact,
    SleeveResearchStep,
)
from .portfolio_simulator import PortfolioSecuritySignal, TradeConstraintState
from .regime_overlay import (
    RegimeOverlayDecision,
    RegimeOverlayEvaluator,
    RegimeOverlayObservationArtifact,
    load_regime_overlay_observation_artifact,
)
from .research_artifact_loader import load_sleeve_artifact


JsonMap = dict[str, Any]


@dataclass(slots=True)
class PortfolioBacktestCaseDefinition:
    case_id: str
    description: str
    portfolio_path: str
    execution_policy_path: str
    default_cost_model_path: str
    output_path: str
    artifact_paths: list[str] = field(default_factory=list)
    additional_cost_model_paths: list[str] = field(default_factory=list)
    source_db_path: str = "output/research_source.duckdb"
    start_date: str = ""
    end_date: str = ""
    initial_cash_cny: float = 10_000_000.0
    risk_free_rate_annual: float = 0.0
    benchmark_state_path: str = ""
    regime_overlay_observation_path: str = ""

    @classmethod
    def from_toml(cls, data: JsonMap) -> "PortfolioBacktestCaseDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(
                f"Unsupported portfolio backtest case schema version: {schema_version}"
            )
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "portfolio_backtest_case":
            raise ValueError(f"Unsupported portfolio backtest case type: {artifact_type}")
        return cls(
            case_id=str(data["case_id"]),
            description=str(data["description"]),
            portfolio_path=str(data["portfolio_path"]),
            execution_policy_path=str(data["execution_policy_path"]),
            default_cost_model_path=str(data["default_cost_model_path"]),
            output_path=str(data["output_path"]),
            artifact_paths=[str(path) for path in data.get("artifact_paths", [])],
            additional_cost_model_paths=[
                str(path) for path in data.get("additional_cost_model_paths", [])
            ],
            source_db_path=str(data.get("source_db_path", "output/research_source.duckdb")),
            start_date=str(data["start_date"]),
            end_date=str(data["end_date"]),
            initial_cash_cny=float(data.get("initial_cash_cny", 10_000_000.0)),
            risk_free_rate_annual=float(data.get("risk_free_rate_annual", 0.0)),
            benchmark_state_path=str(data.get("benchmark_state_path", "")),
            regime_overlay_observation_path=str(
                data.get("regime_overlay_observation_path", "")
            ),
        )


@dataclass(slots=True)
class LoadedPortfolioBacktestCase:
    definition: PortfolioBacktestCaseDefinition
    mandate: Mandate
    portfolio: PortfolioRecipe
    construction_model: PortfolioConstructionModel
    execution_policy: ExecutionPolicy
    default_cost_model: CostModel
    source_db_path: Path
    artifacts: list[SleeveResearchArtifact] = field(default_factory=list)
    cost_models: dict[str, CostModel] = field(default_factory=dict)
    benchmark_state_artifact: BenchmarkStateArtifact | None = None
    regime_overlay: RegimeOverlay | None = None
    regime_overlay_observations: RegimeOverlayObservationArtifact | None = None


@dataclass(slots=True)
class PortfolioBacktestInput:
    portfolio: PortfolioRecipe
    artifacts: list[SleeveResearchArtifact]
    start_date: str
    end_date: str
    initial_cash_cny: float = 10_000_000.0
    risk_free_rate_annual: float = 0.0
    benchmark_industry_weights_by_date: dict[str, dict[str, float]] = field(
        default_factory=dict
    )
    benchmark_constituent_weights_by_date: dict[str, dict[str, float]] = field(
        default_factory=dict
    )
    regime_overlay: RegimeOverlay | None = None
    regime_overlay_observations: list = field(default_factory=list)


@dataclass(slots=True)
class DailyBar:
    security_id: str
    trade_date: str
    price_basis: str
    board: str
    is_st: bool
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    pre_close: float | None
    adj_factor: float | None
    turnover_value_cny: float | None
    raw_open: float | None
    raw_high: float | None
    raw_low: float | None
    raw_close: float | None
    raw_pre_close: float | None


@dataclass(slots=True)
class Position:
    asset_id: str
    shares: float
    last_price: float
    adj_factor: float | None = None
    cost_model_id: str = ""
    price_basis: str = "unadjusted"

    def value(self) -> float:
        return self.shares * self.last_price


@dataclass(slots=True)
class PortfolioOrder:
    order_id: str
    decision_date: str
    execution_date: str
    asset_id: str
    side: str
    requested_quantity: float
    target_weight: float
    reason: str = "rebalance"


@dataclass(slots=True)
class PortfolioFill:
    order_id: str
    decision_date: str
    execution_date: str
    asset_id: str
    side: str
    quantity: float
    price: float
    gross_value: float
    cost: float
    net_cash_flow: float
    cost_model_id: str
    participation_cap: float


@dataclass(slots=True)
class BlockedOrderDiagnostic:
    order_id: str
    decision_date: str
    execution_date: str
    asset_id: str
    side: str
    requested_quantity: float
    reason: str


@dataclass(slots=True)
class PartialFillDiagnostic:
    order_id: str
    decision_date: str
    execution_date: str
    asset_id: str
    side: str
    requested_quantity: float
    filled_quantity: float
    reason: str


@dataclass(slots=True)
class DailyHolding:
    trade_date: str
    asset_id: str
    shares: float
    mark_price: float
    market_value: float
    weight: float


@dataclass(slots=True)
class DailyPortfolioState:
    trade_date: str
    equity: float
    cash: float
    positions_value: float
    gross_exposure: float
    daily_return: float
    weights: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class PortfolioBacktestDiagnostics:
    blocked_orders: list[BlockedOrderDiagnostic] = field(default_factory=list)
    partial_fills: list[PartialFillDiagnostic] = field(default_factory=list)
    known_limitations: list[str] = field(
        default_factory=lambda: [
            "The V1 ledger is an adjusted-price research ledger: adjusted OHLC columns are used when available; dividend cash accounting and broker-literal corporate-action share/cash booking are out of scope."
        ]
    )


@dataclass(slots=True)
class PortfolioBacktestSummary:
    start_date: str
    end_date: str
    trading_days: int
    initial_cash_cny: float
    final_equity: float
    total_return: float
    cagr: float
    annualized_return: float
    annualized_volatility: float
    sharpe: float
    risk_free_rate_annual: float
    benchmark_annualized_return: float
    active_annualized_return: float
    tracking_error: float
    beta: float
    alpha: float
    information_ratio: float
    max_drawdown: float
    calmar: float
    win_rate: float
    turnover: float
    buy_turnover: float
    sell_turnover: float
    total_costs: float
    blocked_trade_share: float
    partial_fill_share: float
    yearly_returns: dict[str, float] = field(default_factory=dict)
    yearly_max_drawdown: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class PortfolioBacktestResult:
    daily_curve: list[DailyPortfolioState] = field(default_factory=list)
    daily_holdings: list[DailyHolding] = field(default_factory=list)
    orders: list[PortfolioOrder] = field(default_factory=list)
    fills: list[PortfolioFill] = field(default_factory=list)
    diagnostics: PortfolioBacktestDiagnostics = field(
        default_factory=PortfolioBacktestDiagnostics
    )
    summary: PortfolioBacktestSummary | None = None


class DuckDBMarketData:
    def __init__(self, source_db_path: Path | str) -> None:
        self.source_db_path = _resolve_project_path(source_db_path)

    def calendar(self) -> list[str]:
        import duckdb

        conn = duckdb.connect(str(self.source_db_path), read_only=True)
        try:
            rows = conn.execute(
                "SELECT trade_date FROM market_trade_calendar ORDER BY trade_date"
            ).fetchall()
        finally:
            conn.close()
        return [_normalize_date_key(str(row[0])) for row in rows]

    def bars(
        self,
        *,
        asset_ids: set[str],
        start_date: str,
        end_date: str,
    ) -> dict[tuple[str, str], DailyBar]:
        if not asset_ids:
            return {}

        import duckdb

        ordered_assets = sorted(asset_ids)
        placeholders = ", ".join("?" for _ in ordered_assets)
        parameters = [*ordered_assets, start_date, end_date]
        conn = duckdb.connect(str(self.source_db_path), read_only=True)
        try:
            columns = {str(row[1]) for row in conn.execute("PRAGMA table_info('daily_bar_pit')").fetchall()}
            price_basis_sql = (
                "price_basis"
                if "price_basis" in columns
                else "'unadjusted' AS price_basis"
            )
            open_sql = "COALESCE(open_adj, open) AS open" if "open_adj" in columns else "open"
            high_sql = "COALESCE(high_adj, high) AS high" if "high_adj" in columns else "high"
            low_sql = "COALESCE(low_adj, low) AS low" if "low_adj" in columns else "low"
            close_sql = (
                "COALESCE(close_adj, close) AS close"
                if "close_adj" in columns
                else "close"
            )
            rows = conn.execute(
                f"""
                SELECT
                    security_id,
                    trade_date,
                    {price_basis_sql},
                    board,
                    is_st,
                    {open_sql},
                    {high_sql},
                    {low_sql},
                    {close_sql},
                    pre_close,
                    adj_factor,
                    turnover_value_cny,
                    open AS raw_open,
                    high AS raw_high,
                    low AS raw_low,
                    close AS raw_close,
                    pre_close AS raw_pre_close
                FROM daily_bar_pit
                WHERE security_id IN ({placeholders})
                  AND trade_date >= ?
                  AND trade_date <= ?
                ORDER BY trade_date, security_id
                """,
                parameters,
            ).fetchall()
        finally:
            conn.close()

        return {
            (str(security_id), _normalize_date_key(str(trade_date))): DailyBar(
                security_id=str(security_id),
                trade_date=_normalize_date_key(str(trade_date)),
                price_basis=str(price_basis or "unadjusted"),
                board=str(board or "main_board"),
                is_st=bool(is_st),
                open=None if open_price is None else float(open_price),
                high=None if high_price is None else float(high_price),
                low=None if low_price is None else float(low_price),
                close=None if close_price is None else float(close_price),
                pre_close=None if pre_close is None else float(pre_close),
                adj_factor=None if adj_factor is None else float(adj_factor),
                turnover_value_cny=(
                    None if turnover_value_cny is None else float(turnover_value_cny)
                ),
                raw_open=None if raw_open is None else float(raw_open),
                raw_high=None if raw_high is None else float(raw_high),
                raw_low=None if raw_low is None else float(raw_low),
                raw_close=None if raw_close is None else float(raw_close),
                raw_pre_close=None if raw_pre_close is None else float(raw_pre_close),
            )
            for (
                security_id,
                trade_date,
                price_basis,
                board,
                is_st,
                open_price,
                high_price,
                low_price,
                close_price,
                pre_close,
                adj_factor,
                turnover_value_cny,
                raw_open,
                raw_high,
                raw_low,
                raw_close,
                raw_pre_close,
            ) in rows
        }


class PortfolioBacktester:
    def __init__(
        self,
        *,
        mandate: Mandate,
        portfolio: PortfolioRecipe,
        construction_model: PortfolioConstructionModel,
        execution_policy: ExecutionPolicy,
        default_cost_model: CostModel,
        source_db_path: Path | str,
        cost_models: dict[str, CostModel] | None = None,
    ) -> None:
        if execution_policy.trade_timing != "next_day_open":
            raise ValueError("PortfolioBacktester V1 supports only next_day_open execution.")
        self.mandate = mandate
        self.portfolio = portfolio
        self.construction_model = construction_model
        self.execution_policy = execution_policy
        self.default_cost_model = default_cost_model
        self.cost_models = {default_cost_model.id: default_cost_model}
        if cost_models:
            self.cost_models.update(cost_models)
        self.market_data = DuckDBMarketData(source_db_path)

    def run(self, backtest_input: PortfolioBacktestInput) -> PortfolioBacktestResult:
        start_date = _normalize_date_key(backtest_input.start_date)
        end_date = _normalize_date_key(backtest_input.end_date)
        if start_date > end_date:
            raise ValueError("Portfolio backtest start_date must be <= end_date.")
        if backtest_input.initial_cash_cny <= 0.0:
            raise ValueError("Portfolio backtest initial_cash_cny must be positive.")
        backtest_input.benchmark_industry_weights_by_date = _normalize_weights_by_date(
            backtest_input.benchmark_industry_weights_by_date
        )
        backtest_input.benchmark_constituent_weights_by_date = _normalize_weights_by_date(
            backtest_input.benchmark_constituent_weights_by_date
        )

        calendar = self.market_data.calendar()
        calendar_index = {trade_date: index for index, trade_date in enumerate(calendar)}
        if start_date not in calendar_index:
            raise ValueError(f"Portfolio backtest start_date not found in calendar: {start_date}")
        if end_date not in calendar_index:
            raise ValueError(f"Portfolio backtest end_date not found in calendar: {end_date}")

        construction = self._build_construction(
            backtest_input=backtest_input,
            start_date=start_date,
            end_date=end_date,
            calendar=calendar,
        )
        construction = self._apply_regime_overlay(backtest_input, construction)
        scheduled_steps = self._schedule_executions(
            construction=construction,
            calendar=calendar,
            calendar_index=calendar_index,
            end_date=end_date,
        )
        asset_ids = {
            signal.asset_id
            for step in construction.steps
            for signal in step.signals
        }
        asset_ids.update(
            asset_id
            for weights in backtest_input.benchmark_constituent_weights_by_date.values()
            for asset_id in weights
        )
        bars = self.market_data.bars(
            asset_ids=asset_ids,
            start_date=start_date,
            end_date=end_date,
        )

        positions: dict[str, Position] = {}
        cash = float(backtest_input.initial_cash_cny)
        result = PortfolioBacktestResult()
        previous_equity = float(backtest_input.initial_cash_cny)
        run_calendar = calendar[calendar_index[start_date] : calendar_index[end_date] + 1]

        for trade_date in run_calendar:
            self._mark_positions(
                positions=positions,
                bars=bars,
                trade_date=trade_date,
                price_field="open",
            )
            if trade_date in scheduled_steps:
                cash = self._rebalance(
                    cash=cash,
                    positions=positions,
                    bars=bars,
                    trade_date=trade_date,
                    step=scheduled_steps[trade_date],
                    result=result,
                )
            self._mark_positions(
                positions=positions,
                bars=bars,
                trade_date=trade_date,
                price_field="close",
            )
            state, holdings = self._daily_state(
                trade_date=trade_date,
                cash=cash,
                positions=positions,
                previous_equity=previous_equity,
            )
            result.daily_curve.append(state)
            result.daily_holdings.extend(holdings)
            previous_equity = state.equity

        benchmark_daily_returns = self._benchmark_daily_returns(
            daily_curve=result.daily_curve,
            bars=bars,
            benchmark_constituent_weights_by_date=backtest_input.benchmark_constituent_weights_by_date,
        )
        result.summary = self._summarize(
            daily_curve=result.daily_curve,
            orders=result.orders,
            fills=result.fills,
            diagnostics=result.diagnostics,
            initial_cash_cny=backtest_input.initial_cash_cny,
            risk_free_rate_annual=backtest_input.risk_free_rate_annual,
            benchmark_daily_returns=benchmark_daily_returns,
        )
        return result

    def _build_construction(
        self,
        *,
        backtest_input: PortfolioBacktestInput,
        start_date: str,
        end_date: str,
        calendar: list[str],
    ) -> PortfolioConstructionResult:
        artifacts_by_sleeve = {artifact.sleeve_id: artifact for artifact in backtest_input.artifacts}
        missing_sleeves = sorted(set(backtest_input.portfolio.sleeves) - set(artifacts_by_sleeve))
        if missing_sleeves:
            raise ValueError(
                "Portfolio backtest artifacts missing sleeves: " + ", ".join(missing_sleeves)
            )
        if not backtest_input.portfolio.sleeves:
            raise ValueError("Portfolio backtest portfolio must contain at least one sleeve.")
        calendar_dates = set(calendar)
        decision_dates = sorted(
            {
                _normalize_date_key(step.trade_date)
                for sleeve_id in backtest_input.portfolio.sleeves
                for step in artifacts_by_sleeve[sleeve_id].steps
                if start_date <= _normalize_date_key(step.trade_date) <= end_date
                and _normalize_date_key(step.trade_date) in calendar_dates
            }
        )
        if not decision_dates:
            raise ValueError("Portfolio backtest found no decision dates in the requested window.")

        construction_inputs = [
            PortfolioConstructionInput(
                trade_date=trade_date,
                benchmark_industry_weights=backtest_input.benchmark_industry_weights_by_date.get(
                    trade_date,
                    {},
                ),
                sleeves=[
                    self._step_for_date(artifacts_by_sleeve[sleeve_id], trade_date)
                    .to_construction_input(sleeve_id)
                    for sleeve_id in backtest_input.portfolio.sleeves
                ],
            )
            for trade_date in decision_dates
        ]
        return PortfolioConstructor(
            mandate=self.mandate,
            portfolio=backtest_input.portfolio,
            construction_model=self.construction_model,
        ).build(construction_inputs)

    def _apply_regime_overlay(
        self,
        backtest_input: PortfolioBacktestInput,
        construction: PortfolioConstructionResult,
    ) -> PortfolioConstructionResult:
        if backtest_input.regime_overlay is None:
            return construction
        decisions = RegimeOverlayEvaluator(backtest_input.regime_overlay).evaluate_history(
            trade_dates=[step.trade_date for step in construction.steps],
            observations=backtest_input.regime_overlay_observations,
        ).decisions
        decisions_by_date = {decision.trade_date: decision for decision in decisions}

        adjusted_steps: list[PortfolioConstructionStep] = []
        for step in construction.steps:
            decision = decisions_by_date[step.trade_date]
            overlay_exposure = self._overlay_gross_exposure(
                overlay=backtest_input.regime_overlay,
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

    def _schedule_executions(
        self,
        *,
        construction: PortfolioConstructionResult,
        calendar: list[str],
        calendar_index: dict[str, int],
        end_date: str,
    ) -> dict[str, PortfolioConstructionStep]:
        scheduled: dict[str, PortfolioConstructionStep] = {}
        for step in construction.steps:
            decision_index = calendar_index.get(step.trade_date)
            if decision_index is None or decision_index + 1 >= len(calendar):
                continue
            execution_date = calendar[decision_index + 1]
            if execution_date > end_date:
                continue
            scheduled[execution_date] = step
        return scheduled

    def _rebalance(
        self,
        *,
        cash: float,
        positions: dict[str, Position],
        bars: dict[tuple[str, str], DailyBar],
        trade_date: str,
        step: PortfolioConstructionStep,
        result: PortfolioBacktestResult,
    ) -> float:
        target_weights = self._target_weights(step)
        pre_trade_equity = cash + sum(position.value() for position in positions.values())
        if pre_trade_equity <= 0.0:
            return cash

        signals_by_asset = {signal.asset_id: signal for signal in step.signals}
        all_assets = sorted(set(positions) | set(target_weights))
        order_number = len(result.orders) + 1

        for asset_id in all_assets:
            position = positions.get(asset_id)
            if position is None or position.shares <= 0.0:
                continue
            bar = bars.get((asset_id, trade_date))
            price = self._execution_price(bar)
            current_value = position.shares * (price or position.last_price)
            target_value = target_weights.get(asset_id, 0.0) * pre_trade_equity
            if current_value <= target_value + 1e-9:
                continue
            requested_quantity = self._sell_quantity(
                position=position,
                price=price or position.last_price,
                target_value=target_value,
            )
            if requested_quantity <= 0.0:
                continue
            order = self._order(
                order_number=order_number,
                decision_date=step.trade_date,
                execution_date=trade_date,
                asset_id=asset_id,
                side="sell",
                requested_quantity=requested_quantity,
                target_weight=target_weights.get(asset_id, 0.0),
            )
            order_number += 1
            result.orders.append(order)
            signal = signals_by_asset.get(asset_id)
            if signal is not None and not signal.trade_state.can_exit:
                result.diagnostics.blocked_orders.append(
                    self._blocked(order, "trade_state_exit_block")
                )
                continue
            block_reason = self._sell_block_reason(bar)
            if block_reason:
                result.diagnostics.blocked_orders.append(
                    self._blocked(order, block_reason)
                )
                continue
            assert price is not None
            cost_model = self._cost_model_for_asset(
                asset_id=asset_id,
                signal=signal,
                positions=positions,
            )
            fill_quantity = self._participation_limited_quantity(
                requested_quantity=requested_quantity,
                price=price,
                bar=bar,
                cost_model=cost_model,
            )
            if fill_quantity <= 0.0:
                result.diagnostics.blocked_orders.append(
                    self._blocked(order, "participation_cap")
                )
                continue
            if fill_quantity < requested_quantity - 1e-9:
                result.diagnostics.partial_fills.append(
                    self._partial(order, fill_quantity, "participation_cap")
                )
            cash = self._apply_sell_fill(
                cash=cash,
                positions=positions,
                order=order,
                price=price,
                quantity=fill_quantity,
                cost_model=cost_model,
                result=result,
            )

        for asset_id in sorted(target_weights):
            bar = bars.get((asset_id, trade_date))
            price = self._execution_price(bar)
            current_shares = positions.get(asset_id).shares if asset_id in positions else 0.0
            current_value = current_shares * (price or positions.get(asset_id, Position(asset_id, 0.0, 0.0)).last_price)
            target_value = target_weights[asset_id] * pre_trade_equity
            if target_value <= current_value + 1e-9:
                continue
            requested_quantity = self._buy_quantity(
                price=price,
                current_value=current_value,
                target_value=target_value,
            )
            if requested_quantity <= 0.0:
                continue
            order = self._order(
                order_number=order_number,
                decision_date=step.trade_date,
                execution_date=trade_date,
                asset_id=asset_id,
                side="buy",
                requested_quantity=requested_quantity,
                target_weight=target_weights[asset_id],
            )
            order_number += 1
            result.orders.append(order)
            signal = signals_by_asset.get(asset_id)
            if signal is not None and not signal.trade_state.can_enter:
                result.diagnostics.blocked_orders.append(
                    self._blocked(order, "trade_state_entry_block")
                )
                continue
            block_reason = self._buy_block_reason(bar)
            if block_reason:
                result.diagnostics.blocked_orders.append(
                    self._blocked(order, block_reason)
                )
                continue
            assert price is not None
            cost_model = self._cost_model_for_asset(
                asset_id=asset_id,
                signal=signal,
                positions=positions,
            )
            fill_quantity = self._participation_limited_quantity(
                requested_quantity=requested_quantity,
                price=price,
                bar=bar,
                cost_model=cost_model,
            )
            fill_quantity = self._cash_limited_quantity(
                requested_quantity=fill_quantity,
                price=price,
                cash=cash,
                cost_model=cost_model,
            )
            if fill_quantity <= 0.0:
                result.diagnostics.blocked_orders.append(
                    self._blocked(order, "insufficient_cash")
                )
                continue
            if fill_quantity < requested_quantity - 1e-9:
                result.diagnostics.partial_fills.append(
                    self._partial(order, fill_quantity, "cash_or_participation_cap")
                )
            cash = self._apply_buy_fill(
                cash=cash,
                positions=positions,
                order=order,
                price=price,
                quantity=fill_quantity,
                bar=bar,
                cost_model=cost_model,
                result=result,
            )

        return cash

    def _target_weights(self, step: PortfolioConstructionStep) -> dict[str, float]:
        raw_weights = {
            signal.asset_id: signal.target_weight
            for signal in step.signals
            if signal.target_weight > 0.0
        }
        total = sum(raw_weights.values())
        investable_budget = self._investable_budget()
        if total <= 0.0 or total <= investable_budget:
            return raw_weights
        scale = investable_budget / total
        return {asset_id: weight * scale for asset_id, weight in raw_weights.items()}

    def _order(
        self,
        *,
        order_number: int,
        decision_date: str,
        execution_date: str,
        asset_id: str,
        side: str,
        requested_quantity: float,
        target_weight: float,
    ) -> PortfolioOrder:
        return PortfolioOrder(
            order_id=f"{execution_date}:{order_number:04d}",
            decision_date=decision_date,
            execution_date=execution_date,
            asset_id=asset_id,
            side=side,
            requested_quantity=requested_quantity,
            target_weight=target_weight,
        )

    def _apply_sell_fill(
        self,
        *,
        cash: float,
        positions: dict[str, Position],
        order: PortfolioOrder,
        price: float,
        quantity: float,
        cost_model: CostModel,
        result: PortfolioBacktestResult,
    ) -> float:
        position = positions[order.asset_id]
        quantity = min(quantity, position.shares)
        gross_value = quantity * price
        cost = gross_value * (cost_model.sell_total_bps() / 10_000.0)
        net_cash_flow = gross_value - cost
        position.shares -= quantity
        if position.shares <= 1e-9:
            del positions[order.asset_id]
        result.fills.append(
            PortfolioFill(
                order_id=order.order_id,
                decision_date=order.decision_date,
                execution_date=order.execution_date,
                asset_id=order.asset_id,
                side=order.side,
                quantity=quantity,
                price=price,
                gross_value=gross_value,
                cost=cost,
                net_cash_flow=net_cash_flow,
                cost_model_id=cost_model.id,
                participation_cap=self._participation_cap(cost_model),
            )
        )
        return cash + net_cash_flow

    def _apply_buy_fill(
        self,
        *,
        cash: float,
        positions: dict[str, Position],
        order: PortfolioOrder,
        price: float,
        quantity: float,
        bar: DailyBar | None,
        cost_model: CostModel,
        result: PortfolioBacktestResult,
    ) -> float:
        gross_value = quantity * price
        cost = gross_value * (cost_model.buy_total_bps() / 10_000.0)
        net_cash_flow = -(gross_value + cost)
        position = positions.get(order.asset_id)
        if position is None:
            positions[order.asset_id] = Position(
                asset_id=order.asset_id,
                shares=quantity,
                last_price=price,
                adj_factor=bar.adj_factor if bar is not None else None,
                cost_model_id=cost_model.id,
                price_basis=bar.price_basis if bar is not None else "unadjusted",
            )
        else:
            position.shares += quantity
            position.last_price = price
            position.cost_model_id = cost_model.id
            if bar is not None:
                position.price_basis = bar.price_basis
                position.adj_factor = bar.adj_factor
        result.fills.append(
            PortfolioFill(
                order_id=order.order_id,
                decision_date=order.decision_date,
                execution_date=order.execution_date,
                asset_id=order.asset_id,
                side=order.side,
                quantity=quantity,
                price=price,
                gross_value=gross_value,
                cost=cost,
                net_cash_flow=net_cash_flow,
                cost_model_id=cost_model.id,
                participation_cap=self._participation_cap(cost_model),
            )
        )
        return cash + net_cash_flow

    def _mark_positions(
        self,
        *,
        positions: dict[str, Position],
        bars: dict[tuple[str, str], DailyBar],
        trade_date: str,
        price_field: str,
    ) -> None:
        for asset_id, position in list(positions.items()):
            bar = bars.get((asset_id, trade_date))
            if bar is None:
                continue
            position.adj_factor = bar.adj_factor
            price = getattr(bar, price_field)
            if price is not None and price > 0.0:
                position.last_price = float(price)
                position.price_basis = bar.price_basis

    def _daily_state(
        self,
        *,
        trade_date: str,
        cash: float,
        positions: dict[str, Position],
        previous_equity: float,
    ) -> tuple[DailyPortfolioState, list[DailyHolding]]:
        positions_value = sum(position.value() for position in positions.values())
        equity = cash + positions_value
        daily_return = (equity / previous_equity) - 1.0 if previous_equity > 0.0 else 0.0
        holdings = [
            DailyHolding(
                trade_date=trade_date,
                asset_id=asset_id,
                shares=position.shares,
                mark_price=position.last_price,
                market_value=position.value(),
                weight=(position.value() / equity) if equity > 0.0 else 0.0,
            )
            for asset_id, position in sorted(positions.items())
        ]
        weights = {holding.asset_id: holding.weight for holding in holdings}
        state = DailyPortfolioState(
            trade_date=trade_date,
            equity=equity,
            cash=cash,
            positions_value=positions_value,
            gross_exposure=(positions_value / equity) if equity > 0.0 else 0.0,
            daily_return=daily_return,
            weights=weights,
        )
        return state, holdings

    def _benchmark_daily_returns(
        self,
        *,
        daily_curve: list[DailyPortfolioState],
        bars: dict[tuple[str, str], DailyBar],
        benchmark_constituent_weights_by_date: dict[str, dict[str, float]],
    ) -> list[float | None]:
        if not benchmark_constituent_weights_by_date:
            return []

        benchmark_returns: list[float | None] = []
        for previous_state, state in zip(daily_curve, daily_curve[1:]):
            weights = benchmark_constituent_weights_by_date.get(
                previous_state.trade_date,
                {},
            )
            benchmark_returns.append(
                _weighted_close_to_close_return(
                    weights=weights,
                    previous_date=previous_state.trade_date,
                    trade_date=state.trade_date,
                    bars=bars,
                )
            )
        return benchmark_returns

    def _summarize(
        self,
        *,
        daily_curve: list[DailyPortfolioState],
        orders: list[PortfolioOrder],
        fills: list[PortfolioFill],
        diagnostics: PortfolioBacktestDiagnostics,
        initial_cash_cny: float,
        risk_free_rate_annual: float,
        benchmark_daily_returns: list[float | None],
    ) -> PortfolioBacktestSummary:
        if not daily_curve:
            raise ValueError("Portfolio backtest produced no daily curve.")
        returns = [state.daily_return for state in daily_curve[1:]]
        final_equity = daily_curve[-1].equity
        total_return = (final_equity / initial_cash_cny) - 1.0
        trading_days = len(daily_curve)
        calendar_years = max(
            (
                _date_from_key(daily_curve[-1].trade_date)
                - _date_from_key(daily_curve[0].trade_date)
            ).days
            / 365.25,
            trading_days / 252.0,
        )
        cagr = (final_equity / initial_cash_cny) ** (1.0 / calendar_years) - 1.0
        annualized_return = statistics.mean(returns) * 252.0 if returns else 0.0
        annualized_volatility = (
            statistics.stdev(returns) * math.sqrt(252.0)
            if len(returns) > 1
            else 0.0
        )
        sharpe = (
            (annualized_return - risk_free_rate_annual) / annualized_volatility
            if annualized_volatility > 0.0
            else 0.0
        )
        paired_benchmark_returns = [
            (portfolio_return, benchmark_return)
            for portfolio_return, benchmark_return in zip(
                returns,
                benchmark_daily_returns,
            )
            if benchmark_return is not None
        ]
        if paired_benchmark_returns:
            benchmark_returns = [
                benchmark_return
                for _, benchmark_return in paired_benchmark_returns
            ]
            active_returns = [
                portfolio_return - benchmark_return
                for portfolio_return, benchmark_return in paired_benchmark_returns
            ]
            benchmark_annualized_return = statistics.mean(benchmark_returns) * 252.0
            active_annualized_return = statistics.mean(active_returns) * 252.0
            tracking_error = (
                statistics.stdev(active_returns) * math.sqrt(252.0)
                if len(active_returns) > 1
                else 0.0
            )
            information_ratio = (
                active_annualized_return / tracking_error
                if tracking_error > 0.0
                else 0.0
            )
            beta = _beta(
                [portfolio_return for portfolio_return, _ in paired_benchmark_returns],
                benchmark_returns,
            )
            alpha = (
                annualized_return
                - risk_free_rate_annual
                - beta * (benchmark_annualized_return - risk_free_rate_annual)
            )
        else:
            benchmark_annualized_return = 0.0
            active_annualized_return = 0.0
            tracking_error = 0.0
            information_ratio = 0.0
            beta = 0.0
            alpha = 0.0
        max_drawdown = _max_drawdown([state.equity for state in daily_curve])
        calmar = cagr / abs(max_drawdown) if max_drawdown < 0.0 else 0.0
        win_rate = (
            sum(1 for value in returns if value > 0.0) / len(returns)
            if returns
            else 0.0
        )
        average_equity = statistics.mean(state.equity for state in daily_curve)
        buy_gross = sum(fill.gross_value for fill in fills if fill.side == "buy")
        sell_gross = sum(fill.gross_value for fill in fills if fill.side == "sell")
        if average_equity > 0.0:
            buy_turnover = buy_gross / average_equity
            sell_turnover = sell_gross / average_equity
            turnover = ((buy_gross + sell_gross) / 2.0) / average_equity
        else:
            buy_turnover = 0.0
            sell_turnover = 0.0
            turnover = 0.0
        total_costs = sum(fill.cost for fill in fills)
        order_count = len(orders)
        blocked_trade_share = (
            len(diagnostics.blocked_orders) / order_count if order_count else 0.0
        )
        partial_fill_share = (
            len(diagnostics.partial_fills) / order_count if order_count else 0.0
        )
        yearly_returns, yearly_max_drawdown = _yearly_metrics(daily_curve)
        return PortfolioBacktestSummary(
            start_date=daily_curve[0].trade_date,
            end_date=daily_curve[-1].trade_date,
            trading_days=trading_days,
            initial_cash_cny=initial_cash_cny,
            final_equity=final_equity,
            total_return=total_return,
            cagr=cagr,
            annualized_return=annualized_return,
            annualized_volatility=annualized_volatility,
            sharpe=sharpe,
            risk_free_rate_annual=risk_free_rate_annual,
            benchmark_annualized_return=benchmark_annualized_return,
            active_annualized_return=active_annualized_return,
            tracking_error=tracking_error,
            beta=beta,
            alpha=alpha,
            information_ratio=information_ratio,
            max_drawdown=max_drawdown,
            calmar=calmar,
            win_rate=win_rate,
            turnover=turnover,
            buy_turnover=buy_turnover,
            sell_turnover=sell_turnover,
            total_costs=total_costs,
            blocked_trade_share=blocked_trade_share,
            partial_fill_share=partial_fill_share,
            yearly_returns=yearly_returns,
            yearly_max_drawdown=yearly_max_drawdown,
        )

    def _step_for_date(
        self,
        artifact: SleeveResearchArtifact,
        trade_date: str,
    ) -> SleeveResearchStep:
        for step in artifact.steps:
            if _normalize_date_key(step.trade_date) == trade_date:
                return SleeveResearchStep(
                    trade_date=trade_date,
                    records=list(step.records),
                )
        raise ValueError(
            f"Sleeve artifact {artifact.sleeve_id} must cover trade date {trade_date}"
        )

    def _buy_block_reason(self, bar: DailyBar | None) -> str:
        if bar is None or bar.open is None or bar.open <= 0.0:
            return "suspended_or_missing_open"
        if _is_cn_a_directional_open_lock(bar=bar, direction="entry"):
            return "limit_up_open_lock"
        return ""

    def _sell_block_reason(self, bar: DailyBar | None) -> str:
        if bar is None or bar.open is None or bar.open <= 0.0:
            return "suspended_or_missing_open"
        if _is_cn_a_directional_open_lock(bar=bar, direction="exit"):
            return "limit_down_open_lock"
        return ""

    def _execution_price(self, bar: DailyBar | None) -> float | None:
        if bar is None or bar.open is None or bar.open <= 0.0:
            return None
        return bar.open

    def _sell_quantity(
        self,
        *,
        position: Position,
        price: float,
        target_value: float,
    ) -> float:
        if target_value <= 0.0:
            return position.shares
        target_shares = target_value / price if price > 0.0 else 0.0
        return self._round_lot_down(max(position.shares - target_shares, 0.0))

    def _buy_quantity(
        self,
        *,
        price: float | None,
        current_value: float,
        target_value: float,
    ) -> float:
        if price is None or price <= 0.0:
            return 0.0
        return self._round_lot_down(max((target_value - current_value) / price, 0.0))

    def _participation_limited_quantity(
        self,
        *,
        requested_quantity: float,
        price: float,
        bar: DailyBar | None,
        cost_model: CostModel,
    ) -> float:
        cap = self._participation_cap(cost_model)
        if (
            cap <= 0.0
            or bar is None
            or bar.turnover_value_cny is None
            or bar.turnover_value_cny <= 0.0
            or price <= 0.0
        ):
            return requested_quantity
        capped = (cap * bar.turnover_value_cny) / price
        if capped >= requested_quantity:
            return requested_quantity
        return self._round_lot_down(capped)

    def _cash_limited_quantity(
        self,
        *,
        requested_quantity: float,
        price: float,
        cash: float,
        cost_model: CostModel,
    ) -> float:
        if requested_quantity <= 0.0:
            return 0.0
        cost_multiplier = 1.0 + (cost_model.buy_total_bps() / 10_000.0)
        affordable = cash / (price * cost_multiplier) if price > 0.0 else 0.0
        return min(requested_quantity, self._round_lot_down(affordable))

    def _round_lot_down(self, quantity: float) -> float:
        lot_size = self.execution_policy.lot_size or 100
        return math.floor(quantity / lot_size) * lot_size

    def _cost_model_for_asset(
        self,
        *,
        asset_id: str,
        signal: PortfolioSecuritySignal | None,
        positions: dict[str, Position],
    ) -> CostModel:
        if signal is None or not signal.cost_model_id:
            position = positions.get(asset_id)
            if position is not None and position.cost_model_id:
                if position.cost_model_id not in self.cost_models:
                    raise ValueError(
                        f"Unknown cost model for held asset {asset_id}: {position.cost_model_id}"
                    )
                return self.cost_models[position.cost_model_id]
            return self.default_cost_model
        if signal.cost_model_id not in self.cost_models:
            raise ValueError(f"Unknown cost model for asset {signal.asset_id}: {signal.cost_model_id}")
        return self.cost_models[signal.cost_model_id]

    def _participation_cap(self, cost_model: CostModel) -> float:
        caps = [
            cap
            for cap in (self.mandate.execution_participation_cap, cost_model.participation_cap)
            if cap > 0.0
        ]
        return min(caps) if caps else 0.0

    def _investable_budget(self) -> float:
        cash_buffer = self.portfolio.constraints.get(
            "cash_buffer",
            self.mandate.risk.get("cash_buffer", 0.0),
        )
        return max(1.0 - float(cash_buffer), 0.0)

    def _blocked(
        self,
        order: PortfolioOrder,
        reason: str,
    ) -> BlockedOrderDiagnostic:
        return BlockedOrderDiagnostic(
            order_id=order.order_id,
            decision_date=order.decision_date,
            execution_date=order.execution_date,
            asset_id=order.asset_id,
            side=order.side,
            requested_quantity=order.requested_quantity,
            reason=reason,
        )

    def _partial(
        self,
        order: PortfolioOrder,
        filled_quantity: float,
        reason: str,
    ) -> PartialFillDiagnostic:
        return PartialFillDiagnostic(
            order_id=order.order_id,
            decision_date=order.decision_date,
            execution_date=order.execution_date,
            asset_id=order.asset_id,
            side=order.side,
            requested_quantity=order.requested_quantity,
            filled_quantity=filled_quantity,
            reason=reason,
        )

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


def load_portfolio_backtest_case(path: Path | str) -> LoadedPortfolioBacktestCase:
    definition = PortfolioBacktestCaseDefinition.from_toml(_read_toml(path))
    portfolio = load_portfolio(definition.portfolio_path)
    mandate = load_mandate(CONFIG_ROOT / "mandates" / f"{portfolio.mandate_id}.toml")
    construction_model = load_portfolio_construction_model(
        CONFIG_ROOT / "portfolio_construction" / f"{portfolio.construction_model_id}.toml"
    )
    execution_policy = load_execution_policy(definition.execution_policy_path)
    if portfolio.execution_policy_id and execution_policy.id != portfolio.execution_policy_id:
        raise ValueError(
            "Portfolio backtest execution policy must match the portfolio recipe."
        )
    default_cost_model = load_cost_model(definition.default_cost_model_path)
    cost_models = {
        model.id: model
        for model in (
            load_cost_model(path) for path in definition.additional_cost_model_paths
        )
        if model.id != default_cost_model.id
    }
    artifacts = [load_sleeve_artifact(path) for path in definition.artifact_paths]

    benchmark_state_artifact = None
    if definition.benchmark_state_path:
        benchmark_state_artifact = load_benchmark_state_artifact(
            definition.benchmark_state_path
        )
        if benchmark_state_artifact.benchmark_id != portfolio.benchmark:
            raise ValueError(
                "Portfolio backtest benchmark_state_path must match the portfolio benchmark."
            )

    regime_overlay = None
    regime_overlay_observations = None
    if portfolio.regime_overlay_id:
        regime_overlay = load_regime_overlay(
            CONFIG_ROOT / "regime_overlays" / f"{portfolio.regime_overlay_id}.toml"
        )
        if not definition.regime_overlay_observation_path:
            raise ValueError(
                "Portfolio backtest case must define regime_overlay_observation_path when the portfolio declares regime_overlay_id."
            )
        regime_overlay_observations = load_regime_overlay_observation_artifact(
            definition.regime_overlay_observation_path
        )
        if regime_overlay_observations.overlay_id != regime_overlay.id:
            raise ValueError(
                "Portfolio backtest regime overlay observations must match the configured overlay."
            )
    elif definition.regime_overlay_observation_path:
        raise ValueError(
            "Portfolio backtest case cannot define regime_overlay_observation_path without a portfolio regime_overlay_id."
        )

    return LoadedPortfolioBacktestCase(
        definition=definition,
        mandate=mandate,
        portfolio=portfolio,
        construction_model=construction_model,
        execution_policy=execution_policy,
        default_cost_model=default_cost_model,
        source_db_path=_resolve_project_path(definition.source_db_path),
        artifacts=artifacts,
        cost_models=cost_models,
        benchmark_state_artifact=benchmark_state_artifact,
        regime_overlay=regime_overlay,
        regime_overlay_observations=regime_overlay_observations,
    )


def run_loaded_portfolio_backtest(
    loaded_case: LoadedPortfolioBacktestCase,
) -> PortfolioBacktestResult:
    benchmark_industry_weights = (
        loaded_case.benchmark_state_artifact.weights_by_date()
        if loaded_case.benchmark_state_artifact is not None
        else {}
    )
    benchmark_industry_weights = {
        _normalize_date_key(trade_date): weights
        for trade_date, weights in benchmark_industry_weights.items()
    }
    benchmark_constituent_weights = {
        _normalize_date_key(step.trade_date): {
            constituent.asset_id: constituent.weight
            for constituent in step.constituents
        }
        for step in (
            loaded_case.benchmark_state_artifact.steps
            if loaded_case.benchmark_state_artifact is not None
            else []
        )
    }
    return PortfolioBacktester(
        mandate=loaded_case.mandate,
        portfolio=loaded_case.portfolio,
        construction_model=loaded_case.construction_model,
        execution_policy=loaded_case.execution_policy,
        default_cost_model=loaded_case.default_cost_model,
        cost_models=loaded_case.cost_models,
        source_db_path=loaded_case.source_db_path,
    ).run(
        PortfolioBacktestInput(
            portfolio=loaded_case.portfolio,
            artifacts=loaded_case.artifacts,
            start_date=loaded_case.definition.start_date,
            end_date=loaded_case.definition.end_date,
            initial_cash_cny=loaded_case.definition.initial_cash_cny,
            risk_free_rate_annual=loaded_case.definition.risk_free_rate_annual,
            benchmark_industry_weights_by_date=benchmark_industry_weights,
            benchmark_constituent_weights_by_date=benchmark_constituent_weights,
            regime_overlay=loaded_case.regime_overlay,
            regime_overlay_observations=(
                loaded_case.regime_overlay_observations.steps
                if loaded_case.regime_overlay_observations is not None
                else []
            ),
        )
    )


def write_portfolio_backtest_artifact(
    *,
    case_id: str,
    description: str,
    result: PortfolioBacktestResult,
    path: Path | str,
) -> Path:
    target = _resolve_project_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "artifact_type": "portfolio_backtest_result",
        "case_id": case_id,
        "description": description,
        "artifact": asdict(result),
    }
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return target


def _is_cn_a_directional_open_lock(*, bar: DailyBar, direction: str) -> bool:
    open_price = bar.raw_open if bar.raw_open is not None else bar.open
    high_price = bar.raw_high if bar.raw_high is not None else bar.high
    low_price = bar.raw_low if bar.raw_low is not None else bar.low
    pre_close = (
        bar.raw_pre_close if bar.raw_pre_close is not None else bar.pre_close
    )
    if (
        pre_close is None
        or open_price is None
        or high_price is None
        or low_price is None
        or pre_close <= 0.0
    ):
        return False
    limit_ratio = _cn_a_limit_ratio(board=bar.board, is_st=bar.is_st)
    if direction == "entry":
        upper_limit = _round_cn_price(float(pre_close) * (1.0 + limit_ratio))
        return open_price >= upper_limit - 1e-6 and low_price >= upper_limit - 1e-6
    if direction == "exit":
        lower_limit = _round_cn_price(float(pre_close) * (1.0 - limit_ratio))
        return open_price <= lower_limit + 1e-6 and high_price <= lower_limit + 1e-6
    raise ValueError(f"Unsupported directional lock check: {direction}")


def _cn_a_limit_ratio(*, board: str, is_st: bool) -> float:
    if is_st:
        return 0.05
    if board == "beijing":
        return 0.30
    if board in {"chinext", "star"}:
        return 0.20
    return 0.10


def _round_cn_price(value: float) -> float:
    return float(
        Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    )


def _max_drawdown(equity: list[float]) -> float:
    peak = 0.0
    max_drawdown = 0.0
    for value in equity:
        peak = max(peak, value)
        if peak > 0.0:
            max_drawdown = min(max_drawdown, (value / peak) - 1.0)
    return max_drawdown


def _yearly_metrics(
    daily_curve: list[DailyPortfolioState],
) -> tuple[dict[str, float], dict[str, float]]:
    by_year: dict[str, list[DailyPortfolioState]] = {}
    for state in daily_curve:
        by_year.setdefault(state.trade_date[:4], []).append(state)
    yearly_returns = {
        year: (states[-1].equity / states[0].equity) - 1.0
        for year, states in by_year.items()
        if states[0].equity > 0.0
    }
    yearly_max_drawdown = {
        year: _max_drawdown([state.equity for state in states])
        for year, states in by_year.items()
    }
    return yearly_returns, yearly_max_drawdown


def _weighted_close_to_close_return(
    *,
    weights: dict[str, float],
    previous_date: str,
    trade_date: str,
    bars: dict[tuple[str, str], DailyBar],
) -> float | None:
    weighted_return = 0.0
    used_weight = 0.0
    for asset_id, weight in weights.items():
        if weight <= 0.0:
            continue
        previous_bar = bars.get((asset_id, previous_date))
        current_bar = bars.get((asset_id, trade_date))
        if previous_bar is None or current_bar is None:
            continue
        if (
            previous_bar.close is None
            or current_bar.close is None
            or previous_bar.close <= 0.0
        ):
            continue
        weighted_return += weight * ((current_bar.close / previous_bar.close) - 1.0)
        used_weight += weight
    if used_weight <= 0.0:
        return None
    return weighted_return / used_weight


def _beta(portfolio_returns: list[float], benchmark_returns: list[float]) -> float:
    if len(portfolio_returns) < 2 or len(portfolio_returns) != len(benchmark_returns):
        return 0.0
    benchmark_mean = statistics.mean(benchmark_returns)
    portfolio_mean = statistics.mean(portfolio_returns)
    benchmark_variance = sum(
        (value - benchmark_mean) ** 2 for value in benchmark_returns
    )
    if benchmark_variance <= 0.0:
        return 0.0
    covariance = sum(
        (portfolio_return - portfolio_mean) * (benchmark_return - benchmark_mean)
        for portfolio_return, benchmark_return in zip(
            portfolio_returns,
            benchmark_returns,
        )
    )
    return covariance / benchmark_variance


def _date_from_key(value: str):
    return datetime.strptime(_normalize_date_key(value), "%Y%m%d").date()


def _normalize_date_key(value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""
    if len(text) == 8 and text.isdigit():
        return text
    return datetime.strptime(text, "%Y-%m-%d").strftime("%Y%m%d")


def _normalize_weights_by_date(
    weights_by_date: dict[str, dict[str, float]],
) -> dict[str, dict[str, float]]:
    return {
        _normalize_date_key(trade_date): {
            str(asset_id): float(weight)
            for asset_id, weight in weights.items()
        }
        for trade_date, weights in weights_by_date.items()
    }


def _resolve_project_path(path: Path | str) -> Path:
    target = Path(path)
    if target.is_absolute():
        return target
    return PROJECT_ROOT / target


def _read_toml(path: Path | str) -> JsonMap:
    target = _resolve_project_path(path)
    with target.open("rb") as handle:
        return tomllib.load(handle)
