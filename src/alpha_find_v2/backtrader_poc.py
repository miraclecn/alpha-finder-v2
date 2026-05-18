from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from .config_loader import (
    CONFIG_ROOT,
    load_cost_model,
    load_execution_policy,
    load_mandate,
    load_portfolio,
    load_portfolio_construction_model,
)
from .models import (
    CostModel,
    ExecutionPolicy,
    Mandate,
    PortfolioConstructionModel,
    PortfolioRecipe,
)
from .portfolio_backtester import DailyBar, PortfolioBacktestInput, PortfolioBacktester
from .portfolio_promotion_replay import (
    SleeveResearchArtifact,
    SleeveResearchStep,
    SleeveSignalRecord,
)


@dataclass(slots=True)
class BacktraderPocEngineResult:
    fills: list[dict[str, object]] = field(default_factory=list)
    daily_equity: list[dict[str, float]] = field(default_factory=list)
    final_equity: float = 0.0
    rebalance_dates: list[str] = field(default_factory=list)


@dataclass(slots=True)
class BacktraderPocResult:
    scenario_id: str
    internal: BacktraderPocEngineResult
    backtrader: BacktraderPocEngineResult
    equity_delta: float


@dataclass(slots=True)
class BacktraderPocScenario:
    scenario_id: str
    asset_ids: tuple[str, ...]
    start_date: str
    end_date: str
    initial_cash_cny: float
    sleeve_id: str
    target_id: str
    bars: list[DailyBar] = field(default_factory=list)
    target_weights_by_decision_date: dict[str, dict[str, float]] = field(
        default_factory=dict
    )
    mandate: Mandate | None = None
    portfolio: PortfolioRecipe | None = None
    construction_model: PortfolioConstructionModel | None = None
    execution_policy: ExecutionPolicy | None = None
    cost_model: CostModel | None = None


def run_named_backtrader_poc(name: str) -> BacktraderPocResult:
    scenarios = {
        "single_name_round_trip": _single_name_round_trip_scenario,
        "weekly_16_name_rotation_current_params": (
            _weekly_16_name_rotation_current_params_scenario
        ),
    }
    builder = scenarios.get(name)
    if builder is None:
        raise ValueError(f"Unsupported Backtrader POC scenario: {name}")
    scenario = builder()
    internal = _run_internal_backtest(scenario)
    backtrader = _run_backtrader_backtest(scenario)
    return BacktraderPocResult(
        scenario_id=scenario.scenario_id,
        internal=internal,
        backtrader=backtrader,
        equity_delta=_round_money(backtrader.final_equity - internal.final_equity),
    )


def _single_name_round_trip_scenario() -> BacktraderPocScenario:
    asset_id = "AAA"
    return BacktraderPocScenario(
        scenario_id="single_name_round_trip",
        asset_ids=(asset_id,),
        start_date="20260105",
        end_date="20260107",
        initial_cash_cny=10_000.0,
        sleeve_id="trend",
        target_id="open_t1_to_open_t20_net_cost",
        bars=[
            DailyBar(
                security_id=asset_id,
                trade_date="20260105",
                price_basis="unadjusted",
                tradeability_source_priority="official",
                board="main_board",
                is_st=False,
                open=10.0,
                high=10.0,
                low=10.0,
                close=10.0,
                pre_close=10.0,
                adj_factor=1.0,
                turnover_value_cny=100_000_000.0,
                raw_open=10.0,
                raw_high=10.0,
                raw_low=10.0,
                raw_close=10.0,
                raw_pre_close=10.0,
            ),
            DailyBar(
                security_id=asset_id,
                trade_date="20260106",
                price_basis="unadjusted",
                tradeability_source_priority="official",
                board="main_board",
                is_st=False,
                open=10.0,
                high=10.0,
                low=10.0,
                close=10.0,
                pre_close=10.0,
                adj_factor=1.0,
                turnover_value_cny=100_000_000.0,
                raw_open=10.0,
                raw_high=10.0,
                raw_low=10.0,
                raw_close=10.0,
                raw_pre_close=10.0,
            ),
            DailyBar(
                security_id=asset_id,
                trade_date="20260107",
                price_basis="unadjusted",
                tradeability_source_priority="official",
                board="main_board",
                is_st=False,
                open=11.0,
                high=11.0,
                low=11.0,
                close=11.0,
                pre_close=10.0,
                adj_factor=1.0,
                turnover_value_cny=100_000_000.0,
                raw_open=11.0,
                raw_high=11.0,
                raw_low=11.0,
                raw_close=11.0,
                raw_pre_close=10.0,
            ),
        ],
        target_weights_by_decision_date={
            "20260105": {asset_id: 1.0},
            "20260106": {},
        },
        mandate=Mandate(
            id="a_share_long_only_eod",
            name="Backtrader POC Mandate",
            market="CN-A",
            benchmark="CSI 800",
            account_type="cash_equity",
            description="Synthetic mandate for backtrader proof-of-concept.",
            max_single_name_weight=1.0,
            execution_participation_cap=1.0,
            risk={"cash_buffer": 0.0},
        ),
        portfolio=PortfolioRecipe(
            id="backtrader_poc_portfolio",
            name="Backtrader POC Portfolio",
            mandate_id="a_share_long_only_eod",
            benchmark="CSI 800",
            rebalance_policy="weekly",
            description="Synthetic portfolio for backtrader proof-of-concept.",
            construction_model_id="backtrader_poc_model",
            execution_policy_id="a_share_next_open_v1",
            sleeves=["trend"],
            allocation={"trend": 1.0},
            constraints={"max_names": 1, "max_single_name_weight": 1.0},
        ),
        construction_model=PortfolioConstructionModel(
            id="backtrader_poc_model",
            name="Backtrader POC Model",
            description="Synthetic construction model for backtrader proof-of-concept.",
            sleeve_weight_source="portfolio_allocation",
            overlap_mode="sum",
            name_selection="top_weight",
            excess_weight_policy="hold_cash",
            industry_budget_mode="",
        ),
        execution_policy=ExecutionPolicy(
            id="a_share_next_open_v1",
            name="Next open",
            description="Synthetic next-open policy.",
            trade_timing="next_day_open",
            order_basis="weight_delta",
            blocked_trade_policy="carry_positions",
            cash_policy="hold_residual_cash",
            participation_cap_source="mandate_or_cost_model",
            lot_size=100,
            min_trade_weight=0.0,
        ),
        cost_model=CostModel(
            id="base",
            name="Base",
            description="Zero-cost synthetic model for backtrader proof-of-concept.",
            buy_commission_bps=0.0,
            sell_commission_bps=0.0,
            buy_slippage_bps=0.0,
            sell_slippage_bps=0.0,
            sell_stamp_duty_bps=0.0,
            participation_cap=1.0,
        ),
    )


def _weekly_16_name_rotation_current_params_scenario() -> BacktraderPocScenario:
    portfolio = load_portfolio(
        "research/examples/deployment_minimal/trend_real_output_portfolio.toml"
    )
    mandate = load_mandate(CONFIG_ROOT / "mandates" / f"{portfolio.mandate_id}.toml")
    loaded_construction_model = load_portfolio_construction_model(
        CONFIG_ROOT / "portfolio_construction" / f"{portfolio.construction_model_id}.toml"
    )
    construction_model = PortfolioConstructionModel(
        id=loaded_construction_model.id,
        name=loaded_construction_model.name,
        description=loaded_construction_model.description,
        sleeve_weight_source=loaded_construction_model.sleeve_weight_source,
        overlap_mode=loaded_construction_model.overlap_mode,
        name_selection=loaded_construction_model.name_selection,
        excess_weight_policy=loaded_construction_model.excess_weight_policy,
        industry_budget_mode="",
    )
    execution_policy = load_execution_policy(
        CONFIG_ROOT / "execution_policies" / f"{portfolio.execution_policy_id}.toml"
    )
    cost_model = load_cost_model(CONFIG_ROOT / "cost_models" / "base_a_share_cash.toml")

    asset_ids = tuple(f"{600001 + index:06d}.SH" for index in range(20))
    trade_dates = _business_dates("20260105", count=25)
    decision_dates = trade_dates[0::5][:4]
    target_weights_by_decision_date = _weekly_rotation_targets(
        asset_ids=asset_ids,
        decision_dates=decision_dates,
        held_name_count=16,
        weekly_shift=4,
        target_weight=0.06,
    )

    return BacktraderPocScenario(
        scenario_id="weekly_16_name_rotation_current_params",
        asset_ids=asset_ids,
        start_date=trade_dates[0],
        end_date=trade_dates[-1],
        initial_cash_cny=10_000_000.0,
        sleeve_id=portfolio.sleeves[0],
        target_id="open_t1_to_open_t20_net_cost",
        bars=_synthetic_bars_for_assets(asset_ids=asset_ids, trade_dates=trade_dates),
        target_weights_by_decision_date=target_weights_by_decision_date,
        mandate=mandate,
        portfolio=portfolio,
        construction_model=construction_model,
        execution_policy=execution_policy,
        cost_model=cost_model,
    )


def _run_internal_backtest(scenario: BacktraderPocScenario) -> BacktraderPocEngineResult:
    assert scenario.mandate is not None
    assert scenario.portfolio is not None
    assert scenario.construction_model is not None
    assert scenario.execution_policy is not None
    assert scenario.cost_model is not None

    with TemporaryDirectory() as temp_dir:
        source_db_path = Path(temp_dir) / "source.duckdb"
        _create_synthetic_source_db(source_db_path, scenario.bars)
        result = PortfolioBacktester(
            mandate=scenario.mandate,
            portfolio=scenario.portfolio,
            construction_model=scenario.construction_model,
            execution_policy=scenario.execution_policy,
            default_cost_model=scenario.cost_model,
            source_db_path=source_db_path,
        ).run(
            PortfolioBacktestInput(
                portfolio=scenario.portfolio,
                artifacts=[_artifact_for_scenario(scenario)],
                start_date=scenario.start_date,
                end_date=scenario.end_date,
                initial_cash_cny=scenario.initial_cash_cny,
            )
        )

    return BacktraderPocEngineResult(
        fills=_normalize_fills(
            [
                {
                    "asset_id": fill.asset_id,
                    "side": fill.side,
                    "execution_date": fill.execution_date,
                    "quantity": _round_money(fill.quantity),
                    "price": _round_money(fill.price),
                }
                for fill in result.fills
            ]
        ),
        daily_equity=[
            {
                "trade_date": day.trade_date,
                "equity": _round_money(day.equity),
            }
            for day in result.daily_curve
        ],
        final_equity=(
            _round_money(result.summary.final_equity)
            if result.summary is not None
            else 0.0
        ),
        rebalance_dates=sorted(scenario.target_weights_by_decision_date),
    )


def _run_backtrader_backtest(scenario: BacktraderPocScenario) -> BacktraderPocEngineResult:
    try:
        import backtrader as bt
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "Backtrader is required for this proof-of-concept. "
            "Install it in the local environment before running the POC."
        ) from exc

    assert scenario.execution_policy is not None
    assert scenario.cost_model is not None

    frames = {
        asset_id: pd.DataFrame(
            [
                {
                    "datetime": pd.to_datetime(bar.trade_date, format="%Y%m%d"),
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": 1_000_000_000.0,
                    "openinterest": 0.0,
                }
                for bar in scenario.bars
                if bar.security_id == asset_id
            ]
        ).set_index("datetime")
        for asset_id in scenario.asset_ids
    }
    cost_model = scenario.cost_model
    lot_size = scenario.execution_policy.lot_size or 100
    min_trade_weight = scenario.execution_policy.min_trade_weight

    class AShareCostInfo(bt.CommInfoBase):
        params = dict(
            stocklike=True,
            commtype=bt.CommInfoBase.COMM_PERC,
            percabs=True,
            buy_commission=cost_model.buy_commission_bps / 10_000.0,
            sell_commission=cost_model.sell_commission_bps / 10_000.0,
            buy_slippage=cost_model.buy_slippage_bps / 10_000.0,
            sell_slippage=cost_model.sell_slippage_bps / 10_000.0,
            sell_stamp_duty=cost_model.sell_stamp_duty_bps / 10_000.0,
        )

        def _getcommission(self, size, price, pseudoexec):
            notional = abs(size) * price
            if size > 0:
                rate = self.p.buy_commission + self.p.buy_slippage
            else:
                rate = (
                    self.p.sell_commission
                    + self.p.sell_slippage
                    + self.p.sell_stamp_duty
                )
            return notional * rate

    class DecisionTargetStrategy(bt.Strategy):
        params = dict(
            target_weights_by_date={},
            lot_size=100,
            min_trade_weight=0.0,
        )

        def __init__(self):
            self.fill_records: list[dict[str, object]] = []
            self.equity_curve: list[dict[str, float]] = []
            self.rebalance_dates: list[str] = []

        def next(self):
            trade_date = self.datas[0].datetime.date(0).strftime("%Y%m%d")
            target_weights = self.p.target_weights_by_date.get(trade_date)
            if target_weights is not None:
                self.rebalance_dates.append(trade_date)
                broker_value = float(self.broker.getvalue())
                sell_orders: list[tuple[str, object, int]] = []
                buy_orders: list[tuple[str, object, int]] = []

                for data in self.datas:
                    asset_id = data._name
                    price = float(data.close[0] or 0.0)
                    if price <= 0.0 or broker_value <= 0.0:
                        continue
                    position = self.getposition(data)
                    current_size = int(position.size)
                    current_value = current_size * price
                    current_weight = current_value / broker_value
                    target_weight = float(target_weights.get(asset_id, 0.0))
                    target_value = broker_value * target_weight
                    target_size = _round_lot_down(target_value / price, self.p.lot_size)
                    if target_weight <= 0.0:
                        target_size = 0
                    trade_weight = abs(target_weight - current_weight)
                    if target_size > current_size and trade_weight < self.p.min_trade_weight:
                        continue
                    if (
                        target_size < current_size
                        and target_weight > 0.0
                        and trade_weight < self.p.min_trade_weight
                    ):
                        continue
                    if target_size < current_size:
                        sell_orders.append((asset_id, data, target_size))
                    elif target_size > current_size:
                        buy_orders.append((asset_id, data, target_size))

                for _, data, target_size in sorted(sell_orders, key=lambda item: item[0]):
                    self.order_target_size(data=data, target=target_size)
                for _, data, target_size in sorted(buy_orders, key=lambda item: item[0]):
                    self.order_target_size(data=data, target=target_size)

            self.equity_curve.append(
                {
                    "trade_date": trade_date,
                    "equity": _round_money(self.broker.getvalue()),
                }
            )

        def notify_order(self, order):
            if order.status != order.Completed:
                return
            execution_date = bt.num2date(order.executed.dt).strftime("%Y%m%d")
            side = "buy" if order.executed.size > 0 else "sell"
            self.fill_records.append(
                {
                    "asset_id": order.data._name,
                    "side": side,
                    "execution_date": execution_date,
                    "quantity": _round_money(abs(order.executed.size)),
                    "price": _round_money(order.executed.price),
                }
            )

    cerebro = bt.Cerebro()
    cerebro.broker.setcash(scenario.initial_cash_cny)
    cerebro.broker.addcommissioninfo(AShareCostInfo())
    for asset_id, frame in frames.items():
        cerebro.adddata(bt.feeds.PandasData(dataname=frame), name=asset_id)
    cerebro.addstrategy(
        DecisionTargetStrategy,
        target_weights_by_date=scenario.target_weights_by_decision_date,
        lot_size=lot_size,
        min_trade_weight=min_trade_weight,
    )
    strategy = cerebro.run()[0]
    return BacktraderPocEngineResult(
        fills=_normalize_fills(list(strategy.fill_records)),
        daily_equity=list(strategy.equity_curve),
        final_equity=_round_money(strategy.broker.getvalue()),
        rebalance_dates=list(strategy.rebalance_dates),
    )


def _artifact_for_scenario(scenario: BacktraderPocScenario) -> SleeveResearchArtifact:
    return SleeveResearchArtifact(
        sleeve_id=scenario.sleeve_id,
        mandate_id=scenario.mandate.id if scenario.mandate is not None else "",
        target_id=scenario.target_id,
        steps=[
            SleeveResearchStep(
                trade_date=trade_date,
                records=[
                    SleeveSignalRecord(
                        asset_id=asset_id,
                        rank=rank,
                        score=float(len(weights_by_asset) - rank + 1),
                        target_weight=target_weight,
                        realized_return=0.0,
                        cost_model_id=(
                            scenario.cost_model.id if scenario.cost_model is not None else ""
                        ),
                    )
                    for rank, (asset_id, target_weight) in enumerate(
                        sorted(
                            weights_by_asset.items(),
                            key=lambda item: (-item[1], item[0]),
                        ),
                        start=1,
                    )
                ],
            )
            for trade_date, weights_by_asset in sorted(
                scenario.target_weights_by_decision_date.items()
            )
        ],
    )


def _create_synthetic_source_db(path: Path, bars: list[DailyBar]) -> None:
    import duckdb

    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE TABLE market_trade_calendar (trade_date VARCHAR)")
        conn.executemany(
            "INSERT INTO market_trade_calendar VALUES (?)",
            [
                (trade_date,)
                for trade_date in sorted({bar.trade_date for bar in bars})
            ],
        )
        conn.execute(
            """
            CREATE TABLE daily_bar_pit (
                security_id VARCHAR,
                trade_date VARCHAR,
                price_basis VARCHAR,
                board VARCHAR,
                is_st BOOLEAN,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                pre_close DOUBLE,
                adj_factor DOUBLE,
                turnover_value_cny DOUBLE
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO daily_bar_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    bar.security_id,
                    bar.trade_date,
                    bar.price_basis,
                    bar.board,
                    bar.is_st,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.pre_close,
                    bar.adj_factor,
                    bar.turnover_value_cny,
                )
                for bar in sorted(bars, key=lambda item: (item.trade_date, item.security_id))
            ],
        )
    finally:
        conn.close()


def _weekly_rotation_targets(
    *,
    asset_ids: tuple[str, ...],
    decision_dates: list[str],
    held_name_count: int,
    weekly_shift: int,
    target_weight: float,
) -> dict[str, dict[str, float]]:
    targets: dict[str, dict[str, float]] = {}
    asset_count = len(asset_ids)
    for index, trade_date in enumerate(decision_dates):
        start = (index * weekly_shift) % asset_count
        held_assets = [asset_ids[(start + offset) % asset_count] for offset in range(held_name_count)]
        targets[trade_date] = {asset_id: target_weight for asset_id in held_assets}
    return targets


def _synthetic_bars_for_assets(
    *,
    asset_ids: tuple[str, ...],
    trade_dates: list[str],
) -> list[DailyBar]:
    bars: list[DailyBar] = []
    for asset_index, asset_id in enumerate(asset_ids):
        previous_close = round(10.0 + asset_index * 0.35, 2)
        for day_index, trade_date in enumerate(trade_dates):
            open_price = previous_close
            daily_return = (
                0.0008 * ((asset_index % 5) - 2)
                + 0.0012 * ((day_index % 4) - 1.5)
            )
            close_price = round(max(open_price * (1.0 + daily_return), 1.0), 2)
            high_price = round(max(open_price, close_price) * 1.003, 2)
            low_price = round(min(open_price, close_price) * 0.997, 2)
            bars.append(
                DailyBar(
                    security_id=asset_id,
                    trade_date=trade_date,
                    price_basis="unadjusted",
                    tradeability_source_priority="official",
                    board="main_board",
                    is_st=False,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    pre_close=previous_close,
                    adj_factor=1.0,
                    turnover_value_cny=10_000_000_000.0,
                    raw_open=open_price,
                    raw_high=high_price,
                    raw_low=low_price,
                    raw_close=close_price,
                    raw_pre_close=previous_close,
                )
            )
            previous_close = close_price
    return bars


def _business_dates(start_date: str, *, count: int) -> list[str]:
    current = datetime.strptime(start_date, "%Y%m%d").date()
    dates: list[str] = []
    while len(dates) < count:
        if current.weekday() < 5:
            dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates


def _normalize_fills(fills: list[dict[str, object]]) -> list[dict[str, object]]:
    return sorted(
        fills,
        key=lambda item: (
            str(item["execution_date"]),
            str(item["asset_id"]),
            str(item["side"]),
            float(item["quantity"]),
        ),
    )


def _round_lot_down(quantity: float, lot_size: int) -> int:
    if lot_size <= 0:
        return int(quantity)
    return int(quantity // lot_size) * lot_size


def _round_money(value: float) -> float:
    return round(float(value), 10)
