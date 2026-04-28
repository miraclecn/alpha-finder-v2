from __future__ import annotations

import json
import math
from pathlib import Path
import subprocess
import statistics
import tempfile
import unittest

import duckdb

from alpha_find_v2.models import (
    CostModel,
    ExecutionPolicy,
    Mandate,
    PortfolioConstructionModel,
    PortfolioRecipe,
)
from alpha_find_v2.portfolio_backtester import (
    PortfolioBacktestInput,
    PortfolioBacktester,
    load_portfolio_backtest_case,
)
from alpha_find_v2.portfolio_promotion_replay import (
    SleeveResearchArtifact,
    SleeveResearchStep,
    SleeveSignalRecord,
)
from alpha_find_v2.portfolio_simulator import TradeConstraintState


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _mandate(participation_cap: float = 1.0) -> Mandate:
    return Mandate(
        id="a_share_long_only_eod",
        name="Test Mandate",
        market="CN-A",
        benchmark="CSI 800",
        account_type="cash_equity",
        description="Synthetic long-only mandate.",
        max_single_name_weight=1.0,
        execution_participation_cap=participation_cap,
        risk={"cash_buffer": 0.0},
    )


def _portfolio() -> PortfolioRecipe:
    return PortfolioRecipe(
        id="test_portfolio",
        name="Test Portfolio",
        mandate_id="a_share_long_only_eod",
        benchmark="CSI 800",
        rebalance_policy="weekly",
        description="Synthetic portfolio.",
        construction_model_id="test_model",
        execution_policy_id="a_share_next_open_v1",
        sleeves=["trend"],
        allocation={"trend": 1.0},
        constraints={"max_names": 4, "max_single_name_weight": 1.0},
    )


def _multi_sleeve_portfolio() -> PortfolioRecipe:
    return PortfolioRecipe(
        id="test_multi_sleeve_portfolio",
        name="Test Multi Sleeve Portfolio",
        mandate_id="a_share_long_only_eod",
        benchmark="CSI 800",
        rebalance_policy="weekly",
        description="Synthetic multi-sleeve portfolio.",
        construction_model_id="test_model",
        execution_policy_id="a_share_next_open_v1",
        sleeves=["trend", "event"],
        allocation={"trend": 0.50, "event": 0.50},
        constraints={"max_names": 4, "max_single_name_weight": 1.0},
    )


def _construction_model() -> PortfolioConstructionModel:
    return PortfolioConstructionModel(
        id="test_model",
        name="Test Model",
        description="Synthetic construction model.",
        sleeve_weight_source="portfolio_allocation",
        overlap_mode="sum",
        name_selection="top_weight",
        excess_weight_policy="hold_cash",
        industry_budget_mode="",
    )


def _execution_policy() -> ExecutionPolicy:
    return ExecutionPolicy(
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
    )


def _cost_model(participation_cap: float = 1.0) -> CostModel:
    return CostModel(
        id="base",
        name="Base",
        description="Synthetic costs.",
        buy_commission_bps=10.0,
        sell_commission_bps=10.0,
        buy_slippage_bps=20.0,
        sell_slippage_bps=20.0,
        sell_stamp_duty_bps=10.0,
        participation_cap=participation_cap,
    )


def _artifact(
    steps: list[tuple[str, list[tuple[str, float, float]]]],
) -> SleeveResearchArtifact:
    return SleeveResearchArtifact(
        sleeve_id="trend",
        mandate_id="a_share_long_only_eod",
        target_id="open_t1_to_open_t20_net_cost",
        steps=[
            SleeveResearchStep(
                trade_date=trade_date,
                records=[
                    SleeveSignalRecord(
                        asset_id=asset_id,
                        rank=rank,
                        score=1.0 / rank,
                        target_weight=weight,
                        realized_return=realized_return,
                        cost_model_id="base",
                    )
                    for rank, (asset_id, weight, realized_return) in enumerate(records, start=1)
                ],
            )
            for trade_date, records in steps
        ],
    )


def _create_db(path: Path, rows: list[dict[str, object]]) -> None:
    conn = duckdb.connect(str(path))
    conn.execute("CREATE TABLE market_trade_calendar (trade_date VARCHAR)")
    conn.executemany(
        "INSERT INTO market_trade_calendar VALUES (?)",
        [(trade_date,) for trade_date in sorted({str(row["trade_date"]) for row in rows})],
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
            open_adj DOUBLE,
            high_adj DOUBLE,
            low_adj DOUBLE,
            close_adj DOUBLE,
            turnover_value_cny DOUBLE
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO daily_bar_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["security_id"],
                row["trade_date"],
                row.get("price_basis", "unadjusted"),
                row.get("board", "main_board"),
                row.get("is_st", False),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
                row.get("pre_close"),
                row.get("adj_factor", 1.0),
                row.get("open_adj", row.get("open")),
                row.get("high_adj", row.get("high")),
                row.get("low_adj", row.get("low")),
                row.get("close_adj", row.get("close")),
                row.get("turnover_value_cny", 100_000_000.0),
            )
            for row in rows
        ],
    )
    conn.close()


class PortfolioBacktesterTest(unittest.TestCase):
    def test_t_plus_one_next_open_fill_and_daily_pnl_ignore_realized_return(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "open": 10.0, "high": 10.1, "low": 9.9, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260106", "open": 10.0, "high": 10.2, "low": 9.8, "close": 11.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260107", "open": 11.0, "high": 11.2, "low": 10.8, "close": 12.0, "pre_close": 11.0},
                ],
            )

            result = self._run(
                db_path,
                _artifact([("2026-01-05", [("AAA", 1.0, -0.99)])]),
                start_date="2026-01-05",
                end_date="2026-01-07",
                initial_cash_cny=10_000.0,
            )

        self.assertEqual(result.orders[0].decision_date, "20260105")
        self.assertEqual(result.orders[0].execution_date, "20260106")
        self.assertEqual(result.fills[0].price, 10.0)
        self.assertEqual(result.fills[0].quantity, 900.0)
        self.assertGreater(result.daily_curve[-1].equity, result.daily_curve[0].equity)
        self.assertNotAlmostEqual(
            result.daily_curve[-1].equity / result.daily_curve[0].equity - 1.0,
            -0.99,
        )

    def test_cash_ledger_costs_lot_rounding_and_no_leverage(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260106", "open": 33.0, "high": 34.0, "low": 32.0, "close": 33.0, "pre_close": 30.0},
                ],
            )

            result = self._run(
                db_path,
                _artifact([("20260105", [("AAA", 1.0, 0.50)])]),
                start_date="20260105",
                end_date="20260106",
                initial_cash_cny=10_000.0,
            )

        self.assertEqual(result.fills[0].quantity, 300.0)
        self.assertEqual(result.fills[0].side, "buy")
        self.assertAlmostEqual(result.fills[0].gross_value, 9_900.0)
        self.assertAlmostEqual(result.fills[0].cost, 29.70)
        self.assertGreaterEqual(result.daily_curve[-1].cash, 0.0)
        self.assertLess(result.daily_curve[-1].cash, 100.0)

    def test_sell_to_zero_liquidates_odd_lot_but_partial_sell_uses_lots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260106", "open": 33.0, "high": 34.0, "low": 32.0, "close": 33.0, "pre_close": 30.0},
                    {"security_id": "AAA", "trade_date": "20260107", "open": 33.0, "high": 33.0, "low": 33.0, "close": 33.0, "pre_close": 33.0},
                    {"security_id": "BBB", "trade_date": "20260107", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260108", "open": 33.0, "high": 33.0, "low": 33.0, "close": 33.0, "pre_close": 33.0},
                    {"security_id": "BBB", "trade_date": "20260108", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                ],
            )

            result = self._run(
                db_path,
                _artifact(
                    [
                        ("20260105", [("AAA", 1.0, 0.0)]),
                        ("20260106", [("AAA", 0.61, 0.0), ("BBB", 0.39, 0.0)]),
                        ("20260107", []),
                    ]
                ),
                start_date="20260105",
                end_date="20260108",
                initial_cash_cny=10_000.0,
            )

        sell_fills = [
            fill for fill in result.fills if fill.side == "sell" and fill.asset_id == "AAA"
        ]
        self.assertEqual([fill.quantity for fill in sell_fills], [100.0, 200.0])
        self.assertAlmostEqual(result.daily_curve[-1].positions_value, 0.0)

    def test_limit_blocks_and_suspension_carries_last_mark(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "BBB", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260106", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "BBB", "trade_date": "20260106", "open": 11.0, "high": 11.0, "low": 11.0, "close": 11.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260107", "open": None, "high": None, "low": None, "close": None, "pre_close": 10.0},
                    {"security_id": "BBB", "trade_date": "20260107", "open": 11.0, "high": 11.0, "low": 11.0, "close": 11.0, "pre_close": 11.0},
                    {"security_id": "AAA", "trade_date": "20260108", "open": 9.0, "high": 9.0, "low": 9.0, "close": 9.0, "pre_close": 10.0},
                    {"security_id": "BBB", "trade_date": "20260108", "open": 11.0, "high": 11.0, "low": 11.0, "close": 11.0, "pre_close": 11.0},
                ],
            )

            result = self._run(
                db_path,
                _artifact(
                    [
                        ("20260105", [("AAA", 1.0, 0.0), ("BBB", 1.0, 0.0)]),
                        ("20260106", []),
                        ("20260107", []),
                    ]
                ),
                start_date="20260105",
                end_date="20260108",
                initial_cash_cny=10_000.0,
            )

        blocked = {(item.asset_id, item.side, item.reason) for item in result.diagnostics.blocked_orders}
        self.assertIn(("BBB", "buy", "limit_up_open_lock"), blocked)
        self.assertIn(("AAA", "sell", "suspended_or_missing_open"), blocked)
        self.assertIn(("AAA", "sell", "limit_down_open_lock"), blocked)
        suspended_day = next(day for day in result.daily_curve if day.trade_date == "20260107")
        self.assertGreater(suspended_day.positions_value, 0.0)

    def test_signal_trade_state_blocks_entry_and_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "BBB", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "CCC", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260106", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "BBB", "trade_date": "20260106", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "CCC", "trade_date": "20260106", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260107", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "BBB", "trade_date": "20260107", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "CCC", "trade_date": "20260107", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                ],
            )
            artifact = SleeveResearchArtifact(
                sleeve_id="trend",
                mandate_id="a_share_long_only_eod",
                target_id="open_t1_to_open_t20_net_cost",
                steps=[
                    SleeveResearchStep(
                        trade_date="20260105",
                        records=[
                            SleeveSignalRecord(
                                asset_id="AAA",
                                rank=1,
                                score=1.0,
                                target_weight=0.50,
                                realized_return=0.0,
                                cost_model_id="base",
                                trade_state=TradeConstraintState(can_enter=False),
                            ),
                            SleeveSignalRecord(
                                asset_id="BBB",
                                rank=2,
                                score=0.5,
                                target_weight=0.50,
                                realized_return=0.0,
                                cost_model_id="base",
                            ),
                        ],
                    ),
                    SleeveResearchStep(
                        trade_date="20260106",
                        records=[
                            SleeveSignalRecord(
                                asset_id="BBB",
                                rank=1,
                                score=1.0,
                                target_weight=0.25,
                                realized_return=0.0,
                                cost_model_id="base",
                                trade_state=TradeConstraintState(can_exit=False),
                            ),
                            SleeveSignalRecord(
                                asset_id="CCC",
                                rank=2,
                                score=0.5,
                                target_weight=0.75,
                                realized_return=0.0,
                                cost_model_id="base",
                            )
                        ],
                    ),
                ],
            )

            result = self._run(
                db_path,
                artifact,
                start_date="20260105",
                end_date="20260107",
                initial_cash_cny=10_000.0,
            )

        self.assertNotIn("AAA", {fill.asset_id for fill in result.fills})
        blocked = {(item.asset_id, item.side, item.reason) for item in result.diagnostics.blocked_orders}
        self.assertIn(("AAA", "buy", "trade_state_entry_block"), blocked)
        self.assertIn(("BBB", "sell", "trade_state_exit_block"), blocked)

    def test_exit_for_dropped_asset_uses_position_cost_model(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260106", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260107", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                ],
            )
            base_model = _cost_model()
            high_model = CostModel(
                id="high",
                name="High",
                description="High sell cost.",
                buy_commission_bps=0.0,
                sell_commission_bps=1000.0,
                buy_slippage_bps=0.0,
                sell_slippage_bps=0.0,
                sell_stamp_duty_bps=0.0,
                participation_cap=1.0,
            )

            result = PortfolioBacktester(
                mandate=_mandate(),
                portfolio=_portfolio(),
                construction_model=_construction_model(),
                execution_policy=_execution_policy(),
                default_cost_model=base_model,
                cost_models={"base": base_model, "high": high_model},
                source_db_path=db_path,
            ).run(
                PortfolioBacktestInput(
                    portfolio=_portfolio(),
                    artifacts=[
                        SleeveResearchArtifact(
                            sleeve_id="trend",
                            mandate_id="a_share_long_only_eod",
                            target_id="open_t1_to_open_t20_net_cost",
                            steps=[
                                SleeveResearchStep(
                                    trade_date="20260105",
                                    records=[
                                        SleeveSignalRecord(
                                            asset_id="AAA",
                                            rank=1,
                                            score=1.0,
                                            target_weight=1.0,
                                            realized_return=0.0,
                                            cost_model_id="high",
                                        )
                                    ],
                                ),
                                SleeveResearchStep(trade_date="20260106", records=[]),
                            ],
                        )
                    ],
                    start_date="20260105",
                    end_date="20260107",
                    initial_cash_cny=10_000.0,
                )
            )

        sell_fill = next(fill for fill in result.fills if fill.side == "sell")
        self.assertEqual(sell_fill.cost_model_id, "high")
        self.assertAlmostEqual(sell_fill.cost, 1_000.0)

    def test_participation_limited_sell_to_zero_uses_lots_for_partial_fill(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260106", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260107", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0, "turnover_value_cny": 1_505.0},
                ],
            )

            result = self._run(
                db_path,
                _artifact(
                    [
                        ("20260105", [("AAA", 1.0, 0.0)]),
                        ("20260106", []),
                    ]
                ),
                start_date="20260105",
                end_date="20260107",
                initial_cash_cny=10_000.0,
                cost_model=CostModel(
                    id="base",
                    name="Base",
                    description="No cost.",
                    buy_commission_bps=0.0,
                    sell_commission_bps=0.0,
                    buy_slippage_bps=0.0,
                    sell_slippage_bps=0.0,
                    sell_stamp_duty_bps=0.0,
                    participation_cap=1.0,
                ),
            )

        sell_fill = next(fill for fill in result.fills if fill.side == "sell")
        self.assertEqual(sell_fill.quantity, 100.0)
        sell_partial = next(
            partial for partial in result.diagnostics.partial_fills if partial.side == "sell"
        )
        self.assertEqual(sell_partial.filled_quantity, 100.0)

    def test_qfq_fallback_prices_do_not_apply_adj_factor_share_multiplier(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "price_basis": "qfq_fallback", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0, "adj_factor": 2.0},
                    {"security_id": "AAA", "trade_date": "20260106", "price_basis": "qfq_fallback", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0, "adj_factor": 2.0},
                    {"security_id": "AAA", "trade_date": "20260107", "price_basis": "qfq_fallback", "open": 12.0, "high": 12.0, "low": 12.0, "close": 12.0, "pre_close": 10.0, "adj_factor": 4.0},
                ],
            )

            result = self._run(
                db_path,
                _artifact([("20260105", [("AAA", 1.0, 0.0)])]),
                start_date="20260105",
                end_date="20260107",
                initial_cash_cny=10_000.0,
                cost_model=CostModel(
                    id="base",
                    name="Base",
                    description="No cost.",
                    buy_commission_bps=0.0,
                    sell_commission_bps=0.0,
                    buy_slippage_bps=0.0,
                    sell_slippage_bps=0.0,
                    sell_stamp_duty_bps=0.0,
                    participation_cap=1.0,
                ),
            )

        self.assertAlmostEqual(result.daily_curve[-1].equity, 12_000.0)

    def test_adjusted_price_columns_drive_fills_and_marks_without_share_multiplier(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {
                        "security_id": "AAA",
                        "trade_date": "20260105",
                        "open": 10.0,
                        "high": 10.0,
                        "low": 10.0,
                        "close": 10.0,
                        "pre_close": 10.0,
                        "adj_factor": 2.0,
                        "open_adj": 20.0,
                        "high_adj": 20.0,
                        "low_adj": 20.0,
                        "close_adj": 20.0,
                    },
                    {
                        "security_id": "AAA",
                        "trade_date": "20260106",
                        "open": 10.0,
                        "high": 10.0,
                        "low": 10.0,
                        "close": 10.0,
                        "pre_close": 10.0,
                        "adj_factor": 2.0,
                        "open_adj": 20.0,
                        "high_adj": 20.0,
                        "low_adj": 20.0,
                        "close_adj": 20.0,
                    },
                    {
                        "security_id": "AAA",
                        "trade_date": "20260107",
                        "open": 12.0,
                        "high": 12.0,
                        "low": 12.0,
                        "close": 12.0,
                        "pre_close": 10.0,
                        "adj_factor": 4.0,
                        "open_adj": 24.0,
                        "high_adj": 24.0,
                        "low_adj": 24.0,
                        "close_adj": 24.0,
                    },
                ],
            )

            result = self._run(
                db_path,
                _artifact([("20260105", [("AAA", 1.0, 0.0)])]),
                start_date="20260105",
                end_date="20260107",
                initial_cash_cny=10_000.0,
                cost_model=CostModel(
                    id="base",
                    name="Base",
                    description="No cost.",
                    buy_commission_bps=0.0,
                    sell_commission_bps=0.0,
                    buy_slippage_bps=0.0,
                    sell_slippage_bps=0.0,
                    sell_stamp_duty_bps=0.0,
                    participation_cap=1.0,
                ),
            )

        self.assertEqual(result.fills[0].price, 20.0)
        self.assertEqual(result.fills[0].quantity, 500.0)
        self.assertAlmostEqual(result.daily_curve[-1].equity, 12_000.0)

    def test_limit_lock_checks_use_raw_prices_when_backtest_uses_adjusted_prices(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {
                        "security_id": "AAA",
                        "trade_date": "20260105",
                        "open": 10.0,
                        "high": 10.0,
                        "low": 10.0,
                        "close": 10.0,
                        "pre_close": 10.0,
                        "open_adj": 20.0,
                        "high_adj": 20.0,
                        "low_adj": 20.0,
                        "close_adj": 20.0,
                    },
                    {
                        "security_id": "AAA",
                        "trade_date": "20260106",
                        "open": 10.0,
                        "high": 10.0,
                        "low": 10.0,
                        "close": 10.0,
                        "pre_close": 10.0,
                        "open_adj": 20.0,
                        "high_adj": 20.0,
                        "low_adj": 20.0,
                        "close_adj": 20.0,
                    },
                ],
            )

            result = self._run(
                db_path,
                _artifact([("20260105", [("AAA", 1.0, 0.0)])]),
                start_date="20260105",
                end_date="20260106",
                initial_cash_cny=10_000.0,
                cost_model=CostModel(
                    id="base",
                    name="Base",
                    description="No cost.",
                    buy_commission_bps=0.0,
                    sell_commission_bps=0.0,
                    buy_slippage_bps=0.0,
                    sell_slippage_bps=0.0,
                    sell_stamp_duty_bps=0.0,
                    participation_cap=1.0,
                ),
            )

        self.assertEqual(len(result.fills), 1)
        self.assertFalse(result.diagnostics.blocked_orders)

    def test_multi_sleeve_backtest_requires_artifacts_on_union_decision_calendar(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "BBB", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260106", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "BBB", "trade_date": "20260106", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260107", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "BBB", "trade_date": "20260107", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                ],
            )
            model = _cost_model()
            trend_artifact = _artifact([("20260105", [("AAA", 1.0, 0.0)])])
            event_artifact = SleeveResearchArtifact(
                sleeve_id="event",
                mandate_id="a_share_long_only_eod",
                target_id="open_t1_to_open_t20_net_cost",
                steps=[
                    SleeveResearchStep(
                        trade_date="20260105",
                        records=[
                            SleeveSignalRecord(
                                asset_id="BBB",
                                rank=1,
                                score=1.0,
                                target_weight=1.0,
                                realized_return=0.0,
                                cost_model_id="base",
                            )
                        ],
                    ),
                    SleeveResearchStep(
                        trade_date="20260106",
                        records=[
                            SleeveSignalRecord(
                                asset_id="BBB",
                                rank=1,
                                score=1.0,
                                target_weight=1.0,
                                realized_return=0.0,
                                cost_model_id="base",
                            )
                        ],
                    ),
                ],
            )

            with self.assertRaisesRegex(
                ValueError,
                "Sleeve artifact trend must cover trade date 20260106",
            ):
                PortfolioBacktester(
                    mandate=_mandate(),
                    portfolio=_multi_sleeve_portfolio(),
                    construction_model=_construction_model(),
                    execution_policy=_execution_policy(),
                    default_cost_model=model,
                    cost_models={model.id: model},
                    source_db_path=db_path,
                ).run(
                    PortfolioBacktestInput(
                        portfolio=_multi_sleeve_portfolio(),
                        artifacts=[trend_artifact, event_artifact],
                        start_date="20260105",
                        end_date="20260107",
                        initial_cash_cny=10_000.0,
                    )
                )

    def test_participation_cap_records_partial_fill(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260106", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0, "turnover_value_cny": 10_000.0},
                ],
            )

            result = self._run(
                db_path,
                _artifact([("20260105", [("AAA", 1.0, 0.0)])]),
                start_date="20260105",
                end_date="20260106",
                initial_cash_cny=100_000.0,
                mandate=_mandate(participation_cap=0.10),
                cost_model=_cost_model(participation_cap=0.10),
            )

        self.assertEqual(result.fills[0].quantity, 100.0)
        self.assertEqual(result.diagnostics.partial_fills[0].asset_id, "AAA")
        self.assertAlmostEqual(result.summary.partial_fill_share, 1.0)

    def test_equity_curve_summary_uses_daily_mark_to_market_drawdown(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260106", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260107", "open": 10.0, "high": 10.0, "low": 10.0, "close": 7.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260108", "open": 8.0, "high": 12.0, "low": 8.0, "close": 12.0, "pre_close": 8.0},
                ],
            )

            result = self._run(
                db_path,
                _artifact([("20260105", [("AAA", 1.0, 0.99)])]),
                start_date="20260105",
                end_date="20260108",
                initial_cash_cny=10_000.0,
            )

        self.assertLess(result.summary.max_drawdown, -0.19)
        self.assertGreater(result.summary.total_return, 0.0)
        self.assertIn("2026", result.summary.yearly_returns)

    def test_summary_reports_benchmark_relative_metrics_and_symmetric_turnover(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "BBB", "trade_date": "20260105", "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "pre_close": 100.0},
                    {"security_id": "AAA", "trade_date": "20260106", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "BBB", "trade_date": "20260106", "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "pre_close": 100.0},
                    {"security_id": "AAA", "trade_date": "20260107", "open": 11.0, "high": 11.0, "low": 11.0, "close": 11.0, "pre_close": 10.0},
                    {"security_id": "BBB", "trade_date": "20260107", "open": 102.0, "high": 102.0, "low": 102.0, "close": 102.0, "pre_close": 100.0},
                    {"security_id": "AAA", "trade_date": "20260108", "open": 12.1, "high": 12.1, "low": 12.1, "close": 12.1, "pre_close": 11.0},
                    {"security_id": "BBB", "trade_date": "20260108", "open": 103.0, "high": 103.0, "low": 103.0, "close": 103.0, "pre_close": 102.0},
                ],
            )
            no_cost = CostModel(
                id="base",
                name="Base",
                description="No cost.",
                buy_commission_bps=0.0,
                sell_commission_bps=0.0,
                buy_slippage_bps=0.0,
                sell_slippage_bps=0.0,
                sell_stamp_duty_bps=0.0,
                participation_cap=1.0,
            )

            result = PortfolioBacktester(
                mandate=_mandate(),
                portfolio=_portfolio(),
                construction_model=_construction_model(),
                execution_policy=_execution_policy(),
                default_cost_model=no_cost,
                cost_models={no_cost.id: no_cost},
                source_db_path=db_path,
            ).run(
                PortfolioBacktestInput(
                    portfolio=_portfolio(),
                    artifacts=[_artifact([("20260105", [("AAA", 1.0, 0.0)])])],
                    start_date="20260105",
                    end_date="20260108",
                    initial_cash_cny=10_000.0,
                    risk_free_rate_annual=0.02,
                    benchmark_constituent_weights_by_date={
                        "20260105": {"BBB": 1.0},
                        "20260106": {"BBB": 1.0},
                        "20260107": {"BBB": 1.0},
                    },
                )
            )

        portfolio_returns = [0.0, 0.10, 0.10]
        benchmark_returns = [0.0, 0.02, (103.0 / 102.0) - 1.0]
        active_returns = [
            portfolio_return - benchmark_return
            for portfolio_return, benchmark_return in zip(
                portfolio_returns,
                benchmark_returns,
                strict=True,
            )
        ]
        expected_volatility = statistics.stdev(portfolio_returns) * math.sqrt(252.0)
        expected_tracking_error = statistics.stdev(active_returns) * math.sqrt(252.0)
        expected_active_return = statistics.mean(active_returns) * 252.0
        average_equity = statistics.mean(state.equity for state in result.daily_curve)

        summary = result.summary
        self.assertAlmostEqual(summary.risk_free_rate_annual, 0.02)
        self.assertAlmostEqual(summary.sharpe, (summary.annualized_return - 0.02) / expected_volatility)
        self.assertNotAlmostEqual(summary.information_ratio, summary.sharpe)
        self.assertAlmostEqual(summary.benchmark_annualized_return, statistics.mean(benchmark_returns) * 252.0)
        self.assertAlmostEqual(summary.active_annualized_return, expected_active_return)
        self.assertAlmostEqual(summary.tracking_error, expected_tracking_error)
        self.assertAlmostEqual(summary.information_ratio, expected_active_return / expected_tracking_error)
        self.assertAlmostEqual(summary.buy_turnover, 10_000.0 / average_equity)
        self.assertAlmostEqual(summary.sell_turnover, 0.0)
        self.assertAlmostEqual(summary.turnover, 5_000.0 / average_equity)

    def test_summary_information_ratio_is_zero_without_benchmark_returns(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260106", "open": 10.0, "high": 10.0, "low": 10.0, "close": 11.0, "pre_close": 10.0},
                ],
            )

            result = self._run(
                db_path,
                _artifact([("20260105", [("AAA", 1.0, 0.0)])]),
                start_date="20260105",
                end_date="20260106",
                initial_cash_cny=10_000.0,
            )

        self.assertEqual(result.summary.information_ratio, 0.0)
        self.assertEqual(result.summary.tracking_error, 0.0)

    def test_case_loader_and_cli_write_json_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            db_path = temp_root / "source.duckdb"
            _create_db(
                db_path,
                [
                    {"security_id": "AAA", "trade_date": "20260105", "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "pre_close": 10.0},
                    {"security_id": "AAA", "trade_date": "20260106", "open": 10.0, "high": 10.0, "low": 10.0, "close": 11.0, "pre_close": 10.0},
                ],
            )
            artifact_path = temp_root / "artifact.json"
            artifact_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "artifact_type": "sleeve_research_artifact",
                        "sleeve_id": "trend",
                        "mandate_id": "a_share_long_only_eod",
                        "target_id": "open_t1_to_open_t20_net_cost",
                        "steps": [
                            {
                                "trade_date": "20260105",
                                "records": [
                                    {
                                        "asset_id": "AAA",
                                        "rank": 1,
                                        "score": 1.0,
                                        "target_weight": 1.0,
                                        "realized_return": -0.99,
                                        "cost_model_id": "base_a_share_cash",
                                        "industry": "bank",
                                    }
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            portfolio_path = temp_root / "portfolio.toml"
            portfolio_path.write_text(
                """
id = "test_portfolio"
name = "Test Portfolio"
mandate_id = "a_share_long_only_eod"
benchmark = "CSI 800"
rebalance_policy = "weekly"
description = "Synthetic portfolio."
construction_model_id = "a_share_core_blend"
execution_policy_id = "a_share_next_open_v1"
sleeves = ["trend"]

[allocation]
trend = 1.0

[constraints]
max_names = 4
max_single_name_weight = 1.0
max_industry_overweight = 1.0
""".strip()
                + "\n",
                encoding="utf-8",
            )
            benchmark_state_path = temp_root / "benchmark_state.json"
            benchmark_state_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "artifact_type": "benchmark_state_history",
                        "benchmark_id": "CSI 800",
                        "classification": "citics_l1",
                        "weighting_method": "manual_sample",
                        "steps": [
                            {
                                "trade_date": "20260105",
                                "effective_at": "2026-01-05T15:00:00+08:00",
                                "available_at": "2026-01-05T15:30:00+08:00",
                                "industry_weights": {"bank": 1.0},
                                "constituents": [
                                    {"asset_id": "AAA", "weight": 1.0, "industry": "bank"}
                                ],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            output_path = temp_root / "backtest.json"
            case_path = temp_root / "case.toml"
            case_path.write_text(
                f"""
schema_version = 1
artifact_type = "portfolio_backtest_case"
case_id = "synthetic_case"
description = "Synthetic case."
portfolio_path = "{portfolio_path}"
artifact_paths = ["{artifact_path}"]
source_db_path = "{db_path}"
execution_policy_path = "config/execution_policies/a_share_next_open_v1.toml"
default_cost_model_path = "config/cost_models/base_a_share_cash.toml"
benchmark_state_path = "{benchmark_state_path}"
start_date = "2026-01-05"
end_date = "2026-01-06"
initial_cash_cny = 10000
output_path = "{output_path}"
""".strip()
                + "\n",
                encoding="utf-8",
            )

            loaded_case = load_portfolio_backtest_case(case_path)
            self.assertEqual(loaded_case.definition.case_id, "synthetic_case")

            process = subprocess.run(
                [
                    "python3",
                    "-m",
                    "alpha_find_v2",
                    "run-portfolio-backtest",
                    "--case",
                    str(case_path),
                ],
                cwd=PROJECT_ROOT,
                env={"PYTHONPATH": "src"},
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(process.returncode, 0, msg=process.stderr)
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["case_id"], "synthetic_case")
            self.assertIn("daily_curve", payload["artifact"])
            self.assertIn("summary", payload["artifact"])

    def _run(
        self,
        db_path: Path,
        artifact: SleeveResearchArtifact,
        *,
        start_date: str,
        end_date: str,
        initial_cash_cny: float,
        mandate: Mandate | None = None,
        cost_model: CostModel | None = None,
    ):
        model = cost_model or _cost_model()
        return PortfolioBacktester(
            mandate=mandate or _mandate(),
            portfolio=_portfolio(),
            construction_model=_construction_model(),
            execution_policy=_execution_policy(),
            default_cost_model=model,
            cost_models={model.id: model},
            source_db_path=db_path,
        ).run(
            PortfolioBacktestInput(
                portfolio=_portfolio(),
                artifacts=[artifact],
                start_date=start_date,
                end_date=end_date,
                initial_cash_cny=initial_cash_cny,
            )
        )


if __name__ == "__main__":
    unittest.main()
