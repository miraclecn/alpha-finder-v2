from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any, Iterable

import duckdb
import numpy as np
import pandas as pd


@dataclass(frozen=True, slots=True)
class FixedTpSlPolicy:
    take_profit: float
    stop_loss: float
    max_hold_days: int

    @property
    def policy_name(self) -> str:
        return (
            f"fixed_tp{self.take_profit:.2f}_sl{self.stop_loss:.2f}"
            f"_hold{self.max_hold_days}"
        )


@dataclass(frozen=True, slots=True)
class DynamicTrailingPolicy:
    stop_loss: float
    activation_return: float
    trailing_drawdown: float

    @property
    def policy_name(self) -> str:
        return (
            f"dynamic_sl{self.stop_loss:.2f}_act{self.activation_return:.2f}"
            f"_trail{self.trailing_drawdown:.2f}"
        )


@dataclass(slots=True)
class _Position:
    position_id: int
    security_id: str
    signal_trade_date: str
    signal_origin: str
    entry_trade_date: str
    entry_index: int
    entry_price: float
    shares: int
    gross_entry_value: float
    buy_cost: float
    path_score: float
    last_price: float
    peak_price: float = 0.0
    trailing_active: bool = False


_CURVE_COLUMNS = [
    "year",
    "trade_date",
    "policy_name",
    "portfolio_value",
    "cash",
    "position_value",
    "daily_return",
    "gross_exposure",
    "active_positions",
    "entries",
    "exits",
    "skipped_entries",
]

_TRADE_COLUMNS = [
    "year",
    "policy_name",
    "position_id",
    "security_id",
    "signal_origin",
    "signal_trade_date",
    "entry_trade_date",
    "exit_trade_date",
    "holding_days",
    "exit_reason",
    "shares",
    "entry_price",
    "exit_price",
    "gross_entry_value",
    "gross_exit_value",
    "buy_cost",
    "sell_cost",
    "gross_ret",
    "net_ret",
    "path_score",
]


def load_selected_bars_for_slot_backtest(
    *,
    source_db_path: Path,
    security_ids: Iterable[str],
    query_start: str,
    query_end: str,
) -> pd.DataFrame:
    unique_ids = sorted({str(security_id) for security_id in security_ids})
    columns = [
        "security_id",
        "trade_date",
        "open_adj",
        "high_adj",
        "low_adj",
        "close_adj",
        "turnover_value_cny",
    ]
    if not unique_ids:
        return pd.DataFrame(columns=columns)

    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        security_frame = pd.DataFrame({"security_id": unique_ids})
        conn.register("selected_security_ids", security_frame)
        bars = conn.execute(
            """
            SELECT
                d.security_id,
                d.trade_date,
                d.open_adj,
                d.high_adj,
                d.low_adj,
                d.close_adj,
                d.turnover_value_cny
            FROM daily_bar_pit d
            JOIN selected_security_ids s
              ON d.security_id = s.security_id
            WHERE d.trade_date BETWEEN ? AND ?
              AND d.price_basis = 'unadjusted'
            ORDER BY d.security_id, d.trade_date
            """,
            [query_start, query_end],
        ).fetchdf()
    finally:
        conn.close()
    if bars.empty:
        return pd.DataFrame(columns=columns)
    return bars[columns]


def load_trade_calendar(
    *,
    source_db_path: Path,
    query_start: str,
    query_end: str,
) -> list[str]:
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        calendar = conn.execute(
            """
            SELECT DISTINCT trade_date
            FROM daily_bar_pit
            WHERE trade_date BETWEEN ? AND ?
              AND price_basis = 'unadjusted'
            ORDER BY trade_date
            """,
            [query_start, query_end],
        ).fetchdf()
    finally:
        conn.close()
    return calendar["trade_date"].astype(str).tolist() if not calendar.empty else []


def run_fixed_slot_portfolio_backtest(
    *,
    selected_rows: pd.DataFrame,
    bars: pd.DataFrame,
    calendar: list[str],
    policy: FixedTpSlPolicy,
    initial_cash: float = 10_000_000.0,
    slot_count: int = 100,
    lot_size: int = 100,
    buy_cost_bps: float = 12.0,
    sell_cost_bps: float = 12.0,
    bar_lookup: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if slot_count <= 0:
        raise ValueError("slot_count must be positive")
    if lot_size <= 0:
        raise ValueError("lot_size must be positive")

    clean_calendar = sorted(str(date) for date in calendar)
    if not clean_calendar:
        return pd.DataFrame(columns=_CURVE_COLUMNS), pd.DataFrame(columns=_TRADE_COLUMNS)

    year = int(clean_calendar[0][:4])
    date_to_index = {trade_date: index for index, trade_date in enumerate(clean_calendar)}
    next_trade_date = {
        clean_calendar[index]: clean_calendar[index + 1]
        for index in range(len(clean_calendar) - 1)
    }
    last_trade_date = clean_calendar[-1]

    effective_bar_lookup = bar_lookup if bar_lookup is not None else _build_bar_lookup(bars)
    signals_by_entry_date = _signals_by_entry_date(
        selected_rows=selected_rows,
        next_trade_date=next_trade_date,
        allowed_entry_dates=set(clean_calendar[:-1]),
    )

    cash = float(initial_cash)
    buy_cost_rate = buy_cost_bps / 10_000.0
    sell_cost_rate = sell_cost_bps / 10_000.0
    slot_cash = float(initial_cash) / float(slot_count)
    active_positions: list[_Position] = []
    trade_rows: list[dict[str, Any]] = []
    curve_rows: list[dict[str, Any]] = []
    previous_value = float(initial_cash)
    next_position_id = 1

    for trade_date in clean_calendar:
        entries = 0
        exits = 0
        skipped_entries = 0

        todays_signals = signals_by_entry_date.get(trade_date, [])
        for signal in todays_signals:
            if len(active_positions) >= slot_count:
                skipped_entries += 1
                continue
            if any(position.security_id == signal["security_id"] for position in active_positions):
                skipped_entries += 1
                continue
            bar = effective_bar_lookup.get((signal["security_id"], trade_date))
            if bar is None or not _is_finite_positive(bar.get("open_adj")):
                skipped_entries += 1
                continue

            entry_price = float(bar["open_adj"])
            gross_budget = min(slot_cash, cash / (1.0 + buy_cost_rate))
            shares = int(math.floor(gross_budget / entry_price / lot_size) * lot_size)
            if shares <= 0:
                skipped_entries += 1
                continue

            gross_entry_value = float(shares) * entry_price
            buy_cost = gross_entry_value * buy_cost_rate
            cash -= gross_entry_value + buy_cost
            active_positions.append(
                _Position(
                    position_id=next_position_id,
                    security_id=signal["security_id"],
                    signal_trade_date=signal["signal_trade_date"],
                    signal_origin=signal["signal_origin"],
                    entry_trade_date=trade_date,
                    entry_index=date_to_index[trade_date],
                    entry_price=entry_price,
                    shares=shares,
                    gross_entry_value=gross_entry_value,
                    buy_cost=buy_cost,
                    path_score=signal["path_score"],
                    last_price=float(bar.get("close_adj", entry_price))
                    if _is_finite_positive(bar.get("close_adj"))
                    else entry_price,
                )
            )
            entries += 1
            next_position_id += 1

        remaining_positions: list[_Position] = []
        for position in active_positions:
            bar = effective_bar_lookup.get((position.security_id, trade_date))
            if bar is not None and _is_finite_positive(bar.get("close_adj")):
                position.last_price = float(bar["close_adj"])

            exit_decision = _fixed_exit_decision(
                position=position,
                bar=bar,
                trade_date=trade_date,
                date_index=date_to_index[trade_date],
                last_trade_date=last_trade_date,
                policy=policy,
            )
            if exit_decision is None:
                remaining_positions.append(position)
                continue

            exit_price, exit_reason = exit_decision
            gross_exit_value = float(position.shares) * exit_price
            sell_cost = gross_exit_value * sell_cost_rate
            cash += gross_exit_value - sell_cost
            trade_rows.append(
                {
                    "year": year,
                    "policy_name": policy.policy_name,
                    "position_id": position.position_id,
                    "security_id": position.security_id,
                    "signal_origin": position.signal_origin,
                    "signal_trade_date": position.signal_trade_date,
                    "entry_trade_date": position.entry_trade_date,
                    "exit_trade_date": trade_date,
                    "holding_days": int(date_to_index[trade_date] - position.entry_index),
                    "exit_reason": exit_reason,
                    "shares": position.shares,
                    "entry_price": position.entry_price,
                    "exit_price": exit_price,
                    "gross_entry_value": position.gross_entry_value,
                    "gross_exit_value": gross_exit_value,
                    "buy_cost": position.buy_cost,
                    "sell_cost": sell_cost,
                    "gross_ret": exit_price / position.entry_price - 1.0,
                    "net_ret": (
                        (gross_exit_value - sell_cost)
                        / (position.gross_entry_value + position.buy_cost)
                        - 1.0
                    ),
                    "path_score": position.path_score,
                }
            )
            exits += 1
        active_positions = remaining_positions

        position_value = sum(float(position.shares) * position.last_price for position in active_positions)
        portfolio_value = cash + position_value
        daily_return = portfolio_value / previous_value - 1.0 if previous_value > 0.0 else 0.0
        curve_rows.append(
            {
                "year": year,
                "trade_date": trade_date,
                "policy_name": policy.policy_name,
                "portfolio_value": portfolio_value,
                "cash": cash,
                "position_value": position_value,
                "daily_return": daily_return,
                "gross_exposure": position_value / portfolio_value if portfolio_value > 0.0 else 0.0,
                "active_positions": len(active_positions),
                "entries": entries,
                "exits": exits,
                "skipped_entries": skipped_entries,
            }
        )
        previous_value = portfolio_value

    return (
        pd.DataFrame(curve_rows, columns=_CURVE_COLUMNS),
        pd.DataFrame(trade_rows, columns=_TRADE_COLUMNS),
    )


def run_annual_fixed_slot_portfolio_backtests(
    *,
    selected_rows: pd.DataFrame,
    bars: pd.DataFrame,
    calendar: list[str],
    policy: FixedTpSlPolicy,
    initial_cash: float = 10_000_000.0,
    slot_count: int = 100,
    lot_size: int = 100,
    buy_cost_bps: float = 12.0,
    sell_cost_bps: float = 12.0,
    bar_lookup: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not calendar:
        return pd.DataFrame(columns=_CURVE_COLUMNS), pd.DataFrame(columns=_TRADE_COLUMNS)

    years = sorted({int(str(date)[:4]) for date in calendar})
    curve_parts: list[pd.DataFrame] = []
    trade_parts: list[pd.DataFrame] = []
    working_rows = selected_rows.copy()
    if not working_rows.empty:
        working_rows["trade_date"] = working_rows["trade_date"].astype(str)
        working_rows["year"] = working_rows.get(
            "year",
            working_rows["trade_date"].str.slice(0, 4).astype(int),
        ).astype(int)

    effective_bar_lookup = bar_lookup if bar_lookup is not None else _build_bar_lookup(bars)
    for year in years:
        year_calendar = [date for date in calendar if str(date).startswith(str(year))]
        if not year_calendar:
            continue
        year_rows = working_rows.loc[working_rows["year"] == year].copy() if not working_rows.empty else working_rows
        curve, trades = run_fixed_slot_portfolio_backtest(
            selected_rows=year_rows,
            bars=bars,
            calendar=year_calendar,
            policy=policy,
            initial_cash=initial_cash,
            slot_count=slot_count,
            lot_size=lot_size,
            buy_cost_bps=buy_cost_bps,
            sell_cost_bps=sell_cost_bps,
            bar_lookup=effective_bar_lookup,
        )
        curve_parts.append(curve)
        trade_parts.append(trades)

    curve_result = pd.concat(curve_parts, ignore_index=True) if curve_parts else pd.DataFrame(columns=_CURVE_COLUMNS)
    trade_result = pd.concat(trade_parts, ignore_index=True) if trade_parts else pd.DataFrame(columns=_TRADE_COLUMNS)
    return curve_result, trade_result


def run_dynamic_slot_portfolio_backtest(
    *,
    selected_rows: pd.DataFrame,
    bars: pd.DataFrame,
    calendar: list[str],
    policy: DynamicTrailingPolicy,
    initial_cash: float = 10_000_000.0,
    max_positions: int = 10,
    target_position_fraction: float | None = None,
    lot_size: int = 100,
    buy_cost_bps: float = 12.0,
    sell_cost_bps: float = 12.0,
    bar_lookup: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if max_positions <= 0:
        raise ValueError("max_positions must be positive")
    if target_position_fraction is not None and target_position_fraction <= 0.0:
        raise ValueError("target_position_fraction must be positive")
    if lot_size <= 0:
        raise ValueError("lot_size must be positive")

    clean_calendar = sorted(str(date) for date in calendar)
    if not clean_calendar:
        return pd.DataFrame(columns=_CURVE_COLUMNS), pd.DataFrame(columns=_TRADE_COLUMNS)

    date_to_index = {trade_date: index for index, trade_date in enumerate(clean_calendar)}
    next_trade_date = {
        clean_calendar[index]: clean_calendar[index + 1]
        for index in range(len(clean_calendar) - 1)
    }
    final_trade_date = clean_calendar[-1]
    effective_bar_lookup = bar_lookup if bar_lookup is not None else _build_bar_lookup(bars)
    signals_by_entry_date = _signals_by_entry_date(
        selected_rows=selected_rows,
        next_trade_date=next_trade_date,
        allowed_entry_dates=set(clean_calendar[:-1]),
    )

    cash = float(initial_cash)
    buy_cost_rate = buy_cost_bps / 10_000.0
    sell_cost_rate = sell_cost_bps / 10_000.0
    active_positions: list[_Position] = []
    trade_rows: list[dict[str, Any]] = []
    curve_rows: list[dict[str, Any]] = []
    previous_value = float(initial_cash)
    next_position_id = 1

    for trade_date in clean_calendar:
        entries = 0
        exits = 0
        skipped_entries = 0

        portfolio_value_before_entries = cash + sum(
            float(position.shares) * position.last_price for position in active_positions
        )
        default_target_fraction = (
            float(target_position_fraction)
            if target_position_fraction is not None
            else 1.0 / float(max_positions)
        )
        default_target_position_value = portfolio_value_before_entries * default_target_fraction

        todays_signals = signals_by_entry_date.get(trade_date, [])
        for signal in todays_signals:
            if len(active_positions) >= max_positions:
                skipped_entries += 1
                continue
            if any(position.security_id == signal["security_id"] for position in active_positions):
                skipped_entries += 1
                continue
            bar = effective_bar_lookup.get((signal["security_id"], trade_date))
            if bar is None or not _is_finite_positive(bar.get("open_adj")):
                skipped_entries += 1
                continue

            entry_price = float(bar["open_adj"])
            signal_target_fraction = signal.get("target_position_fraction")
            signal_target_position_value = (
                portfolio_value_before_entries * float(signal_target_fraction)
                if _is_finite_positive(signal_target_fraction)
                else default_target_position_value
            )
            gross_budget = min(signal_target_position_value, cash / (1.0 + buy_cost_rate))
            shares = int(math.floor(gross_budget / entry_price / lot_size) * lot_size)
            if shares <= 0:
                skipped_entries += 1
                continue

            gross_entry_value = float(shares) * entry_price
            buy_cost = gross_entry_value * buy_cost_rate
            cash -= gross_entry_value + buy_cost
            entry_close = (
                float(bar["close_adj"])
                if _is_finite_positive(bar.get("close_adj"))
                else entry_price
            )
            active_positions.append(
                _Position(
                    position_id=next_position_id,
                    security_id=signal["security_id"],
                    signal_trade_date=signal["signal_trade_date"],
                    signal_origin=signal["signal_origin"],
                    entry_trade_date=trade_date,
                    entry_index=date_to_index[trade_date],
                    entry_price=entry_price,
                    shares=shares,
                    gross_entry_value=gross_entry_value,
                    buy_cost=buy_cost,
                    path_score=signal["path_score"],
                    last_price=entry_close,
                    peak_price=entry_price,
                    trailing_active=False,
                )
            )
            entries += 1
            next_position_id += 1

        remaining_positions: list[_Position] = []
        for position in active_positions:
            bar = effective_bar_lookup.get((position.security_id, trade_date))
            if bar is not None and _is_finite_positive(bar.get("close_adj")):
                position.last_price = float(bar["close_adj"])

            exit_decision = _dynamic_exit_decision(
                position=position,
                bar=bar,
                trade_date=trade_date,
                date_index=date_to_index[trade_date],
                final_trade_date=final_trade_date,
                policy=policy,
            )
            if exit_decision is None:
                remaining_positions.append(position)
                continue

            exit_price, exit_reason = exit_decision
            gross_exit_value = float(position.shares) * exit_price
            sell_cost = gross_exit_value * sell_cost_rate
            cash += gross_exit_value - sell_cost
            trade_rows.append(
                {
                    "year": int(str(trade_date)[:4]),
                    "policy_name": policy.policy_name,
                    "position_id": position.position_id,
                    "security_id": position.security_id,
                    "signal_origin": position.signal_origin,
                    "signal_trade_date": position.signal_trade_date,
                    "entry_trade_date": position.entry_trade_date,
                    "exit_trade_date": trade_date,
                    "holding_days": int(date_to_index[trade_date] - position.entry_index),
                    "exit_reason": exit_reason,
                    "shares": position.shares,
                    "entry_price": position.entry_price,
                    "exit_price": exit_price,
                    "gross_entry_value": position.gross_entry_value,
                    "gross_exit_value": gross_exit_value,
                    "buy_cost": position.buy_cost,
                    "sell_cost": sell_cost,
                    "gross_ret": exit_price / position.entry_price - 1.0,
                    "net_ret": (
                        (gross_exit_value - sell_cost)
                        / (position.gross_entry_value + position.buy_cost)
                        - 1.0
                    ),
                    "path_score": position.path_score,
                }
            )
            exits += 1
        active_positions = remaining_positions

        position_value = sum(float(position.shares) * position.last_price for position in active_positions)
        portfolio_value = cash + position_value
        daily_return = portfolio_value / previous_value - 1.0 if previous_value > 0.0 else 0.0
        curve_rows.append(
            {
                "year": int(str(trade_date)[:4]),
                "trade_date": trade_date,
                "policy_name": policy.policy_name,
                "portfolio_value": portfolio_value,
                "cash": cash,
                "position_value": position_value,
                "daily_return": daily_return,
                "gross_exposure": position_value / portfolio_value if portfolio_value > 0.0 else 0.0,
                "active_positions": len(active_positions),
                "entries": entries,
                "exits": exits,
                "skipped_entries": skipped_entries,
            }
        )
        previous_value = portfolio_value

    return (
        pd.DataFrame(curve_rows, columns=_CURVE_COLUMNS),
        pd.DataFrame(trade_rows, columns=_TRADE_COLUMNS),
    )


def summarize_slot_backtest(
    *,
    daily_curve: pd.DataFrame,
    trades: pd.DataFrame,
    initial_cash: float = 10_000_000.0,
) -> pd.DataFrame:
    columns = [
        "year",
        "policy_name",
        "total_return",
        "max_drawdown",
        "annual_vol",
        "sharpe_like",
        "avg_gross_exposure",
        "avg_active_positions",
        "entries",
        "skipped_entries",
        "closed_trades",
        "trade_win_rate",
        "avg_trade_net_ret",
        "avg_holding_days",
    ]
    if daily_curve.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, Any]] = []
    for (year, policy_name), frame in daily_curve.groupby(["year", "policy_name"], sort=True):
        frame = frame.sort_values("trade_date").reset_index(drop=True)
        values = frame["portfolio_value"].astype(float)
        returns = frame["daily_return"].astype(float)
        peak = values.cummax()
        drawdown = values / peak - 1.0
        annual_vol = float(returns.std(ddof=0) * math.sqrt(252.0))
        sharpe_like = (
            float(returns.mean() / returns.std(ddof=0) * math.sqrt(252.0))
            if float(returns.std(ddof=0)) > 0.0
            else float("nan")
        )
        trade_frame = trades.loc[
            (trades["year"] == year) & (trades["policy_name"] == policy_name)
        ] if not trades.empty else pd.DataFrame()
        rows.append(
            {
                "year": int(year),
                "policy_name": str(policy_name),
                "total_return": float(values.iloc[-1] / initial_cash - 1.0),
                "max_drawdown": float(drawdown.min()),
                "annual_vol": annual_vol,
                "sharpe_like": sharpe_like,
                "avg_gross_exposure": float(frame["gross_exposure"].mean()),
                "avg_active_positions": float(frame["active_positions"].mean()),
                "entries": int(frame["entries"].sum()),
                "skipped_entries": int(frame["skipped_entries"].sum()),
                "closed_trades": int(len(trade_frame)),
                "trade_win_rate": float((trade_frame["net_ret"] > 0.0).mean()) if not trade_frame.empty else float("nan"),
                "avg_trade_net_ret": float(trade_frame["net_ret"].mean()) if not trade_frame.empty else float("nan"),
                "avg_holding_days": float(trade_frame["holding_days"].mean()) if not trade_frame.empty else float("nan"),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def summarize_continuous_slot_backtest_by_year(
    *,
    daily_curve: pd.DataFrame,
    trades: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "year",
        "policy_name",
        "total_return",
        "max_drawdown",
        "annual_vol",
        "sharpe_like",
        "avg_gross_exposure",
        "avg_active_positions",
        "entries",
        "skipped_entries",
        "closed_trades",
        "trade_win_rate",
        "avg_trade_net_ret",
        "avg_holding_days",
    ]
    if daily_curve.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, Any]] = []
    for (year, policy_name), frame in daily_curve.groupby(["year", "policy_name"], sort=True):
        frame = frame.sort_values("trade_date").reset_index(drop=True)
        values = frame["portfolio_value"].astype(float)
        returns = frame["daily_return"].astype(float)
        start_value = float(values.iloc[0]) / (1.0 + float(returns.iloc[0])) if len(values) else float("nan")
        peak = values.cummax()
        drawdown = values / peak - 1.0
        annual_vol = float(returns.std(ddof=0) * math.sqrt(252.0))
        return_std = float(returns.std(ddof=0))
        trade_frame = trades.loc[
            (trades["year"] == year) & (trades["policy_name"] == policy_name)
        ] if not trades.empty else pd.DataFrame()
        rows.append(
            {
                "year": int(year),
                "policy_name": str(policy_name),
                "total_return": float(values.iloc[-1] / start_value - 1.0) if start_value > 0.0 else float("nan"),
                "max_drawdown": float(drawdown.min()),
                "annual_vol": annual_vol,
                "sharpe_like": (
                    float(returns.mean() / return_std * math.sqrt(252.0))
                    if return_std > 0.0
                    else float("nan")
                ),
                "avg_gross_exposure": float(frame["gross_exposure"].mean()),
                "avg_active_positions": float(frame["active_positions"].mean()),
                "entries": int(frame["entries"].sum()),
                "skipped_entries": int(frame["skipped_entries"].sum()),
                "closed_trades": int(len(trade_frame)),
                "trade_win_rate": float((trade_frame["net_ret"] > 0.0).mean()) if not trade_frame.empty else float("nan"),
                "avg_trade_net_ret": float(trade_frame["net_ret"].mean()) if not trade_frame.empty else float("nan"),
                "avg_holding_days": float(trade_frame["holding_days"].mean()) if not trade_frame.empty else float("nan"),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def summarize_exit_reasons(trades: pd.DataFrame) -> pd.DataFrame:
    columns = ["year", "policy_name", "exit_reason", "trades", "share", "mean_net_ret"]
    if trades.empty:
        return pd.DataFrame(columns=columns)

    summary = (
        trades.groupby(["year", "policy_name", "exit_reason"], as_index=False)
        .agg(trades=("net_ret", "size"), mean_net_ret=("net_ret", "mean"))
        .sort_values(["year", "policy_name", "exit_reason"])
        .reset_index(drop=True)
    )
    totals = summary.groupby(["year", "policy_name"], as_index=False)["trades"].sum().rename(
        columns={"trades": "total_trades"}
    )
    summary = summary.merge(totals, on=["year", "policy_name"], how="left")
    summary["share"] = summary["trades"] / summary["total_trades"]
    return summary[columns]


def build_bar_lookup_for_slot_backtest(bars: pd.DataFrame) -> dict[tuple[str, str], dict[str, Any]]:
    return _build_bar_lookup(bars)


def _build_bar_lookup(bars: pd.DataFrame) -> dict[tuple[str, str], dict[str, Any]]:
    if bars.empty:
        return {}
    working = bars.copy()
    working["security_id"] = working["security_id"].astype(str)
    working["trade_date"] = working["trade_date"].astype(str)
    return {
        (str(row["security_id"]), str(row["trade_date"])): row
        for row in working.to_dict(orient="records")
    }


def _signals_by_entry_date(
    *,
    selected_rows: pd.DataFrame,
    next_trade_date: dict[str, str],
    allowed_entry_dates: set[str],
) -> dict[str, list[dict[str, Any]]]:
    if selected_rows.empty:
        return {}

    rows = selected_rows.copy()
    rows["trade_date"] = rows["trade_date"].astype(str)
    if "path_score" not in rows.columns:
        rows["path_score"] = 0.0
    if "signal_priority" not in rows.columns:
        rows["signal_priority"] = 100.0
    rows["signal_priority"] = pd.to_numeric(rows["signal_priority"], errors="coerce").fillna(100.0)
    if "target_position_fraction" not in rows.columns:
        rows["target_position_fraction"] = np.nan
    rows["target_position_fraction"] = pd.to_numeric(
        rows["target_position_fraction"],
        errors="coerce",
    )
    rows = rows.sort_values(
        ["trade_date", "signal_priority", "path_score", "security_id"],
        ascending=[True, True, False, True],
    )

    result: dict[str, list[dict[str, Any]]] = {}
    for row in rows.to_dict(orient="records"):
        signal_date = str(row["trade_date"])
        entry_date = next_trade_date.get(signal_date)
        if entry_date is None or entry_date not in allowed_entry_dates:
            continue
        result.setdefault(entry_date, []).append(
            {
                "security_id": str(row["security_id"]),
                "signal_trade_date": signal_date,
                "signal_origin": str(row.get("signal_origin", "baseline")),
                "path_score": float(row.get("path_score", 0.0)),
                "signal_priority": float(row.get("signal_priority", 100.0)),
                "target_position_fraction": (
                    float(row["target_position_fraction"])
                    if _is_finite_positive(row.get("target_position_fraction"))
                    else None
                ),
            }
        )
    return result


def _fixed_exit_decision(
    *,
    position: _Position,
    bar: dict[str, Any] | None,
    trade_date: str,
    date_index: int,
    last_trade_date: str,
    policy: FixedTpSlPolicy,
) -> tuple[float, str] | None:
    holding_days = date_index - position.entry_index
    if holding_days <= 0:
        return None

    if bar is not None and _is_finite_positive(bar.get("high_adj")) and _is_finite_positive(bar.get("low_adj")):
        high_ret = float(bar["high_adj"]) / position.entry_price - 1.0
        low_ret = float(bar["low_adj"]) / position.entry_price - 1.0
        if low_ret <= -policy.stop_loss and high_ret >= policy.take_profit:
            return position.entry_price * (1.0 - policy.stop_loss), "same_day_stop_first"
        if low_ret <= -policy.stop_loss:
            return position.entry_price * (1.0 - policy.stop_loss), "stop_loss"
        if high_ret >= policy.take_profit:
            return position.entry_price * (1.0 + policy.take_profit), "take_profit"

    if holding_days >= policy.max_hold_days:
        if bar is not None and _is_finite_positive(bar.get("close_adj")):
            return float(bar["close_adj"]), "time_exit"
        return position.last_price, "time_exit"

    if trade_date == last_trade_date:
        if bar is not None and _is_finite_positive(bar.get("close_adj")):
            return float(bar["close_adj"]), "forced_year_end"
        return position.last_price, "forced_year_end"
    return None


def _dynamic_exit_decision(
    *,
    position: _Position,
    bar: dict[str, Any] | None,
    trade_date: str,
    date_index: int,
    final_trade_date: str,
    policy: DynamicTrailingPolicy,
) -> tuple[float, str] | None:
    holding_days = date_index - position.entry_index
    if holding_days <= 0:
        return None

    if bar is not None and _is_finite_positive(bar.get("low_adj")):
        hard_stop_price = position.entry_price * (1.0 - policy.stop_loss)
        if float(bar["low_adj"]) <= hard_stop_price:
            return hard_stop_price, "hard_stop_loss"

    if (
        position.trailing_active
        and bar is not None
        and _is_finite_positive(bar.get("low_adj"))
    ):
        trailing_stop_price = position.peak_price * (1.0 - policy.trailing_drawdown)
        if float(bar["low_adj"]) <= trailing_stop_price:
            return trailing_stop_price, "dynamic_trailing_stop"

    if bar is not None and _is_finite_positive(bar.get("high_adj")):
        current_high = float(bar["high_adj"])
        if current_high > position.peak_price:
            position.peak_price = current_high
        if position.peak_price / position.entry_price - 1.0 >= policy.activation_return:
            position.trailing_active = True

    if trade_date == final_trade_date:
        if bar is not None and _is_finite_positive(bar.get("close_adj")):
            return float(bar["close_adj"]), "forced_final_close"
        return position.last_price, "forced_final_close"
    return None


def _is_finite_positive(value: Any) -> bool:
    if value is None:
        return False
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return bool(np.isfinite(numeric) and numeric > 0.0)
