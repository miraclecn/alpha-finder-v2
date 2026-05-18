from __future__ import annotations

import argparse
import math
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from .price_volume_regime_validation import (
    CONTRACT_THRESHOLD,
    DEFAULT_YEAR_TO_REGIME,
    ENTRY_FEATURE_COLUMNS,
    EXPAND_THRESHOLD,
    FORWARD_DAYS,
    PRICE_DOWN,
    PRICE_UP,
    REGIME_FEATURE_COLUMNS,
    TURNOVER_BASELINE_DAYS,
    _daily_market_feature_frame,
    _markdown_table,
    build_candidate_feature_rows,
    classify_regime_dates,
    fit_regime_entry_evaluators,
    fit_regime_prototypes,
    score_candidate_rows,
)

DEFAULT_ANALYSIS_YEARS: tuple[int, ...] = (2022, 2023, 2024, 2025)


def classify_price_volume_state(*, daily_return: float, turnover_ratio: float) -> str:
    if turnover_ratio >= EXPAND_THRESHOLD:
        if daily_return > PRICE_UP:
            return "expand_up"
        if daily_return < PRICE_DOWN:
            return "expand_down"
        return "expand_flat"
    if turnover_ratio <= CONTRACT_THRESHOLD:
        if daily_return > PRICE_UP:
            return "contract_up"
        if daily_return < PRICE_DOWN:
            return "contract_down"
        return "contract_flat"
    return "neutral"


def _peak_profit_bucket(value: float) -> str:
    if value < 0.05:
        return "lt_5"
    if value < 0.10:
        return "5_10"
    if value < 0.20:
        return "10_20"
    return "gt_20"


def _drawdown_bucket(value: float) -> str:
    if value >= -0.03:
        return "tight"
    if value >= -0.08:
        return "mild_pullback"
    return "deep_pullback"


def _future_first_hit_5(
    *,
    future_window: pd.DataFrame,
    base_price: float,
) -> str:
    up_level = base_price * 1.05
    down_level = base_price * 0.95
    for row in future_window.itertuples(index=False):
        up_hit = float(row.high_adj) >= up_level
        down_hit = float(row.low_adj) <= down_level
        if up_hit and down_hit:
            return "down5_first"
        if down_hit:
            return "down5_first"
        if up_hit:
            return "up5_first"
    return "unresolved"


def should_exit_state_row(
    row: pd.Series | dict[str, Any],
    *,
    policy_name: str,
) -> bool:
    peak_ret = float(row["peak_ret_so_far"])
    if peak_ret < 0.10:
        return False

    state = str(row["state"])
    drawdown_bucket = str(row["drawdown_bucket"])
    peak_profit_bucket = str(row["peak_profit_bucket"])

    if policy_name == "strict":
        if drawdown_bucket == "deep_pullback" and state in {"expand_down", "expand_up", "neutral"}:
            return True
        if peak_profit_bucket == "gt_20" and drawdown_bucket == "mild_pullback" and state in {"expand_up", "expand_down"}:
            return True
        return False

    if policy_name == "aggressive":
        if drawdown_bucket == "deep_pullback" and state in {"expand_down", "expand_up", "neutral", "expand_flat"}:
            return True
        if drawdown_bucket == "mild_pullback" and state == "expand_up" and peak_profit_bucket in {"10_20", "gt_20"}:
            return True
        if drawdown_bucket == "mild_pullback" and state == "expand_down" and peak_profit_bucket == "gt_20":
            return True
        return False

    raise ValueError(f"Unsupported policy_name: {policy_name}")


def fit_in_sample_selected_rows(
    *,
    candidate_rows: pd.DataFrame,
    analysis_years: tuple[int, ...] = DEFAULT_ANALYSIS_YEARS,
    top_n_per_day: int = 5,
    regime_rolling_days: int = 20,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    regime_map = dict(DEFAULT_YEAR_TO_REGIME)
    training_rows = candidate_rows.loc[candidate_rows["year"].isin(regime_map.keys())].copy()
    analysis_rows = candidate_rows.loc[candidate_rows["year"].isin(analysis_years)].copy()
    if training_rows.empty or analysis_rows.empty:
        empty = pd.DataFrame()
        return empty, empty, pd.DataFrame(columns=["trade_date", "year", "predicted_regime", "distance"])

    prototypes, feature_scales = fit_regime_prototypes(
        candidate_rows=training_rows,
        year_to_regime=regime_map,
        feature_names=REGIME_FEATURE_COLUMNS,
        regime_rolling_days=regime_rolling_days,
    )
    daily_market = _daily_market_feature_frame(
        candidate_rows=training_rows,
        feature_names=REGIME_FEATURE_COLUMNS,
        regime_rolling_days=regime_rolling_days,
    )
    regime_dates = classify_regime_dates(
        daily_market_features=daily_market.loc[daily_market["year"].isin(analysis_years)].copy(),
        prototypes=prototypes,
        feature_scales=feature_scales,
        feature_names=REGIME_FEATURE_COLUMNS,
    )
    evaluators = fit_regime_entry_evaluators(
        candidate_rows=training_rows,
        year_to_regime=regime_map,
        feature_names=ENTRY_FEATURE_COLUMNS,
    )
    scored = score_candidate_rows(
        candidate_rows=analysis_rows,
        regime_by_date=regime_dates,
        evaluators=evaluators,
        feature_names=ENTRY_FEATURE_COLUMNS,
    )
    selected = (
        scored.sort_values(["trade_date", "score"], ascending=[True, False])
        .groupby("trade_date", group_keys=False)
        .head(top_n_per_day)
        .reset_index(drop=True)
    )
    return scored, selected, regime_dates


def _build_trade_forward_observations(
    *,
    trade: dict[str, Any],
    bars: pd.DataFrame,
    entry_index: int,
    forward_days: int = FORWARD_DAYS,
) -> list[dict[str, Any]]:
    if entry_index < 0 or entry_index >= len(bars) - 1:
        return []

    end_index = min(len(bars) - 1, entry_index + forward_days)
    entry_price = float(trade["entry_close_adj"])
    final_close = float(bars.iloc[end_index]["close_adj"])
    running_peak_high = float("-inf")
    rows: list[dict[str, Any]] = []

    for current_index in range(entry_index + 1, end_index + 1):
        current = bars.iloc[current_index]
        next_trade_date: str | None = None
        next_open_ret_from_entry = float("nan")
        if current_index + 1 < len(bars):
            next_trade_date = str(bars.iloc[current_index + 1]["trade_date"])
            next_open_ret_from_entry = float(bars.iloc[current_index + 1]["open_adj"]) / entry_price - 1.0
        running_peak_high = max(running_peak_high, float(current["high_adj"]))
        current_close = float(current["close_adj"])
        future_window = bars.iloc[current_index + 1 : end_index + 1]

        future_max_ret = float("nan")
        future_min_ret = float("nan")
        future_first_hit_5 = "unresolved"
        if not future_window.empty:
            future_max_ret = float(future_window["high_adj"].max() / current_close - 1.0)
            future_min_ret = float(future_window["low_adj"].min() / current_close - 1.0)
            future_first_hit_5 = _future_first_hit_5(
                future_window=future_window,
                base_price=current_close,
            )

        close_drawdown_from_peak = current_close / running_peak_high - 1.0
        peak_ret_so_far = running_peak_high / entry_price - 1.0
        rows.append(
            {
                "security_id": str(trade["security_id"]),
                "entry_trade_date": str(trade["trade_date"]),
                "future_trade_date": str(current["trade_date"]),
                "next_trade_date": next_trade_date,
                "year": int(trade["year"]),
                "predicted_regime": str(trade.get("predicted_regime", "")),
                "score": float(trade.get("score", float("nan"))),
                "step": int(current_index - entry_index),
                "state": classify_price_volume_state(
                    daily_return=float(current["daily_ret1"]),
                    turnover_ratio=float(current["turnover_ratio"]),
                ),
                "daily_ret1": float(current["daily_ret1"]),
                "turnover_ratio": float(current["turnover_ratio"]),
                "close_ret_from_entry": current_close / entry_price - 1.0,
                "high_ret_from_entry": float(current["high_adj"]) / entry_price - 1.0,
                "low_ret_from_entry": float(current["low_adj"]) / entry_price - 1.0,
                "peak_ret_so_far": peak_ret_so_far,
                "peak_profit_bucket": _peak_profit_bucket(peak_ret_so_far),
                "close_drawdown_from_peak": close_drawdown_from_peak,
                "drawdown_bucket": _drawdown_bucket(close_drawdown_from_peak),
                "open_ret_from_entry": float(current["open_adj"]) / entry_price - 1.0,
                "remaining_close_ret": final_close / current_close - 1.0,
                "future_max_ret_from_today": future_max_ret,
                "future_min_ret_from_today": future_min_ret,
                "future_first_hit_5": future_first_hit_5,
                "next_open_ret_from_entry": next_open_ret_from_entry,
            }
        )
    return rows


def build_selected_forward_observations(
    *,
    source_db_path: Path,
    selected_rows: pd.DataFrame,
    query_start: str,
    query_end: str,
    forward_days: int = FORWARD_DAYS,
) -> pd.DataFrame:
    columns = [
        "security_id",
        "entry_trade_date",
        "future_trade_date",
        "next_trade_date",
        "year",
        "predicted_regime",
        "score",
        "step",
        "state",
        "daily_ret1",
        "turnover_ratio",
        "close_ret_from_entry",
        "high_ret_from_entry",
        "low_ret_from_entry",
        "peak_ret_so_far",
        "peak_profit_bucket",
        "close_drawdown_from_peak",
        "drawdown_bucket",
        "open_ret_from_entry",
        "remaining_close_ret",
        "future_max_ret_from_today",
        "future_min_ret_from_today",
        "future_first_hit_5",
        "next_open_ret_from_entry",
    ]
    if selected_rows.empty:
        return pd.DataFrame(columns=columns)

    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        security_frame = pd.DataFrame(
            {"security_id": selected_rows["security_id"].astype(str).drop_duplicates().tolist()}
        )
        conn.register("selected_security_ids", security_frame)
        frame = conn.execute(
            """
            SELECT
                d.security_id,
                d.trade_date,
                d.open_adj,
                d.close_adj,
                d.high_adj,
                d.low_adj,
                d.turnover_value_cny
            FROM daily_bar_pit d
            JOIN selected_security_ids s
              ON d.security_id = s.security_id
            WHERE d.trade_date BETWEEN ? AND ?
              AND d.price_basis = 'unadjusted'
              AND d.open_adj IS NOT NULL
              AND d.close_adj IS NOT NULL
              AND d.high_adj IS NOT NULL
              AND d.low_adj IS NOT NULL
              AND d.turnover_value_cny IS NOT NULL
              AND d.turnover_value_cny > 0
            ORDER BY d.security_id, d.trade_date
            """,
            [query_start, query_end],
        ).fetchdf()
    finally:
        conn.close()

    if frame.empty:
        return pd.DataFrame(columns=columns)

    bars_by_security: dict[str, pd.DataFrame] = {}
    for security_id, security_frame in frame.groupby("security_id", sort=False):
        bars = security_frame.sort_values("trade_date").reset_index(drop=True).copy()
        turnover_med = (
            bars["turnover_value_cny"].rolling(TURNOVER_BASELINE_DAYS).median().shift(1)
        )
        bars["turnover_ratio"] = bars["turnover_value_cny"] / turnover_med
        bars["daily_ret1"] = bars["close_adj"].pct_change().fillna(0.0)
        bars_by_security[str(security_id)] = bars

    rows: list[dict[str, Any]] = []
    for trade in selected_rows.to_dict(orient="records"):
        security_id = str(trade["security_id"])
        bars = bars_by_security.get(security_id)
        if bars is None or bars.empty:
            continue
        matches = bars.index[bars["trade_date"].astype(str) == str(trade["trade_date"])]
        if len(matches) == 0:
            continue
        rows.extend(
            _build_trade_forward_observations(
                trade=trade,
                bars=bars,
                entry_index=int(matches[0]),
                forward_days=forward_days,
            )
        )
    return pd.DataFrame(rows, columns=columns)


def replay_state_exit_policy(
    observations: pd.DataFrame,
    *,
    policy_name: str,
    output_policy_name: str | None = None,
    max_hold_days: int | None = None,
    round_trip_cost_bps: float = 0.0,
) -> pd.DataFrame:
    columns = [
        "security_id",
        "entry_trade_date",
        "year",
        "policy_name",
        "exit_trade_date",
        "exit_step",
        "exit_reason",
        "gross_ret",
        "net_ret",
    ]
    if observations.empty:
        return pd.DataFrame(columns=columns)

    effective_policy_name = output_policy_name
    if effective_policy_name is None:
        effective_policy_name = (
            f"{policy_name}_hold{max_hold_days}"
            if max_hold_days is not None
            else policy_name
        )

    rows: list[dict[str, Any]] = []
    for (_, _), trade_rows in observations.groupby(["security_id", "entry_trade_date"], sort=False):
        trade_rows = trade_rows.sort_values("step").reset_index(drop=True)
        exit_trade_date = str(trade_rows.iloc[-1]["future_trade_date"])
        exit_step = int(trade_rows.iloc[-1]["step"])
        exit_reason = "hold_to_window"
        gross_ret = float(trade_rows.iloc[-1]["close_ret_from_entry"])

        for row in trade_rows.to_dict(orient="records"):
            if max_hold_days is not None and int(row["step"]) >= max_hold_days:
                exit_trade_date = str(row.get("next_trade_date") or row["future_trade_date"])
                exit_step = int(row["step"])
                next_open_ret = row.get("next_open_ret_from_entry", float("nan"))
                gross_ret = float(next_open_ret) if pd.notna(next_open_ret) else float(row["close_ret_from_entry"])
                exit_reason = f"time_exit_{max_hold_days}"
                break
            if should_exit_state_row(row, policy_name=policy_name):
                exit_trade_date = str(row.get("next_trade_date") or row["future_trade_date"])
                exit_step = int(row["step"])
                next_open_ret = row.get("next_open_ret_from_entry", float("nan"))
                gross_ret = float(next_open_ret) if pd.notna(next_open_ret) else float(row["close_ret_from_entry"])
                exit_reason = f"state_exit_{policy_name}"
                break

        net_ret = gross_ret - (round_trip_cost_bps / 10_000.0)
        rows.append(
            {
                "security_id": str(trade_rows.iloc[0]["security_id"]),
                "entry_trade_date": str(trade_rows.iloc[0]["entry_trade_date"]),
                "year": int(trade_rows.iloc[0]["year"]),
                "policy_name": effective_policy_name,
                "exit_trade_date": exit_trade_date,
                "exit_step": exit_step,
                "exit_reason": exit_reason,
                "gross_ret": gross_ret,
                "net_ret": net_ret,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def replay_fixed_tp_sl_policy(
    observations: pd.DataFrame,
    *,
    take_profit: float,
    stop_loss: float,
    max_hold_days: int,
    round_trip_cost_bps: float = 0.0,
) -> pd.DataFrame:
    columns = [
        "security_id",
        "entry_trade_date",
        "year",
        "policy_name",
        "exit_trade_date",
        "exit_step",
        "exit_reason",
        "gross_ret",
        "net_ret",
    ]
    if observations.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, Any]] = []
    for (_, _), trade_rows in observations.groupby(["security_id", "entry_trade_date"], sort=False):
        trade_rows = trade_rows.sort_values("step").reset_index(drop=True)
        eligible = trade_rows.loc[trade_rows["step"] <= max_hold_days].copy()
        if eligible.empty:
            eligible = trade_rows.tail(1).copy()

        last = eligible.iloc[-1]
        exit_trade_date = str(last["future_trade_date"])
        exit_step = int(last["step"])
        exit_reason = "time_exit"
        gross_ret = float(last["close_ret_from_entry"])

        for row in eligible.to_dict(orient="records"):
            high_ret = float(row["high_ret_from_entry"])
            low_ret = float(row["low_ret_from_entry"])
            if low_ret <= -stop_loss and high_ret >= take_profit:
                exit_trade_date = str(row["future_trade_date"])
                exit_step = int(row["step"])
                exit_reason = "same_day_stop_first"
                gross_ret = -stop_loss
                break
            if low_ret <= -stop_loss:
                exit_trade_date = str(row["future_trade_date"])
                exit_step = int(row["step"])
                exit_reason = "stop_loss"
                gross_ret = -stop_loss
                break
            if high_ret >= take_profit:
                exit_trade_date = str(row["future_trade_date"])
                exit_step = int(row["step"])
                exit_reason = "take_profit"
                gross_ret = take_profit
                break

        net_ret = gross_ret - (round_trip_cost_bps / 10_000.0)
        rows.append(
            {
                "security_id": str(trade_rows.iloc[0]["security_id"]),
                "entry_trade_date": str(trade_rows.iloc[0]["entry_trade_date"]),
                "year": int(trade_rows.iloc[0]["year"]),
                "policy_name": f"fixed_tp{take_profit:.2f}_sl{stop_loss:.2f}_hold{max_hold_days}",
                "exit_trade_date": exit_trade_date,
                "exit_step": exit_step,
                "exit_reason": exit_reason,
                "gross_ret": gross_ret,
                "net_ret": net_ret,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def replay_hybrid_exit_policy(
    observations: pd.DataFrame,
    *,
    take_profit: float,
    stop_loss: float,
    max_hold_days: int,
    state_policy_name: str,
    output_policy_name: str | None = None,
    round_trip_cost_bps: float = 0.0,
) -> pd.DataFrame:
    columns = [
        "security_id",
        "entry_trade_date",
        "year",
        "policy_name",
        "exit_trade_date",
        "exit_step",
        "exit_reason",
        "gross_ret",
        "net_ret",
    ]
    if observations.empty:
        return pd.DataFrame(columns=columns)

    effective_policy_name = output_policy_name
    if effective_policy_name is None:
        effective_policy_name = (
            f"hybrid_{state_policy_name}_tp{take_profit:.2f}_sl{stop_loss:.2f}_hold{max_hold_days}"
        )

    rows: list[dict[str, Any]] = []
    for (_, _), trade_rows in observations.groupby(["security_id", "entry_trade_date"], sort=False):
        trade_rows = trade_rows.sort_values("step").reset_index(drop=True)
        eligible = trade_rows.loc[trade_rows["step"] <= max_hold_days].copy()
        if eligible.empty:
            eligible = trade_rows.tail(1).copy()

        last = eligible.iloc[-1]
        exit_trade_date = str(last["future_trade_date"])
        exit_step = int(last["step"])
        exit_reason = "time_exit"
        gross_ret = float(last["close_ret_from_entry"])

        for row in eligible.to_dict(orient="records"):
            high_ret = float(row["high_ret_from_entry"])
            low_ret = float(row["low_ret_from_entry"])
            if low_ret <= -stop_loss and high_ret >= take_profit:
                exit_trade_date = str(row["future_trade_date"])
                exit_step = int(row["step"])
                exit_reason = "same_day_stop_first"
                gross_ret = -stop_loss
                break
            if low_ret <= -stop_loss:
                exit_trade_date = str(row["future_trade_date"])
                exit_step = int(row["step"])
                exit_reason = "stop_loss"
                gross_ret = -stop_loss
                break
            if high_ret >= take_profit:
                exit_trade_date = str(row["future_trade_date"])
                exit_step = int(row["step"])
                exit_reason = "take_profit"
                gross_ret = take_profit
                break
            if int(row["step"]) >= max_hold_days:
                exit_trade_date = str(row["future_trade_date"])
                exit_step = int(row["step"])
                exit_reason = "time_exit"
                gross_ret = float(row["close_ret_from_entry"])
                break
            if should_exit_state_row(row, policy_name=state_policy_name):
                exit_trade_date = str(row.get("next_trade_date") or row["future_trade_date"])
                exit_step = int(row["step"])
                next_open_ret = row.get("next_open_ret_from_entry", float("nan"))
                gross_ret = float(next_open_ret) if pd.notna(next_open_ret) else float(row["close_ret_from_entry"])
                exit_reason = f"hybrid_state_exit_{state_policy_name}"
                break

        net_ret = gross_ret - (round_trip_cost_bps / 10_000.0)
        rows.append(
            {
                "security_id": str(trade_rows.iloc[0]["security_id"]),
                "entry_trade_date": str(trade_rows.iloc[0]["entry_trade_date"]),
                "year": int(trade_rows.iloc[0]["year"]),
                "policy_name": effective_policy_name,
                "exit_trade_date": exit_trade_date,
                "exit_step": exit_step,
                "exit_reason": exit_reason,
                "gross_ret": gross_ret,
                "net_ret": net_ret,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def summarize_policy_replays(replay_rows: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "policy_name",
        "year",
        "trades",
        "mean_gross_ret",
        "mean_net_ret",
        "median_net_ret",
        "win_rate_pos",
        "non_loss_rate",
        "avg_exit_step",
    ]
    if replay_rows.empty:
        return pd.DataFrame(columns=columns)

    summary = (
        replay_rows.groupby(["policy_name", "year"], as_index=False)
        .agg(
            trades=("gross_ret", "size"),
            mean_gross_ret=("gross_ret", "mean"),
            mean_net_ret=("net_ret", "mean"),
            median_net_ret=("net_ret", "median"),
            win_rate_pos=("net_ret", lambda s: float((s > 0.0).mean())),
            non_loss_rate=("net_ret", lambda s: float((s >= 0.0).mean())),
            avg_exit_step=("exit_step", "mean"),
        )
        .sort_values(["policy_name", "year"])
        .reset_index(drop=True)
    )
    return summary[columns]


def summarize_exit_state_candidates(
    observations: pd.DataFrame,
    *,
    min_peak_ret: float = 0.10,
    min_count: int = 30,
) -> pd.DataFrame:
    columns = [
        "state",
        "drawdown_bucket",
        "peak_profit_bucket",
        "count",
        "remaining_close_ret_mean",
        "future_max_ret_from_today_mean",
        "future_min_ret_from_today_mean",
        "future_drop_5_rate",
        "future_rebound_5_rate",
        "future_down5_first_rate",
        "future_up5_first_rate",
        "negative_remaining_years",
        "drop_gt_rebound_years",
        "downside_first_gt_upside_years",
    ]
    if observations.empty:
        return pd.DataFrame(columns=columns)

    working = observations.loc[observations["peak_ret_so_far"] >= min_peak_ret].copy()
    if working.empty:
        return pd.DataFrame(columns=columns)

    grouped = (
        working.groupby(["state", "drawdown_bucket", "peak_profit_bucket"], as_index=False)
        .agg(
            count=("remaining_close_ret", "size"),
            remaining_close_ret_mean=("remaining_close_ret", "mean"),
            future_max_ret_from_today_mean=("future_max_ret_from_today", "mean"),
            future_min_ret_from_today_mean=("future_min_ret_from_today", "mean"),
            future_drop_5_rate=("future_min_ret_from_today", lambda s: float((s <= -0.05).mean())),
            future_rebound_5_rate=("future_max_ret_from_today", lambda s: float((s >= 0.05).mean())),
            future_down5_first_rate=("future_first_hit_5", lambda s: float((s == "down5_first").mean())),
            future_up5_first_rate=("future_first_hit_5", lambda s: float((s == "up5_first").mean())),
        )
        .loc[lambda frame: frame["count"] >= min_count]
        .reset_index(drop=True)
    )
    if grouped.empty:
        return grouped

    years = sorted(int(year) for year in working["year"].dropna().unique().tolist())
    negative_year_counts: list[int] = []
    drop_gt_rebound_counts: list[int] = []
    downside_first_gt_upside_counts: list[int] = []

    for row in grouped.itertuples(index=False):
        mask = (
            (working["state"] == row.state)
            & (working["drawdown_bucket"] == row.drawdown_bucket)
            & (working["peak_profit_bucket"] == row.peak_profit_bucket)
        )
        subset = working.loc[mask]
        negative_years = 0
        drop_gt_rebound_years = 0
        downside_first_gt_upside_years = 0
        for year in years:
            year_rows = subset.loc[subset["year"] == year]
            if year_rows.empty:
                continue
            remaining_mean = float(year_rows["remaining_close_ret"].mean())
            drop_rate = float((year_rows["future_min_ret_from_today"] <= -0.05).mean())
            rebound_rate = float((year_rows["future_max_ret_from_today"] >= 0.05).mean())
            down_first_rate = float((year_rows["future_first_hit_5"] == "down5_first").mean())
            up_first_rate = float((year_rows["future_first_hit_5"] == "up5_first").mean())
            if remaining_mean < 0.0:
                negative_years += 1
            if drop_rate > rebound_rate:
                drop_gt_rebound_years += 1
            if down_first_rate > up_first_rate:
                downside_first_gt_upside_years += 1
        negative_year_counts.append(negative_years)
        drop_gt_rebound_counts.append(drop_gt_rebound_years)
        downside_first_gt_upside_counts.append(downside_first_gt_upside_years)

    grouped["negative_remaining_years"] = negative_year_counts
    grouped["drop_gt_rebound_years"] = drop_gt_rebound_counts
    grouped["downside_first_gt_upside_years"] = downside_first_gt_upside_counts
    return grouped.sort_values(
        [
            "negative_remaining_years",
            "downside_first_gt_upside_years",
            "drop_gt_rebound_years",
            "remaining_close_ret_mean",
            "count",
        ],
        ascending=[False, False, False, True, False],
    ).reset_index(drop=True)


def write_exit_state_report(
    *,
    report_markdown_path: Path,
    selected_rows: pd.DataFrame,
    observations: pd.DataFrame,
    exit_state_summary: pd.DataFrame,
    analysis_years: tuple[int, ...],
    top_n_per_day: int,
) -> None:
    selected_summary = (
        selected_rows.groupby(["year", "predicted_regime"], as_index=False)
        .size()
        .rename(columns={"size": "selected_rows"})
        .sort_values(["year", "selected_rows"], ascending=[True, False])
        .reset_index(drop=True)
    )
    step_state_summary = (
        observations.groupby(["step", "state"], as_index=False)
        .size()
        .rename(columns={"size": "rows"})
        .sort_values(["step", "rows"], ascending=[True, False])
        .groupby("step", group_keys=False)
        .head(3)
        .reset_index(drop=True)
    )
    sell_candidates = (
        exit_state_summary.loc[
            (exit_state_summary["negative_remaining_years"] >= 3)
            & (exit_state_summary["downside_first_gt_upside_years"] >= 3)
            & (exit_state_summary["remaining_close_ret_mean"] < 0.0)
        ]
        .head(15)
        .reset_index(drop=True)
    )
    continuation_states = (
        exit_state_summary.loc[
            (exit_state_summary["negative_remaining_years"] <= 1)
            & (exit_state_summary["remaining_close_ret_mean"] > 0.0)
            & (exit_state_summary["future_up5_first_rate"] >= exit_state_summary["future_down5_first_rate"])
        ]
        .sort_values(["remaining_close_ret_mean", "count"], ascending=[False, False])
        .head(15)
        .reset_index(drop=True)
    )

    lines = [
        f"# Price-Volume Exit State Study - {date.today().isoformat()}",
        "",
        "## Object",
        f"- Analysis years: `{', '.join(str(year) for year in analysis_years)}`",
        f"- Selection method: in-sample regime gate + regime-specific scorer, then `Top {top_n_per_day} / day`.",
        "- Exit-state purpose: find post-entry price-volume states after which continuing to hold has weak expectancy or unfavorable risk/reward.",
        "",
        "## Selected Sample",
        _markdown_table(selected_summary),
        "",
        "## Forward State Coverage",
        _markdown_table(step_state_summary),
        "",
        "## Sell Candidates",
        "- These rows are the strongest sell-state candidates in this study because remaining close expectancy is weak and the future drawdown probability dominates the rebound probability across multiple years.",
        _markdown_table(sell_candidates),
        "",
        "## Continuation States",
        "- These rows are the opposite: after they appear, continuing to hold still tends to have positive remaining expectancy.",
        _markdown_table(continuation_states),
    ]
    report_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    report_markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_exit_state_study(
    *,
    source_db_path: Path,
    selected_csv_path: Path,
    observations_csv_path: Path,
    exit_summary_csv_path: Path,
    report_markdown_path: Path,
    entry_start: str = "20220101",
    entry_end: str = "20251231",
    query_start: str = "20210101",
    query_end: str = "20260430",
    analysis_years: tuple[int, ...] = DEFAULT_ANALYSIS_YEARS,
    top_n_per_day: int = 5,
    regime_rolling_days: int = 20,
    min_peak_ret: float = 0.10,
    min_count: int = 30,
) -> dict[str, pd.DataFrame]:
    candidate_rows = build_candidate_feature_rows(
        source_db_path=source_db_path,
        entry_start=entry_start,
        entry_end=entry_end,
        query_start=query_start,
        query_end=query_end,
    )
    _, selected_rows, _ = fit_in_sample_selected_rows(
        candidate_rows=candidate_rows,
        analysis_years=analysis_years,
        top_n_per_day=top_n_per_day,
        regime_rolling_days=regime_rolling_days,
    )
    observations = build_selected_forward_observations(
        source_db_path=source_db_path,
        selected_rows=selected_rows,
        query_start=query_start,
        query_end=query_end,
        forward_days=FORWARD_DAYS,
    )
    exit_summary = summarize_exit_state_candidates(
        observations,
        min_peak_ret=min_peak_ret,
        min_count=min_count,
    )

    selected_csv_path.parent.mkdir(parents=True, exist_ok=True)
    observations_csv_path.parent.mkdir(parents=True, exist_ok=True)
    exit_summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
    selected_rows.to_csv(selected_csv_path, index=False)
    observations.to_csv(observations_csv_path, index=False)
    exit_summary.to_csv(exit_summary_csv_path, index=False)
    write_exit_state_report(
        report_markdown_path=report_markdown_path,
        selected_rows=selected_rows,
        observations=observations,
        exit_state_summary=exit_summary,
        analysis_years=analysis_years,
        top_n_per_day=top_n_per_day,
    )
    return {
        "selected": selected_rows,
        "observations": observations,
        "exit_summary": exit_summary,
    }


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the price-volume exit state study.")
    parser.add_argument("--source-db", required=True, type=Path)
    parser.add_argument("--selected-csv", required=True, type=Path)
    parser.add_argument("--observations-csv", required=True, type=Path)
    parser.add_argument("--exit-summary-csv", required=True, type=Path)
    parser.add_argument("--report-markdown", required=True, type=Path)
    parser.add_argument("--entry-start", default="20220101")
    parser.add_argument("--entry-end", default="20251231")
    parser.add_argument("--query-start", default="20210101")
    parser.add_argument("--query-end", default="20260430")
    parser.add_argument("--top-n-per-day", default=5, type=int)
    parser.add_argument("--regime-rolling-days", default=20, type=int)
    parser.add_argument("--min-peak-ret", default=0.10, type=float)
    parser.add_argument("--min-count", default=30, type=int)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_exit_state_study(
        source_db_path=args.source_db,
        selected_csv_path=args.selected_csv,
        observations_csv_path=args.observations_csv,
        exit_summary_csv_path=args.exit_summary_csv,
        report_markdown_path=args.report_markdown,
        entry_start=str(args.entry_start),
        entry_end=str(args.entry_end),
        query_start=str(args.query_start),
        query_end=str(args.query_end),
        top_n_per_day=int(args.top_n_per_day),
        regime_rolling_days=int(args.regime_rolling_days),
        min_peak_ret=float(args.min_peak_ret),
        min_count=int(args.min_count),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
