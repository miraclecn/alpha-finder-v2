from __future__ import annotations

import argparse
import math
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from .up5in10_price_volume_study import _markdown_table

TURNOVER_BASELINE_DAYS = 20
LEVEL_LOOKBACK_DAYS = 252
FORWARD_DAYS = 120
BASE_WINDOWS = (120, 100, 80, 60, 40, 20)
RANGE_WIDTH_MAX = 0.30
LOW_LEVEL_MAX_POSITION = 0.45
LOW_ZONE_MAX_POSITION = 0.25
HIGH_ZONE_MIN_POSITION = 0.75
MIN_ZONE_DAYS = 3
MIN_VOLUME_ASYMMETRY_RATIO = 1.30
SHADOW_TURNOVER_RATIO_MIN = 1.30
LONG_LOWER_SHADOW_RECOVERY_MIN = 0.04
LONG_LOWER_SHADOW_DOMINANCE_MIN = 1.50

EVENT_COLUMNS = [
    "security_id",
    "current_name",
    "board",
    "breakout_date",
    "entry_date",
    "base_days",
    "base_start_date",
    "base_end_date",
    "base_low",
    "base_high",
    "base_width",
    "price_level_pct_252",
    "low_zone_turnover_ratio",
    "high_zone_turnover_ratio",
    "volume_asymmetry_ratio",
    "shadow_signal_days",
    "last_shadow_date",
    "last_shadow_gap_days",
    "max_shadow_turnover_ratio",
    "max_shadow_close_recovery_pct",
    "base_low_date",
    "base_high_date",
    "base_low_turnover_ratio",
    "base_high_turnover_ratio",
    "breakout_close",
    "entry_open",
    "close_ret20",
    "close_ret60",
    "close_ret120",
    "max_ret120",
    "min_ret120",
    "up20",
    "up40",
    "loss10",
    "days_to_up20",
    "days_to_loss10",
    "first_hit",
]


def _close_ret(close_values: np.ndarray, *, entry_open: float, horizon: int) -> float:
    if horizon >= len(close_values):
        return float("nan")
    return (float(close_values[horizon]) - entry_open) / entry_open


def _compute_first_hit(high_values: np.ndarray, low_values: np.ndarray, *, entry_open: float) -> tuple[float, float, str]:
    up20_level = entry_open * 1.20
    loss10_level = entry_open * 0.90
    days_to_up20 = float("nan")
    days_to_loss10 = float("nan")

    for day_index, (high_value, low_value) in enumerate(zip(high_values, low_values, strict=False), start=1):
        up20_hit = float(high_value) >= up20_level
        loss10_hit = float(low_value) <= loss10_level
        if up20_hit and math.isnan(days_to_up20):
            days_to_up20 = float(day_index)
        if loss10_hit and math.isnan(days_to_loss10):
            days_to_loss10 = float(day_index)
        if up20_hit and loss10_hit:
            return days_to_up20, days_to_loss10, "both_same_day"
        if not math.isnan(days_to_up20) and not math.isnan(days_to_loss10):
            break

    if math.isnan(days_to_up20) and math.isnan(days_to_loss10):
        return days_to_up20, days_to_loss10, "unresolved"
    if math.isnan(days_to_up20):
        return days_to_up20, days_to_loss10, "loss10_first"
    if math.isnan(days_to_loss10):
        return days_to_up20, days_to_loss10, "up20_first"
    if days_to_up20 < days_to_loss10:
        return days_to_up20, days_to_loss10, "up20_first"
    if days_to_loss10 < days_to_up20:
        return days_to_up20, days_to_loss10, "loss10_first"
    return days_to_up20, days_to_loss10, "both_same_day"


def _compute_forward_stats(
    *,
    trade_date: np.ndarray,
    open_adj: np.ndarray,
    high_adj: np.ndarray,
    low_adj: np.ndarray,
    close_adj: np.ndarray,
    breakout_index: int,
) -> dict[str, Any]:
    entry_index = breakout_index + 1
    forward_end = entry_index + FORWARD_DAYS + 1
    if forward_end > len(close_adj):
        return {
            "entry_date": None,
            "entry_open": float("nan"),
            "close_ret20": float("nan"),
            "close_ret60": float("nan"),
            "close_ret120": float("nan"),
            "max_ret120": float("nan"),
            "min_ret120": float("nan"),
            "up20": False,
            "up40": False,
            "loss10": False,
            "days_to_up20": float("nan"),
            "days_to_loss10": float("nan"),
            "first_hit": "unresolved",
        }

    entry_open = float(open_adj[entry_index])
    high_values = high_adj[entry_index:forward_end]
    low_values = low_adj[entry_index:forward_end]
    close_values = close_adj[entry_index:forward_end]
    days_to_up20, days_to_loss10, first_hit = _compute_first_hit(high_values, low_values, entry_open=entry_open)
    max_ret120 = (float(high_values.max()) - entry_open) / entry_open
    min_ret120 = (float(low_values.min()) - entry_open) / entry_open
    return {
        "entry_date": str(trade_date[entry_index]),
        "entry_open": entry_open,
        "close_ret20": _close_ret(close_values, entry_open=entry_open, horizon=20),
        "close_ret60": _close_ret(close_values, entry_open=entry_open, horizon=60),
        "close_ret120": _close_ret(close_values, entry_open=entry_open, horizon=120),
        "max_ret120": max_ret120,
        "min_ret120": min_ret120,
        "up20": max_ret120 >= 0.20,
        "up40": max_ret120 >= 0.40,
        "loss10": min_ret120 <= -0.10,
        "days_to_up20": days_to_up20,
        "days_to_loss10": days_to_loss10,
        "first_hit": first_hit,
    }


def _select_base_candidate(
    *,
    trade_date: np.ndarray,
    close_adj: np.ndarray,
    turnover_ratio: np.ndarray,
    prior_252_low: np.ndarray,
    prior_252_high: np.ndarray,
    breakout_index: int,
) -> dict[str, Any] | None:
    breakout_close = float(close_adj[breakout_index])
    prior_low = float(prior_252_low[breakout_index])
    prior_high = float(prior_252_high[breakout_index])
    if math.isnan(prior_low) or math.isnan(prior_high) or prior_high <= prior_low:
        return None

    for base_days in BASE_WINDOWS:
        start_index = breakout_index - base_days
        if start_index < TURNOVER_BASELINE_DAYS:
            continue

        base_close = close_adj[start_index:breakout_index]
        base_turnover_ratio = turnover_ratio[start_index:breakout_index]
        if np.isnan(base_turnover_ratio).any():
            continue

        base_low_index = int(np.argmin(base_close))
        base_high_index = int(np.argmax(base_close))
        base_low = float(base_close[base_low_index])
        base_high = float(base_close[base_high_index])
        if breakout_close <= base_high or base_high <= base_low:
            continue

        base_width = base_high / base_low - 1.0
        if base_width > RANGE_WIDTH_MAX:
            continue

        price_position = (base_close - base_low) / (base_high - base_low)
        low_mask = price_position <= LOW_ZONE_MAX_POSITION
        high_mask = price_position >= HIGH_ZONE_MIN_POSITION
        if int(low_mask.sum()) < MIN_ZONE_DAYS or int(high_mask.sum()) < MIN_ZONE_DAYS:
            continue

        low_zone_turnover_ratio = float(base_turnover_ratio[low_mask].mean())
        high_zone_turnover_ratio = float(base_turnover_ratio[high_mask].mean())
        if not math.isfinite(low_zone_turnover_ratio) or not math.isfinite(high_zone_turnover_ratio):
            continue
        if high_zone_turnover_ratio <= 0:
            continue

        volume_asymmetry_ratio = low_zone_turnover_ratio / high_zone_turnover_ratio
        if volume_asymmetry_ratio < MIN_VOLUME_ASYMMETRY_RATIO:
            continue

        price_level_pct_252 = (base_high - prior_low) / (prior_high - prior_low)
        if price_level_pct_252 > LOW_LEVEL_MAX_POSITION:
            continue

        return {
            "base_days": base_days,
            "base_start_date": str(trade_date[start_index]),
            "base_end_date": str(trade_date[breakout_index - 1]),
            "base_low": base_low,
            "base_high": base_high,
            "base_width": base_width,
            "price_level_pct_252": price_level_pct_252,
            "low_zone_turnover_ratio": low_zone_turnover_ratio,
            "high_zone_turnover_ratio": high_zone_turnover_ratio,
            "volume_asymmetry_ratio": volume_asymmetry_ratio,
            "base_low_date": str(trade_date[start_index + base_low_index]),
            "base_high_date": str(trade_date[start_index + base_high_index]),
            "base_low_turnover_ratio": float(base_turnover_ratio[base_low_index]),
            "base_high_turnover_ratio": float(base_turnover_ratio[base_high_index]),
        }

    return None


def _select_shadow_signal(
    *,
    trade_date: np.ndarray,
    open_adj: np.ndarray,
    high_adj: np.ndarray,
    low_adj: np.ndarray,
    close_adj: np.ndarray,
    turnover_ratio: np.ndarray,
    start_index: int,
    breakout_index: int,
) -> dict[str, Any] | None:
    shadow_open = open_adj[start_index:breakout_index]
    shadow_high = high_adj[start_index:breakout_index]
    shadow_low = low_adj[start_index:breakout_index]
    shadow_close = close_adj[start_index:breakout_index]
    shadow_turnover_ratio = turnover_ratio[start_index:breakout_index]
    if np.isnan(shadow_turnover_ratio).any():
        return None

    candle_low = np.minimum(shadow_open, shadow_close)
    candle_high = np.maximum(shadow_open, shadow_close)
    lower_shadow = candle_low - shadow_low
    upper_shadow = shadow_high - candle_high
    body = np.abs(shadow_close - shadow_open)
    dominant_other_part = np.maximum(body, upper_shadow)
    close_recovery = (shadow_close - shadow_low) / shadow_close

    shadow_mask = (
        (shadow_turnover_ratio >= SHADOW_TURNOVER_RATIO_MIN)
        & (lower_shadow > 0.0)
        & (close_recovery >= LONG_LOWER_SHADOW_RECOVERY_MIN)
        & (lower_shadow >= LONG_LOWER_SHADOW_DOMINANCE_MIN * dominant_other_part)
    )
    shadow_indices = np.flatnonzero(shadow_mask)
    if shadow_indices.size == 0:
        return None

    last_shadow_offset = int(shadow_indices[-1])
    last_shadow_index = start_index + last_shadow_offset
    return {
        "shadow_signal_days": int(shadow_indices.size),
        "last_shadow_date": str(trade_date[last_shadow_index]),
        "last_shadow_gap_days": int(breakout_index - last_shadow_index),
        "max_shadow_turnover_ratio": float(shadow_turnover_ratio[shadow_mask].max()),
        "max_shadow_close_recovery_pct": float(close_recovery[shadow_mask].max()),
    }


def build_breakout_events(
    *,
    source_db_path: Path,
    entry_start: str,
    entry_end: str,
    query_start: str,
    query_end: str,
) -> pd.DataFrame:
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        frame = conn.execute(
            """
            SELECT
                d.security_id,
                coalesce(m.current_name, d.security_id) AS current_name,
                d.trade_date,
                d.board,
                coalesce(d.is_st, false) AS is_st,
                coalesce(t.is_suspended, false) AS is_suspended,
                d.open_adj,
                d.high_adj,
                d.low_adj,
                d.close_adj,
                d.turnover_value_cny
            FROM daily_bar_pit d
            LEFT JOIN tradeability_state_daily t
              ON d.security_id = t.security_id
             AND d.trade_date = t.trade_date
            LEFT JOIN security_master_ref m
              ON d.security_id = m.security_id
            WHERE d.trade_date BETWEEN ? AND ?
              AND d.price_basis = 'unadjusted'
              AND d.board <> 'beijing'
              AND d.open_adj IS NOT NULL
              AND d.high_adj IS NOT NULL
              AND d.low_adj IS NOT NULL
              AND d.close_adj IS NOT NULL
              AND d.turnover_value_cny IS NOT NULL
              AND d.turnover_value_cny > 0
            ORDER BY d.security_id, d.trade_date
            """,
            [query_start, query_end],
        ).fetchdf()
    finally:
        conn.close()

    if frame.empty:
        return pd.DataFrame(columns=EVENT_COLUMNS)

    rows: list[dict[str, Any]] = []
    min_required_rows = LEVEL_LOOKBACK_DAYS + min(BASE_WINDOWS) + FORWARD_DAYS + 1
    for _, security_frame in frame.groupby("security_id", sort=False):
        security_frame = security_frame.reset_index(drop=True).copy()
        if len(security_frame) < min_required_rows:
            continue

        security_frame["turnover_median20"] = (
            security_frame["turnover_value_cny"]
            .rolling(TURNOVER_BASELINE_DAYS, min_periods=TURNOVER_BASELINE_DAYS)
            .median()
            .shift(1)
        )
        security_frame["turnover_ratio"] = (
            security_frame["turnover_value_cny"] / security_frame["turnover_median20"]
        )
        security_frame["prior_252_low"] = (
            security_frame["low_adj"]
            .rolling(LEVEL_LOOKBACK_DAYS, min_periods=LEVEL_LOOKBACK_DAYS)
            .min()
            .shift(1)
        )
        security_frame["prior_252_high"] = (
            security_frame["high_adj"]
            .rolling(LEVEL_LOOKBACK_DAYS, min_periods=LEVEL_LOOKBACK_DAYS)
            .max()
            .shift(1)
        )

        trade_date = security_frame["trade_date"].astype(str).to_numpy()
        security_id = security_frame["security_id"].astype(str).to_numpy()
        current_name = security_frame["current_name"].astype(str).to_numpy()
        board = security_frame["board"].astype(str).to_numpy()
        is_st = security_frame["is_st"].fillna(False).to_numpy(dtype=bool)
        is_suspended = security_frame["is_suspended"].fillna(False).to_numpy(dtype=bool)
        open_adj = security_frame["open_adj"].to_numpy(dtype=float)
        high_adj = security_frame["high_adj"].to_numpy(dtype=float)
        low_adj = security_frame["low_adj"].to_numpy(dtype=float)
        close_adj = security_frame["close_adj"].to_numpy(dtype=float)
        turnover_ratio = security_frame["turnover_ratio"].to_numpy(dtype=float)
        prior_252_low = security_frame["prior_252_low"].to_numpy(dtype=float)
        prior_252_high = security_frame["prior_252_high"].to_numpy(dtype=float)

        for breakout_index in range(LEVEL_LOOKBACK_DAYS, len(security_frame) - FORWARD_DAYS - 1):
            breakout_date = str(trade_date[breakout_index])
            if breakout_date < entry_start or breakout_date > entry_end:
                continue
            if is_st[breakout_index] or is_suspended[breakout_index]:
                continue

            base_candidate = _select_base_candidate(
                trade_date=trade_date,
                close_adj=close_adj,
                turnover_ratio=turnover_ratio,
                prior_252_low=prior_252_low,
                prior_252_high=prior_252_high,
                breakout_index=breakout_index,
            )
            if base_candidate is None:
                continue

            shadow_signal = _select_shadow_signal(
                trade_date=trade_date,
                open_adj=open_adj,
                high_adj=high_adj,
                low_adj=low_adj,
                close_adj=close_adj,
                turnover_ratio=turnover_ratio,
                start_index=breakout_index - int(base_candidate["base_days"]),
                breakout_index=breakout_index,
            )
            if shadow_signal is None:
                continue

            forward_stats = _compute_forward_stats(
                trade_date=trade_date,
                open_adj=open_adj,
                high_adj=high_adj,
                low_adj=low_adj,
                close_adj=close_adj,
                breakout_index=breakout_index,
            )
            if forward_stats["entry_date"] is None:
                continue

            row = {
                "security_id": str(security_id[breakout_index]),
                "current_name": str(current_name[breakout_index]),
                "board": str(board[breakout_index]),
                "breakout_date": breakout_date,
                "breakout_close": float(close_adj[breakout_index]),
            }
            row.update(base_candidate)
            row.update(shadow_signal)
            row.update(forward_stats)
            rows.append(row)

    return pd.DataFrame(rows, columns=EVENT_COLUMNS).sort_values(
        ["breakout_date", "security_id"], ascending=[True, True]
    ).reset_index(drop=True)


def _summary_scope_frame(events: pd.DataFrame, *, scope: str) -> pd.DataFrame:
    if scope == "all":
        return events.copy()
    return events.loc[events["board"] == scope].copy()


def summarize_events(events: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    event_columns = [
        "scope",
        "year_group",
        "events",
        "unique_stocks",
        "mean_base_days",
        "median_base_days",
        "mean_volume_asymmetry_ratio",
        "median_base_width",
        "median_price_level_pct_252",
        "mean_shadow_signal_days",
        "median_last_shadow_gap_days",
    ]
    forward_columns = [
        "scope",
        "year_group",
        "events",
        "mean_close_ret20",
        "median_close_ret20",
        "mean_close_ret60",
        "median_close_ret60",
        "mean_close_ret120",
        "median_close_ret120",
        "mean_max_ret120",
        "median_max_ret120",
        "mean_min_ret120",
        "median_min_ret120",
        "up20_rate",
        "up40_rate",
        "loss10_rate",
        "first_hit_up20_rate",
        "first_hit_loss10_rate",
        "first_hit_unresolved_rate",
        "both_same_day_rate",
    ]
    if events.empty:
        return pd.DataFrame(columns=event_columns), pd.DataFrame(columns=forward_columns)

    working = events.copy()
    working["event_year"] = working["breakout_date"].astype(str).str.slice(0, 4)
    event_rows: list[dict[str, Any]] = []
    forward_rows: list[dict[str, Any]] = []
    for scope in ("all", "main_board", "chinext", "star"):
        scoped = _summary_scope_frame(working, scope=scope)
        if scoped.empty:
            continue
        for year_group in ("all", *sorted(scoped["event_year"].unique().tolist())):
            if year_group == "all":
                group = scoped
            else:
                group = scoped.loc[scoped["event_year"] == year_group]
            if group.empty:
                continue
            event_rows.append(
                {
                    "scope": scope,
                    "year_group": year_group,
                    "events": int(len(group)),
                    "unique_stocks": int(group["security_id"].nunique()),
                    "mean_base_days": float(group["base_days"].mean()),
                    "median_base_days": float(group["base_days"].median()),
                    "mean_volume_asymmetry_ratio": float(group["volume_asymmetry_ratio"].mean()),
                    "median_base_width": float(group["base_width"].median()),
                    "median_price_level_pct_252": float(group["price_level_pct_252"].median()),
                    "mean_shadow_signal_days": float(group["shadow_signal_days"].mean()),
                    "median_last_shadow_gap_days": float(group["last_shadow_gap_days"].median()),
                }
            )
            first_hit = group["first_hit"].astype(str)
            forward_rows.append(
                {
                    "scope": scope,
                    "year_group": year_group,
                    "events": int(len(group)),
                    "mean_close_ret20": float(group["close_ret20"].mean()),
                    "median_close_ret20": float(group["close_ret20"].median()),
                    "mean_close_ret60": float(group["close_ret60"].mean()),
                    "median_close_ret60": float(group["close_ret60"].median()),
                    "mean_close_ret120": float(group["close_ret120"].mean()),
                    "median_close_ret120": float(group["close_ret120"].median()),
                    "mean_max_ret120": float(group["max_ret120"].mean()),
                    "median_max_ret120": float(group["max_ret120"].median()),
                    "mean_min_ret120": float(group["min_ret120"].mean()),
                    "median_min_ret120": float(group["min_ret120"].median()),
                    "up20_rate": float(group["up20"].mean()),
                    "up40_rate": float(group["up40"].mean()),
                    "loss10_rate": float(group["loss10"].mean()),
                    "first_hit_up20_rate": float((first_hit == "up20_first").mean()),
                    "first_hit_loss10_rate": float((first_hit == "loss10_first").mean()),
                    "first_hit_unresolved_rate": float((first_hit == "unresolved").mean()),
                    "both_same_day_rate": float((first_hit == "both_same_day").mean()),
                }
            )
    return pd.DataFrame(event_rows), pd.DataFrame(forward_rows)


def _build_base_days_summary(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(
            columns=["base_days", "events", "unique_stocks", "mean_close_ret120", "up20_rate", "loss10_rate"]
        )
    summary = (
        events.groupby("base_days", as_index=False)
        .agg(
            events=("security_id", "size"),
            unique_stocks=("security_id", "nunique"),
            mean_close_ret120=("close_ret120", "mean"),
            up20_rate=("up20", "mean"),
            loss10_rate=("loss10", "mean"),
        )
        .sort_values("base_days", ascending=True)
        .reset_index(drop=True)
    )
    return summary


def write_summary_report(
    *,
    report_markdown_path: Path,
    source_db_path: Path,
    events_csv_path: Path,
    event_summary_csv_path: Path,
    forward_summary_csv_path: Path,
    events: pd.DataFrame,
    event_summary: pd.DataFrame,
    forward_summary: pd.DataFrame,
    entry_start: str,
    entry_end: str,
) -> None:
    overall_events = event_summary.loc[
        (event_summary["scope"] == "all") & (event_summary["year_group"] == "all")
    ].reset_index(drop=True)
    overall_forward = forward_summary.loc[
        (forward_summary["scope"] == "all") & (forward_summary["year_group"] == "all")
    ].reset_index(drop=True)
    base_days_summary = _build_base_days_summary(events)
    strongest = (
        events.loc[
            :,
            [
                "security_id",
                "current_name",
                "breakout_date",
                "base_days",
                "volume_asymmetry_ratio",
                "close_ret120",
                "max_ret120",
                "min_ret120",
            ],
        ]
        .sort_values(["close_ret120", "max_ret120"], ascending=[False, False])
        .head(10)
        .reset_index(drop=True)
    )
    weakest = (
        events.loc[
            :,
            [
                "security_id",
                "current_name",
                "breakout_date",
                "base_days",
                "volume_asymmetry_ratio",
                "close_ret120",
                "max_ret120",
                "min_ret120",
            ],
        ]
        .sort_values(["close_ret120", "min_ret120"], ascending=[True, True])
        .head(10)
        .reset_index(drop=True)
    )
    report_lines = [
        f"# Low-Base Volume-Asymmetry Breakout Study - {date.today().isoformat()}",
        "",
        "## 对象",
        f"- 源数据库：`{source_db_path}`",
        f"- 事件明细：`{events_csv_path}`",
        f"- 事件汇总：`{event_summary_csv_path}`",
        f"- 半年走势汇总：`{forward_summary_csv_path}`",
        "",
        "## 研究定义",
        f"- 候选突破日：`{entry_start}` 到 `{entry_end}`，A 股，排除 `beijing`，突破日不是 `ST` 且不是停牌日。",
        f"- 低位定义：突破日前 `252` 个交易日价格区间内，震荡上沿仍处于该区间下 `45%`。",
        f"- 震荡定义：向前回看 `20/40/60/80/100/120` 个交易日，选满足条件的最长窗口；区间宽度不超过 `30%`。",
        f"- 量能不对称：震荡区间内，价格落在下四分位的日子，其成交额/前 `20` 日成交额中位数的均值，至少是上四分位对应均值的 `1.30x`。",
        f"- 启动前长下影：震荡区间内至少出现 `1` 次高量长下影日；该日成交额/前 `20` 日成交额中位数至少 `1.30x`，且 `(收盘-最低)/收盘 >= 4%`，下影长度至少是实体和上影的 `1.5x`。",
        f"- 突破定义：突破日复权收盘价严格高于震荡区间内前序复权收盘价上沿。",
        f"- 前瞻口径：突破次日开盘买入，向后观察 `120` 个交易日，统计 `20/60/120` 日收盘收益、区间最大涨幅与最大回撤。",
        "",
        "## 事件总览",
        _markdown_table(overall_events),
        "",
        "## 半年走势总览",
        _markdown_table(overall_forward),
        "",
        "## 分年半年走势",
        _markdown_table(
            forward_summary.loc[
                (forward_summary["scope"] == "all") & (forward_summary["year_group"] != "all")
            ].reset_index(drop=True)
        ),
        "",
        "## 按震荡长度分组",
        _markdown_table(base_days_summary),
        "",
        "## 代表性强势样本",
        _markdown_table(strongest),
        "",
        "## 代表性弱势样本",
        _markdown_table(weakest),
    ]
    report_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    report_markdown_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")


def run_low_base_volume_asymmetry_breakout_study(
    *,
    source_db_path: Path,
    events_csv_path: Path,
    event_summary_csv_path: Path,
    forward_summary_csv_path: Path,
    report_markdown_path: Path,
    entry_start: str = "20240101",
    entry_end: str = "20251231",
    query_start: str = "20220101",
    query_end: str = "20261231",
) -> dict[str, pd.DataFrame]:
    events = build_breakout_events(
        source_db_path=source_db_path,
        entry_start=entry_start,
        entry_end=entry_end,
        query_start=query_start,
        query_end=query_end,
    )
    event_summary, forward_summary = summarize_events(events)

    events_csv_path.parent.mkdir(parents=True, exist_ok=True)
    event_summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
    forward_summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
    events.to_csv(events_csv_path, index=False)
    event_summary.to_csv(event_summary_csv_path, index=False)
    forward_summary.to_csv(forward_summary_csv_path, index=False)
    write_summary_report(
        report_markdown_path=report_markdown_path,
        source_db_path=source_db_path,
        events_csv_path=events_csv_path,
        event_summary_csv_path=event_summary_csv_path,
        forward_summary_csv_path=forward_summary_csv_path,
        events=events,
        event_summary=event_summary,
        forward_summary=forward_summary,
        entry_start=entry_start,
        entry_end=entry_end,
    )
    return {
        "events": events,
        "event_summary": event_summary,
        "forward_summary": forward_summary,
    }


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the low-base volume-asymmetry breakout study.",
    )
    parser.add_argument("--source-db", required=True, type=Path)
    parser.add_argument("--events-csv", required=True, type=Path)
    parser.add_argument("--event-summary-csv", required=True, type=Path)
    parser.add_argument("--forward-summary-csv", required=True, type=Path)
    parser.add_argument("--report-markdown", required=True, type=Path)
    parser.add_argument("--entry-start", default="20240101")
    parser.add_argument("--entry-end", default="20251231")
    parser.add_argument("--query-start", default="20220101")
    parser.add_argument("--query-end", default="20261231")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_low_base_volume_asymmetry_breakout_study(
        source_db_path=args.source_db,
        events_csv_path=args.events_csv,
        event_summary_csv_path=args.event_summary_csv,
        forward_summary_csv_path=args.forward_summary_csv,
        report_markdown_path=args.report_markdown,
        entry_start=str(args.entry_start),
        entry_end=str(args.entry_end),
        query_start=str(args.query_start),
        query_end=str(args.query_end),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
