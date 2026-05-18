from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from .up5in10_price_volume_study import _markdown_table

DEFAULT_WINDOWS: tuple[int, ...] = (120, 250)


def classify_range_zone(
    range_position: float,
    *,
    low_cutoff: float = 0.20,
    high_cutoff: float = 0.80,
) -> str:
    if not np.isfinite(range_position):
        return "unknown"
    if range_position <= low_cutoff:
        return "low"
    if range_position >= high_cutoff:
        return "high"
    return "mid"


def load_selected_security_bars(
    *,
    source_db_path: Path,
    security_ids: list[str],
    query_start: str,
    query_end: str,
) -> pd.DataFrame:
    columns = ["security_id", "trade_date", "close_adj", "high_adj", "low_adj"]
    if not security_ids:
        return pd.DataFrame(columns=columns)

    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        security_frame = pd.DataFrame({"security_id": sorted(set(str(security_id) for security_id in security_ids))})
        conn.register("selected_security_ids", security_frame)
        frame = conn.execute(
            """
            SELECT
                d.security_id,
                d.trade_date,
                d.close_adj,
                d.high_adj,
                d.low_adj
            FROM daily_bar_pit d
            JOIN selected_security_ids s
              ON d.security_id = s.security_id
            WHERE d.trade_date BETWEEN ? AND ?
              AND d.price_basis = 'unadjusted'
              AND d.close_adj IS NOT NULL
              AND d.high_adj IS NOT NULL
              AND d.low_adj IS NOT NULL
            ORDER BY d.security_id, d.trade_date
            """,
            [query_start, query_end],
        ).fetchdf()
    finally:
        conn.close()
    return frame[columns] if not frame.empty else pd.DataFrame(columns=columns)


def attach_trailing_price_levels(
    bars: pd.DataFrame,
    *,
    windows: tuple[int, ...] = DEFAULT_WINDOWS,
) -> pd.DataFrame:
    if bars.empty:
        return bars.copy()

    parts: list[pd.DataFrame] = []
    for _, frame in bars.groupby("security_id", sort=False):
        working = frame.sort_values("trade_date").reset_index(drop=True).copy()
        for window_size in windows:
            rolling_high = working["high_adj"].rolling(window=window_size, min_periods=window_size).max()
            rolling_low = working["low_adj"].rolling(window=window_size, min_periods=window_size).min()
            denom = rolling_high - rolling_low
            range_position = (working["close_adj"] - rolling_low) / denom
            range_position = range_position.where(denom > 0.0)
            working[f"range_pos_{window_size}"] = range_position
            working[f"range_zone_{window_size}"] = range_position.map(classify_range_zone)
        parts.append(working)
    return pd.concat(parts, ignore_index=True)


def attach_price_levels_to_selected_rows(
    selected_rows: pd.DataFrame,
    *,
    price_level_rows: pd.DataFrame,
) -> pd.DataFrame:
    if selected_rows.empty or price_level_rows.empty:
        return pd.DataFrame()

    left = selected_rows.copy()
    right = price_level_rows.copy()
    left["trade_date"] = left["trade_date"].astype(str)
    right["trade_date"] = right["trade_date"].astype(str)
    return left.merge(
        right,
        on=["security_id", "trade_date"],
        how="left",
    )


def build_zone_summary(
    rows: pd.DataFrame,
    *,
    window_size: int,
    group_columns: tuple[str, ...] = ("top_n",),
) -> pd.DataFrame:
    zone_column = f"range_zone_{window_size}"
    required = [*group_columns, zone_column, "success_label", "close_ret30", "max_ret30", "min_ret30"]
    if rows.empty:
        return pd.DataFrame(
            columns=[
                *group_columns,
                "window_size",
                "range_zone",
                "selected_rows",
                "share_within_group",
                "success_rate",
                "mean_close_ret30",
                "mean_max_ret30",
                "mean_min_ret30",
            ]
        )

    working = rows.loc[rows[zone_column].isin(["low", "mid", "high"]), required].copy()
    if working.empty:
        return pd.DataFrame(
            columns=[
                *group_columns,
                "window_size",
                "range_zone",
                "selected_rows",
                "share_within_group",
                "success_rate",
                "mean_close_ret30",
                "mean_max_ret30",
                "mean_min_ret30",
            ]
        )

    summary = (
        working.groupby([*group_columns, zone_column], as_index=False)
        .agg(
            selected_rows=("success_label", "size"),
            success_rate=("success_label", "mean"),
            mean_close_ret30=("close_ret30", "mean"),
            mean_max_ret30=("max_ret30", "mean"),
            mean_min_ret30=("min_ret30", "mean"),
        )
        .rename(columns={zone_column: "range_zone"})
    )
    totals = summary.groupby(list(group_columns), as_index=False)["selected_rows"].sum().rename(
        columns={"selected_rows": "group_total"}
    )
    summary = summary.merge(totals, on=list(group_columns), how="left")
    summary["share_within_group"] = summary["selected_rows"] / summary["group_total"]
    summary["window_size"] = int(window_size)
    columns = [
        *group_columns,
        "window_size",
        "range_zone",
        "selected_rows",
        "share_within_group",
        "success_rate",
        "mean_close_ret30",
        "mean_max_ret30",
        "mean_min_ret30",
    ]
    return summary[columns].sort_values([*group_columns, "range_zone"]).reset_index(drop=True)


def build_high_low_diff_summary(
    zone_summary: pd.DataFrame,
    *,
    group_columns: tuple[str, ...] = ("top_n",),
) -> pd.DataFrame:
    if zone_summary.empty:
        return pd.DataFrame(
            columns=[
                *group_columns,
                "window_size",
                "high_rows",
                "low_rows",
                "high_share",
                "low_share",
                "success_rate_high",
                "success_rate_low",
                "success_rate_high_minus_low",
                "mean_close_ret30_high",
                "mean_close_ret30_low",
                "mean_close_ret30_high_minus_low",
                "mean_max_ret30_high",
                "mean_max_ret30_low",
                "mean_max_ret30_high_minus_low",
                "mean_min_ret30_high",
                "mean_min_ret30_low",
                "mean_min_ret30_high_minus_low",
            ]
        )

    index_columns = [*group_columns, "window_size"]
    pivot = zone_summary.pivot_table(
        index=index_columns,
        columns="range_zone",
        values=[
            "selected_rows",
            "share_within_group",
            "success_rate",
            "mean_close_ret30",
            "mean_max_ret30",
            "mean_min_ret30",
        ],
        aggfunc="first",
    )
    if pivot.empty:
        return pd.DataFrame()
    pivot.columns = [f"{metric}_{zone}" for metric, zone in pivot.columns]
    result = pivot.reset_index().copy()
    result["high_rows"] = result.get("selected_rows_high", 0.0)
    result["low_rows"] = result.get("selected_rows_low", 0.0)
    result["high_share"] = result.get("share_within_group_high", np.nan)
    result["low_share"] = result.get("share_within_group_low", np.nan)
    result["success_rate_high"] = result.get("success_rate_high", np.nan)
    result["success_rate_low"] = result.get("success_rate_low", np.nan)
    result["success_rate_high_minus_low"] = result["success_rate_high"] - result["success_rate_low"]
    result["mean_close_ret30_high"] = result.get("mean_close_ret30_high", np.nan)
    result["mean_close_ret30_low"] = result.get("mean_close_ret30_low", np.nan)
    result["mean_close_ret30_high_minus_low"] = (
        result["mean_close_ret30_high"] - result["mean_close_ret30_low"]
    )
    result["mean_max_ret30_high"] = result.get("mean_max_ret30_high", np.nan)
    result["mean_max_ret30_low"] = result.get("mean_max_ret30_low", np.nan)
    result["mean_max_ret30_high_minus_low"] = result["mean_max_ret30_high"] - result["mean_max_ret30_low"]
    result["mean_min_ret30_high"] = result.get("mean_min_ret30_high", np.nan)
    result["mean_min_ret30_low"] = result.get("mean_min_ret30_low", np.nan)
    result["mean_min_ret30_high_minus_low"] = result["mean_min_ret30_high"] - result["mean_min_ret30_low"]
    columns = [
        *group_columns,
        "window_size",
        "high_rows",
        "low_rows",
        "high_share",
        "low_share",
        "success_rate_high",
        "success_rate_low",
        "success_rate_high_minus_low",
        "mean_close_ret30_high",
        "mean_close_ret30_low",
        "mean_close_ret30_high_minus_low",
        "mean_max_ret30_high",
        "mean_max_ret30_low",
        "mean_max_ret30_high_minus_low",
        "mean_min_ret30_high",
        "mean_min_ret30_low",
        "mean_min_ret30_high_minus_low",
    ]
    return result[columns].sort_values(index_columns).reset_index(drop=True)


def attach_price_levels_to_replays(
    replay_rows: pd.DataFrame,
    *,
    selected_with_levels: pd.DataFrame,
) -> pd.DataFrame:
    if replay_rows.empty or selected_with_levels.empty:
        return pd.DataFrame()

    level_columns = [
        "security_id",
        "trade_date",
        "top_n",
        "range_pos_120",
        "range_zone_120",
        "range_pos_250",
        "range_zone_250",
    ]
    merge_levels = selected_with_levels[level_columns].drop_duplicates().rename(
        columns={"trade_date": "entry_trade_date"}
    )
    left = replay_rows.copy()
    left["entry_trade_date"] = left["entry_trade_date"].astype(str)
    merge_levels["entry_trade_date"] = merge_levels["entry_trade_date"].astype(str)
    return left.merge(
        merge_levels,
        on=["security_id", "entry_trade_date", "top_n"],
        how="left",
    )


def build_replay_zone_summary(
    replay_rows: pd.DataFrame,
    *,
    window_size: int,
    group_columns: tuple[str, ...] = ("top_n", "policy_name"),
) -> pd.DataFrame:
    zone_column = f"range_zone_{window_size}"
    if replay_rows.empty:
        return pd.DataFrame(
            columns=[
                *group_columns,
                "window_size",
                "range_zone",
                "trades",
                "share_within_group",
                "mean_net_ret",
                "median_net_ret",
                "win_rate_pos",
                "avg_exit_step",
            ]
        )

    working = replay_rows.loc[replay_rows[zone_column].isin(["low", "mid", "high"])].copy()
    if working.empty:
        return pd.DataFrame(
            columns=[
                *group_columns,
                "window_size",
                "range_zone",
                "trades",
                "share_within_group",
                "mean_net_ret",
                "median_net_ret",
                "win_rate_pos",
                "avg_exit_step",
            ]
        )

    summary = (
        working.groupby([*group_columns, zone_column], as_index=False)
        .agg(
            trades=("net_ret", "size"),
            mean_net_ret=("net_ret", "mean"),
            median_net_ret=("net_ret", "median"),
            win_rate_pos=("net_ret", lambda series: float((series > 0.0).mean())),
            avg_exit_step=("exit_step", "mean"),
        )
        .rename(columns={zone_column: "range_zone"})
    )
    totals = summary.groupby(list(group_columns), as_index=False)["trades"].sum().rename(columns={"trades": "group_total"})
    summary = summary.merge(totals, on=list(group_columns), how="left")
    summary["share_within_group"] = summary["trades"] / summary["group_total"]
    summary["window_size"] = int(window_size)
    columns = [
        *group_columns,
        "window_size",
        "range_zone",
        "trades",
        "share_within_group",
        "mean_net_ret",
        "median_net_ret",
        "win_rate_pos",
        "avg_exit_step",
    ]
    return summary[columns].sort_values([*group_columns, "range_zone"]).reset_index(drop=True)


def write_level_report(
    *,
    report_markdown_path: Path,
    zone_summary: pd.DataFrame,
    diff_summary: pd.DataFrame,
    replay_zone_summary: pd.DataFrame,
) -> None:
    lines = [
        "# up5in10 独立信号的半年/一年高低位研究",
        "",
        "这份报告只分析独立路径分数选出来的股票，默认只保留 `p80`，因为上一轮已经确认 `p80` 与 `p90` 在 `top1/top3/top5` 下选出的股票完全相同。",
        "",
        "## 事件层分布",
        "",
        _markdown_table(zone_summary) if not zone_summary.empty else "无事件层结果。",
        "",
        "## 高低位差值",
        "",
        _markdown_table(diff_summary) if not diff_summary.empty else "无高低位差值。",
        "",
        "## 交易回放分布",
        "",
        _markdown_table(replay_zone_summary) if not replay_zone_summary.empty else "无交易回放结果。",
        "",
    ]
    report_markdown_path.write_text("\n".join(lines), encoding="utf-8")


def run_level_study(
    *,
    source_db_path: Path,
    selected_rows_csv_path: Path,
    replay_rows_csv_path: Path,
    selected_with_levels_output_path: Path,
    zone_summary_output_path: Path,
    high_low_diff_output_path: Path,
    replay_zone_summary_output_path: Path,
    report_markdown_path: Path,
    threshold_name: str = "p80",
    query_start: str = "20200101",
    query_end: str = date.today().strftime("%Y%m%d"),
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    selected_rows = pd.read_csv(selected_rows_csv_path)
    selected_rows = selected_rows.loc[selected_rows["threshold_name"] == threshold_name].copy()
    selected_rows["trade_date"] = selected_rows["trade_date"].astype(str)

    replay_rows = pd.read_csv(replay_rows_csv_path)
    replay_rows = replay_rows.loc[replay_rows["threshold_name"] == threshold_name].copy()
    replay_rows["entry_trade_date"] = replay_rows["entry_trade_date"].astype(str)

    bars = load_selected_security_bars(
        source_db_path=source_db_path,
        security_ids=selected_rows["security_id"].astype(str).drop_duplicates().tolist(),
        query_start=query_start,
        query_end=query_end,
    )
    price_level_rows = attach_trailing_price_levels(bars)
    selected_with_levels = attach_price_levels_to_selected_rows(selected_rows, price_level_rows=price_level_rows)

    zone_parts: list[pd.DataFrame] = []
    zone_year_parts: list[pd.DataFrame] = []
    diff_parts: list[pd.DataFrame] = []
    replay_parts: list[pd.DataFrame] = []
    for window_size in DEFAULT_WINDOWS:
        zone_parts.append(build_zone_summary(selected_with_levels, window_size=window_size, group_columns=("top_n",)))
        zone_year_parts.append(
            build_zone_summary(selected_with_levels, window_size=window_size, group_columns=("top_n", "year"))
        )
        diff_parts.append(
            build_high_low_diff_summary(zone_year_parts[-1], group_columns=("top_n", "year"))
        )

    replay_with_levels = attach_price_levels_to_replays(replay_rows, selected_with_levels=selected_with_levels)
    for window_size in DEFAULT_WINDOWS:
        replay_parts.append(
            build_replay_zone_summary(
                replay_with_levels,
                window_size=window_size,
                group_columns=("top_n", "policy_name"),
            )
        )

    zone_summary = pd.concat(zone_parts + zone_year_parts, ignore_index=True)
    diff_summary = pd.concat(diff_parts, ignore_index=True)
    replay_zone_summary = pd.concat(replay_parts, ignore_index=True)

    selected_with_levels.to_csv(selected_with_levels_output_path, index=False)
    zone_summary.to_csv(zone_summary_output_path, index=False)
    diff_summary.to_csv(high_low_diff_output_path, index=False)
    replay_zone_summary.to_csv(replay_zone_summary_output_path, index=False)
    write_level_report(
        report_markdown_path=report_markdown_path,
        zone_summary=zone_summary,
        diff_summary=diff_summary,
        replay_zone_summary=replay_zone_summary,
    )
    return selected_with_levels, zone_summary, diff_summary, replay_zone_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Study high-vs-low price level effects for standalone up5in10 selections.")
    parser.add_argument("--source-db", type=Path, default=Path("output/research_source.duckdb"))
    parser.add_argument("--selected-rows-csv", type=Path, default=Path(".tmp/up5in10_standalone_selected_rows.csv"))
    parser.add_argument("--replay-rows-csv", type=Path, default=Path(".tmp/up5in10_standalone_replays.csv"))
    parser.add_argument("--selected-with-levels-output", type=Path, default=Path(".tmp/up5in10_standalone_selected_with_levels.csv"))
    parser.add_argument("--zone-summary-output", type=Path, default=Path(".tmp/up5in10_standalone_level_zone_summary.csv"))
    parser.add_argument("--high-low-diff-output", type=Path, default=Path(".tmp/up5in10_standalone_level_high_low_diff.csv"))
    parser.add_argument("--replay-zone-summary-output", type=Path, default=Path(".tmp/up5in10_standalone_level_replay_zone_summary.csv"))
    parser.add_argument("--report-output", type=Path, default=Path(".tmp/up5in10_standalone_level_report.md"))
    parser.add_argument("--threshold-name", default="p80")
    parser.add_argument("--query-start", default="20200101")
    parser.add_argument("--query-end", default=date.today().strftime("%Y%m%d"))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    _, zone_summary, diff_summary, replay_zone_summary = run_level_study(
        source_db_path=args.source_db,
        selected_rows_csv_path=args.selected_rows_csv,
        replay_rows_csv_path=args.replay_rows_csv,
        selected_with_levels_output_path=args.selected_with_levels_output,
        zone_summary_output_path=args.zone_summary_output,
        high_low_diff_output_path=args.high_low_diff_output,
        replay_zone_summary_output_path=args.replay_zone_summary_output,
        report_markdown_path=args.report_output,
        threshold_name=args.threshold_name,
        query_start=args.query_start,
        query_end=args.query_end,
    )
    print("ZONE SUMMARY")
    print(zone_summary.to_string(index=False))
    print("\nHIGH LOW DIFF")
    print(diff_summary.to_string(index=False))
    print("\nREPLAY ZONE SUMMARY")
    print(replay_zone_summary.to_string(index=False))


if __name__ == "__main__":
    main()
