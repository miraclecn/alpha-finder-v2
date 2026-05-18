from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .up5in10_price_volume_study import _markdown_table

DEFAULT_BAND_EDGES: tuple[tuple[float, float, str], ...] = (
    (0.00, 0.10, "0_10"),
    (0.10, 0.20, "10_20"),
    (0.20, 0.35, "20_35"),
    (0.35, 0.50, "35_50"),
    (0.50, 0.65, "50_65"),
    (0.65, 0.80, "65_80"),
    (0.80, 1.01, "80_100"),
)

DEFAULT_FILTER_SPECS: dict[str, dict[str, tuple[float, float]]] = {
    "10_35": {"range_pos_120": (0.10, 0.35)},
    "20_35": {"range_pos_120": (0.20, 0.35)},
    "10_50": {"range_pos_120": (0.10, 0.50)},
    "20_50": {"range_pos_120": (0.20, 0.50)},
    "10_65": {"range_pos_120": (0.10, 0.65)},
    "120:10_35 & 250:10_35": {"range_pos_120": (0.10, 0.35), "range_pos_250": (0.10, 0.35)},
    "120:20_35 & 250:10_35": {"range_pos_120": (0.20, 0.35), "range_pos_250": (0.10, 0.35)},
    "120:10_50 & 250:10_35": {"range_pos_120": (0.10, 0.50), "range_pos_250": (0.10, 0.35)},
    "120:20_50 & 250:10_35": {"range_pos_120": (0.20, 0.50), "range_pos_250": (0.10, 0.35)},
    "120:10_35 & 250:10_50": {"range_pos_120": (0.10, 0.35), "range_pos_250": (0.10, 0.50)},
}


def assign_position_band(
    range_position: float,
    *,
    band_edges: tuple[tuple[float, float, str], ...] = DEFAULT_BAND_EDGES,
) -> str:
    if not np.isfinite(range_position):
        return "unknown"
    for left, right, label in band_edges:
        if left <= float(range_position) < right:
            return label
    return "unknown"


def add_band_columns(
    rows: pd.DataFrame,
    *,
    windows: tuple[int, ...] = (120, 250),
) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    working = rows.copy()
    for window in windows:
        position_column = f"range_pos_{window}"
        band_column = f"band_{window}"
        working[band_column] = working[position_column].map(assign_position_band)
    return working


def _apply_filter_mask(
    rows: pd.DataFrame,
    *,
    filter_rules: dict[str, tuple[float, float]],
) -> pd.Series:
    mask = pd.Series(True, index=rows.index)
    for column, (lower, upper) in filter_rules.items():
        mask &= rows[column].ge(lower) & rows[column].lt(upper)
    return mask


def build_filter_event_summary(
    rows: pd.DataFrame,
    *,
    filter_specs: dict[str, dict[str, tuple[float, float]]] = DEFAULT_FILTER_SPECS,
    group_columns: tuple[str, ...] = ("top_n",),
) -> pd.DataFrame:
    columns = [
        *group_columns,
        "filter_name",
        "selected_rows",
        "share_within_group",
        "success_rate",
        "mean_close_ret30",
        "mean_max_ret30",
        "mean_min_ret30",
    ]
    if rows.empty:
        return pd.DataFrame(columns=columns)

    group_totals = rows.groupby(list(group_columns)).size().rename("group_total").reset_index()
    summary_rows: list[dict[str, Any]] = []
    for group_values, frame in rows.groupby(list(group_columns), sort=False):
        if not isinstance(group_values, tuple):
            group_values = (group_values,)
        base_payload = dict(zip(group_columns, group_values, strict=False))
        total = int(len(frame))
        for filter_name, filter_rules in filter_specs.items():
            selected = frame.loc[_apply_filter_mask(frame, filter_rules=filter_rules)].copy()
            if selected.empty:
                continue
            summary_rows.append(
                {
                    **base_payload,
                    "filter_name": filter_name,
                    "selected_rows": int(len(selected)),
                    "share_within_group": float(len(selected) / total),
                    "success_rate": float(selected["success_label"].mean()),
                    "mean_close_ret30": float(selected["close_ret30"].mean()),
                    "mean_max_ret30": float(selected["max_ret30"].mean()),
                    "mean_min_ret30": float(selected["min_ret30"].mean()),
                }
            )
    return pd.DataFrame(summary_rows, columns=columns).sort_values([*group_columns, "filter_name"]).reset_index(drop=True)


def build_filter_replay_summary(
    rows: pd.DataFrame,
    *,
    filter_specs: dict[str, dict[str, tuple[float, float]]] = DEFAULT_FILTER_SPECS,
    group_columns: tuple[str, ...] = ("top_n", "policy_name"),
) -> pd.DataFrame:
    columns = [
        *group_columns,
        "filter_name",
        "trades",
        "share_within_group",
        "mean_net_ret",
        "median_net_ret",
        "win_rate_pos",
        "avg_exit_step",
    ]
    if rows.empty:
        return pd.DataFrame(columns=columns)

    summary_rows: list[dict[str, Any]] = []
    for group_values, frame in rows.groupby(list(group_columns), sort=False):
        if not isinstance(group_values, tuple):
            group_values = (group_values,)
        base_payload = dict(zip(group_columns, group_values, strict=False))
        total = int(len(frame))
        for filter_name, filter_rules in filter_specs.items():
            selected = frame.loc[_apply_filter_mask(frame, filter_rules=filter_rules)].copy()
            if selected.empty:
                continue
            summary_rows.append(
                {
                    **base_payload,
                    "filter_name": filter_name,
                    "trades": int(len(selected)),
                    "share_within_group": float(len(selected) / total),
                    "mean_net_ret": float(selected["net_ret"].mean()),
                    "median_net_ret": float(selected["net_ret"].median()),
                    "win_rate_pos": float((selected["net_ret"] > 0.0).mean()),
                    "avg_exit_step": float(selected["exit_step"].mean()),
                }
            )
    return pd.DataFrame(summary_rows, columns=columns).sort_values([*group_columns, "filter_name"]).reset_index(drop=True)


def write_rebound_report(
    *,
    report_markdown_path: Path,
    band_event_summary: pd.DataFrame,
    filter_event_summary: pd.DataFrame,
    filter_replay_summary: pd.DataFrame,
) -> None:
    lines = [
        "# up5in10 独立信号的相对低位反弹段研究",
        "",
        "这份报告只分析独立路径分数选出的股票，目标是找出“不是最低位，但仍处在相对低位，且已经反弹一段”的位置区间。",
        "",
        "## 分段事件结果",
        "",
        _markdown_table(band_event_summary) if not band_event_summary.empty else "无分段事件结果。",
        "",
        "## 候选过滤区间事件结果",
        "",
        _markdown_table(filter_event_summary) if not filter_event_summary.empty else "无候选过滤区间事件结果。",
        "",
        "## 候选过滤区间交易结果",
        "",
        _markdown_table(filter_replay_summary) if not filter_replay_summary.empty else "无候选过滤区间交易结果。",
        "",
    ]
    report_markdown_path.write_text("\n".join(lines), encoding="utf-8")


def run_rebound_band_study(
    *,
    selected_with_levels_csv_path: Path,
    replay_rows_csv_path: Path,
    band_event_summary_output_path: Path,
    filter_event_summary_output_path: Path,
    filter_replay_summary_output_path: Path,
    report_markdown_path: Path,
    threshold_name: str = "p80",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    selected_rows = pd.read_csv(selected_with_levels_csv_path)
    selected_rows = selected_rows.loc[selected_rows["threshold_name"] == threshold_name].copy()
    selected_rows = add_band_columns(selected_rows)

    band_event_parts: list[pd.DataFrame] = []
    for window in (120, 250):
        band_column = f"band_{window}"
        part = (
            selected_rows.groupby(["top_n", band_column], as_index=False, observed=False)
            .agg(
                selected_rows=("security_id", "size"),
                success_rate=("success_label", "mean"),
                mean_close_ret30=("close_ret30", "mean"),
                mean_max_ret30=("max_ret30", "mean"),
                mean_min_ret30=("min_ret30", "mean"),
            )
            .rename(columns={band_column: "band"})
        )
        totals = part.groupby("top_n", as_index=False)["selected_rows"].sum().rename(columns={"selected_rows": "group_total"})
        part = part.merge(totals, on="top_n", how="left")
        part["share_within_group"] = part["selected_rows"] / part["group_total"]
        part["window_size"] = window
        band_event_parts.append(
            part[["top_n", "window_size", "band", "selected_rows", "share_within_group", "success_rate", "mean_close_ret30", "mean_max_ret30", "mean_min_ret30"]]
        )
    band_event_summary = pd.concat(band_event_parts, ignore_index=True)

    filter_event_summary = build_filter_event_summary(selected_rows)

    replay_rows = pd.read_csv(replay_rows_csv_path)
    replay_rows = replay_rows.loc[replay_rows["threshold_name"] == threshold_name].copy()
    level_columns = ["security_id", "trade_date", "top_n", "range_pos_120", "range_pos_250"]
    replay_rows = replay_rows.merge(
        selected_rows[level_columns].drop_duplicates(),
        left_on=["security_id", "entry_trade_date", "top_n"],
        right_on=["security_id", "trade_date", "top_n"],
        how="left",
    )
    replay_rows = replay_rows.loc[replay_rows["policy_name"] == "fixed_tp0.05_sl0.10_hold10"].copy()
    filter_replay_summary = build_filter_replay_summary(replay_rows)

    band_event_summary.to_csv(band_event_summary_output_path, index=False)
    filter_event_summary.to_csv(filter_event_summary_output_path, index=False)
    filter_replay_summary.to_csv(filter_replay_summary_output_path, index=False)
    write_rebound_report(
        report_markdown_path=report_markdown_path,
        band_event_summary=band_event_summary,
        filter_event_summary=filter_event_summary,
        filter_replay_summary=filter_replay_summary,
    )
    return band_event_summary, filter_event_summary, filter_replay_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Study relative-low rebound entry zones for standalone up5in10 selections.")
    parser.add_argument("--selected-with-levels-csv", type=Path, default=Path(".tmp/up5in10_standalone_selected_with_levels.csv"))
    parser.add_argument("--replay-rows-csv", type=Path, default=Path(".tmp/up5in10_standalone_replays.csv"))
    parser.add_argument("--band-event-summary-output", type=Path, default=Path(".tmp/up5in10_rebound_band_event_summary.csv"))
    parser.add_argument("--filter-event-summary-output", type=Path, default=Path(".tmp/up5in10_rebound_filter_event_summary.csv"))
    parser.add_argument("--filter-replay-summary-output", type=Path, default=Path(".tmp/up5in10_rebound_filter_replay_summary.csv"))
    parser.add_argument("--report-output", type=Path, default=Path(".tmp/up5in10_rebound_band_report.md"))
    parser.add_argument("--threshold-name", default="p80")
    parser.add_argument("--query-end", default=date.today().strftime("%Y%m%d"))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    band_event_summary, filter_event_summary, filter_replay_summary = run_rebound_band_study(
        selected_with_levels_csv_path=args.selected_with_levels_csv,
        replay_rows_csv_path=args.replay_rows_csv,
        band_event_summary_output_path=args.band_event_summary_output,
        filter_event_summary_output_path=args.filter_event_summary_output,
        filter_replay_summary_output_path=args.filter_replay_summary_output,
        report_markdown_path=args.report_output,
        threshold_name=args.threshold_name,
    )
    print("BAND EVENT SUMMARY")
    print(band_event_summary.to_string(index=False))
    print("\nFILTER EVENT SUMMARY")
    print(filter_event_summary.to_string(index=False))
    print("\nFILTER REPLAY SUMMARY")
    print(filter_replay_summary.to_string(index=False))


if __name__ == "__main__":
    main()
