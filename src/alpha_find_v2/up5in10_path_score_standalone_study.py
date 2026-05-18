from __future__ import annotations

import argparse
import gc
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from .price_volume_exit_state_study import (
    build_selected_forward_observations,
    replay_fixed_tp_sl_policy,
)
from .price_volume_regime_validation import build_candidate_feature_rows
from .up5in10_path_score_study import apply_path_score, fit_path_score_spec
from .up5in10_price_volume_study import _markdown_table

DEFAULT_TRAIN_YEARS: tuple[int, ...] = (2022, 2023, 2024, 2025)
DEFAULT_EXTRA_TEST_YEARS: tuple[int, ...] = (2021, 2026)
DEFAULT_THRESHOLD_NAMES: tuple[str, ...] = ("p80", "p90")
DEFAULT_TOP_NS: tuple[int, ...] = (1, 3, 5)
DEFAULT_POLICY_SPECS: tuple[dict[str, float | int | str], ...] = (
    {"policy_name": "fixed_tp0.05_sl0.10_hold10", "take_profit": 0.05, "stop_loss": 0.10, "max_hold_days": 10},
    {"policy_name": "fixed_tp0.08_sl0.10_hold10", "take_profit": 0.08, "stop_loss": 0.10, "max_hold_days": 10},
    {"policy_name": "hold10_close_proxy", "take_profit": 0.99, "stop_loss": 0.99, "max_hold_days": 10},
)


def score_main_board_rows(
    path_features: pd.DataFrame,
    *,
    train_years: tuple[int, ...] = DEFAULT_TRAIN_YEARS,
    extra_test_years: tuple[int, ...] = DEFAULT_EXTRA_TEST_YEARS,
) -> pd.DataFrame:
    scoped = path_features.loc[path_features["event_board"] == "main_board"].copy()
    if scoped.empty:
        return pd.DataFrame()

    train_full = scoped.loc[scoped["event_year"].isin(train_years)].copy()
    if train_full.empty:
        return pd.DataFrame()

    spec_full = fit_path_score_spec(train_full)
    train_full_scored = apply_path_score(train_full, spec_full)
    full_thresholds = {
        "p80": float(train_full_scored["path_score"].quantile(0.80)),
        "p90": float(train_full_scored["path_score"].quantile(0.90)),
    }

    scored_parts: list[pd.DataFrame] = []
    for year in train_years:
        train = scoped.loc[
            scoped["event_year"].isin([candidate_year for candidate_year in train_years if candidate_year != year])
        ].copy()
        test = scoped.loc[scoped["event_year"] == year].copy()
        if train.empty or test.empty:
            continue
        spec = fit_path_score_spec(train)
        scored = apply_path_score(test, spec)
        train_scored = apply_path_score(train, spec)
        scored["score_split"] = "leave_one_year_out"
        scored["th_p80"] = float(train_scored["path_score"].quantile(0.80))
        scored["th_p90"] = float(train_scored["path_score"].quantile(0.90))
        scored_parts.append(scored)

    for year in extra_test_years:
        test = scoped.loc[scoped["event_year"] == year].copy()
        if test.empty:
            continue
        scored = apply_path_score(test, spec_full)
        scored["score_split"] = "train_2022_2025_apply"
        scored["th_p80"] = full_thresholds["p80"]
        scored["th_p90"] = full_thresholds["p90"]
        scored_parts.append(scored)

    if not scored_parts:
        return pd.DataFrame()
    return pd.concat(scored_parts, ignore_index=True)


def merge_scored_candidates(
    candidate_rows: pd.DataFrame,
    scored_features: pd.DataFrame,
) -> pd.DataFrame:
    if candidate_rows.empty or scored_features.empty:
        return pd.DataFrame()
    left = candidate_rows.copy()
    right = scored_features.copy()
    left["trade_date"] = left["trade_date"].astype(str)
    right["event_trade_date"] = right["event_trade_date"].astype(str)
    return (
        left.merge(
            right[
                ["security_id", "event_trade_date", "event_year", "path_score", "score_split", "th_p80", "th_p90"]
            ],
            left_on=["security_id", "trade_date", "year"],
            right_on=["security_id", "event_trade_date", "event_year"],
            how="inner",
        )
        .drop(columns=["event_trade_date", "event_year"])
        .sort_values(["trade_date", "path_score", "security_id"], ascending=[True, False, True])
        .reset_index(drop=True)
    )


def select_standalone_rows(
    scored_candidates: pd.DataFrame,
    *,
    threshold_names: tuple[str, ...] = DEFAULT_THRESHOLD_NAMES,
    top_ns: tuple[int, ...] = DEFAULT_TOP_NS,
) -> pd.DataFrame:
    if scored_candidates.empty:
        return pd.DataFrame()

    selected_parts: list[pd.DataFrame] = []
    for threshold_name in threshold_names:
        threshold_column = f"th_{threshold_name}"
        filtered = scored_candidates.loc[
            scored_candidates["path_score"] >= scored_candidates[threshold_column]
        ].copy()
        if filtered.empty:
            continue
        for top_n in top_ns:
            picked = (
                filtered.sort_values(["trade_date", "path_score", "security_id"], ascending=[True, False, True])
                .groupby("trade_date", group_keys=False)
                .head(top_n)
                .copy()
            )
            if picked.empty:
                continue
            picked["threshold_name"] = threshold_name
            picked["score_threshold"] = picked[threshold_column].astype(float)
            picked["top_n"] = int(top_n)
            selected_parts.append(picked)
    if not selected_parts:
        return pd.DataFrame()
    return pd.concat(selected_parts, ignore_index=True)


def summarize_selected_rows(selected_rows: pd.DataFrame) -> pd.DataFrame:
    if selected_rows.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for (score_split, threshold_name, top_n), frame in selected_rows.groupby(
        ["score_split", "threshold_name", "top_n"],
        sort=False,
    ):
        signal_days = int(frame["trade_date"].nunique())
        row: dict[str, Any] = {
            "score_split": str(score_split),
            "threshold_name": str(threshold_name),
            "top_n": int(top_n),
            "selected_rows": int(len(frame)),
            "signal_days": signal_days,
            "avg_names_per_day": float(len(frame) / signal_days) if signal_days > 0 else float("nan"),
            "success_rate": float(frame["success_label"].mean()),
            "mean_close_ret30": float(frame["close_ret30"].mean()),
            "mean_max_ret30": float(frame["max_ret30"].mean()),
            "mean_min_ret30": float(frame["min_ret30"].mean()),
        }
        for year, year_frame in frame.groupby("year", sort=True):
            row[f"success_rate_{year}"] = float(year_frame["success_label"].mean())
            row[f"close_ret30_{year}"] = float(year_frame["close_ret30"].mean())
            row[f"max_ret30_{year}"] = float(year_frame["max_ret30"].mean())
            row[f"min_ret30_{year}"] = float(year_frame["min_ret30"].mean())
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["score_split", "threshold_name", "top_n"]).reset_index(drop=True)


def attach_selection_context_to_observations(
    observations: pd.DataFrame,
    *,
    selected_rows: pd.DataFrame,
) -> pd.DataFrame:
    if observations.empty or selected_rows.empty:
        return pd.DataFrame()

    selection_context = (
        selected_rows[["security_id", "trade_date", "score_split", "threshold_name", "top_n"]]
        .drop_duplicates()
        .rename(columns={"trade_date": "entry_trade_date"})
    )
    return observations.merge(
        selection_context,
        on=["security_id", "entry_trade_date"],
        how="inner",
    )


def replay_standalone_policies(
    context_observations: pd.DataFrame,
    *,
    round_trip_cost_bps: float = 24.0,
) -> pd.DataFrame:
    if context_observations.empty:
        return pd.DataFrame()

    replay_parts: list[pd.DataFrame] = []
    for (score_split, threshold_name, top_n), frame in context_observations.groupby(
        ["score_split", "threshold_name", "top_n"],
        sort=False,
    ):
        for policy_spec in DEFAULT_POLICY_SPECS:
            replay = replay_fixed_tp_sl_policy(
                frame.drop(columns=["score_split", "threshold_name", "top_n"]),
                take_profit=float(policy_spec["take_profit"]),
                stop_loss=float(policy_spec["stop_loss"]),
                max_hold_days=int(policy_spec["max_hold_days"]),
                round_trip_cost_bps=round_trip_cost_bps,
            )
            replay["policy_name"] = str(policy_spec["policy_name"])
            replay["score_split"] = str(score_split)
            replay["threshold_name"] = str(threshold_name)
            replay["top_n"] = int(top_n)
            replay_parts.append(replay)
    return pd.concat(replay_parts, ignore_index=True) if replay_parts else pd.DataFrame()


def summarize_replay_rows_by_context(replay_rows: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "score_split",
        "threshold_name",
        "top_n",
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

    return (
        replay_rows.groupby(
            ["score_split", "threshold_name", "top_n", "policy_name", "year"],
            as_index=False,
        )
        .agg(
            trades=("gross_ret", "size"),
            mean_gross_ret=("gross_ret", "mean"),
            mean_net_ret=("net_ret", "mean"),
            median_net_ret=("net_ret", "median"),
            win_rate_pos=("net_ret", lambda series: float((series > 0.0).mean())),
            non_loss_rate=("net_ret", lambda series: float((series >= 0.0).mean())),
            avg_exit_step=("exit_step", "mean"),
        )
        .sort_values(["score_split", "threshold_name", "top_n", "policy_name", "year"])
        .reset_index(drop=True)[columns]
    )


def summarize_replay_overall(replay_rows: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "score_split",
        "threshold_name",
        "top_n",
        "policy_name",
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

    return (
        replay_rows.groupby(
            ["score_split", "threshold_name", "top_n", "policy_name"],
            as_index=False,
        )
        .agg(
            trades=("gross_ret", "size"),
            mean_gross_ret=("gross_ret", "mean"),
            mean_net_ret=("net_ret", "mean"),
            median_net_ret=("net_ret", "median"),
            win_rate_pos=("net_ret", lambda series: float((series > 0.0).mean())),
            non_loss_rate=("net_ret", lambda series: float((series >= 0.0).mean())),
            avg_exit_step=("exit_step", "mean"),
        )
        .sort_values(["score_split", "threshold_name", "top_n", "policy_name"])
        .reset_index(drop=True)[columns]
    )


def write_standalone_report(
    *,
    report_markdown_path: Path,
    event_summary: pd.DataFrame,
    replay_overall: pd.DataFrame,
    replay_summary: pd.DataFrame,
) -> None:
    lines = [
        "# up5in10 路径分数独立选股研究",
        "",
        "这份报告把 `up5in10` 路径分数当成独立短周期选股器使用，不叠加现有 `Top 5 / day` 策略流。",
        "",
        "## 事件层总览",
        "",
        _markdown_table(
            event_summary[
                [
                    "score_split",
                    "threshold_name",
                    "top_n",
                    "selected_rows",
                    "signal_days",
                    "avg_names_per_day",
                    "success_rate",
                    "mean_close_ret30",
                    "mean_max_ret30",
                    "mean_min_ret30",
                ]
            ]
        )
        if not event_summary.empty
        else "无事件汇总。",
        "",
        "## 交易回放总览",
        "",
        _markdown_table(replay_overall) if not replay_overall.empty else "无回放总览。",
        "",
        "## 分年交易回放",
        "",
        _markdown_table(replay_summary) if not replay_summary.empty else "无分年回放。",
        "",
    ]
    report_markdown_path.write_text("\n".join(lines), encoding="utf-8")


def run_standalone_study(
    *,
    source_db_path: Path,
    path_feature_csv_path: Path,
    selected_output_path: Path,
    event_summary_output_path: Path,
    replay_output_path: Path,
    replay_summary_output_path: Path,
    replay_overall_output_path: Path,
    report_markdown_path: Path,
    entry_start: str,
    entry_end: str,
    query_start: str,
    query_end: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    candidate_rows = build_candidate_feature_rows(
        source_db_path=source_db_path,
        entry_start=entry_start,
        entry_end=entry_end,
        query_start=query_start,
        query_end=query_end,
    )
    path_features = pd.read_csv(
        path_feature_csv_path,
        usecols=[
            "security_id",
            "event_trade_date",
            "event_year",
            "event_board",
            "mean_turnover10",
            "contract_flat_rate10",
            "expand_up_persist",
            "down_to_up",
        ],
    )
    scored_features = score_main_board_rows(path_features)
    scored_candidates = merge_scored_candidates(candidate_rows, scored_features)
    selected_rows = select_standalone_rows(scored_candidates)
    event_summary = summarize_selected_rows(selected_rows)

    selected_rows.to_csv(selected_output_path, index=False)
    event_summary.to_csv(event_summary_output_path, index=False)
    del candidate_rows, path_features, scored_features, scored_candidates
    gc.collect()

    unique_entries = selected_rows[
        ["security_id", "trade_date", "year", "entry_close_adj"]
    ].drop_duplicates().reset_index(drop=True)
    base_observations = build_selected_forward_observations(
        source_db_path=source_db_path,
        selected_rows=unique_entries,
        query_start=query_start,
        query_end=query_end,
        forward_days=10,
    )
    context_observations = attach_selection_context_to_observations(
        base_observations,
        selected_rows=selected_rows,
    )
    replay_rows = replay_standalone_policies(context_observations)
    replay_summary = summarize_replay_rows_by_context(replay_rows)
    replay_overall = summarize_replay_overall(replay_rows)

    replay_rows.to_csv(replay_output_path, index=False)
    replay_summary.to_csv(replay_summary_output_path, index=False)
    replay_overall.to_csv(replay_overall_output_path, index=False)
    write_standalone_report(
        report_markdown_path=report_markdown_path,
        event_summary=event_summary,
        replay_overall=replay_overall,
        replay_summary=replay_summary,
    )
    return selected_rows, event_summary, replay_summary, replay_overall


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the standalone up5in10 path-score study.")
    parser.add_argument("--source-db", type=Path, default=Path("output/research_source.duckdb"))
    parser.add_argument("--path-features-csv", type=Path, default=Path(".tmp/up5in10_path_score_event_features.csv"))
    parser.add_argument("--selected-output", type=Path, default=Path(".tmp/up5in10_standalone_selected_rows.csv"))
    parser.add_argument("--event-summary-output", type=Path, default=Path(".tmp/up5in10_standalone_event_summary.csv"))
    parser.add_argument("--replay-output", type=Path, default=Path(".tmp/up5in10_standalone_replays.csv"))
    parser.add_argument("--replay-summary-output", type=Path, default=Path(".tmp/up5in10_standalone_replay_summary.csv"))
    parser.add_argument("--replay-overall-output", type=Path, default=Path(".tmp/up5in10_standalone_replay_overall.csv"))
    parser.add_argument("--report-output", type=Path, default=Path(".tmp/up5in10_standalone_report.md"))
    parser.add_argument("--entry-start", default="20210101")
    parser.add_argument("--entry-end", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--query-start", default="20200101")
    parser.add_argument("--query-end", default=date.today().strftime("%Y%m%d"))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    _, event_summary, replay_summary, replay_overall = run_standalone_study(
        source_db_path=args.source_db,
        path_feature_csv_path=args.path_features_csv,
        selected_output_path=args.selected_output,
        event_summary_output_path=args.event_summary_output,
        replay_output_path=args.replay_output,
        replay_summary_output_path=args.replay_summary_output,
        replay_overall_output_path=args.replay_overall_output,
        report_markdown_path=args.report_output,
        entry_start=args.entry_start,
        entry_end=args.entry_end,
        query_start=args.query_start,
        query_end=args.query_end,
    )
    print("EVENT SUMMARY")
    print(event_summary.to_string(index=False))
    print("\nREPLAY OVERALL")
    print(replay_overall.to_string(index=False))
    print("\nREPLAY SUMMARY")
    print(replay_summary.to_string(index=False))


if __name__ == "__main__":
    main()
