from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from .price_volume_exit_state_study import (
    build_selected_forward_observations,
    fit_in_sample_selected_rows,
    replay_fixed_tp_sl_policy,
    replay_hybrid_exit_policy,
    summarize_policy_replays,
)
from .price_volume_regime_validation import build_candidate_feature_rows
from .up5in10_path_score_study import (
    apply_path_score,
    build_event_path_feature_rows,
    build_threshold_summary,
    derive_score_thresholds,
    fit_path_score_spec,
)
from .up5in10_price_volume_study import _markdown_table

DEFAULT_TRAIN_YEARS: tuple[int, ...] = (2022, 2023, 2024, 2025)
DEFAULT_THRESHOLD_NAMES: tuple[str, ...] = ("p80", "p90")


def _threshold_map_by_scope_year(
    threshold_rows: pd.DataFrame,
    *,
    scope: str,
) -> dict[str, dict[str, float]]:
    scoped = threshold_rows.loc[threshold_rows["scope"] == scope].copy()
    mapping: dict[str, dict[str, float]] = {}
    for year_value, frame in scoped.groupby("test_year", sort=False):
        mapping[str(year_value)] = {
            str(row["threshold_name"]): float(row["score_threshold"])
            for row in frame.to_dict(orient="records")
        }
    return mapping


def apply_thresholds_to_selected_rows(
    selected_rows: pd.DataFrame,
    *,
    threshold_rows: pd.DataFrame,
    threshold_names: tuple[str, ...],
    scope: str,
) -> pd.DataFrame:
    if selected_rows.empty:
        return pd.DataFrame(columns=[*selected_rows.columns.tolist(), "threshold_name", "score_threshold"])

    threshold_map = _threshold_map_by_scope_year(threshold_rows, scope=scope)
    rows: list[pd.DataFrame] = []
    for year_value, year_rows in selected_rows.groupby("year", sort=False):
        thresholds_for_year = threshold_map.get(str(year_value))
        if not thresholds_for_year:
            continue
        for threshold_name in threshold_names:
            score_threshold = thresholds_for_year.get(str(threshold_name))
            if score_threshold is None:
                continue
            filtered = year_rows.loc[year_rows["path_score"] >= score_threshold].copy()
            if filtered.empty:
                continue
            filtered["threshold_name"] = str(threshold_name)
            filtered["score_threshold"] = float(score_threshold)
            rows.append(filtered)
    if not rows:
        return pd.DataFrame(columns=[*selected_rows.columns.tolist(), "threshold_name", "score_threshold"])
    return pd.concat(rows, ignore_index=True)


def summarize_filtered_selection(
    *,
    base_selected_rows: pd.DataFrame,
    filtered_selected_rows: pd.DataFrame,
    scope: str,
    split_name: str,
) -> pd.DataFrame:
    columns = [
        "split_name",
        "scope",
        "threshold_name",
        "base_selected_rows",
        "filtered_selected_rows",
        "coverage_vs_base",
        "base_signal_days",
        "filtered_signal_days",
        "base_avg_names_per_day",
        "filtered_avg_names_per_day",
        "base_success_rate",
        "filtered_success_rate",
        "success_rate_lift",
        "base_mean_close_ret30",
        "filtered_mean_close_ret30",
        "base_mean_max_ret30",
        "filtered_mean_max_ret30",
        "base_mean_min_ret30",
        "filtered_mean_min_ret30",
    ]
    if base_selected_rows.empty or filtered_selected_rows.empty:
        return pd.DataFrame(columns=columns)

    base_rows = int(len(base_selected_rows))
    base_signal_days = int(base_selected_rows["trade_date"].nunique())
    base_avg_names_per_day = float(base_rows / base_signal_days) if base_signal_days > 0 else float("nan")
    base_success_rate = float(base_selected_rows["success_label"].mean())
    base_mean_close_ret30 = float(base_selected_rows["close_ret30"].mean())
    base_mean_max_ret30 = float(base_selected_rows["max_ret30"].mean())
    base_mean_min_ret30 = float(base_selected_rows["min_ret30"].mean())

    rows: list[dict[str, Any]] = []
    for threshold_name, frame in filtered_selected_rows.groupby("threshold_name", sort=False):
        filtered_rows = int(len(frame))
        filtered_signal_days = int(frame["trade_date"].nunique())
        filtered_avg_names_per_day = (
            float(filtered_rows / filtered_signal_days) if filtered_signal_days > 0 else float("nan")
        )
        filtered_success_rate = float(frame["success_label"].mean())
        rows.append(
            {
                "split_name": split_name,
                "scope": scope,
                "threshold_name": str(threshold_name),
                "base_selected_rows": base_rows,
                "filtered_selected_rows": filtered_rows,
                "coverage_vs_base": float(filtered_rows / base_rows),
                "base_signal_days": base_signal_days,
                "filtered_signal_days": filtered_signal_days,
                "base_avg_names_per_day": base_avg_names_per_day,
                "filtered_avg_names_per_day": filtered_avg_names_per_day,
                "base_success_rate": base_success_rate,
                "filtered_success_rate": filtered_success_rate,
                "success_rate_lift": filtered_success_rate - base_success_rate,
                "base_mean_close_ret30": base_mean_close_ret30,
                "filtered_mean_close_ret30": float(frame["close_ret30"].mean()),
                "base_mean_max_ret30": base_mean_max_ret30,
                "filtered_mean_max_ret30": float(frame["max_ret30"].mean()),
                "base_mean_min_ret30": base_mean_min_ret30,
                "filtered_mean_min_ret30": float(frame["min_ret30"].mean()),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def _fit_threshold_rows_from_event_features(
    event_features: pd.DataFrame,
    *,
    scope: str,
    train_years: tuple[int, ...],
) -> pd.DataFrame:
    scoped = event_features.copy()
    if scope == "main_board":
        scoped = scoped.loc[scoped["event_board"] == "main_board"].copy()
    rows: list[pd.DataFrame] = []

    full_train = scoped.loc[scoped["event_year"].isin(train_years)].copy()
    full_spec = fit_path_score_spec(full_train)
    full_scored = apply_path_score(full_train, full_spec)
    full_thresholds = derive_score_thresholds(full_scored)
    rows.append(
        build_threshold_summary(
            full_scored,
            thresholds=full_thresholds,
            split_name="train_years_reference",
            scope=scope,
            test_year="_".join(str(year) for year in train_years),
        )
    )

    if len(train_years) > 1:
        for year in train_years:
            train_subset = scoped.loc[
                scoped["event_year"].isin([candidate_year for candidate_year in train_years if candidate_year != year])
            ].copy()
            test_subset = scoped.loc[scoped["event_year"] == year].copy()
            if train_subset.empty or test_subset.empty:
                continue
            spec = fit_path_score_spec(train_subset)
            train_scored = apply_path_score(train_subset, spec)
            test_scored = apply_path_score(test_subset, spec)
            thresholds = derive_score_thresholds(train_scored)
            rows.append(
                build_threshold_summary(
                    test_scored,
                    thresholds=thresholds,
                    split_name="leave_one_year_out",
                    scope=scope,
                    test_year=str(year),
                )
            )
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _replay_overlay_policies(
    observations: pd.DataFrame,
    *,
    threshold_name: str,
) -> pd.DataFrame:
    if observations.empty:
        return pd.DataFrame()
    replays = [
        replay_fixed_tp_sl_policy(
            observations,
            take_profit=0.15,
            stop_loss=0.10,
            max_hold_days=12,
            round_trip_cost_bps=24.0,
        ),
        replay_hybrid_exit_policy(
            observations,
            take_profit=0.15,
            stop_loss=0.10,
            max_hold_days=12,
            state_policy_name="strict",
            output_policy_name="hybrid_strict_fixed_15_10_12",
            round_trip_cost_bps=24.0,
        ),
    ]
    replay_rows = pd.concat(replays, ignore_index=True)
    replay_rows["threshold_name"] = threshold_name
    return replay_rows


def _summarize_overlay_replays(
    replay_rows: pd.DataFrame,
    *,
    scope: str,
) -> pd.DataFrame:
    columns = [
        "scope",
        "threshold_name",
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

    summary = summarize_policy_replays(replay_rows.drop(columns=["threshold_name"]))
    summary["threshold_name"] = replay_rows.groupby(
        ["policy_name", "year"],
        sort=False,
    )["threshold_name"].first().values
    summary["scope"] = scope
    return summary[
        [
            "scope",
            "threshold_name",
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
    ]


def write_overlay_report(
    *,
    report_markdown_path: Path,
    selection_summary: pd.DataFrame,
    replay_summary: pd.DataFrame,
    threshold_names: tuple[str, ...],
) -> None:
    selection_all = selection_summary.loc[selection_summary["scope"] == "main_board"].copy()
    replay_all = replay_summary.loc[replay_summary["scope"] == "main_board"].copy()
    hold12 = replay_all.loc[
        replay_all["policy_name"].isin(["fixed_tp0.15_sl0.10_hold12", "hybrid_strict_fixed_15_10_12"])
    ].copy()
    report_lines = [
        f"# Path Score Overlay Study - {date.today().isoformat()}",
        "",
        "## Object",
        "- Base stream: in-sample `Top 5 / day` selected rows from the price-volume selector.",
        f"- Overlay thresholds: `{', '.join(threshold_names)}` on the up5in10 path score.",
        "- Replay focus: the `hold12` fixed and hybrid exits, because they were the least bad portfolio frame in prior research.",
        "",
        "## Selection Quality",
        _markdown_table(selection_all),
        "",
        "## Replay Summary",
        _markdown_table(hold12),
    ]
    report_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    report_markdown_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")


def run_path_score_overlay_study(
    *,
    source_db_path: Path,
    selected_overlay_csv_path: Path,
    selection_summary_csv_path: Path,
    replay_summary_csv_path: Path,
    report_markdown_path: Path,
    entry_start: str = "20220101",
    entry_end: str = "20251231",
    query_start: str = "20210101",
    query_end: str = "20260428",
    train_years: tuple[int, ...] = DEFAULT_TRAIN_YEARS,
    threshold_names: tuple[str, ...] = DEFAULT_THRESHOLD_NAMES,
    top_n_per_day: int = 5,
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
        analysis_years=train_years,
        top_n_per_day=top_n_per_day,
    )
    event_features = build_event_path_feature_rows(
        source_db_path=source_db_path,
        entry_start=query_start,
        entry_end=query_end,
        query_start=query_start,
        query_end=query_end,
    )
    score_spec = fit_path_score_spec(
        event_features.loc[event_features["event_year"].isin(train_years)].copy()
    )
    scored_event_features = apply_path_score(event_features, score_spec)
    selected_with_score = selected_rows.merge(
        scored_event_features[
            [
                "security_id",
                "event_trade_date",
                "path_score",
                "mean_turnover10",
                "contract_flat_rate10",
                "expand_up_persist",
                "down_to_up",
            ]
        ],
        left_on=["security_id", "trade_date"],
        right_on=["security_id", "event_trade_date"],
        how="left",
    ).drop(columns=["event_trade_date"])

    threshold_rows = _fit_threshold_rows_from_event_features(
        scored_event_features,
        scope="main_board",
        train_years=train_years,
    )
    filtered_selected = apply_thresholds_to_selected_rows(
        selected_with_score,
        threshold_rows=threshold_rows.loc[threshold_rows["split_name"] == "leave_one_year_out"].copy(),
        threshold_names=threshold_names,
        scope="main_board",
    )

    selection_summary = summarize_filtered_selection(
        base_selected_rows=selected_with_score,
        filtered_selected_rows=filtered_selected,
        scope="main_board",
        split_name="leave_one_year_out",
    )

    replay_frames: list[pd.DataFrame] = []
    for threshold_name, frame in filtered_selected.groupby("threshold_name", sort=False):
        observations = build_selected_forward_observations(
            source_db_path=source_db_path,
            selected_rows=frame,
            query_start=query_start,
            query_end=query_end,
        )
        replay_frames.append(_replay_overlay_policies(observations, threshold_name=str(threshold_name)))
    replay_rows = pd.concat(replay_frames, ignore_index=True) if replay_frames else pd.DataFrame()
    replay_summary = _summarize_overlay_replays(replay_rows, scope="main_board")

    selected_overlay_csv_path.parent.mkdir(parents=True, exist_ok=True)
    selection_summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
    replay_summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
    filtered_selected.to_csv(selected_overlay_csv_path, index=False)
    selection_summary.to_csv(selection_summary_csv_path, index=False)
    replay_summary.to_csv(replay_summary_csv_path, index=False)
    write_overlay_report(
        report_markdown_path=report_markdown_path,
        selection_summary=selection_summary,
        replay_summary=replay_summary,
        threshold_names=threshold_names,
    )
    return {
        "selected_overlay": filtered_selected,
        "selection_summary": selection_summary,
        "replay_summary": replay_summary,
    }


def _parse_int_tuple(text: str) -> tuple[int, ...]:
    return tuple(int(value.strip()) for value in text.split(",") if value.strip())


def _parse_text_tuple(text: str) -> tuple[str, ...]:
    return tuple(value.strip() for value in text.split(",") if value.strip())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the path-score overlay study on the selected stream.")
    parser.add_argument("--source-db", required=True, type=Path)
    parser.add_argument("--selected-overlay-csv", required=True, type=Path)
    parser.add_argument("--selection-summary-csv", required=True, type=Path)
    parser.add_argument("--replay-summary-csv", required=True, type=Path)
    parser.add_argument("--report-markdown", required=True, type=Path)
    parser.add_argument("--entry-start", default="20220101")
    parser.add_argument("--entry-end", default="20251231")
    parser.add_argument("--query-start", default="20210101")
    parser.add_argument("--query-end", default="20260428")
    parser.add_argument("--train-years", default="2022,2023,2024,2025")
    parser.add_argument("--threshold-names", default="p80,p90")
    parser.add_argument("--top-n-per-day", default=5, type=int)
    args = parser.parse_args(argv)

    run_path_score_overlay_study(
        source_db_path=args.source_db,
        selected_overlay_csv_path=args.selected_overlay_csv,
        selection_summary_csv_path=args.selection_summary_csv,
        replay_summary_csv_path=args.replay_summary_csv,
        report_markdown_path=args.report_markdown,
        entry_start=str(args.entry_start),
        entry_end=str(args.entry_end),
        query_start=str(args.query_start),
        query_end=str(args.query_end),
        train_years=_parse_int_tuple(str(args.train_years)),
        threshold_names=_parse_text_tuple(str(args.threshold_names)),
        top_n_per_day=int(args.top_n_per_day),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
