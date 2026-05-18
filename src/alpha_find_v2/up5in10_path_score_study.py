from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from .up5in10_price_volume_study import _markdown_table, _prepare_study_tables

PATH_SCORE_FEATURES = [
    "mean_turnover10",
    "contract_flat_rate10",
    "expand_up_persist",
    "down_to_up",
]

PATH_SCORE_WEIGHTS: dict[str, float] = {
    "mean_turnover10": 1.0,
    "contract_flat_rate10": -1.0,
    "expand_up_persist": 1.0,
    "down_to_up": 0.5,
}

DEFAULT_THRESHOLD_QUANTILES: tuple[float, ...] = (0.50, 0.60, 0.70, 0.80, 0.90, 0.95)
DEFAULT_QUINTILE_QUANTILES: tuple[float, ...] = (0.20, 0.40, 0.60, 0.80)
DEFAULT_TRAIN_YEARS: tuple[int, ...] = (2022, 2023, 2024, 2025)
DEFAULT_EXTRA_TEST_YEARS: tuple[int, ...] = (2021, 2026)


def build_event_path_feature_rows(
    *,
    source_db_path: Path,
    entry_start: str,
    entry_end: str,
    query_start: str,
    query_end: str,
) -> pd.DataFrame:
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        _prepare_study_tables(
            conn=conn,
            entry_start=entry_start,
            entry_end=entry_end,
            query_start=query_start,
            query_end=query_end,
        )
        frame = conn.execute(
            """
            WITH enriched AS (
                SELECT
                    security_id,
                    event_trade_date,
                    event_year,
                    event_board,
                    hit5,
                    relative_day,
                    daily_ret1,
                    turnover_ratio,
                    pv_state,
                    lag(pv_state, 1) OVER (
                        PARTITION BY security_id, event_trade_date
                        ORDER BY relative_day
                    ) AS prev_state
                FROM candidate_lookback
            )
            SELECT
                security_id,
                event_trade_date,
                event_year,
                event_board,
                hit5,
                avg(CASE WHEN relative_day >= -10 THEN turnover_ratio END) AS mean_turnover10,
                avg(CASE WHEN relative_day >= -10 THEN (pv_state = 'contract_flat')::INT END) AS contract_flat_rate10,
                avg(CASE WHEN relative_day >= -10 THEN (pv_state = 'expand_up')::INT END) AS expand_up_rate10,
                corr(
                    CASE WHEN relative_day >= -10 THEN daily_ret1 END,
                    CASE WHEN relative_day >= -10 THEN turnover_ratio END
                ) AS ret_turnover_corr10,
                sum((pv_state = 'expand_up' AND prev_state = 'expand_up')::INT) AS expand_up_persist,
                sum((pv_state = 'expand_up' AND prev_state = 'expand_down')::INT) AS down_to_up,
                sum((pv_state = 'contract_flat' AND prev_state = 'contract_flat')::INT) AS flat_sleep
            FROM enriched
            GROUP BY 1, 2, 3, 4, 5
            ORDER BY event_trade_date, security_id
            """
        ).fetchdf()
    finally:
        conn.close()
    return frame


def fit_path_score_spec(
    event_rows: pd.DataFrame,
    *,
    lower_quantile: float = 0.01,
    upper_quantile: float = 0.99,
) -> list[dict[str, float | str]]:
    if event_rows.empty:
        return []

    rows: list[dict[str, float | str]] = []
    for feature_name in PATH_SCORE_FEATURES:
        series = event_rows[feature_name].astype(float)
        clip_low = float(series.quantile(lower_quantile))
        clip_high = float(series.quantile(upper_quantile))
        clipped = series.clip(lower=clip_low, upper=clip_high)
        mean_value = float(clipped.mean())
        std_value = float(clipped.std(ddof=0))
        if not np.isfinite(std_value) or std_value == 0.0:
            std_value = 1.0
        rows.append(
            {
                "feature": feature_name,
                "clip_low": clip_low,
                "clip_high": clip_high,
                "mean": mean_value,
                "std": std_value,
                "weight": float(PATH_SCORE_WEIGHTS[feature_name]),
            }
        )
    return rows


def apply_path_score(
    event_rows: pd.DataFrame,
    score_spec: list[dict[str, float | str]] | pd.DataFrame,
) -> pd.DataFrame:
    if event_rows.empty:
        return event_rows.copy()

    spec_rows: list[dict[str, float | str]]
    if isinstance(score_spec, pd.DataFrame):
        spec_rows = score_spec.to_dict(orient="records")
    else:
        spec_rows = list(score_spec)

    scored = event_rows.copy()
    path_score = np.zeros(len(scored), dtype=float)
    for spec_row in spec_rows:
        feature_name = str(spec_row["feature"])
        clip_low = float(spec_row["clip_low"])
        clip_high = float(spec_row["clip_high"])
        mean_value = float(spec_row["mean"])
        std_value = float(spec_row["std"])
        weight = float(spec_row["weight"])
        clipped = scored[feature_name].astype(float).clip(lower=clip_low, upper=clip_high)
        zscore = (clipped - mean_value) / std_value
        scored[f"{feature_name}_z"] = zscore
        path_score += weight * zscore.to_numpy(dtype=float)
    scored["path_score"] = path_score
    return scored


def derive_score_thresholds(
    scored_rows: pd.DataFrame,
    *,
    threshold_quantiles: tuple[float, ...] = DEFAULT_THRESHOLD_QUANTILES,
) -> list[dict[str, float | str]]:
    if scored_rows.empty:
        return []
    rows = [{"threshold_name": "all", "score_threshold": float("-inf")}]
    for quantile in threshold_quantiles:
        rows.append(
            {
                "threshold_name": f"p{int(round(quantile * 100)):02d}",
                "score_threshold": float(scored_rows["path_score"].quantile(quantile)),
            }
        )
    return rows


def build_threshold_summary(
    scored_rows: pd.DataFrame,
    *,
    thresholds: list[dict[str, float | str]],
    split_name: str,
    scope: str,
    test_year: str,
) -> pd.DataFrame:
    columns = [
        "split_name",
        "scope",
        "test_year",
        "threshold_name",
        "score_threshold",
        "candidate_events",
        "selected_events",
        "coverage",
        "base_rate",
        "success_rate",
        "lift_vs_base",
    ]
    if scored_rows.empty:
        return pd.DataFrame(columns=columns)

    total = int(len(scored_rows))
    base_rate = float(scored_rows["hit5"].mean())
    rows: list[dict[str, Any]] = []
    for threshold_row in thresholds:
        score_threshold = float(threshold_row["score_threshold"])
        selected = scored_rows.loc[scored_rows["path_score"] >= score_threshold].copy()
        selected_events = int(len(selected))
        coverage = float(selected_events / total)
        success_rate = float(selected["hit5"].mean()) if selected_events > 0 else float("nan")
        rows.append(
            {
                "split_name": split_name,
                "scope": scope,
                "test_year": test_year,
                "threshold_name": str(threshold_row["threshold_name"]),
                "score_threshold": score_threshold,
                "candidate_events": total,
                "selected_events": selected_events,
                "coverage": coverage,
                "base_rate": base_rate,
                "success_rate": success_rate,
                "lift_vs_base": success_rate - base_rate if selected_events > 0 else float("nan"),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def derive_quintile_edges(
    scored_rows: pd.DataFrame,
    *,
    quantiles: tuple[float, ...] = DEFAULT_QUINTILE_QUANTILES,
) -> list[float]:
    if scored_rows.empty:
        return [float("-inf"), float("inf")]
    return [
        float("-inf"),
        *(float(scored_rows["path_score"].quantile(quantile)) for quantile in quantiles),
        float("inf"),
    ]


def build_quintile_summary(
    scored_rows: pd.DataFrame,
    *,
    score_edges: list[float],
    split_name: str,
    scope: str,
    test_year: str,
) -> pd.DataFrame:
    columns = [
        "split_name",
        "scope",
        "test_year",
        "bin",
        "n",
        "success_rate",
        "base_rate",
    ]
    if scored_rows.empty:
        return pd.DataFrame(columns=columns)

    working = scored_rows.copy()
    working["bin"] = pd.cut(
        working["path_score"],
        bins=score_edges,
        labels=[0, 1, 2, 3, 4],
        include_lowest=True,
        right=True,
    )
    grouped = (
        working.groupby("bin", as_index=False, observed=False)
        .agg(
            n=("hit5", "size"),
            success_rate=("hit5", "mean"),
        )
        .sort_values("bin")
        .reset_index(drop=True)
    )
    grouped["bin"] = grouped["bin"].astype(int)
    grouped["split_name"] = split_name
    grouped["scope"] = scope
    grouped["test_year"] = test_year
    grouped["base_rate"] = float(scored_rows["hit5"].mean())
    return grouped[columns]


def summarize_quintile_performance(quintile_summary: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "split_name",
        "scope",
        "test_year",
        "base_rate",
        "top_bin_rate",
        "bottom_bin_rate",
        "top_minus_bottom",
        "top_minus_base",
        "bottom_minus_base",
        "top_coverage",
    ]
    if quintile_summary.empty:
        return pd.DataFrame(columns=columns)

    grouped_rows: list[dict[str, Any]] = []
    for (split_name, scope, test_year), frame in quintile_summary.groupby(
        ["split_name", "scope", "test_year"],
        sort=False,
    ):
        ordered = frame.sort_values("bin").reset_index(drop=True)
        top = ordered.iloc[-1]
        bottom = ordered.iloc[0]
        total = int(ordered["n"].sum())
        grouped_rows.append(
            {
                "split_name": str(split_name),
                "scope": str(scope),
                "test_year": str(test_year),
                "base_rate": float(ordered["base_rate"].iloc[0]),
                "top_bin_rate": float(top["success_rate"]),
                "bottom_bin_rate": float(bottom["success_rate"]),
                "top_minus_bottom": float(top["success_rate"] - bottom["success_rate"]),
                "top_minus_base": float(top["success_rate"] - ordered["base_rate"].iloc[0]),
                "bottom_minus_base": float(bottom["success_rate"] - ordered["base_rate"].iloc[0]),
                "top_coverage": float(int(top["n"]) / total) if total > 0 else float("nan"),
            }
        )
    return pd.DataFrame(grouped_rows, columns=columns)


def _average_threshold_tradeoff(
    threshold_summary: pd.DataFrame,
    *,
    scope: str,
    split_name: str,
) -> pd.DataFrame:
    working = threshold_summary.loc[
        (threshold_summary["scope"] == scope)
        & (threshold_summary["split_name"] == split_name)
        & (threshold_summary["threshold_name"] != "all")
    ].copy()
    if working.empty:
        return pd.DataFrame()
    return (
        working.groupby("threshold_name", as_index=False)
        .agg(
            mean_threshold=("score_threshold", "mean"),
            mean_coverage=("coverage", "mean"),
            mean_success_rate=("success_rate", "mean"),
            mean_lift_vs_base=("lift_vs_base", "mean"),
        )
        .sort_values("mean_threshold")
        .reset_index(drop=True)
    )


def write_path_score_report(
    *,
    report_markdown_path: Path,
    event_features: pd.DataFrame,
    threshold_summary: pd.DataFrame,
    quintile_summary: pd.DataFrame,
    oos_summary: pd.DataFrame,
    train_years: tuple[int, ...],
    extra_test_years: tuple[int, ...],
) -> None:
    scope = "all"
    in_sample_quintiles = quintile_summary.loc[
        (quintile_summary["scope"] == scope)
        & (quintile_summary["split_name"] == "in_sample_train_years")
    ].copy()
    loo_summary = oos_summary.loc[
        (oos_summary["scope"] == scope)
        & (oos_summary["split_name"] == "leave_one_year_out")
    ].copy()
    extra_summary = oos_summary.loc[
        (oos_summary["scope"] == scope)
        & (oos_summary["split_name"] == "train_years_apply")
    ].copy()
    loo_threshold_tradeoff = _average_threshold_tradeoff(
        threshold_summary,
        scope=scope,
        split_name="leave_one_year_out",
    )
    apply_threshold_tradeoff = _average_threshold_tradeoff(
        threshold_summary,
        scope=scope,
        split_name="train_years_apply",
    )

    report_lines = [
        f"# Path Score Threshold Study - {date.today().isoformat()}",
        "",
        "## Object",
        f"- Train years: `{', '.join(str(year) for year in train_years)}`",
        f"- Extra test years: `{', '.join(str(year) for year in extra_test_years)}`",
        "- Path score formula: `z(mean_turnover10) - z(contract_flat_rate10) + z(expand_up_persist) + 0.5 * z(down_to_up)`.",
        f"- Event rows: `{len(event_features):,}`",
        "",
        "## In-Sample Quintiles",
        _markdown_table(in_sample_quintiles),
        "",
        "## Leave-One-Year-Out Quintile Summary",
        _markdown_table(loo_summary),
        "",
        "## Leave-One-Year-Out Threshold Tradeoff",
        _markdown_table(loo_threshold_tradeoff),
        "",
        "## Train-Years-Apply Quintile Summary",
        _markdown_table(extra_summary),
        "",
        "## Train-Years-Apply Threshold Tradeoff",
        _markdown_table(apply_threshold_tradeoff),
    ]
    report_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    report_markdown_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")


def run_up5in10_path_score_study(
    *,
    source_db_path: Path,
    event_feature_csv_path: Path,
    threshold_summary_csv_path: Path,
    quintile_summary_csv_path: Path,
    oos_summary_csv_path: Path,
    report_markdown_path: Path,
    entry_start: str = "20210101",
    entry_end: str = "20260428",
    query_start: str = "20200101",
    query_end: str = "20260428",
    train_years: tuple[int, ...] = DEFAULT_TRAIN_YEARS,
    extra_test_years: tuple[int, ...] = DEFAULT_EXTRA_TEST_YEARS,
    threshold_quantiles: tuple[float, ...] = DEFAULT_THRESHOLD_QUANTILES,
    quintile_quantiles: tuple[float, ...] = DEFAULT_QUINTILE_QUANTILES,
) -> dict[str, pd.DataFrame]:
    event_features = build_event_path_feature_rows(
        source_db_path=source_db_path,
        entry_start=entry_start,
        entry_end=entry_end,
        query_start=query_start,
        query_end=query_end,
    )

    threshold_frames: list[pd.DataFrame] = []
    quintile_frames: list[pd.DataFrame] = []
    oos_frames: list[pd.DataFrame] = []

    for scope_name, scope_mask in [
        ("all", event_features["event_board"].notna()),
        ("main_board", event_features["event_board"].eq("main_board")),
    ]:
        scoped = event_features.loc[scope_mask].copy().reset_index(drop=True)
        if scoped.empty:
            continue

        train_full = scoped.loc[scoped["event_year"].isin(train_years)].copy().reset_index(drop=True)
        if train_full.empty:
            continue
        spec_full = fit_path_score_spec(train_full)
        train_full_scored = apply_path_score(train_full, spec_full)
        full_thresholds = derive_score_thresholds(
            train_full_scored,
            threshold_quantiles=threshold_quantiles,
        )
        full_edges = derive_quintile_edges(
            train_full_scored,
            quantiles=quintile_quantiles,
        )

        in_sample_year_label = "_".join(str(year) for year in train_years)
        threshold_frames.append(
            build_threshold_summary(
                train_full_scored,
                thresholds=full_thresholds,
                split_name="in_sample_train_years",
                scope=scope_name,
                test_year=in_sample_year_label,
            )
        )
        in_sample_quintiles = build_quintile_summary(
            train_full_scored,
            score_edges=full_edges,
            split_name="in_sample_train_years",
            scope=scope_name,
            test_year=in_sample_year_label,
        )
        quintile_frames.append(in_sample_quintiles)
        oos_frames.append(summarize_quintile_performance(in_sample_quintiles))

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
                thresholds = derive_score_thresholds(
                    train_scored,
                    threshold_quantiles=threshold_quantiles,
                )
                edges = derive_quintile_edges(
                    train_scored,
                    quantiles=quintile_quantiles,
                )
                threshold_frames.append(
                    build_threshold_summary(
                        test_scored,
                        thresholds=thresholds,
                        split_name="leave_one_year_out",
                        scope=scope_name,
                        test_year=str(year),
                    )
                )
                quintile_frame = build_quintile_summary(
                    test_scored,
                    score_edges=edges,
                    split_name="leave_one_year_out",
                    scope=scope_name,
                    test_year=str(year),
                )
                quintile_frames.append(quintile_frame)
                oos_frames.append(summarize_quintile_performance(quintile_frame))

        for year in extra_test_years:
            test_subset = scoped.loc[scoped["event_year"] == year].copy()
            if test_subset.empty:
                continue
            test_scored = apply_path_score(test_subset, spec_full)
            threshold_frames.append(
                build_threshold_summary(
                    test_scored,
                    thresholds=full_thresholds,
                    split_name="train_years_apply",
                    scope=scope_name,
                    test_year=str(year),
                )
            )
            quintile_frame = build_quintile_summary(
                test_scored,
                score_edges=full_edges,
                split_name="train_years_apply",
                scope=scope_name,
                test_year=str(year),
            )
            quintile_frames.append(quintile_frame)
            oos_frames.append(summarize_quintile_performance(quintile_frame))

    threshold_summary = pd.concat(threshold_frames, ignore_index=True) if threshold_frames else pd.DataFrame()
    quintile_summary = pd.concat(quintile_frames, ignore_index=True) if quintile_frames else pd.DataFrame()
    oos_summary = pd.concat(oos_frames, ignore_index=True) if oos_frames else pd.DataFrame()

    event_feature_csv_path.parent.mkdir(parents=True, exist_ok=True)
    threshold_summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
    quintile_summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
    oos_summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
    event_features.to_csv(event_feature_csv_path, index=False)
    threshold_summary.to_csv(threshold_summary_csv_path, index=False)
    quintile_summary.to_csv(quintile_summary_csv_path, index=False)
    oos_summary.to_csv(oos_summary_csv_path, index=False)
    write_path_score_report(
        report_markdown_path=report_markdown_path,
        event_features=event_features,
        threshold_summary=threshold_summary,
        quintile_summary=quintile_summary,
        oos_summary=oos_summary,
        train_years=train_years,
        extra_test_years=extra_test_years,
    )
    return {
        "event_features": event_features,
        "threshold_summary": threshold_summary,
        "quintile_summary": quintile_summary,
        "oos_summary": oos_summary,
    }


def _parse_int_tuple(text: str) -> tuple[int, ...]:
    values = [value.strip() for value in text.split(",") if value.strip()]
    return tuple(int(value) for value in values)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the up5in10 path-score threshold study.")
    parser.add_argument("--source-db", required=True, type=Path)
    parser.add_argument("--event-feature-csv", required=True, type=Path)
    parser.add_argument("--threshold-summary-csv", required=True, type=Path)
    parser.add_argument("--quintile-summary-csv", required=True, type=Path)
    parser.add_argument("--oos-summary-csv", required=True, type=Path)
    parser.add_argument("--report-markdown", required=True, type=Path)
    parser.add_argument("--entry-start", default="20210101")
    parser.add_argument("--entry-end", default="20260428")
    parser.add_argument("--query-start", default="20200101")
    parser.add_argument("--query-end", default="20260428")
    parser.add_argument("--train-years", default="2022,2023,2024,2025")
    parser.add_argument("--extra-test-years", default="2021,2026")
    args = parser.parse_args(argv)

    run_up5in10_path_score_study(
        source_db_path=args.source_db,
        event_feature_csv_path=args.event_feature_csv,
        threshold_summary_csv_path=args.threshold_summary_csv,
        quintile_summary_csv_path=args.quintile_summary_csv,
        oos_summary_csv_path=args.oos_summary_csv,
        report_markdown_path=args.report_markdown,
        entry_start=str(args.entry_start),
        entry_end=str(args.entry_end),
        query_start=str(args.query_start),
        query_end=str(args.query_end),
        train_years=_parse_int_tuple(str(args.train_years)),
        extra_test_years=_parse_int_tuple(str(args.extra_test_years)),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
