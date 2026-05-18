from __future__ import annotations

import argparse
import math
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

LOOKBACK_DAYS = 60
TURNOVER_BASELINE_DAYS = 20
FORWARD_DAYS = 30
MIN_HISTORY = LOOKBACK_DAYS + TURNOVER_BASELINE_DAYS
EXPAND_THRESHOLD = 1.3
CONTRACT_THRESHOLD = 0.8
PRICE_UP = 0.01
PRICE_DOWN = -0.01

DEFAULT_YEAR_TO_REGIME: dict[int, str] = {
    2022: "attention_transition",
    2023: "clean_breakout",
    2024: "repair_retake",
    2025: "trend_continuation",
}

REGIME_FEATURE_COLUMNS = [
    "early_mean_ret",
    "late_mean_ret",
    "launch_pad_mean_ret",
    "early_mean_turnover_ratio",
    "late_mean_turnover_ratio",
    "launch_pad_mean_turnover_ratio",
    "late_expand_up_rate",
    "launch_pad_expand_up_rate",
    "late_contract_down_rate",
    "launch_pad_contract_down_rate",
    "late_contract_flat_rate",
    "launch_pad_contract_flat_rate",
]

ENTRY_FEATURE_COLUMNS = [
    "late_mean_turnover_ratio",
    "launch_pad_mean_turnover_ratio",
    "late_expand_up_rate",
    "launch_pad_expand_up_rate",
    "late_contract_down_rate",
    "launch_pad_contract_down_rate",
    "late_contract_flat_rate",
    "launch_pad_contract_flat_rate",
    "late_up_mean_turnover_ratio",
    "launch_pad_up_mean_turnover_ratio",
    "launch_pad_down_mean_turnover_ratio",
    "early_mean_ret",
    "late_mean_ret",
    "launch_pad_mean_ret",
]

PHASE_WINDOWS: dict[str, tuple[int, int]] = {
    "early": (-60, -30),
    "late": (-30, 0),
    "launch_pad": (-10, 0),
}


def _safe_mean(values: np.ndarray) -> float:
    if values.size == 0:
        return float("nan")
    return float(values.mean())


def _conditional_mean(values: np.ndarray, mask: np.ndarray) -> float:
    if not mask.any():
        return float("nan")
    return float(values[mask].mean())


def _phase_feature_dict(
    *,
    prefix: str,
    returns: np.ndarray,
    turnover_ratio: np.ndarray,
) -> dict[str, float]:
    up = returns > PRICE_UP
    down = returns < PRICE_DOWN
    flat = ~(up | down)
    expand = turnover_ratio >= EXPAND_THRESHOLD
    contract = turnover_ratio <= CONTRACT_THRESHOLD

    expand_up = expand & up
    expand_down = expand & down
    expand_flat = expand & flat
    contract_up = contract & up
    contract_down = contract & down
    contract_flat = contract & flat

    length = float(len(returns))
    return {
        f"{prefix}_mean_ret": _safe_mean(returns),
        f"{prefix}_mean_turnover_ratio": _safe_mean(turnover_ratio),
        f"{prefix}_expand_up_rate": float(expand_up.sum() / length),
        f"{prefix}_expand_down_rate": float(expand_down.sum() / length),
        f"{prefix}_expand_flat_rate": float(expand_flat.sum() / length),
        f"{prefix}_contract_up_rate": float(contract_up.sum() / length),
        f"{prefix}_contract_down_rate": float(contract_down.sum() / length),
        f"{prefix}_contract_flat_rate": float(contract_flat.sum() / length),
        f"{prefix}_up_mean_turnover_ratio": _conditional_mean(turnover_ratio, up),
        f"{prefix}_down_mean_turnover_ratio": _conditional_mean(turnover_ratio, down),
        f"{prefix}_flat_mean_turnover_ratio": _conditional_mean(turnover_ratio, flat),
    }


def _compute_forward_hit_stats(
    *,
    entry_price: float,
    forward_high: np.ndarray,
    forward_low: np.ndarray,
) -> dict[str, Any]:
    target = entry_price * 1.20
    floor = entry_price * 0.90
    days_to_up20 = float("nan")
    days_to_loss10 = float("nan")

    for idx, (high_value, low_value) in enumerate(zip(forward_high, forward_low, strict=False), start=1):
        if math.isnan(days_to_up20) and float(high_value) >= target:
            days_to_up20 = float(idx)
        if math.isnan(days_to_loss10) and float(low_value) < floor:
            days_to_loss10 = float(idx)
        if not math.isnan(days_to_up20) and not math.isnan(days_to_loss10):
            break

    if math.isnan(days_to_up20) and math.isnan(days_to_loss10):
        return {
            "days_to_up20": float("nan"),
            "days_to_loss10": float("nan"),
            "success_label": False,
            "first_hit": "unresolved",
        }
    if math.isnan(days_to_up20):
        return {
            "days_to_up20": float("nan"),
            "days_to_loss10": days_to_loss10,
            "success_label": False,
            "first_hit": "loss10_first",
        }
    if math.isnan(days_to_loss10):
        return {
            "days_to_up20": days_to_up20,
            "days_to_loss10": float("nan"),
            "success_label": True,
            "first_hit": "up20_first",
        }
    if days_to_loss10 <= days_to_up20:
        return {
            "days_to_up20": days_to_up20,
            "days_to_loss10": days_to_loss10,
            "success_label": False,
            "first_hit": "loss10_first",
        }
    return {
        "days_to_up20": days_to_up20,
        "days_to_loss10": days_to_loss10,
        "success_label": True,
        "first_hit": "up20_first",
    }


def build_candidate_feature_rows(
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
                d.trade_date,
                d.board,
                coalesce(d.is_st, false) AS is_st,
                coalesce(t.is_suspended, false) AS is_suspended,
                d.close_adj,
                d.high_adj,
                d.low_adj,
                d.turnover_value_cny
            FROM daily_bar_pit d
            LEFT JOIN tradeability_state_daily t
              ON d.security_id = t.security_id
             AND d.trade_date = t.trade_date
            WHERE d.trade_date BETWEEN ? AND ?
              AND d.price_basis = 'unadjusted'
              AND d.board <> 'beijing'
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
        return pd.DataFrame()

    security = frame["security_id"].to_numpy()
    trade_date = frame["trade_date"].astype(str).to_numpy()
    board = frame["board"].astype(str).to_numpy()
    is_st = frame["is_st"].fillna(False).to_numpy(dtype=bool)
    is_suspended = frame["is_suspended"].fillna(False).to_numpy(dtype=bool)
    close = frame["close_adj"].to_numpy(dtype=float)
    high = frame["high_adj"].to_numpy(dtype=float)
    low = frame["low_adj"].to_numpy(dtype=float)
    turnover = frame["turnover_value_cny"].to_numpy(dtype=float)

    boundaries = np.concatenate(([0], np.flatnonzero(security[1:] != security[:-1]) + 1, [len(frame)]))
    rows: list[dict[str, Any]] = []

    for group_idx in range(len(boundaries) - 1):
        start = int(boundaries[group_idx])
        end = int(boundaries[group_idx + 1])
        n = end - start
        if n < MIN_HISTORY + FORWARD_DAYS + 1:
            continue

        dates = trade_date[start:end]
        boards = board[start:end]
        st = is_st[start:end]
        suspended = is_suspended[start:end]
        c = close[start:end]
        h = high[start:end]
        l = low[start:end]
        tv = turnover[start:end]

        turnover_med = pd.Series(tv).rolling(TURNOVER_BASELINE_DAYS).median().shift(1).to_numpy(dtype=float)
        ret1 = np.full(n, np.nan)
        ret1[1:] = c[1:] / c[:-1] - 1.0
        turnover_ratio = tv / turnover_med

        for i in range(MIN_HISTORY, n - FORWARD_DAYS):
            day = dates[i]
            if day < entry_start or day > entry_end:
                continue
            if boards[i] != "main_board":
                continue
            if st[i] or suspended[i]:
                continue
            if not np.isfinite(turnover_med[i]):
                continue
            if np.any(~np.isfinite(turnover_ratio[i - LOOKBACK_DAYS : i])):
                continue
            if np.any(~np.isfinite(ret1[i - LOOKBACK_DAYS : i])):
                continue

            phase_payload: dict[str, float] = {}
            for phase_name, (left_offset, right_offset) in PHASE_WINDOWS.items():
                left = i + left_offset
                right = i + right_offset
                phase_payload.update(
                    _phase_feature_dict(
                        prefix=phase_name,
                        returns=ret1[left:right],
                        turnover_ratio=turnover_ratio[left:right],
                    )
                )

            entry_price = c[i]
            forward_high = h[i + 1 : i + FORWARD_DAYS + 1]
            forward_low = l[i + 1 : i + FORWARD_DAYS + 1]
            hit_stats = _compute_forward_hit_stats(
                entry_price=float(entry_price),
                forward_high=forward_high,
                forward_low=forward_low,
            )

            close_index = i + FORWARD_DAYS
            close_ret30 = float("nan")
            if close_index < n:
                close_ret30 = (float(c[close_index]) - entry_price) / entry_price

            rows.append(
                {
                    "security_id": security[start],
                    "trade_date": day,
                    "year": int(day[:4]),
                    "board": boards[i],
                    "entry_close_adj": float(entry_price),
                    "entry_turnover_ratio": float(turnover_ratio[i]),
                    "prior60_ret": float(c[i - 1] / c[i - 60] - 1.0),
                    "late30_ret": float(c[i - 1] / c[i - 30] - 1.0),
                    "close_ret30": close_ret30,
                    "max_ret30": float(np.max(forward_high) / entry_price - 1.0),
                    "min_ret30": float(np.min(forward_low) / entry_price - 1.0),
                    **hit_stats,
                    **phase_payload,
                }
            )

    return pd.DataFrame(rows).sort_values(["trade_date", "security_id"]).reset_index(drop=True)


def _resolve_feature_names(
    feature_names: Sequence[str] | None,
    default_columns: Sequence[str],
) -> list[str]:
    if feature_names is None:
        return list(default_columns)
    return [str(column) for column in feature_names]


def _daily_market_feature_frame(
    candidate_rows: pd.DataFrame,
    *,
    feature_names: Sequence[str],
    regime_rolling_days: int,
) -> pd.DataFrame:
    working = candidate_rows.copy()
    grouped = (
        working.groupby(["trade_date", "year"], as_index=False)[list(feature_names)]
        .mean()
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    min_periods = max(2, min(regime_rolling_days, 10))
    rolled = grouped[list(feature_names)].rolling(window=regime_rolling_days, min_periods=min_periods).mean()
    grouped.loc[:, list(feature_names)] = rolled.to_numpy()
    return grouped.dropna().reset_index(drop=True)


def fit_regime_prototypes(
    *,
    candidate_rows: pd.DataFrame,
    year_to_regime: Mapping[int, str],
    feature_names: Sequence[str] | None = None,
    regime_rolling_days: int = 20,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    names = _resolve_feature_names(feature_names, REGIME_FEATURE_COLUMNS)
    training = candidate_rows.loc[candidate_rows["year"].isin(year_to_regime.keys())].copy()
    if training.empty:
        return (
            pd.DataFrame(columns=["regime_label", *names]),
            pd.DataFrame({"feature": names, "scale": [1.0] * len(names)}),
        )

    daily = _daily_market_feature_frame(training, feature_names=names, regime_rolling_days=regime_rolling_days)
    daily["regime_label"] = daily["year"].map(year_to_regime)
    prototypes = daily.groupby("regime_label", as_index=False)[names].mean()

    scales = daily[names].std(ddof=0).replace(0.0, 1.0).fillna(1.0).reset_index()
    scales.columns = ["feature", "scale"]
    return prototypes, scales


def classify_regime_dates(
    *,
    daily_market_features: pd.DataFrame,
    prototypes: pd.DataFrame,
    feature_scales: pd.DataFrame,
    feature_names: Sequence[str] | None = None,
) -> pd.DataFrame:
    names = _resolve_feature_names(feature_names, prototypes.columns.drop("regime_label", errors="ignore").tolist())
    if daily_market_features.empty or prototypes.empty:
        return pd.DataFrame(columns=["trade_date", "year", "predicted_regime", "distance"])

    prototype_names = prototypes["regime_label"].astype(str).tolist()
    prototype_matrix = prototypes[names].to_numpy(dtype=float)
    scale_series = feature_scales.set_index("feature")["scale"].reindex(names).fillna(1.0)
    scale_vector = scale_series.to_numpy(dtype=float)

    rows: list[dict[str, Any]] = []
    for record in daily_market_features.to_dict(orient="records"):
        feature_vector = np.array([float(record[name]) for name in names], dtype=float)
        diff = (prototype_matrix - feature_vector) / scale_vector
        distances = np.sqrt(np.sum(diff * diff, axis=1))
        best_idx = int(np.argmin(distances))
        rows.append(
            {
                "trade_date": str(record["trade_date"]),
                "year": int(record["year"]),
                "predicted_regime": prototype_names[best_idx],
                "distance": float(distances[best_idx]),
            }
        )
    return pd.DataFrame(rows)


def fit_regime_entry_evaluators(
    *,
    candidate_rows: pd.DataFrame,
    year_to_regime: Mapping[int, str],
    feature_names: Sequence[str] | None = None,
) -> pd.DataFrame:
    names = _resolve_feature_names(feature_names, ENTRY_FEATURE_COLUMNS)
    training = candidate_rows.loc[candidate_rows["year"].isin(year_to_regime.keys())].copy()
    if training.empty:
        return pd.DataFrame(columns=["regime_label", "feature", "baseline_mean", "baseline_std", "weight"])

    training["regime_label"] = training["year"].map(year_to_regime)
    rows: list[dict[str, Any]] = []
    for regime_label, frame in training.groupby("regime_label", sort=True, dropna=False):
        baseline_mean = frame[names].mean()
        baseline_std = frame[names].std(ddof=0).replace(0.0, 1.0).fillna(1.0)
        success = frame.loc[frame["success_label"].fillna(False)]
        success_mean = success[names].mean() if not success.empty else baseline_mean
        weights = (success_mean - baseline_mean) / baseline_std
        for feature in names:
            rows.append(
                {
                    "regime_label": str(regime_label),
                    "feature": feature,
                    "baseline_mean": float(baseline_mean[feature]),
                    "baseline_std": float(baseline_std[feature]),
                    "weight": float(weights[feature]),
                }
            )
    return pd.DataFrame(rows)


def score_candidate_rows(
    *,
    candidate_rows: pd.DataFrame,
    regime_by_date: pd.DataFrame,
    evaluators: pd.DataFrame,
    feature_names: Sequence[str] | None = None,
) -> pd.DataFrame:
    names = _resolve_feature_names(feature_names, ENTRY_FEATURE_COLUMNS)
    merged = candidate_rows.merge(
        regime_by_date[["trade_date", "predicted_regime"]],
        on="trade_date",
        how="left",
    )
    merged["score"] = float("nan")
    if merged.empty or evaluators.empty:
        return merged

    for regime_label, frame in evaluators.groupby("regime_label", sort=True):
        mask = merged["predicted_regime"] == regime_label
        if not mask.any():
            continue
        ordered = frame.set_index("feature").reindex(names)
        means = ordered["baseline_mean"].to_numpy(dtype=float)
        stds = ordered["baseline_std"].replace(0.0, 1.0).fillna(1.0).to_numpy(dtype=float)
        weights = ordered["weight"].to_numpy(dtype=float)
        values = merged.loc[mask, names].to_numpy(dtype=float)
        zscores = (values - means) / stds
        merged.loc[mask, "score"] = np.dot(zscores, weights)
    return merged


def _majority_value(series: pd.Series) -> tuple[str | None, float]:
    clean = series.dropna().astype(str)
    if clean.empty:
        return None, float("nan")
    counts = clean.value_counts(normalize=True)
    return str(counts.index[0]), float(counts.iloc[0])


def evaluate_ranked_signals(
    *,
    scored_rows: pd.DataFrame,
    validation_years: Sequence[int],
    top_n_per_day: int,
) -> pd.DataFrame:
    working = scored_rows.loc[scored_rows["year"].isin(validation_years)].copy()
    if working.empty:
        return pd.DataFrame(
            columns=[
                "year",
                "candidate_rows",
                "candidate_event_rate",
                "candidate_mean_close_ret30",
                "selected_rows",
                "selected_signal_days",
                "selected_event_rate",
                "selected_mean_close_ret30",
                "selected_mean_max_ret30",
                "selected_mean_min_ret30",
                "predicted_regime_majority",
                "predicted_regime_majority_share",
            ]
        )

    selected = (
        working.sort_values(["trade_date", "score"], ascending=[True, False])
        .groupby("trade_date", group_keys=False)
        .head(top_n_per_day)
        .reset_index(drop=True)
    )

    rows: list[dict[str, Any]] = []
    for year, frame in working.groupby("year", sort=True):
        picked = selected.loc[selected["year"] == year]
        regime_majority, regime_share = _majority_value(picked["predicted_regime"])
        rows.append(
            {
                "year": int(year),
                "candidate_rows": int(len(frame)),
                "candidate_event_rate": float(frame["success_label"].mean()),
                "candidate_mean_close_ret30": float(frame["close_ret30"].mean()),
                "selected_rows": int(len(picked)),
                "selected_signal_days": int(picked["trade_date"].nunique()),
                "selected_event_rate": float(picked["success_label"].mean()) if not picked.empty else float("nan"),
                "selected_mean_close_ret30": float(picked["close_ret30"].mean()) if not picked.empty else float("nan"),
                "selected_mean_max_ret30": float(picked["max_ret30"].mean()) if not picked.empty else float("nan"),
                "selected_mean_min_ret30": float(picked["min_ret30"].mean()) if not picked.empty else float("nan"),
                "predicted_regime_majority": regime_majority,
                "predicted_regime_majority_share": regime_share,
            }
        )
    return pd.DataFrame(rows).sort_values("year").reset_index(drop=True)


def _format_markdown_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        return f"{value:.6g}"
    return str(value)


def _markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows._"
    headers = [str(col) for col in frame.columns]
    lines = [
        f"| {' | '.join(headers)} |",
        f"| {' | '.join(['---'] * len(headers))} |",
    ]
    for row in frame.to_dict(orient="records"):
        cells = [_format_markdown_cell(row.get(col)) for col in frame.columns]
        lines.append(f"| {' | '.join(cells)} |")
    return "\n".join(lines)


def write_markdown_report(
    *,
    report_markdown_path: Path,
    source_db_path: Path | None,
    summary: pd.DataFrame,
    regime_dates: pd.DataFrame,
    training_years: Sequence[int],
    validation_years: Sequence[int],
    top_n_per_day: int,
) -> None:
    regime_summary = (
        regime_dates.groupby(["year", "predicted_regime"], as_index=False)
        .size()
        .rename(columns={"size": "days"})
        .sort_values(["year", "days"], ascending=[True, False])
        .reset_index(drop=True)
    )

    object_lines = [
        f"- Training years: `{', '.join(str(year) for year in training_years)}`",
        f"- Validation years: `{', '.join(str(year) for year in validation_years)}`",
        f"- Top N per day: `{top_n_per_day}`",
    ]
    if source_db_path is not None:
        object_lines.insert(0, f"- Source DB: `{source_db_path}`")
    else:
        object_lines.insert(0, "- Source DB: synthetic / prebuilt candidate rows")

    lines = [
        f"# Price-Volume Regime Validation Study - {date.today().isoformat()}",
        "",
        "## Object",
        *object_lines,
        "",
        "## Validation Summary",
        "- This is an event-level replay / ranking validation, not a broker-grade portfolio simulator.",
        _markdown_table(summary),
        "",
        "## Predicted Regime Days",
        _markdown_table(regime_summary),
        "",
        "## Judgment",
        "- Use the classifier as a regime gate first, then score entries with the regime-specific evaluator.",
        "- Treat the ranking summary as signal validation evidence, not deployable portfolio evidence.",
    ]

    report_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    report_markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_price_volume_regime_validation_study(
    *,
    summary_csv_path: Path,
    scored_csv_path: Path,
    regimes_csv_path: Path,
    report_markdown_path: Path | None = None,
    candidate_rows: pd.DataFrame | None = None,
    source_db_path: Path | None = None,
    entry_start: str = "20210101",
    entry_end: str = "20261231",
    query_start: str = "20200101",
    query_end: str = "20261231",
    training_years: Sequence[int] = (2022, 2023, 2024, 2025),
    validation_years: Sequence[int] = (2021, 2026),
    year_to_regime: Mapping[int, str] | None = None,
    top_n_per_day: int = 5,
    regime_rolling_days: int = 20,
) -> dict[str, pd.DataFrame]:
    regime_map = dict(year_to_regime or DEFAULT_YEAR_TO_REGIME)
    if candidate_rows is None:
        if source_db_path is None:
            raise ValueError("source_db_path is required when candidate_rows is not provided.")
        candidate_rows = build_candidate_feature_rows(
            source_db_path=source_db_path,
            entry_start=entry_start,
            entry_end=entry_end,
            query_start=query_start,
            query_end=query_end,
        )
    if candidate_rows.empty:
        summary = evaluate_ranked_signals(scored_rows=pd.DataFrame(), validation_years=validation_years, top_n_per_day=top_n_per_day)
        empty_scored = pd.DataFrame()
        empty_regimes = pd.DataFrame(columns=["trade_date", "year", "predicted_regime", "distance"])
        summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
        scored_csv_path.parent.mkdir(parents=True, exist_ok=True)
        regimes_csv_path.parent.mkdir(parents=True, exist_ok=True)
        summary.to_csv(summary_csv_path, index=False)
        empty_scored.to_csv(scored_csv_path, index=False)
        empty_regimes.to_csv(regimes_csv_path, index=False)
        if report_markdown_path is not None:
            write_markdown_report(
                report_markdown_path=report_markdown_path,
                source_db_path=source_db_path,
                summary=summary,
                regime_dates=empty_regimes,
                training_years=training_years,
                validation_years=validation_years,
                top_n_per_day=top_n_per_day,
            )
        return {"summary": summary, "scored": empty_scored, "regimes": empty_regimes}

    candidate_rows = candidate_rows.sort_values(["trade_date", "security_id"]).reset_index(drop=True)
    training_rows = candidate_rows.loc[candidate_rows["year"].isin(training_years)].copy()
    validation_rows = candidate_rows.loc[candidate_rows["year"].isin(validation_years)].copy()

    prototypes, feature_scales = fit_regime_prototypes(
        candidate_rows=training_rows,
        year_to_regime=regime_map,
        feature_names=REGIME_FEATURE_COLUMNS,
        regime_rolling_days=regime_rolling_days,
    )
    daily_market = _daily_market_feature_frame(
        candidate_rows,
        feature_names=REGIME_FEATURE_COLUMNS,
        regime_rolling_days=regime_rolling_days,
    )
    regime_dates = classify_regime_dates(
        daily_market_features=daily_market.loc[daily_market["year"].isin(validation_years)].copy(),
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
        candidate_rows=validation_rows,
        regime_by_date=regime_dates,
        evaluators=evaluators,
        feature_names=ENTRY_FEATURE_COLUMNS,
    )
    summary = evaluate_ranked_signals(
        scored_rows=scored,
        validation_years=validation_years,
        top_n_per_day=top_n_per_day,
    )

    summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
    scored_csv_path.parent.mkdir(parents=True, exist_ok=True)
    regimes_csv_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_csv_path, index=False)
    scored.to_csv(scored_csv_path, index=False)
    regime_dates.to_csv(regimes_csv_path, index=False)

    if report_markdown_path is not None:
        write_markdown_report(
            report_markdown_path=report_markdown_path,
            source_db_path=source_db_path,
            summary=summary,
            regime_dates=regime_dates,
            training_years=training_years,
            validation_years=validation_years,
            top_n_per_day=top_n_per_day,
        )

    return {"summary": summary, "scored": scored, "regimes": regime_dates}


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the price-volume regime validation study.")
    parser.add_argument("--source-db", required=True, type=Path)
    parser.add_argument("--summary-csv", required=True, type=Path)
    parser.add_argument("--scored-csv", required=True, type=Path)
    parser.add_argument("--regimes-csv", required=True, type=Path)
    parser.add_argument("--report-markdown", required=False, type=Path)
    parser.add_argument("--entry-start", default="20210101")
    parser.add_argument("--entry-end", default="20261231")
    parser.add_argument("--query-start", default="20200101")
    parser.add_argument("--query-end", default="20261231")
    parser.add_argument("--top-n-per-day", default=5, type=int)
    parser.add_argument("--regime-rolling-days", default=20, type=int)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_price_volume_regime_validation_study(
        source_db_path=args.source_db,
        summary_csv_path=args.summary_csv,
        scored_csv_path=args.scored_csv,
        regimes_csv_path=args.regimes_csv,
        report_markdown_path=args.report_markdown,
        entry_start=str(args.entry_start),
        entry_end=str(args.entry_end),
        query_start=str(args.query_start),
        query_end=str(args.query_end),
        top_n_per_day=int(args.top_n_per_day),
        regime_rolling_days=int(args.regime_rolling_days),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
