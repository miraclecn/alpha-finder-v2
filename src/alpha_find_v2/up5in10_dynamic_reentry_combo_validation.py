from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .up5in10_price_volume_study import _markdown_table
from .up5in10_slot_portfolio_backtest import (
    DynamicTrailingPolicy,
    build_bar_lookup_for_slot_backtest,
    load_selected_bars_for_slot_backtest,
    load_trade_calendar,
    run_dynamic_slot_portfolio_backtest,
    summarize_continuous_slot_backtest_by_year,
    summarize_exit_reasons,
)

BASELINE_SIGNAL_ORIGIN = "baseline"
BASELINE_SIGNAL_PRIORITY = 10.0
REENTRY_SIGNAL_PRIORITY = 20.0
BASELINE_TARGET_POSITION_FRACTION = 0.15
REENTRY_TARGET_POSITION_FRACTION = 0.05
MAIN_RULE_MIN_RANGE_120 = 0.10
MAIN_RULE_MAX_RANGE_120 = 0.35
DEFAULT_POLICY = DynamicTrailingPolicy(
    stop_loss=0.06,
    activation_return=0.15,
    trailing_drawdown=0.03,
)
DEFAULT_RULE_NAMES: tuple[str, ...] = ("A", "A1", "A2", "A3", "A4")
DEFAULT_MAIN_RULE_ARTIFACTS: tuple[str, ...] = (
    ".tmp/up5in10_2020_group3_p15_stop6_act15_trail3_selected_with_levels.csv",
    ".tmp/up5in10_top10_selected_with_levels.csv",
)


def load_default_main_rule_selected_rows(
    artifact_paths: tuple[Path, ...] | None = None,
) -> pd.DataFrame:
    paths = artifact_paths or tuple(Path(path) for path in DEFAULT_MAIN_RULE_ARTIFACTS)
    frames: list[pd.DataFrame] = []
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"missing main-rule selected artifact: {path}")
        frames.append(pd.read_csv(path))
    combined = pd.concat(frames, ignore_index=True)
    return filter_main_rule_selected_rows(combined)


def filter_main_rule_selected_rows(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    working = rows.copy()
    working["trade_date"] = working["trade_date"].astype(str)
    working["year"] = working["trade_date"].str[:4].astype(int)
    working = working.loc[
        working["range_pos_120"].ge(MAIN_RULE_MIN_RANGE_120)
        & working["range_pos_120"].lt(MAIN_RULE_MAX_RANGE_120)
    ].copy()
    return working.sort_values(
        ["trade_date", "path_score", "security_id"],
        ascending=[True, False, True],
    ).reset_index(drop=True)


def prepare_baseline_selected_rows(
    rows: pd.DataFrame,
    *,
    target_position_fraction: float = BASELINE_TARGET_POSITION_FRACTION,
    signal_priority: float = BASELINE_SIGNAL_PRIORITY,
) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    working = rows.copy()
    working["trade_date"] = working["trade_date"].astype(str)
    working["year"] = working["trade_date"].str[:4].astype(int)
    if "path_score" not in working.columns:
        working["path_score"] = 0.0
    working["signal_origin"] = BASELINE_SIGNAL_ORIGIN
    working["target_position_fraction"] = float(target_position_fraction)
    working["signal_priority"] = float(signal_priority)
    return working


def build_reentry_feature_bars(bars: pd.DataFrame) -> pd.DataFrame:
    if bars.empty:
        return bars.copy()
    working = bars.copy()
    working["trade_date"] = working["trade_date"].astype(str)
    working = working.sort_values(["security_id", "trade_date"]).reset_index(drop=True)
    turnover = pd.to_numeric(working["turnover_value_cny"], errors="coerce")
    working["turnover_value_cny"] = turnover
    parts: list[pd.DataFrame] = []
    for _, frame in working.groupby("security_id", sort=False):
        piece = frame.copy()
        piece["prev5_turnover_mean"] = (
            piece["turnover_value_cny"].shift(1).rolling(window=5, min_periods=5).mean()
        )
        piece["signal_turnover_vs_prev5"] = (
            piece["turnover_value_cny"] / piece["prev5_turnover_mean"]
        )
        parts.append(piece)
    return pd.concat(parts, ignore_index=True)


def extract_a_reentry_candidates(
    source_trades: pd.DataFrame,
    *,
    feature_bars: pd.DataFrame,
    calendar: list[str],
) -> pd.DataFrame:
    columns = [
        "security_id",
        "signal_date",
        "reentry_date",
        "source_entry_date",
        "source_exit_date",
        "source_gross_ret",
        "source_holding_days",
        "path_score",
        "signal_turnover_vs_prev5",
        "reentry_open_vs_exit_price",
    ]
    if source_trades.empty or feature_bars.empty or not calendar:
        return pd.DataFrame(columns=columns)

    working_trades = source_trades.copy()
    working_trades["exit_trade_date"] = working_trades["exit_trade_date"].astype(str)
    working_trades["entry_trade_date"] = working_trades["entry_trade_date"].astype(str)
    working_trades = working_trades.loc[
        working_trades["exit_reason"] == "dynamic_trailing_stop"
    ].copy()
    if working_trades.empty:
        return pd.DataFrame(columns=columns)

    next_trade_date = {
        str(calendar[index]): str(calendar[index + 1])
        for index in range(len(calendar) - 1)
    }
    bar_lookup = build_bar_lookup_for_slot_backtest(feature_bars)

    rows: list[dict[str, Any]] = []
    for row in working_trades.to_dict(orient="records"):
        signal_date = str(row["exit_trade_date"])
        reentry_date = next_trade_date.get(signal_date)
        if reentry_date is None:
            continue
        next_bar = bar_lookup.get((str(row["security_id"]), reentry_date))
        if next_bar is None or not _is_finite_positive(next_bar.get("open_adj")):
            continue
        exit_bar = bar_lookup.get((str(row["security_id"]), signal_date))
        exit_price = float(row["exit_price"])
        rows.append(
            {
                "security_id": str(row["security_id"]),
                "signal_date": signal_date,
                "reentry_date": reentry_date,
                "source_entry_date": str(row["entry_trade_date"]),
                "source_exit_date": signal_date,
                "source_gross_ret": float(row["gross_ret"]),
                "source_holding_days": int(row["holding_days"]),
                "path_score": float(row.get("path_score", 0.0)),
                "signal_turnover_vs_prev5": (
                    float(exit_bar["signal_turnover_vs_prev5"])
                    if exit_bar is not None and _is_finite_number(exit_bar.get("signal_turnover_vs_prev5"))
                    else float("nan")
                ),
                "reentry_open_vs_exit_price": float(next_bar["open_adj"]) / exit_price - 1.0,
            }
        )
    result = pd.DataFrame(rows, columns=columns)
    if result.empty:
        return result
    return result.sort_values(
        ["signal_date", "path_score", "security_id"],
        ascending=[True, False, True],
    ).reset_index(drop=True)


def filter_reentry_candidates_by_rule(
    candidates: pd.DataFrame,
    rule_name: str,
) -> pd.DataFrame:
    if candidates.empty:
        return candidates.copy()
    working = candidates.copy()
    base_mask = (
        working["source_gross_ret"].ge(0.20)
        & working["reentry_open_vs_exit_price"].le(0.0)
    )
    rule_masks: dict[str, pd.Series] = {
        "A": base_mask,
        "A1": base_mask & working["signal_turnover_vs_prev5"].lt(1.5),
        "A2": base_mask
        & working["signal_turnover_vs_prev5"].lt(1.5)
        & working["source_holding_days"].gt(10),
        "A3": base_mask
        & working["signal_turnover_vs_prev5"].lt(1.5)
        & working["source_gross_ret"].ge(0.25)
        & working["source_gross_ret"].lt(0.35),
        "A4": base_mask
        & working["signal_turnover_vs_prev5"].lt(1.5)
        & working["reentry_open_vs_exit_price"].le(-0.02),
    }
    if rule_name not in rule_masks:
        raise ValueError(f"unsupported reentry rule: {rule_name}")
    filtered = working.loc[rule_masks[rule_name]].copy()
    return filtered.sort_values(
        ["signal_date", "path_score", "security_id"],
        ascending=[True, False, True],
    ).reset_index(drop=True)


def build_reentry_selected_rows(
    candidates: pd.DataFrame,
    *,
    rule_name: str,
    target_position_fraction: float = REENTRY_TARGET_POSITION_FRACTION,
    signal_priority: float = REENTRY_SIGNAL_PRIORITY,
) -> pd.DataFrame:
    if candidates.empty:
        return pd.DataFrame(
            columns=[
                "security_id",
                "trade_date",
                "year",
                "path_score",
                "signal_origin",
                "target_position_fraction",
                "signal_priority",
            ]
        )
    working = candidates.copy()
    working["trade_date"] = working["signal_date"].astype(str)
    working["year"] = working["trade_date"].str[:4].astype(int)
    if "path_score" not in working.columns:
        working["path_score"] = 0.0
    working["signal_origin"] = f"reentry_{rule_name}"
    working["target_position_fraction"] = float(target_position_fraction)
    working["signal_priority"] = float(signal_priority)
    columns = [
        "security_id",
        "trade_date",
        "year",
        "path_score",
        "signal_origin",
        "target_position_fraction",
        "signal_priority",
    ]
    result = working[columns].copy()
    return result.drop_duplicates(["security_id", "trade_date"], keep="first").reset_index(drop=True)


def summarize_full_period(
    *,
    case_name: str,
    daily_curve: pd.DataFrame,
    trades: pd.DataFrame,
    selected_rows: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "case_name",
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
        "baseline_rows",
        "reentry_rows",
        "reentry_trades",
        "reentry_trade_win_rate",
        "reentry_avg_net_ret",
    ]
    if daily_curve.empty:
        return pd.DataFrame(columns=columns)
    frame = daily_curve.sort_values("trade_date").reset_index(drop=True)
    values = frame["portfolio_value"].astype(float)
    returns = frame["daily_return"].astype(float)
    start_value = float(values.iloc[0]) / (1.0 + float(returns.iloc[0])) if len(values) else float("nan")
    peak = values.cummax()
    drawdown = values / peak - 1.0
    return_std = float(returns.std(ddof=0))
    reentry_trades = trades.loc[trades["signal_origin"] != BASELINE_SIGNAL_ORIGIN].copy() if not trades.empty else pd.DataFrame()
    row = {
        "case_name": case_name,
        "total_return": float(values.iloc[-1] / start_value - 1.0) if start_value > 0.0 else float("nan"),
        "max_drawdown": float(drawdown.min()),
        "annual_vol": float(returns.std(ddof=0) * np.sqrt(252.0)),
        "sharpe_like": float(returns.mean() / return_std * np.sqrt(252.0)) if return_std > 0.0 else float("nan"),
        "avg_gross_exposure": float(frame["gross_exposure"].mean()),
        "avg_active_positions": float(frame["active_positions"].mean()),
        "entries": int(frame["entries"].sum()),
        "skipped_entries": int(frame["skipped_entries"].sum()),
        "closed_trades": int(len(trades)),
        "trade_win_rate": float((trades["net_ret"] > 0.0).mean()) if not trades.empty else float("nan"),
        "avg_trade_net_ret": float(trades["net_ret"].mean()) if not trades.empty else float("nan"),
        "avg_holding_days": float(trades["holding_days"].mean()) if not trades.empty else float("nan"),
        "baseline_rows": int((selected_rows["signal_origin"] == BASELINE_SIGNAL_ORIGIN).sum()),
        "reentry_rows": int((selected_rows["signal_origin"] != BASELINE_SIGNAL_ORIGIN).sum()),
        "reentry_trades": int(len(reentry_trades)),
        "reentry_trade_win_rate": (
            float((reentry_trades["net_ret"] > 0.0).mean()) if not reentry_trades.empty else float("nan")
        ),
        "reentry_avg_net_ret": float(reentry_trades["net_ret"].mean()) if not reentry_trades.empty else float("nan"),
    }
    return pd.DataFrame([row], columns=columns)


def summarize_trade_origins(
    *,
    case_name: str,
    trades: pd.DataFrame,
) -> pd.DataFrame:
    columns = ["case_name", "signal_origin", "trades", "win_rate", "avg_net_ret", "avg_holding_days"]
    if trades.empty:
        return pd.DataFrame(columns=columns)
    summary = (
        trades.groupby("signal_origin", as_index=False)
        .agg(
            trades=("net_ret", "size"),
            win_rate=("net_ret", lambda s: float((s > 0.0).mean())),
            avg_net_ret=("net_ret", "mean"),
            avg_holding_days=("holding_days", "mean"),
        )
        .sort_values("signal_origin")
        .reset_index(drop=True)
    )
    summary.insert(0, "case_name", case_name)
    return summary[columns]


def build_case_selected_rows(
    *,
    baseline_selected_rows: pd.DataFrame,
    reentry_candidates: pd.DataFrame,
    rule_name: str | None,
    reentry_fraction: float,
) -> pd.DataFrame:
    baseline = prepare_baseline_selected_rows(baseline_selected_rows)
    if rule_name is None:
        return baseline
    reentry_selected = build_reentry_selected_rows(
        filter_reentry_candidates_by_rule(reentry_candidates, rule_name),
        rule_name=rule_name,
        target_position_fraction=reentry_fraction,
    )
    return pd.concat([baseline, reentry_selected], ignore_index=True)


def run_up5in10_dynamic_reentry_combo_validation(
    *,
    source_db_path: Path,
    baseline_selected_rows: pd.DataFrame,
    output_prefix: Path,
    rule_names: tuple[str, ...] = DEFAULT_RULE_NAMES,
    reentry_fraction: float = REENTRY_TARGET_POSITION_FRACTION,
    policy: DynamicTrailingPolicy = DEFAULT_POLICY,
    initial_cash: float = 10_000_000.0,
    max_positions: int = 10,
    buy_cost_bps: float = 12.0,
    sell_cost_bps: float = 12.0,
) -> dict[str, Path]:
    baseline = prepare_baseline_selected_rows(baseline_selected_rows)
    if baseline.empty:
        raise ValueError("baseline_selected_rows must not be empty")

    query_start = _shift_date(baseline["trade_date"].min(), -40)
    query_end = _shift_date(baseline["trade_date"].max(), 160)
    security_ids = baseline["security_id"].dropna().astype(str).unique().tolist()
    calendar = load_trade_calendar(
        source_db_path=source_db_path,
        query_start=query_start,
        query_end=query_end,
    )
    bars = load_selected_bars_for_slot_backtest(
        source_db_path=source_db_path,
        security_ids=security_ids,
        query_start=query_start,
        query_end=query_end,
    )
    feature_bars = build_reentry_feature_bars(bars)

    baseline_curve, baseline_trades = run_dynamic_slot_portfolio_backtest(
        selected_rows=baseline,
        bars=bars,
        calendar=calendar,
        policy=policy,
        initial_cash=initial_cash,
        max_positions=max_positions,
        buy_cost_bps=buy_cost_bps,
        sell_cost_bps=sell_cost_bps,
    )
    reentry_candidates = extract_a_reentry_candidates(
        baseline_trades,
        feature_bars=feature_bars,
        calendar=calendar,
    )

    case_defs: list[tuple[str, str | None]] = [("baseline_only", None)]
    case_defs.extend((f"reentry_{rule_name.lower()}_5pct", rule_name) for rule_name in rule_names)

    full_summary_parts: list[pd.DataFrame] = []
    year_summary_parts: list[pd.DataFrame] = []
    trade_parts: list[pd.DataFrame] = []
    exit_reason_parts: list[pd.DataFrame] = []
    selection_parts: list[pd.DataFrame] = []
    origin_parts: list[pd.DataFrame] = []

    for case_name, rule_name in case_defs:
        if rule_name is None:
            selected = baseline
            curve = baseline_curve.copy()
            trades = baseline_trades.copy()
        else:
            selected = build_case_selected_rows(
                baseline_selected_rows=baseline_selected_rows,
                reentry_candidates=reentry_candidates,
                rule_name=rule_name,
                reentry_fraction=reentry_fraction,
            )
            curve, trades = run_dynamic_slot_portfolio_backtest(
                selected_rows=selected,
                bars=bars,
                calendar=calendar,
                policy=policy,
                initial_cash=initial_cash,
                max_positions=max_positions,
                buy_cost_bps=buy_cost_bps,
                sell_cost_bps=sell_cost_bps,
            )

        case_curve = curve.copy()
        case_curve.insert(0, "case_name", case_name)
        case_trades = trades.copy()
        case_trades.insert(0, "case_name", case_name)
        full_summary_parts.append(
            summarize_full_period(
                case_name=case_name,
                daily_curve=curve,
                trades=trades,
                selected_rows=selected,
            )
        )
        year_summary = summarize_continuous_slot_backtest_by_year(
            daily_curve=curve,
            trades=trades,
        )
        year_summary.insert(0, "case_name", case_name)
        year_summary_parts.append(year_summary)
        exit_reasons = summarize_exit_reasons(trades)
        exit_reasons.insert(0, "case_name", case_name)
        exit_reason_parts.append(exit_reasons)
        trade_parts.append(case_trades)
        selection_parts.append(
            pd.DataFrame(
                [
                    {
                        "case_name": case_name,
                        "baseline_rows": int((selected["signal_origin"] == BASELINE_SIGNAL_ORIGIN).sum()),
                        "reentry_rows": int((selected["signal_origin"] != BASELINE_SIGNAL_ORIGIN).sum()),
                    }
                ]
            )
        )
        origin_parts.append(summarize_trade_origins(case_name=case_name, trades=trades))

    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    candidate_path = output_prefix.with_name(f"{output_prefix.name}_reentry_candidates.csv")
    full_summary_path = output_prefix.with_name(f"{output_prefix.name}_full_summary.csv")
    year_summary_path = output_prefix.with_name(f"{output_prefix.name}_year_summary.csv")
    trade_path = output_prefix.with_name(f"{output_prefix.name}_trades.csv")
    exit_reason_path = output_prefix.with_name(f"{output_prefix.name}_exit_reasons.csv")
    selection_path = output_prefix.with_name(f"{output_prefix.name}_selection_summary.csv")
    origin_path = output_prefix.with_name(f"{output_prefix.name}_origin_summary.csv")

    reentry_candidates.to_csv(candidate_path, index=False)
    pd.concat(full_summary_parts, ignore_index=True).to_csv(full_summary_path, index=False)
    pd.concat(year_summary_parts, ignore_index=True).to_csv(year_summary_path, index=False)
    pd.concat(trade_parts, ignore_index=True).to_csv(trade_path, index=False)
    pd.concat(exit_reason_parts, ignore_index=True).to_csv(exit_reason_path, index=False)
    pd.concat(selection_parts, ignore_index=True).to_csv(selection_path, index=False)
    pd.concat(origin_parts, ignore_index=True).to_csv(origin_path, index=False)

    return {
        "candidate_path": candidate_path,
        "full_summary_path": full_summary_path,
        "year_summary_path": year_summary_path,
        "trade_path": trade_path,
        "exit_reason_path": exit_reason_path,
        "selection_path": selection_path,
        "origin_path": origin_path,
    }


def write_combo_validation_report(
    *,
    output_path: Path,
    full_summary: pd.DataFrame,
    year_summary: pd.DataFrame,
    origin_summary: pd.DataFrame,
) -> None:
    focus_columns = [
        "case_name",
        "total_return",
        "max_drawdown",
        "annual_vol",
        "sharpe_like",
        "avg_gross_exposure",
        "avg_active_positions",
        "closed_trades",
        "trade_win_rate",
        "avg_trade_net_ret",
        "reentry_rows",
        "reentry_trades",
        "reentry_trade_win_rate",
        "reentry_avg_net_ret",
    ]
    report = "\n".join(
        [
            "# up5in10 动态接回组合验证",
            "",
            "## 口径",
            "",
            "- 主规则仍为 `top10 + 120日位置10%-35% + 15%单仓 + 最多10仓 + 6%硬止损 + 15%启动/3%回撤动态止盈`。",
            "- 接回仓位统一固定为 `5%`。",
            "- 主规则新票优先级高于接回票。",
            "- 接回只研究 A 系列：`A / A1 / A2 / A3 / A4`。",
            "- `2026` 为截至当前数据的部分年份样本。",
            "",
            "## 全样本汇总",
            "",
            _markdown_table(full_summary[focus_columns], float_digits=4),
            "",
            "## 分年汇总",
            "",
            _markdown_table(
                year_summary[
                    [
                        "case_name",
                        "year",
                        "total_return",
                        "max_drawdown",
                        "avg_gross_exposure",
                        "avg_active_positions",
                        "closed_trades",
                        "trade_win_rate",
                        "avg_trade_net_ret",
                    ]
                ],
                float_digits=4,
            ),
            "",
            "## 交易来源拆分",
            "",
            _markdown_table(origin_summary, float_digits=4),
        ]
    )
    output_path.write_text(report + "\n", encoding="utf-8")


def _shift_date(date_str: str, days: int) -> str:
    parsed = datetime.strptime(str(date_str), "%Y%m%d").date()
    return (parsed + timedelta(days=days)).strftime("%Y%m%d")


def _is_finite_positive(value: Any) -> bool:
    return _is_finite_number(value) and float(value) > 0.0


def _is_finite_number(value: Any) -> bool:
    if value is None:
        return False
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return bool(np.isfinite(numeric))


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate up5in10 dynamic reentry combo variants.")
    parser.add_argument(
        "--source-db",
        type=Path,
        default=Path("output/research_source.duckdb"),
    )
    parser.add_argument(
        "--output-prefix",
        type=Path,
        default=Path(".tmp/up5in10_dynamic_reentry_combo_validation"),
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=Path(
            f"docs/research/{date.today().isoformat()}-up5in10-dynamic-reentry-combo-validation.md"
        ),
    )
    args = parser.parse_args()

    baseline_selected_rows = load_default_main_rule_selected_rows()
    outputs = run_up5in10_dynamic_reentry_combo_validation(
        source_db_path=args.source_db,
        baseline_selected_rows=baseline_selected_rows,
        output_prefix=args.output_prefix,
    )
    full_summary = pd.read_csv(outputs["full_summary_path"])
    year_summary = pd.read_csv(outputs["year_summary_path"])
    origin_summary = pd.read_csv(outputs["origin_path"])
    write_combo_validation_report(
        output_path=args.report_path,
        full_summary=full_summary,
        year_summary=year_summary,
        origin_summary=origin_summary,
    )


if __name__ == "__main__":
    main()
