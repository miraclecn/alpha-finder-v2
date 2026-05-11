from __future__ import annotations

import argparse
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

EVENT_OUTPUT_COLUMNS = [
    "variant_name",
    "confirm_days",
    "security_id",
    "signal_date",
    "start_high",
    "start_date",
    "trough_date",
    "buy_date",
    "candidate_entry_date",
    "confirmation_pass",
    "entry_open",
    "close_ret30",
    "max_ret30",
    "min_ret30",
    "up10",
    "up20",
    "up30",
    "loss10",
    "first_hit",
]


def _to_frame(rows: pd.DataFrame | list[dict[str, Any]]) -> pd.DataFrame:
    if isinstance(rows, pd.DataFrame):
        return rows.copy()
    return pd.DataFrame(rows)


def _empty_forward_stats() -> dict[str, Any]:
    return {
        "close_ret30": float("nan"),
        "max_ret30": float("nan"),
        "min_ret30": float("nan"),
        "up10": False,
        "up20": False,
        "up30": False,
        "loss10": False,
        "first_hit": "unresolved",
    }


def _compute_first_hit(window: pd.DataFrame, entry_open: float) -> str:
    up10_level = entry_open * 1.10
    dn10_level = entry_open * 0.90
    first_up_idx: int | None = None
    first_dn_idx: int | None = None

    for idx, row in window.reset_index(drop=True).iterrows():
        up_hit = float(row["high_adj"]) >= up10_level
        dn_hit = float(row["low_adj"]) <= dn10_level
        if up_hit and dn_hit:
            return "both_same_day"
        if up_hit and first_up_idx is None:
            first_up_idx = idx
        if dn_hit and first_dn_idx is None:
            first_dn_idx = idx

    if first_up_idx is None and first_dn_idx is None:
        return "unresolved"
    if first_up_idx is None:
        return "dn10_first"
    if first_dn_idx is None:
        return "up10_first"
    if first_up_idx < first_dn_idx:
        return "up10_first"
    if first_dn_idx < first_up_idx:
        return "dn10_first"
    return "both_same_day"


def _forward_stats(bars: pd.DataFrame, entry_index: int, entry_open: float) -> dict[str, Any]:
    window = bars.iloc[entry_index : min(len(bars), entry_index + 31)]
    if window.empty:
        return _empty_forward_stats()

    close_index = entry_index + 30
    close_ret30 = float("nan")
    if close_index < len(bars):
        close30 = float(bars.iloc[close_index]["close_adj"])
        close_ret30 = (close30 - entry_open) / entry_open

    max_ret30 = (float(window["high_adj"].max()) - entry_open) / entry_open
    min_ret30 = (float(window["low_adj"].min()) - entry_open) / entry_open

    return {
        "close_ret30": close_ret30,
        "max_ret30": max_ret30,
        "min_ret30": min_ret30,
        "up10": max_ret30 >= 0.10,
        "up20": max_ret30 >= 0.20,
        "up30": max_ret30 >= 0.30,
        "loss10": min_ret30 <= -0.10,
        "first_hit": _compute_first_hit(window, entry_open),
    }


def build_confirmation_variant(
    events: pd.DataFrame | list[dict[str, Any]],
    bars_by_security: Mapping[str, pd.DataFrame],
    *,
    variant_name: str,
    confirm_days: int,
) -> pd.DataFrame:
    event_frame = _to_frame(events)
    rows: list[dict[str, Any]] = []

    for event in event_frame.to_dict(orient="records"):
        security_id = str(event["security_id"])
        signal_date = str(event["signal_date"])
        start_high = float(event["start_high"])

        candidate_entry_date: str | None = None
        confirmation_pass = False
        entry_open = float("nan")
        stats = _empty_forward_stats()

        bars = bars_by_security.get(security_id)
        if bars is not None and not bars.empty:
            bars = bars.sort_values("trade_date").reset_index(drop=True)
            signal_matches = bars.index[bars["trade_date"].astype(str) == signal_date]
            if len(signal_matches) > 0:
                signal_index = int(signal_matches[0])
                observation_start = signal_index + 1
                observation_end = observation_start + confirm_days
                observation = bars.iloc[observation_start:observation_end]

                candidate_index = signal_index + confirm_days + 1
                if candidate_index < len(bars):
                    candidate_entry_date = str(bars.iloc[candidate_index]["trade_date"])

                if confirm_days == 0:
                    confirmation_pass = candidate_entry_date is not None
                elif candidate_entry_date is not None and len(observation) == confirm_days:
                    confirmation_pass = bool((observation["low_adj"] > start_high).all())

                if confirmation_pass and candidate_entry_date is not None:
                    entry_open = float(bars.iloc[candidate_index]["open_adj"])
                    stats = _forward_stats(bars, candidate_index, entry_open)

        row = {
            "variant_name": variant_name,
            "confirm_days": confirm_days,
            "security_id": security_id,
            "signal_date": signal_date,
            "start_high": start_high,
            "start_date": event.get("start_date"),
            "trough_date": event.get("trough_date"),
            "buy_date": event.get("buy_date"),
            "candidate_entry_date": candidate_entry_date,
            "confirmation_pass": confirmation_pass,
            "entry_open": entry_open,
        }
        row.update(stats)
        rows.append(row)

    return pd.DataFrame(rows, columns=EVENT_OUTPUT_COLUMNS)


def summarize_variant_years(variant_rows: pd.DataFrame | list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = _to_frame(variant_rows)
    if frame.empty:
        return (
            pd.DataFrame(
                columns=[
                    "variant_name",
                    "year",
                    "candidate_rows",
                    "events",
                    "confirmation_pass_rate",
                ]
            ),
            pd.DataFrame(columns=["variant_name", "year", "events", "signal_days", "avg_per_day"]),
        )

    working = frame.copy()
    working["signal_date"] = working["signal_date"].astype(str)
    working["year"] = working["signal_date"].str.slice(0, 4).astype(int)
    working["candidate_row"] = working["candidate_entry_date"].notna() & (working["candidate_entry_date"] != "")
    working["pass_row"] = working["confirmation_pass"].fillna(False).astype(bool)

    summary_rows: list[dict[str, Any]] = []
    density_rows: list[dict[str, Any]] = []

    grouped = working.groupby(["variant_name", "year"], sort=True, dropna=False)
    for (variant_name, year), group in grouped:
        year_candidates = int(group["candidate_row"].sum())
        year_pass_events = int((group["candidate_row"] & group["pass_row"]).sum())
        pass_rate = 0.0
        if year_candidates > 0:
            pass_rate = year_pass_events / year_candidates

        summary_rows.append(
            {
                "variant_name": variant_name,
                "year": int(year),
                "candidate_rows": year_candidates,
                "events": year_pass_events,
                "confirmation_pass_rate": pass_rate,
            }
        )

        passed = group.loc[group["candidate_row"] & group["pass_row"]]
        signal_days = int(passed["signal_date"].nunique())
        avg_per_day = 0.0
        if signal_days > 0:
            avg_per_day = year_pass_events / signal_days
        density_rows.append(
            {
                "variant_name": variant_name,
                "year": int(year),
                "events": year_pass_events,
                "signal_days": signal_days,
                "avg_per_day": avg_per_day,
            }
        )

    return pd.DataFrame(summary_rows), pd.DataFrame(density_rows)


def load_first_break_events(events_csv_path: Path) -> pd.DataFrame:
    events = pd.read_csv(events_csv_path)
    required_columns = {"security_id", "signal_date", "start_high"}
    missing = sorted(required_columns - set(events.columns))
    if missing:
        missing_text = ", ".join(missing)
        raise ValueError(f"events CSV missing required columns: {missing_text}")

    ordered = events.copy()
    ordered["security_id"] = ordered["security_id"].astype(str)
    ordered["signal_date"] = ordered["signal_date"].astype(str)
    return ordered.sort_values(["security_id", "signal_date"]).reset_index(drop=True)


def load_bar_history(
    source_db_path: Path,
    security_ids: list[str],
    min_signal_date: str,
) -> dict[str, pd.DataFrame]:
    if not security_ids:
        return {}

    import duckdb

    placeholders = ", ".join(["?"] * len(security_ids))
    sql = f"""
        SELECT
            security_id,
            trade_date,
            open_adj,
            high_adj,
            low_adj,
            close_adj
        FROM daily_bar_pit
        WHERE security_id IN ({placeholders})
          AND trade_date >= ?
          AND exchange IN ('SH', 'SZ')
          AND board = 'main_board'
          AND coalesce(is_st, false) = false
        ORDER BY security_id, trade_date
    """
    params: list[Any] = [*security_ids, str(min_signal_date)]

    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        frame = conn.execute(sql, params).fetchdf()
    finally:
        conn.close()

    if frame.empty:
        return {}

    grouped: dict[str, pd.DataFrame] = {}
    for security_id, group in frame.groupby("security_id", sort=True, dropna=False):
        grouped[str(security_id)] = group.reset_index(drop=True)
    return grouped


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
    events_csv_path: Path,
    source_db_path: Path,
    summary: pd.DataFrame,
    density: pd.DataFrame,
) -> None:
    lines = [
        "# V Shape First Break Confirmation Study - 2026-05-12",
        "",
        "## Inputs",
        f"- events_csv: `{events_csv_path}`",
        f"- source_db: `{source_db_path}`",
        "",
        "## Summary",
        _markdown_table(summary),
        "",
        "## Signal Density",
        _markdown_table(density),
        "",
        "## Judgment",
    ]

    if summary.empty:
        lines.append("- No rows available for judgment.")
    else:
        ranked = (
            summary.groupby("variant_name", as_index=False)["confirmation_pass_rate"]
            .mean()
            .sort_values(["confirmation_pass_rate", "variant_name"], ascending=[False, True])
            .reset_index(drop=True)
        )
        best = ranked.iloc[0]
        lines.append(
            "- Best average confirmation pass rate: "
            f"`{best['variant_name']}` at {float(best['confirmation_pass_rate']):.2%}."
        )

    report_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    report_markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_first_break_confirmation_study(
    *,
    events_csv_path: Path,
    source_db_path: Path,
    summary_csv_path: Path,
    density_csv_path: Path,
    events_output_csv_path: Path,
    report_markdown_path: Path | None = None,
) -> dict[str, pd.DataFrame]:
    events = load_first_break_events(events_csv_path)
    security_ids = sorted(events["security_id"].astype(str).unique().tolist())
    min_signal_date = ""
    if not events.empty:
        min_signal_date = str(events["signal_date"].min())

    bars_by_security = load_bar_history(source_db_path, security_ids, min_signal_date)

    variants = [
        ("baseline_first_break", 0),
        ("confirm_2d", 2),
        ("confirm_3d", 3),
    ]
    variant_frames = [
        build_confirmation_variant(events, bars_by_security, variant_name=name, confirm_days=days)
        for name, days in variants
    ]
    combined = pd.concat(variant_frames, ignore_index=True)

    summary, density = summarize_variant_years(combined)
    summary = summary.sort_values(["variant_name", "year"]).reset_index(drop=True)
    density = density.sort_values(["variant_name", "year"]).reset_index(drop=True)
    combined = combined.sort_values(["variant_name", "security_id", "signal_date"]).reset_index(drop=True)

    summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
    density_csv_path.parent.mkdir(parents=True, exist_ok=True)
    events_output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_csv_path, index=False)
    density.to_csv(density_csv_path, index=False)
    combined.to_csv(events_output_csv_path, index=False)

    if report_markdown_path is not None:
        write_markdown_report(
            report_markdown_path=report_markdown_path,
            events_csv_path=events_csv_path,
            source_db_path=source_db_path,
            summary=summary,
            density=density,
        )

    return {"summary": summary, "density": density, "events": combined}


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run V-shape first-break confirmation variants.")
    parser.add_argument("--events-csv", required=True, type=Path)
    parser.add_argument("--source-db", required=True, type=Path)
    parser.add_argument("--summary-csv", required=True, type=Path)
    parser.add_argument("--density-csv", required=True, type=Path)
    parser.add_argument("--events-output-csv", required=True, type=Path)
    parser.add_argument("--report-markdown", required=False, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_first_break_confirmation_study(
        events_csv_path=args.events_csv,
        source_db_path=args.source_db,
        summary_csv_path=args.summary_csv,
        density_csv_path=args.density_csv,
        events_output_csv_path=args.events_output_csv,
        report_markdown_path=args.report_markdown,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
