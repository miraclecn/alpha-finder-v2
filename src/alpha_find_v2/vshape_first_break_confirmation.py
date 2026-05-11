from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd


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

    return pd.DataFrame(rows)


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

    variant_name = str(frame["variant_name"].iloc[0])
    working = frame.copy()
    working["signal_date"] = working["signal_date"].astype(str)
    working["year"] = working["signal_date"].str.slice(0, 4).astype(int)
    candidate_mask = working["candidate_entry_date"].notna() & (working["candidate_entry_date"] != "")
    pass_mask = working["confirmation_pass"].fillna(False).astype(bool)

    summary_rows: list[dict[str, Any]] = []
    density_rows: list[dict[str, Any]] = []

    for year in sorted(working["year"].unique()):
        year_mask = working["year"] == year
        year_candidates = int((candidate_mask & year_mask).sum())
        year_pass_events = int((candidate_mask & pass_mask & year_mask).sum())
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

        passed = working.loc[candidate_mask & pass_mask & year_mask]
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
