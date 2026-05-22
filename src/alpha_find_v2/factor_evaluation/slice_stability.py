"""
Slice stability: IC by industry and by size tertile.
"""
from __future__ import annotations

import math
from typing import Any

import pandas as pd

from .descriptor_evaluator import ICStats, SliceRow, SliceStability, _compute_per_date_ic, _ic_stats


def compute_slice_stability(
    descriptor_panel: pd.DataFrame,
    fwd_df: pd.DataFrame | None,
    conn: Any,
) -> SliceStability:
    """
    Compute IC grouped by SW2021 L1 industry and by market-cap size tertile.

    Args:
        descriptor_panel: (trade_date, security_id, descriptor_value)
        fwd_df: forward-return DataFrame for the primary horizon
                (trade_date, security_id, forward_return)
        conn: research_source.duckdb connection

    Returns: SliceStability with by_industry and by_size_tertile lists.
    """
    if fwd_df is None or fwd_df.empty or descriptor_panel.empty:
        return SliceStability(by_industry=[], by_size_tertile=[])

    fwd_df = fwd_df.copy()
    if "forward_return" not in fwd_df.columns:
        # handle renamed column
        ret_cols = [c for c in fwd_df.columns if c.startswith("fwd_")]
        if not ret_cols:
            return SliceStability(by_industry=[], by_size_tertile=[])
        fwd_df = fwd_df.rename(columns={ret_cols[0]: "forward_return"})

    # Merge descriptor + forward returns
    panel = descriptor_panel.merge(
        fwd_df[["security_id", "trade_date", "forward_return"]],
        on=["security_id", "trade_date"],
        how="inner",
    )

    # ------------------------------------------------------------------
    # Industry slice
    # ------------------------------------------------------------------
    industry_rows: list[SliceRow] = []
    try:
        ind_map = conn.execute(
            """
            SELECT security_id, industry_code
            FROM industry_classification_pit
            WHERE industry_schema = 'sw2021_l1'
              AND removed_at IS NULL
            """
        ).df()
        if not ind_map.empty:
            panel_ind = panel.merge(ind_map, on="security_id", how="left")
            for ind_code, grp in panel_ind.groupby("industry_code"):
                if len(grp) < 4:
                    continue
                ic_p = _ic_stats(_compute_per_date_ic(grp, "forward_return", "pearson"))
                ic_s = _ic_stats(_compute_per_date_ic(grp, "forward_return", "spearman"))
                industry_rows.append(SliceRow(
                    slice_value=str(ind_code),
                    ic_pearson_mean=ic_p.mean,
                    ic_spearman_mean=ic_s.mean,
                    n=ic_p.n,
                ))
    except Exception:
        pass

    # ------------------------------------------------------------------
    # Size tertile slice
    # ------------------------------------------------------------------
    size_rows: list[SliceRow] = []
    try:
        # market cap proxy: float_mcap_cny from daily_bar_pit
        mcap = conn.execute(
            "SELECT security_id, trade_date, float_mcap_cny FROM daily_bar_pit"
        ).df()
        if not mcap.empty:
            panel_sz = panel.merge(mcap, on=["security_id", "trade_date"], how="left")
            # Assign size tertile per trade_date using explicit loop for pandas 3.x compat
            panel_sz = panel_sz.copy()
            panel_sz["size_tertile"] = "unknown"
            for td, grp in panel_sz.groupby("trade_date"):
                try:
                    labels = pd.qcut(grp["float_mcap_cny"], q=3, labels=["low", "mid", "high"], duplicates="drop")
                    panel_sz.loc[grp.index, "size_tertile"] = labels.astype(str)
                except Exception:
                    pass
            for tertile, grp in panel_sz.groupby("size_tertile"):
                if len(grp) < 4:
                    continue
                ic_p = _ic_stats(_compute_per_date_ic(grp, "forward_return", "pearson"))
                ic_s = _ic_stats(_compute_per_date_ic(grp, "forward_return", "spearman"))
                size_rows.append(SliceRow(
                    slice_value=str(tertile),
                    ic_pearson_mean=ic_p.mean,
                    ic_spearman_mean=ic_s.mean,
                    n=ic_p.n,
                ))
    except Exception:
        pass

    return SliceStability(by_industry=industry_rows, by_size_tertile=size_rows)
