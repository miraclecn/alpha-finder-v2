from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from .price_volume_regime_validation import (
    CONTRACT_THRESHOLD,
    EXPAND_THRESHOLD,
    PRICE_DOWN,
    PRICE_UP,
    TURNOVER_BASELINE_DAYS,
)

LOOKBACK_DAYS = 30
FORWARD_DAYS = 10
MIN_HISTORY = LOOKBACK_DAYS + TURNOVER_BASELINE_DAYS


def _prepare_study_tables(
    *,
    conn: duckdb.DuckDBPyConnection,
    entry_start: str,
    entry_end: str,
    query_start: str,
    query_end: str,
) -> None:
    conn.execute(
        f"""
        CREATE TEMP TABLE study_base AS
        WITH raw AS (
            SELECT
                d.security_id,
                d.trade_date,
                d.board,
                coalesce(d.is_st, false) AS is_st,
                coalesce(t.is_suspended, false) AS is_suspended,
                d.close_adj,
                d.high_adj,
                d.turnover_value_cny,
                row_number() OVER (
                    PARTITION BY d.security_id
                    ORDER BY d.trade_date
                ) AS pos,
                d.close_adj / lag(d.close_adj) OVER (
                    PARTITION BY d.security_id
                    ORDER BY d.trade_date
                ) - 1.0 AS daily_ret1,
                median(d.turnover_value_cny) OVER (
                    PARTITION BY d.security_id
                    ORDER BY d.trade_date
                    ROWS BETWEEN {TURNOVER_BASELINE_DAYS} PRECEDING AND 1 PRECEDING
                ) AS turnover_median20
            FROM daily_bar_pit d
            LEFT JOIN tradeability_state_daily t
              ON d.security_id = t.security_id
             AND d.trade_date = t.trade_date
            WHERE d.trade_date BETWEEN ? AND ?
              AND d.price_basis = 'unadjusted'
              AND d.board <> 'beijing'
              AND d.close_adj IS NOT NULL
              AND d.high_adj IS NOT NULL
              AND d.turnover_value_cny IS NOT NULL
              AND d.turnover_value_cny > 0
        )
        SELECT
            *,
            turnover_value_cny / turnover_median20 AS turnover_ratio,
            CASE
                WHEN turnover_median20 IS NULL OR daily_ret1 IS NULL THEN NULL
                WHEN turnover_value_cny / turnover_median20 >= {EXPAND_THRESHOLD}
                    AND daily_ret1 > {PRICE_UP} THEN 'expand_up'
                WHEN turnover_value_cny / turnover_median20 >= {EXPAND_THRESHOLD}
                    AND daily_ret1 < {PRICE_DOWN} THEN 'expand_down'
                WHEN turnover_value_cny / turnover_median20 >= {EXPAND_THRESHOLD} THEN 'expand_flat'
                WHEN turnover_value_cny / turnover_median20 <= {CONTRACT_THRESHOLD}
                    AND daily_ret1 > {PRICE_UP} THEN 'contract_up'
                WHEN turnover_value_cny / turnover_median20 <= {CONTRACT_THRESHOLD}
                    AND daily_ret1 < {PRICE_DOWN} THEN 'contract_down'
                WHEN turnover_value_cny / turnover_median20 <= {CONTRACT_THRESHOLD} THEN 'contract_flat'
                ELSE 'neutral'
            END AS pv_state,
            max(high_adj) OVER (
                PARTITION BY security_id
                ORDER BY trade_date
                ROWS BETWEEN 1 FOLLOWING AND {FORWARD_DAYS} FOLLOWING
            ) AS future_max_high_10
        FROM raw
        """,
        [query_start, query_end],
    )
    conn.execute(
        f"""
        CREATE TEMP TABLE candidate_events AS
        SELECT
            security_id,
            trade_date AS event_trade_date,
            cast(substr(trade_date, 1, 4) AS INTEGER) AS event_year,
            board AS event_board,
            pos AS event_pos,
            close_adj AS event_close_adj,
            future_max_high_10 / close_adj - 1.0 AS future_max_ret10,
            future_max_high_10 >= close_adj * 1.05 AS hit5
        FROM study_base
        WHERE trade_date BETWEEN ? AND ?
          AND pos > {MIN_HISTORY}
          AND NOT is_st
          AND NOT is_suspended
          AND future_max_high_10 IS NOT NULL
        """,
        [entry_start, entry_end],
    )
    conn.execute(
        f"""
        CREATE TEMP TABLE candidate_lookback AS
        SELECT
            c.security_id,
            c.event_trade_date,
            c.event_year,
            c.event_board,
            c.hit5,
            l.pos - c.event_pos AS relative_day,
            l.daily_ret1,
            l.turnover_ratio,
            l.pv_state
        FROM candidate_events c
        JOIN study_base l
          ON c.security_id = l.security_id
         AND l.pos BETWEEN c.event_pos - {LOOKBACK_DAYS} AND c.event_pos - 1
        WHERE l.daily_ret1 IS NOT NULL
          AND l.turnover_ratio IS NOT NULL
          AND l.pv_state IS NOT NULL
        """
    )


def _query_event_summary(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute(
        """
        WITH expanded AS (
            SELECT
                'all' AS scope,
                cast(event_year AS VARCHAR) AS year_group,
                hit5,
                future_max_ret10
            FROM candidate_events
            UNION ALL
            SELECT
                'all' AS scope,
                'all' AS year_group,
                hit5,
                future_max_ret10
            FROM candidate_events
            UNION ALL
            SELECT
                'main_board' AS scope,
                cast(event_year AS VARCHAR) AS year_group,
                hit5,
                future_max_ret10
            FROM candidate_events
            WHERE event_board = 'main_board'
            UNION ALL
            SELECT
                'main_board' AS scope,
                'all' AS year_group,
                hit5,
                future_max_ret10
            FROM candidate_events
            WHERE event_board = 'main_board'
        )
        SELECT
            scope,
            year_group,
            count(*) AS candidate_events,
            sum(CASE WHEN hit5 THEN 1 ELSE 0 END) AS success_events,
            avg(CASE WHEN hit5 THEN 1.0 ELSE 0.0 END) AS success_rate,
            avg(future_max_ret10) AS mean_future_max_ret10
        FROM expanded
        GROUP BY 1, 2
        ORDER BY scope, year_group
        """
    ).fetchdf()


def _query_daily_profile(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute(
        """
        WITH expanded AS (
            SELECT
                'all' AS scope,
                'baseline' AS sample,
                relative_day,
                daily_ret1,
                turnover_ratio,
                pv_state
            FROM candidate_lookback
            UNION ALL
            SELECT
                'all' AS scope,
                'success' AS sample,
                relative_day,
                daily_ret1,
                turnover_ratio,
                pv_state
            FROM candidate_lookback
            WHERE hit5
            UNION ALL
            SELECT
                'main_board' AS scope,
                'baseline' AS sample,
                relative_day,
                daily_ret1,
                turnover_ratio,
                pv_state
            FROM candidate_lookback
            WHERE event_board = 'main_board'
            UNION ALL
            SELECT
                'main_board' AS scope,
                'success' AS sample,
                relative_day,
                daily_ret1,
                turnover_ratio,
                pv_state
            FROM candidate_lookback
            WHERE event_board = 'main_board'
              AND hit5
        )
        SELECT
            scope,
            sample,
            relative_day,
            count(*) AS rows,
            avg(daily_ret1) AS mean_daily_ret,
            avg(turnover_ratio) AS mean_turnover_ratio,
            corr(daily_ret1, turnover_ratio) AS ret_turnover_corr,
            avg(CASE WHEN pv_state = 'expand_up' THEN 1.0 ELSE 0.0 END) AS expand_up_rate,
            avg(CASE WHEN pv_state = 'expand_down' THEN 1.0 ELSE 0.0 END) AS expand_down_rate,
            avg(CASE WHEN pv_state = 'expand_flat' THEN 1.0 ELSE 0.0 END) AS expand_flat_rate,
            avg(CASE WHEN pv_state = 'contract_up' THEN 1.0 ELSE 0.0 END) AS contract_up_rate,
            avg(CASE WHEN pv_state = 'contract_down' THEN 1.0 ELSE 0.0 END) AS contract_down_rate,
            avg(CASE WHEN pv_state = 'contract_flat' THEN 1.0 ELSE 0.0 END) AS contract_flat_rate,
            avg(CASE WHEN pv_state = 'neutral' THEN 1.0 ELSE 0.0 END) AS neutral_rate,
            avg(CASE WHEN daily_ret1 > 0.01 THEN turnover_ratio END) AS up_mean_turnover_ratio,
            avg(CASE WHEN daily_ret1 < -0.01 THEN turnover_ratio END) AS down_mean_turnover_ratio,
            avg(CASE WHEN daily_ret1 BETWEEN -0.01 AND 0.01 THEN turnover_ratio END) AS flat_mean_turnover_ratio
        FROM expanded
        GROUP BY 1, 2, 3
        ORDER BY scope, sample, relative_day
        """
    ).fetchdf()


def _query_transition_profile(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute(
        """
        WITH sequenced AS (
            SELECT
                security_id,
                event_trade_date,
                event_board,
                hit5,
                relative_day,
                pv_state AS from_state,
                lead(pv_state) OVER (
                    PARTITION BY security_id, event_trade_date
                    ORDER BY relative_day
                ) AS to_state
            FROM candidate_lookback
        ),
        expanded AS (
            SELECT
                'all' AS scope,
                'baseline' AS sample,
                relative_day AS start_relative_day,
                from_state,
                to_state
            FROM sequenced
            WHERE to_state IS NOT NULL
            UNION ALL
            SELECT
                'all' AS scope,
                'success' AS sample,
                relative_day AS start_relative_day,
                from_state,
                to_state
            FROM sequenced
            WHERE hit5
              AND to_state IS NOT NULL
            UNION ALL
            SELECT
                'main_board' AS scope,
                'baseline' AS sample,
                relative_day AS start_relative_day,
                from_state,
                to_state
            FROM sequenced
            WHERE event_board = 'main_board'
              AND to_state IS NOT NULL
            UNION ALL
            SELECT
                'main_board' AS scope,
                'success' AS sample,
                relative_day AS start_relative_day,
                from_state,
                to_state
            FROM sequenced
            WHERE event_board = 'main_board'
              AND hit5
              AND to_state IS NOT NULL
        ),
        counts AS (
            SELECT
                scope,
                sample,
                start_relative_day,
                from_state,
                to_state,
                count(*) AS transition_count
            FROM expanded
            GROUP BY 1, 2, 3, 4, 5
        ),
        totals AS (
            SELECT
                scope,
                sample,
                start_relative_day,
                from_state,
                sum(transition_count) AS from_count
            FROM counts
            GROUP BY 1, 2, 3, 4
        )
        SELECT
            c.scope,
            c.sample,
            c.start_relative_day,
            c.from_state,
            c.to_state,
            c.transition_count,
            t.from_count,
            c.transition_count * 1.0 / t.from_count AS transition_rate
        FROM counts c
        JOIN totals t
          ON c.scope = t.scope
         AND c.sample = t.sample
         AND c.start_relative_day = t.start_relative_day
         AND c.from_state = t.from_state
        ORDER BY scope, sample, start_relative_day, from_state, transition_rate DESC, to_state
        """
    ).fetchdf()


def _query_sequence_summary(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute(
        """
        WITH enriched AS (
            SELECT
                security_id,
                event_trade_date,
                event_board,
                hit5,
                relative_day,
                pv_state,
                lag(pv_state, 1) OVER w AS prev1,
                lag(pv_state, 2) OVER w AS prev2,
                lag(pv_state, 3) OVER w AS prev3,
                lead(pv_state, 1) OVER w AS next1,
                lead(pv_state, 2) OVER w AS next2
            FROM candidate_lookback
            WINDOW w AS (
                PARTITION BY security_id, event_trade_date
                ORDER BY relative_day
            )
        ),
        flags AS (
            SELECT
                security_id,
                event_trade_date,
                event_board,
                hit5,
                max(CASE
                    WHEN pv_state = 'expand_up'
                     AND prev1 IN ('contract_up', 'contract_down', 'contract_flat')
                     AND prev2 IN ('contract_up', 'contract_down', 'contract_flat')
                    THEN 1 ELSE 0 END) AS contract_any_run2_then_expand_up,
                max(CASE
                    WHEN pv_state = 'expand_up'
                     AND prev1 IN ('contract_up', 'contract_down', 'contract_flat')
                     AND prev2 IN ('contract_up', 'contract_down', 'contract_flat')
                     AND prev3 IN ('contract_up', 'contract_down', 'contract_flat')
                    THEN 1 ELSE 0 END) AS contract_any_run3_then_expand_up,
                max(CASE
                    WHEN pv_state = 'expand_up'
                     AND prev1 = 'contract_flat'
                     AND prev2 = 'contract_flat'
                    THEN 1 ELSE 0 END) AS contract_flat_run2_then_expand_up,
                max(CASE
                    WHEN pv_state = 'expand_up'
                     AND prev1 = 'contract_flat'
                     AND prev2 = 'contract_flat'
                     AND prev3 = 'contract_flat'
                    THEN 1 ELSE 0 END) AS contract_flat_run3_then_expand_up,
                max(CASE
                    WHEN pv_state = 'expand_up'
                     AND next1 IN ('contract_up', 'contract_down', 'contract_flat')
                     AND next2 IN ('contract_up', 'contract_down', 'contract_flat')
                    THEN 1 ELSE 0 END) AS expand_up_then_contract_any_run2,
                max(CASE
                    WHEN pv_state = 'expand_up'
                     AND next1 = 'contract_flat'
                     AND next2 = 'contract_flat'
                    THEN 1 ELSE 0 END) AS expand_up_then_contract_flat_run2,
                max(CASE
                    WHEN pv_state = 'expand_up'
                     AND prev1 = 'expand_up'
                    THEN 1 ELSE 0 END) AS expand_up_streak2,
                max(CASE
                    WHEN pv_state = 'expand_up'
                     AND prev1 = 'expand_up'
                     AND prev2 = 'expand_up'
                    THEN 1 ELSE 0 END) AS expand_up_streak3,
                max(CASE
                    WHEN pv_state = 'expand_up'
                     AND prev1 = 'contract_down'
                    THEN 1 ELSE 0 END) AS contract_down_then_expand_up,
                max(CASE
                    WHEN pv_state = 'expand_up'
                     AND prev1 = 'expand_down'
                    THEN 1 ELSE 0 END) AS expand_down_then_expand_up
            FROM enriched
            GROUP BY 1, 2, 3, 4
        ),
        expanded AS (
            SELECT
                'all' AS scope,
                'baseline' AS sample,
                contract_any_run2_then_expand_up,
                contract_any_run3_then_expand_up,
                contract_flat_run2_then_expand_up,
                contract_flat_run3_then_expand_up,
                expand_up_then_contract_any_run2,
                expand_up_then_contract_flat_run2,
                expand_up_streak2,
                expand_up_streak3,
                contract_down_then_expand_up,
                expand_down_then_expand_up
            FROM flags
            UNION ALL
            SELECT
                'all' AS scope,
                'success' AS sample,
                contract_any_run2_then_expand_up,
                contract_any_run3_then_expand_up,
                contract_flat_run2_then_expand_up,
                contract_flat_run3_then_expand_up,
                expand_up_then_contract_any_run2,
                expand_up_then_contract_flat_run2,
                expand_up_streak2,
                expand_up_streak3,
                contract_down_then_expand_up,
                expand_down_then_expand_up
            FROM flags
            WHERE hit5
            UNION ALL
            SELECT
                'main_board' AS scope,
                'baseline' AS sample,
                contract_any_run2_then_expand_up,
                contract_any_run3_then_expand_up,
                contract_flat_run2_then_expand_up,
                contract_flat_run3_then_expand_up,
                expand_up_then_contract_any_run2,
                expand_up_then_contract_flat_run2,
                expand_up_streak2,
                expand_up_streak3,
                contract_down_then_expand_up,
                expand_down_then_expand_up
            FROM flags
            WHERE event_board = 'main_board'
            UNION ALL
            SELECT
                'main_board' AS scope,
                'success' AS sample,
                contract_any_run2_then_expand_up,
                contract_any_run3_then_expand_up,
                contract_flat_run2_then_expand_up,
                contract_flat_run3_then_expand_up,
                expand_up_then_contract_any_run2,
                expand_up_then_contract_flat_run2,
                expand_up_streak2,
                expand_up_streak3,
                contract_down_then_expand_up,
                expand_down_then_expand_up
            FROM flags
            WHERE event_board = 'main_board'
              AND hit5
        )
        SELECT
            scope,
            sample,
            count(*) AS events,
            avg(contract_any_run2_then_expand_up) AS contract_any_run2_then_expand_up_rate,
            avg(contract_any_run3_then_expand_up) AS contract_any_run3_then_expand_up_rate,
            avg(contract_flat_run2_then_expand_up) AS contract_flat_run2_then_expand_up_rate,
            avg(contract_flat_run3_then_expand_up) AS contract_flat_run3_then_expand_up_rate,
            avg(expand_up_then_contract_any_run2) AS expand_up_then_contract_any_run2_rate,
            avg(expand_up_then_contract_flat_run2) AS expand_up_then_contract_flat_run2_rate,
            avg(expand_up_streak2) AS expand_up_streak2_rate,
            avg(expand_up_streak3) AS expand_up_streak3_rate,
            avg(contract_down_then_expand_up) AS contract_down_then_expand_up_rate,
            avg(expand_down_then_expand_up) AS expand_down_then_expand_up_rate
        FROM expanded
        GROUP BY 1, 2
        ORDER BY scope, sample
        """
    ).fetchdf()


def build_daily_delta_frame(daily_profile: pd.DataFrame, *, scope: str) -> pd.DataFrame:
    scoped = daily_profile.loc[daily_profile["scope"] == scope].copy()
    baseline = (
        scoped.loc[scoped["sample"] == "baseline"]
        .drop(columns=["scope", "sample"])
        .rename(columns=lambda column: f"{column}_baseline" if column != "relative_day" else column)
    )
    success = (
        scoped.loc[scoped["sample"] == "success"]
        .drop(columns=["scope", "sample"])
        .rename(columns=lambda column: f"{column}_success" if column != "relative_day" else column)
    )
    merged = baseline.merge(success, on="relative_day", how="inner")
    metric_names = [
        "mean_daily_ret",
        "mean_turnover_ratio",
        "ret_turnover_corr",
        "expand_up_rate",
        "expand_down_rate",
        "expand_flat_rate",
        "contract_up_rate",
        "contract_down_rate",
        "contract_flat_rate",
        "neutral_rate",
        "up_mean_turnover_ratio",
        "down_mean_turnover_ratio",
        "flat_mean_turnover_ratio",
    ]
    for metric_name in metric_names:
        merged[f"{metric_name}_delta"] = (
            merged[f"{metric_name}_success"] - merged[f"{metric_name}_baseline"]
        )
    return merged.sort_values("relative_day").reset_index(drop=True)


def build_phase_profile(daily_profile: pd.DataFrame, *, scope: str) -> pd.DataFrame:
    daily_delta = build_daily_delta_frame(daily_profile, scope=scope)
    working = daily_delta.copy()
    working["phase"] = "launch_pad"
    working.loc[working["relative_day"] <= -21, "phase"] = "setup"
    working.loc[
        (working["relative_day"] >= -20) & (working["relative_day"] <= -11),
        "phase",
    ] = "mid"
    return (
        working.groupby("phase", as_index=False)
        .agg(
            mean_daily_ret_delta=("mean_daily_ret_delta", "mean"),
            mean_turnover_ratio_delta=("mean_turnover_ratio_delta", "mean"),
            ret_turnover_corr_delta=("ret_turnover_corr_delta", "mean"),
            expand_up_rate_delta=("expand_up_rate_delta", "mean"),
            expand_down_rate_delta=("expand_down_rate_delta", "mean"),
            contract_flat_rate_delta=("contract_flat_rate_delta", "mean"),
            up_mean_turnover_ratio_delta=("up_mean_turnover_ratio_delta", "mean"),
            down_mean_turnover_ratio_delta=("down_mean_turnover_ratio_delta", "mean"),
        )
        .reset_index(drop=True)
    )


def build_transition_delta_frame(transition_profile: pd.DataFrame, *, scope: str) -> pd.DataFrame:
    scoped = transition_profile.loc[transition_profile["scope"] == scope].copy()
    aggregated = (
        scoped.groupby(["sample", "from_state", "to_state"], as_index=False)
        .agg(
            transition_count=("transition_count", "sum"),
            from_count=("from_count", "sum"),
        )
        .assign(transition_rate=lambda frame: frame["transition_count"] / frame["from_count"])
    )
    baseline = (
        aggregated.loc[aggregated["sample"] == "baseline"]
        .drop(columns=["sample"])
        .rename(
            columns={
                "transition_count": "baseline_transition_count",
                "from_count": "baseline_from_count",
                "transition_rate": "baseline_transition_rate",
            }
        )
    )
    success = (
        aggregated.loc[aggregated["sample"] == "success"]
        .drop(columns=["sample"])
        .rename(
            columns={
                "transition_count": "success_transition_count",
                "from_count": "success_from_count",
                "transition_rate": "success_transition_rate",
            }
        )
    )
    merged = baseline.merge(success, on=["from_state", "to_state"], how="outer").fillna(0.0)
    merged["transition_rate_delta"] = (
        merged["success_transition_rate"] - merged["baseline_transition_rate"]
    )
    return merged.sort_values(
        ["transition_rate_delta", "success_transition_count"],
        ascending=[False, False],
    ).reset_index(drop=True)


def build_sequence_delta_frame(sequence_summary: pd.DataFrame, *, scope: str) -> pd.DataFrame:
    scoped = sequence_summary.loc[sequence_summary["scope"] == scope].copy()
    baseline = (
        scoped.loc[scoped["sample"] == "baseline"]
        .drop(columns=["scope", "sample"])
        .reset_index(drop=True)
    )
    success = (
        scoped.loc[scoped["sample"] == "success"]
        .drop(columns=["scope", "sample"])
        .reset_index(drop=True)
    )
    if baseline.empty or success.empty:
        return pd.DataFrame()
    row = {"scope": scope}
    for column in baseline.columns:
        base_value = float(baseline.loc[0, column])
        success_value = float(success.loc[0, column])
        row[f"{column}_baseline"] = base_value
        row[f"{column}_success"] = success_value
        if column != "events":
            row[f"{column}_delta"] = success_value - base_value
    return pd.DataFrame([row])


def _markdown_table(frame: pd.DataFrame, *, float_digits: int = 4) -> str:
    if frame.empty:
        return "_empty_"
    display = frame.copy()
    for column in display.columns:
        if pd.api.types.is_float_dtype(display[column]):
            display[column] = display[column].map(lambda value: f"{value:.{float_digits}f}")
    header = "| " + " | ".join(str(column) for column in display.columns) + " |"
    divider = "| " + " | ".join(["---"] * len(display.columns)) + " |"
    rows = [header, divider]
    for row in display.itertuples(index=False, name=None):
        cells: list[str] = []
        for value in row:
            if pd.isna(value):
                cells.append("")
            else:
                cells.append(str(value))
        rows.append("| " + " | ".join(cells) + " |")
    return "\n".join(rows)


def write_summary_report(
    *,
    report_markdown_path: Path,
    event_summary: pd.DataFrame,
    daily_profile: pd.DataFrame,
    transition_profile: pd.DataFrame,
    sequence_summary: pd.DataFrame,
    entry_start: str,
    entry_end: str,
) -> None:
    all_phase = build_phase_profile(daily_profile, scope="all")
    all_daily_delta = build_daily_delta_frame(daily_profile, scope="all")
    all_transition_delta = build_transition_delta_frame(transition_profile, scope="all").head(10)
    all_sequence_delta = build_sequence_delta_frame(sequence_summary, scope="all")
    top_turnover_days = (
        all_daily_delta.sort_values("mean_turnover_ratio_delta", ascending=False)
        .loc[:, ["relative_day", "mean_turnover_ratio_delta", "expand_up_rate_delta", "ret_turnover_corr_delta"]]
        .head(8)
        .reset_index(drop=True)
    )
    report_lines = [
        f"# 10-Day +5% Price-Volume Study - {date.today().isoformat()}",
        "",
        "## Study Setup",
        f"- Entry window: `{entry_start}` to `{entry_end}`",
        f"- Success event: future `{FORWARD_DAYS}` trading days reach at least `+5%` by adjusted high.",
        f"- Lookback window: `t-{LOOKBACK_DAYS}` to `t-1`.",
        "- Scope: A-share daily bars excluding Beijing, event day not ST and not suspended.",
        "",
        "## Event Summary",
        _markdown_table(event_summary),
        "",
        "## Phase Delta (success minus baseline)",
        _markdown_table(all_phase),
        "",
        "## Top Relative Days By Turnover Delta",
        _markdown_table(top_turnover_days),
        "",
        "## Transition Delta (success minus baseline)",
        _markdown_table(all_transition_delta),
        "",
        "## Sequence Delta",
        _markdown_table(all_sequence_delta),
    ]
    report_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    report_markdown_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")


def run_up5in10_price_volume_study(
    *,
    source_db_path: Path,
    event_summary_csv_path: Path,
    daily_profile_csv_path: Path,
    transition_profile_csv_path: Path,
    sequence_summary_csv_path: Path,
    report_markdown_path: Path,
    entry_start: str = "20220101",
    entry_end: str = "20251231",
    query_start: str = "20210101",
    query_end: str = "20251231",
) -> dict[str, pd.DataFrame]:
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        _prepare_study_tables(
            conn=conn,
            entry_start=entry_start,
            entry_end=entry_end,
            query_start=query_start,
            query_end=query_end,
        )
        event_summary = _query_event_summary(conn)
        daily_profile = _query_daily_profile(conn)
        transition_profile = _query_transition_profile(conn)
        sequence_summary = _query_sequence_summary(conn)
    finally:
        conn.close()

    event_summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
    daily_profile_csv_path.parent.mkdir(parents=True, exist_ok=True)
    transition_profile_csv_path.parent.mkdir(parents=True, exist_ok=True)
    sequence_summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
    event_summary.to_csv(event_summary_csv_path, index=False)
    daily_profile.to_csv(daily_profile_csv_path, index=False)
    transition_profile.to_csv(transition_profile_csv_path, index=False)
    sequence_summary.to_csv(sequence_summary_csv_path, index=False)
    write_summary_report(
        report_markdown_path=report_markdown_path,
        event_summary=event_summary,
        daily_profile=daily_profile,
        transition_profile=transition_profile,
        sequence_summary=sequence_summary,
        entry_start=entry_start,
        entry_end=entry_end,
    )
    return {
        "event_summary": event_summary,
        "daily_profile": daily_profile,
        "transition_profile": transition_profile,
        "sequence_summary": sequence_summary,
    }


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the up5-in-10 pre-30-day price-volume study.")
    parser.add_argument("--source-db", required=True, type=Path)
    parser.add_argument("--event-summary-csv", required=True, type=Path)
    parser.add_argument("--daily-profile-csv", required=True, type=Path)
    parser.add_argument("--transition-profile-csv", required=True, type=Path)
    parser.add_argument("--sequence-summary-csv", required=True, type=Path)
    parser.add_argument("--report-markdown", required=True, type=Path)
    parser.add_argument("--entry-start", default="20220101")
    parser.add_argument("--entry-end", default="20251231")
    parser.add_argument("--query-start", default="20210101")
    parser.add_argument("--query-end", default="20251231")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_up5in10_price_volume_study(
        source_db_path=args.source_db,
        event_summary_csv_path=args.event_summary_csv,
        daily_profile_csv_path=args.daily_profile_csv,
        transition_profile_csv_path=args.transition_profile_csv,
        sequence_summary_csv_path=args.sequence_summary_csv,
        report_markdown_path=args.report_markdown,
        entry_start=str(args.entry_start),
        entry_end=str(args.entry_end),
        query_start=str(args.query_start),
        query_end=str(args.query_end),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
