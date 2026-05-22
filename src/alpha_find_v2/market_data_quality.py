from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import duckdb


UNRESOLVED_ADJ_FACTOR_EXAMPLE_LIMIT = 500
QUALITY_TABLES = ("daily_bar_pit", "corporate_action_ledger", "tradeability_state_daily")


@dataclass(frozen=True)
class MarketDataQualitySummary:
    daily_bar_rows: int
    qfq_fallback_rows: int
    missing_price_rows: int
    zero_or_missing_adj_factor_rows: int
    corporate_action_rows: int
    tradeability_rows: int
    tradeability_official_rows: int
    tradeability_ohlc_fallback_rows: int
    adj_factor_jump_assessable: bool
    missing_quality_tables: tuple[str, ...]
    promotion_blocking_quality_state: str
    adj_factor_jump_rows: int
    explained_adj_factor_jump_rows: int
    unresolved_adj_factor_jump_rows: int
    promotion_blocking_unresolved_adj_factor_jump_rows: int
    unresolved_adj_factor_jump_years: tuple[dict[str, Any], ...]
    unresolved_adj_factor_jump_magnitude_buckets: tuple[dict[str, Any], ...]
    unresolved_adj_factor_jump_top_securities: tuple[dict[str, Any], ...]
    unresolved_adj_factor_jump_dividend_proximity: tuple[dict[str, Any], ...]
    unresolved_adj_factor_jump_triage: tuple[dict[str, Any], ...]
    unresolved_adj_factor_jump_examples: tuple[dict[str, Any], ...]


def summarize_market_data_quality(db_path: Path | str) -> MarketDataQualitySummary:
    source_path = Path(db_path).expanduser().resolve()
    conn = duckdb.connect(str(source_path), read_only=True)
    try:
        missing_quality_tables = tuple(
            table for table in QUALITY_TABLES if not _table_exists(conn, table)
        )
        adj_factor_jump_assessable = _adj_factor_jump_assessable(conn)
        unresolved_jump_breakdown = _unresolved_adj_factor_jump_breakdown(conn)
        return MarketDataQualitySummary(
            daily_bar_rows=_count(conn, "daily_bar_pit"),
            qfq_fallback_rows=_qfq_fallback_rows(conn),
            missing_price_rows=_missing_price_rows(conn),
            zero_or_missing_adj_factor_rows=_zero_or_missing_adj_factor_rows(conn),
            corporate_action_rows=_count(conn, "corporate_action_ledger"),
            tradeability_rows=_count(conn, "tradeability_state_daily"),
            tradeability_official_rows=_tradeability_source_rows(conn, "official"),
            tradeability_ohlc_fallback_rows=_tradeability_source_rows(
                conn,
                "ohlc_fallback",
            ),
            adj_factor_jump_assessable=adj_factor_jump_assessable,
            missing_quality_tables=missing_quality_tables,
            promotion_blocking_quality_state=_promotion_blocking_quality_state(
                missing_quality_tables=missing_quality_tables,
                promotion_blocking_unresolved_adj_factor_jump_rows=unresolved_jump_breakdown[
                    "promotion_blocking_rows"
                ],
            ),
            adj_factor_jump_rows=unresolved_jump_breakdown["total_rows"],
            explained_adj_factor_jump_rows=unresolved_jump_breakdown["explained_rows"],
            unresolved_adj_factor_jump_rows=unresolved_jump_breakdown["rows"],
            promotion_blocking_unresolved_adj_factor_jump_rows=unresolved_jump_breakdown[
                "promotion_blocking_rows"
            ],
            unresolved_adj_factor_jump_years=unresolved_jump_breakdown["years"],
            unresolved_adj_factor_jump_magnitude_buckets=unresolved_jump_breakdown[
                "magnitude_buckets"
            ],
            unresolved_adj_factor_jump_top_securities=unresolved_jump_breakdown[
                "top_securities"
            ],
            unresolved_adj_factor_jump_dividend_proximity=unresolved_jump_breakdown[
                "dividend_proximity"
            ],
            unresolved_adj_factor_jump_triage=unresolved_jump_breakdown["triage"],
            unresolved_adj_factor_jump_examples=unresolved_jump_breakdown["examples"],
        )
    finally:
        conn.close()


def _promotion_blocking_quality_state(
    *,
    missing_quality_tables: tuple[str, ...],
    promotion_blocking_unresolved_adj_factor_jump_rows: int,
) -> str:
    if missing_quality_tables:
        return "blocked_unassessable"
    if promotion_blocking_unresolved_adj_factor_jump_rows > 0:
        return "blocked_unresolved_adj_factor_jumps"
    return "pass"


def _adj_factor_jump_assessable(conn: duckdb.DuckDBPyConnection) -> bool:
    daily_columns = {
        "security_id",
        "trade_date",
        "price_basis",
        "close",
        "adj_factor",
    }
    if not _has_columns(conn, "daily_bar_pit", daily_columns):
        return False
    if not _has_columns(conn, "corporate_action_ledger", {"security_id"}):
        return False
    return bool(_corporate_action_window_predicate(conn))


def write_market_data_quality_audit(
    *,
    source_db: Path | str,
    output_path: Path | str,
) -> dict[str, Any]:
    source_path = Path(source_db).expanduser().resolve()
    target_path = Path(output_path).expanduser()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    summary = summarize_market_data_quality(source_path)
    payload = {
        "artifact_type": "market_data_quality_audit",
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_db": str(source_path),
        "summary": asdict(summary),
    }
    target_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        "output_path": str(target_path),
        "source_db": str(source_path),
        "summary": asdict(summary),
    }


def _qfq_fallback_rows(conn: duckdb.DuckDBPyConnection) -> int:
    if not _has_columns(conn, "daily_bar_pit", {"price_basis"}):
        return 0
    return _scalar(
        conn,
        "SELECT count(*) FROM daily_bar_pit WHERE price_basis = 'qfq_fallback'",
    )


def _missing_price_rows(conn: duckdb.DuckDBPyConnection) -> int:
    price_columns = {"open", "high", "low", "close"}
    if not _has_columns(conn, "daily_bar_pit", price_columns):
        return 0
    return _scalar(
        conn,
        """
        SELECT count(*)
        FROM daily_bar_pit
        WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
        """,
    )


def _zero_or_missing_adj_factor_rows(conn: duckdb.DuckDBPyConnection) -> int:
    if not _has_columns(conn, "daily_bar_pit", {"adj_factor"}):
        return 0
    return _scalar(
        conn,
        """
        SELECT count(*)
        FROM daily_bar_pit
        WHERE adj_factor IS NULL OR adj_factor <= 0.0
        """,
    )


def _tradeability_source_rows(
    conn: duckdb.DuckDBPyConnection,
    source_priority: str,
) -> int:
    if not _has_columns(conn, "tradeability_state_daily", {"source_priority"}):
        return 0
    return _scalar(
        conn,
        "SELECT count(*) FROM tradeability_state_daily WHERE source_priority = ?",
        [source_priority],
    )


def _unresolved_adj_factor_jump_rows(conn: duckdb.DuckDBPyConnection) -> int:
    return _unresolved_adj_factor_jump_breakdown(conn)["rows"]


def _unresolved_adj_factor_jump_breakdown(
    conn: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    empty = {
        "total_rows": 0,
        "explained_rows": 0,
        "rows": 0,
        "promotion_blocking_rows": 0,
        "years": (),
        "magnitude_buckets": (),
        "top_securities": (),
        "dividend_proximity": (),
        "triage": (),
        "examples": (),
    }
    daily_columns = {
        "security_id",
        "trade_date",
        "price_basis",
        "close",
        "adj_factor",
    }
    if not _has_columns(conn, "daily_bar_pit", daily_columns):
        return empty
    if not _has_columns(conn, "corporate_action_ledger", {"security_id"}):
        return empty

    action_window_predicate = _corporate_action_window_predicate(conn)
    if not action_window_predicate:
        return empty
    pre_close_select_sql = (
        "pre_close"
        if _has_columns(conn, "daily_bar_pit", {"pre_close"})
        else "NULL AS pre_close"
    )

    classified_cte = f"""
        WITH jumps AS (
            SELECT
                security_id,
                {_date_key_sql("trade_date")} AS trade_date_key,
                lag({_date_key_sql("trade_date")}) OVER (
                    PARTITION BY security_id
                    ORDER BY {_date_key_sql("trade_date")}
                ) AS previous_trade_date_key,
                close,
                lag(close) OVER (
                    PARTITION BY security_id
                    ORDER BY {_date_key_sql("trade_date")}
                ) AS previous_close,
                {pre_close_select_sql},
                adj_factor / NULLIF(
                    lag(adj_factor) OVER (
                        PARTITION BY security_id
                        ORDER BY {_date_key_sql("trade_date")}
                    ),
                    0.0
                ) AS factor_ratio
            FROM daily_bar_pit
            WHERE COALESCE(price_basis, 'unadjusted') = 'unadjusted'
              AND adj_factor IS NOT NULL
              AND adj_factor > 0.0
        ),
        significant_jumps AS (
            SELECT *
            FROM jumps
            WHERE previous_trade_date_key IS NOT NULL
              AND factor_ratio IS NOT NULL
              AND abs(factor_ratio - 1.0) > 0.001
        ),
        classified_jumps AS (
            SELECT
                j.*,
                EXISTS (
                    SELECT 1
                    FROM corporate_action_ledger AS c
                    WHERE c.security_id = j.security_id
                      AND ({action_window_predicate})
                ) AS has_explaining_corporate_action
            FROM significant_jumps AS j
        ),
        unresolved AS (
            SELECT
                security_id,
                previous_trade_date_key,
                trade_date_key,
                previous_close,
                pre_close,
                factor_ratio
            FROM classified_jumps
            WHERE NOT has_explaining_corporate_action
        )
        """

    total_rows = _scalar(conn, f"{classified_cte} SELECT count(*) FROM classified_jumps")
    if total_rows == 0:
        return empty
    explained_rows = _scalar(
        conn,
        f"""
        {classified_cte}
        SELECT count(*)
        FROM classified_jumps
        WHERE has_explaining_corporate_action
        """,
    )
    rows = _scalar(conn, f"{classified_cte} SELECT count(*) FROM unresolved")
    if rows == 0:
        return {
            **empty,
            "total_rows": total_rows,
            "explained_rows": explained_rows,
        }

    years = tuple(
        {
            "year": str(year),
            "rows": int(row_count),
            "securities": int(security_count),
            "earliest_date": earliest_date,
            "latest_date": latest_date,
        }
        for year, row_count, security_count, earliest_date, latest_date in conn.execute(
            f"""
            {classified_cte}
            SELECT
                substr(trade_date_key, 1, 4) AS year,
                count(*) AS row_count,
                count(DISTINCT security_id) AS security_count,
                min(trade_date_key) AS earliest_date,
                max(trade_date_key) AS latest_date
            FROM unresolved
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()
    )
    magnitude_buckets = tuple(
        {
            "bucket": str(bucket),
            "rows": int(row_count),
            "securities": int(security_count),
        }
        for bucket, row_count, security_count, _bucket_order in conn.execute(
            f"""
            {classified_cte}
            SELECT
                {_factor_ratio_magnitude_bucket_sql("factor_ratio")} AS bucket,
                count(*) AS row_count,
                count(DISTINCT security_id) AS security_count,
                min({_factor_ratio_magnitude_bucket_order_sql("factor_ratio")}) AS bucket_order
            FROM unresolved
            GROUP BY 1
            ORDER BY bucket_order
            """
        ).fetchall()
    )
    top_securities = tuple(
        {
            "security_id": str(security_id),
            "rows": int(row_count),
            "earliest_date": earliest_date,
            "latest_date": latest_date,
            "min_factor_ratio": float(min_factor_ratio),
            "max_factor_ratio": float(max_factor_ratio),
        }
        for (
            security_id,
            row_count,
            earliest_date,
            latest_date,
            min_factor_ratio,
            max_factor_ratio,
        ) in conn.execute(
            f"""
            {classified_cte}
            SELECT
                security_id,
                count(*) AS row_count,
                min(trade_date_key) AS earliest_date,
                max(trade_date_key) AS latest_date,
                min(factor_ratio) AS min_factor_ratio,
                max(factor_ratio) AS max_factor_ratio
            FROM unresolved
            GROUP BY 1
            ORDER BY row_count DESC, security_id
            LIMIT 30
            """
        ).fetchall()
    )
    dividend_proximity = _unresolved_adj_factor_jump_dividend_proximity(
        conn,
        classified_cte,
    )
    triage = _unresolved_adj_factor_jump_triage(conn, classified_cte)
    examples = _unresolved_adj_factor_jump_examples(conn, classified_cte)

    return {
        "total_rows": total_rows,
        "explained_rows": explained_rows,
        "rows": rows,
        "promotion_blocking_rows": rows,
        "years": years,
        "magnitude_buckets": magnitude_buckets,
        "top_securities": top_securities,
        "dividend_proximity": dividend_proximity,
        "triage": triage,
        "examples": examples,
    }


def _factor_ratio_magnitude_bucket_sql(factor_ratio_column: str) -> str:
    return f"""
        CASE
            WHEN abs({factor_ratio_column} - 1.0) <= 0.005 THEN '<=50bp'
            WHEN abs({factor_ratio_column} - 1.0) <= 0.02 THEN '<=2pct'
            WHEN abs({factor_ratio_column} - 1.0) <= 0.10 THEN '<=10pct'
            ELSE '>10pct'
        END
        """


def _factor_ratio_magnitude_bucket_order_sql(factor_ratio_column: str) -> str:
    return f"""
        CASE
            WHEN abs({factor_ratio_column} - 1.0) <= 0.005 THEN 1
            WHEN abs({factor_ratio_column} - 1.0) <= 0.02 THEN 2
            WHEN abs({factor_ratio_column} - 1.0) <= 0.10 THEN 3
            ELSE 4
        END
        """


def _factor_pre_close_basis_diff_sql(alias: str) -> str:
    return (
        f"abs({alias}.factor_ratio / "
        f"NULLIF({alias}.previous_close / NULLIF({alias}.pre_close, 0.0), 0.0) - 1.0)"
    )


def _has_suspend_window_sql(conn: duckdb.DuckDBPyConnection, alias: str) -> str:
    if not _has_columns(conn, "raw_suspend_d", {"ts_code", "trade_date"}):
        return "FALSE"
    return f"""
        EXISTS (
            SELECT 1
            FROM raw_suspend_d AS s
            WHERE s.ts_code = {alias}.security_id
              AND {_date_key_sql("s.trade_date")} > {alias}.previous_trade_date_key
              AND {_date_key_sql("s.trade_date")} <= {alias}.trade_date_key
        )
        """


def _triage_class_case_sql(
    *,
    has_same_date_nonimplemented_column: str,
    nearest_implemented_days_column: str,
    factor_pre_close_basis_diff_column: str,
    factor_ratio_column: str,
) -> str:
    return f"""
        CASE
            WHEN {has_same_date_nonimplemented_column}
                THEN 'nonimplemented_dividend_same_date'
            WHEN {nearest_implemented_days_column} BETWEEN 1 AND 30
                THEN 'implemented_dividend_outside_factor_window'
            WHEN {factor_pre_close_basis_diff_column} <= 0.001
                THEN 'daily_pre_close_ex_right_without_ledger'
            WHEN abs({factor_ratio_column} - 1.0) <= 0.005
                THEN 'low_materiality_provider_factor_noise'
            ELSE 'provider_factor_jump_without_event_evidence'
        END
        """


def _triage_class_order_sql(triage_class_column: str) -> str:
    return f"""
        CASE
            WHEN {triage_class_column} = 'implemented_dividend_outside_factor_window' THEN 1
            WHEN {triage_class_column} = 'nonimplemented_dividend_same_date' THEN 2
            WHEN {triage_class_column} = 'daily_pre_close_ex_right_without_ledger' THEN 3
            WHEN {triage_class_column} = 'low_materiality_provider_factor_noise' THEN 4
            ELSE 5
        END
        """


def _unresolved_adj_factor_jump_triage(
    conn: duckdb.DuckDBPyConnection,
    unresolved_cte: str,
) -> tuple[dict[str, Any], ...]:
    required_columns = {"ts_code", "div_proc", "ex_date"}
    has_suspend_window_sql = _has_suspend_window_sql(conn, "u")
    factor_pre_close_basis_diff_sql = _factor_pre_close_basis_diff_sql("u")
    if not _has_columns(conn, "raw_dividend", required_columns):
        return tuple(
            {
                "triage_class": str(triage_class),
                "rows": int(row_count),
                "securities": int(security_count),
            }
            for triage_class, row_count, security_count, _triage_order in conn.execute(
                f"""
                {unresolved_cte},
                classified AS (
                    SELECT
                        u.security_id,
                        CASE
                            WHEN {factor_pre_close_basis_diff_sql} <= 0.001
                                THEN 'daily_pre_close_ex_right_without_ledger'
                            WHEN abs(u.factor_ratio - 1.0) <= 0.005
                                THEN 'low_materiality_provider_factor_noise'
                            ELSE 'raw_dividend_missing'
                        END AS triage_class,
                        {has_suspend_window_sql} AS has_suspend_window
                    FROM unresolved AS u
                )
                SELECT
                    triage_class,
                    count(*) AS row_count,
                    count(DISTINCT security_id) AS security_count,
                    min({_triage_class_order_sql("triage_class")}) AS triage_order
                FROM classified
                GROUP BY 1
                ORDER BY triage_order
                """
            ).fetchall()
        )

    return tuple(
        {
            "triage_class": str(triage_class),
            "rows": int(row_count),
            "securities": int(security_count),
        }
        for triage_class, row_count, security_count, _triage_order in conn.execute(
            f"""
            {unresolved_cte},
            valid_raw_dividend AS (
                SELECT
                    CAST(ts_code AS VARCHAR) AS security_id,
                    CAST(div_proc AS VARCHAR) AS div_proc,
                    {_date_key_sql("ex_date")} AS ex_date_key
                FROM raw_dividend
                WHERE ts_code IS NOT NULL
                  AND ex_date IS NOT NULL
                  AND regexp_matches({_date_key_sql("ex_date")}, '^[0-9]{{8}}$')
            ),
            classified AS (
                SELECT
                    u.security_id,
                    u.previous_trade_date_key,
                    u.trade_date_key,
                    bool_or(
                        r.ex_date_key = u.trade_date_key
                        AND COALESCE(r.div_proc, '') <> '瀹炴柦'
                    ) AS has_same_date_nonimplemented,
                    min(
                        CASE
                            WHEN r.div_proc = '瀹炴柦' THEN abs(
                                date_diff(
                                    'day',
                                    strptime(r.ex_date_key, '%Y%m%d'),
                                    strptime(u.trade_date_key, '%Y%m%d')
                                )
                            )
                            ELSE NULL
                        END
                    ) AS nearest_implemented_days,
                    {factor_pre_close_basis_diff_sql} AS factor_pre_close_basis_diff,
                    u.factor_ratio,
                    {has_suspend_window_sql} AS has_suspend_window
                FROM unresolved AS u
                LEFT JOIN valid_raw_dividend AS r
                  ON r.security_id = u.security_id
                GROUP BY 1, 2, 3, 6, 7, 8
            ),
            triaged AS (
                SELECT
                    security_id,
                    {_triage_class_case_sql(
                        has_same_date_nonimplemented_column="has_same_date_nonimplemented",
                        nearest_implemented_days_column="nearest_implemented_days",
                        factor_pre_close_basis_diff_column="factor_pre_close_basis_diff",
                        factor_ratio_column="factor_ratio",
                    )} AS triage_class
                FROM classified
            )
            SELECT
                triage_class,
                count(*) AS row_count,
                count(DISTINCT security_id) AS security_count,
                min({_triage_class_order_sql("triage_class")}) AS triage_order
            FROM triaged
            GROUP BY 1
            ORDER BY triage_order
            """
        ).fetchall()
    )


def _unresolved_adj_factor_jump_examples(
    conn: duckdb.DuckDBPyConnection,
    unresolved_cte: str,
) -> tuple[dict[str, Any], ...]:
    required_columns = {"ts_code", "div_proc", "ex_date"}
    has_suspend_window_sql = _has_suspend_window_sql(conn, "u")
    factor_pre_close_basis_diff_sql = _factor_pre_close_basis_diff_sql("u")
    if not _has_columns(conn, "raw_dividend", required_columns):
        return tuple(
            {
                "security_id": str(security_id),
                "previous_trade_date": previous_trade_date,
                "trade_date": trade_date,
                "factor_ratio": float(factor_ratio),
                "magnitude_bucket": str(magnitude_bucket),
                "dividend_proximity_bucket": "raw_dividend_missing",
                "nearest_implemented_dividend_ex_date": None,
                "nearest_implemented_dividend_days": None,
                "has_suspend_window": bool(has_suspend_window),
                "factor_pre_close_basis_diff": _round_optional_float(
                    factor_pre_close_basis_diff
                ),
                "triage_class": str(triage_class),
                "recommended_action": "quarantine_security_window_from_promotion",
            }
            for (
                security_id,
                previous_trade_date,
                trade_date,
                factor_ratio,
                magnitude_bucket,
                has_suspend_window,
                factor_pre_close_basis_diff,
                triage_class,
            ) in conn.execute(
                f"""
                {unresolved_cte}
                SELECT
                    u.security_id,
                    u.previous_trade_date_key,
                    u.trade_date_key,
                    u.factor_ratio,
                    {_factor_ratio_magnitude_bucket_sql("u.factor_ratio")} AS magnitude_bucket,
                    {has_suspend_window_sql} AS has_suspend_window,
                    {factor_pre_close_basis_diff_sql} AS factor_pre_close_basis_diff,
                    CASE
                        WHEN {factor_pre_close_basis_diff_sql} <= 0.001
                            THEN 'daily_pre_close_ex_right_without_ledger'
                        WHEN abs(u.factor_ratio - 1.0) <= 0.005
                            THEN 'low_materiality_provider_factor_noise'
                        ELSE 'raw_dividend_missing'
                    END AS triage_class
                FROM unresolved AS u
                ORDER BY abs(u.factor_ratio - 1.0) DESC, u.security_id, u.trade_date_key
                LIMIT {UNRESOLVED_ADJ_FACTOR_EXAMPLE_LIMIT}
                """
            ).fetchall()
        )

    return tuple(
        {
            "security_id": str(security_id),
            "previous_trade_date": previous_trade_date,
            "trade_date": trade_date,
            "factor_ratio": float(factor_ratio),
            "magnitude_bucket": str(magnitude_bucket),
            "dividend_proximity_bucket": str(dividend_proximity_bucket),
            "nearest_implemented_dividend_ex_date": nearest_ex_date,
            "nearest_implemented_dividend_days": (
                None if nearest_days is None else int(nearest_days)
            ),
            "has_suspend_window": bool(has_suspend_window),
            "factor_pre_close_basis_diff": _round_optional_float(
                factor_pre_close_basis_diff
            ),
            "triage_class": str(triage_class),
            "recommended_action": "quarantine_security_window_from_promotion",
        }
        for (
            security_id,
            previous_trade_date,
            trade_date,
            factor_ratio,
            magnitude_bucket,
            dividend_proximity_bucket,
            nearest_ex_date,
            nearest_days,
            has_suspend_window,
            factor_pre_close_basis_diff,
            triage_class,
        ) in conn.execute(
            f"""
            {unresolved_cte},
            valid_raw_dividend AS (
                SELECT
                    CAST(ts_code AS VARCHAR) AS security_id,
                    CAST(div_proc AS VARCHAR) AS div_proc,
                    {_date_key_sql("ex_date")} AS ex_date_key
                FROM raw_dividend
                WHERE ts_code IS NOT NULL
                  AND ex_date IS NOT NULL
                  AND regexp_matches({_date_key_sql("ex_date")}, '^[0-9]{{8}}$')
            ),
            classified AS (
                SELECT
                    u.security_id,
                    u.previous_trade_date_key,
                    u.trade_date_key,
                    u.factor_ratio,
                    {factor_pre_close_basis_diff_sql} AS factor_pre_close_basis_diff,
                    {has_suspend_window_sql} AS has_suspend_window,
                    bool_or(
                        r.ex_date_key = u.trade_date_key
                        AND r.div_proc = '瀹炴柦'
                    ) AS has_same_date_implemented,
                    bool_or(
                        r.ex_date_key = u.trade_date_key
                        AND COALESCE(r.div_proc, '') <> '瀹炴柦'
                    ) AS has_same_date_nonimplemented,
                    min(
                        CASE
                            WHEN r.div_proc = '瀹炴柦' THEN abs(
                                date_diff(
                                    'day',
                                    strptime(r.ex_date_key, '%Y%m%d'),
                                    strptime(u.trade_date_key, '%Y%m%d')
                                )
                            )
                            ELSE NULL
                        END
                    ) AS nearest_implemented_days
                FROM unresolved AS u
                LEFT JOIN valid_raw_dividend AS r
                  ON r.security_id = u.security_id
                GROUP BY 1, 2, 3, 4, 5, 6
            ),
            nearest_implemented AS (
                SELECT
                    u.security_id,
                    u.trade_date_key,
                    r.ex_date_key,
                    abs(
                        date_diff(
                            'day',
                            strptime(r.ex_date_key, '%Y%m%d'),
                            strptime(u.trade_date_key, '%Y%m%d')
                        )
                    ) AS nearest_days,
                    row_number() OVER (
                        PARTITION BY u.security_id, u.trade_date_key
                        ORDER BY abs(
                            date_diff(
                                'day',
                                strptime(r.ex_date_key, '%Y%m%d'),
                                strptime(u.trade_date_key, '%Y%m%d')
                            )
                        ), r.ex_date_key
                    ) AS rn
                FROM unresolved AS u
                INNER JOIN valid_raw_dividend AS r
                  ON r.security_id = u.security_id
                 AND r.div_proc = '瀹炴柦'
            )
            SELECT
                c.security_id,
                c.previous_trade_date_key,
                c.trade_date_key,
                c.factor_ratio,
                {_factor_ratio_magnitude_bucket_sql("c.factor_ratio")} AS magnitude_bucket,
                CASE
                    WHEN c.has_same_date_implemented THEN 'same_date_implemented'
                    WHEN c.has_same_date_nonimplemented THEN 'same_date_nonimplemented_only'
                    WHEN c.nearest_implemented_days BETWEEN 1 AND 5 THEN 'implemented_within_5d'
                    WHEN c.nearest_implemented_days BETWEEN 6 AND 30 THEN 'implemented_within_30d'
                    ELSE 'no_implemented_within_30d'
                END AS dividend_proximity_bucket,
                n.ex_date_key AS nearest_implemented_dividend_ex_date,
                n.nearest_days AS nearest_implemented_dividend_days,
                c.has_suspend_window,
                c.factor_pre_close_basis_diff,
                {_triage_class_case_sql(
                    has_same_date_nonimplemented_column="c.has_same_date_nonimplemented",
                    nearest_implemented_days_column="c.nearest_implemented_days",
                    factor_pre_close_basis_diff_column="c.factor_pre_close_basis_diff",
                    factor_ratio_column="c.factor_ratio",
                )} AS triage_class
            FROM classified AS c
            LEFT JOIN nearest_implemented AS n
              ON n.security_id = c.security_id
             AND n.trade_date_key = c.trade_date_key
             AND n.rn = 1
            ORDER BY abs(c.factor_ratio - 1.0) DESC, c.security_id, c.trade_date_key
            LIMIT {UNRESOLVED_ADJ_FACTOR_EXAMPLE_LIMIT}
            """
        ).fetchall()
    )


def _unresolved_adj_factor_jump_dividend_proximity(
    conn: duckdb.DuckDBPyConnection,
    unresolved_cte: str,
) -> tuple[dict[str, Any], ...]:
    required_columns = {"ts_code", "div_proc", "ex_date"}
    if not _has_columns(conn, "raw_dividend", required_columns):
        return ()

    return tuple(
        {
            "bucket": str(bucket),
            "rows": int(row_count),
            "securities": int(security_count),
        }
        for bucket, row_count, security_count, _bucket_order in conn.execute(
            f"""
            {unresolved_cte},
            valid_raw_dividend AS (
                SELECT
                    CAST(ts_code AS VARCHAR) AS security_id,
                    CAST(div_proc AS VARCHAR) AS div_proc,
                    {_date_key_sql("ex_date")} AS ex_date_key
                FROM raw_dividend
                WHERE ts_code IS NOT NULL
                  AND ex_date IS NOT NULL
                  AND regexp_matches({_date_key_sql("ex_date")}, '^[0-9]{{8}}$')
            ),
            classified AS (
                SELECT
                    u.security_id,
                    u.trade_date_key,
                    bool_or(
                        r.ex_date_key = u.trade_date_key
                        AND r.div_proc = '瀹炴柦'
                    ) AS has_same_date_implemented,
                    bool_or(
                        r.ex_date_key = u.trade_date_key
                        AND COALESCE(r.div_proc, '') <> '瀹炴柦'
                    ) AS has_same_date_nonimplemented,
                    min(
                        CASE
                            WHEN r.div_proc = '瀹炴柦' THEN abs(
                                date_diff(
                                    'day',
                                    strptime(r.ex_date_key, '%Y%m%d'),
                                    strptime(u.trade_date_key, '%Y%m%d')
                                )
                            )
                            ELSE NULL
                        END
                    ) AS nearest_implemented_days
                FROM unresolved AS u
                LEFT JOIN valid_raw_dividend AS r
                  ON r.security_id = u.security_id
                GROUP BY 1, 2
            ),
            bucketed AS (
                SELECT
                    security_id,
                    CASE
                        WHEN has_same_date_implemented THEN 'same_date_implemented'
                        WHEN has_same_date_nonimplemented THEN 'same_date_nonimplemented_only'
                        WHEN nearest_implemented_days BETWEEN 1 AND 5 THEN 'implemented_within_5d'
                        WHEN nearest_implemented_days BETWEEN 6 AND 30 THEN 'implemented_within_30d'
                        ELSE 'no_implemented_within_30d'
                    END AS bucket,
                    CASE
                        WHEN has_same_date_implemented THEN 1
                        WHEN has_same_date_nonimplemented THEN 2
                        WHEN nearest_implemented_days BETWEEN 1 AND 5 THEN 3
                        WHEN nearest_implemented_days BETWEEN 6 AND 30 THEN 4
                        ELSE 5
                    END AS bucket_order
                FROM classified
            )
            SELECT
                bucket,
                count(*) AS row_count,
                count(DISTINCT security_id) AS security_count,
                min(bucket_order) AS bucket_order
            FROM bucketed
            GROUP BY 1
            ORDER BY bucket_order
            """
        ).fetchall()
    )


def _corporate_action_window_predicate(conn: duckdb.DuckDBPyConnection) -> str:
    columns = _table_columns(conn, "corporate_action_ledger")
    predicates: list[str] = []
    if "action_date" in columns:
        predicates.append(_date_in_jump_window_sql(_date_key_sql("c.action_date")))
    if "ex_date" in columns:
        predicates.append(_date_in_jump_window_sql(_date_key_sql("c.ex_date")))
    if "book_date" in columns:
        predicates.append(_date_in_jump_window_sql(_date_key_sql("c.book_date")))
    return " OR ".join(predicates)


def _date_in_jump_window_sql(date_expr: str) -> str:
    return f"({date_expr} > j.previous_trade_date_key AND {date_expr} <= j.trade_date_key)"


def _count(conn: duckdb.DuckDBPyConnection, table: str) -> int:
    if not _table_exists(conn, table):
        return 0
    return _scalar(conn, f"SELECT count(*) FROM {table}")


def _scalar(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    parameters: list[Any] | None = None,
) -> int:
    value = conn.execute(sql, parameters or []).fetchone()[0]
    return int(value or 0)


def _round_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return round(float(value), 12)


def _has_columns(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    required_columns: set[str],
) -> bool:
    if not _table_exists(conn, table_name):
        return False
    return required_columns.issubset(_table_columns(conn, table_name))


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return row is not None


def _table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchall()
    return {str(column_name) for (column_name,) in rows}


def _date_key_sql(column_name: str) -> str:
    return f"replace(left(CAST({column_name} AS VARCHAR), 10), '-', '')"
