from __future__ import annotations

from pathlib import Path
from typing import Any


def _sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _board_case_sql(alias: str) -> str:
    return f"""
        CASE
            WHEN split_part({alias}.ts_code, '.', 2) = 'BJ' THEN 'beijing'
            WHEN {alias}.symbol LIKE '688%' THEN 'star'
            WHEN {alias}.symbol LIKE '300%' OR {alias}.symbol LIKE '301%' THEN 'chinext'
            ELSE 'main_board'
        END
    """


def build_research_source_db(
    source_db: str | Path,
    target_db: str | Path,
    supplemental_db: str | Path | None = None,
) -> dict[str, Any]:
    import duckdb

    source_path = Path(source_db).expanduser().resolve()
    target_path = Path(target_db).expanduser().resolve()
    supplemental_path = (
        Path(supplemental_db).expanduser().resolve()
        if supplemental_db is not None
        else None
    )
    target_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(target_path))
    conn.execute(f"ATTACH '{_sql_path(source_path)}' AS source")
    if supplemental_path is not None:
        conn.execute(f"ATTACH '{_sql_path(supplemental_path)}' AS supplemental")

    board_case = _board_case_sql("s")
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE security_master_ref AS
        SELECT
            s.ts_code AS security_id,
            s.symbol,
            s.name AS current_name,
            split_part(s.ts_code, '.', 2) AS exchange,
            {board_case} AS board,
            s.area,
            s.list_date,
            NULLIF(s.delist_date, '') AS delist_date,
            s.is_hs,
            TRUE AS is_a_share,
            s.ingested_at
        FROM source.stock_basic_ref AS s
        WHERE split_part(s.ts_code, '.', 2) IN ('SZ', 'SH', 'BJ')
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE industry_classification_static AS
        SELECT
            security_id,
            current_name,
            industry AS industry_name,
            'current_static' AS classification_scope
        FROM (
            SELECT
                s.ts_code AS security_id,
                s.name AS current_name,
                s.industry
            FROM source.stock_basic_ref AS s
            WHERE split_part(s.ts_code, '.', 2) IN ('SZ', 'SH', 'BJ')
        )
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE name_change_history AS
        SELECT
            n.ts_code AS security_id,
            n.name,
            n.start_date,
            NULLIF(n.end_date, '') AS end_date,
            NULLIF(n.ann_date, '') AS announcement_date,
            n.change_reason,
            n.source_table,
            n.ingested_at,
            CASE
                WHEN n.name LIKE '%ST%' THEN TRUE
                WHEN n.change_reason IN ('ST', '*ST') THEN TRUE
                ELSE FALSE
            END AS is_st_name
        FROM source.raw_namechange AS n
        INNER JOIN security_master_ref AS s
            ON s.security_id = n.ts_code
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE market_trade_calendar AS
        SELECT DISTINCT d.trade_date
        FROM source.raw_daily_basic AS d
        INNER JOIN security_master_ref AS s
            ON s.security_id = d.ts_code
        ORDER BY d.trade_date
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TEMP VIEW st_daily AS
        SELECT DISTINCT
            c.trade_date,
            n.security_id,
            TRUE AS is_st
        FROM name_change_history AS n
        INNER JOIN market_trade_calendar AS c
            ON c.trade_date >= COALESCE(n.start_date, n.announcement_date)
           AND (n.end_date IS NULL OR c.trade_date <= n.end_date)
        WHERE n.is_st_name
        """
    )

    conn.execute(
        f"""
        CREATE OR REPLACE TABLE daily_bar_pit AS
        SELECT
            d.ts_code AS security_id,
            d.trade_date,
            s.exchange,
            s.board,
            COALESCE(st.is_st, FALSE) AS is_st,
            CASE
                WHEN u.ts_code IS NOT NULL THEN 'unadjusted'
                ELSE 'qfq_fallback'
            END AS price_basis,
            COALESCE(u.open, q.open) AS open,
            COALESCE(u.high, q.high) AS high,
            COALESCE(u.low, q.low) AS low,
            COALESCE(u.close, q.close) AS close,
            COALESCE(u.pre_close, q.pre_close) AS pre_close,
            COALESCE(u.change, q.change) AS change,
            COALESCE(u.pct_chg, q.pct_chg) AS pct_chg,
            a.adj_factor,
            CASE
                WHEN u.ts_code IS NOT NULL THEN u.open * a.adj_factor
                ELSE q.open
            END AS open_adj,
            CASE
                WHEN u.ts_code IS NOT NULL THEN u.high * a.adj_factor
                ELSE q.high
            END AS high_adj,
            CASE
                WHEN u.ts_code IS NOT NULL THEN u.low * a.adj_factor
                ELSE q.low
            END AS low_adj,
            CASE
                WHEN u.ts_code IS NOT NULL THEN u.close * a.adj_factor
                ELSE q.close
            END AS close_adj,
            COALESCE(u.vol, q.vol) * 100.0 AS volume_shares,
            COALESCE(u.amount, q.amount) * 1000.0 AS turnover_value_cny,
            d.turnover_rate AS turnover_rate_pct,
            d.turnover_rate_f AS turnover_rate_free_float_pct,
            d.volume_ratio,
            d.pe,
            d.pe_ttm,
            d.pb,
            d.ps,
            d.ps_ttm,
            d.dv_ratio,
            d.dv_ttm,
            d.total_share * 10000.0 AS total_shares,
            d.float_share * 10000.0 AS float_shares,
            d.free_share * 10000.0 AS free_float_shares,
            d.total_mv * 10000.0 AS total_mcap_cny,
            d.circ_mv * 10000.0 AS float_mcap_cny,
            COALESCE(u.source_table, q.source_table) AS price_source,
            d.source_table AS liquidity_source,
            a.source_table AS adj_factor_source,
            GREATEST(
                COALESCE(u.ingested_at, q.ingested_at),
                d.ingested_at,
                a.ingested_at
            ) AS ingested_at
        FROM source.raw_daily_basic AS d
        INNER JOIN security_master_ref AS s
            ON s.security_id = d.ts_code
        LEFT JOIN source.raw_kline_unadj AS u
            ON u.ts_code = d.ts_code
           AND u.trade_date = d.trade_date
        LEFT JOIN source.raw_kline_qfq AS q
            ON q.ts_code = d.ts_code
           AND q.trade_date = d.trade_date
        INNER JOIN source.raw_adj_factor AS a
            ON a.ts_code = d.ts_code
           AND a.trade_date = d.trade_date
        LEFT JOIN st_daily AS st
            ON st.security_id = d.ts_code
           AND st.trade_date = d.trade_date
        WHERE u.ts_code IS NOT NULL OR q.ts_code IS NOT NULL
        """
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE fundamental_snapshot_pit AS
        WITH base AS (
            SELECT
                f.ts_code AS security_id,
                f.ann_date AS announcement_date,
                f.end_date AS period_end,
                (
                    SELECT MIN(c.trade_date)
                    FROM market_trade_calendar AS c
                    WHERE c.trade_date > f.ann_date
                ) AS available_date,
                f.eps,
                f.roe,
                f.roa,
                f.gross_margin,
                f.netprofit_margin,
                f.current_ratio,
                f.debt_to_assets,
                f.revenue_ps AS revenue_per_share_cny,
                f.netprofit_yoy,
                f.dt_netprofit_yoy,
                f.or_yoy AS revenue_yoy,
                f.q_sales_yoy,
                f.assets_yoy,
                f.equity_yoy
            FROM source.pit_fina_indicator AS f
            INNER JOIN security_master_ref AS s
                ON s.security_id = f.ts_code
            WHERE f.ann_date IS NOT NULL
              AND f.ann_date <> ''
        )
        SELECT *
        FROM base
        WHERE available_date IS NOT NULL
        """
    )

    imported_industry = _materialize_optional_pit_table(
        conn,
        table_name="industry_classification_pit",
        required_columns={
            "security_id",
            "industry_schema",
            "industry_code",
            "effective_at",
            "removed_at",
        },
    )
    imported_benchmark = _materialize_optional_pit_table(
        conn,
        table_name="benchmark_membership_pit",
        required_columns={
            "benchmark_id",
            "security_id",
            "effective_at",
            "removed_at",
        },
    )
    imported_benchmark_weights = _materialize_optional_pit_table(
        conn,
        table_name="benchmark_weight_snapshot_pit",
        required_columns={
            "benchmark_id",
            "security_id",
            "trade_date",
            "weight",
        },
    )

    conn.execute(
        """
        CREATE OR REPLACE TABLE dataset_registry AS
        SELECT
            'daily_bar_pit' AS dataset_id,
            'green' AS status,
            '2014+ daily bars with unit normalization; raw_kline_unadj preferred and raw_kline_qfq used only as labeled fallback' AS note,
            COUNT(*) AS row_count,
            MIN(trade_date) AS earliest_date,
            MAX(trade_date) AS latest_date
        FROM daily_bar_pit
        UNION ALL
        SELECT
            'fundamental_snapshot_pit',
            'amber',
            'announcement timing is conservatively lagged to the next observed trade date',
            COUNT(*),
            MIN(announcement_date),
            MAX(announcement_date)
        FROM fundamental_snapshot_pit
        UNION ALL
        SELECT
            'industry_classification_static',
            'amber',
            'current stock_basic_ref industry only; no historical PIT classification yet',
            COUNT(*),
            NULL,
            NULL
        FROM industry_classification_static
        UNION ALL
        SELECT
            'market_trade_calendar',
            'green',
            'derived from observed raw_daily_basic trade dates for A-share securities',
            COUNT(*),
            MIN(trade_date),
            MAX(trade_date)
        FROM market_trade_calendar
        UNION ALL
        SELECT
            'name_change_history',
            'green',
            'used to derive historical ST name windows',
            COUNT(*),
            MIN(COALESCE(start_date, announcement_date)),
            MAX(COALESCE(end_date, announcement_date))
        FROM name_change_history
        UNION ALL
        SELECT
            'security_master_ref',
            'green',
            'A-share security master with exchange and board normalization',
            COUNT(*),
            MIN(list_date),
            MAX(COALESCE(delist_date, list_date))
        FROM security_master_ref
        """
    )
    if imported_benchmark:
        conn.execute(
            f"""
            INSERT INTO dataset_registry
            SELECT
                'benchmark_membership_pit',
                'green',
                'historical benchmark membership staged explicitly for V2 PIT replay',
                COUNT(*),
                MIN({_date_key_sql('effective_at')}),
                MAX(COALESCE({_date_key_sql('removed_at')}, {_date_key_sql('effective_at')}))
            FROM benchmark_membership_pit
            """
        )
    if imported_benchmark_weights:
        conn.execute(
            """
            INSERT INTO dataset_registry
            SELECT
                'benchmark_weight_snapshot_pit',
                'green',
                'historical provider benchmark weights staged explicitly for V2 PIT replay',
                COUNT(*),
                MIN(trade_date),
                MAX(trade_date)
            FROM benchmark_weight_snapshot_pit
            """
        )
    if imported_industry:
        conn.execute(
            f"""
            INSERT INTO dataset_registry
            SELECT
                'industry_classification_pit',
                'green',
                'historical PIT industry classification staged explicitly for V2 research',
                COUNT(*),
                MIN({_date_key_sql('effective_at')}),
                MAX(COALESCE({_date_key_sql('removed_at')}, {_date_key_sql('effective_at')}))
            FROM industry_classification_pit
            """
        )

    summary_rows = conn.execute(
        """
        SELECT dataset_id, status, row_count, earliest_date, latest_date
        FROM dataset_registry
        ORDER BY dataset_id
        """
    ).fetchall()
    if supplemental_path is not None:
        conn.execute("DETACH supplemental")
    conn.execute("DETACH source")
    conn.close()

    return {
        "source_db": str(source_path),
        "supplemental_db": str(supplemental_path) if supplemental_path is not None else "",
        "target_db": str(target_path),
        "datasets": [
            {
                "dataset_id": dataset_id,
                "status": status,
                "row_count": row_count,
                "earliest_date": earliest_date,
                "latest_date": latest_date,
            }
            for dataset_id, status, row_count, earliest_date, latest_date in summary_rows
        ],
    }


def _materialize_optional_pit_table(
    conn: Any,
    *,
    table_name: str,
    required_columns: set[str],
) -> bool:
    source_alias = _preferred_attached_source(conn, table_name)
    if source_alias is None:
        return False

    columns = _table_columns(conn, source_alias, table_name)
    missing_columns = sorted(required_columns - columns)
    if missing_columns:
        raise ValueError(
            f"{source_alias}.{table_name} is missing required columns: {', '.join(missing_columns)}"
        )

    if table_name == "industry_classification_pit":
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE industry_classification_pit AS
            SELECT
                CAST(security_id AS VARCHAR) AS security_id,
                CAST(industry_schema AS VARCHAR) AS industry_schema,
                CAST(industry_code AS VARCHAR) AS industry_code,
                CAST(effective_at AS VARCHAR) AS effective_at,
                NULLIF(CAST(removed_at AS VARCHAR), '') AS removed_at
            FROM {source_alias}.industry_classification_pit
            """
        )
        return True

    if table_name == "benchmark_membership_pit":
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE benchmark_membership_pit AS
            SELECT
                CAST(benchmark_id AS VARCHAR) AS benchmark_id,
                CAST(security_id AS VARCHAR) AS security_id,
                CAST(effective_at AS VARCHAR) AS effective_at,
                NULLIF(CAST(removed_at AS VARCHAR), '') AS removed_at
            FROM {source_alias}.benchmark_membership_pit
            """
        )
        return True

    if table_name == "benchmark_weight_snapshot_pit":
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE benchmark_weight_snapshot_pit AS
            SELECT
                CAST(benchmark_id AS VARCHAR) AS benchmark_id,
                CAST(security_id AS VARCHAR) AS security_id,
                CAST(trade_date AS VARCHAR) AS trade_date,
                CAST(weight AS DOUBLE) AS weight
            FROM {source_alias}.benchmark_weight_snapshot_pit
            """
        )
        return True

    raise ValueError(f"Unsupported optional PIT table import: {table_name}")


def _preferred_attached_source(conn: Any, table_name: str) -> str | None:
    for database_name in ("supplemental", "source"):
        if _table_exists(conn, database_name, table_name):
            return database_name
    return None


def _table_exists(conn: Any, database_name: str, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM duckdb_tables()
        WHERE database_name = ? AND table_name = ?
        LIMIT 1
        """,
        [database_name, table_name],
    ).fetchone()
    return row is not None


def _table_columns(conn: Any, database_name: str, table_name: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM duckdb_columns()
        WHERE database_name = ? AND table_name = ?
        """,
        [database_name, table_name],
    ).fetchall()
    return {str(column_name) for (column_name,) in rows}


def _date_key_sql(column_name: str) -> str:
    return f"""
        CASE
            WHEN {column_name} IS NULL OR trim({column_name}) = '' THEN NULL
            WHEN length(trim({column_name})) = 8 THEN trim({column_name})
            ELSE strftime(CAST({column_name} AS TIMESTAMP), '%Y%m%d')
        END
    """
