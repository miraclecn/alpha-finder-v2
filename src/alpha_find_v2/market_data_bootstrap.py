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
            CASE
                WHEN u.ts_code IS NOT NULL THEN 'raw_times_adj_factor'
                ELSE 'qfq_diagnostic_only'
            END AS adjusted_price_source,
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
    imported_index_basic = _materialize_optional_index_reference_table(
        conn,
        table_name="index_basic_ref",
    )
    imported_raw_index_daily = _materialize_optional_index_reference_table(
        conn,
        table_name="raw_index_daily",
    )
    imported_raw_dividend = _materialize_optional_raw_event_table(
        conn,
        table_name="raw_dividend",
    )
    imported_raw_stk_limit = _materialize_optional_raw_event_table(
        conn,
        table_name="raw_stk_limit",
    )
    imported_raw_suspend_d = _materialize_optional_raw_event_table(
        conn,
        table_name="raw_suspend_d",
    )
    imported_raw_share_float = _materialize_optional_raw_event_table(
        conn,
        table_name="raw_share_float",
    )
    imported_raw_repurchase = _materialize_optional_raw_event_table(
        conn,
        table_name="raw_repurchase",
    )
    _build_corporate_action_ledger(conn, imported_raw_dividend=imported_raw_dividend)
    _build_corporate_action_exception_ledger(conn)
    _build_tradeability_state_daily(
        conn,
        imported_raw_stk_limit=imported_raw_stk_limit,
        imported_raw_suspend_d=imported_raw_suspend_d,
    )
    if imported_raw_index_daily:
        _build_index_daily_bar_pit(conn)

    conn.execute(
        """
        CREATE OR REPLACE TABLE dataset_registry AS
        SELECT
            'daily_bar_pit' AS dataset_id,
            'green' AS status,
            '2014+ daily bars with unit normalization; raw_kline_unadj is execution truth and adjusted fields are explicitly sourced diagnostics',
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
        UNION ALL
        SELECT
            'corporate_action_ledger',
            'green',
            'implemented dividend rows normalized into cash and share action bookings',
            COUNT(*),
            MIN(book_date),
            MAX(book_date)
        FROM corporate_action_ledger
        UNION ALL
        SELECT
            'corporate_action_exception_ledger',
            CASE WHEN COUNT(*) = 0 THEN 'green' ELSE 'amber' END,
            'unexplained adj_factor jumps are quarantined as promotion-blocking security windows; no inferred cash or share booking',
            COUNT(*),
            MIN(trade_date),
            MAX(trade_date)
        FROM corporate_action_exception_ledger
        UNION ALL
        SELECT
            'tradeability_state_daily',
            'green',
            'official suspension and limit records when available with OHLC fallback diagnostics',
            COUNT(*),
            MIN(trade_date),
            MAX(trade_date)
        FROM tradeability_state_daily
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
    if imported_index_basic:
        conn.execute(
            """
            INSERT INTO dataset_registry
            SELECT
                'index_basic_ref',
                'green',
                'staged benchmark index metadata for V2 benchmark and regime research',
                COUNT(*),
                MIN(list_date),
                MAX(list_date)
            FROM index_basic_ref
            """
        )
    if imported_raw_index_daily:
        conn.execute(
            """
            INSERT INTO dataset_registry
            SELECT
                'index_daily_bar_pit',
                'green',
                'staged benchmark index daily bars for V2 benchmark and anomaly research',
                COUNT(*),
                MIN(trade_date),
                MAX(trade_date)
            FROM index_daily_bar_pit
            """
        )
    _write_data_spine_registry(
        conn,
        source_path=source_path,
        supplemental_path=supplemental_path,
        target_path=target_path,
    )
    _write_build_chain_registry(conn)
    _write_data_boundary_registry(conn)

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


def _materialize_optional_index_reference_table(
    conn: Any,
    *,
    table_name: str,
) -> bool:
    source_alias = _preferred_attached_source(conn, table_name)
    if source_alias is None:
        return False

    columns = _table_columns(conn, source_alias, table_name)
    required_columns = _index_reference_required_columns(table_name)
    missing_columns = sorted(required_columns - columns)
    if missing_columns:
        raise ValueError(
            f"{source_alias}.{table_name} is missing required columns: {', '.join(missing_columns)}"
        )

    if table_name == "index_basic_ref":
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE index_basic_ref AS
            SELECT
                CAST(ts_code AS VARCHAR) AS ts_code,
                CAST(name AS VARCHAR) AS name,
                CAST(market AS VARCHAR) AS market,
                CAST(publisher AS VARCHAR) AS publisher,
                CAST(category AS VARCHAR) AS category,
                NULLIF(CAST(base_date AS VARCHAR), '') AS base_date,
                CAST(base_point AS DOUBLE) AS base_point,
                NULLIF(CAST(list_date AS VARCHAR), '') AS list_date
            FROM {source_alias}.index_basic_ref
            """
        )
        return True

    if table_name == "raw_index_daily":
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE raw_index_daily AS
            SELECT
                CAST(ts_code AS VARCHAR) AS ts_code,
                CAST(trade_date AS VARCHAR) AS trade_date,
                CAST(close AS DOUBLE) AS close,
                CAST(open AS DOUBLE) AS open,
                CAST(high AS DOUBLE) AS high,
                CAST(low AS DOUBLE) AS low,
                CAST(pre_close AS DOUBLE) AS pre_close,
                CAST(change AS DOUBLE) AS change,
                CAST(pct_chg AS DOUBLE) AS pct_chg,
                CAST(vol AS DOUBLE) AS vol,
                CAST(amount AS DOUBLE) AS amount,
                COALESCE(CAST(source_table AS VARCHAR), 'raw_index_daily') AS source_table,
                ingested_at
            FROM {source_alias}.raw_index_daily
            """
        )
        return True

    raise ValueError(f"Unsupported optional index reference table import: {table_name}")


def _materialize_optional_raw_event_table(
    conn: Any,
    *,
    table_name: str,
) -> bool:
    source_alias = _preferred_attached_source(conn, table_name)
    if source_alias is None:
        _create_empty_raw_event_table(conn, table_name)
        return False
    columns = _table_columns(conn, source_alias, table_name)
    required_columns = _raw_event_required_columns(table_name)
    missing_columns = sorted(required_columns - columns)
    if missing_columns:
        raise ValueError(
            f"{source_alias}.{table_name} is missing required columns: {', '.join(missing_columns)}"
        )
    if table_name == "raw_dividend":
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE raw_dividend AS
            SELECT
                CAST(ts_code AS VARCHAR) AS ts_code,
                {_nullable_varchar_sql('end_date')} AS end_date,
                {_nullable_varchar_sql('ann_date')} AS ann_date,
                CAST(div_proc AS VARCHAR) AS div_proc,
                CAST(stk_div AS DOUBLE) AS stk_div,
                CAST(stk_bo_rate AS DOUBLE) AS stk_bo_rate,
                CAST(stk_co_rate AS DOUBLE) AS stk_co_rate,
                CAST(cash_div AS DOUBLE) AS cash_div,
                CAST(cash_div_tax AS DOUBLE) AS cash_div_tax,
                {_nullable_varchar_sql('record_date')} AS record_date,
                {_nullable_varchar_sql('ex_date')} AS ex_date,
                {_nullable_varchar_sql('pay_date')} AS pay_date,
                {_nullable_varchar_sql('div_listdate')} AS div_listdate,
                COALESCE(CAST(source_table AS VARCHAR), 'raw_dividend') AS source_table,
                ingested_at
            FROM {source_alias}.raw_dividend
            """
        )
        return True
    if table_name == "raw_stk_limit":
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE raw_stk_limit AS
            SELECT
                CAST(ts_code AS VARCHAR) AS ts_code,
                CAST(trade_date AS VARCHAR) AS trade_date,
                CAST(up_limit AS DOUBLE) AS up_limit,
                CAST(down_limit AS DOUBLE) AS down_limit,
                COALESCE(CAST(source_table AS VARCHAR), 'raw_stk_limit') AS source_table,
                ingested_at
            FROM {source_alias}.raw_stk_limit
            """
        )
        return True
    if table_name == "raw_suspend_d":
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE raw_suspend_d AS
            SELECT
                CAST(ts_code AS VARCHAR) AS ts_code,
                CAST(trade_date AS VARCHAR) AS trade_date,
                CAST(suspend_timing AS VARCHAR) AS suspend_timing,
                CAST(suspend_type AS VARCHAR) AS suspend_type,
                COALESCE(CAST(source_table AS VARCHAR), 'raw_suspend_d') AS source_table,
                ingested_at
            FROM {source_alias}.raw_suspend_d
            """
        )
        return True
    if table_name == "raw_share_float":
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE raw_share_float AS
            SELECT
                CAST(ts_code AS VARCHAR) AS ts_code,
                {_nullable_varchar_sql('ann_date')} AS ann_date,
                {_nullable_varchar_sql('float_date')} AS float_date,
                CAST(float_share AS DOUBLE) AS float_share,
                CAST(float_ratio AS DOUBLE) AS float_ratio,
                CAST(holder_name AS VARCHAR) AS holder_name,
                CAST(share_type AS VARCHAR) AS share_type,
                COALESCE(CAST(source_table AS VARCHAR), 'raw_share_float') AS source_table,
                ingested_at
            FROM {source_alias}.raw_share_float
            """
        )
        return True
    if table_name == "raw_repurchase":
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE raw_repurchase AS
            SELECT
                CAST(ts_code AS VARCHAR) AS ts_code,
                {_nullable_varchar_sql('ann_date')} AS ann_date,
                {_nullable_varchar_sql('end_date')} AS end_date,
                CAST(proc AS VARCHAR) AS proc,
                {_nullable_varchar_sql('exp_date')} AS exp_date,
                CAST(vol AS DOUBLE) AS vol,
                CAST(amount AS DOUBLE) AS amount,
                CAST(high_limit AS DOUBLE) AS high_limit,
                CAST(low_limit AS DOUBLE) AS low_limit,
                COALESCE(CAST(source_table AS VARCHAR), 'raw_repurchase') AS source_table,
                ingested_at
            FROM {source_alias}.raw_repurchase
            """
        )
        return True
    raise ValueError(f"Unsupported optional raw event table import: {table_name}")


def _index_reference_required_columns(table_name: str) -> set[str]:
    if table_name == "index_basic_ref":
        return {
            "ts_code",
            "name",
            "market",
            "publisher",
            "category",
            "base_date",
            "base_point",
            "list_date",
        }
    if table_name == "raw_index_daily":
        return {
            "ts_code",
            "trade_date",
            "close",
            "open",
            "high",
            "low",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount",
            "source_table",
            "ingested_at",
        }
    raise ValueError(f"Unsupported optional index reference table: {table_name}")


def _create_empty_raw_event_table(conn: Any, table_name: str) -> None:
    schemas = {
        "raw_dividend": """
            CREATE OR REPLACE TABLE raw_dividend (
                ts_code VARCHAR,
                end_date VARCHAR,
                ann_date VARCHAR,
                div_proc VARCHAR,
                stk_div DOUBLE,
                stk_bo_rate DOUBLE,
                stk_co_rate DOUBLE,
                cash_div DOUBLE,
                cash_div_tax DOUBLE,
                record_date VARCHAR,
                ex_date VARCHAR,
                pay_date VARCHAR,
                div_listdate VARCHAR,
                source_table VARCHAR,
                ingested_at TIMESTAMP
            )
        """,
        "raw_stk_limit": """
            CREATE OR REPLACE TABLE raw_stk_limit (
                ts_code VARCHAR,
                trade_date VARCHAR,
                up_limit DOUBLE,
                down_limit DOUBLE,
                source_table VARCHAR,
                ingested_at TIMESTAMP
            )
        """,
        "raw_suspend_d": """
            CREATE OR REPLACE TABLE raw_suspend_d (
                ts_code VARCHAR,
                trade_date VARCHAR,
                suspend_timing VARCHAR,
                suspend_type VARCHAR,
                source_table VARCHAR,
                ingested_at TIMESTAMP
            )
        """,
        "raw_share_float": """
            CREATE OR REPLACE TABLE raw_share_float (
                ts_code VARCHAR,
                ann_date VARCHAR,
                float_date VARCHAR,
                float_share DOUBLE,
                float_ratio DOUBLE,
                holder_name VARCHAR,
                share_type VARCHAR,
                source_table VARCHAR,
                ingested_at TIMESTAMP
            )
        """,
        "raw_repurchase": """
            CREATE OR REPLACE TABLE raw_repurchase (
                ts_code VARCHAR,
                ann_date VARCHAR,
                end_date VARCHAR,
                proc VARCHAR,
                exp_date VARCHAR,
                vol DOUBLE,
                amount DOUBLE,
                high_limit DOUBLE,
                low_limit DOUBLE,
                source_table VARCHAR,
                ingested_at TIMESTAMP
            )
        """,
    }
    conn.execute(schemas[table_name])


def _raw_event_required_columns(table_name: str) -> set[str]:
    common = {"source_table", "ingested_at"}
    if table_name == "raw_dividend":
        return {
            "ts_code",
            "end_date",
            "ann_date",
            "div_proc",
            "stk_div",
            "stk_bo_rate",
            "stk_co_rate",
            "cash_div",
            "cash_div_tax",
            "record_date",
            "ex_date",
            "pay_date",
            "div_listdate",
            *common,
        }
    if table_name == "raw_stk_limit":
        return {"ts_code", "trade_date", "up_limit", "down_limit", *common}
    if table_name == "raw_suspend_d":
        return {"ts_code", "trade_date", "suspend_timing", "suspend_type", *common}
    if table_name == "raw_share_float":
        return {
            "ts_code",
            "ann_date",
            "float_date",
            "float_share",
            "float_ratio",
            "holder_name",
            "share_type",
            *common,
        }
    if table_name == "raw_repurchase":
        return {
            "ts_code",
            "ann_date",
            "end_date",
            "proc",
            "exp_date",
            "vol",
            "amount",
            "high_limit",
            "low_limit",
            *common,
        }
    raise ValueError(f"Unsupported raw event table: {table_name}")


def _build_index_daily_bar_pit(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE index_daily_bar_pit AS
        SELECT
            ts_code AS index_code,
            trade_date,
            open,
            high,
            low,
            close,
            pre_close,
            change,
            pct_chg,
            vol AS volume,
            amount AS turnover_value,
            source_table,
            ingested_at
        FROM raw_index_daily
        """
    )


def _nullable_varchar_sql(column_name: str) -> str:
    return f"NULLIF(CAST({column_name} AS VARCHAR), '')"


def _build_corporate_action_ledger(
    conn: Any,
    *,
    imported_raw_dividend: bool,
) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE corporate_action_ledger AS
        WITH implemented AS (
            SELECT
                ts_code AS security_id,
                end_date,
                record_date,
                ex_date,
                COALESCE(pay_date, ex_date) AS pay_date,
                COALESCE(cash_div_tax, cash_div, 0.0) AS cash_per_share,
                COALESCE(stk_div, 0.0) + COALESCE(stk_bo_rate, 0.0) + COALESCE(stk_co_rate, 0.0) AS share_ratio,
                source_table
            FROM raw_dividend
            WHERE div_proc = '实施'
              AND ex_date IS NOT NULL
              AND trim(ex_date) <> ''
        ),
        action_rows AS (
            SELECT
                security_id || ':' || COALESCE(end_date, '') || ':' || ex_date || ':cash' AS action_id,
                security_id,
                'cash_dividend' AS action_type,
                record_date,
                pay_date AS book_date,
                ex_date,
                cash_per_share,
                0.0 AS share_ratio,
                source_table
            FROM implemented
            WHERE cash_per_share IS NOT NULL
              AND cash_per_share > 0.0
              AND pay_date IS NOT NULL
              AND trim(pay_date) <> ''
            UNION ALL
            SELECT
                security_id || ':' || COALESCE(end_date, '') || ':' || ex_date || ':share' AS action_id,
                security_id,
                'share_dividend' AS action_type,
                record_date,
                COALESCE(ex_date, pay_date) AS book_date,
                ex_date,
                0.0 AS cash_per_share,
                share_ratio,
                source_table
            FROM implemented
            WHERE share_ratio IS NOT NULL
              AND share_ratio > 0.0
        )
        SELECT *
        FROM action_rows
        """
    )


def _build_corporate_action_exception_ledger(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE corporate_action_exception_ledger AS
        WITH jumps AS (
            SELECT
                security_id,
                trade_date,
                lag(trade_date) OVER (
                    PARTITION BY security_id
                    ORDER BY trade_date
                ) AS previous_trade_date,
                close,
                lag(close) OVER (
                    PARTITION BY security_id
                    ORDER BY trade_date
                ) AS previous_close,
                pre_close,
                adj_factor,
                lag(adj_factor) OVER (
                    PARTITION BY security_id
                    ORDER BY trade_date
                ) AS previous_adj_factor
            FROM daily_bar_pit
            WHERE COALESCE(price_basis, 'unadjusted') = 'unadjusted'
              AND adj_factor IS NOT NULL
              AND adj_factor > 0.0
        ),
        significant_jumps AS (
            SELECT
                security_id,
                previous_trade_date,
                trade_date,
                previous_close,
                pre_close,
                previous_adj_factor,
                adj_factor AS current_adj_factor,
                adj_factor / NULLIF(previous_adj_factor, 0.0) AS factor_ratio,
                abs((adj_factor / NULLIF(previous_adj_factor, 0.0)) - 1.0)
                    AS abs_factor_change,
                previous_close / NULLIF(pre_close, 0.0) AS pre_close_factor_ratio
            FROM jumps
            WHERE previous_trade_date IS NOT NULL
              AND previous_adj_factor IS NOT NULL
              AND previous_adj_factor > 0.0
              AND abs((adj_factor / NULLIF(previous_adj_factor, 0.0)) - 1.0) > 0.001
        ),
        unresolved AS (
            SELECT
                j.*
            FROM significant_jumps AS j
            WHERE NOT EXISTS (
                SELECT 1
                FROM corporate_action_ledger AS c
                WHERE c.security_id = j.security_id
                  AND (
                    (
                        c.ex_date IS NOT NULL
                        AND regexp_matches(CAST(c.ex_date AS VARCHAR), '^[0-9]{8}$')
                        AND CAST(c.ex_date AS VARCHAR) > j.previous_trade_date
                        AND CAST(c.ex_date AS VARCHAR) <= j.trade_date
                    )
                    OR (
                        c.book_date IS NOT NULL
                        AND regexp_matches(CAST(c.book_date AS VARCHAR), '^[0-9]{8}$')
                        AND CAST(c.book_date AS VARCHAR) > j.previous_trade_date
                        AND CAST(c.book_date AS VARCHAR) <= j.trade_date
                    )
                  )
            )
        ),
        with_context AS (
            SELECT
                u.*,
                abs(
                    u.factor_ratio / NULLIF(u.pre_close_factor_ratio, 0.0) - 1.0
                ) AS factor_pre_close_basis_diff,
                EXISTS (
                    SELECT 1
                    FROM raw_suspend_d AS s
                    WHERE s.ts_code = u.security_id
                      AND s.trade_date > u.previous_trade_date
                      AND s.trade_date <= u.trade_date
                ) AS has_suspend_window,
                EXISTS (
                    SELECT 1
                    FROM raw_dividend AS r
                    WHERE r.ts_code = u.security_id
                      AND r.div_proc <> '实施'
                      AND r.ex_date = u.trade_date
                ) AS has_same_date_nonimplemented_dividend,
                (
                    SELECT min(
                        abs(
                            date_diff(
                                'day',
                                strptime(r.ex_date, '%Y%m%d'),
                                strptime(u.trade_date, '%Y%m%d')
                            )
                        )
                    )
                    FROM raw_dividend AS r
                    WHERE r.ts_code = u.security_id
                      AND r.div_proc = '实施'
                      AND r.ex_date IS NOT NULL
                      AND regexp_matches(r.ex_date, '^[0-9]{8}$')
                ) AS nearest_implemented_dividend_days
            FROM unresolved AS u
        ),
        classified AS (
            SELECT
                security_id || ':' || previous_trade_date || ':' || trade_date
                    || ':adj_factor_exception' AS exception_id,
                security_id,
                previous_trade_date,
                trade_date,
                previous_adj_factor,
                current_adj_factor,
                factor_ratio,
                abs_factor_change,
                previous_close,
                pre_close,
                pre_close_factor_ratio,
                factor_pre_close_basis_diff,
                CASE
                    WHEN abs_factor_change <= 0.005 THEN '<=50bp'
                    WHEN abs_factor_change <= 0.02 THEN '<=2pct'
                    WHEN abs_factor_change <= 0.10 THEN '<=10pct'
                    ELSE '>10pct'
                END AS magnitude_bucket,
                CASE
                    WHEN abs_factor_change <= 0.005 THEN 'low'
                    WHEN abs_factor_change <= 0.02 THEN 'medium'
                    WHEN abs_factor_change <= 0.10 THEN 'high'
                    ELSE 'critical'
                END AS severity,
                has_suspend_window,
                CASE
                    WHEN has_same_date_nonimplemented_dividend
                        THEN 'nonimplemented_dividend_same_date'
                    WHEN nearest_implemented_dividend_days BETWEEN 1 AND 30
                        THEN 'implemented_dividend_outside_factor_window'
                    WHEN factor_pre_close_basis_diff <= 0.001
                        THEN 'daily_pre_close_ex_right_without_ledger'
                    WHEN abs_factor_change <= 0.005
                        THEN 'low_materiality_provider_factor_noise'
                    ELSE 'provider_factor_jump_without_event_evidence'
                END AS triage_class,
                'quarantine_security_window_from_promotion' AS recommended_action,
                'derived.adj_factor_reconciliation' AS source_table
            FROM with_context
        )
        SELECT *
        FROM classified
        ORDER BY security_id, trade_date
        """
    )


def _build_tradeability_state_daily(
    conn: Any,
    *,
    imported_raw_stk_limit: bool,
    imported_raw_suspend_d: bool,
) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE tradeability_state_daily AS
        WITH suspend_key AS (
            SELECT
                ts_code,
                trade_date
            FROM raw_suspend_d
            WHERE ts_code IS NOT NULL
              AND trade_date IS NOT NULL
            GROUP BY ts_code, trade_date
        ),
        limit_key AS (
            SELECT
                ts_code,
                trade_date,
                MAX(up_limit) AS up_limit,
                MIN(down_limit) AS down_limit
            FROM raw_stk_limit
            WHERE ts_code IS NOT NULL
              AND trade_date IS NOT NULL
            GROUP BY ts_code, trade_date
        )
        SELECT
            d.security_id,
            d.trade_date,
            CASE
                WHEN s.ts_code IS NOT NULL THEN TRUE
                WHEN d.open IS NULL OR d.open <= 0.0 THEN TRUE
                ELSE FALSE
            END AS is_suspended,
            l.up_limit,
            l.down_limit,
            CASE
                WHEN l.up_limit IS NOT NULL THEN d.open >= l.up_limit - 1e-6 AND d.high >= l.up_limit - 1e-6 AND d.low >= l.up_limit - 1e-6
                ELSE FALSE
            END AS is_limit_up_open_lock,
            CASE
                WHEN l.down_limit IS NOT NULL THEN d.open <= l.down_limit + 1e-6 AND d.high <= l.down_limit + 1e-6 AND d.low <= l.down_limit + 1e-6
                ELSE FALSE
            END AS is_limit_down_open_lock,
            CASE
                WHEN s.ts_code IS NOT NULL OR l.ts_code IS NOT NULL THEN 'official'
                ELSE 'ohlc_fallback'
            END AS source_priority
        FROM daily_bar_pit AS d
        LEFT JOIN suspend_key AS s
            ON s.ts_code = d.security_id
           AND s.trade_date = d.trade_date
        LEFT JOIN limit_key AS l
            ON l.ts_code = d.security_id
           AND l.trade_date = d.trade_date
        """
    )


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


def _write_data_spine_registry(
    conn: Any,
    *,
    source_path: Path,
    supplemental_path: Path | None,
    target_path: Path,
) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE data_spine_registry (
            surface_id VARCHAR,
            provider VARCHAR,
            boundary_role VARCHAR,
            path VARCHAR,
            note VARCHAR
        )
        """
    )
    conn.execute("DELETE FROM data_spine_registry")
    conn.executemany(
        "INSERT INTO data_spine_registry VALUES (?, ?, ?, ?, ?)",
        [
            (
                "source_market_db",
                "audited_v1_tushare_market_source",
                "external_source",
                str(source_path),
                "audited market backbone copied into V2 instead of queried live as the mixed legacy DB",
            ),
            (
                "supplemental_reference_db",
                "tushare_reference_staging",
                "pit_reference_staging",
                str(supplemental_path) if supplemental_path is not None else "",
                "only PIT benchmark and industry reference staging surface for the release-1 chain",
            ),
            (
                "target_research_db",
                "v2_isolated_research_db",
                "isolated_v2_research_surface",
                str(target_path),
                "only isolated V2 research DuckDB consumed by benchmark, replay, and deployment builders",
            ),
        ],
    )


def _write_build_chain_registry(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE build_chain_registry (
            step_order INTEGER,
            command_id VARCHAR,
            boundary_role VARCHAR,
            note VARCHAR
        )
        """
    )
    conn.execute("DELETE FROM build_chain_registry")
    conn.executemany(
        "INSERT INTO build_chain_registry VALUES (?, ?, ?, ?)",
        [
            (
                1,
                "build-reference-staging-db",
                "required_entrypoint",
                "stage benchmark and industry PIT truth from Tushare before benchmark-state work",
            ),
            (
                2,
                "build-research-source-db",
                "required_entrypoint",
                "materialize the isolated V2 research DuckDB from the audited market source plus staged PIT references",
            ),
            (
                3,
                "build-benchmark-state",
                "required_entrypoint",
                "derive benchmark_state_history only from the isolated V2 research DuckDB",
            ),
        ],
    )


def _write_data_boundary_registry(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE data_boundary_registry (
            category VARCHAR,
            entry_id VARCHAR,
            decision VARCHAR,
            note VARCHAR
        )
        """
    )
    conn.execute("DELETE FROM data_boundary_registry")
    conn.executemany(
        "INSERT INTO data_boundary_registry VALUES (?, ?, ?, ?)",
        [
            (
                "tier_rule",
                "green",
                "production_truth",
                "stable daily and structural truth used by benchmark state, trend research, tradeability checks, and regime monitoring",
            ),
            (
                "tier_rule",
                "amber",
                "slow_anchor",
                "slower fundamental snapshots allowed only for lag-aware anchor or veto work",
            ),
            (
                "tier_rule",
                "experimental",
                "exploration_only",
                "everything else, including unaudited AKShare-only fields, stays outside promotion-safe research",
            ),
            (
                "allowed_reuse",
                "daily_bar_pit",
                "allow",
                "point-in-time daily price and liquidity truth may feed release-1 research",
            ),
            (
                "allowed_reuse",
                "market_trade_calendar",
                "allow",
                "observed market calendar may feed release-1 research timing",
            ),
            (
                "allowed_reuse",
                "security_master_ref",
                "allow",
                "normalized A-share security master may feed release-1 research filters",
            ),
            (
                "allowed_reuse",
                "name_change_history",
                "allow",
                "historical ST name windows may feed tradeability and eligibility checks",
            ),
            (
                "allowed_reuse",
                "fundamental_snapshot_pit",
                "allow",
                "lagged fundamentals may feed slow anchor or veto logic only",
            ),
            (
                "allowed_reuse",
                "benchmark_membership_pit",
                "allow",
                "staged benchmark membership may enter V2 only through the PIT staging chain",
            ),
            (
                "allowed_reuse",
                "benchmark_weight_snapshot_pit",
                "allow",
                "staged provider benchmark weights may enter V2 only through the PIT staging chain",
            ),
            (
                "allowed_reuse",
                "index_daily_bar_pit",
                "allow",
                "staged benchmark index daily bars may feed V2 benchmark and market-regime research",
            ),
            (
                "allowed_reuse",
                "index_basic_ref",
                "allow",
                "staged benchmark index metadata may define benchmark identity inside the V2 staging chain",
            ),
            (
                "allowed_reuse",
                "industry_classification_pit",
                "allow",
                "staged PIT industry classification may enter V2 only through the PIT staging chain",
            ),
            (
                "forbidden_reuse",
                "v1_factor_outputs",
                "forbid",
                "legacy V1 factor outputs must not become V2 source-of-truth inputs",
            ),
            (
                "forbidden_reuse",
                "v1_strategy_outputs",
                "forbid",
                "legacy V1 strategy outputs must not become V2 source-of-truth inputs",
            ),
            (
                "forbidden_reuse",
                "v1_promotion_artifacts",
                "forbid",
                "legacy V1 promotion artifacts must stay outside the V2 production path",
            ),
            (
                "forbidden_reuse",
                "mixed_v1_query_time_dependency",
                "forbid",
                "V2 must not depend on the mixed V1 DuckDB at query time",
            ),
            (
                "known_gap",
                "first_source_suspension_realism",
                "visible_gap",
                "first-source suspension realism still needs a separate audited intake",
            ),
            (
                "known_gap",
                "exact_limit_state_reconstruction",
                "visible_gap",
                "exact price-limit state reconstruction still needs a separate audited intake",
            ),
            (
                "known_gap",
                "unaudited_akshare_only_fields",
                "visible_gap",
                "fields that exist only through unaudited AKShare paths remain blocked",
            ),
            (
                "audit_rule",
                "akshare_field_requires_explicit_v2_audit",
                "required",
                "AKShare may supplement or validate coverage gaps, but each field needs an explicit V2 audit before promotion-safe use",
            ),
            (
                "failure_condition",
                "akshare_unaudited_in_production_path",
                "stop",
                "stop if a production-safe path reads an unaudited AKShare-only field",
            ),
            (
                "failure_condition",
                "pit_reference_bypass_staging",
                "stop",
                "stop if benchmark or industry PIT references bypass staged Tushare inputs",
            ),
            (
                "failure_condition",
                "mixed_v1_live_research_dependency",
                "stop",
                "stop if new work keeps V2 dependent on the mixed V1 DB at query time",
            ),
        ],
    )
