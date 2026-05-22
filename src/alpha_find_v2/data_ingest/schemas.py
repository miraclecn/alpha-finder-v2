"""
Raw table DDL constants for the data-ingest pipeline.

These schemas are the boundary contract between the new sync command and the
existing market_data_bootstrap.build_research_source_db.  Column types and
names must match exactly what market_data_bootstrap reads from
source.<table_name>.

Any change here must be accompanied by a migration in
market_data_bootstrap.py if the consumed columns change.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Raw table DDL
# Each entry maps dataset_id -> CREATE TABLE IF NOT EXISTS statement.
# The sync command creates these tables in output/raw.duckdb.
# ---------------------------------------------------------------------------

RAW_TABLE_DDL: dict[str, str] = {
    "stock_basic": """
        CREATE TABLE IF NOT EXISTS stock_basic_ref (
            ts_code       VARCHAR,
            symbol        VARCHAR,
            name          VARCHAR,
            area          VARCHAR,
            industry      VARCHAR,
            list_date     VARCHAR,
            delist_date   VARCHAR,
            is_hs         VARCHAR,
            ingested_at   TIMESTAMP,
            source_table  VARCHAR
        )
    """,

    "trade_cal": """
        CREATE TABLE IF NOT EXISTS raw_trade_cal (
            exchange      VARCHAR,
            cal_date      VARCHAR,
            is_open       INTEGER,
            pretrade_date VARCHAR,
            ingested_at   TIMESTAMP,
            source_table  VARCHAR
        )
    """,

    "namechange": """
        CREATE TABLE IF NOT EXISTS raw_namechange (
            ts_code       VARCHAR,
            name          VARCHAR,
            start_date    VARCHAR,
            end_date      VARCHAR,
            ann_date      VARCHAR,
            change_reason VARCHAR,
            source_table  VARCHAR,
            ingested_at   TIMESTAMP
        )
    """,

    # daily -> raw_kline_unadj (unadjusted OHLCV)
    "daily": """
        CREATE TABLE IF NOT EXISTS raw_kline_unadj (
            ts_code      VARCHAR,
            trade_date   VARCHAR,
            open         DOUBLE,
            high         DOUBLE,
            low          DOUBLE,
            close        DOUBLE,
            pre_close    DOUBLE,
            change       DOUBLE,
            pct_chg      DOUBLE,
            vol          DOUBLE,
            amount       DOUBLE,
            source_table VARCHAR,
            ingested_at  TIMESTAMP
        )
    """,

    # daily_basic -> raw_daily_basic
    "daily_basic": """
        CREATE TABLE IF NOT EXISTS raw_daily_basic (
            ts_code        VARCHAR,
            trade_date     VARCHAR,
            close          DOUBLE,
            turnover_rate  DOUBLE,
            turnover_rate_f DOUBLE,
            volume_ratio   DOUBLE,
            pe             DOUBLE,
            pe_ttm         DOUBLE,
            pb             DOUBLE,
            ps             DOUBLE,
            ps_ttm         DOUBLE,
            dv_ratio       DOUBLE,
            dv_ttm         DOUBLE,
            total_share    DOUBLE,
            float_share    DOUBLE,
            free_share     DOUBLE,
            total_mv       DOUBLE,
            circ_mv        DOUBLE,
            source_table   VARCHAR,
            ingested_at    TIMESTAMP
        )
    """,

    # adj_factor -> raw_adj_factor
    "adj_factor": """
        CREATE TABLE IF NOT EXISTS raw_adj_factor (
            ts_code      VARCHAR,
            trade_date   VARCHAR,
            adj_factor   DOUBLE,
            source_table VARCHAR,
            ingested_at  TIMESTAMP
        )
    """,

    # daily_qfq (pro_bar adj=qfq) -> raw_kline_qfq
    "daily_qfq": """
        CREATE TABLE IF NOT EXISTS raw_kline_qfq (
            ts_code      VARCHAR,
            trade_date   VARCHAR,
            open         DOUBLE,
            high         DOUBLE,
            low          DOUBLE,
            close        DOUBLE,
            pre_close    DOUBLE,
            change       DOUBLE,
            pct_chg      DOUBLE,
            vol          DOUBLE,
            amount       DOUBLE,
            source_table VARCHAR,
            ingested_at  TIMESTAMP
        )
    """,

    # suspend_d -> raw_suspend_d
    "suspend_d": """
        CREATE TABLE IF NOT EXISTS raw_suspend_d (
            ts_code         VARCHAR,
            trade_date      VARCHAR,
            suspend_timing  VARCHAR,
            suspend_type    VARCHAR,
            ingested_at     TIMESTAMP,
            source_table    VARCHAR
        )
    """,

    # stk_limit -> raw_stk_limit
    "stk_limit": """
        CREATE TABLE IF NOT EXISTS raw_stk_limit (
            trade_date   VARCHAR,
            ts_code      VARCHAR,
            up_limit     DOUBLE,
            down_limit   DOUBLE,
            pre_close    DOUBLE,
            ingested_at  TIMESTAMP,
            source_table VARCHAR
        )
    """,

    # index_daily -> raw_index_daily
    "index_daily": """
        CREATE TABLE IF NOT EXISTS raw_index_daily (
            ts_code      VARCHAR,
            trade_date   VARCHAR,
            close        DOUBLE,
            open         DOUBLE,
            high         DOUBLE,
            low          DOUBLE,
            pre_close    DOUBLE,
            change       DOUBLE,
            pct_chg      DOUBLE,
            vol          DOUBLE,
            amount       DOUBLE,
            ingested_at  TIMESTAMP,
            source_table VARCHAR
        )
    """,

    # index_weight -> raw_index_weight
    # Handled by existing reference_data_staging; included for completeness.
    "index_weight": """
        CREATE TABLE IF NOT EXISTS raw_index_weight (
            index_code   VARCHAR,
            con_code     VARCHAR,
            trade_date   VARCHAR,
            weight       DOUBLE,
            ingested_at  TIMESTAMP,
            source_table VARCHAR
        )
    """,

    # index_member_all -> raw_index_member_all
    "index_member_all": """
        CREATE TABLE IF NOT EXISTS raw_index_member_all (
            l1_code      VARCHAR,
            l1_name      VARCHAR,
            l2_code      VARCHAR,
            l2_name      VARCHAR,
            l3_code      VARCHAR,
            l3_name      VARCHAR,
            ts_code      VARCHAR,
            name         VARCHAR,
            in_date      VARCHAR,
            out_date     VARCHAR,
            is_new       VARCHAR,
            ingested_at  TIMESTAMP,
            source_table VARCHAR
        )
    """,

    # ---- 5000-credit datasets (default disabled) ----

    # fina_indicator -> pit_fina_indicator
    "fina_indicator": """
        CREATE TABLE IF NOT EXISTS pit_fina_indicator (
            ts_code          VARCHAR,
            ann_date         VARCHAR,
            end_date         VARCHAR,
            eps              DOUBLE,
            roe              DOUBLE,
            roa              DOUBLE,
            gross_margin     DOUBLE,
            netprofit_margin DOUBLE,
            current_ratio    DOUBLE,
            debt_to_assets   DOUBLE,
            revenue_ps       DOUBLE,
            netprofit_yoy    DOUBLE,
            dt_netprofit_yoy DOUBLE,
            or_yoy           DOUBLE,
            q_sales_yoy      DOUBLE,
            assets_yoy       DOUBLE,
            equity_yoy       DOUBLE,
            ingested_at      TIMESTAMP,
            source_table     VARCHAR
        )
    """,

    "income": """
        CREATE TABLE IF NOT EXISTS raw_income (
            ts_code      VARCHAR,
            ann_date     VARCHAR,
            f_ann_date   VARCHAR,
            end_date     VARCHAR,
            report_type  VARCHAR,
            comp_type    VARCHAR,
            total_revenue DOUBLE,
            revenue      DOUBLE,
            operate_profit DOUBLE,
            total_profit DOUBLE,
            income_tax   DOUBLE,
            n_income     DOUBLE,
            n_income_attr_p DOUBLE,
            ingested_at  TIMESTAMP,
            source_table VARCHAR
        )
    """,

    "balancesheet": """
        CREATE TABLE IF NOT EXISTS raw_balancesheet (
            ts_code       VARCHAR,
            ann_date      VARCHAR,
            f_ann_date    VARCHAR,
            end_date      VARCHAR,
            report_type   VARCHAR,
            comp_type     VARCHAR,
            total_assets  DOUBLE,
            total_liab    DOUBLE,
            total_hldr_eqy_exc_min_int DOUBLE,
            total_hldr_eqy_inc_min_int DOUBLE,
            money_cap     DOUBLE,
            accounts_receiv DOUBLE,
            inventories   DOUBLE,
            ingested_at   TIMESTAMP,
            source_table  VARCHAR
        )
    """,

    "cashflow": """
        CREATE TABLE IF NOT EXISTS raw_cashflow (
            ts_code           VARCHAR,
            ann_date          VARCHAR,
            f_ann_date        VARCHAR,
            end_date          VARCHAR,
            report_type       VARCHAR,
            comp_type         VARCHAR,
            net_profit        DOUBLE,
            n_cashflow_act    DOUBLE,
            n_cashflow_inv_act DOUBLE,
            n_cash_flows_fnc_act DOUBLE,
            free_cashflow     DOUBLE,
            ingested_at       TIMESTAMP,
            source_table      VARCHAR
        )
    """,

    "forecast": """
        CREATE TABLE IF NOT EXISTS raw_forecast (
            ts_code          VARCHAR,
            ann_date         VARCHAR,
            end_date         VARCHAR,
            type             VARCHAR,
            p_change_min     DOUBLE,
            p_change_max     DOUBLE,
            net_profit_min   DOUBLE,
            net_profit_max   DOUBLE,
            last_parent_net  DOUBLE,
            first_ann_date   VARCHAR,
            summary          VARCHAR,
            change_reason    VARCHAR,
            ingested_at      TIMESTAMP,
            source_table     VARCHAR
        )
    """,

    "express": """
        CREATE TABLE IF NOT EXISTS raw_express (
            ts_code        VARCHAR,
            ann_date       VARCHAR,
            end_date       VARCHAR,
            revenue        DOUBLE,
            operate_profit DOUBLE,
            total_profit   DOUBLE,
            n_income       DOUBLE,
            total_assets   DOUBLE,
            total_hldr_eqy_exc_min_int DOUBLE,
            diluted_eps    DOUBLE,
            diluted_roe    DOUBLE,
            yoy_net_profit DOUBLE,
            ingested_at    TIMESTAMP,
            source_table   VARCHAR
        )
    """,
}

# ---------------------------------------------------------------------------
# Meta schema DDL (dataset_sync_state lives in meta schema namespace)
# ---------------------------------------------------------------------------

META_DDL: dict[str, str] = {
    "meta_schema": "CREATE SCHEMA IF NOT EXISTS meta",

    "dataset_sync_state": """
        CREATE TABLE IF NOT EXISTS meta.dataset_sync_state (
            dataset_id      VARCHAR PRIMARY KEY,
            adapter         VARCHAR NOT NULL,
            last_trade_date VARCHAR,
            last_period_end VARCHAR,
            last_run_at     TIMESTAMP NOT NULL,
            last_status     VARCHAR NOT NULL,
            last_row_count  BIGINT NOT NULL DEFAULT 0,
            error_message   VARCHAR,
            schema_version  INTEGER NOT NULL DEFAULT 1
        )
    """,
}

# ---------------------------------------------------------------------------
# Primary keys per table (used for incremental upsert logic)
# ---------------------------------------------------------------------------

DATASET_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "stock_basic":      ("ts_code",),
    "trade_cal":        ("exchange", "cal_date"),
    "namechange":       ("ts_code", "start_date"),
    "daily":            ("ts_code", "trade_date"),
    "daily_basic":      ("ts_code", "trade_date"),
    "adj_factor":       ("ts_code", "trade_date"),
    "daily_qfq":        ("ts_code", "trade_date"),
    "suspend_d":        ("ts_code", "trade_date"),
    "stk_limit":        ("ts_code", "trade_date"),
    "index_daily":      ("ts_code", "trade_date"),
    "index_weight":     ("index_code", "con_code", "trade_date"),
    "index_member_all": ("ts_code", "l3_code", "in_date"),
    "fina_indicator":   ("ts_code", "end_date", "ann_date"),
    "income":           ("ts_code", "end_date", "ann_date", "report_type"),
    "balancesheet":     ("ts_code", "end_date", "ann_date", "report_type"),
    "cashflow":         ("ts_code", "end_date", "ann_date", "report_type"),
    "forecast":         ("ts_code", "ann_date", "end_date"),
    "express":          ("ts_code", "ann_date", "end_date"),
}

# ---------------------------------------------------------------------------
# Incremental axis per dataset
# "trade_date"  -> use last_trade_date in sync_state for incremental pull
# "period_end"  -> use last_period_end (fundamentals)
# "static"      -> full pull every time (small master tables)
# ---------------------------------------------------------------------------

DATASET_INCREMENTAL_AXIS: dict[str, str] = {
    "stock_basic":      "static",
    "trade_cal":        "trade_date",
    "namechange":       "static",
    "daily":            "trade_date",
    "daily_basic":      "trade_date",
    "adj_factor":       "trade_date",
    "daily_qfq":        "trade_date",
    "suspend_d":        "trade_date",
    "stk_limit":        "trade_date",
    "index_daily":      "trade_date",
    "index_weight":     "trade_date",
    "index_member_all": "static",
    "fina_indicator":   "period_end",
    "income":           "period_end",
    "balancesheet":     "period_end",
    "cashflow":         "period_end",
    "forecast":         "period_end",
    "express":          "period_end",
}

# Convenience: canonical table name for each dataset_id
DATASET_TABLE_NAME: dict[str, str] = {
    "stock_basic":      "stock_basic_ref",
    "trade_cal":        "raw_trade_cal",
    "namechange":       "raw_namechange",
    "daily":            "raw_kline_unadj",
    "daily_basic":      "raw_daily_basic",
    "adj_factor":       "raw_adj_factor",
    "daily_qfq":        "raw_kline_qfq",
    "suspend_d":        "raw_suspend_d",
    "stk_limit":        "raw_stk_limit",
    "index_daily":      "raw_index_daily",
    "index_weight":     "raw_index_weight",
    "index_member_all": "raw_index_member_all",
    "fina_indicator":   "pit_fina_indicator",
    "income":           "raw_income",
    "balancesheet":     "raw_balancesheet",
    "cashflow":         "raw_cashflow",
    "forecast":         "raw_forecast",
    "express":          "raw_express",
}

# All supported dataset ids in dependency order
# (stock_basic first; price data before fundamentals)
ALL_DATASET_IDS: tuple[str, ...] = (
    "stock_basic",
    "trade_cal",
    "namechange",
    "daily",
    "daily_basic",
    "adj_factor",
    "daily_qfq",
    "suspend_d",
    "stk_limit",
    "index_daily",
    "index_weight",
    "index_member_all",
    # 5000-credit datasets last
    "fina_indicator",
    "income",
    "balancesheet",
    "cashflow",
    "forecast",
    "express",
)
