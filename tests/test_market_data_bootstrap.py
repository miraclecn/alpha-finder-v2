from __future__ import annotations

from pathlib import Path
import subprocess
import tempfile
import unittest

import duckdb


def _create_source_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    conn.execute(
        """
        CREATE TABLE stock_basic_ref (
            ts_code VARCHAR,
            symbol VARCHAR,
            name VARCHAR,
            area VARCHAR,
            industry VARCHAR,
            list_date VARCHAR,
            delist_date VARCHAR,
            is_hs VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE raw_namechange (
            ts_code VARCHAR,
            name VARCHAR,
            start_date VARCHAR,
            end_date VARCHAR,
            ann_date VARCHAR,
            change_reason VARCHAR,
            source_table VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE raw_kline_unadj (
            ts_code VARCHAR,
            trade_date VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            pre_close DOUBLE,
            change DOUBLE,
            pct_chg DOUBLE,
            vol DOUBLE,
            amount DOUBLE,
            source_table VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE raw_kline_qfq (
            ts_code VARCHAR,
            trade_date VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            pre_close DOUBLE,
            change DOUBLE,
            pct_chg DOUBLE,
            vol DOUBLE,
            amount DOUBLE,
            source_table VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE raw_adj_factor (
            ts_code VARCHAR,
            trade_date VARCHAR,
            adj_factor DOUBLE,
            source_table VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE raw_daily_basic (
            ts_code VARCHAR,
            trade_date VARCHAR,
            close DOUBLE,
            turnover_rate DOUBLE,
            turnover_rate_f DOUBLE,
            volume_ratio DOUBLE,
            pe DOUBLE,
            pe_ttm DOUBLE,
            pb DOUBLE,
            ps DOUBLE,
            ps_ttm DOUBLE,
            dv_ratio DOUBLE,
            dv_ttm DOUBLE,
            total_share DOUBLE,
            float_share DOUBLE,
            free_share DOUBLE,
            total_mv DOUBLE,
            circ_mv DOUBLE,
            source_table VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE raw_dividend (
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
        """
    )
    conn.execute(
        """
        CREATE TABLE raw_stk_limit (
            ts_code VARCHAR,
            trade_date VARCHAR,
            up_limit DOUBLE,
            down_limit DOUBLE,
            source_table VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE raw_suspend_d (
            ts_code VARCHAR,
            trade_date VARCHAR,
            suspend_timing VARCHAR,
            suspend_type VARCHAR,
            source_table VARCHAR,
            ingested_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE raw_share_float (
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
        """
    )
    conn.execute(
        """
        CREATE TABLE raw_repurchase (
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
        """
    )
    conn.execute(
        """
        CREATE TABLE pit_fina_indicator (
            ts_code VARCHAR,
            ann_date VARCHAR,
            end_date VARCHAR,
            eps DOUBLE,
            roe DOUBLE,
            roa DOUBLE,
            gross_margin DOUBLE,
            netprofit_margin DOUBLE,
            current_ratio DOUBLE,
            debt_to_assets DOUBLE,
            revenue_ps DOUBLE,
            netprofit_yoy DOUBLE,
            dt_netprofit_yoy DOUBLE,
            or_yoy DOUBLE,
            q_sales_yoy DOUBLE,
            assets_yoy DOUBLE,
            equity_yoy DOUBLE
        )
        """
    )

    conn.execute(
        """
        INSERT INTO stock_basic_ref VALUES
        ('000001.SZ', '000001', '平安银行', '深圳', '银行', '19910403', NULL, 'N', TIMESTAMP '2026-04-22 15:00:00'),
        ('300001.SZ', '300001', '特锐德', '青岛', '电气设备', '20091030', NULL, 'N', TIMESTAMP '2026-04-22 15:00:00'),
        ('688001.SH', '688001', '华兴源创', '苏州', '专用机械', '20190722', NULL, 'N', TIMESTAMP '2026-04-22 15:00:00'),
        ('920001.BJ', '920001', '北交样本', '北京', '专用机械', '20240110', NULL, 'N', TIMESTAMP '2026-04-22 15:00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO raw_namechange VALUES
        ('000001.SZ', '平安银行', '19910403', NULL, '19910403', '证券简称变更', 'tushare.namechange', TIMESTAMP '2026-04-22 15:00:00'),
        ('300001.SZ', 'ST特锐德', '20240102', '20240103', '20240101', 'ST', 'tushare.namechange', TIMESTAMP '2026-04-22 15:00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO raw_kline_unadj VALUES
        ('000001.SZ', '20240102', 10.0, 10.5, 9.9, 10.2, 10.0, 0.2, 2.0, 100.0, 200.0, 'tushare.daily', TIMESTAMP '2026-04-22 15:00:00'),
        ('000001.SZ', '20240103', 10.3, 10.6, 10.1, 10.4, 10.2, 0.2, 1.9608, 110.0, 220.0, 'tushare.daily', TIMESTAMP '2026-04-22 15:00:00'),
        ('300001.SZ', '20240102', 20.0, 21.0, 19.5, 20.5, 20.0, 0.5, 2.5, 90.0, 180.0, 'tushare.daily', TIMESTAMP '2026-04-22 15:00:00'),
        ('688001.SH', '20240102', 30.0, 31.5, 29.8, 31.0, 30.0, 1.0, 3.3333, 80.0, 240.0, 'tushare.daily', TIMESTAMP '2026-04-22 15:00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO raw_kline_qfq VALUES
        ('000001.SZ', '20240102', 15.0, 15.75, 14.85, 15.3, 15.0, 0.3, 2.0, 100.0, 200.0, 'tushare.daily:qfq_repair', TIMESTAMP '2026-04-22 15:00:00'),
        ('000001.SZ', '20240103', 16.48, 16.96, 16.16, 16.64, 16.32, 0.32, 1.9608, 110.0, 220.0, 'tushare.daily:qfq_repair', TIMESTAMP '2026-04-22 15:00:00'),
        ('300001.SZ', '20240102', 40.0, 42.0, 39.0, 41.0, 40.0, 1.0, 2.5, 90.0, 180.0, 'tushare.daily:qfq_repair', TIMESTAMP '2026-04-22 15:00:00'),
        ('688001.SH', '20240102', 36.0, 37.8, 35.76, 37.2, 36.0, 1.2, 3.3333, 80.0, 240.0, 'tushare.daily:qfq_repair', TIMESTAMP '2026-04-22 15:00:00'),
        ('920001.BJ', '20240110', 16.5, 17.05, 16.28, 16.72, 16.5, 0.22, 1.3333, 70.0, 105.0, 'tushare.daily:qfq_repair', TIMESTAMP '2026-04-22 15:00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO raw_adj_factor VALUES
        ('000001.SZ', '20240102', 1.5, 'tushare.adj_factor', TIMESTAMP '2026-04-22 15:00:00'),
        ('000001.SZ', '20240103', 1.6, 'tushare.adj_factor', TIMESTAMP '2026-04-22 15:00:00'),
        ('300001.SZ', '20240102', 2.0, 'tushare.adj_factor', TIMESTAMP '2026-04-22 15:00:00'),
        ('688001.SH', '20240102', 1.2, 'tushare.adj_factor', TIMESTAMP '2026-04-22 15:00:00'),
        ('920001.BJ', '20240110', 1.1, 'tushare.adj_factor', TIMESTAMP '2026-04-22 15:00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO raw_daily_basic VALUES
        ('000001.SZ', '20240102', 10.2, 1.0, 1.2, 0.8, 8.0, 7.8, 1.1, 2.2, 2.1, 0.5, 0.4, 1000.0, 900.0, 800.0, 12000.0, 9000.0, 'tushare.daily_basic', TIMESTAMP '2026-04-22 15:00:00'),
        ('000001.SZ', '20240103', 10.4, 1.1, 1.3, 0.9, 8.1, 7.9, 1.2, 2.3, 2.2, 0.5, 0.4, 1010.0, 910.0, 810.0, 12100.0, 9100.0, 'tushare.daily_basic', TIMESTAMP '2026-04-22 15:00:00'),
        ('300001.SZ', '20240102', 20.5, 2.0, 2.2, 1.1, 30.0, 29.0, 4.0, 5.0, 4.8, 0.0, 0.0, 200.0, 180.0, 150.0, 4200.0, 3075.0, 'tushare.daily_basic', TIMESTAMP '2026-04-22 15:00:00'),
        ('688001.SH', '20240102', 31.0, 3.0, 3.5, 1.4, 60.0, 58.0, 6.0, 7.0, 6.8, 0.0, 0.0, 300.0, 250.0, 220.0, 9300.0, 6820.0, 'tushare.daily_basic', TIMESTAMP '2026-04-22 15:00:00'),
        ('920001.BJ', '20240110', 15.2, 4.0, 4.5, 1.0, 25.0, 24.0, 3.0, 4.0, 3.9, 0.0, 0.0, 150.0, 120.0, 110.0, 2280.0, 1672.0, 'tushare.daily_basic', TIMESTAMP '2026-04-22 15:00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO pit_fina_indicator VALUES
        ('000001.SZ', '20240102', '20231231', 1.23, 10.5, 0.8, 35.0, 18.0, 1.5, 70.0, 3.21, 12.0, 10.0, 8.0, 7.0, 6.0, 5.0),
        ('300001.SZ', '20240103', '20231231', 0.88, 9.5, 0.7, 30.0, 15.0, 1.3, 45.0, 2.11, 22.0, 20.0, 18.0, 17.0, 16.0, 15.0)
        """
    )
    conn.execute(
        """
        INSERT INTO raw_dividend VALUES
        ('000001.SZ', '20231231', '20231231', '实施', 0.0, 0.0, 0.0, 0.19, 0.20, '20240102', '20240103', '20240103', NULL, 'tushare.dividend', TIMESTAMP '2026-04-22 15:00:00'),
        ('300001.SZ', '20231231', '20231231', '实施', 0.0, 0.1, 0.2, 0.0, 0.0, '20240102', '20240103', NULL, '20240103', 'tushare.dividend', TIMESTAMP '2026-04-22 15:00:00'),
        ('688001.SH', '20231231', '20231231', '预案', 0.0, 0.0, 0.0, 0.10, 0.10, '20240102', '20240103', '20240103', NULL, 'tushare.dividend', TIMESTAMP '2026-04-22 15:00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO raw_stk_limit VALUES
        ('000001.SZ', '20240103', 11.22, 9.18, 'tushare.stk_limit', TIMESTAMP '2026-04-22 15:00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO raw_suspend_d VALUES
        ('300001.SZ', '20240102', '全天停牌', '停牌', 'tushare.suspend_d', TIMESTAMP '2026-04-22 15:00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO raw_share_float VALUES
        ('000001.SZ', '20240101', '20240108', 100.0, 1.0, 'holder', '首发原股东限售股份', 'tushare.share_float', TIMESTAMP '2026-04-22 15:00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO raw_repurchase VALUES
        ('000001.SZ', '20240101', '20240109', '实施', '20240131', 10.0, 100.0, 12.0, 8.0, 'tushare.repurchase', TIMESTAMP '2026-04-22 15:00:00')
        """
    )
    conn.close()


def _create_supplemental_pit_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    conn.execute(
        """
        CREATE TABLE industry_classification_pit (
            security_id VARCHAR,
            industry_schema VARCHAR,
            industry_code VARCHAR,
            effective_at VARCHAR,
            removed_at VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE benchmark_membership_pit (
            benchmark_id VARCHAR,
            security_id VARCHAR,
            effective_at VARCHAR,
            removed_at VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE benchmark_weight_snapshot_pit (
            benchmark_id VARCHAR,
            security_id VARCHAR,
            trade_date VARCHAR,
            weight DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO industry_classification_pit VALUES
        ('000001.SZ', 'citics_l1', 'bank', '20200101', NULL),
        ('300001.SZ', 'citics_l1', 'industrial', '20200101', NULL),
        ('688001.SH', 'citics_l1', 'tech', '20200101', NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO benchmark_membership_pit VALUES
        ('CSI 800', '000001.SZ', '20200101', NULL),
        ('CSI 800', '300001.SZ', '20200101', NULL),
        ('CSI 800', '688001.SH', '20200101', NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO benchmark_weight_snapshot_pit VALUES
        ('CSI 800', '000001.SZ', '20240102', 65.0),
        ('CSI 800', '300001.SZ', '20240102', 35.0),
        ('CSI 800', '000001.SZ', '20240103', 55.0),
        ('CSI 800', '688001.SH', '20240103', 45.0)
        """
    )
    conn.close()


class MarketDataBootstrapTest(unittest.TestCase):
    def test_build_research_source_db_materializes_green_and_amber_tables(self) -> None:
        from alpha_find_v2.market_data_bootstrap import build_research_source_db

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "source.duckdb"
            target_db = temp_root / "target.duckdb"
            _create_source_db(source_db)

            build_research_source_db(source_db, target_db)

            conn = duckdb.connect(str(target_db), read_only=True)
            registry_rows = conn.execute(
                "SELECT dataset_id, status FROM dataset_registry ORDER BY dataset_id"
            ).fetchall()
            self.assertEqual(
                registry_rows,
                [
                    ("corporate_action_exception_ledger", "green"),
                    ("corporate_action_ledger", "green"),
                    ("daily_bar_pit", "green"),
                    ("fundamental_snapshot_pit", "amber"),
                    ("industry_classification_static", "amber"),
                    ("market_trade_calendar", "green"),
                    ("name_change_history", "green"),
                    ("security_master_ref", "green"),
                    ("tradeability_state_daily", "green"),
                ],
            )
            daily_row = conn.execute(
                """
                SELECT
                    security_id,
                    trade_date,
                    board,
                    is_st,
                    volume_shares,
                    turnover_value_cny,
                    free_float_shares,
                    float_mcap_cny,
                    close_adj
                FROM daily_bar_pit
                WHERE security_id = '300001.SZ' AND trade_date = '20240102'
                """
            ).fetchone()
            self.assertEqual(daily_row[0], "300001.SZ")
            self.assertEqual(daily_row[1], "20240102")
            self.assertEqual(daily_row[2], "chinext")
            self.assertTrue(daily_row[3])
            self.assertAlmostEqual(daily_row[4], 9000.0)
            self.assertAlmostEqual(daily_row[5], 180000.0)
            self.assertAlmostEqual(daily_row[6], 1500000.0)
            self.assertAlmostEqual(daily_row[7], 30750000.0)
            self.assertAlmostEqual(daily_row[8], 41.0)

            market_dates = conn.execute(
                "SELECT trade_date FROM market_trade_calendar ORDER BY trade_date"
            ).fetchall()
            self.assertEqual(market_dates, [("20240102",), ("20240103",), ("20240110",)])
            conn.close()

    def test_build_research_source_db_materializes_corporate_action_exception_ledger(self) -> None:
        from alpha_find_v2.market_data_bootstrap import build_research_source_db

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "source.duckdb"
            target_db = temp_root / "target.duckdb"
            _create_source_db(source_db)
            conn = duckdb.connect(str(source_db))
            try:
                conn.execute("DELETE FROM raw_dividend WHERE ts_code = '688001.SH'")
                conn.execute(
                    """
                    INSERT INTO raw_kline_unadj VALUES
                    ('688001.SH', '20240103', 27.8, 28.4, 27.1, 27.5, 27.678571428571427,
                     -3.5, -11.2903, 82.0, 225.5, 'tushare.daily',
                     TIMESTAMP '2026-04-22 15:00:00')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO raw_adj_factor VALUES
                    ('688001.SH', '20240103', 1.344, 'tushare.adj_factor',
                     TIMESTAMP '2026-04-22 15:00:00')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO raw_daily_basic VALUES
                    ('688001.SH', '20240103', 27.5, 3.1, 3.6, 1.5, 55.0, 53.0, 5.8,
                     6.7, 6.5, 0.0, 0.0, 300.0, 250.0, 220.0, 8250.0, 6050.0,
                     'tushare.daily_basic', TIMESTAMP '2026-04-22 15:00:00')
                    """
                )
            finally:
                conn.close()

            build_research_source_db(source_db, target_db)

            conn = duckdb.connect(str(target_db), read_only=True)
            try:
                row = conn.execute(
                    """
                    SELECT
                        security_id,
                        previous_trade_date,
                        trade_date,
                        round(factor_ratio, 6),
                        magnitude_bucket,
                        severity,
                        has_suspend_window,
                        triage_class,
                        recommended_action
                    FROM corporate_action_exception_ledger
                    WHERE security_id = '688001.SH'
                    """
                ).fetchone()
                self.assertEqual(
                    row,
                    (
                        "688001.SH",
                        "20240102",
                        "20240103",
                        1.12,
                        ">10pct",
                        "critical",
                        False,
                        "daily_pre_close_ex_right_without_ledger",
                        "quarantine_security_window_from_promotion",
                    ),
                )
                registry_row = conn.execute(
                    """
                    SELECT status
                    FROM dataset_registry
                    WHERE dataset_id = 'corporate_action_exception_ledger'
                    """
                ).fetchone()
                self.assertEqual(registry_row, ("amber",))
            finally:
                conn.close()

    def test_build_research_source_db_sets_conservative_available_date_for_fundamentals(self) -> None:
        from alpha_find_v2.market_data_bootstrap import build_research_source_db

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "source.duckdb"
            target_db = temp_root / "target.duckdb"
            _create_source_db(source_db)

            build_research_source_db(source_db, target_db)

            conn = duckdb.connect(str(target_db), read_only=True)
            rows = conn.execute(
                """
                SELECT security_id, announcement_date, available_date, period_end, roe
                FROM fundamental_snapshot_pit
                ORDER BY security_id
                """
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    ("000001.SZ", "20240102", "20240103", "20231231", 10.5),
                    ("300001.SZ", "20240103", "20240110", "20231231", 9.5),
                ],
            )
            industry_rows = conn.execute(
                """
                SELECT security_id, industry_name, classification_scope
                FROM industry_classification_static
                ORDER BY security_id
                """
            ).fetchall()
            self.assertEqual(
                industry_rows,
                [
                    ("000001.SZ", "银行", "current_static"),
                    ("300001.SZ", "电气设备", "current_static"),
                    ("688001.SH", "专用机械", "current_static"),
                    ("920001.BJ", "专用机械", "current_static"),
                ],
            )
            conn.close()

    def test_build_research_source_db_deduplicates_overlapping_st_name_windows(self) -> None:
        from alpha_find_v2.market_data_bootstrap import build_research_source_db

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "source.duckdb"
            target_db = temp_root / "target.duckdb"
            _create_source_db(source_db)

            conn = duckdb.connect(str(source_db))
            conn.execute(
                """
                INSERT INTO raw_namechange VALUES
                ('300001.SZ', 'ST特锐德', '20240102', NULL, '20240101', 'ST', 'tushare.namechange', TIMESTAMP '2026-04-22 15:00:00')
                """
            )
            conn.close()

            build_research_source_db(source_db, target_db)

            conn = duckdb.connect(str(target_db), read_only=True)
            duplicate_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT security_id, trade_date, COUNT(*) AS c
                    FROM daily_bar_pit
                    GROUP BY 1, 2
                    HAVING c > 1
                )
                """
            ).fetchone()[0]
            rows = conn.execute(
                """
                SELECT security_id, trade_date, is_st
                FROM daily_bar_pit
                WHERE security_id = '300001.SZ'
                ORDER BY trade_date
                """
            ).fetchall()
            self.assertEqual(duplicate_count, 0)
            self.assertEqual(rows, [("300001.SZ", "20240102", True)])
            conn.close()

    def test_build_research_source_db_uses_qfq_fallback_when_unadjusted_bar_is_missing(self) -> None:
        from alpha_find_v2.market_data_bootstrap import build_research_source_db

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "source.duckdb"
            target_db = temp_root / "target.duckdb"
            _create_source_db(source_db)

            build_research_source_db(source_db, target_db)

            conn = duckdb.connect(str(target_db), read_only=True)
            row = conn.execute(
                """
                SELECT
                    security_id,
                    trade_date,
                    price_basis,
                    open,
                    close,
                    open_adj,
                    close_adj,
                    volume_shares,
                    turnover_value_cny
                FROM daily_bar_pit
                WHERE security_id = '920001.BJ' AND trade_date = '20240110'
                """
            ).fetchone()
            self.assertEqual(
                row,
                (
                    "920001.BJ",
                    "20240110",
                    "qfq_fallback",
                    16.5,
                    16.72,
                    16.5,
                    16.72,
                    7000.0,
                    105000.0,
                ),
            )
            conn.close()

    def test_build_research_source_db_does_not_treat_static_qfq_as_pit_truth(self) -> None:
        from alpha_find_v2.market_data_bootstrap import build_research_source_db

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "source.duckdb"
            target_db = temp_root / "target.duckdb"
            _create_source_db(source_db)
            conn = duckdb.connect(str(source_db))
            conn.execute(
                """
                UPDATE raw_kline_qfq
                SET open = 9.0,
                    high = 9.5,
                    low = 8.8,
                    close = 9.2
                WHERE ts_code = '000001.SZ'
                  AND trade_date = '20240102'
                """
            )
            conn.close()

            build_research_source_db(source_db, target_db)

            conn = duckdb.connect(str(target_db), read_only=True)
            row = conn.execute(
                """
                SELECT open, high, low, close, open_adj, high_adj, low_adj, close_adj, adjusted_price_source
                FROM daily_bar_pit
                WHERE security_id = '000001.SZ'
                  AND trade_date = '20240102'
                """
            ).fetchone()
            self.assertEqual(row[:4], (10.0, 10.5, 9.9, 10.2))
            self.assertAlmostEqual(row[4], 15.0)
            self.assertAlmostEqual(row[5], 15.75)
            self.assertAlmostEqual(row[6], 14.85)
            self.assertAlmostEqual(row[7], 15.3)
            self.assertEqual(row[8], "raw_times_adj_factor")
            conn.close()

    def test_build_research_source_db_derives_corporate_action_ledger_and_tradeability(self) -> None:
        from alpha_find_v2.market_data_bootstrap import build_research_source_db

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "source.duckdb"
            target_db = temp_root / "target.duckdb"
            _create_source_db(source_db)

            build_research_source_db(source_db, target_db)

            conn = duckdb.connect(str(target_db), read_only=True)
            ledger_rows = conn.execute(
                """
                SELECT security_id, action_type, record_date, book_date, ex_date, cash_per_share, share_ratio
                FROM corporate_action_ledger
                ORDER BY security_id, action_type
                """
            ).fetchall()
            self.assertEqual(
                ledger_rows,
                [
                    ("000001.SZ", "cash_dividend", "20240102", "20240103", "20240103", 0.20, 0.0),
                    ("300001.SZ", "share_dividend", "20240102", "20240103", "20240103", 0.0, 0.30000000000000004),
                ],
            )
            tradeability_rows = conn.execute(
                """
                SELECT
                    security_id,
                    trade_date,
                    is_suspended,
                    up_limit,
                    down_limit,
                    is_limit_up_open_lock,
                    is_limit_down_open_lock,
                    source_priority
                FROM tradeability_state_daily
                WHERE (security_id, trade_date) IN (
                    ('000001.SZ', '20240103'),
                    ('300001.SZ', '20240102')
                )
                ORDER BY security_id, trade_date
                """
            ).fetchall()
            self.assertEqual(
                tradeability_rows,
                [
                    ("000001.SZ", "20240103", False, 11.22, 9.18, False, False, "official"),
                    ("300001.SZ", "20240102", True, None, None, False, False, "official"),
                ],
            )
            conn.close()

    def test_build_research_source_db_deduplicates_official_tradeability_keys(self) -> None:
        from alpha_find_v2.market_data_bootstrap import build_research_source_db

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "source.duckdb"
            target_db = temp_root / "target.duckdb"
            _create_source_db(source_db)
            conn = duckdb.connect(str(source_db))
            try:
                conn.execute(
                    """
                    INSERT INTO raw_suspend_d VALUES
                    ('300001.SZ', '20240102', '临时停牌', '停牌',
                     'tushare.suspend_d', TIMESTAMP '2026-04-22 15:00:00')
                    """
                )
            finally:
                conn.close()

            build_research_source_db(source_db, target_db)

            conn = duckdb.connect(str(target_db), read_only=True)
            try:
                daily_rows, tradeability_rows = conn.execute(
                    """
                    SELECT
                        (SELECT count(*) FROM daily_bar_pit),
                        (SELECT count(*) FROM tradeability_state_daily)
                    """
                ).fetchone()
                duplicate_rows = conn.execute(
                    """
                    SELECT count(*)
                    FROM (
                        SELECT security_id, trade_date
                        FROM tradeability_state_daily
                        GROUP BY security_id, trade_date
                        HAVING count(*) > 1
                    )
                    """
                ).fetchone()[0]
                self.assertEqual(tradeability_rows, daily_rows)
                self.assertEqual(duplicate_rows, 0)
            finally:
                conn.close()

    def test_build_research_source_db_imports_staged_pit_reference_tables(self) -> None:
        from alpha_find_v2.market_data_bootstrap import build_research_source_db

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "source.duckdb"
            supplemental_db = temp_root / "supplemental.duckdb"
            target_db = temp_root / "target.duckdb"
            _create_source_db(source_db)
            _create_supplemental_pit_db(supplemental_db)

            build_research_source_db(
                source_db,
                target_db,
                supplemental_db=supplemental_db,
            )

            conn = duckdb.connect(str(target_db), read_only=True)
            registry_rows = conn.execute(
                """
                SELECT dataset_id, status
                FROM dataset_registry
                WHERE dataset_id IN (
                    'benchmark_membership_pit',
                    'benchmark_weight_snapshot_pit',
                    'industry_classification_pit'
                )
                ORDER BY dataset_id
                """
            ).fetchall()
            self.assertEqual(
                registry_rows,
                [
                    ("benchmark_membership_pit", "green"),
                    ("benchmark_weight_snapshot_pit", "green"),
                    ("industry_classification_pit", "green"),
                ],
            )
            membership_rows = conn.execute(
                """
                SELECT benchmark_id, security_id
                FROM benchmark_membership_pit
                ORDER BY benchmark_id, security_id
                """
            ).fetchall()
            self.assertEqual(
                membership_rows,
                [
                    ("CSI 800", "000001.SZ"),
                    ("CSI 800", "300001.SZ"),
                    ("CSI 800", "688001.SH"),
                ],
            )
            industry_rows = conn.execute(
                """
                SELECT security_id, industry_schema, industry_code
                FROM industry_classification_pit
                ORDER BY security_id
                """
            ).fetchall()
            self.assertEqual(
                industry_rows,
                [
                    ("000001.SZ", "citics_l1", "bank"),
                    ("300001.SZ", "citics_l1", "industrial"),
                    ("688001.SH", "citics_l1", "tech"),
                ],
            )
            weight_rows = conn.execute(
                """
                SELECT benchmark_id, security_id, trade_date, weight
                FROM benchmark_weight_snapshot_pit
                ORDER BY benchmark_id, trade_date, security_id
                """
            ).fetchall()
            self.assertEqual(
                weight_rows,
                [
                    ("CSI 800", "000001.SZ", "20240102", 65.0),
                    ("CSI 800", "300001.SZ", "20240102", 35.0),
                    ("CSI 800", "000001.SZ", "20240103", 55.0),
                    ("CSI 800", "688001.SH", "20240103", 45.0),
                ],
            )
            conn.close()

    def test_build_research_source_db_writes_phase1_boundary_registries(self) -> None:
        from alpha_find_v2.market_data_bootstrap import build_research_source_db

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "source.duckdb"
            supplemental_db = temp_root / "supplemental.duckdb"
            target_db = temp_root / "target.duckdb"
            _create_source_db(source_db)
            _create_supplemental_pit_db(supplemental_db)

            build_research_source_db(
                source_db,
                target_db,
                supplemental_db=supplemental_db,
            )

            conn = duckdb.connect(str(target_db), read_only=True)
            spine_rows = conn.execute(
                """
                SELECT surface_id, provider, boundary_role, path
                FROM data_spine_registry
                ORDER BY surface_id
                """
            ).fetchall()
            self.assertEqual(
                spine_rows,
                [
                    (
                        "source_market_db",
                        "audited_v1_tushare_market_source",
                        "external_source",
                        str(source_db.resolve()),
                    ),
                    (
                        "supplemental_reference_db",
                        "tushare_reference_staging",
                        "pit_reference_staging",
                        str(supplemental_db.resolve()),
                    ),
                    (
                        "target_research_db",
                        "v2_isolated_research_db",
                        "isolated_v2_research_surface",
                        str(target_db.resolve()),
                    ),
                ],
            )
            build_chain_rows = conn.execute(
                """
                SELECT step_order, command_id, boundary_role
                FROM build_chain_registry
                ORDER BY step_order
                """
            ).fetchall()
            self.assertEqual(
                build_chain_rows,
                [
                    (1, "build-reference-staging-db", "required_entrypoint"),
                    (2, "build-research-source-db", "required_entrypoint"),
                    (3, "build-benchmark-state", "required_entrypoint"),
                ],
            )
            boundary_rows = conn.execute(
                """
                SELECT category, entry_id, decision
                FROM data_boundary_registry
                WHERE (category, entry_id) IN (
                    ('allowed_reuse', 'daily_bar_pit'),
                    ('allowed_reuse', 'benchmark_membership_pit'),
                    ('forbidden_reuse', 'v1_factor_outputs'),
                    ('known_gap', 'exact_limit_state_reconstruction'),
                    ('audit_rule', 'akshare_field_requires_explicit_v2_audit'),
                    ('failure_condition', 'akshare_unaudited_in_production_path')
                )
                ORDER BY category, entry_id
                """
            ).fetchall()
            self.assertEqual(
                boundary_rows,
                [
                    ("allowed_reuse", "benchmark_membership_pit", "allow"),
                    ("allowed_reuse", "daily_bar_pit", "allow"),
                    ("audit_rule", "akshare_field_requires_explicit_v2_audit", "required"),
                    (
                        "failure_condition",
                        "akshare_unaudited_in_production_path",
                        "stop",
                    ),
                    ("forbidden_reuse", "v1_factor_outputs", "forbid"),
                    ("known_gap", "exact_limit_state_reconstruction", "visible_gap"),
                ],
            )
            conn.close()

    def test_cli_build_research_source_db_writes_target_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "source.duckdb"
            target_db = temp_root / "target.duckdb"
            _create_source_db(source_db)

            result = subprocess.run(
                [
                    "python3",
                    "-m",
                    "alpha_find_v2",
                    "build-research-source-db",
                    "--source-db",
                    str(source_db),
                    "--target-db",
                    str(target_db),
                ],
                cwd=Path(__file__).resolve().parents[1],
                env={"PYTHONPATH": "src"},
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(target_db.exists())


if __name__ == "__main__":
    unittest.main()
