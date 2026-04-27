from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import duckdb
import pandas as pd


class _FakeTushareClient:
    def __init__(self) -> None:
        self.member_calls: list[dict[str, object]] = []
        self.weight_calls: list[dict[str, object]] = []

    def index_member_all(self, **kwargs: object) -> pd.DataFrame:
        self.member_calls.append(dict(kwargs))
        offset = int(kwargs.get("offset", 0))
        if offset == 0:
            return pd.DataFrame(
                [
                    {
                        "l1_code": "801010.SI",
                        "l1_name": "农林牧渔",
                        "l2_code": "801012.SI",
                        "l2_name": "农产品加工",
                        "l3_code": "850151.SI",
                        "l3_name": "果蔬加工",
                        "ts_code": "000001.SZ",
                        "name": "平安银行",
                        "in_date": "20200101",
                        "out_date": None,
                        "is_new": "Y",
                    },
                    {
                        "l1_code": "801080.SI",
                        "l1_name": "电子",
                        "l2_code": "801081.SI",
                        "l2_name": "半导体",
                        "l3_code": "850811.SI",
                        "l3_name": "数字芯片设计",
                        "ts_code": "300001.SZ",
                        "name": "特锐德",
                        "in_date": "20200101",
                        "out_date": None,
                        "is_new": "Y",
                    },
                ]
            )
        if offset == 2:
            return pd.DataFrame(
                [
                    {
                        "l1_code": "801080.SI",
                        "l1_name": "电子",
                        "l2_code": "801081.SI",
                        "l2_name": "半导体",
                        "l3_code": "850811.SI",
                        "l3_name": "数字芯片设计",
                        "ts_code": "688001.SH",
                        "name": "华兴源创",
                        "in_date": "20240103",
                        "out_date": None,
                        "is_new": "Y",
                    },
                ]
            )
        return pd.DataFrame(
            columns=[
                "l1_code",
                "l1_name",
                "l2_code",
                "l2_name",
                "l3_code",
                "l3_name",
                "ts_code",
                "name",
                "in_date",
                "out_date",
                "is_new",
            ]
        )

    def index_weight(self, **kwargs: object) -> pd.DataFrame:
        self.weight_calls.append(dict(kwargs))
        start_date = str(kwargs["start_date"])
        if start_date == "20240101":
            return pd.DataFrame(
                [
                    {
                        "index_code": "000906.SH",
                        "con_code": "000001.SZ",
                        "trade_date": "20240131",
                        "weight": 60.0,
                    },
                    {
                        "index_code": "000906.SH",
                        "con_code": "300001.SZ",
                        "trade_date": "20240131",
                        "weight": 40.0,
                    },
                ]
            )
        if start_date == "20240201":
            return pd.DataFrame(
                [
                    {
                        "index_code": "000906.SH",
                        "con_code": "000001.SZ",
                        "trade_date": "20240229",
                        "weight": 55.0,
                    },
                    {
                        "index_code": "000906.SH",
                        "con_code": "688001.SH",
                        "trade_date": "20240229",
                        "weight": 45.0,
                    },
                ]
            )
        return pd.DataFrame(columns=["index_code", "con_code", "trade_date", "weight"])


class ReferenceDataStagingTest(unittest.TestCase):
    def test_build_tushare_reference_db_materializes_pit_tables(self) -> None:
        from alpha_find_v2.reference_data_staging import (
            BenchmarkReferenceDefinition,
            build_tushare_reference_db,
        )

        client = _FakeTushareClient()
        with tempfile.TemporaryDirectory() as temp_dir:
            target_db = Path(temp_dir) / "pit_reference.duckdb"

            summary = build_tushare_reference_db(
                target_db=target_db,
                benchmarks=[
                    BenchmarkReferenceDefinition(
                        benchmark_id="CSI 800",
                        index_code="000906.SH",
                    )
                ],
                start_date="20240101",
                end_date="20240229",
                client=client,
                industry_levels=("L1", "L2", "L3"),
                index_weight_window_months=1,
                member_page_size=2,
            )

            self.assertEqual(summary["industry_rows"], 9)
            self.assertEqual(summary["weight_rows"], 4)
            self.assertEqual(summary["membership_rows"], 3)
            self.assertEqual(len(client.member_calls), 2)
            self.assertEqual(
                [call["start_date"] for call in client.weight_calls],
                ["20240101", "20240201"],
            )

            conn = duckdb.connect(str(target_db), read_only=True)
            industry_rows = conn.execute(
                """
                SELECT security_id, industry_schema, industry_code, effective_at, removed_at
                FROM industry_classification_pit
                ORDER BY security_id, industry_schema
                """
            ).fetchall()
            self.assertEqual(
                industry_rows,
                [
                    ("000001.SZ", "sw2021_l1", "801010.SI", "20200101", None),
                    ("000001.SZ", "sw2021_l2", "801012.SI", "20200101", None),
                    ("000001.SZ", "sw2021_l3", "850151.SI", "20200101", None),
                    ("300001.SZ", "sw2021_l1", "801080.SI", "20200101", None),
                    ("300001.SZ", "sw2021_l2", "801081.SI", "20200101", None),
                    ("300001.SZ", "sw2021_l3", "850811.SI", "20200101", None),
                    ("688001.SH", "sw2021_l1", "801080.SI", "20240103", None),
                    ("688001.SH", "sw2021_l2", "801081.SI", "20240103", None),
                    ("688001.SH", "sw2021_l3", "850811.SI", "20240103", None),
                ],
            )
            membership_rows = conn.execute(
                """
                SELECT benchmark_id, security_id, effective_at, removed_at
                FROM benchmark_membership_pit
                ORDER BY benchmark_id, security_id, effective_at
                """
            ).fetchall()
            self.assertEqual(
                membership_rows,
                [
                    ("CSI 800", "000001.SZ", "20240131", None),
                    ("CSI 800", "300001.SZ", "20240131", "20240229"),
                    ("CSI 800", "688001.SH", "20240229", None),
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
                    ("CSI 800", "000001.SZ", "20240131", 60.0),
                    ("CSI 800", "300001.SZ", "20240131", 40.0),
                    ("CSI 800", "000001.SZ", "20240229", 55.0),
                    ("CSI 800", "688001.SH", "20240229", 45.0),
                ],
            )
            conn.close()

    def test_build_tushare_reference_db_closes_overlapping_industry_intervals(self) -> None:
        from alpha_find_v2.reference_data_staging import (
            BenchmarkReferenceDefinition,
            build_tushare_reference_db,
        )

        class _OverlappingIndustryClient:
            def index_member_all(self, **kwargs: object) -> pd.DataFrame:
                offset = int(kwargs.get("offset", 0))
                if offset > 0:
                    return pd.DataFrame(
                        columns=[
                            "l1_code",
                            "l1_name",
                            "l2_code",
                            "l2_name",
                            "l3_code",
                            "l3_name",
                            "ts_code",
                            "name",
                            "in_date",
                            "out_date",
                            "is_new",
                        ]
                    )
                return pd.DataFrame(
                    [
                        {
                            "l1_code": "801120.SI",
                            "l1_name": "食品饮料",
                            "l2_code": "801128.SI",
                            "l2_name": "休闲食品",
                            "l3_code": "851281.SI",
                            "l3_name": "零食",
                            "ts_code": "300972.SZ",
                            "name": "万辰集团",
                            "in_date": "20240730",
                            "out_date": None,
                            "is_new": "Y",
                        },
                        {
                            "l1_code": "801200.SI",
                            "l1_name": "商贸零售",
                            "l2_code": "801203.SI",
                            "l2_name": "一般零售",
                            "l3_code": "852032.SI",
                            "l3_name": "超市",
                            "ts_code": "300972.SZ",
                            "name": "万辰集团",
                            "in_date": "20260305",
                            "out_date": None,
                            "is_new": "Y",
                        },
                    ]
                )

            def index_weight(self, **kwargs: object) -> pd.DataFrame:
                return pd.DataFrame(
                    [
                        {
                            "index_code": "000906.SH",
                            "con_code": "300972.SZ",
                            "trade_date": "20260331",
                            "weight": 0.051,
                        }
                    ]
                )

        with tempfile.TemporaryDirectory() as temp_dir:
            target_db = Path(temp_dir) / "pit_reference.duckdb"
            build_tushare_reference_db(
                target_db=target_db,
                benchmarks=[
                    BenchmarkReferenceDefinition(
                        benchmark_id="CSI 800",
                        index_code="000906.SH",
                    )
                ],
                start_date="20260301",
                end_date="20260331",
                client=_OverlappingIndustryClient(),
                industry_levels=("L1",),
                index_weight_window_months=1,
            )

            conn = duckdb.connect(str(target_db), read_only=True)
            rows = conn.execute(
                """
                SELECT security_id, industry_schema, industry_code, effective_at, removed_at
                FROM industry_classification_pit
                ORDER BY effective_at
                """
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    ("300972.SZ", "sw2021_l1", "801120.SI", "20240730", "20260305"),
                    ("300972.SZ", "sw2021_l1", "801200.SI", "20260305", None),
                ],
            )
            conn.close()

    def test_build_tushare_reference_db_writes_reference_dataset_registry(self) -> None:
        from alpha_find_v2.reference_data_staging import (
            BenchmarkReferenceDefinition,
            build_tushare_reference_db,
        )

        client = _FakeTushareClient()
        with tempfile.TemporaryDirectory() as temp_dir:
            target_db = Path(temp_dir) / "pit_reference.duckdb"

            build_tushare_reference_db(
                target_db=target_db,
                benchmarks=[
                    BenchmarkReferenceDefinition(
                        benchmark_id="CSI 800",
                        index_code="000906.SH",
                    )
                ],
                start_date="20240101",
                end_date="20240229",
                client=client,
                industry_levels=("L1", "L2", "L3"),
                index_weight_window_months=1,
                member_page_size=2,
            )

            conn = duckdb.connect(str(target_db), read_only=True)
            registry_rows = conn.execute(
                """
                SELECT
                    dataset_id,
                    source_provider,
                    boundary_role,
                    status,
                    row_count,
                    earliest_date,
                    latest_date
                FROM reference_dataset_registry
                ORDER BY dataset_id
                """
            ).fetchall()
            self.assertEqual(
                registry_rows,
                [
                    (
                        "benchmark_membership_pit",
                        "tushare",
                        "pit_reference_staging",
                        "green",
                        3,
                        "20240131",
                        "20240229",
                    ),
                    (
                        "benchmark_weight_snapshot_pit",
                        "tushare",
                        "pit_reference_staging",
                        "green",
                        4,
                        "20240131",
                        "20240229",
                    ),
                    (
                        "industry_classification_pit",
                        "tushare",
                        "pit_reference_staging",
                        "green",
                        9,
                        "20200101",
                        "20240103",
                    ),
                ],
            )
            conn.close()

    def test_refresh_official_sw_industry_reference_db_imports_conservative_intervals(self) -> None:
        from alpha_find_v2.reference_data_staging import (
            refresh_official_sw_industry_reference_db,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_db = temp_root / "pit_reference.duckdb"
            stock_file = temp_root / "StockClassifyUse_stock.xlsx"
            crosswalk_file = temp_root / "2014to2021.xlsx"
            code_file = temp_root / "SwClassCode_2021.xlsx"
            snapshot_file = temp_root / "最新个股申万行业分类(完整版-截至7月末).xlsx"

            pd.DataFrame(
                [
                    {"股票代码": 1, "计入日期": "2013-01-01", "行业代码": "210202", "更新日期": "2026-04-27 09:00:00"},
                    {"股票代码": 1, "计入日期": "2014-02-21", "行业代码": "210202", "更新日期": "2026-04-27 09:00:00"},
                    {"股票代码": 1, "计入日期": "2021-07-30 12:45:00", "行业代码": "740201", "更新日期": "2026-04-27 09:00:00"},
                    {"股票代码": 2, "计入日期": "1990-01-01", "行业代码": "620307", "更新日期": "2026-04-27 09:00:00"},
                    {"股票代码": 3, "计入日期": "2018-06-19 16:10:00", "行业代码": "330104", "更新日期": "2026-04-27 09:00:00"},
                    {"股票代码": 3, "计入日期": "2021-07-30", "行业代码": "330301", "更新日期": "2026-04-27 09:00:00"},
                    {"股票代码": 4, "计入日期": "2018-06-19 16:10:00", "行业代码": "330104", "更新日期": "2026-04-27 09:00:00"},
                    {"股票代码": 4, "计入日期": "2021-07-30", "行业代码": "630702", "更新日期": "2026-04-27 09:00:00"},
                    {"股票代码": 5, "计入日期": "2013-12-31 09:00:00", "行业代码": "330104", "更新日期": "2026-04-27 09:00:00"},
                    {"股票代码": 5, "计入日期": "2019-07-24", "行业代码": "630702", "更新日期": "2026-04-27 09:00:00"},
                ]
            ).to_excel(stock_file, index=False)

            crosswalk_rows = [
                ["旧版一级行业", "旧版二级行业", "旧版三级行业", "行业代码", "新版一级行业", "新版二级行业", "新版三级行业", "行业代码"],
                ["采掘", "煤炭开采", "焦炭", "210202", "煤炭", "焦炭Ⅱ", "焦炭Ⅲ", "740201"],
                ["家用电器", None, None, "330000", "家用电器", None, None, "330000"],
                [None, "白色家电", None, "330100", None, "白色家电", None, "330100"],
                [None, None, "小家电", "330104", None, None, None, None],
            ]
            pd.DataFrame(crosswalk_rows).to_excel(
                crosswalk_file,
                sheet_name="新旧对比版本2",
                index=False,
                header=False,
            )

            pd.DataFrame(
                [
                    {"交易所": "A股", "行业代码": "740201", "股票代码": "000001.SZ", "公司简称": "平安银行", "新版一级行业": "煤炭", "新版二级行业": "焦炭Ⅱ", "新版三级行业": "焦炭Ⅲ"},
                    {"交易所": "A股", "行业代码": "630702", "股票代码": "000002.SZ", "公司简称": "万科A", "新版一级行业": "电力设备", "新版二级行业": "电池", "新版三级行业": "锂电池"},
                    {"交易所": "A股", "行业代码": "330301", "股票代码": "000003.SZ", "公司简称": "国华网安", "新版一级行业": "家用电器", "新版二级行业": "黑色家电", "新版三级行业": "电视音响设备"},
                    {"交易所": "A股", "行业代码": "630702", "股票代码": "000004.SZ", "公司简称": "国华网安2", "新版一级行业": "电力设备", "新版二级行业": "电池", "新版三级行业": "锂电池"},
                    {"交易所": "港股", "行业代码": "630702", "股票代码": "000005.HK", "公司简称": "Ignore HK", "新版一级行业": "电力设备", "新版二级行业": "电池", "新版三级行业": "锂电池"},
                ]
            ).to_excel(snapshot_file, index=False)

            pd.DataFrame(
                [
                    {"行业代码": "740000", "一级行业名称": "煤炭", "二级行业名称": None, "三级行业名称": None},
                    {"行业代码": "740200", "一级行业名称": "煤炭", "二级行业名称": "焦炭Ⅱ", "三级行业名称": None},
                    {"行业代码": "740201", "一级行业名称": "煤炭", "二级行业名称": "焦炭Ⅱ", "三级行业名称": "焦炭Ⅲ"},
                    {"行业代码": "330000", "一级行业名称": "家用电器", "二级行业名称": None, "三级行业名称": None},
                    {"行业代码": "330300", "一级行业名称": "家用电器", "二级行业名称": "黑色家电", "三级行业名称": None},
                    {"行业代码": "330301", "一级行业名称": "家用电器", "二级行业名称": "黑色家电", "三级行业名称": "电视音响设备"},
                    {"行业代码": "630000", "一级行业名称": "电力设备", "二级行业名称": None, "三级行业名称": None},
                    {"行业代码": "630700", "一级行业名称": "电力设备", "二级行业名称": "电池", "三级行业名称": None},
                    {"行业代码": "630702", "一级行业名称": "电力设备", "二级行业名称": "电池", "三级行业名称": "锂电池"},
                ]
            ).to_excel(code_file, index=False)

            summary = refresh_official_sw_industry_reference_db(
                target_db=target_db,
                stock_file=stock_file,
                crosswalk_file=crosswalk_file,
                code_file=code_file,
                snapshot_file=snapshot_file,
                industry_levels=("L1", "L2", "L3"),
            )

            self.assertEqual(summary["industry_rows"], 24)
            self.assertEqual(summary["snapshot_anchor_rows"], 1)
            self.assertEqual(summary["snapshot_backfill_rows"]["L1"], 1)
            self.assertEqual(summary["snapshot_backfill_rows"]["L2"], 1)
            self.assertEqual(summary["snapshot_backfill_rows"]["L3"], 1)
            self.assertEqual(summary["window_carry_forward_rows"]["L1"], 1)
            self.assertEqual(summary["window_carry_forward_rows"]["L2"], 0)
            self.assertEqual(summary["window_carry_forward_rows"]["L3"], 0)
            self.assertEqual(summary["quarantined_placeholder_rows"], 1)
            self.assertEqual(summary["quarantined_pre_2014_rows"], 2)
            self.assertEqual(summary["quarantined_unresolved_rows"]["L1"], 0)
            self.assertEqual(summary["quarantined_unresolved_rows"]["L2"], 2)
            self.assertEqual(summary["quarantined_unresolved_rows"]["L3"], 2)

            conn = duckdb.connect(str(target_db), read_only=True)
            rows = conn.execute(
                """
                SELECT security_id, industry_schema, industry_code, effective_at, removed_at
                FROM industry_classification_pit
                ORDER BY security_id, industry_schema, effective_at
                """
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    ("000001.SZ", "sw2021_l1", "740000", "2014-02-21 00:00:00", "2021-07-30 12:45:00"),
                    ("000001.SZ", "sw2021_l1", "740000", "2021-07-30 12:45:00", None),
                    ("000001.SZ", "sw2021_l2", "740200", "2014-02-21 00:00:00", "2021-07-30 12:45:00"),
                    ("000001.SZ", "sw2021_l2", "740200", "2021-07-30 12:45:00", None),
                    ("000001.SZ", "sw2021_l3", "740201", "2014-02-21 00:00:00", "2021-07-30 12:45:00"),
                    ("000001.SZ", "sw2021_l3", "740201", "2021-07-30 12:45:00", None),
                    ("000002.SZ", "sw2021_l1", "630000", "2014-02-21 00:00:00", "2021-07-30 00:00:00"),
                    ("000002.SZ", "sw2021_l1", "630000", "2021-07-30 00:00:00", None),
                    ("000002.SZ", "sw2021_l2", "630700", "2014-02-21 00:00:00", "2021-07-30 00:00:00"),
                    ("000002.SZ", "sw2021_l2", "630700", "2021-07-30 00:00:00", None),
                    ("000002.SZ", "sw2021_l3", "630702", "2014-02-21 00:00:00", "2021-07-30 00:00:00"),
                    ("000002.SZ", "sw2021_l3", "630702", "2021-07-30 00:00:00", None),
                    ("000003.SZ", "sw2021_l1", "330000", "2018-06-19 16:10:00", "2021-07-30 00:00:00"),
                    ("000003.SZ", "sw2021_l1", "330000", "2021-07-30 00:00:00", None),
                    ("000003.SZ", "sw2021_l2", "330300", "2021-07-30 00:00:00", None),
                    ("000003.SZ", "sw2021_l3", "330301", "2021-07-30 00:00:00", None),
                    ("000004.SZ", "sw2021_l1", "330000", "2018-06-19 16:10:00", "2021-07-30 00:00:00"),
                    ("000004.SZ", "sw2021_l1", "630000", "2021-07-30 00:00:00", None),
                    ("000004.SZ", "sw2021_l2", "630700", "2021-07-30 00:00:00", None),
                    ("000004.SZ", "sw2021_l3", "630702", "2021-07-30 00:00:00", None),
                    ("000005.SZ", "sw2021_l1", "330000", "2014-02-21 00:00:00", "2019-07-24 00:00:00"),
                    ("000005.SZ", "sw2021_l1", "630000", "2019-07-24 00:00:00", None),
                    ("000005.SZ", "sw2021_l2", "630700", "2019-07-24 00:00:00", None),
                    ("000005.SZ", "sw2021_l3", "630702", "2019-07-24 00:00:00", None),
                ],
            )
            registry_rows = conn.execute(
                """
                SELECT dataset_id, source_provider, status
                FROM reference_dataset_registry
                ORDER BY dataset_id
                """
            ).fetchall()
            self.assertEqual(
                registry_rows,
                [
                    ("industry_classification_pit", "official_shenwan_packet", "green"),
                ],
            )
            conn.close()

    def test_refresh_official_sw_industry_reference_db_applies_listing_lag_manual_adjudication(
        self,
    ) -> None:
        from alpha_find_v2.reference_data_staging import (
            refresh_official_sw_industry_reference_db,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_db = temp_root / "pit_reference.duckdb"
            stock_file = temp_root / "StockClassifyUse_stock.xlsx"
            crosswalk_file = temp_root / "2014to2021.xlsx"
            code_file = temp_root / "SwClassCode_2021.xlsx"
            snapshot_file = temp_root / "最新个股申万行业分类(完整版-截至7月末).xlsx"

            pd.DataFrame(
                [
                    {"股票代码": "001979", "计入日期": "2016-01-07", "行业代码": "430101", "更新日期": "2021-03-03 15:31:00"},
                    {"股票代码": "001979", "计入日期": "2021-07-30", "行业代码": "430102", "更新日期": "2025-02-19 17:18:00"},
                ]
            ).to_excel(stock_file, index=False)

            pd.DataFrame(
                [
                    ["旧版一级行业", "旧版二级行业", "旧版三级行业", "行业代码", "新版一级行业", "新版二级行业", "新版三级行业", "行业代码"],
                ]
            ).to_excel(
                crosswalk_file,
                sheet_name="新旧对比版本2",
                index=False,
                header=False,
            )

            pd.DataFrame(
                [
                    {"交易所": "A股", "行业代码": "430102", "股票代码": "001979.SZ", "公司简称": "招商蛇口"},
                ]
            ).to_excel(snapshot_file, index=False)

            pd.DataFrame(
                [
                    {"行业代码": "430000", "一级行业名称": "房地产", "二级行业名称": None, "三级行业名称": None},
                    {"行业代码": "430100", "一级行业名称": "房地产", "二级行业名称": "房地产开发", "三级行业名称": None},
                    {"行业代码": "430101", "一级行业名称": "房地产", "二级行业名称": "房地产开发", "三级行业名称": "住宅开发"},
                    {"行业代码": "430102", "一级行业名称": "房地产", "二级行业名称": "房地产开发", "三级行业名称": "综合开发"},
                ]
            ).to_excel(code_file, index=False)

            summary = refresh_official_sw_industry_reference_db(
                target_db=target_db,
                stock_file=stock_file,
                crosswalk_file=crosswalk_file,
                code_file=code_file,
                snapshot_file=snapshot_file,
                industry_levels=("L1",),
            )

            self.assertEqual(summary["official_industry_rows"], 2)
            self.assertEqual(summary["industry_rows"], 3)
            self.assertEqual(summary["manual_adjudication_rows"], 1)

            conn = duckdb.connect(str(target_db), read_only=True)
            effective_rows = conn.execute(
                """
                SELECT security_id, industry_schema, industry_code, effective_at, removed_at
                FROM industry_classification_pit
                ORDER BY effective_at
                """
            ).fetchall()
            self.assertEqual(
                effective_rows,
                [
                    ("001979.SZ", "sw2021_l1", "430000", "2015-12-31 00:00:00", "2016-01-07 00:00:00"),
                    ("001979.SZ", "sw2021_l1", "430000", "2016-01-07 00:00:00", "2021-07-30 00:00:00"),
                    ("001979.SZ", "sw2021_l1", "430000", "2021-07-30 00:00:00", None),
                ],
            )
            official_rows = conn.execute(
                """
                SELECT security_id, industry_schema, industry_code, effective_at, removed_at
                FROM industry_classification_pit_official_raw
                ORDER BY effective_at
                """
            ).fetchall()
            self.assertEqual(
                official_rows,
                [
                    ("001979.SZ", "sw2021_l1", "430000", "2016-01-07 00:00:00", "2021-07-30 00:00:00"),
                    ("001979.SZ", "sw2021_l1", "430000", "2021-07-30 00:00:00", None),
                ],
            )
            manual_rows = conn.execute(
                """
                SELECT
                    security_id,
                    start_date,
                    end_date,
                    industry_schema,
                    industry_level,
                    industry_code,
                    source_type,
                    confidence
                FROM industry_classification_pit_manual_adjudication
                """
            ).fetchall()
            self.assertEqual(
                manual_rows,
                [
                    (
                        "001979.SZ",
                        "2015-12-31 00:00:00",
                        "2016-01-07 00:00:00",
                        "sw2021_l1",
                        "L1",
                        "430000",
                        "listing_lag_backfill",
                        "medium",
                    )
                ],
            )
            conn.close()

    def test_refresh_official_sw_industry_reference_db_persists_provider_gap_confirmation(
        self,
    ) -> None:
        from alpha_find_v2.reference_data_staging import (
            refresh_official_sw_industry_reference_db,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_db = temp_root / "pit_reference.duckdb"
            stock_file = temp_root / "StockClassifyUse_stock.xlsx"
            crosswalk_file = temp_root / "2014to2021.xlsx"
            code_file = temp_root / "SwClassCode_2021.xlsx"
            snapshot_file = temp_root / "最新个股申万行业分类(完整版-截至7月末).xlsx"

            pd.DataFrame(
                [
                    {"股票代码": "000623", "计入日期": "2014-02-21", "行业代码": "490101", "更新日期": "2024-09-27 12:41:00"},
                    {"股票代码": "000623", "计入日期": "2014-07-01", "行业代码": "370201", "更新日期": "2015-10-27 15:29:00"},
                    {"股票代码": "000623", "计入日期": "2019-07-24", "行业代码": "370102", "更新日期": "2020-12-04 14:32:00"},
                    {"股票代码": "000623", "计入日期": "2022-07-29", "行业代码": "370201", "更新日期": "2022-07-29 11:54:00"},
                ]
            ).to_excel(stock_file, index=False)

            pd.DataFrame(
                [
                    ["旧版一级行业", "旧版二级行业", "旧版三级行业", "行业代码", "新版一级行业", "新版二级行业", "新版三级行业", "行业代码"],
                ]
            ).to_excel(
                crosswalk_file,
                sheet_name="新旧对比版本2",
                index=False,
                header=False,
            )

            pd.DataFrame(
                [
                    {"交易所": "A股", "行业代码": "370201", "股票代码": "000623.SZ", "公司简称": "吉林敖东"},
                ]
            ).to_excel(snapshot_file, index=False)

            pd.DataFrame(
                [
                    {"行业代码": "370000", "一级行业名称": "医药生物", "二级行业名称": None, "三级行业名称": None},
                    {"行业代码": "370100", "一级行业名称": "医药生物", "二级行业名称": "化学制药", "三级行业名称": None},
                    {"行业代码": "370102", "一级行业名称": "医药生物", "二级行业名称": "化学制药", "三级行业名称": "原料药"},
                    {"行业代码": "370200", "一级行业名称": "医药生物", "二级行业名称": "中药", "三级行业名称": None},
                    {"行业代码": "370201", "一级行业名称": "医药生物", "二级行业名称": "中药", "三级行业名称": "中药Ⅲ"},
                    {"行业代码": "490000", "一级行业名称": "非银金融", "二级行业名称": None, "三级行业名称": None},
                    {"行业代码": "490100", "一级行业名称": "非银金融", "二级行业名称": "证券Ⅱ", "三级行业名称": None},
                    {"行业代码": "490101", "一级行业名称": "非银金融", "二级行业名称": "证券Ⅱ", "三级行业名称": "证券Ⅲ"},
                ]
            ).to_excel(code_file, index=False)

            summary = refresh_official_sw_industry_reference_db(
                target_db=target_db,
                stock_file=stock_file,
                crosswalk_file=crosswalk_file,
                code_file=code_file,
                snapshot_file=snapshot_file,
                industry_levels=("L1",),
            )

            self.assertEqual(summary["official_industry_rows"], 5)
            self.assertEqual(summary["industry_rows"], 5)
            self.assertEqual(summary["manual_adjudication_rows"], 1)

            conn = duckdb.connect(str(target_db), read_only=True)
            effective_rows = conn.execute(
                """
                SELECT security_id, industry_schema, industry_code, effective_at, removed_at
                FROM industry_classification_pit
                ORDER BY effective_at
                """
            ).fetchall()
            self.assertEqual(
                effective_rows,
                [
                    ("000623.SZ", "sw2021_l1", "490000", "2014-02-21 00:00:00", "2014-07-01 00:00:00"),
                    ("000623.SZ", "sw2021_l1", "370000", "2014-07-01 00:00:00", "2019-07-24 00:00:00"),
                    ("000623.SZ", "sw2021_l1", "370000", "2019-07-24 00:00:00", "2021-07-30 00:00:00"),
                    ("000623.SZ", "sw2021_l1", "370000", "2021-07-30 00:00:00", "2022-07-29 00:00:00"),
                    ("000623.SZ", "sw2021_l1", "370000", "2022-07-29 00:00:00", None),
                ],
            )
            manual_rows = conn.execute(
                """
                SELECT
                    security_id,
                    start_date,
                    end_date,
                    industry_schema,
                    industry_level,
                    industry_code,
                    source_type,
                    confidence
                FROM industry_classification_pit_manual_adjudication
                """
            ).fetchall()
            self.assertEqual(
                manual_rows,
                [
                    (
                        "000623.SZ",
                        "2014-07-01 00:00:00",
                        "2019-07-24 00:00:00",
                        "sw2021_l1",
                        "L1",
                        "370000",
                        "provider_gap_confirmation",
                        "medium",
                    )
                ],
            )
            conn.close()

    def test_refresh_official_sw_industry_reference_db_applies_external_left_edge_backfill(
        self,
    ) -> None:
        from alpha_find_v2.reference_data_staging import (
            refresh_official_sw_industry_reference_db,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_db = temp_root / "pit_reference.duckdb"
            stock_file = temp_root / "StockClassifyUse_stock.xlsx"
            crosswalk_file = temp_root / "2014to2021.xlsx"
            code_file = temp_root / "SwClassCode_2021.xlsx"
            snapshot_file = temp_root / "最新个股申万行业分类(完整版-截至7月末).xlsx"

            pd.DataFrame(
                [
                    {"股票代码": "600651", "计入日期": "1990-01-01", "行业代码": "270401", "更新日期": "2024-10-09 12:17:00"},
                    {"股票代码": "600651", "计入日期": "2017-06-29", "行业代码": "270302", "更新日期": "2019-08-09 15:38:00"},
                    {"股票代码": "600651", "计入日期": "2024-07-30 12:45:00", "行业代码": "280206", "更新日期": "2025-01-08 14:36:00"},
                ]
            ).to_excel(stock_file, index=False)

            pd.DataFrame(
                [
                    ["旧版一级行业", "旧版二级行业", "旧版三级行业", "行业代码", "新版一级行业", "新版二级行业", "新版三级行业", "行业代码"],
                    [None, None, "其他电子", "270401", None, None, "其他电子Ⅲ", "270401"],
                ]
            ).to_excel(
                crosswalk_file,
                sheet_name="新旧对比版本2",
                index=False,
                header=False,
            )

            pd.DataFrame(
                [
                    {"交易所": "A股", "行业代码": "270302", "股票代码": "600651.SH", "公司简称": "飞乐音响"},
                ]
            ).to_excel(snapshot_file, index=False)

            pd.DataFrame(
                [
                    {"行业代码": "270000", "一级行业名称": "电子", "二级行业名称": None, "三级行业名称": None},
                    {"行业代码": "270300", "一级行业名称": "电子", "二级行业名称": "光学光电子", "三级行业名称": None},
                    {"行业代码": "270302", "一级行业名称": "电子", "二级行业名称": "光学光电子", "三级行业名称": "LED"},
                    {"行业代码": "270400", "一级行业名称": "电子", "二级行业名称": "其他电子Ⅱ", "三级行业名称": None},
                    {"行业代码": "270401", "一级行业名称": "电子", "二级行业名称": "其他电子Ⅱ", "三级行业名称": "其他电子Ⅲ"},
                    {"行业代码": "280000", "一级行业名称": "汽车", "二级行业名称": None, "三级行业名称": None},
                    {"行业代码": "280200", "一级行业名称": "汽车", "二级行业名称": "汽车零部件", "三级行业名称": None},
                    {"行业代码": "280206", "一级行业名称": "汽车", "二级行业名称": "汽车零部件", "三级行业名称": "汽车电子电气系统"},
                ]
            ).to_excel(code_file, index=False)

            summary = refresh_official_sw_industry_reference_db(
                target_db=target_db,
                stock_file=stock_file,
                crosswalk_file=crosswalk_file,
                code_file=code_file,
                snapshot_file=snapshot_file,
                industry_levels=("L1",),
            )

            self.assertEqual(summary["official_industry_rows"], 3)
            self.assertEqual(summary["industry_rows"], 4)
            self.assertEqual(summary["manual_adjudication_rows"], 1)
            self.assertEqual(summary["effective_manual_adjudication_rows"], 1)

            conn = duckdb.connect(str(target_db), read_only=True)
            effective_rows = conn.execute(
                """
                SELECT security_id, industry_schema, industry_code, effective_at, removed_at
                FROM industry_classification_pit
                ORDER BY effective_at
                """
            ).fetchall()
            self.assertEqual(
                effective_rows,
                [
                    ("600651.SH", "sw2021_l1", "270000", "2014-02-21 00:00:00", "2017-06-29 00:00:00"),
                    ("600651.SH", "sw2021_l1", "270000", "2017-06-29 00:00:00", "2021-07-30 00:00:00"),
                    ("600651.SH", "sw2021_l1", "270000", "2021-07-30 00:00:00", "2024-07-30 12:45:00"),
                    ("600651.SH", "sw2021_l1", "280000", "2024-07-30 12:45:00", None),
                ],
            )
            official_rows = conn.execute(
                """
                SELECT security_id, industry_schema, industry_code, effective_at, removed_at
                FROM industry_classification_pit_official_raw
                ORDER BY effective_at
                """
            ).fetchall()
            self.assertEqual(
                official_rows,
                [
                    ("600651.SH", "sw2021_l1", "270000", "2017-06-29 00:00:00", "2021-07-30 00:00:00"),
                    ("600651.SH", "sw2021_l1", "270000", "2021-07-30 00:00:00", "2024-07-30 12:45:00"),
                    ("600651.SH", "sw2021_l1", "280000", "2024-07-30 12:45:00", None),
                ],
            )
            manual_rows = conn.execute(
                """
                SELECT security_id, start_date, end_date, industry_code, source_type, confidence
                FROM industry_classification_pit_manual_adjudication
                """
            ).fetchall()
            self.assertEqual(
                manual_rows,
                [
                    (
                        "600651.SH",
                        "2014-02-21 00:00:00",
                        "2017-06-29 00:00:00",
                        "270000",
                        "external_left_edge_backfill",
                        "medium",
                    )
                ],
            )
            conn.close()

    def test_refresh_official_sw_industry_reference_db_backfills_security_code_alias(
        self,
    ) -> None:
        from alpha_find_v2.reference_data_staging import (
            refresh_official_sw_industry_reference_db,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_db = temp_root / "pit_reference.duckdb"
            stock_file = temp_root / "StockClassifyUse_stock.xlsx"
            crosswalk_file = temp_root / "2014to2021.xlsx"
            code_file = temp_root / "SwClassCode_2021.xlsx"
            snapshot_file = temp_root / "最新个股申万行业分类(完整版-截至7月末).xlsx"

            pd.DataFrame(
                [
                    {"股票代码": "300114", "计入日期": "2014-02-21", "行业代码": "640301", "更新日期": "2021-03-03 15:31:00"},
                    {"股票代码": "300114", "计入日期": "2021-07-30", "行业代码": "650101", "更新日期": "2025-02-19 17:18:00"},
                    {"股票代码": "302132", "计入日期": "2025-02-17", "行业代码": "650101", "更新日期": "2025-02-19 17:18:00"},
                ]
            ).to_excel(stock_file, index=False)

            pd.DataFrame(
                [
                    ["旧版一级行业", "旧版二级行业", "旧版三级行业", "行业代码", "新版一级行业", "新版二级行业", "新版三级行业", "行业代码"],
                ]
            ).to_excel(
                crosswalk_file,
                sheet_name="新旧对比版本2",
                index=False,
                header=False,
            )

            pd.DataFrame(
                [
                    {"交易所": "A股", "行业代码": "650101", "股票代码": "300114.SZ", "公司简称": "中航电测"},
                ]
            ).to_excel(snapshot_file, index=False)

            pd.DataFrame(
                [
                    {"行业代码": "640000", "一级行业名称": "机械设备", "二级行业名称": None, "三级行业名称": None},
                    {"行业代码": "640300", "一级行业名称": "机械设备", "二级行业名称": "自动化设备", "三级行业名称": None},
                    {"行业代码": "640301", "一级行业名称": "机械设备", "二级行业名称": "自动化设备", "三级行业名称": "仪器仪表"},
                    {"行业代码": "650000", "一级行业名称": "国防军工", "二级行业名称": None, "三级行业名称": None},
                    {"行业代码": "650100", "一级行业名称": "国防军工", "二级行业名称": "航天装备", "三级行业名称": None},
                    {"行业代码": "650101", "一级行业名称": "国防军工", "二级行业名称": "航天装备", "三级行业名称": "航空装备"},
                ]
            ).to_excel(code_file, index=False)

            summary = refresh_official_sw_industry_reference_db(
                target_db=target_db,
                stock_file=stock_file,
                crosswalk_file=crosswalk_file,
                code_file=code_file,
                snapshot_file=snapshot_file,
                industry_levels=("L1",),
            )

            self.assertEqual(summary["manual_adjudication_rows"], 3)
            self.assertEqual(summary["effective_manual_adjudication_rows"], 2)

            conn = duckdb.connect(str(target_db), read_only=True)
            alias_rows = conn.execute(
                """
                SELECT security_id, industry_schema, industry_code, effective_at, removed_at
                FROM industry_classification_pit
                WHERE security_id = '302132.SZ'
                ORDER BY effective_at
                """
            ).fetchall()
            self.assertEqual(
                alias_rows,
                [
                    ("302132.SZ", "sw2021_l1", "640000", "2014-02-21 00:00:00", "2021-07-30 00:00:00"),
                    ("302132.SZ", "sw2021_l1", "650000", "2021-07-30 00:00:00", "2025-02-17 00:00:00"),
                    ("302132.SZ", "sw2021_l1", "650000", "2025-02-17 00:00:00", None),
                ],
            )
            manual_rows = conn.execute(
                """
                SELECT security_id, start_date, end_date, industry_code, source_type
                FROM industry_classification_pit_manual_adjudication
                WHERE security_id = '302132.SZ'
                ORDER BY start_date
                """
            ).fetchall()
            self.assertEqual(
                manual_rows,
                [
                    (
                        "302132.SZ",
                        "2014-02-21 00:00:00",
                        "2021-07-30 00:00:00",
                        "640000",
                        "security_code_alias_backfill",
                    ),
                    (
                        "302132.SZ",
                        "2021-07-30 00:00:00",
                        "2025-02-17 00:00:00",
                        "650000",
                        "security_code_alias_backfill",
                    ),
                ],
            )
            conn.close()

    def test_refresh_official_sw_industry_reference_db_persists_schema_transition_gap_confirmations(
        self,
    ) -> None:
        from alpha_find_v2.reference_data_staging import (
            refresh_official_sw_industry_reference_db,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            target_db = temp_root / "pit_reference.duckdb"
            stock_file = temp_root / "StockClassifyUse_stock.xlsx"
            crosswalk_file = temp_root / "2014to2021.xlsx"
            code_file = temp_root / "SwClassCode_2021.xlsx"
            snapshot_file = temp_root / "最新个股申万行业分类(完整版-截至7月末).xlsx"

            pd.DataFrame(
                [
                    {"股票代码": "600061", "计入日期": "1997-05-19 00:00:00", "行业代码": "220401", "更新日期": "2015-10-27 15:28:00"},
                    {"股票代码": "600061", "计入日期": "2018-06-19 16:12:00", "行业代码": "490101", "更新日期": "2021-07-01 09:03:00"},
                    {"股票代码": "600061", "计入日期": "2021-07-30 00:00:00", "行业代码": "490302", "更新日期": "2023-11-24 15:36:00"},
                    {"股票代码": "603650", "计入日期": "2018-06-26 15:40:00", "行业代码": "220602", "更新日期": "2019-01-11 14:17:00"},
                    {"股票代码": "603650", "计入日期": "2019-07-24 00:00:00", "行业代码": "220309", "更新日期": "2020-06-08 13:49:00"},
                    {"股票代码": "603650", "计入日期": "2021-07-30 00:00:00", "行业代码": "220604", "更新日期": "2024-03-01 09:49:00"},
                ]
            ).to_excel(stock_file, index=False)

            pd.DataFrame(
                [
                    ["旧版一级行业", "旧版二级行业", "旧版三级行业", "行业代码", "新版一级行业", "新版二级行业", "新版三级行业", "行业代码"],
                ]
            ).to_excel(
                crosswalk_file,
                sheet_name="新旧对比版本2",
                index=False,
                header=False,
            )

            pd.DataFrame(
                [
                    {"交易所": "A股", "行业代码": "490302", "股票代码": "600061.SH", "公司简称": "国投资本"},
                    {"交易所": "A股", "行业代码": "220604", "股票代码": "603650.SH", "公司简称": "彤程新材"},
                ]
            ).to_excel(snapshot_file, index=False)

            pd.DataFrame(
                [
                    {"行业代码": "220000", "一级行业名称": "基础化工", "二级行业名称": None, "三级行业名称": None},
                    {"行业代码": "220300", "一级行业名称": "基础化工", "二级行业名称": "化学制品", "三级行业名称": None},
                    {"行业代码": "220309", "一级行业名称": "基础化工", "二级行业名称": "化学制品", "三级行业名称": "其他化学制品"},
                    {"行业代码": "220400", "一级行业名称": "基础化工", "二级行业名称": "化学纤维", "三级行业名称": None},
                    {"行业代码": "220401", "一级行业名称": "基础化工", "二级行业名称": "化学纤维", "三级行业名称": "涤纶"},
                    {"行业代码": "220600", "一级行业名称": "基础化工", "二级行业名称": "橡胶", "三级行业名称": None},
                    {"行业代码": "220602", "一级行业名称": "基础化工", "二级行业名称": "橡胶", "三级行业名称": "其他橡胶制品"},
                    {"行业代码": "220604", "一级行业名称": "基础化工", "二级行业名称": "橡胶", "三级行业名称": "炭黑"},
                    {"行业代码": "490000", "一级行业名称": "非银金融", "二级行业名称": None, "三级行业名称": None},
                    {"行业代码": "490100", "一级行业名称": "非银金融", "二级行业名称": "证券Ⅱ", "三级行业名称": None},
                    {"行业代码": "490101", "一级行业名称": "非银金融", "二级行业名称": "证券Ⅱ", "三级行业名称": "证券Ⅲ"},
                    {"行业代码": "490300", "一级行业名称": "非银金融", "二级行业名称": "多元金融", "三级行业名称": None},
                    {"行业代码": "490302", "一级行业名称": "非银金融", "二级行业名称": "多元金融", "三级行业名称": "金融控股"},
                ]
            ).to_excel(code_file, index=False)

            summary = refresh_official_sw_industry_reference_db(
                target_db=target_db,
                stock_file=stock_file,
                crosswalk_file=crosswalk_file,
                code_file=code_file,
                snapshot_file=snapshot_file,
                industry_levels=("L1",),
            )

            self.assertEqual(summary["manual_adjudication_rows"], 2)
            self.assertEqual(summary["effective_manual_adjudication_rows"], 0)

            conn = duckdb.connect(str(target_db), read_only=True)
            manual_rows = conn.execute(
                """
                SELECT security_id, start_date, end_date, industry_code, source_type
                FROM industry_classification_pit_manual_adjudication
                ORDER BY security_id
                """
            ).fetchall()
            self.assertEqual(
                manual_rows,
                [
                    (
                        "600061.SH",
                        "2018-06-19 16:12:00",
                        "2021-07-30 00:00:00",
                        "490000",
                        "provider_gap_confirmation",
                    ),
                    (
                        "603650.SH",
                        "2019-07-24 00:00:00",
                        "2021-07-30 00:00:00",
                        "220000",
                        "provider_gap_confirmation",
                    ),
                ],
            )
            conn.close()

    def test_default_manual_adjudications_include_schema_transition_confirmations(
        self,
    ) -> None:
        from alpha_find_v2.reference_data_staging import (
            _build_default_manual_industry_adjudications,
        )

        official_rows = [
            ("000408.SZ", "sw2021_l1", "220000", "2018-07-13 00:00:00", "2021-07-30 00:00:00"),
            ("000919.SZ", "sw2021_l1", "370000", "2014-02-21 00:00:00", "2015-11-02 00:00:00"),
            ("000919.SZ", "sw2021_l1", "370000", "2015-11-02 00:00:00", "2021-07-30 00:00:00"),
            ("000975.SZ", "sw2021_l1", "240000", "2014-02-21 00:00:00", "2014-07-01 00:00:00"),
            ("002332.SZ", "sw2021_l1", "370000", "2014-02-21 00:00:00", "2021-07-30 00:00:00"),
            ("002411.SZ", "sw2021_l1", "370000", "2016-04-14 00:00:00", "2021-07-30 00:00:00"),
            ("600061.SH", "sw2021_l1", "490000", "2018-06-19 16:12:00", "2021-07-30 00:00:00"),
            ("600575.SH", "sw2021_l1", "420000", "2014-02-21 00:00:00", "2017-05-20 01:33:00"),
            ("600575.SH", "sw2021_l1", "420000", "2017-05-20 01:33:00", "2021-07-30 00:00:00"),
            ("603456.SH", "sw2021_l1", "370000", "2021-07-30 00:00:00", "2022-07-29 00:00:00"),
            ("603456.SH", "sw2021_l1", "370000", "2022-07-29 00:00:00", "2023-07-04 00:00:00"),
            ("603650.SH", "sw2021_l1", "220000", "2019-07-24 00:00:00", "2021-07-30 00:00:00"),
        ]

        records = _build_default_manual_industry_adjudications(
            stock_path=Path("docs/data/StockClassifyUse_stock.xls"),
            official_industry_rows=official_rows,
        )

        actual_rows = sorted(
            (
                record.security_id,
                record.start_date,
                record.end_date,
                record.industry_code,
                record.source_type,
            )
            for record in records
        )
        expected_rows = sorted(
            (
                security_id,
                start_date,
                end_date,
                industry_code,
                "provider_gap_confirmation",
            )
            for security_id, _, industry_code, start_date, end_date in official_rows
        )
        self.assertEqual(actual_rows, expected_rows)

    def test_default_manual_adjudications_include_one_sided_provider_gap_confirmations(
        self,
    ) -> None:
        from alpha_find_v2.reference_data_staging import (
            _build_default_manual_industry_adjudications,
        )

        official_rows = [
            ("000088.SZ", "sw2021_l1", "420000", "2014-02-21 00:00:00", "2021-07-30 00:00:00"),
            ("000417.SZ", "sw2021_l1", "450000", "2014-02-21 00:00:00", "2017-06-29 00:00:00"),
            ("000422.SZ", "sw2021_l1", "220000", "2014-02-21 00:00:00", "2017-06-29 00:00:00"),
            ("000541.SZ", "sw2021_l1", "270000", "2014-02-21 00:00:00", "2017-06-29 00:00:00"),
            ("002310.SZ", "sw2021_l1", "620000", "2014-02-21 00:00:00", "2016-05-25 00:00:00"),
            ("002648.SZ", "sw2021_l1", "220000", "2014-02-21 00:00:00", "2021-07-30 00:00:00"),
            ("300285.SZ", "sw2021_l1", "220000", "2014-02-21 00:00:00", "2021-07-30 00:00:00"),
            ("300450.SZ", "sw2021_l1", "640000", "2015-01-06 00:05:00", "2019-07-24 00:00:00"),
            ("600251.SH", "sw2021_l1", "220000", "2014-02-21 00:00:00", "2015-07-01 00:00:00"),
            ("600261.SH", "sw2021_l1", "270000", "2014-02-21 00:00:00", "2017-05-20 01:33:00"),
            ("600409.SH", "sw2021_l1", "220000", "2014-02-21 00:00:00", "2021-07-30 00:00:00"),
            ("600426.SH", "sw2021_l1", "220000", "2014-02-21 00:00:00", "2021-07-30 00:00:00"),
            ("600488.SH", "sw2021_l1", "370000", "2014-02-21 00:00:00", "2019-07-24 00:00:00"),
            ("600589.SH", "sw2021_l1", "220000", "2014-02-21 00:00:00", "2021-07-30 00:00:00"),
            ("600596.SH", "sw2021_l1", "220000", "2014-02-21 00:00:00", "2019-07-24 00:00:00"),
            ("600673.SH", "sw2021_l1", "240000", "2014-02-21 00:00:00", "2019-07-24 00:00:00"),
            ("600803.SH", "sw2021_l1", "220000", "2014-02-21 00:00:00", "2017-06-29 00:00:00"),
            ("600841.SH", "sw2021_l1", "640000", "2014-02-21 00:00:00", "2021-07-30 00:00:00"),
            ("601010.SH", "sw2021_l1", "450000", "2014-02-21 00:00:00", "2019-07-24 00:00:00"),
            ("601020.SH", "sw2021_l1", "240000", "2016-03-23 00:00:00", "2021-07-30 00:00:00"),
            ("601168.SH", "sw2021_l1", "240000", "2014-02-21 00:00:00", "2017-06-29 00:00:00"),
            ("601212.SH", "sw2021_l1", "240000", "2017-01-05 21:39:00", "2021-07-30 00:00:00"),
            ("603993.SH", "sw2021_l1", "240000", "2014-02-21 00:00:00", "2021-07-30 00:00:00"),
        ]

        records = _build_default_manual_industry_adjudications(
            stock_path=Path("docs/data/StockClassifyUse_stock.xls"),
            official_industry_rows=official_rows,
        )

        actual_rows = sorted(
            (
                record.security_id,
                record.start_date,
                record.end_date,
                record.industry_code,
                record.source_type,
            )
            for record in records
        )
        expected_rows = sorted(
            (
                security_id,
                start_date,
                end_date,
                industry_code,
                "provider_gap_confirmation",
            )
            for security_id, _, industry_code, start_date, end_date in official_rows
        )
        self.assertEqual(actual_rows, expected_rows)

    def test_default_manual_adjudications_support_open_ended_provider_gap_confirmation(
        self,
    ) -> None:
        from alpha_find_v2.reference_data_staging import (
            _build_default_manual_industry_adjudications,
        )

        records = _build_default_manual_industry_adjudications(
            stock_path=Path("docs/data/StockClassifyUse_stock.xls"),
            official_industry_rows=[
                ("300114.SZ", "sw2021_l1", "650000", "2021-07-30 00:00:00", None),
            ],
        )

        self.assertEqual(
            [
                (
                    record.security_id,
                    record.start_date,
                    record.end_date,
                    record.industry_code,
                    record.source_type,
                )
                for record in records
            ],
            [
                (
                    "300114.SZ",
                    "2021-07-30 00:00:00",
                    "2025-02-28 00:00:00",
                    "650000",
                    "provider_gap_confirmation",
                )
            ],
        )


if __name__ == "__main__":
    unittest.main()
