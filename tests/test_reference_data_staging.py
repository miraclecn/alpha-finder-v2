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


if __name__ == "__main__":
    unittest.main()
