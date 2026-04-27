from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _trading_days(start: date, count: int) -> list[str]:
    days: list[str] = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return days


def _create_combined_research_source_db(path: Path, trade_dates: list[str]) -> None:
    conn = duckdb.connect(str(path))
    conn.execute(
        """
        CREATE TABLE market_trade_calendar (
            trade_date VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_master_ref (
            security_id VARCHAR,
            symbol VARCHAR,
            current_name VARCHAR,
            exchange VARCHAR,
            board VARCHAR,
            area VARCHAR,
            list_date VARCHAR,
            delist_date VARCHAR,
            is_hs VARCHAR,
            is_a_share BOOLEAN,
            ingested_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE daily_bar_pit (
            security_id VARCHAR,
            trade_date VARCHAR,
            exchange VARCHAR,
            board VARCHAR,
            is_st BOOLEAN,
            pre_close DOUBLE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            close_adj DOUBLE,
            turnover_value_cny DOUBLE,
            pb DOUBLE
        )
        """
    )
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
        CREATE TABLE fundamental_snapshot_pit (
            security_id VARCHAR,
            announcement_date VARCHAR,
            period_end VARCHAR,
            available_date VARCHAR,
            roe DOUBLE,
            roa DOUBLE,
            gross_margin DOUBLE,
            netprofit_margin DOUBLE,
            current_ratio DOUBLE,
            debt_to_assets DOUBLE,
            revenue_per_share_cny DOUBLE,
            netprofit_yoy DOUBLE,
            dt_netprofit_yoy DOUBLE,
            revenue_yoy DOUBLE,
            q_sales_yoy DOUBLE,
            assets_yoy DOUBLE,
            equity_yoy DOUBLE
        )
        """
    )

    conn.executemany(
        "INSERT INTO market_trade_calendar VALUES (?)",
        [(trade_date,) for trade_date in trade_dates],
    )
    conn.executemany(
        """
        INSERT INTO security_master_ref VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TIMESTAMP '2026-04-26 09:00:00')
        """,
        [
            ("600001.SH", "600001", "Bank Alpha", "SH", "main_board", "上海", "20200102", None, "N", True),
            ("600002.SH", "600002", "Bank Beta", "SH", "main_board", "上海", "20200102", None, "N", True),
            ("600003.SH", "600003", "Tech Gamma", "SH", "main_board", "上海", "20200102", None, "N", True),
            ("600004.SH", "600004", "Tech Delta", "SH", "main_board", "上海", "20200102", None, "N", True),
        ],
    )
    conn.executemany(
        """
        INSERT INTO industry_classification_pit VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("600001.SH", "sw2021_l1", "bank", "20200102", None),
            ("600002.SH", "sw2021_l1", "bank", "20200102", None),
            ("600003.SH", "sw2021_l1", "tech", "20200102", None),
            ("600004.SH", "sw2021_l1", "tech", "20200102", None),
        ],
    )
    conn.executemany(
        """
        INSERT INTO fundamental_snapshot_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "600001.SH",
                "20240112",
                "20231231",
                "20240115",
                18.0,
                7.0,
                0.0,
                19.0,
                1.6,
                35.0,
                4.0,
                15.0,
                14.0,
                12.0,
                11.0,
                8.0,
                7.0,
            ),
            (
                "600002.SH",
                "20240112",
                "20231231",
                "20240115",
                10.0,
                4.0,
                0.0,
                9.0,
                1.1,
                62.0,
                3.0,
                28.0,
                4.0,
                8.0,
                7.0,
                6.0,
                5.0,
            ),
            (
                "600003.SH",
                "20240112",
                "20231231",
                "20240115",
                16.0,
                6.0,
                0.0,
                17.0,
                1.7,
                32.0,
                4.5,
                18.0,
                17.0,
                14.0,
                13.0,
                10.0,
                9.0,
            ),
            (
                "600004.SH",
                "20240112",
                "20231231",
                "20240115",
                9.0,
                3.0,
                0.0,
                7.0,
                1.0,
                68.0,
                2.8,
                24.0,
                2.0,
                7.0,
                6.0,
                5.0,
                4.0,
            ),
        ],
    )

    rows: list[tuple[object, ...]] = []
    previous_close_by_security: dict[str, float] = {}
    security_parameters = {
        "600001.SH": {"growth": 1.0120, "turnover": 240_000_000.0, "pb": 0.85},
        "600002.SH": {"growth": 1.0070, "turnover": 180_000_000.0, "pb": 1.45},
        "600003.SH": {"growth": 1.0110, "turnover": 220_000_000.0, "pb": 0.95},
        "600004.SH": {"growth": 1.0050, "turnover": 170_000_000.0, "pb": 1.80},
    }
    for index, trade_date in enumerate(trade_dates):
        for security_id, params in security_parameters.items():
            open_price = 10.0 * (params["growth"] ** index)
            pre_close = previous_close_by_security.get(security_id, open_price / 1.001)
            close_price = open_price * 1.001
            rows.append(
                (
                    security_id,
                    trade_date,
                    "SH",
                    "main_board",
                    False,
                    pre_close,
                    open_price,
                    open_price * 1.01,
                    open_price * 0.99,
                    close_price,
                    close_price,
                    params["turnover"],
                    params["pb"],
                )
            )
            previous_close_by_security[security_id] = close_price
    conn.executemany(
        """
        INSERT INTO daily_bar_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.close()


def _write_temp_fundamental_sleeve(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                'id = "fundamental_rerating_core_test"',
                'name = "Quality Value Anchor Core Sleeve Test"',
                'mandate_id = "a_share_long_only_eod"',
                'thesis_id = "fundamental_rerating"',
                'descriptor_set_id = "fundamental_rerating_core"',
                'target_id = "open_t1_to_open_t20_residual_net_cost"',
                'universe = "investable_a_share_core"',
                'rebalance_frequency = "weekly"',
                'target_holding_days = 30',
                'turnover_budget = 0.10',
                'execution_rule = "next_day_open"',
                'neutralization = ["industry", "size", "beta"]',
                "",
                "[construction]",
                'selection = "rank_then_cap_weight"',
                'holding_count = 2',
                'weight_cap = 0.08',
                "",
                "[constraints]",
                'min_median_daily_turnover_cny_mn = 50',
                'exclude_price_limit_lock = true',
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_temp_trend_sleeve(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                'id = "trend_leadership_core_residual_test"',
                'name = "Trend Leadership Core Residual Test"',
                'mandate_id = "a_share_long_only_eod"',
                'thesis_id = "trend_leadership"',
                'descriptor_set_id = "trend_leadership_core_residual"',
                'target_id = "open_t1_to_open_t20_residual_net_cost"',
                'universe = "investable_a_share_core"',
                'rebalance_frequency = "weekly"',
                'target_holding_days = 20',
                'turnover_budget = 0.16',
                'execution_rule = "next_day_open"',
                'neutralization = ["industry", "size", "beta"]',
                "",
                "[construction]",
                'selection = "rank_then_cap_weight"',
                'holding_count = 2',
                'weight_cap = 0.07',
                "",
                "[constraints]",
                'min_median_daily_turnover_cny_mn = 80',
                'exclude_price_limit_lock = true',
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_fundamental_case(path: Path, source_db: Path, sleeve_path: Path, start_date: str, end_date: str) -> None:
    path.write_text(
        "\n".join(
            [
                'schema_version = 1',
                'artifact_type = "fundamental_research_input_build_case"',
                'case_id = "fundamental_rerating_duckdb_case"',
                'description = "Build residualized fundamental observation inputs from the isolated V2 DuckDB."',
                f'sleeve_path = "{sleeve_path}"',
                f'source_db_path = "{source_db}"',
                f'output_path = "{path.parent / "fundamental_input.json"}"',
                'residual_component_snapshot_path = "output/open_t1_to_open_t20_residual_component_snapshot.json"',
                f'start_date = "{start_date}"',
                f'end_date = "{end_date}"',
                'min_listing_days = 120',
                'lookback_days = 20',
                'turnover_window_days = 20',
                'rebalance_stride = 5',
                'industry_label_source = "industry_classification_pit"',
                'industry_schema = "sw2021_l1"',
                'limit_lock_mode = "disabled"',
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_trend_case(path: Path, source_db: Path, sleeve_path: Path, start_date: str, end_date: str) -> None:
    path.write_text(
        "\n".join(
            [
                'schema_version = 1',
                'artifact_type = "trend_research_input_build_case"',
                'case_id = "trend_leadership_residual_duckdb_case"',
                'description = "Build residualized trend observation inputs from the isolated V2 DuckDB."',
                f'sleeve_path = "{sleeve_path}"',
                f'source_db_path = "{source_db}"',
                f'output_path = "{path.parent / "trend_input.json"}"',
                'residual_component_snapshot_path = "output/open_t1_to_open_t20_residual_component_snapshot.json"',
                f'start_date = "{start_date}"',
                f'end_date = "{end_date}"',
                'min_listing_days = 120',
                'lookback_days = 60',
                'short_window_days = 20',
                'turnover_window_days = 20',
                'rebalance_stride = 5',
                'industry_label_source = "industry_classification_pit"',
                'industry_schema = "sw2021_l1"',
                'limit_lock_mode = "disabled"',
                'residualization_mode = "audited_residual_components"',
                "",
            ]
        ),
        encoding="utf-8",
    )


class ResidualSnapshotRequiredCoverageBuilderTest(unittest.TestCase):
    def test_cli_builds_required_coverage_manifest_from_cases_without_snapshot_file(self) -> None:
        from alpha_find_v2.fundamental_research_input_builder import (
            build_fundamental_research_observation_input,
            load_fundamental_research_input_build_case,
        )
        from alpha_find_v2.trend_research_input_builder import (
            build_trend_research_observation_input,
            load_trend_research_input_build_case,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            fundamental_sleeve_path = temp_root / "fundamental_rerating_core_test.toml"
            trend_sleeve_path = temp_root / "trend_leadership_core_residual_test.toml"
            fundamental_case_path = temp_root / "fundamental_case.toml"
            trend_case_path = temp_root / "trend_case.toml"
            output_path = temp_root / "required_coverage.json"
            trade_dates = _trading_days(date(2024, 1, 2), 100)
            first_signal = trade_dates[60]
            second_signal = trade_dates[65]

            _create_combined_research_source_db(source_db, trade_dates)
            _write_temp_fundamental_sleeve(fundamental_sleeve_path)
            _write_temp_trend_sleeve(trend_sleeve_path)
            _write_fundamental_case(
                fundamental_case_path,
                source_db,
                fundamental_sleeve_path,
                first_signal,
                trade_dates[69],
            )
            _write_trend_case(
                trend_case_path,
                source_db,
                trend_sleeve_path,
                first_signal,
                trade_dates[69],
            )

            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "alpha_find_v2",
                    "build-residual-snapshot-required-coverage",
                    "--fundamental-case",
                    str(fundamental_case_path),
                    "--trend-case",
                    str(trend_case_path),
                    "--output-path",
                    str(output_path),
                    "--as-of-date",
                    "2026-04-26",
                ],
                capture_output=True,
                check=False,
                cwd=PROJECT_ROOT,
                env={**os.environ, "PYTHONPATH": "src"},
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)

            payload = json.loads(completed.stdout)
            manifest = json.loads(output_path.read_text(encoding="utf-8"))

            fundamental_case = load_fundamental_research_input_build_case(fundamental_case_path)
            trend_case = load_trend_research_input_build_case(trend_case_path)
            fundamental_result = build_fundamental_research_observation_input(
                replace(fundamental_case, residual_components=[])
            )
            trend_result = build_trend_research_observation_input(
                replace(trend_case, residual_components=[])
            )

            expected_by_date: dict[str, set[str]] = {}
            for result in (fundamental_result, trend_result):
                for step in result.observation_input.steps:
                    expected_by_date.setdefault(step.trade_date, set()).update(
                        record.asset_id for record in step.records
                    )

            expected_records_by_trade_date = [
                {
                    "trade_date": trade_date,
                    "required_union_asset_count": len(asset_ids),
                    "required_union_asset_ids": sorted(asset_ids),
                }
                for trade_date, asset_ids in sorted(expected_by_date.items())
            ]
            expected_observation_count = sum(
                len(asset_ids) for asset_ids in expected_by_date.values()
            )
            expected_unique_asset_count = len(
                {
                    asset_id
                    for asset_ids in expected_by_date.values()
                    for asset_id in asset_ids
                }
            )

            self.assertEqual(
                manifest,
                {
                    "schema_version": 1,
                    "artifact_type": "residual_snapshot_required_coverage",
                    "target_id": "open_t1_to_open_t20_residual_net_cost",
                    "generated_from": {
                        "fundamental_case_path": str(fundamental_case_path),
                        "trend_case_path": str(trend_case_path),
                        "source_db_path": str(source_db),
                        "as_of_date": "2026-04-26",
                    },
                    "summary": {
                        "decision_date_count": len(expected_by_date),
                        "required_union_observation_count": expected_observation_count,
                        "required_union_asset_count": expected_unique_asset_count,
                    },
                    "records_by_trade_date": expected_records_by_trade_date,
                },
            )
            self.assertEqual(payload["output_path"], str(output_path))
            self.assertEqual(payload["decision_date_count"], len(expected_by_date))
            self.assertEqual(
                payload["required_union_observation_count"],
                expected_observation_count,
            )
            self.assertEqual(
                payload["required_union_asset_count"],
                expected_unique_asset_count,
            )
            self.assertEqual(payload["fundamental_observation_count"], 4)
            self.assertEqual(payload["trend_observation_count"], 4)
            self.assertEqual(payload["trade_dates"], [first_signal, second_signal])
