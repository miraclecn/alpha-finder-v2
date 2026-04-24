from __future__ import annotations

from datetime import date, timedelta
import json
from math import isclose
from pathlib import Path
import subprocess
import tempfile
import unittest

import duckdb

from alpha_find_v2.research_artifact_loader import load_sleeve_research_observation_input


def _trading_days(start: date, count: int) -> list[str]:
    days: list[str] = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return days


def _create_fundamental_source_db(path: Path, trade_dates: list[str]) -> None:
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
        INSERT INTO security_master_ref VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TIMESTAMP '2026-04-24 09:00:00')
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
        "600001.SH": {"growth": 1.0100, "turnover": 240_000_000.0, "pb": 0.85},
        "600002.SH": {"growth": 1.0060, "turnover": 180_000_000.0, "pb": 1.45},
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


def _write_residual_component_snapshot(
    path: Path,
    *,
    target_id: str,
    trade_dates: list[str],
    security_ids: list[str],
) -> None:
    payload = {
        "schema_version": 1,
        "artifact_type": "residual_component_snapshot",
        "target_id": target_id,
        "steps": [
            {
                "trade_date": trade_date,
                "records": [
                    {
                        "asset_id": security_id,
                        "residual_components": {
                            "benchmark": 0.0100,
                            "industry": 0.0040 if security_id in {"600001.SH", "600002.SH"} else 0.0030,
                            "size": -0.0010,
                            "beta": 0.0020,
                        },
                    }
                    for security_id in security_ids
                ],
            }
            for trade_date in trade_dates
        ],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


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


class FundamentalResearchInputBuilderTest(unittest.TestCase):
    def test_builder_emits_residualized_fundamental_observation_input_from_duckdb(self) -> None:
        from alpha_find_v2.fundamental_research_input_builder import (
            build_fundamental_research_observation_input,
            load_fundamental_research_input_build_case,
            write_fundamental_research_observation_input,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            output_path = temp_root / "fundamental_input.json"
            case_path = temp_root / "build_case.toml"
            sleeve_path = temp_root / "fundamental_rerating_core_test.toml"
            residual_snapshot_path = temp_root / "residual_components.json"
            trade_dates = _trading_days(date(2024, 1, 2), 70)
            _create_fundamental_source_db(source_db, trade_dates)
            _write_temp_fundamental_sleeve(sleeve_path)
            _write_residual_component_snapshot(
                residual_snapshot_path,
                target_id="open_t1_to_open_t20_residual_net_cost",
                trade_dates=[trade_dates[20], trade_dates[25]],
                security_ids=["600001.SH", "600002.SH", "600003.SH", "600004.SH"],
            )

            first_signal = trade_dates[20]
            second_signal = trade_dates[25]
            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "fundamental_research_input_build_case"',
                        'case_id = "fundamental_rerating_duckdb_case"',
                        'description = "Build residualized fundamental observation inputs from the isolated V2 DuckDB."',
                        f'sleeve_path = "{sleeve_path}"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{output_path}"',
                        f'residual_component_snapshot_path = "{residual_snapshot_path}"',
                        f'start_date = "{first_signal}"',
                        f'end_date = "{trade_dates[29]}"',
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

            loaded_case = load_fundamental_research_input_build_case(case_path)
            result = build_fundamental_research_observation_input(loaded_case)
            write_fundamental_research_observation_input(result, loaded_case.definition.output_path)

            self.assertEqual(result.sleeve_id, "fundamental_rerating_core_test")
            self.assertEqual([step.trade_date for step in result.observation_input.steps], [first_signal, second_signal])
            self.assertEqual(
                result.warnings,
                ["fundamental_snapshot_pit_amber_anchor_only", "limit_lock_detection_disabled"],
            )

            first_step = result.observation_input.steps[0]
            self.assertEqual([record.asset_id for record in first_step.records], ["600001.SH", "600003.SH"])
            self.assertEqual([record.rank for record in first_step.records], [1, 2])
            self.assertTrue(all(isclose(record.target_weight, 0.5) for record in first_step.records))
            self.assertEqual([record.industry for record in first_step.records], ["bank", "tech"])
            self.assertEqual(
                first_step.records[0].residual_components,
                {
                    "benchmark": 0.01,
                    "industry": 0.004,
                    "size": -0.001,
                    "beta": 0.002,
                },
            )

            roundtrip = load_sleeve_research_observation_input(output_path)
            self.assertEqual(
                [record.asset_id for record in roundtrip.steps[0].records],
                ["600001.SH", "600003.SH"],
            )

    def test_builder_requires_residual_component_snapshot_for_residual_target(self) -> None:
        from alpha_find_v2.fundamental_research_input_builder import load_fundamental_research_input_build_case

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            sleeve_path = temp_root / "fundamental_rerating_core_test.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 40)
            _create_fundamental_source_db(source_db, trade_dates)
            _write_temp_fundamental_sleeve(sleeve_path)

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "fundamental_research_input_build_case"',
                        'case_id = "fundamental_rerating_duckdb_case"',
                        'description = "Reject missing residual snapshot path for residual targets."',
                        f'sleeve_path = "{sleeve_path}"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "fundamental_input.json"}"',
                        f'start_date = "{trade_dates[20]}"',
                        'industry_label_source = "industry_classification_pit"',
                        'industry_schema = "sw2021_l1"',
                        'limit_lock_mode = "disabled"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                "Fundamental research input build case requires residual_component_snapshot_path",
            ):
                load_fundamental_research_input_build_case(case_path)

    def test_builder_rejects_missing_residual_component_for_selected_name(self) -> None:
        from alpha_find_v2.fundamental_research_input_builder import (
            build_fundamental_research_observation_input,
            load_fundamental_research_input_build_case,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            sleeve_path = temp_root / "fundamental_rerating_core_test.toml"
            residual_snapshot_path = temp_root / "residual_components.json"
            trade_dates = _trading_days(date(2024, 1, 2), 50)
            _create_fundamental_source_db(source_db, trade_dates)
            _write_temp_fundamental_sleeve(sleeve_path)
            _write_residual_component_snapshot(
                residual_snapshot_path,
                target_id="open_t1_to_open_t20_residual_net_cost",
                trade_dates=[trade_dates[20]],
                security_ids=["600002.SH", "600003.SH", "600004.SH"],
            )

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "fundamental_research_input_build_case"',
                        'case_id = "fundamental_rerating_duckdb_case"',
                        'description = "Reject missing residual components for selected names."',
                        f'sleeve_path = "{sleeve_path}"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "fundamental_input.json"}"',
                        f'residual_component_snapshot_path = "{residual_snapshot_path}"',
                        f'start_date = "{trade_dates[20]}"',
                        f'end_date = "{trade_dates[20]}"',
                        'lookback_days = 20',
                        'turnover_window_days = 20',
                        'industry_label_source = "industry_classification_pit"',
                        'industry_schema = "sw2021_l1"',
                        'limit_lock_mode = "disabled"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_fundamental_research_input_build_case(case_path)
            with self.assertRaisesRegex(
                ValueError,
                "Missing residual components for selected observation: 600001\\.SH",
            ):
                build_fundamental_research_observation_input(loaded_case)

    def test_cli_build_fundamental_research_input_writes_output_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            output_path = temp_root / "fundamental_input.json"
            case_path = temp_root / "build_case.toml"
            sleeve_path = temp_root / "fundamental_rerating_core_test.toml"
            residual_snapshot_path = temp_root / "residual_components.json"
            trade_dates = _trading_days(date(2024, 1, 2), 70)
            _create_fundamental_source_db(source_db, trade_dates)
            _write_temp_fundamental_sleeve(sleeve_path)
            _write_residual_component_snapshot(
                residual_snapshot_path,
                target_id="open_t1_to_open_t20_residual_net_cost",
                trade_dates=[trade_dates[20], trade_dates[25]],
                security_ids=["600001.SH", "600002.SH", "600003.SH", "600004.SH"],
            )

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "fundamental_research_input_build_case"',
                        'case_id = "fundamental_rerating_duckdb_case"',
                        'description = "CLI build of residualized fundamental observation input."',
                        f'sleeve_path = "{sleeve_path}"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{output_path}"',
                        f'residual_component_snapshot_path = "{residual_snapshot_path}"',
                        f'start_date = "{trade_dates[20]}"',
                        f'end_date = "{trade_dates[29]}"',
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

            result = subprocess.run(
                [
                    "python3",
                    "-m",
                    "alpha_find_v2",
                    "build-fundamental-research-input",
                    "--case",
                    str(case_path),
                ],
                cwd=Path(__file__).resolve().parents[1],
                env={"PYTHONPATH": "src"},
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertTrue(output_path.exists())
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["artifact_type"], "sleeve_research_observation_input")
            self.assertEqual(payload["warnings"], [
                "fundamental_snapshot_pit_amber_anchor_only",
                "limit_lock_detection_disabled",
            ])
            self.assertEqual(len(payload["steps"]), 2)


if __name__ == "__main__":
    unittest.main()
