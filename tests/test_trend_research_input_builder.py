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


def _create_research_source_db(
    path: Path,
    trade_dates: list[str],
    bar_overrides: dict[tuple[str, str], dict[str, object]] | None = None,
    missing_rows: set[tuple[str, str]] | None = None,
) -> None:
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
            turnover_value_cny DOUBLE
        )
        """
    )

    conn.executemany(
        "INSERT INTO market_trade_calendar VALUES (?)",
        [(trade_date,) for trade_date in trade_dates],
    )
    conn.executemany(
        """
        INSERT INTO security_master_ref VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TIMESTAMP '2026-04-23 09:00:00')
        """,
        [
            ("600001.SH", "600001", "Alpha Leader", "SH", "main_board", "上海", "20200102", None, "N", True),
            ("600002.SH", "600002", "Beta Runner", "SH", "main_board", "上海", "20200102", None, "N", True),
            ("600003.SH", "600003", "ST Ghost", "SH", "main_board", "上海", "20200102", None, "N", True),
            ("600004.SH", "600004", "New Listing", "SH", "main_board", "上海", "20240515", None, "N", True),
            ("600005.SH", "600005", "Illiquid Drift", "SH", "main_board", "上海", "20200102", None, "N", True),
        ],
    )

    rows: list[tuple[object, ...]] = []
    previous_close_by_security: dict[str, float] = {}
    overrides = bar_overrides or {}
    skipped_rows = missing_rows or set()
    for index, trade_date in enumerate(trade_dates):
        for security_id, growth, turnover, is_st in (
            ("600001.SH", 1.0120, 220_000_000.0, False),
            ("600002.SH", 1.0070, 150_000_000.0, False),
            ("600003.SH", 1.0100, 180_000_000.0, True),
            ("600004.SH", 1.0090, 140_000_000.0, False),
            ("600005.SH", 1.0080, 12_000_000.0, False),
        ):
            if (security_id, trade_date) in skipped_rows:
                continue
            open_price = 10.0 * (growth**index)
            pre_close = previous_close_by_security.get(security_id, open_price / 1.001)
            close_price = open_price * 1.001
            row = {
                "is_st": is_st,
                "pre_close": pre_close,
                "open": open_price,
                "high": open_price * 1.01,
                "low": open_price * 0.99,
                "close": close_price,
                "close_adj": close_price,
                "turnover_value_cny": turnover,
            }
            row.update(overrides.get((security_id, trade_date), {}))
            rows.append(
                (
                    security_id,
                    trade_date,
                    "SH",
                    "main_board",
                    row["is_st"],
                    row["pre_close"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["close_adj"],
                    row["turnover_value_cny"],
                )
            )
            previous_close_by_security[security_id] = float(row["close"])
    conn.executemany(
        """
        INSERT INTO daily_bar_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.close()


def _add_security_with_daily_bars(
    path: Path,
    trade_dates: list[str],
    *,
    security_id: str,
    symbol: str,
    exchange: str,
    board: str,
    growth: float,
    turnover_value_cny: float,
    list_date: str = "20200102",
    is_st: bool = False,
) -> None:
    conn = duckdb.connect(str(path))
    conn.execute(
        """
        INSERT INTO security_master_ref VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TIMESTAMP '2026-04-23 09:00:00')
        """,
        (
            security_id,
            symbol,
            f"{symbol} Candidate",
            exchange,
            board,
            "上海",
            list_date,
            None,
            "N",
            True,
        ),
    )

    rows: list[tuple[object, ...]] = []
    previous_close: float | None = None
    for index, trade_date in enumerate(trade_dates):
        open_price = 10.0 * (growth**index)
        pre_close = previous_close if previous_close is not None else open_price / 1.001
        close_price = open_price * 1.001
        rows.append(
            (
                security_id,
                trade_date,
                exchange,
                board,
                is_st,
                pre_close,
                open_price,
                open_price * 1.01,
                open_price * 0.99,
                close_price,
                close_price,
                turnover_value_cny,
            )
        )
        previous_close = close_price
    conn.executemany(
        """
        INSERT INTO daily_bar_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.close()


def _add_industry_classification_pit(
    path: Path,
    rows: list[tuple[str, str, str, str, str | None]],
) -> None:
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
    conn.executemany(
        """
        INSERT INTO industry_classification_pit VALUES (?, ?, ?, ?, ?)
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
    risk_model_id: str = "a_share_core_equity",
) -> None:
    payload = {
        "schema_version": 1,
        "artifact_type": "residual_component_snapshot",
        "target_id": target_id,
        "risk_model_id": risk_model_id,
        "provenance": {
            "benchmark_definition": "CSI 800",
            "industry_schema": "sw2021_l1",
            "generation_date": "2026-04-25",
            "audited_export_path": "/audit/export/residual_component_snapshot.py",
            "risk_model_id": risk_model_id,
        },
        "steps": [
            {
                "trade_date": trade_date,
                "records": [
                    {
                        "asset_id": security_id,
                        "residual_components": {
                            "benchmark": 0.0100,
                            "industry": 0.0040 if security_id == "600001.SH" else 0.0030,
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


def _write_temp_residual_trend_sleeve(path: Path) -> None:
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


class TrendResearchInputBuilderTest(unittest.TestCase):
    def test_builder_emits_weekly_trend_observation_input_from_duckdb(self) -> None:
        from alpha_find_v2.trend_research_input_builder import (
            build_trend_research_observation_input,
            load_trend_research_input_build_case,
            write_trend_research_observation_input,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            output_path = temp_root / "trend_input.json"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            _create_research_source_db(source_db, trade_dates)

            first_signal = trade_dates[60]
            second_signal = trade_dates[65]
            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_leadership_duckdb_case"',
                        'description = "Build trend observation inputs from the isolated V2 DuckDB."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{output_path}"',
                        f'start_date = "{first_signal}"',
                        f'end_date = "{trade_dates[69]}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "omit"',
                        'limit_lock_mode = "disabled"',
                        'residualization_mode = "non_residual_target"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_trend_research_input_build_case(case_path)
            result = build_trend_research_observation_input(loaded_case)
            write_trend_research_observation_input(result, loaded_case.definition.output_path)

            self.assertEqual(result.sleeve_id, "trend_leadership_core")
            self.assertIn("industry_relative_branch_blocked", result.warnings)
            self.assertEqual([step.trade_date for step in result.observation_input.steps], [first_signal, second_signal])

            first_step = result.observation_input.steps[0]
            self.assertEqual([record.asset_id for record in first_step.records], ["600001.SH", "600002.SH"])
            self.assertEqual([record.rank for record in first_step.records], [1, 2])
            self.assertTrue(isclose(sum(record.target_weight for record in first_step.records), 1.0))
            self.assertTrue(all(isclose(record.target_weight, 0.5) for record in first_step.records))
            self.assertEqual(first_step.records[0].industry, "")
            self.assertTrue(first_step.records[0].entry_state.liquidity_pass)
            self.assertTrue(first_step.records[0].exit_state.liquidity_pass)
            self.assertEqual(first_step.records[0].residual_components, {})

            entry_index = trade_dates.index(first_signal) + 1
            exit_index = trade_dates.index(first_signal) + 20
            expected_entry = 10.0 * (1.0120**entry_index)
            expected_exit = 10.0 * (1.0120**exit_index)
            self.assertAlmostEqual(first_step.records[0].entry_open, expected_entry)
            self.assertAlmostEqual(first_step.records[0].exit_open, expected_exit)

            roundtrip = load_sleeve_research_observation_input(output_path)
            self.assertEqual([step.trade_date for step in roundtrip.steps], [first_signal, second_signal])
            self.assertEqual([record.asset_id for record in roundtrip.steps[0].records], ["600001.SH", "600002.SH"])

    def test_builder_excludes_configured_boards_without_excluding_twenty_percent_boards(self) -> None:
        from alpha_find_v2.trend_research_input_builder import (
            build_trend_research_observation_input,
            load_trend_research_input_build_case,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            _create_research_source_db(source_db, trade_dates)
            _add_security_with_daily_bars(
                source_db,
                trade_dates,
                security_id="920946.BJ",
                symbol="920946",
                exchange="BJ",
                board="beijing",
                growth=1.0200,
                turnover_value_cny=500_000_000.0,
            )
            _add_security_with_daily_bars(
                source_db,
                trade_dates,
                security_id="300001.SZ",
                symbol="300001",
                exchange="SZ",
                board="chinext",
                growth=1.0180,
                turnover_value_cny=450_000_000.0,
            )

            first_signal = trade_dates[60]
            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_exclude_beijing_case"',
                        'description = "Exclude boards outside the live trading mandate."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "trend_input.json"}"',
                        f'start_date = "{first_signal}"',
                        f'end_date = "{first_signal}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "omit"',
                        'limit_lock_mode = "disabled"',
                        'residualization_mode = "non_residual_target"',
                        'exclude_boards = ["beijing"]',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_trend_research_input_build_case(case_path)
            result = build_trend_research_observation_input(loaded_case)

            selected_ids = [
                record.asset_id for record in result.observation_input.steps[0].records
            ]
            self.assertIn("300001.SZ", selected_ids)
            self.assertNotIn("920946.BJ", selected_ids)

    def test_builder_populates_industry_from_pit_classification_table(self) -> None:
        from alpha_find_v2.trend_research_input_builder import (
            build_trend_research_observation_input,
            load_trend_research_input_build_case,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            _create_research_source_db(source_db, trade_dates)
            _add_industry_classification_pit(
                source_db,
                [
                    ("600001.SH", "sw2021_l1", "801010.SI", "20210101", None),
                    ("600001.SH", "citics_l1", "bank", "20200102", None),
                    ("600002.SH", "citics_l1", "tech", "20200102", None),
                    ("600005.SH", "citics_l1", "industrial", "20200102", None),
                ],
            )

            first_signal = trade_dates[60]
            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_leadership_duckdb_case"',
                        'description = "Build trend observation inputs from the isolated V2 DuckDB."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "trend_input.json"}"',
                        f'start_date = "{first_signal}"',
                        f'end_date = "{trade_dates[69]}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "industry_classification_pit"',
                        'industry_schema = "citics_l1"',
                        'limit_lock_mode = "disabled"',
                        'residualization_mode = "non_residual_target"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_trend_research_input_build_case(case_path)
            result = build_trend_research_observation_input(loaded_case)

            first_step = result.observation_input.steps[0]
            self.assertEqual([record.industry for record in first_step.records], ["bank", "tech"])
            self.assertNotIn("industry_labels_omitted", result.warnings)
            self.assertEqual(
                result.warnings,
                ["industry_relative_branch_blocked", "limit_lock_detection_disabled"],
            )

    def test_builder_treats_intraday_industry_changes_as_not_same_day_usable(self) -> None:
        from alpha_find_v2.trend_research_input_builder import (
            build_trend_research_observation_input,
            load_trend_research_input_build_case,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            first_signal = trade_dates[60]
            intraday_cutover = (
                f"{first_signal[:4]}-{first_signal[4:6]}-{first_signal[6:8]} 12:00:00"
            )
            _create_research_source_db(source_db, trade_dates)
            _add_industry_classification_pit(
                source_db,
                [
                    ("600001.SH", "sw2021_l1", "bank", "20200102", intraday_cutover),
                    ("600001.SH", "sw2021_l1", "tech", intraday_cutover, None),
                    ("600002.SH", "sw2021_l1", "industrial", "20200102", None),
                    ("600005.SH", "sw2021_l1", "materials", "20200102", None),
                ],
            )

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_intraday_industry_case"',
                        'description = "Use conservative PIT timing for intraday industry changes."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "trend_input.json"}"',
                        f'start_date = "{first_signal}"',
                        f'end_date = "{first_signal}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "industry_classification_pit"',
                        'industry_schema = "sw2021_l1"',
                        'limit_lock_mode = "disabled"',
                        'residualization_mode = "non_residual_target"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_trend_research_input_build_case(case_path)
            result = build_trend_research_observation_input(loaded_case)

            first_step = result.observation_input.steps[0]
            self.assertEqual([record.asset_id for record in first_step.records], ["600001.SH", "600002.SH"])
            self.assertEqual([record.industry for record in first_step.records], ["bank", "industrial"])

    def test_builder_marks_directional_cn_a_open_limit_locks(self) -> None:
        from alpha_find_v2.trend_research_input_builder import (
            build_trend_research_observation_input,
            load_trend_research_input_build_case,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            first_signal = trade_dates[60]
            _create_research_source_db(
                source_db,
                trade_dates,
                bar_overrides={
                    ("600001.SH", trade_dates[61]): {
                        "pre_close": 10.0,
                        "open": 11.0,
                        "high": 11.0,
                        "low": 11.0,
                        "close": 11.0,
                        "close_adj": 11.0,
                    },
                    ("600002.SH", trade_dates[80]): {
                        "pre_close": 10.0,
                        "open": 9.0,
                        "high": 9.0,
                        "low": 9.0,
                        "close": 9.0,
                        "close_adj": 9.0,
                    },
                },
            )

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_leadership_duckdb_case"',
                        'description = "Detect directional CN-A open-limit locks."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "trend_input.json"}"',
                        f'start_date = "{first_signal}"',
                        f'end_date = "{trade_dates[69]}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "omit"',
                        'limit_lock_mode = "cn_a_directional_open_lock"',
                        'residualization_mode = "non_residual_target"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_trend_research_input_build_case(case_path)
            result = build_trend_research_observation_input(loaded_case)

            first_step = result.observation_input.steps[0]
            self.assertEqual([record.asset_id for record in first_step.records], ["600001.SH", "600002.SH"])
            self.assertTrue(first_step.records[0].entry_state.limit_locked)
            self.assertFalse(first_step.records[0].exit_state.limit_locked)
            self.assertFalse(first_step.records[1].entry_state.limit_locked)
            self.assertTrue(first_step.records[1].exit_state.limit_locked)
            self.assertEqual(result.warnings, ["industry_relative_branch_blocked", "industry_labels_omitted"])

    def test_builder_retains_no_open_legs_as_suspended_states(self) -> None:
        from alpha_find_v2.trend_research_input_builder import (
            build_trend_research_observation_input,
            load_trend_research_input_build_case,
            write_trend_research_observation_input,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            output_path = temp_root / "trend_input.json"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            first_signal = trade_dates[60]
            _create_research_source_db(
                source_db,
                trade_dates,
                bar_overrides={
                    ("600001.SH", trade_dates[61]): {
                        "pre_close": 10.2,
                        "open": None,
                        "high": None,
                        "low": None,
                        "close": 10.2,
                        "close_adj": 10.2,
                        "turnover_value_cny": 0.0,
                    },
                    ("600002.SH", trade_dates[80]): {
                        "pre_close": 10.3,
                        "open": None,
                        "high": None,
                        "low": None,
                        "close": 10.3,
                        "close_adj": 10.3,
                        "turnover_value_cny": 0.0,
                    },
                },
            )

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_leadership_duckdb_case"',
                        'description = "Preserve no-open trade legs as suspended states."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{output_path}"',
                        f'start_date = "{first_signal}"',
                        f'end_date = "{first_signal}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "omit"',
                        'limit_lock_mode = "cn_a_directional_open_lock"',
                        'residualization_mode = "non_residual_target"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_trend_research_input_build_case(case_path)
            result = build_trend_research_observation_input(loaded_case)
            write_trend_research_observation_input(result, output_path)

            first_step = result.observation_input.steps[0]
            self.assertEqual([record.asset_id for record in first_step.records], ["600001.SH", "600002.SH"])
            self.assertTrue(first_step.records[0].entry_state.suspended)
            self.assertFalse(first_step.records[0].entry_state.limit_locked)
            self.assertAlmostEqual(first_step.records[0].entry_open, 10.2)
            self.assertFalse(first_step.records[0].exit_state.suspended)
            self.assertFalse(first_step.records[1].entry_state.suspended)
            self.assertTrue(first_step.records[1].exit_state.suspended)
            self.assertFalse(first_step.records[1].exit_state.limit_locked)
            self.assertAlmostEqual(first_step.records[1].exit_open, 10.3)

            roundtrip = load_sleeve_research_observation_input(output_path)
            self.assertTrue(roundtrip.steps[0].records[0].entry_state.suspended)
            self.assertTrue(roundtrip.steps[0].records[1].exit_state.suspended)

    def test_builder_marks_missing_entry_row_as_suspended(self) -> None:
        from alpha_find_v2.trend_research_input_builder import (
            build_trend_research_observation_input,
            load_trend_research_input_build_case,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            first_signal = trade_dates[60]
            entry_trade_date = trade_dates[61]
            expected_fallback = 10.0 * (1.0120**60) * 1.001
            _create_research_source_db(
                source_db,
                trade_dates,
                missing_rows={("600001.SH", entry_trade_date)},
            )

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_leadership_duckdb_case"',
                        'description = "Treat missing entry rows as suspended trade legs."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "trend_input.json"}"',
                        f'start_date = "{first_signal}"',
                        f'end_date = "{first_signal}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "omit"',
                        'limit_lock_mode = "cn_a_directional_open_lock"',
                        'residualization_mode = "non_residual_target"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_trend_research_input_build_case(case_path)
            result = build_trend_research_observation_input(loaded_case)

            first_step = result.observation_input.steps[0]
            self.assertEqual(first_step.records[0].asset_id, "600001.SH")
            self.assertTrue(first_step.records[0].entry_state.suspended)
            self.assertFalse(first_step.records[0].entry_state.limit_locked)
            self.assertAlmostEqual(first_step.records[0].entry_open, expected_fallback)

    def test_builder_marks_missing_exit_row_as_suspended(self) -> None:
        from alpha_find_v2.trend_research_input_builder import (
            build_trend_research_observation_input,
            load_trend_research_input_build_case,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            first_signal = trade_dates[60]
            exit_trade_date = trade_dates[80]
            expected_fallback = 10.0 * (1.0070**79) * 1.001
            _create_research_source_db(
                source_db,
                trade_dates,
                missing_rows={("600002.SH", exit_trade_date)},
            )

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_leadership_duckdb_case"',
                        'description = "Treat missing exit rows as suspended trade legs."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "trend_input.json"}"',
                        f'start_date = "{first_signal}"',
                        f'end_date = "{first_signal}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "omit"',
                        'limit_lock_mode = "cn_a_directional_open_lock"',
                        'residualization_mode = "non_residual_target"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_trend_research_input_build_case(case_path)
            result = build_trend_research_observation_input(loaded_case)

            first_step = result.observation_input.steps[0]
            self.assertEqual(first_step.records[1].asset_id, "600002.SH")
            self.assertTrue(first_step.records[1].exit_state.suspended)
            self.assertFalse(first_step.records[1].exit_state.limit_locked)
            self.assertAlmostEqual(first_step.records[1].exit_open, expected_fallback)

    def test_builder_rejects_missing_pit_industry_for_selected_name(self) -> None:
        from alpha_find_v2.trend_research_input_builder import (
            build_trend_research_observation_input,
            load_trend_research_input_build_case,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            _create_research_source_db(source_db, trade_dates)
            _add_industry_classification_pit(
                source_db,
                [
                    ("600001.SH", "citics_l1", "bank", "20200102", None),
                ],
            )

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_leadership_duckdb_case"',
                        'description = "Build trend observation inputs from the isolated V2 DuckDB."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "trend_input.json"}"',
                        f'start_date = "{trade_dates[60]}"',
                        f'end_date = "{trade_dates[69]}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "industry_classification_pit"',
                        'industry_schema = "citics_l1"',
                        'limit_lock_mode = "disabled"',
                        'residualization_mode = "non_residual_target"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_trend_research_input_build_case(case_path)
            with self.assertRaisesRegex(
                ValueError,
                "Missing PIT industry label for trend research observation: 600002\\.SH",
            ):
                build_trend_research_observation_input(loaded_case)

    def test_builder_requires_explicit_industry_schema_for_pit_labels(self) -> None:
        from alpha_find_v2.trend_research_input_builder import load_trend_research_input_build_case

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            _create_research_source_db(source_db, trade_dates)

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_leadership_duckdb_case"',
                        'description = "Reject missing PIT industry schema."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "trend_input.json"}"',
                        f'start_date = "{trade_dates[60]}"',
                        f'end_date = "{trade_dates[69]}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "industry_classification_pit"',
                        'limit_lock_mode = "disabled"',
                        'residualization_mode = "non_residual_target"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                "Trend research input build case must define industry_schema when industry_label_source='industry_classification_pit'",
            ):
                load_trend_research_input_build_case(case_path)

    def test_builder_requires_residual_component_snapshot_for_residual_target(self) -> None:
        from alpha_find_v2.trend_research_input_builder import load_trend_research_input_build_case

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "build_case.toml"
            sleeve_path = temp_root / "trend_leadership_core_residual_test.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            _create_research_source_db(source_db, trade_dates)
            _write_temp_residual_trend_sleeve(sleeve_path)

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_leadership_residual_duckdb_case"',
                        'description = "Reject missing residual snapshot path for residual trend targets."',
                        f'sleeve_path = "{sleeve_path}"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "trend_input.json"}"',
                        f'start_date = "{trade_dates[60]}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "omit"',
                        'limit_lock_mode = "disabled"',
                        'residualization_mode = "audited_residual_components"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                "Trend research input build case requires residual_component_snapshot_path",
            ):
                load_trend_research_input_build_case(case_path)

    def test_builder_rejects_residual_mode_for_non_residual_target(self) -> None:
        from alpha_find_v2.trend_research_input_builder import load_trend_research_input_build_case

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            residual_snapshot_path = temp_root / "residual_components.json"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            _create_research_source_db(source_db, trade_dates)
            _write_residual_component_snapshot(
                residual_snapshot_path,
                target_id="open_t1_to_open_t20_residual_net_cost",
                trade_dates=[trade_dates[60]],
                security_ids=["600001.SH", "600002.SH"],
            )

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_leadership_mismatched_residual_mode_case"',
                        'description = "Reject residual mode when the sleeve target is still non-residual."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "trend_input.json"}"',
                        f'residual_component_snapshot_path = "{residual_snapshot_path}"',
                        f'start_date = "{trade_dates[60]}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "omit"',
                        'limit_lock_mode = "disabled"',
                        'residualization_mode = "audited_residual_components"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                "Trend research input build case residualization_mode='audited_residual_components' requires a residual target",
            ):
                load_trend_research_input_build_case(case_path)

    def test_builder_emits_residualized_trend_observation_input_from_snapshot(self) -> None:
        from alpha_find_v2.trend_research_input_builder import (
            build_trend_research_observation_input,
            load_trend_research_input_build_case,
            write_trend_research_observation_input,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            output_path = temp_root / "trend_input.json"
            case_path = temp_root / "build_case.toml"
            sleeve_path = temp_root / "trend_leadership_core_residual_test.toml"
            residual_snapshot_path = temp_root / "residual_components.json"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            _create_research_source_db(source_db, trade_dates)
            _write_temp_residual_trend_sleeve(sleeve_path)
            _write_residual_component_snapshot(
                residual_snapshot_path,
                target_id="open_t1_to_open_t20_residual_net_cost",
                trade_dates=[trade_dates[60], trade_dates[65]],
                security_ids=["600001.SH", "600002.SH"],
            )

            first_signal = trade_dates[60]
            second_signal = trade_dates[65]
            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_leadership_residual_duckdb_case"',
                        'description = "Build residualized trend observation inputs from the isolated V2 DuckDB."',
                        f'sleeve_path = "{sleeve_path}"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{output_path}"',
                        f'residual_component_snapshot_path = "{residual_snapshot_path}"',
                        f'start_date = "{first_signal}"',
                        f'end_date = "{trade_dates[69]}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "omit"',
                        'limit_lock_mode = "disabled"',
                        'residualization_mode = "audited_residual_components"',
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_trend_research_input_build_case(case_path)
            result = build_trend_research_observation_input(loaded_case)
            write_trend_research_observation_input(result, output_path)

            self.assertEqual(result.sleeve_id, "trend_leadership_core_residual_test")
            self.assertEqual([step.trade_date for step in result.observation_input.steps], [first_signal, second_signal])
            self.assertEqual(
                result.observation_input.steps[0].records[0].residual_components,
                {
                    "benchmark": 0.01,
                    "industry": 0.004,
                    "size": -0.001,
                    "beta": 0.002,
                },
            )
            self.assertEqual(
                result.observation_input.steps[0].records[1].residual_components,
                {
                    "benchmark": 0.01,
                    "industry": 0.003,
                    "size": -0.001,
                    "beta": 0.002,
                },
            )

            roundtrip = load_sleeve_research_observation_input(output_path)
            self.assertEqual(
                roundtrip.steps[0].records[0].residual_components,
                {
                    "benchmark": 0.01,
                    "industry": 0.004,
                    "size": -0.001,
                    "beta": 0.002,
                },
            )

    def test_cn_a_directional_open_lock_uses_board_specific_limit_bands(self) -> None:
        from alpha_find_v2.trend_research_input_builder import _is_cn_a_directional_open_lock

        self.assertTrue(
            _is_cn_a_directional_open_lock(
                board="main_board",
                is_st=False,
                pre_close=10.0,
                open_price=11.0,
                high_price=11.0,
                low_price=11.0,
                direction="entry",
            )
        )
        self.assertTrue(
            _is_cn_a_directional_open_lock(
                board="main_board",
                is_st=True,
                pre_close=10.0,
                open_price=10.5,
                high_price=10.5,
                low_price=10.5,
                direction="entry",
            )
        )
        self.assertTrue(
            _is_cn_a_directional_open_lock(
                board="chinext",
                is_st=False,
                pre_close=10.0,
                open_price=12.0,
                high_price=12.0,
                low_price=12.0,
                direction="entry",
            )
        )
        self.assertTrue(
            _is_cn_a_directional_open_lock(
                board="beijing",
                is_st=False,
                pre_close=10.0,
                open_price=13.0,
                high_price=13.0,
                low_price=13.0,
                direction="entry",
            )
        )
        self.assertTrue(
            _is_cn_a_directional_open_lock(
                board="main_board",
                is_st=False,
                pre_close=10.0,
                open_price=9.0,
                high_price=9.0,
                low_price=9.0,
                direction="exit",
            )
        )
        self.assertFalse(
            _is_cn_a_directional_open_lock(
                board="main_board",
                is_st=False,
                pre_close=10.0,
                open_price=11.0,
                high_price=11.0,
                low_price=10.95,
                direction="entry",
            )
        )

    def test_cli_build_trend_research_input_writes_output_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            output_path = temp_root / "trend_input.json"
            case_path = temp_root / "build_case.toml"
            trade_dates = _trading_days(date(2024, 1, 2), 95)
            _create_research_source_db(source_db, trade_dates)

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "trend_research_input_build_case"',
                        'case_id = "trend_leadership_duckdb_case"',
                        'description = "Build trend observation inputs from the isolated V2 DuckDB."',
                        'sleeve_path = "config/sleeves/trend_leadership_core.toml"',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{output_path}"',
                        f'start_date = "{trade_dates[60]}"',
                        f'end_date = "{trade_dates[69]}"',
                        'min_listing_days = 120',
                        'lookback_days = 60',
                        'short_window_days = 20',
                        'turnover_window_days = 20',
                        'rebalance_stride = 5',
                        'industry_label_source = "omit"',
                        'limit_lock_mode = "disabled"',
                        'residualization_mode = "non_residual_target"',
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
                    "build-trend-research-input",
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
            self.assertEqual(payload["sleeve_id"], "trend_leadership_core")
            self.assertEqual(payload["warnings"], [
                "industry_relative_branch_blocked",
                "industry_labels_omitted",
                "limit_lock_detection_disabled",
            ])
            self.assertEqual(len(payload["steps"]), 2)


if __name__ == "__main__":
    unittest.main()
