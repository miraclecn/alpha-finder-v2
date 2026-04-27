from __future__ import annotations

from datetime import date, timedelta
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import duckdb

from alpha_find_v2.config_loader import CONFIG_ROOT, load_regime_overlay
from alpha_find_v2.regime_overlay import (
    RegimeOverlayEvaluator,
    load_regime_overlay_observation_artifact,
)
from alpha_find_v2.regime_overlay_observation_builder import (
    build_regime_overlay_observation_history,
    load_regime_overlay_observation_build_case,
    write_regime_overlay_observation_history,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _trading_days(start: date, count: int) -> list[str]:
    days: list[str] = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return days


class RegimeOverlayObservationBuilderTest(unittest.TestCase):
    def test_builder_maps_green_inputs_into_repeatable_overlay_observations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            trade_dates = _trading_days(date(2026, 1, 5), 12)
            source_db_path = temp_root / "research_source.duckdb"
            benchmark_state_path = temp_root / "benchmark_state_history.json"
            sleeve_artifact_path = temp_root / "trend_artifact.json"
            case_path = temp_root / "overlay_case.toml"

            self._create_source_db(source_db_path, trade_dates)
            self._write_benchmark_state(benchmark_state_path, trade_dates)
            self._write_sleeve_artifact(
                sleeve_artifact_path,
                decision_dates=[trade_dates[5], trade_dates[8], trade_dates[11]],
            )
            self._write_case(
                case_path=case_path,
                source_db_path=source_db_path,
                benchmark_state_path=benchmark_state_path,
                sleeve_artifact_path=sleeve_artifact_path,
                output_path=temp_root / "overlay_observations.json",
            )

            loaded_case = load_regime_overlay_observation_build_case(case_path)
            result = build_regime_overlay_observation_history(loaded_case)
            output_path = write_regime_overlay_observation_history(
                result,
                loaded_case.definition.output_path,
            )
            artifact = load_regime_overlay_observation_artifact(output_path)
            overlay = load_regime_overlay(
                CONFIG_ROOT / "regime_overlays" / "a_share_risk_overlay.toml"
            )
            evidence = RegimeOverlayEvaluator(overlay).evaluate_history(
                trade_dates=[step.trade_date for step in artifact.steps],
                observations=artifact.steps,
            )

            self.assertEqual(
                [step.trade_date for step in artifact.steps],
                [trade_dates[5], trade_dates[8], trade_dates[11]],
            )
            self.assertEqual(
                [step.input_states for step in artifact.steps],
                [
                    {
                        "benchmark_trend": "supportive",
                        "market_breadth": "supportive",
                        "dispersion": "supportive",
                        "realized_volatility": "supportive",
                        "price_limit_stress": "supportive",
                    },
                    {
                        "benchmark_trend": "risk_off",
                        "market_breadth": "risk_off",
                        "dispersion": "supportive",
                        "realized_volatility": "supportive",
                        "price_limit_stress": "supportive",
                    },
                    {
                        "benchmark_trend": "neutral",
                        "market_breadth": "neutral",
                        "dispersion": "risk_off",
                        "realized_volatility": "supportive",
                        "price_limit_stress": "risk_off",
                    },
                ],
            )
            self.assertEqual(
                [decision.state for decision in evidence.decisions],
                ["normal", "de_risk", "de_risk"],
            )
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["artifact_type"], "regime_overlay_observation_history")
            self.assertIn("metrics", payload["steps"][0])
            self.assertGreater(
                payload["steps"][0]["metrics"]["benchmark_long_return"],
                0.0,
            )
            self.assertEqual(
                payload["steps"][2]["metrics"]["price_limit_stress_share"],
                0.5,
            )

    def test_cli_build_regime_overlay_observations_writes_history(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            trade_dates = _trading_days(date(2026, 1, 5), 12)
            source_db_path = temp_root / "research_source.duckdb"
            benchmark_state_path = temp_root / "benchmark_state_history.json"
            sleeve_artifact_path = temp_root / "trend_artifact.json"
            output_path = temp_root / "overlay_observations.json"
            case_path = temp_root / "overlay_case.toml"

            self._create_source_db(source_db_path, trade_dates)
            self._write_benchmark_state(benchmark_state_path, trade_dates)
            self._write_sleeve_artifact(
                sleeve_artifact_path,
                decision_dates=[trade_dates[5], trade_dates[8], trade_dates[11]],
            )
            self._write_case(
                case_path=case_path,
                source_db_path=source_db_path,
                benchmark_state_path=benchmark_state_path,
                sleeve_artifact_path=sleeve_artifact_path,
                output_path=output_path,
            )

            result = subprocess.run(
                [
                    "python3",
                    "-m",
                    "alpha_find_v2",
                    "build-regime-overlay-observations",
                    "--case",
                    str(case_path),
                ],
                cwd=PROJECT_ROOT,
                env={"PYTHONPATH": "src"},
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["case_id"], "overlay_case")
            self.assertEqual(payload["overlay_id"], "a_share_risk_overlay")
            self.assertEqual(payload["trade_date_count"], 3)
            self.assertTrue(output_path.exists())

    def _create_source_db(self, path: Path, trade_dates: list[str]) -> None:
        conn = duckdb.connect(str(path))
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
                float_mcap_cny DOUBLE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE market_trade_calendar (
                trade_date VARCHAR
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
        conn.executemany(
            "INSERT INTO market_trade_calendar VALUES (?)",
            [(trade_date,) for trade_date in trade_dates],
        )

        securities = {
            "600001.SH": {
                "close_series": self._close_series([1.01] * 6 + [0.98] * 3 + [1.10] * 3),
                "limit_lock_dates": {trade_dates[11]},
            },
            "600002.SH": {
                "close_series": self._close_series([1.01] * 6 + [0.98] * 3 + [1.10] * 3),
                "limit_lock_dates": {trade_dates[11]},
            },
            "600003.SH": {
                "close_series": self._close_series([1.01] * 6 + [0.98] * 3 + [0.90] * 3),
                "limit_lock_dates": set(),
            },
            "600004.SH": {
                "close_series": self._close_series([1.01] * 6 + [0.98] * 3 + [0.90] * 3),
                "limit_lock_dates": set(),
            },
        }

        rows: list[tuple[object, ...]] = []
        for security_index, (security_id, config) in enumerate(sorted(securities.items())):
            previous_close = 100.0
            for row_index, trade_date in enumerate(trade_dates):
                close_price = config["close_series"][row_index]
                pre_close = previous_close
                is_limit_locked = trade_date in config["limit_lock_dates"]
                if is_limit_locked:
                    open_price = round(pre_close * 1.10, 2)
                    high_price = open_price
                    low_price = open_price
                    close_price = open_price
                else:
                    open_price = round(pre_close * 1.001, 2)
                    high_price = round(max(open_price, close_price) * 1.01, 2)
                    low_price = round(min(open_price, close_price) * 0.99, 2)
                rows.append(
                    (
                        security_id,
                        trade_date,
                        "SH",
                        "main_board",
                        False,
                        round(pre_close, 2),
                        round(open_price, 2),
                        round(high_price, 2),
                        round(low_price, 2),
                        round(close_price, 2),
                        round(close_price, 2),
                        200_000_000.0,
                        5_000_000_000.0 + (security_index * 100_000_000.0),
                    )
                )
                previous_close = close_price

        conn.executemany(
            """
            INSERT INTO daily_bar_pit VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.executemany(
            """
            INSERT INTO benchmark_weight_snapshot_pit VALUES (?, ?, ?, ?)
            """,
            [
                ("CSI 800", security_id, trade_date, 0.25)
                for trade_date in trade_dates
                for security_id in sorted(securities)
            ],
        )
        conn.close()

    def _write_benchmark_state(self, path: Path, trade_dates: list[str]) -> None:
        steps = []
        for trade_date in trade_dates:
            steps.append(
                {
                    "trade_date": trade_date,
                    "effective_at": f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}T15:00:00+08:00",
                    "available_at": f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}T15:30:00+08:00",
                    "industry_weights": {"tech": 0.5, "bank": 0.5},
                    "constituents": [
                        {"asset_id": "600001.SH", "weight": 0.25, "industry": "bank"},
                        {"asset_id": "600002.SH", "weight": 0.25, "industry": "bank"},
                        {"asset_id": "600003.SH", "weight": 0.25, "industry": "tech"},
                        {"asset_id": "600004.SH", "weight": 0.25, "industry": "tech"},
                    ],
                }
            )
        payload = {
            "schema_version": 1,
            "artifact_type": "benchmark_state_history",
            "benchmark_id": "CSI 800",
            "classification": "sw2021_l1",
            "weighting_method": "provider_weight",
            "steps": steps,
        }
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def _write_sleeve_artifact(self, path: Path, *, decision_dates: list[str]) -> None:
        payload = {
            "schema_version": 1,
            "artifact_type": "sleeve_research_artifact",
            "sleeve_id": "trend_leadership_core",
            "mandate_id": "a_share_long_only_eod",
            "target_id": "open_t1_to_open_t20_net_cost",
            "steps": [
                {
                    "trade_date": trade_date,
                    "records": [
                        {
                            "asset_id": "600001.SH",
                            "rank": 1,
                            "score": 1.0,
                            "target_weight": 1.0,
                            "realized_return": 0.01,
                            "trade_state": {"can_enter": True, "can_exit": True},
                        }
                    ],
                }
                for trade_date in decision_dates
            ],
        }
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def _write_case(
        self,
        *,
        case_path: Path,
        source_db_path: Path,
        benchmark_state_path: Path,
        sleeve_artifact_path: Path,
        output_path: Path,
    ) -> None:
        case_path.write_text(
            "\n".join(
                [
                    "schema_version = 1",
                    'artifact_type = "regime_overlay_observation_build_case"',
                    'case_id = "overlay_case"',
                    'description = "Build a reproducible regime overlay observation history."',
                    f'overlay_path = "{CONFIG_ROOT / "regime_overlays" / "a_share_risk_overlay.toml"}"',
                    f'source_db_path = "{source_db_path}"',
                    f'benchmark_state_path = "{benchmark_state_path}"',
                    f'trade_dates_artifact_path = "{sleeve_artifact_path}"',
                    f'output_path = "{output_path}"',
                    "trend_short_lookback_days = 2",
                    "trend_long_lookback_days = 3",
                    "breadth_return_lookback_days = 2",
                    "dispersion_return_lookback_days = 2",
                    "realized_volatility_lookback_days = 3",
                    "breadth_supportive_min = 0.75",
                    "breadth_risk_off_max = 0.25",
                    "dispersion_supportive_max = 0.02",
                    "dispersion_risk_off_min = 0.15",
                    "realized_volatility_supportive_max = 0.10",
                    "realized_volatility_risk_off_min = 0.40",
                    "price_limit_stress_supportive_max = 0.00",
                    "price_limit_stress_risk_off_min = 0.25",
                    "",
                ]
            ),
            encoding="utf-8",
        )

    def _close_series(self, multipliers: list[float]) -> list[float]:
        values: list[float] = []
        current = 100.0
        for multiplier in multipliers:
            current *= multiplier
            values.append(round(current, 2))
        return values


if __name__ == "__main__":
    unittest.main()
