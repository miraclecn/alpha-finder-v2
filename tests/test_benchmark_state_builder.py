from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest

import duckdb


def _create_research_source_db(path: Path) -> None:
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
        CREATE TABLE daily_bar_pit (
            security_id VARCHAR,
            trade_date VARCHAR,
            float_mcap_cny DOUBLE
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
        INSERT INTO market_trade_calendar VALUES
        ('20240102'),
        ('20240103')
        """
    )
    conn.execute(
        """
        INSERT INTO daily_bar_pit VALUES
        ('000001.SZ', '20240102', 700.0),
        ('300001.SZ', '20240102', 300.0),
        ('000001.SZ', '20240103', 600.0),
        ('688001.SH', '20240103', 400.0)
        """
    )
    conn.execute(
        """
        INSERT INTO benchmark_membership_pit VALUES
        ('CSI 800', '000001.SZ', '20200101', NULL),
        ('CSI 800', '300001.SZ', '20200101', '20240103'),
        ('CSI 800', '688001.SH', '20240103', NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO industry_classification_pit VALUES
        ('000001.SZ', 'citics_l1', 'bank', '20200101', NULL),
        ('300001.SZ', 'citics_l1', 'tech', '20200101', NULL),
        ('688001.SH', 'citics_l1', 'industrial', '20240103', NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO benchmark_weight_snapshot_pit VALUES
        ('CSI 800', '000001.SZ', '20240102', 70.0),
        ('CSI 800', '300001.SZ', '20240102', 30.0),
        ('CSI 800', '000001.SZ', '20240103', 55.0),
        ('CSI 800', '688001.SH', '20240103', 45.0)
        """
    )
    conn.close()


class BenchmarkStateBuilderTest(unittest.TestCase):
    def test_builder_materializes_benchmark_state_history_from_pit_tables(self) -> None:
        from alpha_find_v2.benchmark_state_builder import (
            build_benchmark_state_artifact,
            load_benchmark_state_build_case,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            output_path = temp_root / "benchmark_state_history.json"
            case_path = temp_root / "benchmark_state.toml"
            _create_research_source_db(source_db)
            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "benchmark_state_build_case"',
                        'case_id = "csi800_proxy_weights"',
                        'description = "Build benchmark state from PIT membership and industry tables."',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{output_path}"',
                        'benchmark_id = "CSI 800"',
                        'industry_schema = "citics_l1"',
                        'start_date = "20240102"',
                        'end_date = "20240103"',
                        'weighting_method = "float_mcap_proxy"',
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_benchmark_state_build_case(case_path)
            artifact = build_benchmark_state_artifact(loaded_case)

            self.assertEqual(artifact.benchmark_id, "CSI 800")
            self.assertEqual(artifact.classification, "citics_l1")
            self.assertEqual(artifact.weighting_method, "float_mcap_proxy")
            self.assertEqual([step.trade_date for step in artifact.steps], ["20240102", "20240103"])
            self.assertEqual(
                artifact.steps[0].industry_weights,
                {"bank": 0.7, "tech": 0.3},
            )
            self.assertEqual(
                [
                    (item.asset_id, round(item.weight, 4), item.industry)
                    for item in artifact.steps[1].constituents
                ],
                [
                    ("000001.SZ", 0.6, "bank"),
                    ("688001.SH", 0.4, "industrial"),
                ],
            )

    def test_builder_rejects_missing_industry_for_active_member(self) -> None:
        from alpha_find_v2.benchmark_state_builder import (
            build_benchmark_state_artifact,
            load_benchmark_state_build_case,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "benchmark_state.toml"
            _create_research_source_db(source_db)
            conn = duckdb.connect(str(source_db))
            conn.execute(
                """
                DELETE FROM industry_classification_pit
                WHERE security_id = '688001.SH'
                """
            )
            conn.close()
            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "benchmark_state_build_case"',
                        'case_id = "csi800_missing_industry"',
                        'description = "Reject missing PIT industry on active member."',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "benchmark_state_history.json"}"',
                        'benchmark_id = "CSI 800"',
                        'industry_schema = "citics_l1"',
                        'start_date = "20240103"',
                        'end_date = "20240103"',
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_benchmark_state_build_case(case_path)
            with self.assertRaisesRegex(
                ValueError,
                "Missing PIT industry classification for benchmark member: 688001\\.SH on 20240103",
            ):
                build_benchmark_state_artifact(loaded_case)

    def test_builder_supports_provider_weight_snapshots(self) -> None:
        from alpha_find_v2.benchmark_state_builder import (
            build_benchmark_state_artifact,
            load_benchmark_state_build_case,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            case_path = temp_root / "benchmark_state.toml"
            _create_research_source_db(source_db)

            conn = duckdb.connect(str(source_db))
            conn.execute("INSERT INTO market_trade_calendar VALUES ('20240104')")
            conn.close()

            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "benchmark_state_build_case"',
                        'case_id = "csi800_provider_weight"',
                        'description = "Build benchmark state from provider snapshots."',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{temp_root / "benchmark_state_history.json"}"',
                        'benchmark_id = "CSI 800"',
                        'industry_schema = "citics_l1"',
                        'start_date = "20240102"',
                        'end_date = "20240104"',
                        'weighting_method = "provider_weight"',
                    ]
                ),
                encoding="utf-8",
            )

            loaded_case = load_benchmark_state_build_case(case_path)
            artifact = build_benchmark_state_artifact(loaded_case)

            self.assertEqual(artifact.weighting_method, "provider_weight")
            self.assertEqual(
                [step.trade_date for step in artifact.steps],
                ["20240102", "20240103", "20240104"],
            )
            self.assertEqual(
                [
                    (item.asset_id, round(item.weight, 4), item.industry)
                    for item in artifact.steps[0].constituents
                ],
                [
                    ("000001.SZ", 0.7, "bank"),
                    ("300001.SZ", 0.3, "tech"),
                ],
            )
            self.assertEqual(
                artifact.steps[1].industry_weights,
                {"bank": 0.55, "industrial": 0.45},
            )
            self.assertEqual(
                [
                    (item.asset_id, round(item.weight, 4), item.industry)
                    for item in artifact.steps[2].constituents
                ],
                [
                    ("000001.SZ", 0.55, "bank"),
                    ("688001.SH", 0.45, "industrial"),
                ],
            )

    def test_cli_build_benchmark_state_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            source_db = temp_root / "research_source.duckdb"
            output_path = temp_root / "benchmark_state_history.json"
            case_path = temp_root / "benchmark_state.toml"
            _create_research_source_db(source_db)
            case_path.write_text(
                "\n".join(
                    [
                        'schema_version = 1',
                        'artifact_type = "benchmark_state_build_case"',
                        'case_id = "csi800_cli_build"',
                        'description = "CLI build of benchmark state history."',
                        f'source_db_path = "{source_db}"',
                        f'output_path = "{output_path}"',
                        'benchmark_id = "CSI 800"',
                        'industry_schema = "citics_l1"',
                        'start_date = "20240102"',
                        'end_date = "20240103"',
                    ]
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    "python3",
                    "-m",
                    "alpha_find_v2",
                    "build-benchmark-state",
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
            self.assertEqual(payload["artifact_type"], "benchmark_state_history")
            self.assertEqual(payload["weighting_method"], "float_mcap_proxy")


if __name__ == "__main__":
    unittest.main()
