"""
Tests for data_ingest Waves 4-6:
  - orchestrator.py (sync state I/O, dispatcher, incremental, fallback)
  - init_workspace.py
  - audit.py
  - cli.py new commands (smoke tests)
"""
from __future__ import annotations

import json
import os
import sys
import subprocess
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

import duckdb

from alpha_find_v2.data_ingest.schemas import RAW_TABLE_DDL, META_DDL, DATASET_TABLE_NAME
from alpha_find_v2.data_ingest.orchestrator import (
    DatasetSyncState,
    _ensure_meta_schema,
    _load_state,
    _record_state,
    _effective_since,
    sync,
    SyncReport,
)
from alpha_find_v2.data_ingest.init_workspace import init_workspace
from alpha_find_v2.data_ingest.audit import run_audit, AuditReport
from alpha_find_v2.data_ingest.adapters.base import (
    AdapterPermissionError,
    AdapterUnavailable,
    DataSourceAdapter,
)


# ---------------------------------------------------------------------------
# Helpers: minimal in-memory raw.duckdb
# ---------------------------------------------------------------------------


def _make_raw_db(path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(path))
    for ddl in RAW_TABLE_DDL.values():
        conn.execute(ddl)
    conn.execute(META_DDL["meta_schema"])
    conn.execute(META_DDL["dataset_sync_state"])
    return conn


# ---------------------------------------------------------------------------
# Fake adapters
# ---------------------------------------------------------------------------


class FakeAdapter:
    """Streams a fixed list of rows for a single dataset_id."""

    def __init__(self, name: str, rows: dict[str, list[dict[str, Any]]]) -> None:
        self.name = name
        self._rows = rows
        self.calls: list[tuple] = []

    def supports(self, dataset_id: str) -> bool:
        return dataset_id in self._rows

    def fetch(
        self,
        dataset_id: str,
        *,
        since: str | None,
        until: str | None,
        full: bool,
    ) -> Iterator[dict[str, Any]]:
        self.calls.append((dataset_id, since, until, full))
        for row in self._rows.get(dataset_id, []):
            yield row


class PermissionDeniedAdapter:
    """Always raises AdapterPermissionError."""

    name = "perm_denied"

    def supports(self, dataset_id: str) -> bool:
        return True

    def fetch(self, dataset_id: str, **kwargs: Any) -> Iterator[dict[str, Any]]:
        raise AdapterPermissionError(f"Permission denied for {dataset_id}")


class MidStreamFailAdapter:
    """Fails after yielding `fail_after` rows."""

    name = "midstream_fail"

    def __init__(self, rows: list[dict[str, Any]], fail_after: int = 2) -> None:
        self._rows = rows
        self._fail_after = fail_after

    def supports(self, dataset_id: str) -> bool:
        return True

    def fetch(self, dataset_id: str, **kwargs: Any) -> Iterator[dict[str, Any]]:
        for i, row in enumerate(self._rows):
            if i >= self._fail_after:
                raise RuntimeError("Simulated mid-stream failure")
            yield row


# ---------------------------------------------------------------------------
# Helper: minimal DataSourcesConfig
# ---------------------------------------------------------------------------


def _minimal_config(
    *,
    dataset_id: str = "trade_cal",
    adapter_names: list[str] | None = None,
    enabled: bool = True,
) -> Any:
    """Build a minimal DataSourcesConfig for testing."""
    from alpha_find_v2.data_ingest.config_models import (
        AdapterConfig,
        DatasetConfig,
        DataSourcesConfig,
    )
    from alpha_find_v2.data_ingest.schemas import DATASET_INCREMENTAL_AXIS

    names = adapter_names or ["fake"]
    adapters = {n: AdapterConfig(name=n, enabled=True, calls_per_minute=1000, calls_per_day=0) for n in names}
    credit_tier = 120
    datasets = {
        dataset_id: DatasetConfig(
            dataset_id=dataset_id,
            enabled=enabled,
            credit_tier=credit_tier,
            priority=tuple(names),
        )
    }
    return DataSourcesConfig(schema_version=1, adapters=adapters, datasets=datasets)


# ---------------------------------------------------------------------------
# Tests: sync state I/O
# ---------------------------------------------------------------------------


class SyncStateIOTest(unittest.TestCase):
    def test_write_and_read_back_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "raw.duckdb"
            conn = _make_raw_db(db)
            now = datetime.now(UTC)
            state = DatasetSyncState(
                dataset_id="daily",
                adapter="tushare",
                last_trade_date="20240105",
                last_period_end=None,
                last_run_at=now,
                last_status="success",
                last_row_count=42,
                error_message=None,
            )
            _record_state(conn, state)
            loaded = _load_state(conn)
            conn.close()

            self.assertIn("daily", loaded)
            s = loaded["daily"]
            self.assertEqual(s.dataset_id, "daily")
            self.assertEqual(s.adapter, "tushare")
            self.assertEqual(s.last_trade_date, "20240105")
            self.assertIsNone(s.last_period_end)
            self.assertEqual(s.last_status, "success")
            self.assertEqual(s.last_row_count, 42)

    def test_upsert_replaces_existing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "raw.duckdb"
            conn = _make_raw_db(db)
            state = DatasetSyncState(
                dataset_id="daily",
                adapter="tushare",
                last_trade_date="20240101",
                last_period_end=None,
                last_run_at=datetime.now(UTC),
                last_status="success",
                last_row_count=10,
                error_message=None,
            )
            _record_state(conn, state)
            # Update
            state2 = DatasetSyncState(
                dataset_id="daily",
                adapter="akshare",
                last_trade_date="20240110",
                last_period_end=None,
                last_run_at=datetime.now(UTC),
                last_status="success",
                last_row_count=20,
                error_message=None,
            )
            _record_state(conn, state2)
            loaded = _load_state(conn)
            conn.close()

            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded["daily"].last_trade_date, "20240110")
            self.assertEqual(loaded["daily"].adapter, "akshare")


# ---------------------------------------------------------------------------
# Tests: incremental since logic
# ---------------------------------------------------------------------------


class EffectiveSinceTest(unittest.TestCase):
    def _make_state(self, last_trade_date: str | None = None, last_period_end: str | None = None) -> DatasetSyncState:
        return DatasetSyncState(
            dataset_id="daily",
            adapter="tushare",
            last_trade_date=last_trade_date,
            last_period_end=last_period_end,
            last_run_at=datetime.now(UTC),
            last_status="success",
            last_row_count=0,
            error_message=None,
        )

    def test_static_dataset_always_returns_none(self) -> None:
        state = self._make_state("20240105")
        result = _effective_since("stock_basic", state, None)
        self.assertIsNone(result)

    def test_no_state_returns_cli_since(self) -> None:
        result = _effective_since("daily", None, "20230101")
        self.assertEqual(result, "20230101")

    def test_incremental_advances_by_one_day(self) -> None:
        state = self._make_state("20240105")
        result = _effective_since("daily", state, None)
        self.assertEqual(result, "20240106")

    def test_incremental_returns_later_of_state_and_cli(self) -> None:
        state = self._make_state("20240105")
        # cli_since is later
        result = _effective_since("daily", state, "20240110")
        self.assertEqual(result, "20240110")
        # cli_since is earlier
        result2 = _effective_since("daily", state, "20240101")
        self.assertEqual(result2, "20240106")


# ---------------------------------------------------------------------------
# Tests: dispatcher + retry + batch write
# ---------------------------------------------------------------------------


class DispatcherTest(unittest.TestCase):
    def _trade_cal_rows(self, count: int = 5) -> list[dict]:
        return [
            {
                "exchange": "SSE",
                "cal_date": f"202401{i+1:02d}",
                "is_open": 1,
                "pretrade_date": f"202401{i:02d}",
                "ingested_at": datetime.now(UTC),
                "source_table": "fake.trade_cal",
            }
            for i in range(count)
        ]

    def test_sync_writes_rows_and_updates_state(self) -> None:
        rows = self._trade_cal_rows(5)
        adapter = FakeAdapter("fake", {"trade_cal": rows})
        config = _minimal_config(dataset_id="trade_cal", adapter_names=["fake"])

        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "raw.duckdb"
            report = sync(
                raw_db_path=db_path,
                config=config,
                adapter_map={"fake": adapter},
            )

        self.assertEqual(len(report.results), 1)
        r = report.results[0]
        self.assertEqual(r.dataset_id, "trade_cal")
        self.assertEqual(r.status, "success")
        self.assertEqual(r.rows_added, 5)

    def test_permission_error_marks_permission_denied_not_retried(self) -> None:
        config = _minimal_config(dataset_id="trade_cal", adapter_names=["perm"])

        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "raw.duckdb"
            report = sync(
                raw_db_path=db_path,
                config=config,
                adapter_map={"perm": PermissionDeniedAdapter()},
                max_retries=3,
                retry_base_delay=0.001,
            )

        r = report.results[0]
        self.assertEqual(r.status, "permission_denied")
        self.assertEqual(r.rows_added, 0)

    def test_midstream_failure_rolls_back(self) -> None:
        rows = self._trade_cal_rows(10)
        bad_adapter = MidStreamFailAdapter(rows, fail_after=3)
        config = _minimal_config(dataset_id="trade_cal", adapter_names=["bad"])

        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "raw.duckdb"
            report = sync(
                raw_db_path=db_path,
                config=config,
                adapter_map={"bad": bad_adapter},
                max_retries=1,
                retry_base_delay=0.001,
            )

            r = report.results[0]
            self.assertEqual(r.status, "failed")
            self.assertEqual(r.rows_added, 0)

            # Verify raw table is empty (rollback worked)
            conn = duckdb.connect(str(db_path))
            count = conn.execute("SELECT COUNT(*) FROM raw_trade_cal").fetchone()[0]
            conn.close()
            self.assertEqual(count, 0)

    def test_second_sync_is_incremental(self) -> None:
        rows_first = self._trade_cal_rows(5)
        rows_second = [
            {
                "exchange": "SSE",
                "cal_date": "20240110",
                "is_open": 1,
                "pretrade_date": "20240109",
                "ingested_at": datetime.now(UTC),
                "source_table": "fake.trade_cal",
            }
        ]

        adapter = FakeAdapter("fake", {"trade_cal": rows_first})
        config = _minimal_config(dataset_id="trade_cal", adapter_names=["fake"])

        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "raw.duckdb"
            sync(raw_db_path=db_path, config=config, adapter_map={"fake": adapter})

            # Second sync: adapter serves only new rows
            adapter2 = FakeAdapter("fake", {"trade_cal": rows_second})
            report2 = sync(
                raw_db_path=db_path, config=config, adapter_map={"fake": adapter2}
            )

            r = report2.results[0]
            self.assertEqual(r.status, "success")
            self.assertEqual(r.rows_added, 1)

            # Total rows should be 6
            conn = duckdb.connect(str(db_path))
            total = conn.execute("SELECT COUNT(*) FROM raw_trade_cal").fetchone()[0]
            conn.close()
            self.assertEqual(total, 6)

    def test_dry_run_makes_no_api_calls(self) -> None:
        adapter = FakeAdapter("fake", {"trade_cal": [{"x": 1}]})
        config = _minimal_config(dataset_id="trade_cal", adapter_names=["fake"])

        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "raw.duckdb"
            report = sync(
                raw_db_path=db_path,
                config=config,
                adapter_map={"fake": adapter},
                dry_run=True,
            )

        self.assertEqual(len(adapter.calls), 0)
        self.assertEqual(report.results[0].status, "skipped")


# ---------------------------------------------------------------------------
# Tests: fallback chain
# ---------------------------------------------------------------------------


class FallbackChainTest(unittest.TestCase):
    def test_fallback_to_second_adapter_on_permission_error(self) -> None:
        from alpha_find_v2.data_ingest.config_models import (
            AdapterConfig, DatasetConfig, DataSourcesConfig
        )

        rows = [
            {
                "exchange": "SSE",
                "cal_date": "20240101",
                "is_open": 1,
                "pretrade_date": "20231229",
                "ingested_at": datetime.now(UTC),
                "source_table": "akshare.trade_cal",
            }
        ]
        fallback_adapter = FakeAdapter("akshare", {"trade_cal": rows})

        config = DataSourcesConfig(
            schema_version=1,
            adapters={
                "tushare": AdapterConfig("tushare", True, 490, 0),
                "akshare": AdapterConfig("akshare", True, 60, 0),
            },
            datasets={
                "trade_cal": DatasetConfig("trade_cal", True, 120, ("tushare", "akshare"))
            },
        )

        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "raw.duckdb"
            report = sync(
                raw_db_path=db_path,
                config=config,
                adapter_map={
                    "tushare": PermissionDeniedAdapter(),
                    "akshare": fallback_adapter,
                },
            )

        r = report.results[0]
        self.assertEqual(r.status, "success")
        self.assertEqual(r.adapter, "akshare")


# ---------------------------------------------------------------------------
# Tests: init_workspace
# ---------------------------------------------------------------------------


class InitWorkspaceTest(unittest.TestCase):
    def test_creates_three_files_on_first_run(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ws = Path(td)
            report = init_workspace(ws)
            created = [a.action for a in report.actions]
            self.assertEqual(created.count("created"), 3)
            self.assertTrue((ws / ".env").exists())
            self.assertTrue((ws / "config" / "data_sources.toml").exists())
            self.assertTrue((ws / "output" / ".gitkeep").exists())

    def test_second_run_skips_all(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ws = Path(td)
            init_workspace(ws)
            report2 = init_workspace(ws)
            for action in report2.actions:
                self.assertEqual(action.action, "skipped")

    def test_generated_config_parses(self) -> None:
        from alpha_find_v2.data_ingest.config_models import load_data_sources_config
        with tempfile.TemporaryDirectory() as td:
            ws = Path(td)
            init_workspace(ws)
            config = load_data_sources_config(ws / "config" / "data_sources.toml")
            self.assertEqual(config.schema_version, 1)
            self.assertFalse(config.datasets["fina_indicator"].enabled)


# ---------------------------------------------------------------------------
# Tests: audit
# ---------------------------------------------------------------------------


class AuditTest(unittest.TestCase):
    def _make_minimal_db(self, path: Path) -> None:
        """Write a tiny raw.duckdb fixture with plausible data."""
        conn = duckdb.connect(str(path))
        for ddl in RAW_TABLE_DDL.values():
            conn.execute(ddl)
        # stock_basic_ref
        conn.execute(
            "INSERT INTO stock_basic_ref VALUES "
            "('000001.SZ','000001','平安银行','深圳','银行','19910403',NULL,'N',current_timestamp,'tushare')"
        )
        # raw_kline_unadj
        conn.execute(
            "INSERT INTO raw_kline_unadj VALUES "
            "('000001.SZ','20240102',10.0,10.5,9.9,10.2,10.0,0.2,2.0,100.0,200.0,'tushare.daily',current_timestamp)"
        )
        # raw_kline_qfq
        conn.execute(
            "INSERT INTO raw_kline_qfq VALUES "
            "('000001.SZ','20240102',15.0,15.75,14.85,15.3,15.0,0.3,2.0,100.0,200.0,'tushare',current_timestamp)"
        )
        # raw_adj_factor: 15/10 = 1.5
        conn.execute(
            "INSERT INTO raw_adj_factor VALUES "
            "('000001.SZ','20240102',1.5,'tushare.adj_factor',current_timestamp)"
        )
        # raw_trade_cal
        conn.execute(
            "INSERT INTO raw_trade_cal VALUES ('SSE','20240102',1,'20231229',current_timestamp,'tushare')"
        )
        conn.close()

    def test_audit_passes_on_clean_data(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "raw.duckdb"
            self._make_minimal_db(db)
            out = Path(td) / "audit"
            report = run_audit(raw_db_path=db, out_dir=out)

            self.assertEqual(report.overall_status, "ok")
            self.assertFalse(report.blocking_failures())
            # Output files created — check inside tempdir context
            audit_dirs = list(out.iterdir())
            self.assertEqual(len(audit_dirs), 1)
            self.assertTrue((audit_dirs[0] / "audit.json").exists())
            self.assertTrue((audit_dirs[0] / "audit.md").exists())

    def test_audit_json_has_correct_shape(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "raw.duckdb"
            self._make_minimal_db(db)
            out = Path(td) / "audit"
            run_audit(raw_db_path=db, out_dir=out)
            audit_dirs = list(out.iterdir())
            payload = json.loads((audit_dirs[0] / "audit.json").read_text(encoding="utf-8"))

        self.assertIn("overall_status", payload)
        self.assertIn("outcomes", payload)
        check_ids = {o["check_id"] for o in payload["outcomes"]}
        self.assertIn("pit_leak_sample", check_ids)
        self.assertIn("adj_factor_consistency", check_ids)

    def test_audit_md_has_table_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "raw.duckdb"
            self._make_minimal_db(db)
            out = Path(td) / "audit"
            run_audit(raw_db_path=db, out_dir=out)
            audit_dirs = list(out.iterdir())
            md_content = (audit_dirs[0] / "audit.md").read_text(encoding="utf-8")

        self.assertIn("| ID |", md_content)
        self.assertIn("pit_leak_sample", md_content)


# ---------------------------------------------------------------------------
# Tests: CLI smoke tests
# ---------------------------------------------------------------------------


class CLISmokeTest(unittest.TestCase):
    def _run(self, *args: str, cwd: str | None = None) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "alpha_find_v2", *args],
            cwd=cwd or os.getcwd(),
            env={**os.environ, "PYTHONPATH": "src"},
            capture_output=True,
            text=True,
            check=False,
        )

    def test_init_exits_0_and_creates_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            result = self._run("init", "--workspace", td, cwd=str(Path(__file__).parents[1]))
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(result.stdout)
            self.assertIn("actions", payload)
            self.assertTrue((Path(td) / ".env").exists())
            self.assertTrue((Path(td) / "config" / "data_sources.toml").exists())

    def test_sync_dry_run_exits_0_without_api_calls(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            # First init
            ws = Path(td)
            init_workspace(ws)
            result = self._run(
                "sync",
                "--raw-db", str(ws / "output" / "raw.duckdb"),
                "--config", str(ws / "config" / "data_sources.toml"),
                "--dry-run",
                cwd=str(Path(__file__).parents[1]),
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(result.stdout)
            self.assertIn("results", payload)
            # All should be skipped
            for r in payload["results"]:
                self.assertEqual(r["status"], "skipped")

    def test_audit_data_exits_0_on_clean_db(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "raw.duckdb"
            # Tiny valid DB
            conn = duckdb.connect(str(db))
            for ddl in RAW_TABLE_DDL.values():
                conn.execute(ddl)
            conn.execute(
                "INSERT INTO raw_kline_unadj VALUES "
                "('000001.SZ','20240102',10.0,10.5,9.9,10.2,10.0,0.2,2.0,100.0,200.0,'tushare',current_timestamp)"
            )
            conn.execute(
                "INSERT INTO raw_kline_qfq VALUES "
                "('000001.SZ','20240102',15.0,15.75,14.85,15.3,15.0,0.3,2.0,100.0,200.0,'tushare',current_timestamp)"
            )
            conn.execute(
                "INSERT INTO raw_adj_factor VALUES "
                "('000001.SZ','20240102',1.5,'tushare',current_timestamp)"
            )
            conn.execute(
                "INSERT INTO raw_trade_cal VALUES ('SSE','20240102',1,'20231229',current_timestamp,'tushare')"
            )
            conn.close()

            result = self._run(
                "audit-data",
                "--raw-db", str(db),
                "--out-dir", str(Path(td) / "audit"),
                cwd=str(Path(__file__).parents[1]),
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["overall_status"], "ok")


if __name__ == "__main__":
    unittest.main()
