"""
CLI integration tests for the data-ingest subcommands: init, sync, audit-data.

Uses subprocess.run([sys.executable, "-m", "alpha_find_v2", ...]) so that the
actual CLI entry-point is exercised end-to-end.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

import duckdb
import pytest

# Repo root is two levels above this file
_REPO_ROOT = Path(__file__).parent.parent
_ENV = {**os.environ, "PYTHONPATH": str(_REPO_ROOT / "src")}


def _run(*args: str, cwd: Path | None = None) -> "subprocess.CompletedProcess[str]":
    import subprocess

    return subprocess.run(
        [sys.executable, "-m", "alpha_find_v2", *args],
        capture_output=True,
        text=True,
        env=_ENV,
        cwd=str(cwd or _REPO_ROOT),
    )


# ---------------------------------------------------------------------------
# Test 1: init creates .env, config/data_sources.toml, output/.gitkeep
# ---------------------------------------------------------------------------


def test_init_subcommand_creates_workspace_files(tmp_path: Path) -> None:
    result = _run("init", "--workspace", str(tmp_path))

    assert result.returncode == 0, (
        f"Expected exit 0, got {result.returncode}.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )

    # All three sentinel files must exist
    assert (tmp_path / ".env").is_file(), ".env was not created"
    assert (tmp_path / "config" / "data_sources.toml").is_file(), "config/data_sources.toml was not created"
    assert (tmp_path / "output" / ".gitkeep").is_file(), "output/.gitkeep was not created"

    # stdout must be valid JSON with "actions" list
    payload = json.loads(result.stdout)
    assert "workspace" in payload
    assert "actions" in payload
    assert len(payload["actions"]) == 3


# ---------------------------------------------------------------------------
# Test 2: sync --dry-run returns exit 0 and all results have status "skipped"
# ---------------------------------------------------------------------------


def test_sync_dry_run_exits_0_all_skipped(tmp_path: Path) -> None:
    # First initialise so that config/data_sources.toml exists
    init_result = _run("init", "--workspace", str(tmp_path))
    assert init_result.returncode == 0

    config_path = tmp_path / "config" / "data_sources.toml"
    raw_db_path = tmp_path / "raw.duckdb"

    result = _run(
        "sync",
        "--dry-run",
        "--raw-db", str(raw_db_path),
        "--config", str(config_path),
    )

    assert result.returncode == 0, (
        f"Expected exit 0 for dry-run, got {result.returncode}.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )

    payload = json.loads(result.stdout)
    assert "results" in payload, f"No 'results' key in output: {payload}"

    # Every result must have status == "skipped" in a dry-run
    statuses = [r["status"] for r in payload["results"]]
    non_skipped = [s for s in statuses if s != "skipped"]
    assert not non_skipped, (
        f"Expected all statuses 'skipped' in dry-run, got: {non_skipped}"
    )


# ---------------------------------------------------------------------------
# Test 3: audit-data with a blocking failure exits 1
# ---------------------------------------------------------------------------


def _make_db_with_pit_leak(path: Path) -> None:
    """Create a raw.duckdb that has a PIT-leak row (ann_date > end_date)."""
    from alpha_find_v2.data_ingest.schemas import RAW_TABLE_DDL

    conn = duckdb.connect(str(path))
    for ddl in RAW_TABLE_DDL.values():
        conn.execute(ddl)

    # Inject PIT leak: ann_date (20240401) > end_date (20240101) → blocking fail
    conn.execute(
        "INSERT INTO pit_fina_indicator "
        "(ts_code, ann_date, end_date, eps) VALUES "
        "('000001.SZ', '20240401', '20240101', 1.23)"
    )
    conn.close()


def test_audit_data_blocking_failure_exits_1(tmp_path: Path) -> None:
    db_path = tmp_path / "raw.duckdb"
    _make_db_with_pit_leak(db_path)

    result = _run(
        "audit-data",
        "--raw-db", str(db_path),
        "--out-dir", str(tmp_path / "audit"),
    )

    assert result.returncode == 1, (
        f"Expected exit 1 for blocking audit failure, got {result.returncode}.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )

    # Output must be valid JSON with overall_status == "blocking_failure"
    payload = json.loads(result.stdout)
    assert payload["overall_status"] == "blocking_failure"
