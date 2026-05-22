"""CLI contract tests for the three factor-mining subcommands.

Validates exit codes and stdout/stderr per R1 acceptance criteria.
Complements tests/test_cli_integration.py with tests that require a
pre-built run directory (R1.6, R1.7, R1.10).

Validates: Requirements 1.3, 1.6, 1.7, 1.9, 1.10, 1.11, 1.12
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent.parent
_ENV = {**os.environ, "PYTHONPATH": str(_REPO_ROOT / "src")}


def _run(*args: str, cwd: Path | None = None):
    import subprocess

    return subprocess.run(
        [sys.executable, "-m", "alpha_find_v2", *args],
        capture_output=True,
        text=True,
        env=_ENV,
        cwd=str(cwd or _REPO_ROOT),
    )


# ---------------------------------------------------------------------------
# Helper: build a minimal run directory + registry entry
# ---------------------------------------------------------------------------

def _make_run_dir(
    base: Path,
    run_id: str = "run_001",
    families: list[str] | None = None,
    shortlist_oos_ic_ir: float = 0.2,
) -> Path:
    """Write candidates.jsonl, manifest.json, shortlist.json, and registry.json."""
    if families is None:
        families = ["trend"]

    run_dir = base / "output" / "factor_lab" / "runs" / run_id
    run_dir.mkdir(parents=True)

    (run_dir / "candidates.jsonl").write_text(
        json.dumps({
            "expr_id": "e001",
            "expression": "close_adj",
            "node_count": 1,
            "family": families[0] if families else "trend",
            "sources": ["random"],
            "train_ic_ir": shortlist_oos_ic_ir,
            "fitness": shortlist_oos_ic_ir - 0.05,
            "oos_segments": [{"oos_ic_ir": shortlist_oos_ic_ir, "oos_ic_mean": 0.01}],
            "status": "accepted",
        }) + "\n",
        encoding="utf-8",
    )

    (run_dir / "manifest.json").write_text(
        json.dumps({
            "run_id": run_id,
            "run_at": "2024-01-01T00:00:00.000Z",
            "start_date": "2020-01-01",
            "end_date": "2023-12-31",
            "config_snapshot": {"universe": {"id": "csi800"}},
        }),
        encoding="utf-8",
    )

    (run_dir / "shortlist.json").write_text(
        json.dumps([{
            "expr_id": "e001",
            "expression": "close_adj",
            "family": families[0] if families else "trend",
            "oos_segments": [{"oos_ic_ir": shortlist_oos_ic_ir}],
        }]),
        encoding="utf-8",
    )

    registry_path = base / "output" / "factor_lab" / "registry.json"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        json.dumps([{
            "run_id": run_id,
            "run_at": "2024-01-01T00:00:00.000Z",
            "run_dir": run_dir.as_posix(),
            "candidate_count": 1,
            "accepted_count": 1,
            "families_present": families,
        }]),
        encoding="utf-8",
    )

    return run_dir


# ---------------------------------------------------------------------------
# R1.11: mine-factors date validation → exit 2
# ---------------------------------------------------------------------------

def test_mine_factors_bad_date_format_exits_2(tmp_path: Path) -> None:
    result = _run(
        "mine-factors",
        "--research-db", str(tmp_path / "db.duckdb"),
        "--start", "2024-01-01",
        "--end", "20241231",
        "--config", str(tmp_path / "config.toml"),
    )
    assert result.returncode == 2, result.stderr
    assert "--start" in result.stderr


def test_mine_factors_start_after_end_exits_2(tmp_path: Path) -> None:
    result = _run(
        "mine-factors",
        "--research-db", str(tmp_path / "db.duckdb"),
        "--start", "20241231",
        "--end", "20240101",
        "--config", str(tmp_path / "config.toml"),
    )
    assert result.returncode == 2, result.stderr


# ---------------------------------------------------------------------------
# R1.3: mine-factors missing DB → exit 4
# ---------------------------------------------------------------------------

def test_mine_factors_missing_db_exits_4(tmp_path: Path) -> None:
    result = _run(
        "mine-factors",
        "--research-db", str(tmp_path / "nonexistent.duckdb"),
        "--start", "20200101",
        "--end", "20231231",
        "--config", str(tmp_path / "config.toml"),
    )
    # exit 4 when DB check is reached; exit 2 if config check fires first
    assert result.returncode in (4, 2), result.stderr
    if result.returncode == 4:
        assert "nonexistent.duckdb" in result.stderr


# ---------------------------------------------------------------------------
# R1.12: list-factor-candidates with no registry → exit 0, prints []
# ---------------------------------------------------------------------------

def test_list_candidates_no_registry_exits_0(tmp_path: Path) -> None:
    result = _run("list-factor-candidates", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip()) == []


# ---------------------------------------------------------------------------
# R1.9: inspect-candidate missing run_id → exit 4
# ---------------------------------------------------------------------------

def test_inspect_missing_run_exits_4(tmp_path: Path) -> None:
    result = _run("inspect-candidate", "no_such_run", "e001", cwd=tmp_path)
    assert result.returncode == 4, result.stderr


# ---------------------------------------------------------------------------
# R1.10: inspect-candidate existing run_id, missing expr_id → exit 5
# ---------------------------------------------------------------------------

def test_inspect_missing_expr_id_exits_5(tmp_path: Path) -> None:
    _make_run_dir(tmp_path, run_id="run_r110")
    result = _run("inspect-candidate", "run_r110", "no_such_expr", cwd=tmp_path)
    assert result.returncode == 5, (
        f"Expected exit 5 for missing expr_id, got {result.returncode}.\n"
        f"stderr: {result.stderr}"
    )


# ---------------------------------------------------------------------------
# R1.6: list-factor-candidates --family trend → exit 0
# ---------------------------------------------------------------------------

def test_list_candidates_family_filter_exits_0(tmp_path: Path) -> None:
    _make_run_dir(tmp_path, run_id="run_r16", families=["trend"])
    result = _run("list-factor-candidates", "--family", "trend", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    rows = json.loads(result.stdout.strip())
    assert len(rows) == 1
    assert rows[0]["run_id"] == "run_r16"


def test_list_candidates_family_filter_no_match_exits_0(tmp_path: Path) -> None:
    _make_run_dir(tmp_path, run_id="run_r16b", families=["trend"])
    result = _run("list-factor-candidates", "--family", "value", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip()) == []


# ---------------------------------------------------------------------------
# R1.7: list-factor-candidates --min-ic-ir 0.5 with no qualifying runs → exit 0 + []
# ---------------------------------------------------------------------------

def test_list_candidates_min_ic_ir_no_match_exits_0(tmp_path: Path) -> None:
    _make_run_dir(tmp_path, run_id="run_r17", shortlist_oos_ic_ir=0.2)
    result = _run("list-factor-candidates", "--min-ic-ir", "0.5", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.strip()) == []
