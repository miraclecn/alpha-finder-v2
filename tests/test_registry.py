"""Unit tests for factor_lab.registry.

Verification target: Task 16 (R1.5, R1.6, R1.7, R1.12, R7.6, R7.7)

Test plan
---------
1. append_run_entry creates the file when it doesn't exist
2. Two appends preserve insertion order (first entry remains first)
3. list_runs returns [] when file is missing
4. list_runs returns [] when file is an empty array
5. list_runs sorts by run_at descending
6. list_runs family filter is case-sensitive; combined with min_ic_ir → AND
7. list_runs min_ic_ir filter reads shortlist.json mean OOS IC_IR
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from alpha_find_v2.factor_lab.registry import append_run_entry, list_runs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _entry(
    run_id: str,
    run_at: str,
    *,
    families: list[str] | None = None,
    candidate_count: int = 5,
    accepted_count: int = 2,
) -> dict:
    return dict(
        run_id=run_id,
        run_at=run_at,
        families_present=families or [],
        candidate_count=candidate_count,
        accepted_count=accepted_count,
    )


def _write_entry(
    tmp_path: Path,
    run_id: str,
    run_at: str,
    *,
    families: list[str] | None = None,
    candidate_count: int = 5,
    accepted_count: int = 2,
) -> Path:
    reg = tmp_path / "registry.json"
    run_dir = tmp_path / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    append_run_entry(
        run_id=run_id,
        run_at=run_at,
        run_dir=run_dir,
        candidate_count=candidate_count,
        accepted_count=accepted_count,
        families_present=families or [],
        registry_path=reg,
    )
    return reg


def _write_shortlist(run_dir: Path, candidates: list[dict]) -> None:
    (run_dir / "shortlist.json").write_text(
        json.dumps(candidates, indent=2), encoding="utf-8"
    )


def _shortlist_candidate(oos_ic_ir_values: list[float]) -> dict:
    return {
        "expr_id": "x",
        "status": "accepted_oos",
        "oos_segments": [{"oos_ic_ir": v} for v in oos_ic_ir_values],
    }


# ---------------------------------------------------------------------------
# Test 1: creates file when it does not exist
# ---------------------------------------------------------------------------


def test_append_creates_file(tmp_path: Path) -> None:
    reg = tmp_path / "registry.json"
    assert not reg.exists()

    run_dir = tmp_path / "runs" / "run1"
    run_dir.mkdir(parents=True)
    append_run_entry(
        run_id="run1",
        run_at="2024-01-01T12:00:00.000Z",
        run_dir=run_dir,
        candidate_count=3,
        accepted_count=1,
        families_present=["trend"],
        registry_path=reg,
    )

    assert reg.exists()
    entries = json.loads(reg.read_text(encoding="utf-8"))
    assert len(entries) == 1
    assert entries[0]["run_id"] == "run1"


def test_append_run_dir_posix(tmp_path: Path) -> None:
    """run_dir must be stored with forward slashes regardless of OS (R7.10)."""
    reg = tmp_path / "registry.json"
    run_dir = tmp_path / "runs" / "run1"
    run_dir.mkdir(parents=True)
    append_run_entry(
        run_id="run1",
        run_at="2024-01-01T12:00:00.000Z",
        run_dir=run_dir,
        candidate_count=0,
        accepted_count=0,
        families_present=[],
        registry_path=reg,
    )
    entries = json.loads(reg.read_text(encoding="utf-8"))
    assert "\\" not in entries[0]["run_dir"]


# ---------------------------------------------------------------------------
# Test 2: two appends preserve insertion order
# ---------------------------------------------------------------------------


def test_two_appends_preserve_order(tmp_path: Path) -> None:
    _write_entry(tmp_path, "first", "2024-01-01T10:00:00.000Z")
    _write_entry(tmp_path, "second", "2024-06-01T10:00:00.000Z")

    reg = tmp_path / "registry.json"
    entries = json.loads(reg.read_text(encoding="utf-8"))
    assert len(entries) == 2
    assert entries[0]["run_id"] == "first"   # insertion order preserved
    assert entries[1]["run_id"] == "second"


def test_three_appends_preserve_order(tmp_path: Path) -> None:
    for i in range(1, 4):
        _write_entry(tmp_path, f"run{i}", f"2024-0{i}-01T00:00:00.000Z")

    reg = tmp_path / "registry.json"
    entries = json.loads(reg.read_text(encoding="utf-8"))
    assert [e["run_id"] for e in entries] == ["run1", "run2", "run3"]


# ---------------------------------------------------------------------------
# Test 3: list_runs returns [] when file missing
# ---------------------------------------------------------------------------


def test_list_runs_missing_file(tmp_path: Path) -> None:
    reg = tmp_path / "registry.json"
    result = list_runs(family=None, min_ic_ir=None, shortlist_dir_base=tmp_path, registry_path=reg)
    assert result == []


# ---------------------------------------------------------------------------
# Test 4: list_runs returns [] when file is empty array
# ---------------------------------------------------------------------------


def test_list_runs_empty_registry(tmp_path: Path) -> None:
    reg = tmp_path / "registry.json"
    reg.write_text("[]", encoding="utf-8")
    result = list_runs(family=None, min_ic_ir=None, shortlist_dir_base=tmp_path, registry_path=reg)
    assert result == []


# ---------------------------------------------------------------------------
# Test 5: list_runs sorts by run_at descending
# ---------------------------------------------------------------------------


def test_list_runs_sorted_desc(tmp_path: Path) -> None:
    _write_entry(tmp_path, "run_a", "2024-01-01T00:00:00.000Z")
    _write_entry(tmp_path, "run_b", "2024-03-01T00:00:00.000Z")
    _write_entry(tmp_path, "run_c", "2024-02-01T00:00:00.000Z")

    reg = tmp_path / "registry.json"
    result = list_runs(family=None, min_ic_ir=None, shortlist_dir_base=tmp_path, registry_path=reg)
    run_ids = [e["run_id"] for e in result]
    assert run_ids == ["run_b", "run_c", "run_a"]


# ---------------------------------------------------------------------------
# Test 6: list_runs family filter — case-sensitive; AND with min_ic_ir
# ---------------------------------------------------------------------------


def test_list_runs_family_filter_case_sensitive(tmp_path: Path) -> None:
    _write_entry(tmp_path, "r1", "2024-01-01T00:00:00.000Z", families=["trend"])
    _write_entry(tmp_path, "r2", "2024-02-01T00:00:00.000Z", families=["Trend"])  # different case
    _write_entry(tmp_path, "r3", "2024-03-01T00:00:00.000Z", families=["value"])

    reg = tmp_path / "registry.json"
    result = list_runs(family="trend", min_ic_ir=None, shortlist_dir_base=tmp_path, registry_path=reg)
    assert [e["run_id"] for e in result] == ["r1"]


def test_list_runs_family_filter_no_match(tmp_path: Path) -> None:
    _write_entry(tmp_path, "r1", "2024-01-01T00:00:00.000Z", families=["value"])

    reg = tmp_path / "registry.json"
    result = list_runs(family="trend", min_ic_ir=None, shortlist_dir_base=tmp_path, registry_path=reg)
    assert result == []


def test_list_runs_family_and_min_ic_ir_and_filter(tmp_path: Path) -> None:
    """family filter AND min_ic_ir filter: only runs passing both are returned."""
    reg = tmp_path / "registry.json"

    # r1: trend family, high IC_IR → should pass both filters
    run_dir1 = tmp_path / "runs" / "r1"
    run_dir1.mkdir(parents=True)
    _write_shortlist(run_dir1, [_shortlist_candidate([0.8, 0.9])])
    append_run_entry("r1", "2024-02-01T00:00:00.000Z", run_dir1, 3, 1, ["trend"], registry_path=reg)

    # r2: value family, high IC_IR → fails family filter
    run_dir2 = tmp_path / "runs" / "r2"
    run_dir2.mkdir(parents=True)
    _write_shortlist(run_dir2, [_shortlist_candidate([0.8, 0.9])])
    append_run_entry("r2", "2024-01-01T00:00:00.000Z", run_dir2, 3, 1, ["value"], registry_path=reg)

    # r3: trend family, low IC_IR → fails min_ic_ir filter
    run_dir3 = tmp_path / "runs" / "r3"
    run_dir3.mkdir(parents=True)
    _write_shortlist(run_dir3, [_shortlist_candidate([0.1, 0.2])])
    append_run_entry("r3", "2024-03-01T00:00:00.000Z", run_dir3, 3, 1, ["trend"], registry_path=reg)

    result = list_runs(family="trend", min_ic_ir=0.5, shortlist_dir_base=tmp_path, registry_path=reg)
    assert [e["run_id"] for e in result] == ["r1"]


# ---------------------------------------------------------------------------
# Test 7: list_runs min_ic_ir filter reads shortlist.json
# ---------------------------------------------------------------------------


def test_list_runs_min_ic_ir_passes(tmp_path: Path) -> None:
    reg = tmp_path / "registry.json"
    run_dir = tmp_path / "runs" / "r1"
    run_dir.mkdir(parents=True)
    # Mean IC_IR = (0.6 + 0.8) / 2 = 0.7
    _write_shortlist(run_dir, [_shortlist_candidate([0.6, 0.8])])
    append_run_entry("r1", "2024-01-01T00:00:00.000Z", run_dir, 2, 1, ["trend"], registry_path=reg)

    result = list_runs(family=None, min_ic_ir=0.5, shortlist_dir_base=tmp_path, registry_path=reg)
    assert len(result) == 1
    assert result[0]["run_id"] == "r1"


def test_list_runs_min_ic_ir_fails(tmp_path: Path) -> None:
    reg = tmp_path / "registry.json"
    run_dir = tmp_path / "runs" / "r1"
    run_dir.mkdir(parents=True)
    # Mean IC_IR = (0.1 + 0.2) / 2 = 0.15
    _write_shortlist(run_dir, [_shortlist_candidate([0.1, 0.2])])
    append_run_entry("r1", "2024-01-01T00:00:00.000Z", run_dir, 2, 1, ["trend"], registry_path=reg)

    result = list_runs(family=None, min_ic_ir=0.5, shortlist_dir_base=tmp_path, registry_path=reg)
    assert result == []


def test_list_runs_min_ic_ir_missing_shortlist_skipped(tmp_path: Path) -> None:
    """Run with no shortlist.json is skipped when min_ic_ir is specified."""
    reg = tmp_path / "registry.json"
    run_dir = tmp_path / "runs" / "r1"
    run_dir.mkdir(parents=True)
    # No shortlist.json written
    append_run_entry("r1", "2024-01-01T00:00:00.000Z", run_dir, 2, 1, ["trend"], registry_path=reg)

    result = list_runs(family=None, min_ic_ir=0.5, shortlist_dir_base=tmp_path, registry_path=reg)
    assert result == []


def test_list_runs_min_ic_ir_any_candidate_qualifies(tmp_path: Path) -> None:
    """A run qualifies if ANY candidate's mean OOS IC_IR >= threshold."""
    reg = tmp_path / "registry.json"
    run_dir = tmp_path / "runs" / "r1"
    run_dir.mkdir(parents=True)
    shortlist = [
        _shortlist_candidate([0.1, 0.2]),   # mean 0.15 — does not qualify
        _shortlist_candidate([0.7, 0.9]),   # mean 0.80 — qualifies
    ]
    _write_shortlist(run_dir, shortlist)
    append_run_entry("r1", "2024-01-01T00:00:00.000Z", run_dir, 4, 2, ["trend"], registry_path=reg)

    result = list_runs(family=None, min_ic_ir=0.5, shortlist_dir_base=tmp_path, registry_path=reg)
    assert len(result) == 1
