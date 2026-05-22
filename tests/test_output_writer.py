"""Unit tests for factor_lab.output writers.

Verification target: Task 15 (R7.1–R7.5, R7.7, R7.10)

Covers:
1. manifest.json has all required keys
2. candidates.jsonl has correct count and required fields
3. shortlist.json only contains accepted candidates, ordered correctly, with family_rank
4. correlation_matrix.csv has correct header row and cell formats
5. audit.md contains Promotion Path section
6. All path strings in JSON artifacts use forward slashes
7. Round-trip: parse manifest back and values match
"""

from __future__ import annotations

import csv
import json
import os
import uuid
from pathlib import Path

import pytest

from alpha_find_v2.factor_lab.output import (
    write_audit_md,
    write_candidates_jsonl,
    write_correlation_matrix,
    write_manifest,
    write_shortlist,
)
from alpha_find_v2.factor_lab.search.beam import Candidate
from alpha_find_v2.factor_lab.dsl.grammar import Leaf


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EXPR_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")


def _cid(s: str) -> str:
    return uuid.uuid5(_EXPR_NAMESPACE, s).hex


def _make_candidate(
    canonical: str,
    *,
    status: str = "accepted_oos",
    fitness: float | None = 0.5,
    train_ic_ir: float | None = 0.5,
    family: str | None = "trend",
    node_count: int = 1,
    oos_segments: list[dict] | None = None,
) -> Candidate:
    return Candidate(
        expr_id=_cid(canonical),
        canonical=canonical,
        ast=Leaf(field="close_adj"),
        node_count=node_count,
        family=family,
        sources=["beam"],
        train_ic_ir=train_ic_ir,
        fitness=fitness,
        status=status,
        oos_segments=oos_segments or [
            {
                "segment": 1,
                "train_start": "20210101",
                "train_end": "20211231",
                "oos_start": "20220101",
                "oos_end": "20220630",
                "oos_ic_ir": 0.45,
                "oos_ic_mean": 0.03,
                "oos_coverage": 0.95,
            }
        ],
    )


def _make_manifest(
    run_id: str = "run_abc",
    accepted_count: int = 1,
    total_candidates_evaluated: int = 3,
) -> dict:
    return {
        "run_id": run_id,
        "run_at": "2024-01-01T12:00:00.000Z",
        "seed": 42,
        "git_sha": "a" * 40,
        "config_snapshot": {"search": {"beam_width": 20}},
        "start_date": "2021-01-01",
        "end_date": "2023-12-31",
        "walk_forward_segments": 3,
        "universe_id": "investable_a_share_core",
        "random_sample_size": 0,
        "total_candidates_evaluated": total_candidates_evaluated,
        "accepted_count": accepted_count,
        "rejected_oos_count": 1,
        "rejected_correlation_count": 1,
        "rejected_quota_count": 0,
        "duration_seconds": 12.345,
    }


# ---------------------------------------------------------------------------
# Test 1: manifest.json required keys
# ---------------------------------------------------------------------------


REQUIRED_MANIFEST_KEYS = [
    "run_id", "run_at", "seed", "git_sha", "config_snapshot",
    "start_date", "end_date", "walk_forward_segments", "universe_id",
    "random_sample_size", "total_candidates_evaluated", "accepted_count",
    "rejected_oos_count", "rejected_correlation_count", "rejected_quota_count",
    "duration_seconds",
]


def test_manifest_has_all_required_keys(tmp_path: Path) -> None:
    meta = _make_manifest()
    write_manifest(tmp_path, meta)
    data = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    for key in REQUIRED_MANIFEST_KEYS:
        assert key in data, f"Missing key: {key}"


def test_manifest_keys_present_when_zero(tmp_path: Path) -> None:
    """Even zero/empty values must be present (R7.1)."""
    meta = _make_manifest(accepted_count=0, total_candidates_evaluated=0)
    write_manifest(tmp_path, meta)
    data = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert data["accepted_count"] == 0
    assert data["total_candidates_evaluated"] == 0


# ---------------------------------------------------------------------------
# Test 2: candidates.jsonl count and required fields
# ---------------------------------------------------------------------------

REQUIRED_CANDIDATE_KEYS = [
    "expr_id", "expression", "node_count", "family", "sources",
    "train_ic_ir", "fitness", "oos_segments", "status",
]


def test_candidates_jsonl_count(tmp_path: Path) -> None:
    candidates = [
        _make_candidate("close_adj"),
        _make_candidate("rolling_mean_close_adj", status="rejected_oos"),
        _make_candidate("cs_demean_close_adj", status="rejected_correlation"),
    ]
    write_candidates_jsonl(tmp_path, candidates)
    lines = (tmp_path / "candidates.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3


def test_candidates_jsonl_required_fields(tmp_path: Path) -> None:
    candidates = [_make_candidate("close_adj")]
    write_candidates_jsonl(tmp_path, candidates)
    line = (tmp_path / "candidates.jsonl").read_text(encoding="utf-8").splitlines()[0]
    obj = json.loads(line)
    for key in REQUIRED_CANDIDATE_KEYS:
        assert key in obj, f"Missing key: {key}"


def test_candidates_jsonl_newline_terminated(tmp_path: Path) -> None:
    candidates = [_make_candidate("close_adj")]
    write_candidates_jsonl(tmp_path, candidates)
    raw = (tmp_path / "candidates.jsonl").read_bytes()
    assert raw.endswith(b"\n")


# ---------------------------------------------------------------------------
# Test 3: shortlist.json — accepted only, ordering, family_rank
# ---------------------------------------------------------------------------


def test_shortlist_only_accepted(tmp_path: Path) -> None:
    accepted = [
        _make_candidate("close_adj", status="accepted_oos", fitness=0.6),
        _make_candidate("rolling_close", status="accepted_oos", fitness=0.4),
    ]
    write_shortlist(tmp_path, accepted)
    data = json.loads((tmp_path / "shortlist.json").read_text(encoding="utf-8"))
    assert len(data) == 2
    for entry in data:
        assert entry["status"] == "accepted_oos"


def test_shortlist_ordered_fitness_desc(tmp_path: Path) -> None:
    accepted = [
        _make_candidate("rolling_close", status="accepted_oos", fitness=0.4),
        _make_candidate("close_adj", status="accepted_oos", fitness=0.6),
    ]
    write_shortlist(tmp_path, accepted)
    data = json.loads((tmp_path / "shortlist.json").read_text(encoding="utf-8"))
    fitnesses = [e["fitness"] for e in data]
    assert fitnesses == sorted(fitnesses, reverse=True)


def test_shortlist_family_rank(tmp_path: Path) -> None:
    """family_rank is 1-based rank within each family (R7.3)."""
    accepted = [
        _make_candidate("alpha", status="accepted_oos", fitness=0.9, family="trend"),
        _make_candidate("beta", status="accepted_oos", fitness=0.7, family="trend"),
        _make_candidate("gamma", status="accepted_oos", fitness=0.8, family="value"),
    ]
    write_shortlist(tmp_path, accepted)
    data = json.loads((tmp_path / "shortlist.json").read_text(encoding="utf-8"))
    # Build expr_id → family_rank map
    rank_map = {e["expr_id"]: e["family_rank"] for e in data}
    # alpha (fitness 0.9, trend) → rank 1; beta (fitness 0.7, trend) → rank 2
    assert rank_map[_cid("alpha")] == 1
    assert rank_map[_cid("beta")] == 2
    # gamma (fitness 0.8, value) → rank 1 in its family
    assert rank_map[_cid("gamma")] == 1


def test_shortlist_tiebreaker_expr_id_asc(tmp_path: Path) -> None:
    """Equal fitness → sort by expr_id asc as tiebreaker."""
    c1 = _make_candidate("aaa", status="accepted_oos", fitness=0.5)
    c2 = _make_candidate("zzz", status="accepted_oos", fitness=0.5)
    write_shortlist(tmp_path, [c2, c1])
    data = json.loads((tmp_path / "shortlist.json").read_text(encoding="utf-8"))
    ids = [e["expr_id"] for e in data]
    assert ids == sorted(ids)


# ---------------------------------------------------------------------------
# Test 4: correlation_matrix.csv header and cell formats
# ---------------------------------------------------------------------------


def test_correlation_matrix_header(tmp_path: Path) -> None:
    candidates = [_make_candidate("close_adj")]
    ref_ids = ["desc_a", "desc_b"]
    matrix = {"close_adj": {"desc_a": 0.123456, "desc_b": None}}
    write_correlation_matrix(tmp_path, matrix, candidates, ref_ids)
    rows = list(csv.reader((tmp_path / "correlation_matrix.csv").open(encoding="utf-8")))
    assert rows[0] == ["expr_id", "desc_a", "desc_b"]


def test_correlation_matrix_cell_float_format(tmp_path: Path) -> None:
    """Defined cells formatted to 6 decimal places."""
    candidates = [_make_candidate("close_adj")]
    ref_ids = ["desc_a"]
    matrix = {"close_adj": {"desc_a": 0.1}}
    write_correlation_matrix(tmp_path, matrix, candidates, ref_ids)
    rows = list(csv.reader((tmp_path / "correlation_matrix.csv").open(encoding="utf-8")))
    # data row
    assert rows[1][1] == "0.100000"


def test_correlation_matrix_undefined_cell_is_empty(tmp_path: Path) -> None:
    """None value → empty string in CSV (R6.5)."""
    candidates = [_make_candidate("close_adj")]
    ref_ids = ["desc_a"]
    matrix = {"close_adj": {"desc_a": None}}
    write_correlation_matrix(tmp_path, matrix, candidates, ref_ids)
    rows = list(csv.reader((tmp_path / "correlation_matrix.csv").open(encoding="utf-8")))
    assert rows[1][1] == ""


def test_correlation_matrix_row_keyed_by_expr_id(tmp_path: Path) -> None:
    """Rows keyed by expr_id, not canonical string."""
    c = _make_candidate("close_adj")
    matrix = {"close_adj": {"desc_a": 0.5}}
    write_correlation_matrix(tmp_path, matrix, [c], ["desc_a"])
    rows = list(csv.reader((tmp_path / "correlation_matrix.csv").open(encoding="utf-8")))
    assert rows[1][0] == c.expr_id


# ---------------------------------------------------------------------------
# Test 5: audit.md contains Promotion Path section
# ---------------------------------------------------------------------------


def test_audit_md_has_promotion_path(tmp_path: Path) -> None:
    accepted = [_make_candidate("close_adj")]
    write_audit_md(tmp_path, accepted)
    text = (tmp_path / "audit.md").read_text(encoding="utf-8")
    assert "Promotion Path" in text


def test_audit_md_has_pr_steps(tmp_path: Path) -> None:
    """Promotion Path includes all three manual steps."""
    accepted = [_make_candidate("close_adj")]
    write_audit_md(tmp_path, accepted)
    text = (tmp_path / "audit.md").read_text(encoding="utf-8")
    assert "economic_story" in text
    assert "risk_notes" in text
    assert "config/descriptors/" in text
    assert "review" in text.lower()


def test_audit_md_has_candidate_section(tmp_path: Path) -> None:
    accepted = [_make_candidate("close_adj")]
    write_audit_md(tmp_path, accepted)
    text = (tmp_path / "audit.md").read_text(encoding="utf-8")
    assert "close_adj" in text
    assert "TODO" in text
    assert "needs_more_data" in text


def test_audit_md_empty_accepted(tmp_path: Path) -> None:
    """Zero accepted candidates → only Promotion Path section, no crash."""
    write_audit_md(tmp_path, [])
    text = (tmp_path / "audit.md").read_text(encoding="utf-8")
    assert "Promotion Path" in text


# ---------------------------------------------------------------------------
# Test 6: POSIX paths in JSON artifacts (R7.10)
# ---------------------------------------------------------------------------


def test_manifest_no_backslashes(tmp_path: Path) -> None:
    """manifest.json required fields must not contain backslash path separators (R7.10).

    The manifest schema has no OS-path fields in its required keys, so the
    writer itself must not introduce any backslashes.
    """
    meta = _make_manifest()
    write_manifest(tmp_path, meta)
    data = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    # Check the string fields that the writer is responsible for
    for key in REQUIRED_MANIFEST_KEYS:
        v = data[key]
        if isinstance(v, str):
            assert "\\" not in v, f"Key {key!r} contains backslash: {v!r}"


def test_candidates_expression_no_backslash(tmp_path: Path) -> None:
    """expression field in candidates.jsonl must not contain backslashes."""
    c = _make_candidate("close_adj")
    write_candidates_jsonl(tmp_path, [c])
    line = (tmp_path / "candidates.jsonl").read_text(encoding="utf-8").splitlines()[0]
    obj = json.loads(line)
    assert "\\" not in obj["expression"]


# ---------------------------------------------------------------------------
# Test 7: Round-trip manifest
# ---------------------------------------------------------------------------


def test_manifest_roundtrip(tmp_path: Path) -> None:
    meta = _make_manifest(run_id="test-run-001")
    write_manifest(tmp_path, meta)
    recovered = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert recovered["run_id"] == "test-run-001"
    assert recovered["seed"] == 42
    assert recovered["duration_seconds"] == 12.345
    assert recovered["walk_forward_segments"] == 3
    assert recovered["config_snapshot"] == {"search": {"beam_width": 20}}


def test_manifest_roundtrip_all_keys_preserved(tmp_path: Path) -> None:
    meta = _make_manifest()
    write_manifest(tmp_path, meta)
    recovered = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    for key in REQUIRED_MANIFEST_KEYS:
        assert recovered[key] == meta[key], f"Key mismatch: {key}"
