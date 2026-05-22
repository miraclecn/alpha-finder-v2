"""
Tests for src/alpha_find_v2/data_ingest/audit.py

Tests:
1. Clean raw.duckdb (stock_basic_ref + adj_factor with positive values) → all checks pass,
   has_blocking_failure() == False
2. PIT leak injected (ann_date > end_date) → pit_leak_sample fails, has_blocking_failure() == True
3. PBT: random positive adj_factor rows → adj_factor_consistency always passes;
   one row with adj_factor=0.0 makes it fail
4. audit.md has at least one markdown table row per registered check
5. Output files are written to the correct directory
"""
from __future__ import annotations

import json
import random
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from alpha_find_v2.data_ingest.audit import (
    AuditReport,
    _AUDIT_REGISTRY,
    run_audit,
)
from alpha_find_v2.data_ingest.schemas import RAW_TABLE_DDL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_db(path: Path) -> duckdb.DuckDBPyConnection:
    """Create a raw.duckdb with all tables from DDL (empty)."""
    conn = duckdb.connect(str(path))
    for ddl in RAW_TABLE_DDL.values():
        conn.execute(ddl)
    return conn


def _seed_clean_db(conn: duckdb.DuckDBPyConnection) -> None:
    """Insert minimal clean data so checks pass."""
    # stock_basic_ref: 1 row, no delist_date
    conn.execute(
        "INSERT INTO stock_basic_ref VALUES "
        "('000001.SZ','000001','平安银行','深圳','银行','19910403',NULL,'N',current_timestamp,'tushare')"
    )
    # raw_kline_unadj
    conn.execute(
        "INSERT INTO raw_kline_unadj VALUES "
        "('000001.SZ','20240102',10.0,10.5,9.9,10.2,10.0,0.2,2.0,100.0,200.0,'tushare',current_timestamp)"
    )
    # raw_adj_factor with positive value
    conn.execute(
        "INSERT INTO raw_adj_factor VALUES "
        "('000001.SZ','20240102',1.5,'tushare',current_timestamp)"
    )
    # raw_trade_cal (trade date within range)
    conn.execute(
        "INSERT INTO raw_trade_cal VALUES ('SSE','20240102',1,'20231229',current_timestamp,'tushare')"
    )


# ---------------------------------------------------------------------------
# Test 1: Clean data → all checks pass, has_blocking_failure() == False
# ---------------------------------------------------------------------------


def test_clean_db_all_checks_pass() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "raw.duckdb"
        conn = _make_db(db)
        _seed_clean_db(conn)
        conn.close()

        report = run_audit(raw_db_path=db, out_dir=Path(td) / "audit")

    assert isinstance(report, AuditReport)
    assert report.overall_status == "ok"
    assert report.has_blocking_failure() is False
    # No blocking failure outcomes
    failures = [o for o in report.outcomes if o.result == "fail" and o.severity == "blocking"]
    assert failures == [], f"Unexpected blocking failures: {failures}"


# ---------------------------------------------------------------------------
# Test 2: PIT leak injected → pit_leak_sample fails, has_blocking_failure() == True
# ---------------------------------------------------------------------------


def test_pit_leak_injection_causes_failure() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "raw.duckdb"
        conn = _make_db(db)
        _seed_clean_db(conn)
        # Insert a row where ann_date > end_date (PIT leak)
        conn.execute(
            "INSERT INTO pit_fina_indicator "
            "(ts_code, ann_date, end_date, eps) VALUES "
            "('000001.SZ', '20240401', '20240101', 1.23)"
        )
        conn.close()

        report = run_audit(raw_db_path=db, out_dir=Path(td) / "audit")

    assert report.overall_status == "blocking_failure"
    assert report.has_blocking_failure() is True

    pit_outcome = next(o for o in report.outcomes if o.check_id == "pit_leak_sample")
    assert pit_outcome.result == "fail"
    assert pit_outcome.severity == "blocking"


# ---------------------------------------------------------------------------
# Test 3 (PBT): random positive adj_factor rows → check passes;
#               one row with adj_factor=0.0 makes it fail
# **Validates: Requirements R5.2**
# ---------------------------------------------------------------------------


def _make_adj_factor_db(path: Path, adj_factors: list[float]) -> None:
    conn = duckdb.connect(str(path))
    conn.execute(RAW_TABLE_DDL["daily"])  # raw_kline_unadj
    conn.execute(RAW_TABLE_DDL["adj_factor"])  # raw_adj_factor
    for i, af in enumerate(adj_factors):
        ts_code = f"{100000 + i:06d}.SZ"
        trade_date = "20240102"
        conn.execute(
            "INSERT INTO raw_kline_unadj VALUES (?, ?, 10.0, 10.5, 9.9, 10.2, 10.0, 0.2, 2.0, 100.0, 200.0, 'tushare', current_timestamp)",
            [ts_code, trade_date],
        )
        conn.execute(
            "INSERT INTO raw_adj_factor VALUES (?, ?, ?, 'tushare', current_timestamp)",
            [ts_code, trade_date, af],
        )
    conn.close()


def test_pbt_all_positive_adj_factors_always_pass() -> None:
    """PBT: for many random sets of positive adj_factor values, check passes."""
    rng = random.Random(42)

    for _ in range(30):
        # Generate 1–20 positive adj_factor values
        count = rng.randint(1, 20)
        adj_factors = [rng.uniform(0.001, 5.0) for _ in range(count)]

        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "raw.duckdb"
            _make_adj_factor_db(db, adj_factors)

            report = run_audit(raw_db_path=db, out_dir=Path(td) / "audit")

        outcome = next(o for o in report.outcomes if o.check_id == "adj_factor_consistency")
        assert outcome.result in ("pass", "skip"), (
            f"Expected pass/skip with positive adj_factors, got {outcome.result}: {outcome.details}"
        )


def test_pbt_zero_adj_factor_causes_fail() -> None:
    """PBT: inserting even one row with adj_factor=0.0 makes the check fail."""
    rng = random.Random(99)

    for _ in range(20):
        count = rng.randint(1, 10)
        # All positive, then inject one zero
        adj_factors = [rng.uniform(0.001, 5.0) for _ in range(count)] + [0.0]

        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "raw.duckdb"
            _make_adj_factor_db(db, adj_factors)

            report = run_audit(raw_db_path=db, out_dir=Path(td) / "audit")

        outcome = next(o for o in report.outcomes if o.check_id == "adj_factor_consistency")
        assert outcome.result == "fail", (
            f"Expected fail with adj_factor=0.0 in list, got {outcome.result}: {outcome.details}"
        )


def test_pbt_negative_adj_factor_causes_fail() -> None:
    """PBT: adj_factor <= 0.0 (negative) also triggers failure."""
    rng = random.Random(77)

    for _ in range(15):
        count = rng.randint(1, 8)
        adj_factors = [rng.uniform(0.001, 3.0) for _ in range(count)] + [rng.uniform(-5.0, -0.001)]

        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "raw.duckdb"
            _make_adj_factor_db(db, adj_factors)

            report = run_audit(raw_db_path=db, out_dir=Path(td) / "audit")

        outcome = next(o for o in report.outcomes if o.check_id == "adj_factor_consistency")
        assert outcome.result == "fail", (
            f"Expected fail with negative adj_factor, got {outcome.result}: {outcome.details}"
        )


# ---------------------------------------------------------------------------
# Test 4: audit.md has at least one table row per registered check
# ---------------------------------------------------------------------------


def test_audit_md_has_one_row_per_check() -> None:
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "raw.duckdb"
        conn = _make_db(db)
        _seed_clean_db(conn)
        conn.close()

        out = Path(td) / "audit"
        run_audit(raw_db_path=db, out_dir=out)

        audit_dirs = sorted(out.iterdir())
        assert len(audit_dirs) == 1
        md_content = (audit_dirs[0] / "audit.md").read_text(encoding="utf-8")

    # Every registered check id should appear as a table row
    for check in _AUDIT_REGISTRY:
        assert f"| {check.id} |" in md_content, (
            f"check '{check.id}' not found as table row in audit.md"
        )


# ---------------------------------------------------------------------------
# Test 5: Output files are written to the correct directory
# ---------------------------------------------------------------------------


def test_output_files_written_to_correct_directory() -> None:
    import re

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "raw.duckdb"
        conn = _make_db(db)
        _seed_clean_db(conn)
        conn.close()

        out = Path(td) / "audit_output"
        run_audit(raw_db_path=db, out_dir=out)

        # out_dir must exist and contain exactly one timestamped subdirectory
        assert out.exists()
        subdirs = sorted(out.iterdir())
        assert len(subdirs) == 1, f"Expected 1 timestamped dir, got {[s.name for s in subdirs]}"

        ts_dir = subdirs[0]
        # The timestamp dir name should match YYYYMMDDTHHMMSSZ pattern
        assert re.match(r"\d{8}T\d{6}Z", ts_dir.name), f"Unexpected dir name: {ts_dir.name}"

        json_file = ts_dir / "audit.json"
        md_file = ts_dir / "audit.md"
        assert json_file.exists(), "audit.json not created"
        assert md_file.exists(), "audit.md not created"

        # JSON must be valid and have the right shape
        payload = json.loads(json_file.read_text(encoding="utf-8"))
        assert "raw_db_path" in payload
        assert "run_at" in payload
        assert "overall_status" in payload
        assert "outcomes" in payload
        assert len(payload["outcomes"]) == len(_AUDIT_REGISTRY)

        # MD must have the table header
        md = md_file.read_text(encoding="utf-8")
        assert "# Data Audit Report" in md
        assert "| ID |" in md


# ---------------------------------------------------------------------------
# Additional edge-case tests
# ---------------------------------------------------------------------------


def test_has_blocking_failure_false_when_only_advisory_fail() -> None:
    """A report where only advisory checks fail must not be blocking_failure."""
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "raw.duckdb"
        # Minimal DB with no tables at all → all checks will skip or pass (advisory)
        conn = duckdb.connect(str(db))
        conn.close()

        report = run_audit(raw_db_path=db, out_dir=Path(td) / "audit")

    # With no tables the blocking checks skip, not fail
    assert report.has_blocking_failure() is False


def test_clean_db_with_delisted_stock() -> None:
    """stock_basic_ref with a delist_date row passes survivorship check cleanly."""
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "raw.duckdb"
        conn = _make_db(db)
        # Active stock
        conn.execute(
            "INSERT INTO stock_basic_ref VALUES "
            "('000001.SZ','000001','平安银行','深圳','银行','19910403',NULL,'N',current_timestamp,'tushare')"
        )
        # Delisted stock
        conn.execute(
            "INSERT INTO stock_basic_ref VALUES "
            "('000002.SZ','000002','旧公司','深圳','制造','19950101','20200101','N',current_timestamp,'tushare')"
        )
        # raw_adj_factor positive
        conn.execute(
            "INSERT INTO raw_adj_factor VALUES ('000001.SZ','20240102',1.5,'tushare',current_timestamp)"
        )
        conn.execute(
            "INSERT INTO raw_kline_unadj VALUES "
            "('000001.SZ','20240102',10.0,10.5,9.9,10.2,10.0,0.2,2.0,100.0,200.0,'tushare',current_timestamp)"
        )
        conn.execute(
            "INSERT INTO raw_trade_cal VALUES ('SSE','20240102',1,'20231229',current_timestamp,'tushare')"
        )
        conn.close()

        report = run_audit(raw_db_path=db, out_dir=Path(td) / "audit")

    assert report.overall_status == "ok"
    surv = next(o for o in report.outcomes if o.check_id == "survivorship_delisted_present")
    assert "2/2" in surv.details or "1/2" in surv.details or "delist" in surv.details.lower()
