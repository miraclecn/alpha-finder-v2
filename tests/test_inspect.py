"""Integration tests for factor_lab.inspect.run_inspection.

Task 19 verification:
  - Missing run_id exits 4, run_dir path in stderr
  - Missing expr_id exits 5, known ids in stderr
  - Valid run + valid expr_id creates report.json and report.md under
    output/factor_lab/runs/<run_id>/inspections/<expr_id>/

Uses the large synthetic DuckDB (10 securities × 500 trading days) and a
manually-constructed run directory with candidates.jsonl and manifest.json.
"""
from __future__ import annotations

import json
import tempfile
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from alpha_find_v2.factor_lab.inspect import run_inspection


# ---------------------------------------------------------------------------
# Synth DB builder (same structure as test_walk_forward.py)
# ---------------------------------------------------------------------------

_N_SECURITIES = 10
_N_DATES = 500


def _trading_days(n: int, start: date = date(2018, 1, 2)) -> list[str]:
    days: list[str] = []
    d = start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return days


def _build_synth_db(tmp_dir: Path) -> Path:
    db_path = tmp_dir / "research.duckdb"
    dates = _trading_days(_N_DATES)
    securities = [f"{600000 + i:06d}.SH" for i in range(_N_SECURITIES)]

    conn = duckdb.connect(str(db_path))

    conn.execute("CREATE TABLE market_trade_calendar (trade_date VARCHAR PRIMARY KEY)")
    conn.executemany("INSERT INTO market_trade_calendar VALUES (?)", [(d,) for d in dates])

    conn.execute("""
        CREATE TABLE security_master_ref (
            security_id VARCHAR PRIMARY KEY,
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
    """)
    for sec in securities:
        sym = sec.split(".")[0]
        conn.execute(
            "INSERT INTO security_master_ref VALUES (?,?,?,'SH','main_board','上海','20100101',NULL,'N',TRUE,CURRENT_TIMESTAMP)",
            [sec, sym, f"Stock_{sym}"],
        )

    conn.execute("""
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
            volume_shares DOUBLE,
            price_basis VARCHAR,
            open_adj DOUBLE,
            float_mcap_cny DOUBLE,
            free_float_shares DOUBLE,
            PRIMARY KEY (security_id, trade_date)
        )
    """)
    rows = []
    for i, sec in enumerate(securities):
        growth = 1.001 + i * 0.0002
        base = 10.0 * (i + 1)
        free_mn = 3000.0
        for j, td in enumerate(dates):
            p = base * (growth ** j)
            rows.append((
                sec, td, "SH", "main_board", False,
                p / growth, p * 0.999, p * 1.01, p * 0.99, p, p,
                p * free_mn * 1e4 * 0.002,
                free_mn * 1e4 * 0.002,
                "standard",
                p * 0.999,
                p * free_mn * 1e4,
                free_mn * 1e4,
            ))
    conn.executemany(
        "INSERT INTO daily_bar_pit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )

    conn.execute("""
        CREATE TABLE raw_adj_factor (
            ts_code VARCHAR,
            trade_date VARCHAR,
            adj_factor DOUBLE,
            source_table VARCHAR,
            ingested_at TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    adj_rows = [(sec, td, 1.0, "synth") for sec in securities for td in dates]
    conn.executemany(
        "INSERT INTO raw_adj_factor VALUES (?,?,?,?,CURRENT_TIMESTAMP)",
        adj_rows,
    )

    conn.execute("""
        CREATE TABLE benchmark_membership_pit (
            benchmark_id VARCHAR,
            security_id VARCHAR,
            effective_at VARCHAR,
            removed_at VARCHAR,
            PRIMARY KEY (benchmark_id, security_id, effective_at)
        )
    """)
    for sec in securities:
        conn.execute(
            "INSERT INTO benchmark_membership_pit VALUES ('CSI 800',?,'20100101',NULL)",
            [sec],
        )

    conn.close()
    return db_path


def _make_run_dir(output_root: Path, run_id: str, dates: list[str]) -> Path:
    """Create a pre-built run directory with manifest.json and candidates.jsonl."""
    run_dir = output_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "run_id": run_id,
        "run_at": "2024-01-01T00:00:00+00:00",
        "git_sha": "abc123",
        "research_db": "research.duckdb",
        "start_date": dates[0],
        "end_date": dates[-1],
        "config_snapshot": {"universe": {"id": "csi800"}},
        "total_candidates_evaluated": 1,
        "accepted_count": 1,
        "duration_seconds": 1.0,
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    candidate = {
        "expr_id": "cand_001",
        "expression": "close_adj",
        "node_count": 1,
        "family": "trend",
        "sources": ["beam"],
        "train_ic_ir": 0.5,
        "fitness": 0.45,
        "oos_segments": [],
        "status": "accepted_oos",
    }
    (run_dir / "candidates.jsonl").write_text(
        json.dumps(candidate) + "\n", encoding="utf-8"
    )

    return run_dir


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def synth_env():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        db = _build_synth_db(tmp_path)
        dates = _trading_days(_N_DATES)
        yield db, dates, tmp_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMissingRunId:
    def test_exits_4_with_run_dir_in_stderr(self, synth_env, tmp_path, capsys):
        db, dates, _ = synth_env
        output_root = tmp_path / "output" / "factor_lab"
        output_root.mkdir(parents=True, exist_ok=True)

        with pytest.raises(SystemExit) as exc_info:
            run_inspection(
                run_id="nonexistent_run",
                expr_id="cand_001",
                output_root=output_root,
                research_db=db,
            )

        assert exc_info.value.code == 4
        captured = capsys.readouterr()
        expected_path = str(output_root / "runs" / "nonexistent_run")
        assert expected_path in captured.err


class TestMissingExprId:
    def test_exits_5_with_known_ids_in_stderr(self, synth_env, tmp_path, capsys):
        db, dates, _ = synth_env
        output_root = tmp_path / "output" / "factor_lab"
        _make_run_dir(output_root, "run_abc", dates)

        with pytest.raises(SystemExit) as exc_info:
            run_inspection(
                run_id="run_abc",
                expr_id="no_such_expr",
                output_root=output_root,
                research_db=db,
            )

        assert exc_info.value.code == 5
        captured = capsys.readouterr()
        # stderr should contain the known candidate id
        assert "cand_001" in captured.err


class TestValidInspection:
    def test_writes_report_json_and_report_md(self, synth_env, tmp_path):
        db, dates, _ = synth_env
        output_root = tmp_path / "output" / "factor_lab"
        run_id = "run_valid"
        _make_run_dir(output_root, run_id, dates)

        with pytest.raises(SystemExit) as exc_info:
            run_inspection(
                run_id=run_id,
                expr_id="cand_001",
                output_root=output_root,
                research_db=db,
            )

        assert exc_info.value.code == 0

        inspection_base = output_root / "runs" / run_id / "inspections" / "cand_001"
        assert inspection_base.exists(), "inspection dir not created"

        # report_writer.write_report creates subdirs under out_dir;
        # find report.json and report.md recursively
        json_files = list(inspection_base.rglob("report.json"))
        md_files = list(inspection_base.rglob("report.md"))
        assert json_files, "report.json not found under inspection dir"
        assert md_files, "report.md not found under inspection dir"
