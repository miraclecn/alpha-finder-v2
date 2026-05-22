"""Integration tests for factor_lab.run.execute_mining_run.

Task 18 verification:
  - Full run produces all 5 artifacts + registry entry.
  - Same seed → byte-identical candidates.jsonl (modulo run_id, run_at, duration_seconds).

Uses the same large synthetic DuckDB fixture (10 securities × 500 trading days)
as test_walk_forward.py.
"""
from __future__ import annotations

import json
import tempfile
import tomllib
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from alpha_find_v2.factor_lab.run import execute_mining_run


# ---------------------------------------------------------------------------
# Fixture helpers (copied from test_walk_forward.py)
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


def _build_large_synth_db(tmp_dir: Path) -> Path:
    db_path = tmp_dir / "research_wf.duckdb"
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


def _write_config(tmp_dir: Path) -> Path:
    """Write a minimal TOML mining config for fast integration tests."""
    config_text = """\
[search]
beam_width = 2
max_depth = 2
random_sample_size = 10
seed = 42

[fitness]
complexity_lambda = 0.05

[family]
quota_per_family = 5

[walk_forward]
segments = 1
oos_window_months = 2
min_train_months = 6
oos_ic_ir_threshold = 0.0
primary_horizon_days = 5

[dedup]
rho_threshold = 0.85
min_obs = 2

[universe]
id = "csi800"
"""
    path = tmp_dir / "test_config.toml"
    path.write_text(config_text, encoding="utf-8")
    return path


@pytest.fixture(scope="module")
def synth_db_and_dates():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        db = _build_large_synth_db(tmp_path)
        dates = _trading_days(_N_DATES)
        yield db, dates


# ---------------------------------------------------------------------------
# Test 1: Full run produces all 5 artifacts + registry entry
# ---------------------------------------------------------------------------


class TestFullRunArtifacts:
    def test_all_five_artifacts_exist(self, synth_db_and_dates, tmp_path):
        db, dates = synth_db_and_dates
        start, end = dates[0], dates[-1]
        config_path = _write_config(tmp_path)
        output_root = tmp_path / "output" / "factor_lab"

        result = execute_mining_run(
            research_db=db,
            start=start,
            end=end,
            config_path=config_path,
            output_root=output_root,
            repo_root=Path.cwd(),
        )

        run_dir = Path(result["run_dir"])
        assert (run_dir / "manifest.json").exists(), "manifest.json missing"
        assert (run_dir / "candidates.jsonl").exists(), "candidates.jsonl missing"
        assert (run_dir / "shortlist.json").exists(), "shortlist.json missing"
        assert (run_dir / "correlation_matrix.csv").exists(), "correlation_matrix.csv missing"
        assert (run_dir / "audit.md").exists(), "audit.md missing"

    def test_registry_entry_created(self, synth_db_and_dates, tmp_path):
        db, dates = synth_db_and_dates
        start, end = dates[0], dates[-1]
        config_path = _write_config(tmp_path)
        output_root = tmp_path / "output" / "factor_lab"

        result = execute_mining_run(
            research_db=db,
            start=start,
            end=end,
            config_path=config_path,
            output_root=output_root,
            repo_root=Path.cwd(),
        )

        registry_path = output_root / "registry.json"
        assert registry_path.exists(), "registry.json not created"
        entries = json.loads(registry_path.read_text(encoding="utf-8"))
        run_ids = [e["run_id"] for e in entries]
        assert result["run_id"] in run_ids, "run_id not in registry"

    def test_manifest_has_required_fields(self, synth_db_and_dates, tmp_path):
        db, dates = synth_db_and_dates
        start, end = dates[0], dates[-1]
        config_path = _write_config(tmp_path)
        output_root = tmp_path / "output" / "factor_lab"

        result = execute_mining_run(
            research_db=db,
            start=start,
            end=end,
            config_path=config_path,
            output_root=output_root,
            repo_root=Path.cwd(),
        )

        manifest = json.loads(
            (Path(result["run_dir"]) / "manifest.json").read_text(encoding="utf-8")
        )
        for field in (
            "run_id", "run_at", "git_sha", "research_db",
            "start_date", "end_date", "config_snapshot",
            "total_candidates_evaluated", "accepted_count",
            "duration_seconds",
        ):
            assert field in manifest, f"manifest missing field: {field}"


# ---------------------------------------------------------------------------
# Test 2: Same seed → byte-identical candidates.jsonl
# ---------------------------------------------------------------------------


class TestReproducibility:
    def test_same_seed_identical_candidates(self, synth_db_and_dates, tmp_path):
        db, dates = synth_db_and_dates
        start, end = dates[0], dates[-1]

        output_root_1 = tmp_path / "run1" / "factor_lab"
        output_root_2 = tmp_path / "run2" / "factor_lab"
        config_path = _write_config(tmp_path)

        result1 = execute_mining_run(
            research_db=db,
            start=start,
            end=end,
            config_path=config_path,
            output_root=output_root_1,
            repo_root=Path.cwd(),
        )
        result2 = execute_mining_run(
            research_db=db,
            start=start,
            end=end,
            config_path=config_path,
            output_root=output_root_2,
            repo_root=Path.cwd(),
        )

        lines1 = (Path(result1["run_dir"]) / "candidates.jsonl").read_text(encoding="utf-8").splitlines()
        lines2 = (Path(result2["run_dir"]) / "candidates.jsonl").read_text(encoding="utf-8").splitlines()

        # Parse and compare all fields except run_id, run_at, duration_seconds
        # (those don't appear in candidates.jsonl itself, but strip defensively)
        def _normalize(line: str) -> dict:
            obj = json.loads(line)
            for key in ("run_id", "run_at", "duration_seconds"):
                obj.pop(key, None)
            return obj

        normalized1 = sorted(
            [_normalize(l) for l in lines1 if l.strip()],
            key=lambda x: x.get("expr_id", ""),
        )
        normalized2 = sorted(
            [_normalize(l) for l in lines2 if l.strip()],
            key=lambda x: x.get("expr_id", ""),
        )

        assert normalized1 == normalized2, (
            "candidates.jsonl differs between identical-seed runs"
        )
