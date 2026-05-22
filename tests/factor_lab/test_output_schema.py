"""End-to-end output schema integration test for factor_lab mining run.

Task 26 (R7.1–R7.6, R7.10):
  1. execute_mining_run on the synth fixture produces well-formed artifacts.
  2. manifest.json has all R7.1 required keys.
  3. candidates.jsonl rows have all R7.2 required keys.
  4. shortlist.json entries have family_rank.
  5. correlation_matrix.csv cells are numeric in [-1, 1] or empty.
  6. audit.md contains the Promotion Path section.
  7. run_dir in registry.json uses forward slashes (R7.10).
  8. Two registry appends preserve insertion order (R7.6).
"""
from __future__ import annotations

import csv
import json
from datetime import date, timedelta
from io import StringIO
from pathlib import Path

import duckdb
import pytest

from alpha_find_v2.factor_lab.registry import append_run_entry
from alpha_find_v2.factor_lab.run import execute_mining_run


# ---------------------------------------------------------------------------
# Synth DB builder — 10 securities × 500 trading days
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
            security_id VARCHAR PRIMARY KEY, symbol VARCHAR, current_name VARCHAR,
            exchange VARCHAR, board VARCHAR, area VARCHAR, list_date VARCHAR,
            delist_date VARCHAR, is_hs VARCHAR, is_a_share BOOLEAN, ingested_at TIMESTAMP
        )
    """)
    for sec in securities:
        sym = sec.split(".")[0]
        conn.execute(
            "INSERT INTO security_master_ref VALUES"
            " (?,?,?,'SH','main_board','上海','20100101',NULL,'N',TRUE,CURRENT_TIMESTAMP)",
            [sec, sym, f"Stock_{sym}"],
        )

    conn.execute("""
        CREATE TABLE daily_bar_pit (
            security_id VARCHAR, trade_date VARCHAR, exchange VARCHAR, board VARCHAR,
            is_st BOOLEAN, pre_close DOUBLE, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, close_adj DOUBLE, turnover_value_cny DOUBLE,
            volume_shares DOUBLE, price_basis VARCHAR, open_adj DOUBLE,
            float_mcap_cny DOUBLE, free_float_shares DOUBLE,
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
                p * free_mn * 1e4 * 0.002, free_mn * 1e4 * 0.002, "standard",
                p * 0.999, p * free_mn * 1e4, free_mn * 1e4,
            ))
    conn.executemany(
        "INSERT INTO daily_bar_pit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
    )

    conn.execute("""
        CREATE TABLE raw_adj_factor (
            ts_code VARCHAR, trade_date VARCHAR, adj_factor DOUBLE,
            source_table VARCHAR, ingested_at TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date)
        )
    """)
    conn.executemany(
        "INSERT INTO raw_adj_factor VALUES (?,?,?,?,CURRENT_TIMESTAMP)",
        [(sec, td, 1.0, "synth") for sec in securities for td in dates],
    )

    conn.execute("""
        CREATE TABLE benchmark_membership_pit (
            benchmark_id VARCHAR, security_id VARCHAR, effective_at VARCHAR,
            removed_at VARCHAR, PRIMARY KEY (benchmark_id, security_id, effective_at)
        )
    """)
    for sec in securities:
        conn.execute(
            "INSERT INTO benchmark_membership_pit VALUES ('CSI 800',?,'20100101',NULL)", [sec]
        )

    conn.close()
    return db_path


_MINING_CONFIG = """\
[search]
beam_width = 2
max_depth = 2
random_sample_size = 5
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

# Required keys from run.py's run_metadata dict (R7.1)
_MANIFEST_REQUIRED_KEYS = [
    "run_id", "run_at", "git_sha", "config_snapshot",
    "start_date", "end_date", "total_candidates_evaluated",
    "accepted_count", "duration_seconds",
]

# Required keys per candidates.jsonl row (R7.2)
_CANDIDATE_REQUIRED_KEYS = [
    "expr_id", "expression", "node_count", "family", "sources",
    "train_ic_ir", "fitness", "oos_segments", "status",
]


# ---------------------------------------------------------------------------
# Module-scoped fixture: run the pipeline once, shared across all tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def run_result(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("schema_test")
    db_path = _build_synth_db(tmp)
    config_path = tmp / "config.toml"
    config_path.write_text(_MINING_CONFIG, encoding="utf-8")
    dates = _trading_days(_N_DATES)
    output_root = tmp / "output"
    result = execute_mining_run(
        research_db=db_path,
        start=dates[0],
        end=dates[-1],
        config_path=config_path,
        output_root=output_root,
        repo_root=Path(__file__).parents[2],
    )
    return {
        "run_id": result["run_id"],
        "run_dir": Path(result["run_dir"]),
        "output_root": output_root,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_manifest_required_keys(run_result):
    """R7.1: manifest.json has all required keys; zero-count keys present."""
    manifest = json.loads((run_result["run_dir"] / "manifest.json").read_text(encoding="utf-8"))
    for key in _MANIFEST_REQUIRED_KEYS:
        assert key in manifest, f"manifest missing: {key!r}"


def test_candidates_jsonl_required_keys(run_result):
    """R7.2: every row in candidates.jsonl has all required keys."""
    lines = [
        l for l in
        (run_result["run_dir"] / "candidates.jsonl").read_text(encoding="utf-8").splitlines()
        if l.strip()
    ]
    assert lines, "candidates.jsonl is empty"
    for line in lines:
        row = json.loads(line)
        for key in _CANDIDATE_REQUIRED_KEYS:
            assert key in row, f"candidates.jsonl row missing: {key!r}"


def test_shortlist_has_family_rank(run_result):
    """R7.3: every entry in shortlist.json has family_rank."""
    entries = json.loads(
        (run_result["run_dir"] / "shortlist.json").read_text(encoding="utf-8")
    )
    for entry in entries:
        assert "family_rank" in entry, (
            f"shortlist entry missing family_rank: {entry.get('expr_id')}"
        )


def test_correlation_matrix_cells_in_range(run_result):
    """R7.4: data cells in correlation_matrix.csv are numeric in [-1, 1] or empty."""
    text = (run_result["run_dir"] / "correlation_matrix.csv").read_text(encoding="utf-8")
    rows = list(csv.reader(StringIO(text)))
    for row in rows[1:]:          # skip header
        for cell in row[1:]:      # skip expr_id column
            if cell == "":
                continue
            val = float(cell)
            assert -1.0 <= val <= 1.0, f"correlation value out of range: {val}"


def test_audit_md_has_promotion_path(run_result):
    """R7.5/R8.6: audit.md contains the Promotion Path section."""
    text = (run_result["run_dir"] / "audit.md").read_text(encoding="utf-8")
    assert "Promotion Path" in text


def test_registry_run_dir_forward_slashes(run_result):
    """R7.10: run_dir stored in registry.json uses forward slashes."""
    registry_path = run_result["output_root"] / "registry.json"
    entries = json.loads(registry_path.read_text(encoding="utf-8"))
    for entry in entries:
        assert "\\" not in entry["run_dir"], (
            f"registry run_dir has backslash: {entry['run_dir']!r}"
        )


def test_registry_append_preserves_order(tmp_path):
    """R7.6: successive appends to registry preserve insertion order."""
    registry_path = tmp_path / "registry.json"
    for i, run_id in enumerate(["run_alpha", "run_beta"]):
        run_dir = tmp_path / "runs" / run_id
        run_dir.mkdir(parents=True)
        append_run_entry(
            run_id=run_id,
            run_at=f"2024-0{i + 1}-01T00:00:00.000Z",
            run_dir=run_dir,
            candidate_count=i + 1,
            accepted_count=0,
            families_present=[],
            registry_path=registry_path,
        )
    entries = json.loads(registry_path.read_text(encoding="utf-8"))
    assert [e["run_id"] for e in entries] == ["run_alpha", "run_beta"]
