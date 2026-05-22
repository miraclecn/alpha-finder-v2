"""Reproducibility test for factor_lab mining runs.

Verifies R12.1: Two runs of the same config against the same DB snapshot
with the same seed produce byte-identical candidates.jsonl and shortlist.json
(excluding run_id, run_at, duration_seconds).

Also verifies that config_snapshot and git_sha are identical across both runs.
"""
from __future__ import annotations

import json
import tempfile
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from alpha_find_v2.factor_lab.run import execute_mining_run


# ---------------------------------------------------------------------------
# Synthetic DB builder (minimal — 10 securities × 500 trading days)
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
    db_path = tmp_dir / "research_repro.duckdb"
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
                p * free_mn * 1e4 * 0.002, free_mn * 1e4 * 0.002,
                "standard", p * 0.999, p * free_mn * 1e4, free_mn * 1e4,
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
    conn.executemany(
        "INSERT INTO raw_adj_factor VALUES (?,?,?,?,CURRENT_TIMESTAMP)",
        [(sec, td, 1.0, "synth") for sec in securities for td in dates],
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


# ---------------------------------------------------------------------------
# Config TOML content — minimal, fast, fixed seed
# ---------------------------------------------------------------------------

_MINING_CONFIG_TOML = """\
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
min_obs = 10

[universe]
id = "csi800"
"""

_VOLATILE_KEYS = {"run_id", "run_at", "duration_seconds"}


def _normalize_records(jsonl_path: Path) -> list[dict]:
    """Parse JSONL, strip volatile keys, sort by expr_id."""
    records = []
    for line in jsonl_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        for k in _VOLATILE_KEYS:
            rec.pop(k, None)
        records.append(rec)
    records.sort(key=lambda r: r.get("expr_id", ""))
    return records


def _normalize_shortlist(shortlist_path: Path) -> list[dict]:
    """Parse shortlist.json, strip volatile keys, sort by expr_id."""
    entries = json.loads(shortlist_path.read_text(encoding="utf-8"))
    for entry in entries:
        for k in _VOLATILE_KEYS:
            entry.pop(k, None)
    entries.sort(key=lambda r: r.get("expr_id", ""))
    return entries


# ---------------------------------------------------------------------------
# Reproducibility test
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_reproducibility(tmp_path):
    """R12.1: Two runs with identical config + seed produce identical outputs."""
    with tempfile.TemporaryDirectory() as db_tmp:
        db_path = _build_synth_db(Path(db_tmp))

        # Write the shared config TOML
        config_path = tmp_path / "mining_config.toml"
        config_path.write_text(_MINING_CONFIG_TOML, encoding="utf-8")

        dates = _trading_days(_N_DATES)
        start = dates[0]
        end = dates[-1]

        repo_root = Path(__file__).parents[2]  # project root

        # Run 1
        out1 = tmp_path / "run1"
        result1 = execute_mining_run(
            research_db=db_path,
            start=start,
            end=end,
            config_path=config_path,
            output_root=out1,
            repo_root=repo_root,
        )

        # Run 2
        out2 = tmp_path / "run2"
        result2 = execute_mining_run(
            research_db=db_path,
            start=start,
            end=end,
            config_path=config_path,
            output_root=out2,
            repo_root=repo_root,
        )

        run_dir1 = Path(result1["run_dir"])
        run_dir2 = Path(result2["run_dir"])

        # ── candidates.jsonl ──────────────────────────────────────────────
        cands1 = _normalize_records(run_dir1 / "candidates.jsonl")
        cands2 = _normalize_records(run_dir2 / "candidates.jsonl")
        assert cands1 == cands2, "candidates.jsonl differs between runs"

        # ── shortlist.json ────────────────────────────────────────────────
        short1 = _normalize_shortlist(run_dir1 / "shortlist.json")
        short2 = _normalize_shortlist(run_dir2 / "shortlist.json")
        assert short1 == short2, "shortlist.json differs between runs"

        # ── config_snapshot identical ─────────────────────────────────────
        manifest1 = json.loads((run_dir1 / "manifest.json").read_text(encoding="utf-8"))
        manifest2 = json.loads((run_dir2 / "manifest.json").read_text(encoding="utf-8"))
        assert manifest1["config_snapshot"] == manifest2["config_snapshot"], (
            "config_snapshot differs between runs"
        )

        # ── git_sha identical ─────────────────────────────────────────────
        assert manifest1["git_sha"] == manifest2["git_sha"], (
            "git_sha differs between runs"
        )
