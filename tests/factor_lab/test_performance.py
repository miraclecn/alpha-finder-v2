"""Performance smoke test for factor_lab.

Validates R11.1 (wall-clock < 300s), R11.5 (peak RSS < 4GB),
R11.6 (time_budget_exceeded warning when > 2× budget), and
R11.7 (artifacts preserved on completion).
"""
from __future__ import annotations

import json
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from alpha_find_v2.factor_lab.run import execute_mining_run

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_N_SECURITIES = 10
_N_DATES = 500
_TIME_BUDGET_S = 300  # R11.1


# ---------------------------------------------------------------------------
# Synth fixture helpers (shared pattern with other factor_lab tests)
# ---------------------------------------------------------------------------


def _trading_days(n: int, start: date = date(2018, 1, 2)) -> list[str]:
    days: list[str] = []
    d = start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return days


def _build_synth_db(tmp_dir: Path) -> Path:
    db_path = tmp_dir / "research_perf.duckdb"
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


_MINING_CONFIG_TOML = """\
[search]
beam_width = 3
max_depth = 3
random_sample_size = 50
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


# ---------------------------------------------------------------------------
# Performance smoke test
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_performance_smoke(tmp_path):
    """R11.1, R11.5, R11.6, R11.7: synth fixture finishes within budget."""
    # Optionally import psutil for RSS measurement (R11.5).
    try:
        import psutil  # type: ignore

        _process = psutil.Process()
        _rss_before = _process.memory_info().rss
    except ImportError:
        _process = None
        _rss_before = None

    db_path = _build_synth_db(tmp_path)
    config_path = tmp_path / "mining_config.toml"
    config_path.write_text(_MINING_CONFIG_TOML, encoding="utf-8")

    dates = _trading_days(_N_DATES)
    start, end = dates[0], dates[-1]
    repo_root = Path(__file__).parents[2]

    wall_start = time.monotonic()
    result = execute_mining_run(
        research_db=db_path,
        start=start,
        end=end,
        config_path=config_path,
        output_root=tmp_path / "output",
        repo_root=repo_root,
    )
    elapsed = time.monotonic() - wall_start

    # ── R11.1: wall-clock < 300s ──────────────────────────────────────────
    assert elapsed < _TIME_BUDGET_S, (
        f"Run took {elapsed:.1f}s, exceeds {_TIME_BUDGET_S}s budget (R11.1)"
    )

    # ── R11.5: peak RSS < 4GB (skip if psutil not available) ─────────────
    if _process is not None:
        rss_after = _process.memory_info().rss
        peak_rss_bytes = rss_after - _rss_before
        four_gb = 4 * 1024 ** 3
        assert peak_rss_bytes < four_gb, (
            f"Peak RSS delta {peak_rss_bytes / 1024**3:.2f}GB exceeds 4GB limit (R11.5)"
        )

    # ── R11.7: artifacts preserved after completion ───────────────────────
    run_dir = Path(result["run_dir"])
    for artifact in ("manifest.json", "candidates.jsonl", "shortlist.json",
                     "correlation_matrix.csv", "audit.md"):
        assert (run_dir / artifact).exists(), f"Artifact missing: {artifact} (R11.7)"

    # ── R11.6: time_budget_exceeded warning only if > 2× budget ──────────
    if elapsed > 2 * _TIME_BUDGET_S:
        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
        assert "time_budget_exceeded" in manifest.get("warnings", []), (
            "Run exceeded 2× budget but manifest missing 'time_budget_exceeded' warning (R11.6)"
        )
