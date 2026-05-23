# Implementation Plan: Descriptor Evaluation

## Overview

Implementation of Stage 2: descriptor compute registry, IC/IR/decile evaluator, and three new CLI commands (`compute-descriptor`, `evaluate-descriptor`, `list-evaluation-reports`). All new code lives under `src/alpha_find_v2/factor_evaluation/`. No existing code or schemas are modified.

## Task Dependency Graph

```json
{
  "waves": [
    {
      "wave": 1,
      "parallel": true,
      "tasks": ["1", "2"],
      "description": "Foundation: package scaffold + synthetic test fixture"
    },
    {
      "wave": 2,
      "parallel": false,
      "tasks": ["5"],
      "description": "Compute registry skeleton"
    },
    {
      "wave": 3,
      "parallel": true,
      "tasks": ["3", "4", "6", "7", "8", "9", "10", "11"],
      "description": "Universe resolver, forward returns, 5 descriptor computes, 5000-credit stubs"
    },
    {
      "wave": 4,
      "parallel": true,
      "tasks": ["12", "16"],
      "description": "IC math, cross-correlation"
    },
    {
      "wave": 5,
      "parallel": true,
      "tasks": ["13", "14", "15", "17"],
      "description": "Decile L-S, rank stability, slice stability, tradeability filter"
    },
    {
      "wave": 6,
      "parallel": false,
      "tasks": ["18"],
      "description": "Top-level evaluator entry point"
    },
    {
      "wave": 7,
      "parallel": false,
      "tasks": ["19"],
      "description": "Report writer (JSON + Markdown)"
    },
    {
      "wave": 8,
      "parallel": true,
      "tasks": ["20", "21", "22"],
      "description": "CLI handlers"
    },
    {
      "wave": 9,
      "parallel": false,
      "tasks": ["23"],
      "description": "Wire CLI subcommands in cli.py"
    },
    {
      "wave": 10,
      "parallel": true,
      "tasks": ["24", "25"],
      "description": "E2E tests + README update"
    }
  ]
}
```

## Tasks

- [x] 1. Create package scaffold and exception types
  - Create `src/alpha_find_v2/factor_evaluation/__init__.py` (empty package marker)
  - Create `src/alpha_find_v2/factor_evaluation/exceptions.py` defining `DescriptorNotImplemented(descriptor_id, requires, message)`, `UniverseEmpty`, `EvaluationError`
  - **Verification**: import from package; `DescriptorNotImplemented("foo", ("bar",), "msg")` carries the three fields and stringifies cleanly
  - _Requirements: R4.4_

- [x] 2. Build synthetic research-DB fixture for tests
  - Create `tests/_fixtures/__init__.py` and `tests/_fixtures/synth_research_db.py`
  - Function `build_synth_research_db(path: Path, n_securities: int = 5, n_dates: int = 250) -> None` that writes a tiny `research_source.duckdb` containing: `daily_bar_pit`, `raw_daily_basic`, `raw_adj_factor`, `industry_classification_pit`, `benchmark_membership_pit`, `market_trade_calendar`, `security_master_ref`
  - Schema must match what `market_data_bootstrap.build_research_source_db` produces; columns required by descriptors: `close_adj`, `open`, `turnover_value_cny`, `pe`, `pb`, `free_share`, `industry_code`
  - Returns deterministic synthetic prices with known monotone structure for IC tests
  - **Verification**: unit test loads the fixture and asserts row counts and column presence
  - _Requirements: R5.4, R8.1_

- [x] 3. Implement UniverseResolver (Q2)
  - File: `src/alpha_find_v2/factor_evaluation/universe_resolver.py`
  - Abstract `UniverseResolver.resolve(trade_date: str) -> set[str]`
  - `BenchmarkUniverseResolver(conn, benchmark_id)` queries `benchmark_membership_pit` with PIT join
  - `InvestableCoreUniverseResolver(conn, mandate)` reads `min_listing_days`, `min_median_daily_turnover_cny_mn`, `exclude_st`, `exclude_suspended` from a `Mandate` dataclass and applies them via SQL
  - Factory `resolver_for_universe(universe_id: str, conn, mandate) -> UniverseResolver`
  - **Verification**: against synth fixture, `BenchmarkUniverseResolver.resolve("20240105")` returns the expected member set; `InvestableCoreUniverseResolver` correctly drops names with insufficient listing days; empty resolver for an out-of-range date returns empty set
  - _Requirements: R6.1, R6.2, R6.3_
  - _Dependencies: 1, 2_

- [x] 4. Implement forward-return SQL helper (Q4)
  - File: `src/alpha_find_v2/factor_evaluation/forward_returns.py`
  - Function `compute_forward_returns(conn, *, start_date, end_date, horizons: tuple[int, ...]) -> dict[int, pd.DataFrame]` returning per-horizon DataFrames with columns `(security_id, trade_date, forward_return)` using LEAD-based SQL from design §3.4
  - Result is `(open_t1+H / open_t1) - 1.0` with adj_factor applied
  - Drop rows where either entry or exit open is NULL
  - **Verification**: on a 3-stock × 50-date synthetic fixture with monotone prices, computed `forward_return_5` matches manually-derived expected values; rows where `LEAD` exits the window are absent (no NaN propagation)
  - _Requirements: R5.1, R5.4_
  - _Dependencies: 2_

- [x] 5. Define ComputeContext and registry skeleton
  - File: `src/alpha_find_v2/factor_evaluation/descriptor_compute.py`
  - `@dataclass(slots=True) ComputeContext(conn, start_date, end_date, universe)`
  - `@dataclass(frozen=True, slots=True) DescriptorComputeSpec(descriptor_id, fn, requires, notes)`
  - Module-level `REGISTRY: dict[str, DescriptorComputeSpec] = {}`
  - Public functions: `register(spec)`, `get(descriptor_id)`, `list_registered() -> list[str]`
  - `get(unknown_id)` raises `KeyError` with the registered ids in the message
  - **Verification**: register a no-op spec, retrieve it, list returns it; `get("missing")` raises with helpful message
  - _Requirements: R4.1, R4.3_
  - _Dependencies: 1_

- [x] 6. Implement `medium_term_relative_strength` compute
  - In `descriptor_compute.py`: `_compute_medium_term_relative_strength(ctx) -> pd.DataFrame`
  - Single SQL: for each `(security_id, trade_date)` produce `log(close_adj[t]/close_adj[t-60]) - log(close_adj[t]/close_adj[t-5])`
  - Use `LAG(close_adj, 60)` and `LAG(close_adj, 5)` over `(PARTITION BY security_id ORDER BY trade_date)`
  - Drop rows where either lag is NULL
  - Register with `requires=("daily_bar_pit", "raw_adj_factor")`
  - **Verification**: against synth fixture (known monotone prices), values match manual calculation for 3 sample dates; row count ≈ (n_dates - 60) × n_securities
  - _Requirements: R4.2_
  - _Dependencies: 5_

- [x] 7. Implement `trend_stability` compute
  - SQL approach: rolling 60-day mean and stddev of daily log returns
  - `descriptor = mean(60d log returns) / NULLIF(std(60d log returns), 0)`
  - Use DuckDB `AVG ... OVER` and `STDDEV ... OVER` window functions
  - Register with `requires=("daily_bar_pit", "raw_adj_factor")`
  - **Verification**: against synth fixture with constant-growth stocks, std → 0, descriptor = NULL (handled by NULLIF); against random-walk synthetic prices, descriptor is finite; PIT leak sample test
  - _Requirements: R4.2_
  - _Dependencies: 5_

- [x] 8. Implement `turnover_confirmation` compute
  - SQL: `mean(turnover_value_cny[t-5..t]) / mean(turnover_value_cny[t-60..t-6])`
  - Use two rolling-window aggregates; the older window excludes the recent 5 days
  - Register with `requires=("daily_bar_pit",)`
  - **Verification**: synthetic fixture where one stock's turnover doubles in last 5 days → descriptor for that stock ≈ 2.0; row count expected
  - _Requirements: R4.2_
  - _Dependencies: 5_

- [x] 9. Implement `industry_relative_strength` compute
  - SQL: per `(trade_date, sw2021_l1_code)`, compute industry mean of 60d log returns, then `descriptor = stock_60d_log_return - industry_mean_60d_log_return`
  - Industry code resolved via PIT join: `industry_classification_pit` where `effective_at <= trade_date < removed_at OR removed_at IS NULL` and `industry_schema = 'sw2021_l1'`
  - Register with `requires=("daily_bar_pit", "raw_adj_factor", "industry_classification_pit")`
  - **Verification**: synth fixture with two stocks in same industry (one stronger), the stronger one has positive descriptor, the weaker negative; rows missing PIT industry are dropped, not zero-filled
  - _Requirements: R4.2_
  - _Dependencies: 5_

- [x] 10. Implement `sector_relative_valuation` compute
  - Source: `raw_daily_basic.pb` joined with `industry_classification_pit (sw2021_l1)`
  - SQL: per `(trade_date, industry_code)`, z-score of `1.0 / NULLIF(pb, 0)` so cheaper-is-better has higher score
  - Register with `requires=("raw_daily_basic", "industry_classification_pit")`
  - **Verification**: synth fixture where one stock has lowest PB in its industry → highest z-score; stocks with `pb <= 0` or NULL are dropped; one row per `(trade_date, security_id)`
  - _Requirements: R4.2_
  - _Dependencies: 5_

- [x] 11. Implement stub registry entries for 5000-credit descriptors
  - File: `src/alpha_find_v2/factor_evaluation/descriptor_stubs.py`
  - Register 5 stubs: `accrual_quality`, `profitability_quality`, `leverage_conservatism`, `estimate_revision_breadth`, `post_earnings_drift_signal`
  - Each stub function raises `DescriptorNotImplemented(descriptor_id=..., requires=("pit_fina_indicator", ...), message="Requires 5000 Tushare credits.")`
  - Register via `descriptor_compute.register()` at import time
  - **Verification**: calling any stub raises `DescriptorNotImplemented` with correct `descriptor_id` and non-empty `requires` tuple; CLI exits with code 3 (tested in task 18)
  - _Requirements: R4.2, R4.4_
  - _Dependencies: 5_

- [x] 12. Implement IC and core metric computation
  - File: `src/alpha_find_v2/factor_evaluation/descriptor_evaluator.py`
  - `_compute_per_date_ic(panel: pd.DataFrame, method: Literal["pearson", "spearman"]) -> pd.Series` indexed by trade_date
  - Aggregate to `ICStats(mean, std, tstat, n)` with correct sample-stddev formula
  - `_compute_ic_decay(panel, horizons) -> dict[int, float]` and `half_life(ic_decay_dict) -> int`
  - **Verification**: monotone factor → forward return → Pearson IC mean ≈ 1.0; constant factor → IC ≈ NaN handled gracefully; sign-flipped factor → IC ≈ -1; t-stat formula matches `(mean / std) * sqrt(n)` exactly
  - _Requirements: R2.1_
  - _Dependencies: 4, 5_

- [x] 13. Implement decile bucketing and L-S returns
  - In `descriptor_evaluator.py`: `_assign_deciles(values: pd.Series) -> pd.Series` using `pd.qcut(..., q=10, duplicates="drop")`
  - When fewer than 10 unique buckets emerge for a trade date, log a warning but proceed with whatever number remains
  - Compute per-decile mean forward return, top-minus-bottom L-S series
  - Annualised return: `(1 + mean_period_return) ** periods_per_year - 1`; default `periods_per_year=252` for daily horizons, derived from horizon-aware logic
  - Net of cost: subtract `cost_model.per_side_bps * 2 * 1e-4` per period (full round-trip)
  - Sharpe and max drawdown of the L-S equity curve
  - Monotonicity: `spearmanr(decile_index, decile_mean_return)`
  - **Verification**: linear factor → top decile mean > bottom decile mean and monotonicity Spearman ≈ 1.0; equal-valued factor → qcut warns and degraded buckets, still no exception; L-S net return < gross return when cost > 0
  - _Requirements: R2.1, R2.4_
  - _Dependencies: 12_

- [x] 14. Implement rank stability and turnover metrics
  - In `descriptor_evaluator.py`: `_compute_rank_stability(panel) -> dict[int, float]` returning lag-1 Spearman of cross-section ranks across consecutive trade dates
  - `_compute_turnover_per_period(panel) -> float`: average `|Δ rank| / N` between consecutive rebalance dates
  - **Verification**: identical-rank factor across dates → stability = 1.0, turnover = 0.0; randomly-shuffled factor → stability ≈ 0, turnover ≈ 0.5
  - _Requirements: R2.1_
  - _Dependencies: 12_

- [x] 15. Implement slice stability (industry / size tertile)
  - File: `src/alpha_find_v2/factor_evaluation/slice_stability.py`
  - Function `compute_slice_stability(panel, conn, *, slice_dim: Literal["industry", "size_tertile"]) -> list[dict]`
  - Industry slice: join `industry_classification_pit (sw2021_l1)`; per industry compute IC mean
  - Size tertile slice: market_cap = `free_share * close` from `daily_bar_pit + raw_daily_basic`; per trade-date tertile cut; per tertile compute IC mean
  - Output: list of `{slice_value, ic_pearson_mean, ic_spearman_mean, n}`
  - **Verification**: synth fixture where IC is positive in industry A and negative in industry B → returned slices reflect the split; size tertile slice produces 3 entries
  - _Requirements: R2.2_
  - _Dependencies: 12_

- [x] 16. Implement cross-correlation matrix
  - File: `src/alpha_find_v2/factor_evaluation/correlation_matrix.py`
  - Function `compute_cross_correlation(primary_panel, other_panels: dict[str, pd.DataFrame]) -> dict[str, float]`
  - For each other descriptor: align on `(trade_date, security_id)`, compute Pearson on raw values
  - Cache compute per other descriptor inside the evaluator run (dict keyed by descriptor_id)
  - **Verification**: identical factor → correlation = 1.0; sign-flipped → -1.0; orthogonal random → near 0; missing alignment rows are dropped
  - _Requirements: R2.3_
  - _Dependencies: 5, 6_

- [x] 17. Implement tradeability filter
  - In `descriptor_evaluator.py`: `_apply_tradeability_filter(panel, conn, *, raw_db_path: Path | None, include_untradeable: bool) -> pd.DataFrame`
  - Preferred: attach `output/raw.duckdb` if present; left-join `raw_suspend_d` and `raw_stk_limit` on `(security_id, t1)` (entry date)
  - Fallback: detect limit-lock heuristically from `daily_bar_pit` (open == high == low, |pct_chg| ≈ 10%)
  - Mark each panel row with `tradeable: bool`; default drops untradeable; `include_untradeable=True` keeps both
  - **Verification**: inject one limit-locked row → panel marks it untradeable; default IC excludes it; with `include_untradeable=True` an extra `ic_pearson_raw` series appears; raw_db absent → fallback heuristic runs with a warning
  - _Requirements: R5.3_
  - _Dependencies: 4_

- [x] 18. Implement top-level evaluator entry point
  - In `descriptor_evaluator.py`: `evaluate_descriptor(...) -> DescriptorEvaluationReport` per design §3.1
  - `@dataclass(slots=True) DescriptorEvaluationReport` matching JSON schema in design §3.3
  - Pipeline stages 1–11 from design §3.2, in order
  - Coverage: `rows_used / rows_possible` per trade date (mean and worst); set `low_coverage_warning` when coverage_mean < 0.30
  - descriptor_version computed via SHA256 of `inspect.getsource(fn)` + SHA256 of TOML content
  - **Verification**: run against synth fixture for `medium_term_relative_strength`; assert report contains every field in JSON schema; assert `descriptor_version` is `"sha256:<hex>"`; re-running produces identical report (excluding `run_at`)
  - _Requirements: R2.1, R2.2, R2.3, R2.4, R2.5, R2.6, R2.7, R7.1, R7.2_
  - _Dependencies: 6, 7, 8, 9, 10, 13, 14, 15, 16, 17_

- [x] 19. Implement report writer (JSON + Markdown)
  - File: `src/alpha_find_v2/factor_evaluation/report_writer.py`
  - `write_report(report, out_dir) -> Path` writes `<out_dir>/<descriptor_id>/<run_at>/report.json` (sorted keys, indent=2, UTF-8) and `report.md` (rendered tables)
  - Markdown sections: Header, Per-horizon summary table, Decile returns table, IC decay row, Slice stability tables, Cross-correlation table, Coverage block, Diagnostics
  - **Verification**: byte-identical JSON across two runs of identical inputs (modulo `run_at`); Markdown contains "Decile Returns", "IC Decay", "Slice Stability" headings; UTF-8 industry names ("银行" etc.) survive roundtrip
  - _Requirements: R2.5, R7.2, R7.3_
  - _Dependencies: 18_

- [x] 20. Implement compute-descriptor CLI handler
  - File: `src/alpha_find_v2/factor_evaluation/cli_handlers.py`
  - Function `handle_compute_descriptor(args) -> int`
  - Resolves spec from registry; runs compute(ctx); prints summary JSON; on `--out` writes Parquet via DuckDB COPY
  - Stub call → catch `DescriptorNotImplemented` → exit 3 with descriptor + missing-datasets message
  - Unregistered id → exit 2 with the registered list
  - **Verification**: smoke test with `medium_term_relative_strength` exits 0 with non-empty summary; with stub id exits 3; with unknown id exits 2
  - _Requirements: R1.1, R1.2, R1.3, R1.4_
  - _Dependencies: 6, 11_

- [x] 21. Implement evaluate-descriptor CLI handler
  - In `cli_handlers.py`: `handle_evaluate_descriptor(args) -> int`
  - Build `UniverseResolver` from `--universe` arg (factory)
  - Build cost model from `--cost-model` (or default `base_a_share_cash`)
  - Run `evaluate_descriptor(...)` → `write_report(...)`
  - Print report summary JSON to stdout
  - **Verification**: smoke test runs evaluate against synth fixture, exits 0, writes report.json + report.md, prints summary; missing research-db exits 4; empty universe exits 5
  - _Requirements: R2.1, R2.5, R2.6_
  - _Dependencies: 18, 19, 3_

- [x] 22. Implement list-evaluation-reports CLI handler
  - In `cli_handlers.py`: `handle_list_evaluation_reports(args) -> int`
  - Scan `output/descriptor_evaluation/` directory tree
  - For each `<descriptor_id>/<run_at>/report.json`, read and emit row with `descriptor_id, run_at, ic_ir_primary, coverage_mean, decile_ls_return, status`
  - Filter by `--id` if provided
  - Sort by descriptor_id then run_at desc
  - Print as JSON array
  - **Verification**: empty directory → empty list; after one evaluation → one row; corrupt JSON → skip with warning, still exits 0
  - _Requirements: R3.1, R3.2_
  - _Dependencies: 19_

- [x] 23. Wire CLI subcommands in cli.py
  - Add three subparsers per design §4: `compute-descriptor`, `evaluate-descriptor`, `list-evaluation-reports`
  - Add three branches in `main()` calling into `cli_handlers.py` functions
  - No existing branch is modified
  - **Verification**: `python -m alpha_find_v2 compute-descriptor --id medium_term_relative_strength --research-db <synth>` exits 0; existing 249 tests still pass; new CLI smoke tests under `test_descriptor_evaluation_cli.py` pass
  - _Requirements: R1.1, R2.1, R3.1_
  - _Dependencies: 20, 21, 22_

- [x] 24. End-to-end evaluation tests
  - File: `tests/test_descriptor_evaluation_e2e.py`
  - Uses synth fixture; runs `evaluate_descriptor` for each in-scope descriptor
  - Asserts: report contains all required fields; descriptor_version is stable across runs; tradeability injection (one limit-locked row) → marked untradeable; cost-net L-S < gross L-S
  - PBT: random monotone descriptor → IC > 0 with high probability (flagged with `pytest.mark.property`)
  - **Verification**: all 5 in-scope descriptors evaluate end-to-end on synth fixture in under 30 seconds total; no flake on 100 random seeds for the PBT case
  - _Requirements: R2.1, R5.1, R5.4, R8.1_
  - _Dependencies: 18, 19_

- [x] 25. Update README with Stage 2 example
  - Add to README "Reference Examples" section a Stage-2 block:
    - `alpha-find-v2 compute-descriptor --id medium_term_relative_strength --research-db output/research_source.duckdb --start 20240101 --end 20241231`
    - `alpha-find-v2 evaluate-descriptor --id medium_term_relative_strength --universe csi800 --start 20240101 --end 20241231`
    - `alpha-find-v2 list-evaluation-reports`
  - Add brief note on report layout under `output/descriptor_evaluation/`
  - **Verification**: README markdown lints clean; commands match CLI surface from task 23
  - _Requirements: R3.1_
  - _Dependencies: 23

## Notes

- All compute SQL must use `LAG/LEAD ... OVER (PARTITION BY security_id ORDER BY trade_date)` to remain PIT-safe. Calendar gaps are intentional — N trade-day lag, not N calendar-day lag.
- Window-function rolling aggregates require DuckDB ≥ 0.9; we already depend on it.
- Cost model integration in task 13 reuses `config_loader.load_cost_model` from existing code — read-only.
- Reports under `output/descriptor_evaluation/` are append-only; no rotation policy in Stage 2.
- PBT-flagged tasks: 24 (random monotone factor IC). All other tasks rely on deterministic synth fixtures.
- Forward-return SQL caches per horizon inside one evaluation run; do not cache across runs (no on-disk cache).
- Tasks 6–10 can run in parallel after task 5; tasks 12–17 can run in parallel after their listed prerequisites.
