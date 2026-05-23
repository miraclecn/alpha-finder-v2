# Descriptor Evaluation — Requirements

> Stage 2 of the V2 rebuild roadmap.
>
> Goal: Make every registered descriptor numerically computable from the V2 PIT research database, then evaluate each descriptor against industry-standard quality metrics (IC, Rank IC, IC_IR, IC decay, decile long-short return, monotonicity, half-life, coverage, turnover, cross-correlation, regime stability).

## 1. Scope

### In scope

- A descriptor compute registry: per-descriptor function returning a tidy `(trade_date, security_id, descriptor_value)` frame.
- Concrete compute implementations for 5 price-and-volume descriptors that only depend on Stage 1 green data:
  - `medium_term_relative_strength`
  - `trend_stability`
  - `turnover_confirmation`
  - `industry_relative_strength`
  - `sector_relative_valuation` (uses `daily_basic.pe`/`pb`)
- A descriptor evaluator that ingests descriptor values plus forward returns and emits a standard quality report.
- CLI commands: `compute-descriptor`, `evaluate-descriptor`, `list-evaluation-reports`.
- JSON + Markdown output artifacts persisted under `output/descriptor_evaluation/`.

### Out of scope

- 5000-credit (fundamentals-driven) descriptors get registry stubs only; their compute body raises a clean "5000 credits required" error.
- Multi-factor portfolio backtesting (Stage 4).
- Residualization solvers and Brinson-style attribution (Stage 4).
- Factor auto-mining sandbox (Stage 3).
- Descriptor optimisation / parameter search.
- Modifying any code outside `src/alpha_find_v2/factor_evaluation/` and `cli.py`.

### Non-negotiable invariants

- Every descriptor compute function operates on PIT-safe inputs only. No row may be visible at trade date `t` if its `available_at > t`.
- The forward-return calculator uses `daily_bar_pit.open` from `t+1` to `t+1+H` (H = horizon days), already adjusted via `raw_adj_factor`. Net-of-cost calculation reuses the existing `cost_models/base_a_share_cash.toml`.
- A descriptor is excluded from a given trade date if its required data are not all green / present. Coverage is reported, never silently filled.
- Existing 249-test suite stays green. No schema in `market_data_bootstrap.py` or `data_ingest/` is touched.

## 2. User Stories and Acceptance Criteria

### R1. Researcher can compute a single descriptor's values across a window

**As a** quant researcher,
**I want** one command that produces a tidy table of descriptor values for a date range,
**so that** I can debug a descriptor's plumbing before running full evaluation.

#### EARS

- **R1.1** When the user runs `alpha-find-v2 compute-descriptor --id medium_term_relative_strength --start 20200101 --end 20240101 --research-db output/research_source.duckdb`, the system shall output a JSON dump of summary statistics (rows, security count, distinct trade dates, missing rate) and, on `--out PATH`, write a Parquet file with columns `trade_date, security_id, descriptor_value`.
- **R1.2** When the user supplies `--id <unregistered>`, the system shall exit with status 2 and print the registered descriptor ids.
- **R1.3** When the user supplies `--id <5000-credit-stub>`, the system shall exit with status 3 and print a message naming the missing prerequisite dataset (e.g. `pit_fina_indicator`).
- **R1.4** Compute output shall not contain any row whose `trade_date` is after the latest trade date present in `daily_bar_pit`.

### R2. Researcher can evaluate a descriptor with industry-standard metrics

**As a** quant researcher,
**I want** one command that produces all the metrics needed to judge a descriptor,
**so that** I do not have to wire IC, decile, monotonicity, and stability calculations by hand.

#### EARS

- **R2.1** When the user runs `alpha-find-v2 evaluate-descriptor --id medium_term_relative_strength --universe csi800 --start 20200101 --end 20240101 --horizons 5,20,60`, the system shall produce a `DescriptorEvaluationReport` containing, per horizon:
  - Pearson IC (mean, std, t-stat) cross-section by trade date
  - Spearman Rank IC (mean, std, t-stat)
  - IC_IR = mean / std
  - IC decay curve over horizons {1, 5, 10, 20, 40, 60} (when subset of requested horizons is present)
  - half-life of |IC|
  - decile long-short return (top decile minus bottom decile, equal-weighted, monthly compounded)
  - decile monotonicity Spearman correlation between decile rank and decile mean return
  - stability metric: Spearman rank correlation of cross-section ranks vs prior period (lag-1 autocorrelation of ranks)
  - coverage: rows used / rows possible per trade date (mean and worst)
  - one-period turnover: average |Δ rank| / N
- **R2.2** The system shall produce, in the same report, slice-stability tables grouped by:
  - SW2021 L1 industry (from `industry_classification_pit`)
  - market-cap tertile (low / mid / high based on daily_basic free_share × close)
  - reporting whether IC sign holds within each slice
- **R2.3** The system shall compute the cross-correlation matrix between the descriptor's score and at most 10 named other descriptors passed via `--correlation-against id1,id2,...`, using Pearson on raw values.
- **R2.4** When `--cost-model PATH` is supplied, the long-short return series shall be computed net of `base_a_share_cash` per-side cost (default), or the supplied cost model.
- **R2.5** Evaluation shall write `output/descriptor_evaluation/<descriptor_id>/<UTC-timestamp>/report.json` and `report.md`.
- **R2.6** Exit code is 0 when the report writes successfully even if the descriptor performs poorly. Exit code 1 only on infrastructure failures (missing data, failed compute).
- **R2.7** When the descriptor's effective coverage (rows used / rows possible) over the window is below 30%, the report shall include a top-level `low_coverage_warning` field; this is not a failure.

### R3. Researcher can list past evaluation reports

**As a** researcher,
**I want** to inspect history of evaluation runs,
**so that** I can compare descriptor versions over time.

#### EARS

- **R3.1** `alpha-find-v2 list-evaluation-reports` shall scan `output/descriptor_evaluation/` and print a JSON table sorted by descriptor id then by timestamp (most recent first), each row carrying: descriptor_id, timestamp, IC_IR for the primary horizon (default 20d), coverage mean, decile-LS return, status.
- **R3.2** `--id <descriptor_id>` shall filter to reports for that descriptor.

### R4. Compute registry is honest about descriptor implementation status

**As a** developer,
**I want** the system to reject use of unimplemented descriptors with a clear message,
**so that** I never silently consume a stub for something I expect to compute.

#### EARS

- **R4.1** The system shall ship a `DescriptorComputeRegistry` keyed by `descriptor_id`, mapping to either an implementation function or a stub that raises `DescriptorNotImplemented`.
- **R4.2** The registry shall contain compute implementations for the 5 in-scope descriptors and stubs (with names and the required dataset listed) for: `accrual_quality`, `profitability_quality`, `leverage_conservatism`, `estimate_revision_breadth`, `post_earnings_drift_signal`.
- **R4.3** Every shipped compute function must declare its required input tables (subset of `daily_bar_pit`, `daily_basic`, `adj_factor`, `industry_classification_pit`, `benchmark_membership_pit`).
- **R4.4** Calling a stub shall raise `DescriptorNotImplemented(descriptor_id=..., requires=[dataset_id, ...])` so the CLI can convert it to exit code 3 with a clear message.

### R5. PIT safety enforced by construction

**As a** quant researcher,
**I want** the evaluator to refuse any computation that could leak future information,
**so that** my IC numbers reflect honest predictability.

#### EARS

- **R5.1** Forward returns at trade date `t` shall use `daily_bar_pit.open` from `t+1` (entry) and from `t+1+H` (exit), where `H` is the requested horizon in trade-calendar days.
- **R5.2** A descriptor value at trade date `t` is valid only if every input row's `available_at` (or `trade_date` for daily series) is `<= t`. If an `available_at` column is unavailable, only the daily PIT series may be used.
- **R5.3** When entry row at `t+1` or exit row at `t+1+H` is suspended (`raw_suspend_d` shows S) or limit-locked at the open (using `raw_stk_limit` plus the existing `cn_a_directional_open_lock` rule), that observation shall be marked `tradeable=False` and excluded from IC by default; an `--include-untradeable` flag may include them in a separate "raw IC" series.
- **R5.4** A unit test shall sample 30 random `(security_id, trade_date)` pairs and assert no field involved in the descriptor or forward-return computation has `available_at > t` or `trade_date > t`.

### R6. Universe filtering matches the existing investable definition

**As a** researcher,
**I want** the universe filter to reuse the same definition the rest of V2 uses,
**so that** evaluation IC reflects what the production sleeve would see.

#### EARS

- **R6.1** When `--universe csi800` is supplied, the universe at each trade date `t` is exactly `benchmark_membership_pit` rows where `effective_at <= t < removed_at` (or `removed_at IS NULL`) and `benchmark_id = 'CSI 800'`.
- **R6.2** When `--universe investable_a_share_core` is supplied (default), the universe applies the existing definition encoded in mandate `a_share_long_only_eod`: A-share, listed ≥ 120 trade days, not ST, median 60-day turnover ≥ 50M CNY.
- **R6.3** When the universe is empty for a trade date, that date is omitted from IC computation (not zero-filled).
- **R6.4** The universe definition used shall be persisted in the evaluation report under `universe_definition`.

### R7. Reproducibility and persistence

**As a** researcher running periodic evaluations,
**I want** every report to record everything needed to reproduce it,
**so that** future me can diff results across descriptor versions.

#### EARS

- **R7.1** Every report shall include: descriptor_id, descriptor_version (sha256 of TOML + compute function source), evaluation start/end, universe id, horizons, cost_model_id, sample size, run_at UTC timestamp.
- **R7.2** Re-running the same command on the same database with the same descriptor source shall produce a byte-identical `report.json` (modulo `run_at`).
- **R7.3** Reports shall be stored under `output/descriptor_evaluation/<descriptor_id>/<run_at>/`.

### R8. Performance is acceptable on a personal machine

**As a** personal-stack user,
**I want** evaluation to finish in reasonable time,
**so that** I can iterate on descriptors without waiting overnight.

#### EARS

- **R8.1** Evaluation of one descriptor over CSI 800 for a 4-year window with horizons {5,20,60} shall complete in under 60 seconds on a developer laptop with the research DuckDB locally.
- **R8.2** The compute layer shall produce its full output in a single DuckDB query plus optional Pandas post-processing, not row-by-row Python loops.
- **R8.3** Memory peak for one descriptor evaluation shall stay below 2GB.

## 3. Cross-cutting Constraints

- **No silent NaN propagation**: every dropped observation must be counted in coverage.
- **Trade-calendar awareness**: `t + H` is `H` business days forward via `raw_trade_cal`, never `H` calendar days.
- **Reuse Stage 1 boundaries**: forward returns must only consume `output/research_source.duckdb` (or a path supplied by the user). Never `output/raw.duckdb` directly.
- **Cross-platform**: all Path I/O uses POSIX-style strings inside any TOML emitted, per the Windows backslash escape lessons learned in Stage 1.
- **Logging**: each compute call emits one structured log line: `descriptor=<id>`, `rows=<n>`, `dates=<n>`, `securities=<n>`, `duration_ms=<ms>`.

## 4. Open Questions for Design Phase

- **Q1** Whether `descriptor_version` should hash the full module file or only the named compute function. Default proposal: hash function source via `inspect.getsource`.
- **Q2** How to encode "universe = investable_a_share_core" without hard-coding mandate logic; default proposal: lift mandate constraint resolution into a small `UniverseResolver` class that the evaluator calls.
- **Q3** Whether decile L-S should be equal-weight or rank-weight; default proposal: equal-weight first, rank-weight as a `--weighting rank` opt-in flag.
- **Q4** Where to put forward-return materialisation. Default: compute on the fly inside the evaluator (single DuckDB SQL with LAG / LEAD over trade_date partitioned by security_id). Alternative: write a separate `forward_return_builder.py`. Decide in design.

## 5. Definition of Done

- All 8 user stories above pass automated tests.
- 5 in-scope descriptors compute and evaluate end-to-end against a small synthetic DuckDB fixture.
- Stub descriptors raise `DescriptorNotImplemented` with the missing-dataset list.
- `evaluate-descriptor --id medium_term_relative_strength --universe csi800 --start 20200101 --end 20240101` produces a JSON report whose schema is documented in design.md and validated by a fixture roundtrip.
- README's "Reference Examples" section gains a short Stage-2 example.
- Existing 249-test suite stays green; new tests bring the total above 280.
