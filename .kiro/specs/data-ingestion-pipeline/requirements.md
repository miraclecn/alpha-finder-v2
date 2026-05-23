# Data Ingestion Pipeline — Requirements

> Stage 1 of the V2 rebuild roadmap.
>
> Goal: An open-source user clones the repo, sets a `TUSHARE_TOKEN`, and runs three commands to obtain a PIT-safe DuckDB that the rest of the V2 research stack can consume.

## 1. Scope

### In scope

- Bootstrapping a fresh local data lake from public sources (Tushare primary, AKShare / Baostock fallback for basic price data only).
- Incremental refresh of the local data lake.
- Adapter abstraction so a new data source can be added without changing downstream code.
- Health auditing (missingness, PIT leak sampling, survivorship, adjustment-factor consistency, suspension coverage).
- Rewriting `README.md` "Quick Start" to remove the V1 path dependency.

### Out of scope

- Factor mining sandbox (Stage 3).
- Single-factor evaluator IC / IR / decile L-S (Stage 2).
- Attribution and reporting (Stage 4).
- Intraday / tick / level-2 data.
- Non-equity assets (futures, convertible bonds, funds).
- Backfilling V1 legacy factor / strategy tables.

### Non-negotiable invariants

- The PIT contract in `docs/data/v2-data-boundary-and-pit-audit.md` is binding: every record carries `available_at` and `ingested_at`; revisions never overwrite history silently.
- Existing schemas produced by `market_data_bootstrap.build_research_source_db` must remain unchanged. New ingestion code only writes `raw_*` tables that the existing bootstrap can consume.
- A-share execution realism stays first-class: T+1, suspension, limit-up / limit-down, lot size, liquidity.
- Surgical changes only. No speculative abstractions, no factor logic, no portfolio logic in this feature.

## 2. User Stories and Acceptance Criteria

### R1. First-time setup is a single command

**As an** open-source user who just cloned the repo,
**I want** to run one command that creates configuration scaffolding,
**so that** I can fill in my Tushare token without hunting for paths.

#### EARS

- **R1.1** When the user runs `alpha-find-v2 init` in a clean working tree, the system shall create:
  - `.env` (with `TUSHARE_TOKEN=` placeholder, comments documenting AKShare / Baostock fallback flags)
  - `config/data_sources.toml` (data source priority list, per-dataset enable flags, credit-tier hints)
  - `output/` directory (gitignored)
- **R1.2** When `.env` or `config/data_sources.toml` already exist, the system shall not overwrite them and shall print which files were skipped.
- **R1.3** The generated `config/data_sources.toml` shall mark every dataset that requires more than 2000 Tushare credits as `enabled = false` by default with a comment explaining the credit requirement.
- **R1.4** When `alpha-find-v2 init` exits, the user shall receive printed instructions on the next command to run.

### R2. From-scratch sync of all 2000-credit Tushare datasets

**As a** user with a valid 2000-credit Tushare token,
**I want** one command that pulls every dataset I am entitled to,
**so that** I can build the V2 research database without manual API stitching.

#### EARS

- **R2.1** When the user runs `alpha-find-v2 sync` and `TUSHARE_TOKEN` is set, the system shall pull, in dependency order, the following Tushare datasets and write each to a dedicated `raw_*` table inside `output/raw.duckdb`:
  - `stock_basic` (security master)
  - `trade_cal` (trade calendar)
  - `namechange` (name and ST history)
  - `daily` (unadjusted OHLCV)
  - `daily_basic` (turnover, valuation, market cap, free float)
  - `adj_factor` (adjustment factors)
  - `suspend_d` (suspension state)
  - `stk_limit` (daily price limits)
  - `index_daily` (index OHLCV)
  - `index_weight` (index weight snapshots)
  - `index_member_all` (CSI / SSE index historical constituents)
  - `index_classify` and `index_member` (SW2021 industry classification and historical members)
- **R2.2** When a Tushare API call fails with a permission error, the system shall stop only the affected dataset, mark it as `permission_denied` in the sync state, and continue with the remaining datasets.
- **R2.3** When a Tushare API call fails with a transient error (rate limit, network, 5xx), the system shall retry with exponential backoff at least 3 times before surfacing the error.
- **R2.4** When a 5000-credit dataset (e.g. `fina_indicator`, `income`, `balancesheet`, `cashflow`, `forecast`, `express`) is enabled in `config/data_sources.toml` but the token lacks permission, the system shall log a clear warning naming the dataset and the credit requirement, and shall not abort the run.
- **R2.5** Every row written to a `raw_*` table shall include `ingested_at` and a `source` column identifying the adapter that produced it.
- **R2.6** Sync shall be idempotent: running `sync` twice in a row on a frozen market shall not change the row count of any `raw_*` table.

### R3. Incremental sync

**As a** user maintaining a local database,
**I want** subsequent runs to fetch only new data,
**so that** daily refreshes finish in minutes, not hours.

#### EARS

- **R3.1** The system shall maintain a `dataset_sync_state` table inside `output/raw.duckdb` recording, per dataset: `dataset_id`, `last_trade_date`, `last_period_end`, `last_run_at`, `last_status`, `error_message`.
- **R3.2** When `alpha-find-v2 sync` runs and `dataset_sync_state` already has a successful prior run for a dataset, the system shall request only data with `trade_date > last_trade_date` (or `period_end > last_period_end` for fundamental datasets).
- **R3.3** A user can force a full re-pull of a single dataset with `alpha-find-v2 sync --reset stock_basic` (multi-value).
- **R3.4** A user can restrict sync to a single dataset with `alpha-find-v2 sync --only daily`.
- **R3.5** A user can restrict sync to a date range with `alpha-find-v2 sync --since 20240101 --until 20241231`.

### R4. Adapter abstraction with deterministic fallback

**As a** developer who wants to test the pipeline without paying Tushare credits,
**I want** a clean adapter interface and an AKShare / Baostock fallback for basic price data,
**so that** the smoke test of the project does not require a paid token.

#### EARS

- **R4.1** The system shall define a `DataSourceAdapter` protocol with a single fetch method per dataset, returning a normalized record stream (dicts with declared schema).
- **R4.2** The system shall ship three adapter implementations:
  - `TushareAdapter` (full coverage of the datasets in R2.1, plus 5000-credit ones gated behind config flags)
  - `AKShareAdapter` (covers `daily` and `index_daily` only)
  - `BaostockAdapter` (covers `daily` and `index_daily` only)
- **R4.3** When the user has no `TUSHARE_TOKEN` and runs `alpha-find-v2 sync`, the system shall fall back to AKShare for the supported subset, and emit a banner stating the fallback is for demo only and is not promotion-safe.
- **R4.4** When `config/data_sources.toml` declares a per-dataset priority list, the system shall try adapters in declared order and record the chosen adapter in the `source` column.
- **R4.5** Adapters shall not import each other; each is independent and skipped if its optional dependency is missing.

### R5. Data audit produces a defensible report

**As a** researcher,
**I want** a single command to validate that my local database is fit for V2 research,
**so that** I can detect PIT leaks or coverage gaps before they corrupt downstream signals.

#### EARS

- **R5.1** When the user runs `alpha-find-v2 audit-data`, the system shall produce a JSON report and a Markdown summary in `output/audit/<timestamp>/`.
- **R5.2** The audit shall verify, for every `raw_*` table:
  - row count, distinct security count, distinct trade-date count
  - missingness rate per critical column, sliced by year
- **R5.3** The audit shall verify forward consistency:
  - `daily.close * adj_factor` matches the implied adjusted price within tolerance
  - `stk_limit` rows exist for every `daily` row after a known cutoff date
  - `suspend_d` covers every business date in `trade_cal`
- **R5.4** The audit shall verify survivorship: at least one delisted name in `stock_basic` resolves with `delist_date != NULL` and still has historical `daily` rows.
- **R5.5** The audit shall sample at least 10 random `(security_id, trade_date)` pairs and assert that no field has `available_at > trade_date` (PIT leak check).
- **R5.6** The audit shall return a non-zero exit code when any check classified as `blocking` fails, and exit 0 with warnings when only `advisory` checks fail.

### R6. README Quick Start is reproducible from a fresh clone

**As a** new contributor,
**I want** README to walk me from `git clone` to a populated DuckDB without referencing legacy paths,
**so that** I can verify the project works in isolation.

#### EARS

- **R6.1** The new Quick Start section shall remove every reference to `/home/nan/alpha-find` and to `stock_data_audited.duckdb`.
- **R6.2** The new Quick Start shall list, in order: `pip install -e .`, `alpha-find-v2 init`, edit `.env`, `alpha-find-v2 sync`, `alpha-find-v2 build-research-source-db`, `alpha-find-v2 audit-data`.
- **R6.3** The new Quick Start shall declare the minimum Tushare credit tier (2000) and what becomes available at 5000.
- **R6.4** Existing example commands referencing artifacts in `research/examples/` shall remain in a separate "Reference Examples" section.

### R7. 5000-credit datasets are first-class but optional

**As a** user who later upgrades to 5000 Tushare credits,
**I want** to flip a single config flag and re-run sync to get fundamental data,
**so that** I do not have to wait for code changes.

#### EARS

- **R7.1** `config/data_sources.toml` shall declare `[datasets.fina_indicator]`, `[datasets.income]`, `[datasets.balancesheet]`, `[datasets.cashflow]`, `[datasets.forecast]`, `[datasets.express]` with `enabled = false`.
- **R7.2** When the user sets `enabled = true` and re-runs `alpha-find-v2 sync`, the system shall attempt those datasets through the Tushare adapter, with the same fallback / error handling as R2.2 / R2.3 / R2.4.
- **R7.3** The system shall not silently consume a successful 5000-credit pull into a partially upgraded database; if any prerequisite (e.g. `stock_basic`) is missing, the run shall fail fast with a clear message.

## 3. Cross-cutting Constraints

- **PIT timestamps**: every dataset adapter must return both `ingested_at` (now) and, where the upstream provides them, `published_at` / `available_at`. When unavailable, the adapter shall not invent them; the downstream `market_data_bootstrap` is responsible for conservative fallback rules already documented in `docs/data/v2-data-boundary-and-pit-audit.md`.
- **Determinism**: given the same upstream API responses, two `sync` runs shall produce byte-identical `raw_*` tables (modulo `ingested_at`).
- **No silent schema drift**: if an upstream API column changes, the adapter shall fail loudly rather than coerce. Schemas are versioned in `config/data_sources.toml`.
- **Logging**: every adapter call shall emit one structured log line (`dataset`, `adapter`, `mode` (full/incremental), `rows`, `duration_ms`, `status`).
- **Cross-platform**: Windows, macOS, Linux. No POSIX-only assumptions, no shell-out for parsing.

## 4. Risks and Open Questions

- **R-Q1** Tushare rate limits vary by interface and credit tier. The design phase needs to settle on a unified rate-limiter (per-minute and per-day buckets) and document the conservative defaults.
- **R-Q2** AKShare / Baostock daily data have different field names and slightly different adjustment conventions. The design must specify an exact normalization contract before adapter coding starts.
- **R-Q3** `index_member_all` and `index_member` (SW2021) have different paging semantics. The design must clarify whether intervals are stored raw or pre-merged.
- **R-Q4** Whether `dataset_sync_state` lives in `output/raw.duckdb` or in a separate sidecar. Single-file is simpler; sidecar is friendlier to backups. Default proposal: same file, separate schema namespace.

## 5. Definition of Done

- All EARS items above pass automated tests in `tests/`.
- `alpha-find-v2 init && alpha-find-v2 sync --since 20240101 --only stock_basic,trade_cal,daily && alpha-find-v2 audit-data` completes on a clean machine with only `TUSHARE_TOKEN` set, in under 10 minutes for a 1-year window.
- README Quick Start reproduces the above on macOS, Linux, and Windows.
- The existing 74-test pytest suite stays green; no schemas in `market_data_bootstrap.py` are modified.
