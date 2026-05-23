# Implementation Plan: Data Ingestion Pipeline

## Overview

Implementation of the data ingestion pipeline for alpha-find-v2. Enables open-source users to bootstrap a PIT-safe DuckDB from Tushare (primary) or AKShare/Baostock (fallback) with a single `alpha-find-v2 sync` command, replacing the V1 audited DuckDB dependency.

## Tasks

- [x] 1. Create package scaffold and DDL constants in schemas.py
  - Create `src/alpha_find_v2/data_ingest/__init__.py` (empty package marker)
  - Create `src/alpha_find_v2/data_ingest/schemas.py` with `RAW_TABLE_DDL: dict[str, str]` mapping each dataset_id to its `CREATE TABLE IF NOT EXISTS` DDL
  - Column types and order must match existing `source.*` references in `market_data_bootstrap.py` and fixture schemas in `tests/test_market_data_bootstrap.py::_create_source_db`
  - Every raw table includes `source_table VARCHAR` and `ingested_at TIMESTAMP`
  - Add `META_DDL` for `meta.dataset_sync_state` per design §2.4
  - Export `DATASET_PRIMARY_KEYS: dict[str, tuple[str, ...]]` and `DATASET_INCREMENTAL_AXIS: dict[str, str]`
  - **Verification**: unit test imports `RAW_TABLE_DDL` and asserts all 18 dataset ids present; each DDL parses cleanly in an in-memory duckdb connection; every primary-key column exists in its table DDL
  - _Requirements: R2.1, R2.5_

- [x] 2. Implement config_models.py for data_sources.toml parsing
  - Define `AdapterConfig`, `DatasetConfig`, `DataSourcesConfig` frozen dataclasses per design §3.1
  - `DataSourcesConfig.priority(dataset_id)` returns enabled adapters in priority order
  - `load_data_sources_config(path: Path) -> DataSourcesConfig` using `tomllib`
  - Validation: schema_version==1, every priority adapter exists, credit_tier in {120,2000,5000}
  - Create `src/alpha_find_v2/data_ingest/templates/data_sources.toml.template` with all 18 datasets; 5000-credit ones default `enabled = false`
  - **Verification**: unit test loads packaged template; expects 18 datasets, 3 adapters; malformed config raises `ValueError` with helpful message; 5000-credit datasets default disabled
  - _Requirements: R1.3, R7.1_

- [x] 3. Implement rate_limiter.py token-bucket
  - `TokenBucket(rate_per_minute: int, daily_cap: int = 0)` class
  - `acquire(timeout: float | None = None) -> None` blocks until token available; raises `RateLimitTimeout` if timeout exceeded
  - `record_call()` decrements daily counter; `daily_exhausted() -> bool`
  - Thread-safe via `threading.Lock`; injectable monotonic clock for tests
  - **Verification**: unit test 60/min limiter with fake clock; PBT: across 1000 calls with random bursts, observed rate never exceeds configured rate; daily_cap=10 blocks after 10 calls
  - _Requirements: R2.3_

- [x] 4. Define DataSourceAdapter protocol in adapters/base.py
  - `DataSourceAdapter(Protocol)` with `name: str`, `supports(dataset_id) -> bool`, `fetch(...) -> Iterator[dict]`
  - `DatasetSpec` dataclass with four typed fields per design §3.1
  - `STATIC_SPECS: dict[str, DatasetSpec]` derived from task 1 constants
  - Exception types: `AdapterPermissionError`, `AdapterRateLimitError`, `AdapterSchemaMismatchError`, `AdapterUnavailable`
  - **Verification**: `STATIC_SPECS` covers all 18 dataset ids; no-op adapter satisfies protocol via `runtime_checkable`
  - _Requirements: R4.1_
  - _Dependencies: 1_

- [x] 5. Implement TushareAdapter for price and reference datasets
  - File: `src/alpha_find_v2/data_ingest/adapters/tushare_adapter.py`
  - Reuse `load_tushare_token` and `_build_tushare_client` from `reference_data_staging.py`
  - Implement `fetch` for: `stock_basic`, `trade_cal`, `namechange`, `daily`, `daily_basic`, `adj_factor`, `daily_qfq` (pro_bar qfq), `suspend_d`, `stk_limit`, `index_daily`
  - For `index_weight` and `index_member_all`: thin wrapper yielding rows from `build_tushare_reference_db`; do not duplicate paging logic
  - Each fetch sets `source_table = "tushare.<api_name>"` and `ingested_at = datetime.utcnow()`
  - Permission failures caught and re-raised as `AdapterPermissionError`; schema mismatch raises `AdapterSchemaMismatchError`
  - **Verification**: unit test per dataset with fake pro_api client returning canned DataFrame; permission error wraps to AdapterPermissionError; missing primary-key column raises AdapterSchemaMismatchError
  - _Requirements: R2.1, R2.2_
  - _Dependencies: 4_

- [x] 6. Implement TushareAdapter fundamentals (5000-credit gated)
  - Add fetch implementations for `fina_indicator`, `income`, `balancesheet`, `cashflow`, `forecast`, `express` in same file as task 5
  - `fina_indicator` writes to table name `pit_fina_indicator` (matches existing market_data_bootstrap consumption)
  - Uses `period_end` as incremental axis
  - `AdapterPermissionError` raised cleanly when token tier insufficient
  - **Verification**: unit test valid fina_indicator row produces dict with pit_fina_indicator schema; 5000-credit denial wraps cleanly
  - _Requirements: R7.2_
  - _Dependencies: 5_

- [x] 7. Implement AKShareAdapter
  - File: `src/alpha_find_v2/data_ingest/adapters/akshare_adapter.py`
  - Lazy import of `akshare`; raises `AdapterUnavailable` (not ImportError) if missing
  - Supports `daily` and `index_daily` only; `supports()` returns False for everything else
  - Maps AKShare fields to Tushare-shaped rows per design §2.2; `source_table = "akshare.stock_zh_a_hist"` etc.
  - Converts date format and exchange suffix to Tushare `{symbol}.{SH|SZ|BJ}` convention
  - **Verification**: unit test fake akshare frame → Tushare-shaped dict; PBT: random AKShare frames map deterministically to Tushare-shaped rows with round-trip identity for shared fields
  - _Requirements: R4.2, R4.3_
  - _Dependencies: 4_

- [x] 8. Implement BaostockAdapter
  - File: `src/alpha_find_v2/data_ingest/adapters/baostock_adapter.py`
  - Lazy import; `AdapterUnavailable` if missing
  - Supports `daily` and `index_daily` only
  - Manages baostock `login()` / `logout()` session internally; single-threaded only
  - **Verification**: unit test fake baostock login → yielded rows match schema; missing baostock module raises `AdapterUnavailable` not `ImportError`
  - _Requirements: R4.2_
  - _Dependencies: 4_

- [x] 9. Implement sync state I/O in orchestrator.py
  - File: `src/alpha_find_v2/data_ingest/orchestrator.py`
  - `_ensure_meta_schema(conn)` runs `META_DDL` from schemas.py
  - `DatasetSyncState` dataclass mirroring `meta.dataset_sync_state` columns
  - `_load_state(conn) -> dict[str, DatasetSyncState]` and `_record_state(conn, state)`
  - **Verification**: unit test write state, read back, assert equality across all fields including timestamps
  - _Requirements: R3.1_
  - _Dependencies: 1, 2, 4_

- [x] 10. Implement dispatcher and retry in orchestrator.py
  - `sync(*, raw_db_path, config, only, reset, since, until) -> SyncReport` entry point per design §3.2
  - `_with_retries(call, max_attempts=3, base_delay=2.0)`: retries on `AdapterRateLimitError` and network `OSError` only; never retries `AdapterPermissionError`
  - Per-dataset transactional write: `BEGIN; DELETE WHERE pk IN staging; INSERT FROM staging; COMMIT`
  - 50,000-row streaming batches
  - Returns `SyncReport` with `DatasetSyncResult` per dataset
  - **Verification**: in-memory adapter yielding 100001 rows → two batches, exact row count; `AdapterRateLimitError` once then success → `success` status after 1 retry; `AdapterPermissionError` not retried → `permission_denied`; mid-stream exception rolls back, row count unchanged
  - _Requirements: R2.2, R2.3, R2.6_
  - _Dependencies: 9_

- [x] 11. Implement incremental and reset semantics in orchestrator.py
  - Compute effective `since` per dataset: `max(state.last_trade_date + 1day, cli.since)` for trade_date axis; `max(state.last_period_end + 1day, cli.since)` for period_end axis; full pull when state missing or dataset in `--reset`
  - `--only` restricts execution to listed dataset ids
  - `--reset` clears `meta.dataset_sync_state` rows and corresponding raw tables for listed datasets before sync
  - **Verification**: state has `last_trade_date=20240105`, second sync requests `since=20240106`; `--reset daily` deletes only `daily` state row and `raw_kline_unadj` table
  - _Requirements: R3.2, R3.3, R3.4, R3.5_
  - _Dependencies: 10_

- [x] 12. Implement fallback chain in orchestrator.py
  - Walk `DataSourcesConfig.priority(dataset_id)` adapter list; stop at first adapter that `supports()` and does not raise `AdapterUnavailable`/`AdapterPermissionError`
  - On `AdapterPermissionError` mid-list: log and try next adapter
  - Record final adapter in `state.adapter` and in every row's `source_table`
  - When no adapter succeeds: dataset status `failed` with error message naming each attempt
  - **Verification**: priority `[tushare, akshare]`, Tushare raises `AdapterPermissionError`, AKShare yields rows → state.adapter==`akshare`, all rows source_table starts with `akshare.`; all three adapters fail → status `failed` with message listing three attempts
  - _Requirements: R4.4_
  - _Dependencies: 10, 5, 7, 8_

- [x] 13. Implement init_workspace.py
  - `init_workspace(workspace: Path) -> InitReport`
  - Bundle `.env.template` and `data_sources.toml.template` inside the package via `importlib.resources`
  - Write `.env` from template if missing; write `config/data_sources.toml` from template if missing; create `output/.gitkeep`
  - Return list of (path, action) where action is `"created"` or `"skipped"` for already-present files
  - **Verification**: in tempdir, init creates 3 expected paths; second call reports all 3 as `skipped`; generated `data_sources.toml` parses cleanly; 5000-credit datasets have `enabled = false`
  - _Requirements: R1.1, R1.2, R1.3, R1.4_
  - _Dependencies: 2_

- [x] 14. Implement audit.py and check registry
  - `run_audit(*, raw_db_path, out_dir, blocking_checks=None) -> AuditReport`
  - `AuditCheck(id, severity, run)` dataclass; severity is `"blocking"` or `"advisory"`
  - Blocking checks: `pit_leak_sample` (available_date <= trade_date), `adj_factor_consistency` (close_qfq ≈ close_unadj × adj_factor/latest_adj_factor within 1%), `trade_calendar_coverage`
  - Advisory checks: `missingness_by_year`, `survivorship_delisted_present`, `suspend_coverage`, `stk_limit_coverage`
  - Write `output/audit/<UTC-timestamp>/audit.json` and `audit.md`
  - Return exit status `ok` or `blocking_failure`; CLI maps to exit code 0/1
  - **Verification**: fixture raw.duckdb with normal data → all checks pass; injected PIT leak row → `pit_leak_sample` fails; PBT: random consistent daily+adj_factor rows always pass consistency check; one mutated row makes it fail; audit.md has one table row per registered check
  - _Requirements: R5.1, R5.2, R5.3, R5.4, R5.5, R5.6_
  - _Dependencies: 11_

- [x] 15. Wire init / sync / audit-data subcommands in cli.py
  - Add three new subparsers to `_parse_args` per design §5: `init`, `sync`, `audit-data`
  - Add three new branches in `main`: `init` calls `init_workspace`, prints `InitReport` JSON; `sync` calls `sync(...)`, prints `SyncReport` JSON, exits 0 always but logs failed count to stderr; `audit-data` calls `run_audit`, prints `AuditReport` JSON, exits 1 if blocking failure
  - Do not modify any existing branch
  - **Verification**: smoke test `python -m alpha_find_v2 init --workspace <tmp>` exits 0 and creates expected files; `sync --dry-run` exits 0 with plan JSON and zero API calls; `audit-data --raw-db <bad-fixture>` exits 1 on blocking failure; existing 74 tests still pass
  - _Requirements: R1.1, R2.1, R5.1, R6.2_
  - _Dependencies: 12, 13, 14_

- [x] 16. Write end-to-end smoke test in tests/test_data_ingest_smoke.py
  - In-memory adapter fixture yielding canned rows for 3 securities, 30 trading days
  - Steps: `init_workspace` → `sync` full → `sync` incremental (assert no new rows) → `build_research_source_db(raw_db, target_db)` → assert registry rows → `run_audit` all checks pass
  - Mark with `pytest.mark.smoke`; include PBT property with randomized rows
  - Test must complete under 10 seconds; existing 74 tests still pass
  - **Verification**: all five steps complete without exception; second sync adds zero rows; `build_research_source_db` output matches dataset_registry expectations from `test_market_data_bootstrap.py`
  - _Requirements: R2.6, R3.2, R5.1_
  - _Dependencies: 15_

- [x] 17. Rewrite README Quick Start section
  - Remove every reference to `/home/nan/alpha-find` and `stock_data_audited.duckdb`
  - New Quick Start flow: `pip install -e .` → `alpha-find-v2 init` → edit `.env` → `alpha-find-v2 sync` → `alpha-find-v2 build-research-source-db --source-db output/raw.duckdb --target-db output/research_source.duckdb` → `alpha-find-v2 audit-data`
  - Add "Tushare credit tiers" callout: 2000 covers Stage 1 dataset list; 5000 unlocks fundamentals
  - Move existing example commands under a "Reference Examples" subsection
  - **Verification**: manual walkthrough on clean Windows shell; no broken markdown headings; old example commands remain accessible under Reference Examples
  - _Requirements: R6.1, R6.2, R6.3, R6.4_
  - _Dependencies: 15_

## Notes

- `market_data_bootstrap.py` and `reference_data_staging.py` are not modified. New code only adds `src/alpha_find_v2/data_ingest/` and three new CLI subcommands.
- The boundary contract with existing code is the schema of `raw_*` tables. Task 1 DDL must be audited against existing SQL before task 5 begins.
- AKShare and Baostock adapters use lazy imports; missing optional dependencies raise `AdapterUnavailable`, not `ImportError`, so they are skippable without breaking the run.
- PBT tasks: 3 (rate limiter), 7 (AKShare field mapping), 14 (adj_factor consistency), 16 (E2E with randomised rows).
- All path-bearing values use POSIX format in TOML to avoid the Windows backslash TOML escape issue already encountered in existing tests.
