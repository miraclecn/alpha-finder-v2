# Data Ingestion Pipeline — Design

> Companion to `requirements.md`. This document fixes the open questions
> R-Q1 through R-Q4 and lays out concrete module boundaries, table schemas,
> and contracts so implementation tasks can be cut from it.

## 1. Architectural Decisions

### 1.1 Where the new code lives

A single new package: `src/alpha_find_v2/data_ingest/`. No code outside this
package is modified except for additions to `cli.py` and a new entry in
`pyproject.toml`. `market_data_bootstrap.py` is not touched.

```
src/alpha_find_v2/data_ingest/
├── __init__.py
├── adapters/
│   ├── __init__.py
│   ├── base.py            # DataSourceAdapter protocol + record dataclasses
│   ├── tushare_adapter.py
│   ├── akshare_adapter.py
│   └── baostock_adapter.py
├── orchestrator.py        # sync planner + dispatcher + sync_state I/O
├── rate_limiter.py        # token-bucket per adapter+endpoint
├── schemas.py             # raw_* table DDL constants (single source of truth)
├── audit.py               # audit-data implementation
├── init_workspace.py      # init command implementation
└── config_models.py       # parsed view of config/data_sources.toml
```

### 1.2 Boundary with existing code

The contract between this feature and the existing pipeline is **the schema of
`raw_*` tables in a DuckDB file**. Specifically, the new `sync` command must
write tables whose columns match what `market_data_bootstrap.build_research_source_db`
reads (verified by the grep above):

| Table written by sync       | Read by `market_data_bootstrap` as |
|-----------------------------|-------------------------------------|
| `stock_basic_ref`           | `source.stock_basic_ref` (primary security master) |
| `raw_namechange`            | `source.raw_namechange` |
| `raw_daily_basic`           | `source.raw_daily_basic` (primary trade calendar source) |
| `raw_kline_unadj`           | `source.raw_kline_unadj` |
| `raw_kline_qfq`             | `source.raw_kline_qfq` |
| `raw_adj_factor`            | `source.raw_adj_factor` |
| `pit_fina_indicator`        | `source.pit_fina_indicator` (5000-credit gated) |

To keep the existing test suite green, `output/raw.duckdb` is fed directly to
`build-research-source-db --source-db output/raw.duckdb`. Reference tables
(`industry_classification_pit`, `benchmark_membership_pit`,
`benchmark_weight_snapshot_pit`) keep going through the existing
`build-reference-staging-db` command, which is already wired through
`reference_data_staging.py`.

This decision means the new sync command produces a **drop-in replacement
for the V1 audited DuckDB**, not a different shape. No downstream code change.

### 1.3 Datasets in scope for Stage 1

| Dataset id            | Source table written       | Tushare API        | Credit | Default enabled |
|-----------------------|----------------------------|--------------------|--------|-----------------|
| `stock_basic`         | `stock_basic_ref`          | `stock_basic`      | 120    | yes |
| `trade_cal`           | `raw_trade_cal`            | `trade_cal`        | 120    | yes |
| `namechange`          | `raw_namechange`           | `namechange`       | 120    | yes |
| `daily`               | `raw_kline_unadj`          | `daily`            | 120    | yes |
| `daily_basic`         | `raw_daily_basic`          | `daily_basic`      | 120    | yes |
| `adj_factor`          | `raw_adj_factor`           | `adj_factor`       | 2000   | yes |
| `daily_qfq`           | `raw_kline_qfq`            | `pro_bar(adj=qfq)` | 2000   | yes |
| `suspend_d`           | `raw_suspend_d`            | `suspend_d`        | 2000   | yes |
| `stk_limit`           | `raw_stk_limit`            | `stk_limit`        | 2000   | yes |
| `index_daily`         | `raw_index_daily`          | `index_daily`      | 2000   | yes |
| `index_weight`        | `raw_index_weight`         | `index_weight`     | 2000   | yes (via existing reference staging) |
| `index_member_all`    | `raw_index_member_all`     | `index_member_all` | 2000   | yes (via existing reference staging) |
| `fina_indicator`      | `pit_fina_indicator`       | `fina_indicator`   | 5000   | **no** |
| `income`              | `raw_income`               | `income`           | 5000   | **no** |
| `balancesheet`        | `raw_balancesheet`         | `balancesheet`     | 5000   | **no** |
| `cashflow`            | `raw_cashflow`             | `cashflow`         | 5000   | **no** |
| `forecast`            | `raw_forecast`             | `forecast`         | 5000   | **no** |
| `express`             | `raw_express`              | `express`          | 5000   | **no** |

`raw_trade_cal`, `raw_suspend_d`, `raw_stk_limit`, `raw_index_daily` are new
tables that `market_data_bootstrap` does not yet consume. They are written
because R5 (audit) needs them and Stage 2 will. They are inert for Stage 1
downstream code.

`raw_daily_basic` remains the trade-calendar driver inside
`market_data_bootstrap` because the existing test
`test_build_research_source_db_materializes_green_and_amber_tables` asserts
that. Sync writes both `raw_trade_cal` (canonical) and `raw_daily_basic` (legacy
driver). They must agree.

## 2. Settling the Open Questions

### 2.1 R-Q1 Rate limiting

**Decision.** Token-bucket limiter with two layers, per adapter:

- per-minute bucket sized from `config/data_sources.toml` (default Tushare:
  490 calls/min, leaving headroom under the 500 cap of most tiers)
- per-day bucket disabled by default; when set, blocks further calls and
  marks remaining datasets as `deferred` instead of `failed`

The limiter lives in `rate_limiter.py` and is injected into adapters by the
orchestrator. AKShare and Baostock adapters use their own conservative limits
(60/min) but share the same limiter class.

### 2.2 R-Q2 AKShare / Baostock normalization

**Decision.** Both fallback adapters emit the **Tushare-shaped row**, not their
native shape. Specifically, `daily` rows must have columns:
`ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg,
vol, amount, source_table, ingested_at`.

`source_table` carries provenance like `akshare.stock_zh_a_hist` or
`baostock.k_data` so downstream audit can isolate fallback rows. The
adapter maps `bj` / `sh` / `sz` exchange suffixes to Tushare's
`{symbol}.{SH|SZ|BJ}` convention.

Adjustment factors do **not** have a free fallback. `adj_factor` is Tushare-only.
When AKShare/Baostock is the price source, adapter writes `raw_kline_qfq` only
(AKShare's `qfq=` directly) and emits a warning that unadjusted prices and
adj_factor are missing, which downgrades `market_data_bootstrap` to its
existing `qfq_fallback` path (already tested by
`test_build_research_source_db_uses_qfq_fallback_when_unadjusted_bar_is_missing`).

### 2.3 R-Q3 `index_member_all` and `index_member` paging

**Decision.** Existing `reference_data_staging.py` already handles paging and
interval merging for these. Stage 1 only adds a thin wrapper command alias and
moves the orchestration so `sync` calls into `build_tushare_reference_db`
internally. No re-implementation. The existing 2000-credit behavior is kept.

### 2.4 R-Q4 Where `dataset_sync_state` lives

**Decision.** Same DuckDB file, separate schema namespace `meta`. So the table
is `meta.dataset_sync_state`. Backups still capture state. Detached tooling
that reads only `main.*` tables is unaffected. Schema:

```sql
CREATE TABLE meta.dataset_sync_state (
    dataset_id        VARCHAR PRIMARY KEY,
    adapter           VARCHAR NOT NULL,
    last_trade_date   VARCHAR,         -- YYYYMMDD
    last_period_end   VARCHAR,         -- YYYYMMDD, fundamentals only
    last_run_at       TIMESTAMP NOT NULL,
    last_status       VARCHAR NOT NULL,  -- 'success'|'partial'|'failed'|'permission_denied'|'deferred'
    last_row_count    BIGINT NOT NULL DEFAULT 0,
    error_message     VARCHAR,
    schema_version    INTEGER NOT NULL DEFAULT 1
)
```

## 3. Component Design

### 3.1 `adapters/base.py`

```python
class RawRecord(TypedDict, total=False):
    # Empty marker; concrete shape per dataset is enforced by schemas.py.
    ...

class DatasetSpec(TypedDict):
    dataset_id: str          # canonical key, e.g. "daily"
    raw_table: str           # destination table name
    primary_keys: tuple[str, ...]
    pit_columns: tuple[str, ...]   # which columns get available_at logic
    incremental_axis: Literal["trade_date", "period_end", "static"]

class DataSourceAdapter(Protocol):
    name: str                # "tushare" | "akshare" | "baostock"

    def supports(self, dataset_id: str) -> bool: ...

    def fetch(
        self,
        dataset_id: str,
        *,
        since: str | None,
        until: str | None,
        full: bool,
    ) -> Iterator[dict[str, Any]]:
        """Yield rows already conforming to schemas.py for `dataset_id`.
        Adapter sets `source_table` and `ingested_at`; orchestrator persists.
        """
```

Adapters never write to DuckDB themselves. They only stream rows. The
orchestrator owns persistence so retry / partial-write behavior is uniform.

### 3.2 `orchestrator.py`

Public entry points:

```python
def sync(
    *,
    raw_db_path: Path,
    config: DataSourcesConfig,
    only: set[str] | None = None,
    reset: set[str] | None = None,
    since: str | None = None,
    until: str | None = None,
) -> SyncReport: ...
```

Algorithm:

1. Resolve dataset execution order from a static dependency graph
   (`stock_basic` first, then anything that joins on `ts_code`).
2. For each dataset:
   - Check `meta.dataset_sync_state`; compute effective `since` per R3.2.
   - Pick adapter via `config.priority(dataset_id)`; honor `--only` / `--reset`.
   - Call `adapter.fetch(...)` inside `_with_retries` (R2.3).
   - Stream rows in 50k-row batches into a staging table, then
     `INSERT ... ON CONFLICT DO NOTHING` (DuckDB does not support upsert; we use
     `DELETE ... WHERE (pk) IN (SELECT pk FROM staging) ; INSERT ...` inside a
     single transaction so step is atomic).
   - Update `meta.dataset_sync_state` with status and row count.
3. Emit one `SyncReport` JSON line per dataset to stdout, plus a summary at the
   end.

`SyncReport`:

```python
@dataclass
class DatasetSyncResult:
    dataset_id: str
    adapter: str
    rows_added: int
    duration_seconds: float
    status: Literal["success", "partial", "failed", "permission_denied", "deferred", "skipped"]
    error_message: str | None

@dataclass
class SyncReport:
    raw_db_path: str
    started_at: str
    finished_at: str
    results: list[DatasetSyncResult]
```

### 3.3 `init_workspace.py`

`alpha-find-v2 init` writes three artifacts only if absent:

- `.env` from a packaged template containing `TUSHARE_TOKEN=`,
  `AKSHARE_FALLBACK=true`, `BAOSTOCK_FALLBACK=false`.
- `config/data_sources.toml` from a packaged template; uses the dataset
  registry above; 5000-credit datasets default `enabled = false`.
- `output/.gitkeep` if `output/` is missing.

Already-present files are left untouched and reported in stdout summary.

### 3.4 `audit.py`

Single entry point:

```python
def run_audit(
    *,
    raw_db_path: Path,
    out_dir: Path,
    blocking_checks: set[str] | None = None,
) -> AuditReport: ...
```

Each check is a small dataclass with fields `id`, `severity`
(`"blocking"|"advisory"`), `result`, `details`. Default `blocking` set:

- `pit_leak_sample` — randomly sample 10 `(ts_code, trade_date)` pairs in
  `pit_fina_indicator` (when present), assert `available_date <= trade_date`
- `adj_factor_consistency` — for 50 random rows, assert
  `abs(close_qfq - close_unadj * (adj_factor / latest_adj_factor)) / close_unadj < 0.01`
- `trade_calendar_coverage` — `raw_trade_cal` open dates equal distinct
  `trade_date` in `raw_kline_unadj` (modulo trading-suspended whole-market days)

Default `advisory` set:

- `missingness_by_year`
- `survivorship_delisted_present`
- `suspend_coverage`
- `stk_limit_coverage`

Output: `output/audit/<UTC-timestamp>/audit.json` (machine-readable),
`audit.md` (human summary table). Exit code: `0` if no blocking failures; `1`
otherwise.

## 4. Configuration File Format

### 4.1 `config/data_sources.toml`

```toml
schema_version = 1

[adapter.tushare]
enabled = true
calls_per_minute = 490
calls_per_day = 0          # 0 = unlimited

[adapter.akshare]
enabled = true             # only used as fallback when tushare absent/blocked
calls_per_minute = 60

[adapter.baostock]
enabled = false
calls_per_minute = 60

# Dataset entries declare credit tier and adapter priority list.
[datasets.stock_basic]
enabled = true
credit_tier = 120
priority = ["tushare"]

[datasets.daily]
enabled = true
credit_tier = 120
priority = ["tushare", "akshare", "baostock"]

# ... one entry per dataset in section 1.3 ...

[datasets.fina_indicator]
enabled = false            # requires 5000 Tushare credits
credit_tier = 5000
priority = ["tushare"]
```

The runtime `DataSourcesConfig` validates that `priority` references only
adapters with `enabled = true`, otherwise it falls back to the next in the
list.

### 4.2 `.env` template

```
# Tushare token. Obtain from https://tushare.pro/user/token.
# 2000 credits enables the default Stage 1 dataset list.
# 5000 credits unlocks fina_indicator / income / balancesheet / cashflow / forecast / express.
TUSHARE_TOKEN=

# AKShare fallback (free, daily prices only). Used when TUSHARE_TOKEN is empty
# or a dataset is missing from Tushare.
AKSHARE_FALLBACK=true

# Baostock fallback (free, daily prices only).
BAOSTOCK_FALLBACK=false
```

## 5. CLI Surface (additions only)

Three new subcommands wired into `cli.py`. No existing command is renamed or
removed.

```
alpha-find-v2 init
    [--workspace PATH]                # default: cwd

alpha-find-v2 sync
    [--raw-db PATH]                   # default: output/raw.duckdb
    [--config PATH]                   # default: config/data_sources.toml
    [--only ID[,ID...]]
    [--reset ID[,ID...]]
    [--since YYYYMMDD]
    [--until YYYYMMDD]
    [--dry-run]                       # plan only, no API calls

alpha-find-v2 audit-data
    [--raw-db PATH]                   # default: output/raw.duckdb
    [--out-dir PATH]                  # default: output/audit
    [--blocking ID[,ID...]]           # override default blocking checks
```

Output format: each command prints a single JSON object to stdout for
machine consumption. Human progress lines are routed to stderr.

## 6. Error Handling Matrix

| Failure                                   | Behavior                                                  |
|-------------------------------------------|-----------------------------------------------------------|
| Missing `TUSHARE_TOKEN`, AKShare on       | Use AKShare for supported datasets, banner once on stderr |
| Missing `TUSHARE_TOKEN`, no fallback      | Exit 2 with explanation                                   |
| Tushare permission denied for one dataset | Mark `permission_denied`, continue                        |
| Tushare 429 / 5xx                         | Retry with `2 ** attempt` seconds backoff, max 3          |
| Adapter raises unexpected                 | Mark dataset `failed`, `error_message` recorded, continue |
| Schema mismatch from upstream             | Fail dataset, no partial write; record in error           |
| User Ctrl-C                               | Commit current dataset's transaction or rollback cleanly  |

A run is considered `success` when **all enabled datasets** reached terminal
status `success`. A `partial` run still exits 0 (so it can drive cron) but
prints a clear non-zero count of `failed` datasets to stderr.

## 7. Testing Strategy

### 7.1 Unit tests (per adapter, with mocked HTTP/SDK)

- `TushareAdapter` happy path for each dataset, using a fake `pro_api` client
  that returns `pandas.DataFrame` exactly as Tushare does.
- `TushareAdapter` permission-denied returns `permission_denied` raise.
- `AKShareAdapter` daily normalization to Tushare row shape (one randomized
  case using PBT).
- `BaostockAdapter` same.

### 7.2 Orchestrator tests (no network)

- `sync` against an in-memory adapter that emits canned rows; assert
  `meta.dataset_sync_state` and target tables match expected.
- Incremental: first run with full window, second run with `since` advanced;
  assert second run only fetched rows greater than `last_trade_date`.
- `--reset` clears state for selected datasets only.
- Fallback: Tushare adapter raises `permission_denied`, AKShare next in
  priority; assert AKShare rows land with correct `source_table`.
- Atomicity: simulate adapter raising mid-stream; assert no partial rows
  written for that dataset.

### 7.3 Audit tests

- Build a tiny `raw.duckdb` fixture; run `audit-data`; assert JSON shape and
  exit code with and without injected PIT leak.
- PBT: random valid `daily` rows + random `adj_factor` rows; the
  `adj_factor_consistency` check passes; one mutated row makes it fail.

### 7.4 End-to-end smoke test

`tests/test_data_ingest_smoke.py` — uses fakes for all three adapters,
runs `init`, `sync` (full + incremental), then
`build_research_source_db(raw_db, target_db)` against the synthesized
`raw.duckdb`, then `audit-data`, asserting nothing in
`market_data_bootstrap` regresses. This is the single integration check that
proves the boundary contract holds.

## 8. Out of Scope (Explicit Non-Goals)

- No GUI. No web dashboard.
- No streaming / WebSocket feeds.
- No QMT or live order routing changes.
- No retroactive correction of an already-corrupt `raw.duckdb`. Operators
  delete and re-sync.
- No multi-database / sharded storage. One DuckDB file.

## 9. Migration / Rollout

1. Land all code under `data_ingest/` with no impact on `cli.py` other than
   three new subparsers.
2. Run the new full pipeline on the dev box and produce a fresh
   `output/raw.duckdb`.
3. Run `build-research-source-db --source-db output/raw.duckdb` and confirm
   it produces a `research_source.duckdb` indistinguishable in row counts and
   key sample queries from the legacy V1 audited path.
4. Update `README.md` Quick Start (R6) only after step 3 passes.
5. The legacy CLI default `--source-db /home/nan/alpha-find/output/...` stays
   functional for users who still have V1 data. The README pivots to the new
   path; the legacy default is deprecated in a follow-up release.

## 10. Risks & Mitigations

| Risk                                                       | Mitigation                                              |
|------------------------------------------------------------|---------------------------------------------------------|
| Tushare upstream silently changes column names             | Adapters declare expected columns; mismatch fails loud  |
| `output/raw.duckdb` corruption from interrupted write      | Per-dataset transactional `DELETE+INSERT` in one tx     |
| AKShare drift makes fallback unreliable                    | Mark fallback rows in `source_table`; audit warns       |
| Storage growth on full history                             | DuckDB compresses; document approximate footprint       |
| Cross-platform path issues (we already hit \U TOML escape) | All path-bearing config uses POSIX style; tests on Win  |

## 11. What This Design Does Not Decide

- Exact size of an audit sample beyond minimums in section 3.4. Settable via
  config in a follow-up release.
- Whether `dataset_sync_state` migrates to schema v2; current schema is
  versioned and forward-compatible.
- Whether to publish `raw.duckdb` snapshots as releases; out of scope.
