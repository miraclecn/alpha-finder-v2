# Descriptor Evaluation — Design

> Companion to `requirements.md`. Resolves Q1–Q4 and locks down module
> boundaries, SQL templates, and JSON schemas so tasks can be cut from it.

## 1. Architectural Decisions

### 1.1 Where the new code lives

A single new package: `src/alpha_find_v2/factor_evaluation/`. Outside it,
only `cli.py` is touched (three new subcommands).

```
src/alpha_find_v2/factor_evaluation/
├── __init__.py
├── descriptor_compute.py     # registry + compute functions for 5 descriptors
├── descriptor_stubs.py       # stubs for 5000-credit descriptors
├── universe_resolver.py      # resolve mandate/benchmark universe at each t
├── forward_returns.py        # forward-return SQL helper
├── descriptor_evaluator.py   # IC / decile / monotonicity / stability metrics
├── correlation_matrix.py     # pairwise descriptor correlation
├── slice_stability.py        # by-industry and by-size-tertile stratification
├── report_writer.py          # JSON + Markdown serialiser
├── cli_handlers.py           # thin wrappers used by cli.py
└── exceptions.py             # DescriptorNotImplemented, etc.
```

`config/descriptors/*.toml` is **not** modified. Existing TOML files describe
the descriptor's economic intent; the compute function lives in code and is
linked by `descriptor_id`.

### 1.2 Boundary with existing code

The contract with the existing pipeline is **read-only against
`output/research_source.duckdb`** (the V2 PIT database produced by
`build-research-source-db`).

We do **not** modify:
- `market_data_bootstrap.py`
- `data_ingest/`
- `trend_research_input_builder.py`
- `fundamental_research_input_builder.py`
- existing config schemas or sleeve artifacts

We **read** the following V2 PIT tables:
- `daily_bar_pit` (open, close, close_adj, turnover_value_cny)
- `daily_basic` columns through `raw_daily_basic` (pe, pb, free_share)
- `industry_classification_pit`
- `benchmark_membership_pit`
- `market_trade_calendar`
- `security_master_ref`
- `raw_suspend_d`, `raw_stk_limit` (from `output/raw.duckdb` if attached; else fallback warning)

The evaluator can run with or without `raw_suspend_d` / `raw_stk_limit`. When
absent, tradeability filters degrade to the heuristic already used by
`trend_research_input_builder.py` (`cn_a_directional_open_lock`-style logic).

### 1.3 Closed Open Questions

#### Q1 — descriptor_version hashing

**Decision**: hash the SHA256 of the compute function's source via
`inspect.getsource(fn)` plus the SHA256 of the descriptor TOML file.
The version field in the report is `"sha256:<hex>"` of the concatenation.

#### Q2 — Universe resolver

**Decision**: introduce `UniverseResolver` with two strategies:

```python
class UniverseResolver:
    def resolve(self, trade_date: str, conn: duckdb.Connection) -> set[str]: ...

class BenchmarkUniverseResolver(UniverseResolver):
    """benchmark_membership_pit lookup."""

class InvestableCoreUniverseResolver(UniverseResolver):
    """Apply mandate filters: A-share, listed >= 120 trade days,
    not ST, median 60-day turnover >= 50M CNY."""
```

Mandate filter values are read directly from
`config/mandates/a_share_long_only_eod.toml` at construction time. No
hard-coding.

#### Q3 — Decile weighting

**Decision**: equal-weight first; `--weighting rank` opt-in supported but
not on the default path. Equal-weight is industry-standard reporting; rank
weight is supplementary.

#### Q4 — Forward-return materialisation

**Decision**: compute on the fly inside `forward_returns.py` with a single
DuckDB SQL using `LEAD(open) OVER (PARTITION BY security_id ORDER BY trade_date)`.
No separate persisted table. This keeps the system stateless across
evaluation runs and avoids stale forward-return caches.

## 2. Compute Registry Design

### 2.1 Registry shape

```python
@dataclass(frozen=True, slots=True)
class DescriptorComputeSpec:
    descriptor_id: str
    fn: Callable[[ComputeContext], pd.DataFrame] | None  # None = stub
    requires: tuple[str, ...]   # tables required, e.g. ("daily_bar_pit",)
    notes: str

REGISTRY: dict[str, DescriptorComputeSpec] = {
    "medium_term_relative_strength": DescriptorComputeSpec(...),
    ...
}
```

### 2.2 ComputeContext

```python
@dataclass(slots=True)
class ComputeContext:
    conn: duckdb.DuckDBPyConnection   # research_source.duckdb (read-only)
    start_date: str                   # YYYYMMDD
    end_date: str                     # YYYYMMDD
    universe: set[str] | None         # None = no universe filter at compute time
```

The compute function returns a tidy frame:

```
columns: trade_date (str YYYYMMDD), security_id (str), descriptor_value (float64)
```

Universe filtering happens **post-compute** in the evaluator, so a single
descriptor's compute output can be reused for evaluation against multiple
universes without re-running compute.

### 2.3 In-scope descriptor implementations

#### `medium_term_relative_strength`

- input: `daily_bar_pit.close_adj`
- formula at `t`:
  - `r_60 = log(close_adj[t] / close_adj[t-60])`
  - `r_5 = log(close_adj[t] / close_adj[t-5])`
  - `descriptor = r_60 - r_5` (avoid one-day gap chasing per TOML)
- requires at least 60 prior trade dates of history
- output: float

#### `trend_stability`

- input: daily `close_adj` and `pre_close`
- compute 60-day rolling daily log returns
- `descriptor = mean(60d log returns) / std(60d log returns)`  (Sharpe-like)
- penalises chaotic paths

#### `turnover_confirmation`

- input: `daily_bar_pit.turnover_value_cny`
- recent-vs-baseline ratio at `t`:
  `descriptor = mean(turnover_value_cny[t-5..t]) / mean(turnover_value_cny[t-60..t-6])`
- detects volume confirmation alongside trend

#### `industry_relative_strength`

- input: `daily_bar_pit.close_adj` + `industry_classification_pit (sw2021_l1)`
- 60-day stock log return minus 60-day industry mean log return
- requires PIT industry membership at `t`

#### `sector_relative_valuation`

- input: `daily_basic.pb` + `industry_classification_pit (sw2021_l1)`
- cross-section: per industry, compute z-score of `1/pb` (cheaper-is-better)
- output is industry-relative cheapness

### 2.4 Stub descriptors

For each of `accrual_quality`, `profitability_quality`, `leverage_conservatism`,
`estimate_revision_breadth`, `post_earnings_drift_signal`:

```python
def _stub(ctx: ComputeContext) -> pd.DataFrame:
    raise DescriptorNotImplemented(
        descriptor_id="accrual_quality",
        requires=("pit_fina_indicator", "raw_balancesheet", "raw_cashflow"),
        message="Requires 5000 Tushare credits. Enable in data_sources.toml.",
    )
```

## 3. Evaluator Design

### 3.1 Top-level entry point

```python
def evaluate_descriptor(
    *,
    descriptor_id: str,
    research_db: Path,
    universe: str = "investable_a_share_core",
    start_date: str,
    end_date: str,
    horizons: tuple[int, ...] = (5, 20, 60),
    primary_horizon: int = 20,
    correlation_against: tuple[str, ...] = (),
    cost_model_path: Path | None = None,
    weighting: str = "equal",       # or "rank"
    include_untradeable: bool = False,
    out_dir: Path = Path("output/descriptor_evaluation"),
) -> DescriptorEvaluationReport: ...
```

### 3.2 Pipeline stages

```
1. Resolve descriptor compute spec from registry
2. Resolve universe (set per trade date) via UniverseResolver
3. Run compute(ctx) → tidy DataFrame
4. Build forward-return panel (LEAD-based SQL) for each horizon
5. Apply tradeability filter (suspend_d + stk_limit) → mark untradeable
6. Per-horizon metrics:
   a. Per-trade-date Pearson IC and Spearman IC
   b. Aggregate IC mean/std/t-stat/IR/decay/half-life
   c. Decile bucket assignment (cs rank → 10 buckets)
   d. Decile mean returns + L-S series
   e. Monotonicity Spearman of (decile, mean_return)
7. Cross-sectional rank stability: lag-1 rank autocorr + period-over-period turnover
8. Slice stability: group by SW2021 L1 industry and by size tertile
9. Cross-correlation matrix (Pearson, raw values) vs other descriptors
10. Coverage and tradeable-rate diagnostics
11. Persist report (JSON + MD) under out_dir/<descriptor_id>/<run_at>/
```

### 3.3 Output schema (JSON)

```json
{
  "schema_version": 1,
  "artifact_type": "descriptor_evaluation_report",
  "descriptor_id": "medium_term_relative_strength",
  "descriptor_version": "sha256:0a1b...",
  "evaluation": {
    "start_date": "20200101",
    "end_date": "20240101",
    "universe_definition": {"id": "csi800", "params": {...}},
    "horizons": [5, 20, 60],
    "primary_horizon": 20,
    "cost_model_id": "base_a_share_cash",
    "weighting": "equal",
    "run_at": "2026-05-18T10:00:00Z",
    "sample_size": {"trade_dates": 974, "securities_distinct": 1248, "rows_used": 482356}
  },
  "horizons": {
    "20": {
      "ic_pearson": {"mean": 0.034, "std": 0.082, "tstat": 1.95, "n": 974},
      "ic_spearman": {"mean": 0.041, "std": 0.078, "tstat": 2.32, "n": 974},
      "ic_ir": 0.500,
      "decile_long_short": {
        "annualised_return": 0.087,
        "annualised_return_net": 0.072,
        "sharpe": 1.12,
        "max_drawdown": -0.18
      },
      "decile_returns": [{"decile": 1, "mean_return": 0.001}, ...],
      "monotonicity_spearman": 0.85,
      "rank_stability_lag1": 0.62,
      "turnover_per_period": 0.18
    },
    "5": {...},
    "60": {...}
  },
  "ic_decay": {"horizons": [1,5,10,20,40,60], "ic_means": [...], "half_life": 24},
  "slice_stability": {
    "by_industry": [{"industry": "bank", "ic_pearson_mean": 0.028, "n": 89}, ...],
    "by_size_tertile": [{"tertile": "low",  "ic_pearson_mean": 0.041}, ...]
  },
  "cross_correlation": {
    "trend_stability": 0.42,
    "turnover_confirmation": 0.18
  },
  "coverage": {
    "rows_used_over_possible": 0.81,
    "tradeable_rate": 0.93,
    "low_coverage_warning": false
  },
  "diagnostics": {
    "warnings": [],
    "compute_duration_ms": 3120,
    "evaluation_duration_ms": 4810
  }
}
```

The Markdown summary is a render of the JSON, with tables for decile returns,
slice stability, and IC decay.

### 3.4 Forward-return SQL template

```sql
WITH bars AS (
    SELECT
        b.security_id,
        b.trade_date,
        b.open,
        a.adj_factor,
        b.open * a.adj_factor AS open_adj
    FROM daily_bar_pit b
    LEFT JOIN raw_adj_factor a ON a.ts_code = b.security_id AND a.trade_date = b.trade_date
),
fwd AS (
    SELECT
        security_id,
        trade_date,
        open_adj                                                       AS open_t,
        LEAD(open_adj, 1) OVER w  AS open_t1,
        LEAD(open_adj, 1+:H) OVER w AS open_t1_h
    FROM bars
    WINDOW w AS (PARTITION BY security_id ORDER BY trade_date)
)
SELECT
    security_id,
    trade_date,
    open_t1,
    open_t1_h,
    (open_t1_h / NULLIF(open_t1, 0) - 1.0) AS forward_return_h
FROM fwd
WHERE trade_date BETWEEN :start AND :end
  AND open_t1 IS NOT NULL AND open_t1_h IS NOT NULL;
```

`:H` is the horizon in trade-calendar days. The SQL query is parameterised
once per horizon and cached in memory for the evaluation run.

### 3.5 Tradeability filter

Two cascading sources:

1. **Preferred**: `raw_suspend_d` and `raw_stk_limit` from `output/raw.duckdb`
   attached read-only as `raw_db` schema. If table exists:
   - `tradeable_at_t1 = NOT (suspended OR limit_up_locked OR limit_down_locked)`
2. **Fallback**: when those tables are absent, compute the limit-lock heuristic
   from `daily_bar_pit` itself: open == high == low and pct_chg ≈ ±10%, plus
   detect suspension via missing rows. This degrades gracefully and emits a
   warning.

The default behaviour excludes untradeable observations from IC. With
`--include-untradeable`, an additional `ic_pearson_raw` series is reported
alongside the default `ic_pearson` (tradeable-only).

### 3.6 Universe resolver

```python
class BenchmarkUniverseResolver(UniverseResolver):
    def __init__(self, conn, benchmark_id: str): ...
    def resolve(self, trade_date: str) -> set[str]:
        return set(conn.execute("""
            SELECT security_id FROM benchmark_membership_pit
            WHERE benchmark_id = ?
              AND effective_at <= ?
              AND (removed_at IS NULL OR removed_at > ?)
        """, [benchmark_id, trade_date, trade_date]).fetchall())

class InvestableCoreUniverseResolver(UniverseResolver):
    def __init__(self, conn, mandate: Mandate): ...
    def resolve(self, trade_date: str) -> set[str]:
        # 1. A-share, not ST, listed >= min_listing_days
        # 2. median 60d turnover >= mandate.min_median_daily_turnover_cny_mn
        # 3. not currently suspended
        ...
```

The resolver reads `min_listing_days`, `min_median_daily_turnover_cny_mn`,
`exclude_st`, `exclude_suspended` from the mandate TOML.

## 4. CLI Surface

Three new subcommands added to `cli.py`. No existing command is renamed.

```
alpha-find-v2 compute-descriptor
    --id IDENTIFIER
    [--research-db PATH]                 # default: output/research_source.duckdb
    [--start YYYYMMDD] [--end YYYYMMDD]
    [--out PATH]                         # write Parquet if set
    [--universe ID]                      # optional pre-filter

alpha-find-v2 evaluate-descriptor
    --id IDENTIFIER
    [--research-db PATH]
    [--raw-db PATH]                      # default: output/raw.duckdb (for tradeability)
    --universe ID                        # csi800 | investable_a_share_core
    --start YYYYMMDD --end YYYYMMDD
    [--horizons CSV]                     # default: 5,20,60
    [--primary-horizon INT]              # default: 20
    [--correlation-against CSV]
    [--cost-model PATH]
    [--weighting equal|rank]             # default: equal
    [--include-untradeable]
    [--out-dir PATH]                     # default: output/descriptor_evaluation

alpha-find-v2 list-evaluation-reports
    [--id IDENTIFIER]                    # filter to one descriptor
    [--out-dir PATH]
```

Stdout is one JSON object per command. Human-readable progress goes to stderr.

## 5. Error Handling

| Failure | Behavior | Exit |
|---------|----------|------|
| Descriptor id not in registry | Print registered ids, exit 2 | 2 |
| Descriptor stub called (unimplemented) | Print descriptor_id + missing datasets, exit 3 | 3 |
| `research_source.duckdb` missing | Clear error, suggest `build-research-source-db` | 4 |
| Universe empty for entire window | Refuse to write report, exit 5 | 5 |
| Compute query SQL error | Surface DuckDB error, exit 6 | 6 |
| Insufficient history (< horizon + lookback) | Drop affected dates, warn, exit 0 | 0 |
| Coverage < 30% | Set `low_coverage_warning=true`, exit 0 | 0 |

## 6. Testing Strategy

### 6.1 Synthetic fixture DB

A new helper `tests/_fixtures/synth_research_db.py` builds a tiny
`research_source.duckdb` with 5 securities × 250 trade dates. The fixture is
shared across all evaluator tests.

### 6.2 Compute tests

- Per descriptor: assert correct values on canned inputs.
- Per descriptor: assert frame has exactly columns `(trade_date, security_id, descriptor_value)`.
- PIT leak test: random sample 30 `(security, t)` pairs and verify no input row's `trade_date > t`.

### 6.3 Evaluator tests

- IC math: monotone descriptor → forward return → Pearson IC ≈ 1, Spearman IC ≈ 1.
- Inverse descriptor → IC ≈ -1.
- Random descriptor → IC mean ≈ 0 with t-stat ≈ 0 (PBT, flagged).
- Decile L-S: linear descriptor → top decile mean > bottom decile mean.
- Coverage: drop one date for one stock → coverage drops accordingly.
- Tradeability: inject limit-lock at `t+1` for one observation → that row counted as untradeable, excluded from default IC.

### 6.4 Stub tests

- Calling an unimplemented descriptor's compute raises `DescriptorNotImplemented` with the right `requires` list.
- CLI exits 3 with the missing-dataset message.

### 6.5 CLI smoke tests

- `compute-descriptor --id medium_term_relative_strength` against synthetic fixture exits 0 and writes Parquet.
- `evaluate-descriptor` exits 0 and writes both `report.json` and `report.md`.
- `list-evaluation-reports` after one evaluation exits 0 with non-empty list.
- Re-run `evaluate-descriptor` produces byte-identical `report.json` modulo `run_at` (R7.2).

### 6.6 Cross-platform

All file I/O uses POSIX paths in stored TOML/JSON. Reports use UTF-8.
Unicode in industry names ("银行" etc.) survives JSON roundtrip. Tested on Windows.

## 7. Performance Notes

- Compute layer uses one DuckDB SQL per descriptor. No row-by-row Python.
- Forward-return SQL runs once per horizon, results cached as Pandas frames.
- IC computation per trade date is vectorised with `scipy.stats.pearsonr` /
  `spearmanr` or pure NumPy.
- Decile bucketing uses `pandas.qcut` with duplicates="drop".
- Memory budget: full panel for 4-year × 1500-stock × 3-horizon ~ 18M rows of
  float64 ≈ 0.5 GB. Stays well under 2 GB.

## 8. Out of Scope (Explicit Non-Goals)

- No factor return regression (Fama-MacBeth) — Stage 4.
- No residualisation against risk model — Stage 4.
- No time-series neutralisation — Stage 4.
- No GUI / dashboard.
- No descriptor parameter optimisation.
- No automatic descriptor mining — Stage 3 is the dedicated sandbox.

## 9. Migration / Rollout

1. Land all code under `factor_evaluation/`. No edits to other packages.
2. Add three CLI subcommands. Existing 249 tests stay green.
3. Run evaluation against the dev synthetic fixture; review the JSON schema.
4. Update README "Reference Examples" with one Stage-2 example block.
5. Document in `docs/architecture/` how Stage 2 fits the descriptor → sleeve
   chain (a new short note, not a rewrite of existing docs).

## 10. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| `pandas.qcut` fails on tied descriptor values | Use `pd.qcut(..., duplicates="drop")` and warn when buckets < 10 |
| Forward returns suffer from corporate actions on the entry day | The `close_adj * adj_factor` join already accounts for it; covered by adj_factor consistency audit from Stage 1 |
| `raw_suspend_d` / `raw_stk_limit` absent on user systems | Heuristic fallback emits warning; evaluator still produces a report |
| Large universe + many horizons exhausts memory | Per-horizon streaming compute, intermediate frames released; max 60s budget enforced by smoke test |
| Cross-correlation requires running multiple computes | Cache compute output per descriptor for the run via dict; recompute only on cache miss |
| Universe resolver depends on `benchmark_membership_pit` which may be empty in dev | Smoke tests only use synthetic universe; CLI surfaces clean error in that case |

## 11. What This Design Does Not Decide

- The exact bucket count (default 10) — adjustable via `--deciles INT` later if
  someone wants quintiles.
- Whether `report.md` should embed plots — out of scope; consumers can
  generate plots from `report.json`.
- Storage rotation policy for `output/descriptor_evaluation/` — left to the
  user.
