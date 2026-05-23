# Design Document: Factor Mining Sandbox

## Overview

The Factor Mining Sandbox (`factor_lab`) is a hard-isolated candidate-generation module that proposes new descriptor expressions for human review. It searches a closed DSL grammar via beam search (with a random-sampling control), evaluates survivors through the existing Stage 2 evaluation pipeline, and emits a machine-readable run directory under `output/factor_lab/runs/<run_id>/`.

The sandbox sits between the data foundation (Stage 1) and the descriptor registry (Stage 2) but never writes to either. It is a read-only consumer of both.

```
┌─────────────────────────────────────────────────────────────────────┐
│  Stage 1: research_source.duckdb (read-only)                        │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ SQL reads
┌──────────────────────────────▼──────────────────────────────────────┐
│  Stage 3: factor_lab                                                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐           │
│  │ DSL      │→ │ Search   │→ │ Walk-Fwd │→ │ Dedup &  │→ artifacts│
│  │ Grammar  │  │ Engine   │  │ Evaluator│  │ Output   │           │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘           │
│       ↑ reuses Stage 2 primitives (read-only)                       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ writes ONLY to output/factor_lab/
┌──────────────────────────────▼──────────────────────────────────────┐
│  output/factor_lab/runs/<run_id>/                                    │
│  manifest.json, candidates.jsonl, shortlist.json,                   │
│  correlation_matrix.csv, audit.md                                   │
└─────────────────────────────────────────────────────────────────────┘
```

## Design Goals

1. **Hard isolation** — structurally impossible to auto-register, auto-promote, or write outside `output/factor_lab/`.
2. **Stage 2 reuse** — IC, forward returns, universe, tradeability computed by existing `factor_evaluation` primitives; no re-implementation.
3. **Controlled search** — beam search + random baseline only; no GP/RL.
4. **Interpretability** — closed grammar, depth ≤ 5, family classification, complexity penalty.
5. **Reproducibility** — deterministic given same seed + config + database snapshot + git SHA.
6. **A-share realism** — tradeability filters, next-open entry, cost model applied via Stage 2.

## Non-Goals

- Auto-promotion of candidates to `config/descriptors/`.
- Genetic programming, reinforcement learning, or any search beyond beam + random.
- `quality` family (deferred until Tushare credit prerequisite met).
- Intraday, news, or alternative data consumption.
- Executable signals, sleeve artifacts, or any downstream deliverable.

## Architecture Position

Per the V2 research object chain:

```
mandate → thesis → descriptor set → sleeve → portfolio recipe → executable signal → decay record
```

The sandbox operates **before** the descriptor registry. Its output is a candidate packet that a human may choose to promote into a registered descriptor via PR. The sandbox never touches the chain from descriptor onward.

## Components

### 1. Expression DSL (`factor_lab/dsl/`)

| Module | Responsibility |
|--------|---------------|
| `grammar.py` | AST node types, operator whitelist (6 TS, 4 CS, 5 arith, 5 leaf), window whitelist `{5,10,20,60,120,250}`, depth limit 5 |
| `parser.py` | String → AST, validates arity, composition rules (no TS wrapping CS), depth, window values |
| `evaluator.py` | AST → `DataFrame[trade_date, security_id, descriptor_value]` via vectorized pandas/numpy on DuckDB-loaded panels |
| `canonical.py` | AST → deterministic canonical string (for caching and dedup keying) |
| `validator.py` | Structured rejection records per R2 clause 10 |

Key design decisions:
- AST is a frozen dataclass tree (hashable, cacheable).
- `evaluator.py` loads leaf data once per run into memory-mapped panels, then evaluates expressions without re-querying DuckDB.
- Cross-section ops operate per-date slice; time-series ops operate per-security column.

### 2. Family Classifier (`factor_lab/family.py`)

Deterministic rule cascade (R4 clauses 2–7):
1. Has `cs_rank` or `cs_zscore` → `cross_momentum`
2. Leaf ⊆ {pe, pb} OR has `cs_industry_demean` → `value`
3. Has `turnover_value_cny` → `volume`
4. Has `rolling_std` → `volatility`
5. Has (`delta` or `lag`) AND `close_adj` → `trend`
6. None match → reject as `rejected_family_unclassifiable`

Pure function of the parsed AST; no state, no randomness.

### 3. Search Engine (`factor_lab/search/`)

| Module | Responsibility |
|--------|---------------|
| `beam.py` | Layer-by-layer growth from depth 1→max_depth, retaining top `beam_width` by fitness per layer |
| `random_sampler.py` | Uniform draw of `random_sample_size` expressions from grammar, depth 1→max_depth |
| `expression_generator.py` | Enumerates valid child expansions for a given partial AST (used by beam); draws uniform random complete trees (used by sampler) |
| `fitness.py` | `fitness = train_IC_IR − λ × node_count` |

Both streams share:
- The same RNG seeded from `config.search.seed`.
- The same expression cache (canonical string → score series).
- The same family classifier and quota tracker.

### 4. Walk-Forward Evaluator (`factor_lab/walk_forward.py`)

Wraps Stage 2 primitives:
1. Splits `[start, end]` into `segments` anchored windows.
2. For each segment, constructs an ad-hoc `DescriptorComputeSpec` (not registered) whose `fn` calls `dsl.evaluator.evaluate(ast, ctx)`.
3. Passes the spec to `evaluate_descriptor` with the segment's train/OOS sub-window.
4. Records per-segment OOS IC_IR and IC mean.
5. Applies acceptance gate: ALL segments must pass `oos_ic_ir_threshold` AND positive OOS IC mean.

The ad-hoc spec is never registered; it lives only for the duration of the evaluation call.

### 5. Correlation Dedup (`factor_lab/dedup.py`)

After walk-forward acceptance:
1. Compute score series for each accepted candidate over the full train window.
2. Load registered descriptor score series via `descriptor_compute.get(id).fn(ctx)` (cached once per run).
3. Pairwise absolute Pearson correlation; reject if > `dedup_rho`.
4. Process in fitness-descending order so higher-fitness candidates survive.
5. Write full correlation matrix to CSV regardless of rejections.

### 6. Output Writer (`factor_lab/output.py`)

Writes the 5 required artifacts + registry append:
- `manifest.json` — run metadata, config snapshot, counts, timing, warnings.
- `candidates.jsonl` — one line per evaluated expression with full status.
- `shortlist.json` — accepted candidates only, ordered by fitness.
- `correlation_matrix.csv` — full pairwise matrix.
- `audit.md` — human-review template with promotion path documentation.
- `output/factor_lab/registry.json` — append-only run index.

All paths in artifacts use POSIX forward-slash format regardless of host OS.

### 7. CLI Layer (`factor_lab/cli.py`)

Three commands added to the existing `cli.py` argument parser:

| Command | Entry | Exit codes |
|---------|-------|-----------|
| `mine-factors --research-db --start --end --config` | `factor_lab.run.execute_mining_run()` | 0=ok, 2=config/arg, 4=db, 6=isolation |
| `list-factor-candidates [--family] [--min-ic-ir]` | `factor_lab.registry.list_runs()` | 0 always |
| `inspect-candidate <run_id> <expr_id>` | `factor_lab.inspect.run_inspection()` | 0=ok, 4=missing run, 5=missing expr |

### 8. Isolation Guard (`factor_lab/isolation.py`)

Startup check:
- Resolves output root to absolute path; asserts it is under `output/factor_lab/`.
- If violated → exit code 6.

Import-time guard (enforced by test suite):
- No import of `descriptor_compute.register`.
- No write calls targeting paths outside `output/factor_lab/`.
- AST-based static analysis in `tests/test_isolation.py`.

## Data Flow

```
CLI invocation
  │
  ├─ validate config (TOML schema, R10)
  ├─ validate dates, DB path
  ├─ isolation check (output root)
  ├─ resolve git SHA
  │
  ├─ load leaf data panels from research_source.duckdb (read-only)
  │
  ├─ BEAM SEARCH (layers 1→max_depth)
  │   ├─ generate candidates per layer
  │   ├─ evaluate via DSL evaluator → score series
  │   ├─ compute train IC_IR via Stage 2 forward_returns
  │   ├─ apply fitness = IC_IR − λ × nodes
  │   ├─ classify family
  │   ├─ retain top beam_width by fitness
  │   └─ apply per-family quota
  │
  ├─ RANDOM SAMPLING (parallel)
  │   ├─ draw random_sample_size expressions
  │   ├─ evaluate, fitness, classify, quota (same as beam)
  │   └─ deduplicate against beam (same expr → merge sources)
  │
  ├─ WALK-FORWARD EVALUATION (quota-admitted candidates)
  │   ├─ per segment: ad-hoc DescriptorComputeSpec → evaluate_descriptor
  │   ├─ record per-segment OOS metrics
  │   └─ accept/reject per R5 thresholds
  │
  ├─ CORRELATION DEDUP (accepted candidates)
  │   ├─ compute pairwise vs registered descriptors + earlier accepted
  │   ├─ reject if |r| > dedup_rho
  │   └─ write correlation_matrix.csv
  │
  └─ OUTPUT
      ├─ write manifest.json, candidates.jsonl, shortlist.json, audit.md
      ├─ append to registry.json
      └─ exit 0 with JSON summary to stdout
```

## Module Layout

```
src/alpha_find_v2/factor_lab/
├── __init__.py
├── cli.py              # CLI command handlers
├── config.py           # Mining config TOML schema, validation, defaults
├── run.py              # Top-level orchestrator for mine-factors
├── inspect.py          # inspect-candidate handler
├── registry.py         # registry.json read/append, list-factor-candidates
├── isolation.py        # Output-root guard, import guard helpers
├── dsl/
│   ├── __init__.py
│   ├── grammar.py      # AST nodes, operator/leaf/window whitelists
│   ├── parser.py       # String → AST with validation
│   ├── evaluator.py    # AST → DataFrame[trade_date, security_id, descriptor_value]
│   ├── canonical.py    # AST → canonical string
│   └── validator.py    # Structured rejection records
├── search/
│   ├── __init__.py
│   ├── beam.py         # Beam search engine
│   ├── random_sampler.py  # Uniform random sampling
│   ├── expression_generator.py  # Candidate expansion / random tree draw
│   └── fitness.py      # Fitness function with complexity penalty
├── family.py           # Deterministic family classifier
├── walk_forward.py     # Anchored walk-forward evaluator (wraps Stage 2)
├── dedup.py            # Correlation dedup stage
└── output.py           # Artifact writer (manifest, candidates, shortlist, matrix, audit)
```

## Config Schema (Mining Config TOML)

```toml
[search]
beam_width = 20          # 1..1000
max_depth = 5            # 1..5
random_sample_size = 1000 # 0..100000
seed = 42                # 0..2^32-1

[fitness]
complexity_lambda = 0.05 # 0.0..1.0

[family]
quota_per_family = 5     # 1..50

[walk_forward]
segments = 3             # 1..32
oos_window_months = 6    # 1..60
min_train_months = 24    # 6..120
oos_ic_ir_threshold = 0.30  # 0.0..5.0
primary_horizon_days = 20   # 1..250

[dedup]
rho_threshold = 0.85     # 0.0..1.0
min_obs = 60             # 1..5000

[universe]
id = "investable_a_share_core"
```

Validation rules:
- Unknown keys → exit 2.
- Out-of-range values → exit 2.
- Type mismatch → exit 2.
- Missing keys → substitute defaults, record in `manifest.json` under `config_defaults_applied`.
- `quality` family reference → exit 2 (R13).

## Stage 2 Integration Points

| Stage 2 Symbol | Usage in Sandbox | Access Mode |
|----------------|-----------------|-------------|
| `evaluate_descriptor()` | Walk-forward per-segment evaluation, inspect-candidate | Read-only call |
| `compute_forward_returns()` | Train-set IC_IR computation for beam/random fitness | Read-only call |
| `resolver_for_universe()` | Universe resolution per segment | Read-only call |
| `DescriptorComputeSpec` | Ad-hoc spec construction (never registered) | Instantiate only |
| `ComputeContext` | Context for ad-hoc spec fn | Instantiate only |
| `descriptor_compute.get()` | Load registered descriptor scores for dedup correlation | Read-only call |
| `report_writer.write_report()` | inspect-candidate full report | Read-only call |
| Tradeability filter (internal to `evaluate_descriptor`) | Applied automatically | Indirect |

**Hard boundary**: `descriptor_compute.register()` is never imported or called.

## A-Share Execution Realism

The sandbox inherits A-share realism from Stage 2 without re-implementing:
- **T+1 entry**: forward returns use `open[t+1]` as entry price.
- **Tradeability**: suspend, limit-up, limit-down filters applied by `evaluate_descriptor`.
- **Cost model**: `config/cost_models/base_a_share_cash.toml` (15bps/side default) passed through.
- **Universe**: `investable_a_share_core` applies listing-day, turnover, ST filters.

The sandbox adds no new execution-realism logic; it delegates entirely to Stage 2.

## Reproducibility Design

1. All RNG derived from single `seed` value (numpy `default_rng(seed)`).
2. Beam tie-breaking is deterministic (ascending node_count, then lexicographic).
3. Expression cache keyed by canonical string ensures same expression evaluated once.
4. `git_sha` recorded; dirty tree flagged with `-dirty` suffix.
5. `config_snapshot` in manifest enables exact re-run.
6. No git repo → hard fail (cannot guarantee reproducibility metadata).

## Performance Strategy

| Technique | Target |
|-----------|--------|
| Leaf data loaded once into memory-mapped panels | Avoid repeated DuckDB queries |
| Expression score cache (canonical string key) | Avoid duplicate evaluation |
| Per-segment frame release after metrics recorded | Keep RSS < 4GB (synth) / 8GB (full) |
| Family quota limits OOS evaluation count | Cap expensive walk-forward calls |
| Beam width caps per-layer evaluation | Bound total evaluations |

Time budgets: synth fixture < 5min, full 4-year DB < 30min on 4-core laptop.

## Testing Strategy

| Test Category | What It Verifies |
|---------------|-----------------|
| `test_dsl_grammar.py` | All R2 acceptance/rejection rules, arity, composition, depth, window |
| `test_family_classifier.py` | Deterministic classification, all 5 families + reject case |
| `test_search_beam.py` | Layer growth, retention by fitness, tie-breaking, seed determinism |
| `test_search_random.py` | Uniform sampling, dedup with beam, source merging |
| `test_walk_forward.py` | Segment splitting, precondition checks, OOS gate logic |
| `test_dedup.py` | Correlation computation, rejection threshold, ordering, undefined handling |
| `test_output_schema.py` | All 5 artifacts validate against expected schemas |
| `test_config_validation.py` | All R10 validation rules, defaults, unknown keys, type errors |
| `test_isolation.py` | No writes outside `output/factor_lab/`, no `register()` import, AST scan |
| `test_reproducibility.py` | Byte-identical outputs (modulo run_id/run_at/duration) across two runs |
| `test_cli_integration.py` | Exit codes, stdout/stderr contracts for all 3 commands |
| `test_performance.py` | Synth fixture completes < 5min, RSS < 4GB |

All tests use the synthetic fixture (`tests/_fixtures/synth_research_db.py`). Stage 2 test suite (≥280 tests) must remain green.

## Open Questions

1. **Leaf data caching across runs** — Should we persist pre-computed leaf panels to disk for faster re-runs, or is in-memory-per-run sufficient for the first release?
2. **Parallel evaluation** — Should beam layers evaluate candidates in parallel (multiprocessing), or is single-threaded sufficient given the beam_width=20 default?
3. **Registry.json concurrency** — If two runs execute simultaneously, registry.json append could race. File-lock or accept single-user constraint for V1?

## Decisions Log

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | Single-threaded first release | Simplicity; beam_width=20 × 5 layers = 100 evaluations is fast enough single-threaded on synth fixture |
| D2 | No disk cache for leaf panels | Memory-per-run is simpler; full DB fits in 8GB RSS budget |
| D3 | Single-user assumption for registry.json | Personal research tool; no concurrent runs expected |
| D4 | Frozen dataclass AST | Hashable for caching, immutable for safety |
| D5 | POSIX paths in all artifacts | Cross-platform reproducibility; Windows users get forward-slash paths in JSON |
