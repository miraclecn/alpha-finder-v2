# alpha-find-v2

Finance-first rebuild of a personal A-share quant trading system.

This repo starts from the economic object you actually trade, not from symbolic factor generation. The V2 core model is:

`mandate -> thesis -> descriptor set -> sleeve -> portfolio recipe -> executable signal -> decay record`

## First Product

The first production target is deliberately narrow:

- market: China A-shares
- direction: long-only cash equities
- execution style: end-of-day research, next-day open execution
- holdings: 15-30 names
- cadence: weekly or 2-3 times per week
- risk stance: industry and size controlled, turnover budgeted, A-share constraints explicit

## Current Research Doctrine

The first production research stack is constrained by what a personal system can actually source and maintain:

- core alpha: price/volume-driven medium-horizon stock selection
- slow anchor: lagged quality/value rerating used as a slower sleeve and veto source
- overlay: portfolio-level regime and tradeability control
- deferred: same-day earnings/event, message/news, and fragile flow/crowding pipelines

In practice, V2 now treats `Tushare 2000` daily data as the production truth layer and uses slower fundamentals only with conservative reporting-lag rules.

For `trend_leadership_core`, the current research bridge is intentionally
bound to an interim `open_t1_to_open_t20_net_cost` target. The pipeline now
states net-of-cost forward returns honestly instead of emitting placeholder
zero residual components before audited PIT residualization inputs exist.
V2 now also carries a second weekly price-based real sleeve,
`trend_resilience_core`, which tilts harder toward stable and liquid leaders
on the same decision calendar so generated real-output promotion replay is no
longer blocked on a missing second sleeve artifact.
When audited `industry_classification_pit` data is present in the research
DuckDB, the trend input builder can now bind PIT industry labels directly.
If a portfolio uses `benchmark_relative` industry caps, V2 now rejects sleeve
signals with blank industry labels instead of silently treating them as valid.
The research-source bootstrap can now also import audited PIT reference tables
from a supplemental DuckDB, and V2 can build a formal
`benchmark_state_history` artifact either from staged benchmark membership plus
`float_mcap_proxy` or from staged official `benchmark_weight_snapshot_pit`
provider weights. The trend input builder now requires an explicit
`industry_schema` when binding PIT industry labels so multi-schema reference
tables remain honest.

## Repo Layout

- `config/mandates`: live trading mandates
- `config/theses`: economically underwritten alpha theses
- `config/descriptors`: atomic, point-in-time-safe research measurements
- `config/descriptor_sets`: thesis-specific descriptor bundles
- `config/cost_models`: versioned A-share cash-equity cost assumptions
- `config/execution_policies`: versioned rules for turning approved weights into tradable release packages
- `config/portfolio_construction`: versioned sleeve-combination and hard-cap policies
- `config/risk_models`: versioned common-return models used for residualization
- `config/sleeves`: tradable sleeves linked to a thesis and mandate
- `config/targets`: executable residual return definitions aligned to trade timing and costs
- `config/portfolio`: multi-sleeve portfolio recipes
- `config/promotion_gates`: portfolio-level promotion criteria for sleeve admission
- `config/decay_monitors`: versioned rules for post-promotion watch and retirement decisions
- `docs/architecture`: system principles and operating model
- `docs/data`: V2 data boundary and PIT audit rules
- `docs/migration`: V1 to V2 boundary documents
- `research/examples`: persisted replay and deployment cases plus sample sleeve, benchmark-state, and account-state artifacts
- `research/examples/artifact_build_minimal`: build cases that emit standardized sleeve artifacts from normalized research observations
- `research/examples/trend_input_build_minimal`: DuckDB-backed build cases that emit first-pass trend observation inputs
- `research/examples/deployment_minimal`: deployment cases plus account-state and portfolio-state snapshots that bind the live book to the executable package
- `research/examples/benchmark_state_build_minimal`: build cases for turning PIT benchmark membership and industry classification into benchmark-state artifacts
- `research/examples/promotion_replay_real_output`: honest replay case that compares two generated weekly sleeves on the shared `output/` decision calendar
- `src/alpha_find_v2/reference_data_staging.py`: Tushare-backed staging of PIT benchmark and industry reference tables into supplemental DuckDBs
- `src/alpha_find_v2`: loaders plus portfolio construction, simulation, deployment, artifact I/O, and promotion replay primitives
- `docs/data/v1-duckdb-reuse-audit.md`: explicit V1 DuckDB reuse findings and V2 source-DB decisions
- `tests`: config and loader verification

## Quick Start

### Prerequisites

- Python 3.11+
- A [Tushare Pro](https://tushare.pro/user/token) account token

**Tushare credit tiers:**
- **2000 credits** — covers the full Stage 1 dataset list (daily bars, adj_factor, stk_limit, suspend_d, index data, SW2021 industry history). This is all you need to run the `trend_leadership_core` sleeve.
- **5000 credits** — unlocks fundamentals: `fina_indicator`, `income`, `balancesheet`, `cashflow`, `forecast`, `express`. Required for the `fundamental_rerating_core` sleeve and the factor lab's quality/value family.

No Tushare token? Run the commands below anyway — the system falls back to AKShare for daily price data so you can validate the pipeline with a demo-quality database.

### Step-by-step

```bash
# 1. Install
pip install -e .

# 2. Initialise workspace (creates .env, config/data_sources.toml, output/)
alpha-find-v2 init

# 3. Set your Tushare token
#    Edit .env and set:  TUSHARE_TOKEN=<your_token>
#    Or export it:       export TUSHARE_TOKEN=<your_token>

# 4. Sync A-share data into output/raw.duckdb
alpha-find-v2 sync

# 5. Build the V2 PIT-safe research database
alpha-find-v2 build-research-source-db \
    --source-db output/raw.duckdb \
    --target-db output/research_source.duckdb

# 6. Verify data quality
alpha-find-v2 audit-data

# 7. Run the test suite
pytest -q
```

Sync for a specific date range (useful for a quick smoke test):

```bash
alpha-find-v2 sync --since 20240101 --only stock_basic,trade_cal,daily,adj_factor,daily_basic
```

Dry-run to see what would be synced without calling the API:

```bash
alpha-find-v2 sync --dry-run
```

When you upgrade to 5000 Tushare credits, enable fundamentals by editing `config/data_sources.toml`:

```toml
[datasets.fina_indicator]
enabled = true   # was false

[datasets.income]
enabled = true

# ... (balancesheet, cashflow, forecast, express)
```

Then re-run `alpha-find-v2 sync` to pull the new datasets.

## Reference Examples

The following commands use checked-in example artifacts. They require the research database to already exist (see Quick Start above).

```bash
alpha-find-v2 list-theses
alpha-find-v2 list-descriptor-sets
alpha-find-v2 show-cost-model --path config/cost_models/base_a_share_cash.toml
alpha-find-v2 show-execution-policy --path config/execution_policies/a_share_next_open_v1.toml
alpha-find-v2 show-benchmark-state --path research/examples/promotion_replay_minimal/benchmark_state_history.json
alpha-find-v2 show-account-state --path research/examples/deployment_minimal/account_state_2026_04_20.json
alpha-find-v2 show-portfolio-state --path research/examples/deployment_minimal/portfolio_state_2026_04_20.json
alpha-find-v2 show-portfolio-construction-model --path config/portfolio_construction/a_share_core_blend.toml
alpha-find-v2 show-risk-model --path config/risk_models/a_share_core_equity.toml
alpha-find-v2 show-target --path config/targets/open_t1_to_open_t20_residual_net_cost.toml
alpha-find-v2 show-decay-monitor --path config/decay_monitors/a_share_core_watch.toml
alpha-find-v2 build-reference-staging-db --target-db output/pit_reference_staging.duckdb --start-date 20140101 --benchmark "CSI 800=000906.SH"
alpha-find-v2 build-research-source-db --source-db output/raw.duckdb --supplemental-db output/pit_reference_staging.duckdb --target-db output/research_source.duckdb
alpha-find-v2 build-benchmark-state --case research/examples/benchmark_state_build_minimal/csi800.toml
alpha-find-v2 build-trend-research-input --case research/examples/trend_input_build_minimal/trend_leadership_core.toml
alpha-find-v2 build-trend-research-input --case research/examples/trend_input_build_minimal/trend_resilience_core.toml
alpha-find-v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_output.toml
alpha-find-v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_resilience_core_output.toml
alpha-find-v2 run-promotion-replay --case research/examples/promotion_replay_real_output/replay_case.toml
alpha-find-v2 build-executable-signal --case research/examples/deployment_minimal/executable_signal_real_output_case.toml
alpha-find-v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/fundamental_rerating_core.toml
alpha-find-v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core.toml
alpha-find-v2 show-sleeve-artifact --path research/examples/promotion_replay_minimal/sleeve_artifacts/trend_leadership_core.json
alpha-find-v2 run-promotion-replay --case research/examples/promotion_replay_minimal/replay_case.toml
alpha-find-v2 build-executable-signal --case research/examples/deployment_minimal/executable_signal_case.toml
alpha-find-v2 evaluate-decay-watch --case research/examples/deployment_minimal/decay_watch_case.toml
```

Notes:

- `build-reference-staging-db` stages official `index_weight` snapshots and SW2021 `index_member_all` history for the CSI 800 benchmark case.
- `research/examples/trend_input_build_minimal/trend_leadership_core.toml` enables `cn_a_directional_open_lock` so generated trend artifacts respect A-share open-limit trade blocks.
- The current honest CSI 800 + `sw2021_l1` replay window begins on `2025-08-29` because earlier benchmark constituents still have missing staged SW2021 classification coverage.
- The real-output promotion replay lane compares generated `trend_leadership_core` and `trend_resilience_core` artifacts on that same `2025-08-29+` weekly calendar.
- `run-promotion-replay` emits replay diagnostics for sleeve overlap, candidate-only contribution, concentration, and best/worst incremental periods.

## Stage 3: Factor Mining Sandbox

The Factor Mining Sandbox is a hard-isolated candidate generator for new descriptor expressions. It reads the research database, searches a closed DSL grammar via beam search with a random-sampling baseline, and emits a human-review packet. It never auto-promotes candidates and never writes outside `output/factor_lab/`.

Full documentation: [`docs/architecture/factor-mining-sandbox.md`](docs/architecture/factor-mining-sandbox.md)

CLI commands:

- `mine-factors --research-db <path> --start <YYYYMMDD> --end <YYYYMMDD> --config <toml>` — run one mining session and write a run directory under `output/factor_lab/runs/<run_id>/`
- `list-factor-candidates [--family <name>] [--min-ic-ir <float>]` — list registered runs from `output/factor_lab/registry.json`
- `inspect-candidate <run_id> <expr_id>` — re-run the full Stage 2 evaluation on a specific candidate and write a detailed report

Promotion from sandbox to descriptor registry is a **manual PR workflow**: a researcher fills in the `economic_story` and `risk_notes` in the generated `audit.md`, then opens a PR adding `config/descriptors/<id>.toml` and a compute function in `factor_evaluation/descriptor_compute.py`. The sandbox does not perform or automate any part of this step.

## Operating Principle

V2 is thesis-first and portfolio-first. Legacy V1 outputs may be used as a comparison baseline, but they are not treated as investable alpha assets.

The main V2 control chain now reaches the deployment boundary:

`mandate -> thesis -> descriptor set -> sleeve -> portfolio recipe -> executable signal -> decay record`
