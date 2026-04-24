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

```bash
cd /home/nan/alpha-find-v2
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
python3 -m unittest discover -s tests -v
PYTHONPATH=src python -m alpha_find_v2 list-theses
PYTHONPATH=src python -m alpha_find_v2 list-descriptor-sets
PYTHONPATH=src python -m alpha_find_v2 show-cost-model --path config/cost_models/base_a_share_cash.toml
PYTHONPATH=src python -m alpha_find_v2 show-execution-policy --path config/execution_policies/a_share_next_open_v1.toml
PYTHONPATH=src python -m alpha_find_v2 show-benchmark-state --path research/examples/promotion_replay_minimal/benchmark_state_history.json
PYTHONPATH=src python -m alpha_find_v2 show-account-state --path research/examples/deployment_minimal/account_state_2026_04_20.json
PYTHONPATH=src python -m alpha_find_v2 show-portfolio-state --path research/examples/deployment_minimal/portfolio_state_2026_04_20.json
PYTHONPATH=src python -m alpha_find_v2 show-portfolio-construction-model --path config/portfolio_construction/a_share_core_blend.toml
PYTHONPATH=src python -m alpha_find_v2 show-risk-model --path config/risk_models/a_share_core_equity.toml
PYTHONPATH=src python -m alpha_find_v2 show-target --path config/targets/open_t1_to_open_t20_residual_net_cost.toml
PYTHONPATH=src python -m alpha_find_v2 show-decay-monitor --path config/decay_monitors/a_share_core_watch.toml
PYTHONPATH=src python -m alpha_find_v2 build-reference-staging-db --target-db output/pit_reference_staging.duckdb --start-date 20140101 --benchmark "CSI 800=000906.SH"
PYTHONPATH=src python -m alpha_find_v2 build-research-source-db --source-db /home/nan/alpha-find/output/stock_data_audited.duckdb --supplemental-db output/pit_reference_staging.duckdb --target-db output/research_source.duckdb
PYTHONPATH=src python -m alpha_find_v2 build-benchmark-state --case research/examples/benchmark_state_build_minimal/csi800.toml
PYTHONPATH=src python -m alpha_find_v2 build-trend-research-input --case research/examples/trend_input_build_minimal/trend_leadership_core.toml
PYTHONPATH=src python -m alpha_find_v2 build-trend-research-input --case research/examples/trend_input_build_minimal/trend_resilience_core.toml
PYTHONPATH=src python -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_output.toml
PYTHONPATH=src python -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_resilience_core_output.toml
PYTHONPATH=src python -m alpha_find_v2 run-promotion-replay --case research/examples/promotion_replay_real_output/replay_case.toml
PYTHONPATH=src python -m alpha_find_v2 build-executable-signal --case research/examples/deployment_minimal/executable_signal_real_output_case.toml
PYTHONPATH=src python -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/fundamental_rerating_core.toml
PYTHONPATH=src python -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core.toml
PYTHONPATH=src python -m alpha_find_v2 show-sleeve-artifact --path research/examples/promotion_replay_minimal/sleeve_artifacts/trend_leadership_core.json
PYTHONPATH=src python -m alpha_find_v2 run-promotion-replay --case research/examples/promotion_replay_minimal/replay_case.toml
PYTHONPATH=src python -m alpha_find_v2 build-executable-signal --case research/examples/deployment_minimal/executable_signal_case.toml
PYTHONPATH=src python -m alpha_find_v2 evaluate-decay-watch --case research/examples/deployment_minimal/decay_watch_case.toml
```

If `pytest` is installed in your environment, `pytest -q` is also valid.

Note:

- `/home/nan/alpha-find/output/stock_data_audited.duckdb` still does not carry
  `benchmark_membership_pit`, `benchmark_weight_snapshot_pit`, or
  `industry_classification_pit`.
- Those PIT reference tables must be staged into the supplemental DuckDB passed
  to `build-research-source-db`.
- `build-reference-staging-db` now stages official `index_weight` snapshots and
  SW2021 `index_member_all` history so the checked-in CSI 800 benchmark case can
  run with `provider_weight` and `sw2021_l1`.
- `research/examples/trend_input_build_minimal/trend_leadership_core.toml` now
  enables `cn_a_directional_open_lock`, so generated trend artifacts stop
  silently suppressing A-share open-limit trade blocks.
- The current honest CSI 800 + `sw2021_l1` replay window begins on `2025-08-29`
  because earlier benchmark constituents still have missing staged SW2021
  classification coverage.
- The real-output promotion replay lane now compares generated
  `trend_leadership_core` and `trend_resilience_core` artifacts on that same
  `2025-08-29+` weekly calendar instead of falling back to a synthetic second
  sleeve artifact.
- `run-promotion-replay` now emits replay diagnostics for sleeve overlap,
  candidate-only contribution, concentration, and the best/worst incremental
  periods so portfolio tuning can be guided by economic evidence.

## Operating Principle

V2 is thesis-first and portfolio-first. Legacy V1 outputs may be used as a comparison baseline, but they are not treated as investable alpha assets.

The main V2 control chain now reaches the deployment boundary:

`mandate -> thesis -> descriptor set -> sleeve -> portfolio recipe -> executable signal -> decay record`
