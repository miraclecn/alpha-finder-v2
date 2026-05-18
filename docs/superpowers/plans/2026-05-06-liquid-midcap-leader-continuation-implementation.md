# Liquid Midcap Leader Continuation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and backtest `liquid_midcap_leader_continuation_v1` as a fresh V2 A-share research object for 2020 through April 2026.

**Architecture:** Keep the existing V2 object chain intact. Extend only the trend research input builder with the stock-specific weighted-momentum, R-squared, EMA/Laplace, volume-ratio, RSI, and float-market-cap filters needed by the new object.

**Tech Stack:** Python stdlib, DuckDB, existing TOML config loaders, existing CLI commands, `unittest`, no new dependencies.

---

## Files

- Create descriptor configs:
  - `config/descriptors/weighted_momentum_quality.toml`
  - `config/descriptors/volume_overheat_control.toml`
- Create research-object configs:
  - `config/theses/liquid_midcap_leader_continuation.toml`
  - `config/descriptor_sets/liquid_midcap_leader_continuation_v1.toml`
  - `config/sleeves/liquid_midcap_leader_continuation_v1.toml`
- Create example chain:
  - `research/examples/trend_input_build_minimal/liquid_midcap_leader_continuation_v1.toml`
  - `research/examples/artifact_build_minimal/liquid_midcap_leader_continuation_v1_output.toml`
  - `research/examples/promotion_replay_real_output_liquid_midcap/candidate_portfolio.toml`
  - `research/examples/deployment_minimal/liquid_midcap_leader_continuation_v1_portfolio_backtest.toml`
  - `research/examples/benchmark_state_build_minimal/csi800_2020_20260428.toml`
- Modify code and tests:
  - `src/alpha_find_v2/trend_research_input_builder.py`
  - `tests/test_trend_research_input_builder.py`

## Tasks

- [ ] Add failing tests for weighted momentum scoring, R-squared filtering, float-market-cap eligibility, trend filter eligibility, volume overheat filtering, and new descriptor scoring.
- [ ] Implement minimal builder support for the new metrics and filters.
- [ ] Add the versioned config chain and example cases.
- [ ] Run focused tests until green.
- [ ] Build benchmark state, research input, sleeve artifact, and daily backtest through April 2026.
- [ ] Inspect the generated artifact summaries and produce the user-facing backtest summary.

## Verification Commands

```bash
PYTHONPATH=src python3 -m unittest tests.test_trend_research_input_builder -v
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 -m alpha_find_v2 build-benchmark-state --case research/examples/benchmark_state_build_minimal/csi800_2020_20260428.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-trend-research-input --case research/examples/trend_input_build_minimal/liquid_midcap_leader_continuation_v1.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/liquid_midcap_leader_continuation_v1_output.toml
PYTHONPATH=src python3 -m alpha_find_v2 run-portfolio-backtest --case research/examples/deployment_minimal/liquid_midcap_leader_continuation_v1_portfolio_backtest.toml
```

