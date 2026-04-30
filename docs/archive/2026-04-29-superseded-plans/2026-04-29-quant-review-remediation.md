# Quant Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make V2's quantitative controls, promotion evidence, and A-share execution assumptions auditable and consistent with the project configuration.

**Architecture:** First eliminate false control claims by adding a machine-checkable control contract. Then tighten scoring, construction, execution, promotion, and data-quality evidence without introducing a portfolio optimizer or a Barra-style risk-model estimator. Controls that are not actually enforced must be reported as `planned` or `report_only`, not treated as live constraints.

**Tech Stack:** Python stdlib, `unittest`, TOML/JSON artifacts, DuckDB-backed fixtures.

---

## Ground Rules

- Do not introduce new dependencies.
- Do not port V1 `factor -> strategy -> promotion` logic.
- Do not introduce a portfolio optimizer or Barra-style risk-model estimator in this slice.
- Treat size and beta controls as planned/report-only until an explicit exposure model and enforcement path exist.
- Preserve unrelated working-tree changes. Do not revert user edits.
- Use TDD for each behavior change: write failing tests, verify red, implement minimally, verify green.
- Keep commits small and use the Lore Commit Protocol.

## Task 1: Control Contract And Configuration Truthfulness

**Why:** Current configs can imply that industry, size, beta, turnover, and execution controls are fully enforced even when some fields are only loaded or documented. Institutional quant workflows require risk controls to be explicit about whether they are enforced, reported, planned, or unsupported.

**Inputs:**
- `config/mandates/*.toml`
- `config/sleeves/*.toml`
- `config/portfolio/*.toml`
- Loaded `Mandate`, `Sleeve`, `PortfolioRecipe`, and `ExecutionPolicy` objects.

**Outputs:**
- A new `control_contract` module that classifies configured controls.
- Tests proving production configs do not claim unsupported controls as enforced.
- A fail-fast helper that downstream live/promotion code can call before claiming readiness.

**Files:**
- Create: `src/alpha_find_v2/control_contract.py`
- Create: `tests/test_control_contract.py`
- Modify as needed: `config/mandates/a_share_long_only_eod.toml`
- Modify as needed: `config/sleeves/*.toml`
- Modify as needed: `tests/test_config_loader.py`

**Implementation details:**
- Define statuses: `enforced`, `report_only`, `planned`, `unsupported`.
- Industry is `enforced` only where an implementation path exists: trend/fundamental industry-neutral scoring or portfolio benchmark-relative industry caps.
- Size and beta must be `planned` or `report_only`, never `enforced`, in this slice.
- `turnover_penalty` remains `report_only` until a real optimizer/penalty path exists.
- `min_trade_weight` becomes `planned` in Task 1 and `enforced` after Task 4.
- Add `assert_no_false_enforced_controls(report)` to raise when a production config declares an enforced control without an implementation path.

**Steps:**
- [x] Write `tests/test_control_contract.py` for current production configs and an intentionally false size/beta enforced fixture.
- [x] Run `PYTHONPATH=src python3 -m unittest tests.test_control_contract -v` and verify red.
- [x] Implement `src/alpha_find_v2/control_contract.py`.
- [x] Update misleading configs from raw booleans/lists to explicit status maps where necessary.
- [x] Update existing config loader tests so they assert the new truthfulness contract, not old ambiguous booleans.
- [x] Run `PYTHONPATH=src python3 -m unittest tests.test_control_contract tests.test_config_loader -v`.

## Task 2: Industry-Neutral Scoring And Rank-Cap Weights

**Why:** Trend descriptors declare industry-neutral normalization, but the current trend scorer uses global z-scores. Sleeve construction also declares `rank_then_cap_weight`, while builders emit equal weights.

**Inputs:**
- Candidate rows with scores and PIT industry labels.
- Descriptor normalization metadata.
- Sleeve construction fields: `selection`, `holding_count`, `weight_cap`.

**Outputs:**
- Shared scoring helpers.
- Trend industry-neutral z-score behavior.
- Rank-based capped sleeve weights.

**Files:**
- Create: `src/alpha_find_v2/scoring.py`
- Modify: `src/alpha_find_v2/trend_research_input_builder.py`
- Modify: `src/alpha_find_v2/fundamental_research_input_builder.py`
- Modify tests for both builders.

**Implementation details:**
- Add `zscore_map(values)`.
- Add `group_neutral_zscore_map(values, groups_by_asset)`.
- Add `rank_then_cap_weights(asset_ids, weight_cap)`.
- Use `1 / rank` raw weights, cap each name, and redistribute only to uncapped names until all weight is assigned or all names are capped.
- Keep missing PIT industry labels as hard failures when industry-neutral scoring is configured.

**Steps:**
- [x] Write tests proving trend scores are normalized within industry groups.
- [x] Write tests proving rank weights are monotonic and capped.
- [x] Implement shared helpers and wire both builders.
- [x] Run focused builder tests.

## Task 3: Portfolio Construction Diagnostics And No-Op Config Guards

**Why:** Current single-name and industry caps hold excess as cash. That can be a valid conservative policy, but it must be attributed instead of hidden inside generic cash.

**Inputs:**
- Sleeve artifacts.
- Benchmark industry weights.
- Portfolio construction model.
- Portfolio constraints.

**Outputs:**
- Explicit cap-induced cash diagnostics.
- Fail-fast errors for unsupported construction model values.

**Files:**
- Modify: `src/alpha_find_v2/portfolio_constructor.py`
- Modify: `tests/test_portfolio_constructor.py`

**Implementation details:**
- Validate currently supported values:
  - `sleeve_weight_source = "portfolio_allocation"`
  - `name_selection = "top_weight"`
  - `excess_weight_policy = "hold_cash"`
- Extend `PortfolioConstructionStep` with:
  - `selection_cash_weight`
  - `single_name_cap_cash_weight`
  - `industry_cap_cash_weight`
- Preserve current `hold_cash` behavior.

**Steps:**
- [x] Add tests for unsupported construction fields.
- [x] Add tests for cap-induced cash attribution.
- [x] Implement validation and diagnostics.
- [x] Run constructor tests.

## Task 4: A-Share Execution Ledger: T+1 Available Shares And Minimum Trade Weight

**Why:** A-share trading requires available-share accounting; current backtest positions track shares but not available shares. `ExecutionPolicy.min_trade_weight` is loaded but not used.

**Inputs:**
- Daily bars.
- Execution policy.
- Orders/fills.
- Corporate action ledger.

**Outputs:**
- Backtest T+1 available-share ledger.
- `min_trade_weight` enforcement with diagnostics.

**Files:**
- Modify: `src/alpha_find_v2/portfolio_backtester.py`
- Modify: `tests/test_portfolio_backtester.py`

**Implementation details:**
- Add `available_shares` to backtest `Position`.
- Bought shares become available on the next trading day.
- Sells consume `available_shares`; insufficient availability produces a diagnostic.
- `min_trade_weight` skips buy and non-liquidating sell deltas below threshold.
- Full liquidation sells are never skipped by `min_trade_weight`.

**Steps:**
- [x] Add a failing test for same-day buy then sell being blocked by T+1.
- [x] Add a failing test for next-day sell availability.
- [x] Add a failing test for `min_trade_weight`.
- [x] Implement available-share and min-trade logic.
- [x] Run backtester tests.

## Task 5: Market Data Quality And Benchmark Clock Evidence

**Why:** Missing data-quality tables should not be reported as zero risk. Benchmark-relative metrics must also state the return clock they use.

**Inputs:**
- `daily_bar_pit`
- `corporate_action_ledger`
- Benchmark state history.
- Benchmark weight snapshots.

**Outputs:**
- Assessability fields for market data quality.
- Benchmark snapshot age diagnostics.
- Return clock metadata in backtest summaries.

**Files:**
- Modify: `src/alpha_find_v2/market_data_quality.py`
- Modify: `src/alpha_find_v2/benchmark_state_builder.py`
- Modify: `src/alpha_find_v2/live_state.py`
- Modify: `src/alpha_find_v2/portfolio_backtester.py`
- Modify related tests.

**Implementation details:**
- Add `adj_factor_jump_assessable`.
- Add `missing_quality_tables`.
- Add `promotion_blocking_quality_state`.
- Add benchmark `snapshot_age_days` where provider weights are carried forward.
- Add clock metadata:
  - portfolio: next-open execution, close mark.
  - benchmark: previous close to current close using previous benchmark weights.

**Steps:**
- [x] Add tests for missing corporate action ledger being unassessable, not green.
- [x] Add tests for stale provider benchmark snapshots.
- [x] Add tests for backtest clock metadata.
- [x] Implement fields and JSON serialization.
- [x] Run focused quality/backtester/benchmark tests.

## Task 6: Promotion Evidence Basis And Overlay Consistency

**Why:** Research replay IR is not the same as benchmark-relative tradable portfolio IR. Regime overlay must be applied consistently when measuring marginal portfolio value.

**Inputs:**
- Promotion replay cases.
- Sleeve artifacts.
- Benchmark state.
- Portfolio backtest artifacts.
- Regime overlay observations.

**Outputs:**
- Research metrics clearly labeled as artifact-period absolute-return metrics.
- Live-ready gates that require active backtest evidence.
- Overlay application modes.

**Files:**
- Modify: `src/alpha_find_v2/portfolio_promotion_replay.py`
- Modify: `src/alpha_find_v2/research_evaluator.py`
- Modify: `src/alpha_find_v2/promotion_gate_evaluator.py`
- Modify: `src/alpha_find_v2/live_readiness.py`
- Modify related tests.

**Implementation details:**
- Add metric basis fields:
  - `research_ir`
  - `research_tstat`
  - `metric_basis = "artifact_period_absolute_net_return"`
- Add replay `overlay_application_mode`, defaulting to `both_portfolios`.
- `candidate_only` mode is allowed for research experiments but blocks live-ready promotion.
- Mark user-supplied `cost_scenario_pass` and `regime_pass` as manual gate inputs unless derived evidence exists.
- Live-ready must bind portfolio backtest active metrics before passing.

**Steps:**
- [x] Add tests for overlay applying to baseline and candidate.
- [x] Add tests that candidate-only overlay blocks live-ready.
- [x] Add tests that missing active backtest IR blocks live-ready.
- [x] Implement replay and readiness changes.
- [x] Run promotion/readiness tests.

## Task 7: Documentation, Examples, And Final Verification

**Why:** Future users must understand which claims are enforced, which are reported, and which are planned.

**Inputs:**
- README.
- Architecture docs.
- Example replay/backtest artifacts.

**Outputs:**
- Updated docs and examples aligned with implemented behavior.
- Full test pass.

**Files:**
- Modify docs and examples only after code behavior is settled.

**Implementation details:**
- Document industry-neutral scoring.
- Document cap-induced cash.
- Document T+1 available-share ledger.
- Document research IR vs active IR.
- Document data-quality blocking states.

**Steps:**
- [x] Update docs and examples to match new schemas and behavior.
- [x] Run all focused test groups.
- [x] Run `PYTHONPATH=src python3 -m unittest discover -s tests -v`.
- [x] Record remaining risks and known non-goals.

## Final Acceptance

- No unsupported control is marked as enforced.
- Trend sleeve produces industry-neutral scores when configured.
- Sleeve weights follow rank-cap policy rather than equal weight when configured.
- Constructor reports cap-induced cash.
- Backtester has T+1 available shares and minimum trade weight behavior.
- Data-quality unassessable states block promotion/readiness.
- Promotion distinguishes research replay IR from benchmark-relative active IR.
- Full test suite passes.
