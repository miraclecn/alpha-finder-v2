# Professional Quant Roadmap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Current steering note, 2026-04-30:** The repository has now landed the
> strategy failure attribution report, generated-strategy guardrails, and
> shadow-live admission evaluator described below. The main remaining project
> bottleneck is no longer missing gate machinery; it is the absence of a new
> versioned research object that can pass the existing gates after the frozen
> `trend_leadership_shadow_live_v1` candidate was rejected.

**Goal:** Move V2 from a working A-share research framework to a professional, evidence-gated personal quant process that rejects weak strategies before paper trading.

**Architecture:** Keep the V2 object chain unchanged. Add a strategy-quality gate above the existing data-quality gate, then diagnose the failed `trend_leadership` candidate before any new sleeve or signal generation work. Treat shadow-live as an operating-evidence phase, not as a way to rescue a failed historical strategy.

**Tech Stack:** Python stdlib, DuckDB, existing `alpha_find_v2` CLI, TOML/JSON artifacts, `unittest`, optional `pytest`, no new dependencies.

---

## Decision

The current project should stop treating "data-quality gate clean" as
"strategy releasable." The next professional milestone is a capital-admission
gate with three separate layers:

1. Data-quality admissibility.
2. Strategy-quality admissibility.
3. Operational shadow-live admissibility.

The current `trend_leadership_shadow_live_v1` passes the first layer for the
attached portfolio evidence and fails the second layer.

## Current Blockers

- `trend_leadership_shadow_live_v1` active IR is `-1.33` with overlay.
- Overlay portfolio max drawdown is `-89.20%`.
- Turnover is `73.20x` across the validation window.
- The audit evaluator now separates data-quality and strategy-quality gates;
  the current candidate fails the strategy-quality gate.
- Shadow-live journal has `1` recorded cycle, below the `12` cycle minimum.
- No successor research object is currently underwritten for capital review.

## Task 1: Make Strategy-Quality Failure Machine-Checkable

**Files:**

- Modify: `src/alpha_find_v2/live_readiness.py`
- Modify: `tests/test_live_readiness.py`
- Modify: `docs/operations/trend-leadership-paper-trade-signal-policy.md`
- Modify: `research/examples/deployment_minimal/trend_leadership_multi_year_validation_audit_v1.json`

**Acceptance criteria:**

- A multi-year audit with negative active IR fails a strategy-quality gate.
- A multi-year audit with max drawdown worse than budget fails the same gate.
- The CLI output distinguishes `data_quality_gate_met` from
  `strategy_quality_gate_met`.
- `signal_release_gate_met` is false unless both gates pass.

- [x] Add a test fixture in `tests/test_live_readiness.py` with
  `active_backtest_information_ratio = -1.33`, `max_drawdown = -0.892`, and
  `turnover = 73.20`.
- [x] Run the focused test and verify it fails before implementation:
  `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_live_readiness.py' -v`.
- [x] Extend the audit schema to carry `active_backtest_max_drawdown`,
  `active_backtest_turnover`, `data_quality_gate_met`, and
  `strategy_quality_gate_met`.
- [x] Set conservative default release thresholds in the live-candidate bundle
  or audit case: minimum active IR above `0.30`, max drawdown no worse than the
  candidate budget, and turnover no higher than a documented budget.
- [x] Rebuild the multi-year audit with
  `PYTHONPATH=src python3 -m alpha_find_v2 build-multi-year-validation-audit --case research/examples/deployment_minimal/trend_leadership_multi_year_validation_audit_v1.toml`.
- [x] Verify the current candidate fails `strategy_quality_gate_met`.
- [x] Run
  `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_live_readiness.py' -v`
  and
  `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_deployment.py' -v`.

## Task 2: Build A Strategy Failure Attribution Report

**Files:**

- Create: `src/alpha_find_v2/strategy_failure_attribution.py`
- Create: `tests/test_strategy_failure_attribution.py`
- Modify: `src/alpha_find_v2/cli.py`
- Create output artifact: `output/trend_leadership_failure_attribution_20260429.json`

**Acceptance criteria:**

- The report decomposes failure by year, market state, industry, holding
  contribution, turnover, cost drag, blocked/partial orders, and overlay state.
- It uses the existing daily backtest JSON and source DB; it does not invent new
  alpha data.
- It identifies whether losses are concentrated or broad-based.

- [x] Add a parser for `portfolio_backtest_result` JSON summaries, daily curve,
  fills, orders, and holdings.
- [x] Add yearly and monthly return buckets from the daily equity curve.
- [x] Add top loser/top winner holding contribution using realized holding
  weights and daily raw returns.
- [x] Add industry contribution by joining holdings to
  `industry_classification_pit` in `output/research_source.duckdb`.
- [x] Add overlay-state comparison for dates with overlay observations.
- [x] Add CLI:
  `PYTHONPATH=src python3 -m alpha_find_v2 explain-strategy-failure --backtest output/trend_live_candidate_portfolio_with_overlay_daily_backtest.json --source-db output/research_source.duckdb --output output/trend_leadership_failure_attribution_20260429.json`.
- [x] Run
  `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_strategy_failure_attribution.py' -v`.

## Task 3: Decide Whether `trend_leadership` Is Repairable

**Files:**

- Create: `docs/research/trend-leadership-failure-review-2026-04-29.md`
- Reference: `output/trend_leadership_failure_attribution_20260429.json`
- Reference: `config/theses/trend_leadership.toml`
- Reference: `config/descriptor_sets/trend_leadership_core.toml`

**Acceptance criteria:**

- The review states one of three outcomes:
  `implementation_bug`, `parameterization_failed`, or `thesis_rejected`.
- If it claims repairability, it names the exact economic mechanism and the
  diagnostics that support that mechanism.
- If it rejects the thesis, it blocks further parameter search on this candidate
  version.

- [x] Read the attribution report and classify failure concentration.
- [x] Check whether the failure is consistent with A-share momentum crash,
  liquidity overpayment, industry crowding, or target-timing error.
- [x] Document the decision in
  `docs/research/trend-leadership-failure-review-2026-04-29.md`.
- [x] If no coherent mechanism survives, mark the candidate as rejected in the
  operations doc and leave the bundle only as a reproducible failure artifact.

## Task 4: Add Strategy Generation Guardrails

**Files:**

- Create: `src/alpha_find_v2/strategy_generation_guardrails.py`
- Create: `tests/test_strategy_generation_guardrails.py`
- Modify: `src/alpha_find_v2/cli.py`
- Create: `docs/architecture/strategy-generation-guardrails.md`

**Acceptance criteria:**

- Generated strategy manifests must bind a mandate, thesis, descriptor set,
  sleeve, target, portfolio recipe, cost model, data-quality audit, daily
  backtest, and promotion replay when applicable.
- Bare-return objectives such as `gross_return_only`, `ignore_costs`, and
  `ignore_tradeability` are rejected.
- No generated strategy can enter promotion review without executable evidence.

- [x] Define a manifest dataclass and JSON loader.
- [x] Validate object-chain references.
- [x] Reject bare-return or friction-ignoring objectives.
- [x] Require evidence paths for data-quality audit and daily portfolio
  backtest.
- [x] Add CLI:
  `PYTHONPATH=src python3 -m alpha_find_v2 validate-generated-strategy --manifest path/to/manifest.json`.
- [x] Run
  `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_strategy_generation_guardrails.py' -v`.

## Task 5: Resume Shadow-Live Only After Strategy-Quality Passes

**Files:**

- Modify: `src/alpha_find_v2/live_readiness.py`
- Modify: `tests/test_live_readiness.py`
- Modify: `docs/operations/trend-leadership-live-candidate-v1.md`
- Modify: `research/examples/deployment_minimal/shadow_live_journal_trend_leadership_v1.json`

**Acceptance criteria:**

- Shadow-live journal evaluation refuses candidates whose strategy-quality gate
  is not met.
- Probation remains blocked until at least `12` consecutive weekly cycles and
  `3` calendar months are recorded.
- Realized slippage, blocked trades, cash drag, and manual overrides are
  compared against the promoted expectation.

- [x] Add strategy-quality gate dependency to shadow-live journal evaluation.
- [x] Add stale-data guard before signal build.
- [x] Add ST/delisting runtime guard using `security_master_ref`.
- [x] Add realized-vs-modeled slippage comparison to manual execution outcome
  evaluation.
- [x] Run
  `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_live_readiness.py' -v`
  and
  `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_deployment.py' -v`.

## Task 6: Reassess Portfolio Scope

**Files:**

- Modify: `config/portfolio/a_share_core.toml` only if evidence supports it.
- Modify: `docs/status/project-current-state-2026-04-29.md`.
- Create: `docs/research/release-1-scope-review-2026-04-29.md`.

**Acceptance criteria:**

- The first release remains narrow unless a second sleeve improves the full
  portfolio path after costs and constraints.
- `fundamental_rerating_core` remains paused until residual exposure inputs are
  real.
- The portfolio does not combine weak sleeves simply to make the object chain
  look complete.

- [x] Review whether `a_share_core` should remain a two-sleeve recipe or be
  documented as an inactive target recipe while trend diagnosis continues.
- [x] Record the decision in
  `docs/research/release-1-scope-review-2026-04-29.md`.
- [x] Update current-state documentation if the active release scope changes.

## Verification Gates

Run after each implemented task:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 -m pytest -q
git diff --check
```

If `pytest` is not installed, record that gap and rely on the full unittest
suite plus focused CLI smoke checks.

## Stop Conditions

- Do not release new paper-trade signals while active IR and drawdown fail.
- Do not tune parameters before the failure attribution report exists.
- Do not add new data vendors to rescue strategy quality until current evidence
  decomposition proves a data-source limitation.
- Do not resume the residual fundamental lane without audited residual exposure
  inputs.
- Do not call any sleeve live-ready because data-quality gates are clean.

## Professional Exit Criteria

The project may move from "research framework ready" to "shadow-live candidate
ready" only when:

- data-quality gate is clean for the candidate portfolio evidence;
- strategy-quality gate passes on a 5+ year PIT-safe window;
- failure attribution does not reveal hidden concentration or accounting
  artifacts;
- shadow-live operating policy can record every cycle from signal to realized
  decay review;
- probation policy remains blocked until real shadow-live evidence exists.

Until then, treat the current roadmap as largely complete on process controls
and incomplete on candidate quality. The next forward step is not to loosen the
gates; it is to create a new candidate bundle that survives them.
