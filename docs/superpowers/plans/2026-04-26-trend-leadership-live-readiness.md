# Trend Leadership Live Readiness

## Goal

Turn `trend_leadership_core` from the current honest release-1 research and
semi-auto deployment lane into a live-candidate personal strategy that is
eligible for:

- `shadow live`
- small-capital probation

This document does not authorize broker automation or full-capital live
deployment.
Its job is narrower:
freeze the minimum evidence, data, and operations work required before
`trend_leadership` may claim real-money readiness.

## Current Verified State

- `config/theses/trend_leadership.toml` already defines the main production
  thesis for the first-release personal stack.
- `docs/architecture/a-share-personal-data-research-doctrine.md` already fixes
  `trend_leadership` as the primary alpha sleeve and `risk_regime_filter` as
  the portfolio-level exposure governor.
- `docs/superpowers/plans/2026-04-25-phase-2-trend-production-lane.md`
  already locks the current honest real-output trend mainline and makes any
  earlier benchmark-industry expansion contingent on a fresh PIT audit.
- `docs/superpowers/plans/2026-04-25-phase-3-regime-overlay-and-promotion.md`
  already defines the `regime_overlay` object boundary and the
  `normal / de_risk / cash_heavier` state machine.
- `docs/superpowers/plans/2026-04-25-phase-4-semi-auto-deployment-and-ops.md`
  already closes the minimal execution trace loop through:
  `run_manifest -> manual_execution_outcome -> realized_trading_window ->
  decay_record`.
- `research/examples/promotion_replay_real_output/README.md` already defines
  the current honest replay lane driven by generated `output/` artifacts.
- `research/examples/deployment_minimal/README.md` already defines the current
  executable-signal, run-manifest, and decay-watch bridge.
- Reverified on `2026-04-26`, the repo can already run:
  `build-benchmark-state -> build-trend-research-input ->
  build-sleeve-artifact -> run-promotion-replay -> build-executable-signal ->
  build-run-manifest -> evaluate-decay-watch`.
- Reverified on `2026-04-26`, the repo test suite currently passes under:
  `PYTHONPATH=src python3 -m unittest discover -s tests -v`.
- Reverified on `2026-04-26`, overlay states are no longer replay-only
  annotations: `portfolio_promotion_replay` now applies overlay gross-exposure
  changes to candidate portfolio weights and returns, including walk-forward
  slices.
- The current live-readiness blockers are no longer object-chain blockers.
  They are evidence and operating blockers:
  history length, tradeability realism, paper-trade operating discipline,
  shadow-live evidence, and probation policy.

## In Scope

- define the minimum path from the current release-1 trend lane to
  `shadow live`
- define the minimum additional evidence required before any small-capital
  probation may begin
- keep `trend_leadership_core` as the only first-release alpha sleeve
- require `risk_regime_filter` to act as a real deployment governor instead of
  a doctrine-only idea
- formalize the personal operating and review loop needed for honest live
  admission

## Out of Scope

- broker-direct APIs, QMT routing, or unattended live order submission
- reopening the paused residual fundamental lane to justify trend deployment
- free-form strategy mining or automated thesis discovery
- expanding release-1 into a multi-alpha production stack
- claiming full-capital live readiness from the current sub-1-year honest
  window alone

## Dependencies

- `config/theses/trend_leadership.toml`
- `docs/architecture/a-share-personal-data-research-doctrine.md`
- `docs/architecture/deployment-ladder-and-decay.md`
- `docs/architecture/research-operating-model.md`
- `docs/data/v2-data-boundary-and-pit-audit.md`
- `docs/superpowers/plans/2026-04-25-phase-2-trend-production-lane.md`
- `docs/superpowers/plans/2026-04-25-phase-3-regime-overlay-and-promotion.md`
- `docs/superpowers/plans/2026-04-25-phase-4-semi-auto-deployment-and-ops.md`
- `research/examples/promotion_replay_real_output/README.md`
- `research/examples/deployment_minimal/README.md`

## Execution Breakdown

### 1. Restore A Multi-Year Honest Validation Window

The current checked-in trend lane proves that the object chain is real.
It does not prove that the thesis is stable enough for capital.

Required next state:

- one audited `trend_leadership` validation interval covering at least
  `5` consecutive calendar years as a hard floor
- where PIT-safe history exists, the preferred target is a wider
  `5` to `10` year interval rather than stopping at the first barely-valid
  `5` year slice
- the interval must preserve historical benchmark membership,
  historical industry classification, and trend tradeability realism on every
  decision date
- the validated window must contain multiple market states:
  drawdown, rebound, broad trend, and weak-breadth / fragile leadership
  subperiods
- anchored walk-forward and regime-split evidence must be rerun on the widened
  interval, not only on the old `2026-03-05+` smoke slice

This was the first live-readiness blocker.
No later step can compensate for a short, flattering, or partially synthetic
validation window.
The original `2026-03-05` through `2026-03-19` checked-in slice remains only a
historical pipeline smoke sample.

As of `2026-04-27`, the benchmark + `sw2021_l1` constituent coverage boundary
has been widened materially: the fresh CSI 800 audit spans `2014-02-21`
through `2026-04-23` and covers `2,364,800 / 2,364,800` staged
constituent-days. A temporary benchmark-state build over the same interval also
succeeds.

The downstream blocker is now closed for the frozen trend-leadership candidate:
Beijing-board names are excluded from the live-tradable trend input because the
account will not trade them, and `302132.SZ` is covered by a narrow
`security_code_alias_backfill` from legacy `300114.SZ` industry intervals. The
rebuilt audit spans `2021-03-05` through `2026-03-19`, covers `5.0404`
calendar years after the T+20 exit horizon, and reports no blockers.

Stop if the widened history depends on:

- static industry labels projected backward
- non-audited benchmark membership fills
- synthetic tradeability reconstruction hidden as production truth

### 2. Freeze One Live-Candidate Trend Bundle

Before shadow-live begins, the repo must freeze one explicit
`trend_leadership` live-candidate bundle:

- thesis file
- descriptor set
- sleeve config
- target definition
- portfolio recipe
- cost scenarios
- overlay binding, if used

The freeze rule is simple:

- after the live-candidate bundle is declared, no parameter, screening rule,
  or portfolio budget may change without opening a new candidate version
- research may continue elsewhere, but the shadow-live lane must stay attached
  to one frozen candidate

The current checked-in trend config remains the research trunk until that live
candidate bundle is versioned explicitly.

### 3. Raise Tradeability And Cost Realism To Live-Candidate Standard

The release-1 spine already preserves execution realism as a first-class
constraint.
Live readiness requires the next step:
turn that realism from a generic research assumption into a personal execution
model.

Required next state:

- keep `T+1`, suspension, limit-state, lot-size, liquidity, and cash-residual
  handling explicit on every decision date
- preserve both base and stressed research cost scenarios
- add a personal live-calibrated cost view built from observed manual execution
  records once shadow-live begins
- treat blocked entries, blocked exits, and persistent cash drag as measured
  execution facts rather than one-off anecdotes

The strategy is not live-ready if its claimed edge survives only under
optimistic cost assumptions or under a cleaner tradeability surface than the
account can actually achieve.

### 4. Make Portfolio-Level Evidence Binding

`trend_leadership` is not admitted because the sleeve chart looks good.
It is admitted only if the actual candidate portfolio remains acceptable after:

- concentration limits
- benchmark-relative industry caps
- turnover costs
- cash drag from honest constraints
- overlay interaction, if used

Required next state:

- replay and promotion outputs must be judged from the candidate portfolio path,
  not only from raw sleeve metrics
- walk-forward, regime-breakdown, concentration, and incrementality surfaces
  must all stay part of the admission evidence
- the live-candidate portfolio must publish its expected turnover budget,
  breadth range, drawdown budget, and weak-regime behavior explicitly before
  shadow-live begins

### 5. Bind `risk_regime_filter` As A Real Deployment Governor

For `trend_leadership`, the overlay is not optional decoration.
It is the first controlled way to avoid forcing full-risk deployment when
leadership narrows or volatility turns unstable.

Required next state:

- the live candidate must declare whether deployment uses `regime_overlay`
- if used, every decision date must produce either:
  `normal`, `de_risk`, or `cash_heavier`
- missing or invalid overlay inputs must trigger the documented conservative
  downgrade path, not silent omission
- the shadow-live record must preserve both the overlay state and any resulting
  exposure change

The strategy is not live-ready if the overlay exists only in replay documents
but disappears at the point of deployment.

### 6. Keep Deployment State Honest During Paper Trading And Defer Real-Account Plumbing

The current real-output deployment example still uses a synthetic account
snapshot.
That is acceptable for release-1 paper-trade preparation as long as the
strategy does not claim real-capital readiness from it.

Required next state:

- allow the shadow-live lane to be driven by a simulated account or explicit
  `portfolio_state_snapshot` while capital remains zero
- preserve blocked entries, blocked exits, residual cash, and manual overrides
  honestly inside:
  `portfolio_state_snapshot -> executable_signal -> run_manifest ->
  manual_execution_outcome -> realized_trading_window`
- keep any future broker/account export contract documented separately and out
  of the pre-signal acceptance path until small-capital probation is actually
  in scope

The goal here is not broker automation.
Manual paper execution is acceptable.
Silent spreadsheet-side state repair is not.

### 7. Run A Shadow-Live Trace Loop Long Enough To Matter

After the live-candidate bundle is frozen and the multi-year audit gate is
clear, the next gate is not immediate capital deployment.
It is `shadow live`.

Minimum shadow-live admission record:

- at least `12` consecutive weekly decision cycles
- at least `3` consecutive calendar months
- preferred target before probation:
  `20` weekly cycles and `6` calendar months

Every cycle must preserve:

- `run_manifest`
- `manual_execution_outcome`
- `realized_trading_window`
- `decay_record`

The shadow-live goal is not to maximize paper return.
It is to measure:

- execution drift versus research expectations
- blocked-trade persistence
- realized turnover versus modeled turnover
- cash drag and manual overrides
- whether the thesis survives actual operating friction

### 8. Publish A Small-Capital Probation Policy And Kill Switch

No strategy should jump from shadow-live directly to normal capital.
The required first live step is a bounded probation policy.

Required next state:

- one written small-capital probation policy exists in-repo
- initial probation capital is capped at no more than `10%` of the intended
  steady-state `trend_leadership` allocation
- the strategy may advance from shadow-live to probation only after the
  shadow-live gate above is satisfied
- the policy must define immediate pause conditions for:
  data integrity failure, broken account-state chain, or missing execution
  writeback
- the policy must define mandatory review conditions for:
  materially worse realized drawdown, turnover, blocked-trade pressure, or cash
  drag than the promoted expectation
- the policy must define retirement-candidate conditions for repeated
  post-cost underperformance across explicit validation windows

Until this policy exists, the valid state is still:
`research-ready and shadow-live eligible`, not `capitalized live`.

## Verification Matrix

The existing command chain remains the trunk verification base:

- `PYTHONPATH=src python3 -m unittest discover -s tests -v`
- `build-benchmark-state --case research/examples/benchmark_state_build_minimal/csi800.toml`
- `build-trend-research-input --case research/examples/trend_input_build_minimal/trend_leadership_core.toml`
- `build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_output.toml`
- `run-promotion-replay --case research/examples/promotion_replay_real_output/replay_case.toml`
- `build-executable-signal --case research/examples/deployment_minimal/executable_signal_real_output_case.toml`
- `build-run-manifest --case research/examples/deployment_minimal/run_manifest_case.toml`
- `evaluate-decay-watch --case research/examples/deployment_minimal/decay_watch_case_with_realized_window.toml`

Live-readiness requires additional evidence beyond those commands:

- `build-multi-year-validation-audit --case research/examples/deployment_minimal/trend_leadership_multi_year_validation_audit_v1.toml`
- one audited multi-year trend validation interval exists and is reproducible
  with a hard floor of `5` years and a preferred target of `5` to `10` years
- one frozen live-candidate trend bundle is versioned and documented
- one live-calibrated execution-cost view is published after shadow-live begins
- one paper-trade operating policy exists and keeps the deployment chain honest
- at least `12` consecutive weekly shadow-live cycles are traceable from
  manifest to decay record
- one explicit probation-capital and kill-switch policy is written in-repo

## Stop Conditions

- the strategy is described as live-ready while the widened trend-input rebuild
  still fails on selected securities without PIT industry labels
- the old short `2026-03-05` smoke window is treated as current release-grade
  evidence
- live claims depend on synthetic account-state inputs while pretending they
  are already real-capital evidence
- tradeability or cost realism is weakened to preserve a backtest story
- overlay logic is bypassed silently during deployment
- shadow-live records are missing, incomplete, or not linked by `run_id`
- probation capital starts before the written policy exists

## Exit Criteria

- `trend_leadership` has one audited multi-year validation window suitable for
  portfolio-level replay
- one frozen live-candidate trend bundle is documented and versioned
- tradeability and cost realism are judged against actual personal execution
  evidence, not only research assumptions
- one paper-trade operating policy feeds the deployment chain honestly before
  any real-capital probation begins
- the shadow-live loop has accumulated enough traced evidence to compare
  research expectations with realized outcomes
- one explicit probation-capital and kill-switch policy exists
- only then may the strategy claim:
  eligible for small-capital probation
