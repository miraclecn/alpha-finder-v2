# Leader Pullback Continuation Design

## Scope

This slice defines a new price/trend research object for the A-share long-only
EOD lane.

The object is designed to capture leader pullback continuation with explicit
industry binding, next-open execution realism, and admission-first portfolio
construction.

It does not repair or retune the frozen
`trend_leadership_shadow_live_v1` candidate.
It does not port any V1 `factor -> strategy -> promotion` logic.
It does not introduce a broad multi-sleeve release path before the new object
earns its own evidence package.

## Context

The repository already has machine-checkable diagnosis, guardrails, and
admission evaluation.
The current gap is not missing gate machinery.
The current gap is the absence of a new admissible research object after the
rejection of the existing trend live-candidate bundle.

The frozen candidate fails for strategy quality, not for a missing accounting
or tradeability control.
It also has a construction mismatch: the old trend thesis names
`industry_relative_strength` as required data, but the executable descriptor set
does not bind it.

Future trend work therefore needs a new versioned object with:

- a narrower mechanism
- a descriptor set that actually binds that mechanism
- a fresh evidence package built under the same promotion discipline

## Recommended Object

Use a new thesis family and a new bound research object:

- thesis id: `leader_pullback_continuation`
- descriptor set id: `leader_pullback_continuation_v1`
- sleeve id: `leader_pullback_continuation_v1`
- target id: `open_t1_to_open_t20_net_cost`

The mechanism is:

- identify medium-term price leaders
- require that the stock is still leading within its own industry
- prefer orderly pullback-and-resume paths over noisy end-stage spikes
- keep the construction biased toward lower turnover and fewer broad-spectrum
  losses

Economically, this is not a pure momentum-chase object.
It is a second-leg continuation object that assumes the stock has industry
support, survives a controlled pullback, and remains liquid enough to trade at
the next open.

## Rejected Alternatives

`reweighted_trend_resilience_v2`

- too close to the existing comparator lane
- more likely to become a renamed reweight of current trend descriptors than a
  truly new mechanism

`industry_filter_not_score_v1`

- leaves industry leadership outside the main alpha ranking
- weakens the claim that industry support is part of the mechanism rather than
  just a coarse admission screen

Standalone `pullback_depth` descriptor in `v1`

- adds implementation surface before the repo has shown that the existing
  `trend_stability` descriptor is insufficient
- risks turning the object into either late breakout chasing or broken-trend
  mean reversion

## Descriptor Design

`leader_pullback_continuation_v1` should use four descriptors:

- `medium_term_relative_strength` with weight `0.35`
- `industry_relative_strength` with weight `0.30`
- `trend_stability` with weight `0.25`
- `turnover_confirmation` with weight `0.10`

Role assignment:

- `medium_term_relative_strength` keeps the object anchored to real leaders
- `industry_relative_strength` binds the within-industry leadership claim into
  the main score
- `trend_stability` expresses path quality and controlled pullback behavior
- `turnover_confirmation` remains a tradeability confirmation, not a primary
  alpha source

This object intentionally does not add a new `pullback` descriptor in `v1`.
For the first version, pullback continuation is represented by the joint effect
of:

- leader status
- industry support
- orderly path behavior

That keeps the first implementation narrow and auditable.

## Execution Dependency

The current trend research input builder does not yet support
`industry_relative_strength` in the executable trend path.

For this object, that gap is not a warning-grade condition.
It is a blocking condition.

If the descriptor set requests `industry_relative_strength` and the build path
cannot compute it, the build must fail rather than emit a degraded observation
input with an `industry_relative_branch_blocked` warning.

This object is invalid unless industry-relative leadership is present in the
executable score path.

## Sleeve Design

`leader_pullback_continuation_v1` should be an admission-first sleeve with the
following construction:

- universe: `investable_a_share_core`
- execution rule: `next_day_open`
- rebalance frequency: `biweekly`
- target holding days: `20`
- neutralization: `industry`

Recommended defaults:

- `holding_count = 22`
- allowed operating range: `20` to `24`
- `weight_cap = 0.06`
- `turnover_budget = 0.10` to `0.12`
- `min_median_daily_turnover_cny_mn = 120`
- `exclude_price_limit_lock = true`
- `single_industry_name_cap = 3`

Construction intent:

- lower turnover than the rejected weekly trend sleeve
- broader name count than the old concentrated trend sleeve
- explicit control against hot-industry crowding
- no forced backfill of weak names just to reach the target count

If admission rules produce fewer than `20` admissible names, the sleeve should
hold fewer names instead of lowering its own threshold to stay full.

## Target And Cadence

The target surface remains `next_open -> 20d open net cost`.

The object should not change target timing in `v1`.
The design goal is to test a new mechanism under the same execution surface that
rejected the old live candidate.

`biweekly` cadence should be implemented through `rebalance_stride = 10` in the
trend research build cases so the object remains compatible with the current
framework.

## Validation Design

Validation must follow an absolute-first, relative-second sequence.

### 1. Object Integrity

The new research object must be built as a fresh versioned chain with its own:

- thesis config
- descriptor set
- sleeve config
- research input build case
- promotion replay case
- multi-year validation audit case

The old `trend_leadership_shadow_live_v1` bundle remains a frozen failure
baseline only.
It must not be reused as a tunable candidate.

### 2. Absolute Strategy-Quality Gate

Before any comparison to the old trend object, the new object must pass the
same research-quality discipline already used in repo:

- at least `5` calendar years of validation window
- benchmark-relative daily portfolio evidence
- active IR above `0.30`
- max drawdown no worse than `0.18`
- turnover no worse than the pre-registered audit limit
- no promotion-blocking market-data fallback exposure
- no corporate-action exception exposure in the attached evidence package

The object does not qualify merely because it is less bad than the rejected
baseline.

### 3. Relative Comparison

If the absolute gate is met, compare the new object against both:

- `trend_leadership_shadow_live_v1`
- `trend_resilience_core`

The comparison must check simultaneous improvement in:

- active IR
- max drawdown
- realized turnover
- cost drag

An object that improves only turnover but still lacks positive selection edge
does not validate the thesis.
An object that improves only in one regime window but not across the main
validation path also does not validate the thesis.

### 4. Mechanism Checks

The new object claims industry-supported continuation.
The evidence package must therefore include explicit checks for:

- executable inclusion of `industry_relative_strength`
- industry concentration after applying `single_industry_name_cap`
- behavior change when cadence is tightened from `biweekly` back to `weekly`

Interpretation rule:

- if weekly cadence mainly worsens turnover while leaving the core signal intact,
  the object is likely benefiting from the intended slower continuation harvest
- if the signal disappears entirely when cadence changes, the result is more
  likely cadence-sensitive noise than a stable mechanism

Any build that still emits `industry_relative_branch_blocked` for this object
should be treated as invalid and should not proceed to promotion replay.

### 5. Portfolio Promotion Replay

Only after the object clears its own strategy-quality bar should it enter
portfolio promotion replay.

That replay should continue to use the existing portfolio-gate language:

- base and high cost scenarios
- bull, bear, high-dispersion, and low-dispersion regime requirements
- component correlation limits
- correlation-to-existing-portfolio limits
- marginal IR contribution
- marginal drawdown increase

This keeps the object tied to portfolio usefulness instead of standalone sleeve
appearance.

## Error Handling

The implementation should fail closed in these cases:

- requested descriptor set contains an unsupported executable descriptor
- industry PIT coverage is missing for required trade dates
- attached evidence crosses promotion-blocking fallback windows
- replay or audit inputs do not form one coherent object chain

No part of this object should silently downgrade industry binding from
"required" to "best effort."

## Testing Requirements

Implementation planning for this design should include:

- unit tests for loading the new thesis, descriptor set, and sleeve
- builder tests proving executable support for `industry_relative_strength`
- builder tests proving the object fails when the industry branch cannot be
  computed
- research input case coverage for `rebalance_stride = 10`
- replay and multi-year audit tests that preserve the absolute gate semantics
- regression coverage that keeps the old failed bundle frozen and comparable

## Non-Goals

- no attempt to rehabilitate `trend_leadership_shadow_live_v1`
- no immediate second descriptor wave for custom pullback math
- no release of paper-trade or shadow-live signals before a new evidence package
  clears the existing gates
- no expansion of the inactive `a_share_core` reference portfolio based on this
  design alone
