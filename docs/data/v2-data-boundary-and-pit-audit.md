# V2 Data Boundary and PIT Audit

## Why This Document Exists

V2 is not allowed to inherit V1 data products blindly.

If the data boundary is loose, every downstream object becomes contaminated:

- descriptors become lookahead-tainted
- executable targets become misaligned to the trading clock
- portfolio promotion becomes a test of mislabeled returns instead of real alpha

For V2, the data layer is an economic control surface, not just ETL plumbing.

## Production Scope

This audit is written for one concrete product:

- China A-share long-only cash equities
- end-of-day research
- next-session open execution
- weekly or twice-weekly rebalance
- 15 to 30 names
- explicit T+1, suspension, price-limit, lot-size, and liquidity constraints

Anything outside this scope is not blocked forever, but it is not allowed to weaken the first V2 boundary.

## Binding PIT Rule

Every field used in research must answer one question:

`Could this value have been known by the research process before the trade was sent?`

V2 therefore needs an explicit `available_at` concept.

A row is valid for a signal formed on trade date `t` only if:

- `available_at <= research_cutoff(t)`
- any later vendor correction is ignored until its own later `available_at`
- historical constituents, industry codes, and corporate actions are read using their historical effective state, not today's state

## Required Time Axes

For any dataset that can revise, restate, or arrive late, keep these timestamps if the source allows it:

- `observation_at`: when the underlying economic event happened
- `published_at`: when the issuer / exchange / vendor first published it
- `effective_at`: when the value became effective for trading or classification
- `available_at`: when V2 research is allowed to use it
- `ingested_at`: when the local system captured it
- `revision_id` or equivalent sequence: which version of the record this is

`available_at` is the binding field for label building and signal generation.

## V1 Reuse Policy

### Reuse After Explicit V2 Field Audit

These domains are candidates for reuse if schema meaning and historical behavior are confirmed:

- security master and listing-status history
- daily OHLCV, turnover, float, and adjustment factors
- suspension state and price-limit state
- benchmark index levels and benchmark constituent history
- historical industry classification
- corporate-action files needed for split / dividend handling
- QMT handoff interface and execution constraint plumbing

### Reuse Only After Full PIT Re-Audit

These domains often look reusable but are high-risk without timestamp proof:

- PIT fundamentals and statement snapshots
- announcement dates and event calendars
- earnings, pre-announcement, and guidance tags
- shareholder changes, unlocks, placements, buybacks
- northbound flow, margin financing, and crowding proxies
- any vendor-derived mart that already compresses or normalizes raw inputs

### Do Not Import Forward Into V2

These are explicitly banned as V2 source-of-truth:

- legacy factor tables
- legacy strategy tables
- precomputed forward-return labels
- factor evaluation marts
- top-N portfolio outputs
- promotion or approval flags from V1

They may remain as benchmark archives only.

## Dataset Acceptance Classes

Each dataset must be tagged with one of these states:

- `green`: audited, PIT-safe for the intended V2 use
- `amber`: structurally useful, but blocked on timestamp or coverage audit
- `red`: not acceptable for V2 research use

No dataset may be used by live-facing V2 research unless it is `green`.

For the current personal-stack release, V2 also groups data by operating role:

- `green`: production truth for promotion-safe research
- `amber`: slow anchor data with conservative lag assumptions
- `experimental`: useful for exploration, but blocked from promotion

## First-Round V2 Dataset Registry

### 1. `security_master_pit`

Purpose:
identity, listing age, board, ST status, delisting path, lot size, float status

Minimum fields:
- `security_id`
- `trade_date`
- `listed`
- `listing_days`
- `is_st`
- `board`
- `lot_size`
- `free_float_shares`

Status target:
`green`

### 2. `daily_bar_pit`

Purpose:
open-to-open target construction, liquidity screens, gap behavior, volatility context

Minimum fields:
- `security_id`
- `trade_date`
- `open`, `high`, `low`, `close`
- `volume`, `turnover_value`
- `vwap_proxy` if available
- adjusted and unadjusted references

Status target:
`green`

### 3. `tradeability_state_daily`

Purpose:
A-share execution realism for entry and exit eligibility

Minimum fields:
- `security_id`
- `trade_date`
- `is_suspended`
- `is_limit_up_locked`
- `is_limit_down_locked`
- `liquidity_pass`
- `median_daily_turnover_cny_mn`

Status target:
`green`

### 4. `benchmark_membership_pit`

Purpose:
historical benchmark and universe definition, not today's constituents projected backward

Minimum fields:
- `benchmark_id`
- `security_id`
- `effective_at`
- `removed_at`

Status target:
`green`

### 5. `industry_classification_pit`

Purpose:
historical industry neutralization and sector-relative descriptors

Minimum fields:
- `security_id`
- `industry_schema`
- `industry_code`
- `effective_at`
- `removed_at`

Status target:
`green`

### 6. `fundamental_snapshot_pit`

Purpose:
valuation, profitability, accrual, leverage, capital-efficiency descriptors

Minimum fields:
- `security_id`
- `period_end`
- `published_at`
- `available_at`
- normalized point-in-time statement fields

Status target:
`amber` until publication timing and restatement handling are proven

### 7. `event_calendar_pit`

Purpose:
earnings underreaction and event-driven sleeves

Minimum fields:
- `security_id`
- `event_type`
- `event_observation_at`
- `published_at`
- `available_at`
- whether the event arrived pre-open / intraday / post-close

Status target:
`amber` until same-day versus next-day usability is proven

### 8. `estimate_revision_proxy_pit`

Purpose:
revision breadth and expectation-reset descriptors

Minimum fields:
- `security_id`
- `published_at`
- `available_at`
- revision direction
- breadth / coverage count

Status target:
`amber`

### 9. `flow_crowding_daily`

Purpose:
future flow and anti-consensus sleeves, but not required for first production launch

Minimum fields:
- `security_id`
- `trade_date`
- northbound flow proxy
- margin financing proxy
- abnormal turnover proxy
- crowding or breadth proxy

Status target:
`amber`

## Thesis-Level Minimum Data

### Fundamental Rerating

Must have before production research:

- sector-relative valuation
- profitability quality
- accrual quality
- leverage
- capital efficiency
- historical industry membership
- audited reporting lag rules

This thesis is allowed to launch first because the required data are slower-moving and more auditable.

### Trend Leadership

Must have before production research:

- adjusted daily price history
- turnover and liquidity history
- suspension and price-limit state
- historical benchmark membership
- historical industry classification

Preferred confirmations:

- industry-relative strength
- trend stability versus chaotic blow-off moves
- turnover expansion without illiquid mark-up behavior

This thesis is the best first production fit for a personal stack because the signal clock, execution clock, and data availability are all easy to audit.

### Earnings / Event Underreaction

Must have before production research:

- earnings and pre-announcement timestamps
- surprise or revision proxy with real availability timing
- post-event gap and turnover reaction
- entry and exit tradeability around event windows

This thesis is valuable, but only if event timing is real. A mislabeled event clock will create fake alpha immediately.

### Flow / Liquidity Reversal

Blocked until:

- robust flow proxies exist
- high-cost and stress-cost scenarios exist
- tradeability around limit states is audited

### Crowding / Anti-Consensus

Blocked until:

- crowding proxy is explicit
- breadth deterioration signal is auditable
- crowding is separable from generic small-cap and illiquidity exposure

## PIT Audit Checklist

Every candidate dataset must pass the following audit:

1. Meaning audit
   The economic meaning of every field is documented. No opaque vendor shorthand without translation.

2. Timestamp audit
   `published_at` and `available_at` are either stored directly or reconstructed defensibly.

3. Revision audit
   If a value can restate, V2 keeps the historical version sequence or proves restatements are irrelevant.

4. Coverage audit
   Missingness is measured by date, market segment, and industry, not only in aggregate.

5. Survivorship audit
   Dead, delisted, suspended, and ST names remain visible historically.

6. Tradeability audit
   Suspensions, price limits, and liquidity failure states are present at the daily level used by target labels.

7. Corporate-action audit
   Price series, share counts, and valuation inputs are aligned to the same adjustment convention.

8. Historical-membership audit
   Benchmark and industry memberships are versioned historically.

9. Lag-rule audit
   Slow data use explicit reporting lags; event data use publication timestamps, not period labels.

10. Leak test
   Sample a few dates manually and prove no field from `t+1` is visible at `t`.

## Research Use Rules

V2 research is allowed to use only:

- `green` datasets directly
- `amber` datasets in prototype work only after the exact failure mode is written down

V2 live-candidate promotion is not allowed to use `amber` data.

## Immediate Implementation Consequences

The next code objects should depend on this boundary:

1. `cost model registry`
   Target labels cannot be called executable unless the trading-cost assumption is versioned.

2. `executable residual target builder`
   Must combine target config, cost model, tradeability state, and residual components into a real label.

3. `portfolio promotion gate evaluator`
   Must test marginal portfolio value, not just standalone sleeve beauty.

## Practical Verdict

For V2, the safest first live path is:

- launch `trend_leadership` first on top of audited daily market, tradeability, benchmark, and industry data
- keep `fundamental_rerating` as a slower quality/value anchor after reporting-lag rules are audited
- launch `earnings_underreaction` only after event timing is genuinely point-in-time safe
- keep `flow_liquidity_reversal` and `crowding_anti_consensus` in research quarantine until the missing data domains are green
