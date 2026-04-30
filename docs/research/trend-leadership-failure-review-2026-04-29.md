# Trend Leadership Failure Review - 2026-04-29

## Decision

Outcome: `thesis_rejected`

This decision applies to the frozen `trend_leadership_shadow_live_v1`
candidate version, bound to:

- thesis: `config/theses/trend_leadership.toml`
- descriptor set: `config/descriptor_sets/trend_leadership_core.toml`
- attribution report: `output/trend_leadership_failure_attribution_20260429.json`

The candidate is not repairable through parameter search. Keep the bundle as a
reproducible failure artifact and diagnostic baseline only. Do not emit new
paper-trade signals, resume shadow-live, or tune holding count, weights,
rebalance cadence, or thresholds on this candidate version.

## Evidence

The overlay portfolio backtest covers `2021-03-05` through `2026-03-19`.

Key strategy-quality failures:

| Metric | Value |
| --- | ---: |
| Total return | `-87.76%` |
| Active annualized return | `-41.02%` |
| Active IR | `-1.33` |
| Max drawdown | `-89.20%` |
| Turnover | `73.20x` |
| Cost drag on initial cash | `-7.73%` |

The failure is persistent across years:

| Year | Return | Max drawdown |
| --- | ---: | ---: |
| `2021` | `-5.98%` | `-24.32%` |
| `2022` | `-48.05%` | `-48.29%` |
| `2023` | `-35.86%` | `-36.12%` |
| `2024` | `-49.61%` | `-50.14%` |
| `2025` | `-4.71%` | `-34.85%` |
| `2026` | `-24.22%` | `-33.09%` |

The loss attribution is broad-based:

- `790` losing holdings.
- `28` losing industries.
- top `5` holding losses explain only `3.05%` of total holding losses.
- top `3` industry losses explain only `33.67%` of total industry losses.
- industry coverage has `0` missing observations.

## Mechanism Checks

### Implementation Bug

No implementation bug is supported by the attribution evidence.

The failure does not trace to one data-quality or accounting pocket. The live
candidate audit reports zero exposure to corporate-action exceptions,
qfq-fallback prices, tradeability fallback, and market-data fallback in the
attached portfolio evidence. The attribution report is built from the daily
portfolio backtest, source DB returns, realized holdings, fills, orders, and
overlay observations, and the losses are broad across holdings, industries,
years, and overlay states.

This does not prove that every implementation detail is perfect. It does mean
that the observed capital loss should not be treated as an implementation bug
without a narrower reproducer.

### A-Share Momentum Crash

Momentum crash behavior is only a partial explanation.

The portfolio loses more when benchmark trend is `risk_off`
(`-0.282%` average daily return), but it also fails in `neutral` benchmark
trend (`-0.196%` average daily return) and does not earn meaningful positive
return when benchmark trend is `supportive` (`-0.002%` average daily return).

The overlay also fails to create a viable release candidate:

- `normal`: `-0.142%` average daily return
- `de_risk`: `-0.190%` average daily return
- `cash_heavier`: `0.453%` average daily return on only `2` observations

This is not a strategy that works in normal conditions and only crashes in rare
stress. It is a weak or negative selection process with stress amplification.

### Liquidity Overpayment

Liquidity and trading friction are real blockers, but not a sufficient repair
story.

The sleeve budget is `0.16` turnover, while the realized backtest turnover is
`73.20x` across the validation window. Annual turnover is double-digit in every
full year, and cost drag totals `772,594.10` CNY, or `7.73%` of initial cash.

That cost burden is unacceptable, but it cannot explain an `-87.76%` total
return or `-41.02%` active annualized return by itself. Lowering turnover might
reduce damage, but the attribution report does not show a positive raw
selection edge that costs merely obscure.

### Industry Crowding

Industry crowding is not the dominant failure.

The report classifies the failure as `broad_based`. Losses span `28`
industries, and the top `3` losing industries explain only `33.67%` of industry
losses. That is below the report's `50%` concentration rule.

There is also a construction credibility problem: the thesis declares
`industry_relative_strength` as required data, but
`trend_leadership_core.toml` uses only `medium_term_relative_strength`,
`trend_stability`, and `turnover_confirmation`. The current candidate therefore
does not prove a clean industry-leadership thesis. It also does not provide
evidence that simply wiring the missing descriptor would repair the broad
negative path.

### Target-Timing Error

The report does not support target-timing error as the primary diagnosis.

The configured target is next-day open entry to 20-day open exit, and the
portfolio backtest is the same historical evidence lane used for live-candidate
admission. A target-timing repair would need a new target-surface experiment
with a pre-registered rationale. It is not a justification for parameter search
on this failed candidate.

## Operating Consequences

The frozen candidate version is rejected:

- no further parameter search on `trend_leadership_shadow_live_v1`
- no new paper-trade signals from this bundle
- no shadow-live catch-up attempt to rehabilitate historical failure
- no small-capital probation path from this evidence package
- no portfolio-scope expansion that treats this sleeve as live-ready

Any future trend work must start as a new versioned research object with a
specific mechanism, a descriptor set that actually binds that mechanism, and a
fresh evidence package. It must not reuse this bundle as a tunable capital
candidate.
