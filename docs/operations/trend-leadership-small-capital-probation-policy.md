# Trend Leadership Small-Capital Probation Policy

## Entry Gate

`trend_leadership_shadow_live_v1` may advance from shadow-live to probation
only after all of the following are true:

- the frozen live-candidate bundle remains unchanged
- the honest shadow-live journal contains at least `12` consecutive weekly cycles
- the same journal spans at least `3` calendar months
- the multi-year audited validation requirement is no longer an open blocker

## Initial Capital Cap

Initial probation capital must stay at or below `10%` of the intended
steady-state `trend_leadership` allocation.

## Immediate Pause Conditions

- data integrity failure in benchmark, sleeve, or account-state inputs
- missing `run_manifest`, `manual_execution_outcome`, or `realized_trading_window`
- broken `run_id` chain across execution artifacts
- overlay state missing on a date where the frozen candidate requires it

## Mandatory Review Conditions

- realized drawdown materially worse than the promoted expectation
- realized turnover materially above budget
- blocked-trade pressure persistently above the promoted expectation
- cash drag persistently above the promoted expectation

## Retirement-Candidate Conditions

- repeated post-cost underperformance across explicit validation windows
- repeated evidence that manual execution friction destroys the expected edge
- repeated reliance on manual overrides to keep the thesis investable
