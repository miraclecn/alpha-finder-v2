# Trend Leadership Paper-Trade Signal Policy

## Purpose

Define the only allowed operating lane before small-capital probation:
manual signal execution in a simulated account.

## Release Rule

Before any new `trend_leadership_shadow_live_v1` signal is emitted, the frozen
candidate must pass its attached multi-year validation audit gate.

That gate is not satisfied by:

- one short favorable window
- a replay that omits PIT benchmark or industry history
- a portfolio path that ignores blocked exits, residual cash, or stressed cost

## Allowed State Source

Before real capital is in scope, deployment may start from either:

- a simulated account snapshot, or
- a manually maintained `portfolio_state_snapshot`

Both are acceptable only if they preserve:

- current holdings
- residual cash
- blocked entries
- blocked exits
- manual overrides and exceptions

## Hard Stops

- No broker automation.
- No real-account requirement in the pre-signal acceptance path.
- No signal release while the multi-year validation audit gate is blocked.
- No spreadsheet-side repair that removes blocked holdings or cash drag.
