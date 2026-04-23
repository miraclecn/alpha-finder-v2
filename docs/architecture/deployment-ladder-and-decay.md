# Deployment Ladder and Decay

## Why This Layer Exists

Promotion replay is still not the same thing as a tradable system.

After a portfolio recipe is approved, V2 still has to answer two different questions:

- what exactly should be sent to tomorrow's execution workflow
- how do we decide later that a once-good portfolio has decayed enough to watch or retire

Those are different finance problems and they need different objects.

## Required Object Chain

The V2 deployment spine is now:

`portfolio recipe + account state snapshot -> portfolio state snapshot -> execution policy -> executable signal package -> realized trading window -> decay monitor -> decay record`

Each object has one job:

- `account state snapshot`: records the broker/account-side book in cash, market value, available shares, and trade restrictions
- `portfolio state snapshot`: records the deployment-ready live book after the audited account snapshot is mapped into portfolio weights and blocked trade state
- `execution policy`: defines how approved targets become weight deltas under the mandate's trading clock
- `executable signal package`: records the next rebalance as target deltas, blocked trades, residual cash, and estimated cost
- `realized trading window`: captures what the portfolio actually delivered in paper, shadow, or live monitoring
- `decay monitor`: defines how realized behavior is judged against the promotion baseline
- `decay record`: records whether the portfolio remains healthy, moves to watch, or should be retired

## Executable Signal Package Boundary

For the first release, the execution package is weight-based rather than share-count-based.

That is deliberate.
The first personal A-share system needs to stay honest about:

- current weight
- proposed target weight
- executable target weight after tradeability constraints
- estimated turnover and cost
- which entries or exits were blocked
- how much cash remains after honest constraint handling

This is enough to govern paper and shadow deployment before broker-specific routing is introduced.

## Why Portfolio State Is A Separate Object

Approved targets are not the same thing as the live book.

On an A-share rebalance date, deployment still has to know:

- what is currently held
- how much cash is actually free
- which current positions cannot be exited because they are suspended, limit-locked, or otherwise blocked
- which planned entries cannot be initiated even if research wants them

Those conditions are path-dependent and belong to deployment state, not to sleeve research.
The raw account snapshot is therefore kept separate from the portfolio-state snapshot:
the account object stays close to broker reality, while the portfolio-state object stays aligned to the portfolio construction and execution engine.

Without a separate portfolio-state snapshot, a system drifts into a false assumption:
anything missing from today's target list can be sold immediately.

That is not finance-honest in A-shares.
Blocked legacy holdings must be able to survive into the executable package without forcing the system to over-allocate other names beyond the approved target book.

## Decay Record Boundary

Decay cannot be monitored with a naked PnL chart.

A promoted portfolio should be judged relative to the expectations that justified promotion:

- information ratio versus promoted expectation
- drawdown multiple versus promoted drawdown
- turnover versus the approved budget
- breadth retention
- blocked-trade pressure

That is why the first V2 decay monitor is benchmarked to the promotion snapshot, not to an arbitrary rolling threshold.

## First Release Non-Goals

The deployment layer still does not include:

- broker or QMT integration
- share rounding against live prices
- intraday slicing or smart execution
- persistent OMS state or reconciliation

Those are later operational layers.
The current goal is narrower and more important:
make the research-to-deployment-to-decay control chain explicit and testable.
