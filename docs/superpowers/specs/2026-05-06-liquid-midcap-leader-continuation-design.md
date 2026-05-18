# Liquid Midcap Leader Continuation Design

## Scope

This design turns the ETF rotation ideas into a new A-share stock research
object. It creates a fresh V2 chain and does not tune or rename the rejected
`trend_leadership_shadow_live_v1` evidence bundle.

The object id is `liquid_midcap_leader_continuation_v1`.

## Mechanism

The strategy looks for liquid mid-cap A-share stocks whose medium-term trend is
strong, smooth, and still early enough to avoid obvious blow-off volume.

The ETF source strategy contributes five ideas:

- weighted log-price momentum, with newer prices weighted more heavily
- R-squared as path-quality screening, not as a standalone alpha score
- trend-line filter confirmation, adapted from the Laplace/EMA filter
- turnover confirmation, but with an upper bound to avoid exhaustion spikes
- defensive trading realism: no ST, no suspended names, no directional
  open-limit locks, T+1, lot-size, costs, and participation caps

For individual A-shares, the universe is much larger and noisier than an ETF
pool, so the stock object adds stricter universe controls:

- A-share only
- listed at least 180 trading days
- exclude Beijing board in v1
- 20-day median turnover at least CNY 120mn
- free-float market cap between CNY 5bn and CNY 30bn

## Descriptor Set

`liquid_midcap_leader_continuation_v1` uses five executable descriptor
branches:

- `weighted_momentum_quality`, weight `0.35`
- `industry_relative_strength`, weight `0.25`
- `trend_stability`, weight `0.20`
- `turnover_confirmation`, weight `0.10`
- `volume_overheat_control`, weight `0.10`

Hard filters are applied before ranking:

- weighted momentum score must be positive
- weighted momentum R-squared must be at least `0.35`
- latest close must be above the finite EMA/Laplace trend line
- EMA/Laplace trend-line slope must be positive
- no last-three-day daily return below `-4%`
- close must not be more than `15%` above its 20-day moving average
- 14-day RSI must be no greater than `80`
- current turnover must be less than `1.8x` the prior 5-day average

## Construction

The sleeve is a standalone long-only candidate:

- target: `open_t1_to_open_t20_net_cost`
- rebalance cadence: every 10 trading days
- target holding horizon: 20 trading days
- target names: 24
- single-name cap: 5%
- max names per SW2021 L1 industry: 3
- next-open execution with existing A-share cost and tradeability handling

The strategy should hold cash when the ranked universe cannot supply enough
valid names. It must not backfill weak names by lowering the hard filters.

## Validation Window

The requested validation window is the available data from `2020-01-02`
through the latest April 2026 trading date in the local DuckDB source. In this
workspace the available daily data ends on `2026-04-28`.

The research-input generator still needs a 20-trading-day forward label, so the
last rebalance signal will naturally occur before the final backtest date.
The portfolio backtest can still mark and carry positions through the final
April 2026 date.

## Evidence To Produce

The deliverable evidence package is:

- versioned thesis, descriptor, descriptor-set, sleeve, portfolio, and case
  configs
- executable trend research observations
- sleeve research artifact
- daily portfolio backtest from 2020 through April 2026
- focused unit tests for the new metrics and filters
- a concise result summary with absolute and benchmark-relative metrics

