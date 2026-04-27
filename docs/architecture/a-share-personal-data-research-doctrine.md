# A-share Personal-Data Research Doctrine

## Why This Doctrine Exists

V2 is not being built for an institution with full vendor feeds.

It is being built for one person running a real A-share research stack with:

- stable daily and structural data from `Tushare 2000`
- slower financial statement data with reporting lag
- optional free `AKShare` and `Baostock` supplements that may be useful, but cannot be trusted as production truth

That constraint is not a weakness to hide.
It should determine the entire system shape.

## Production Data Tiers

### Green: production truth

Use these as the first release research foundation:

- daily OHLCV and adjustment data
- daily liquidity and valuation fields
- suspension and price-limit state
- security master and listing-status state
- historical benchmark membership and industry classification

These datasets are stable enough to support:

- medium-horizon trend and leadership research
- tradeability screens
- regime and breadth monitoring
- executable portfolio simulation

### Amber: slow anchor layer

Use lagged fundamentals only for slow signals and veto logic:

- income statement
- balance sheet
- cash flow statement
- `fina_indicator`
- `forecast` and `express`
- disclosure timing

These data may support real value and quality research, but they should not be treated as fast information.
The system must assume conservative availability and tolerate stale updates.

Cross-audit supplements may also live here when they help validate the `green`
spine without replacing it:

- `AKShare` historical change nodes or convenience surfaces used only as
  evidence
- `Baostock` daily bars, adjustment factors, security master, and slow
  fundamentals used only as cross-checks against the main `Tushare` chain
- the local official Shenwan packet
  (`StockClassifyUse_stock.xls`, `2014to2021.xlsx`,
  `SwClassCode_2021.xls`, and the official July-2021 cross-section) used to
  audit and conservatively reinforce `SW2021` industry PIT, but not to justify
  static backward projection

### Experimental: not promotion-safe

These can be explored, but cannot drive production promotion:

- AKShare-only fields without durability guarantees
- Baostock-only fields or transforms without a separate field audit
- news and message data
- same-day event timing that cannot be reconstructed honestly
- ad hoc crowding and flow proxies without a stable history

## First Release Thesis Stack

### 1. `trend_leadership`

This is the main production alpha sleeve.

Economic idea:

- medium-horizon winners often keep winning when the move is orderly, liquid, and confirmed by industry leadership
- in A-shares, trend continuation is often reinforced by gradual information diffusion, benchmark chasing, and retail theme persistence

Why it fits the personal stack:

- it can be built mostly from daily bars, liquidity, benchmark membership, and industry history
- the research clock is clean: signal at close, trade next open, hold for weeks
- it is much easier to backtest honestly than news or same-day earnings logic
- the current `sw2021_l1` industry input is now supportable by an audited,
  slow-moving official Shenwan change-node packet even before broker-direct
  automation exists

### 2. `fundamental_rerating`

This is no longer treated as the main fast alpha sleeve.

It becomes:

- a slow quality/value anchor
- a trap-avoidance layer
- a possible future veto source for the trend sleeve

Why it still belongs:

- slow accounting data are still economically useful for avoiding weak balance-sheet and low-quality names
- it diversifies the trend sleeve if it remains slow, conservative, and lag-aware

### 3. `risk_regime_filter`

This is a portfolio overlay, not a stock-picking thesis.

Its job is to decide when the system should:

- run normal gross exposure
- scale down
- hold extra cash
- avoid new concentration in weak breadth or unstable volatility regimes

It should be built from the same green data tier:

- benchmark trend
- market breadth
- dispersion
- realized volatility
- price-limit stress

## Explicitly Deferred Directions

These are not banned forever, but they are not first-release production research:

- `earnings_underreaction`
- `flow_liquidity_reversal`
- `crowding_anti_consensus`
- all message, news, and sentiment pipelines

The issue is not that these mechanisms are fake.
The issue is that a personal stack usually cannot measure them honestly enough to distinguish real alpha from timestamp leakage, coverage gaps, or execution fantasy.

## First Release Portfolio Shape

The first live-candidate portfolio should be:

- A-share long-only
- next-open executable
- weekly rebalance, with optional staggered refresh
- 15 to 25 names
- medium turnover, not fast turnover

Portfolio recipe:

- `trend_leadership_core` as the main alpha sleeve
- `risk_regime_filter` as a portfolio-level exposure governor

For the current first-release live-candidate lane, `fundamental_rerating_core`
stays outside the capital path.
It remains a documented post-release research option until the residual
snapshot contract is sourced and audited honestly.

This is intentionally narrower than a broad multi-thesis platform.
At personal scale, narrow and honest beats broad and noisy.

## Promotion Rules Under Personal Constraints

A sleeve is not promotable just because its backtest is attractive.

It must also survive:

- anchored walk-forward out-of-sample testing
- realistic next-open execution
- high-cost stress
- universe perturbation
- regime splits
- marginal contribution to the portfolio

V2 therefore blocks production promotion for any sleeve whose core edge depends on:

- uncertain event timestamps
- unstable free data feeds
- untestable news interpretation
- microstructure assumptions that require intraday data

## Practical Conclusion

For this project, the professional route is not to chase the most exotic alpha idea.

It is to build one durable research spine around:

- clean daily data
- medium-horizon price/volume leadership
- slow fundamental anchoring
- strict A-share execution realism

That is the highest-probability path from a personal research environment to a
strategy that is credible enough for paper trading first and only later, after
multi-year audited validation, for real capital.
