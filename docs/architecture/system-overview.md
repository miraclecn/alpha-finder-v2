# System Overview

## Product Definition

V2 is designed for one concrete trading problem:

- personal-account A-share stock selection
- long-only cash equities
- end-of-day research with next-session execution
- medium turnover with explicit cost and tradeability controls

## Design Principle

The system does not start from formulas. It starts from a mandate and a thesis.

Each tradable artifact must answer:

- what inefficiency it exploits
- why the inefficiency should exist in A-shares
- what holding horizon fits the mechanism
- how it survives costs, constraints, and portfolio interactions

## System Layers

1. Data foundation
   - audited daily market and tradeability data as production truth
   - lagged PIT fundamentals as a slower anchor layer
   - experimental data kept outside the promotion path until audited
2. Research kernel
   - mandate, thesis, descriptor, target, alpha construction, validation
3. Risk model
   - market, industry, size, liquidity, momentum, reversal, valuation, quality, volatility
4. Portfolio construction
   - sleeve weighting, PIT benchmark state, exposure control, turnover penalties, liquidity caps
5. Simulation stack
   - research evaluator, portfolio simulator, execution simulator
6. Deployment ladder
   - account-state snapshot, portfolio-state snapshot, execution policy, executable signal package, paper, shadow, live, decay watch, retirement

## Explicit Non-Goals

- no free-form factor mining as the primary discovery engine
- no auto-generated strategy wrappers around factor ids
- no promotion based mainly on standalone IC aesthetics
- no immediate stock-shorting or intraday microstructure strategy buildout

## Current First-Release Doctrine

The first release is intentionally centered on data a personal researcher can maintain reliably.

Priority order:

- primary alpha: trend leadership and medium-horizon relative strength
- slower anchor: quality/value rerating with conservative reporting lags
- overlay: regime and tradeability control

Deferred until the data boundary is stronger:

- same-day earnings and event underreaction
- news or message driven logic
- fragile crowding and flow proxies
