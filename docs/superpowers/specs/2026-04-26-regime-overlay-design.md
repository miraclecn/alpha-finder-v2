# Regime Overlay Design

## Scope

`P3` only formalizes `regime_overlay` as a first-class portfolio object.
This slice does not invent new market estimators and does not backfill missing
overlay truth from non-green data.

## Object Boundary

`regime_overlay` has three layers:

1. config
   - overlay id, mandate / benchmark scope
   - allowed green input family
   - state machine thresholds
   - missing-input downgrade / stop policy
2. observation
   - per-trade-date input states supplied explicitly
   - input state values are `supportive`, `neutral`, `risk_off`, `missing`,
     or `invalid`
3. decision
   - overlay status: `active`, `downgraded`, or `blocked`
   - portfolio state: `normal`, `de_risk`, or `cash_heavier`
   - reasons, missing inputs, and invalid inputs

## State Machine

- if any required stop input is `missing` or `invalid`, mark the overlay
  `blocked` and force `cash_heavier`
- else if any required input is `missing` or `invalid`, mark the overlay
  `downgraded` and force `de_risk`
- else count required inputs in `risk_off`
- `0` risk-off inputs -> `normal`
- `1` to `2` risk-off inputs -> `de_risk`
- `3+` risk-off inputs -> `cash_heavier`

## Green Input Contract

The allowed first-release input family is fixed to:

- `benchmark_trend`
- `market_breadth`
- `dispersion`
- `realized_volatility`
- `price_limit_stress`

If a portfolio wants to use any other input, the loader must reject it.

## Integration Contract

- portfolios may optionally declare `regime_overlay_id`
- promotion replay may optionally load overlay observations and expose the
  evaluated overlay decisions in replay output
- executable signal loading may optionally load overlay observations and expose
  the evaluated overlay decision for the requested trade date
- missing overlay observations must never be silently treated as `normal`

## Intentional Non-Goals

- no automatic derivation of breadth / dispersion / volatility / price-limit
  stress from incomplete artifacts
- no exposure rescaling engine inside this slice
- no migration of V1 factor / strategy / promotion logic
