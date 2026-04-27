# Trend Leadership Live Candidate V1

The frozen release-1 live candidate is `trend_leadership_shadow_live_v1`.

It binds:

- thesis: `config/theses/trend_leadership.toml`
- descriptor set: `config/descriptor_sets/trend_leadership_core.toml`
- sleeve: `config/sleeves/trend_leadership_core.toml`
- target: `config/targets/open_t1_to_open_t20_net_cost.toml`
- portfolio: `research/examples/deployment_minimal/trend_live_candidate_portfolio_with_overlay.toml`
- cost scenarios: `base_a_share_cash`, `high_a_share_cash`
- overlay: `a_share_risk_overlay`
- multi-year validation audit build case:
  `research/examples/deployment_minimal/trend_leadership_multi_year_validation_audit_v1.toml`
- multi-year validation audit:
  `research/examples/deployment_minimal/trend_leadership_multi_year_validation_audit_v1.json`
- paper-trade signal policy:
  `docs/operations/trend-leadership-paper-trade-signal-policy.md`

Expected candidate budgets before shadow-live:

- turnover budget: `0.16`
- breadth range: `12` to `20`
- drawdown budget: `0.18`
- weak-regime behavior: reduce gross exposure through the overlay and accept residual cash instead of forcing replacement risk

Live universe policy:

- exclude `beijing` board names because the live account will not trade them
- do not exclude `chinext` or `star` by default; treat their `20%` daily limit
  regime through liquidity, cost, price-limit-lock, concentration, and exposure
  controls unless the mandate later narrows the investable universe

Current admission state:

- `shadow_live_eligible`
- not eligible for small-capital probation yet
- eligible to build the gated paper-trade signal package because the multi-year
  validation audit gate now passes

Current blockers that remain explicit:

- the CSI 800 benchmark + `sw2021_l1` constituent coverage blocker is closed
  for `2014-02-21` through `2026-04-23`; the fresh audit covers
  `2,364,800 / 2,364,800` staged constituent-days
- the benchmark-state builder can construct that full window with `2,956`
  trading steps and `800` constituents per step
- the honest multi-year trend validation window is rebuilt from `2021-03-05`
  through `2026-03-19`, covering `5.0404` PIT-safe calendar years after the T+20
  exit horizon is applied
- Beijing-board names are excluded from the live-tradable trend input, and the
  `302132.SZ` current-code history is covered through a narrow
  `security_code_alias_backfill` from legacy `300114.SZ` industry intervals
- the checked-in multi-year audit artifact reports no blockers and
  `signal_release_gate_met = true`
- the checked-in shadow-live journal contains fewer than `12` consecutive weekly cycles
