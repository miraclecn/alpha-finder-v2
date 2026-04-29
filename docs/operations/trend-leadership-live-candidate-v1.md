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
- daily portfolio-level backtest case:
  `research/examples/deployment_minimal/trend_live_candidate_portfolio_backtest.toml`
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
- eligible to build the gated paper-trade signal package from the checked-in
  multi-year audit because the attached portfolio-evidence data-quality gates
  are clean; this does not waive the separate strategy-performance blockers
- before shadow-live, historical performance must be evidenced with
  `run-portfolio-backtest`, using raw unadjusted `daily_bar_pit` OHLC for
  fills, marks, and price-limit diagnostics, plus explicit staged corporate
  actions for cash and share adjustments; `run-promotion-replay` remains a
  candidate comparison and promotion-gate tool, not a real equity curve
- portfolio evidence must attach the market-data quality audit and report any
  overlap with `corporate_action_exception_ledger`; the current `122` exception
  windows are promotion-blocking unless explicitly excluded; the frozen
  multi-year replay now binds `output/research_source.duckdb` and
  `output/audits/market_data_quality_20260429.json`, and currently reports
  `0` corporate-action exception exposures
- portfolio backtest evidence must also report and block
  `qfq_fallback_price_exposure` and `tradeability_fallback_exposure`; the
  checked-in daily backtest currently reports `0` qfq fallback price exposures
  and `0` tradeability fallback exposures
- live-ready evidence must include benchmark-relative active portfolio metrics
  from `run-portfolio-backtest`; the checked-in multi-year audit now carries
  `active_backtest_information_ratio`, `active_backtest_active_annualized_return`,
  and `active_backtest_tracking_error` so research replay IR is not mistaken
  for tradable active IR
- the daily backtest maintains a T+1 `available_shares` ledger and enforces
  `min_trade_weight`; small non-liquidating trades are skipped with diagnostics,
  while full liquidations remain allowed subject to tradeability and available
  shares
- generated research inputs must also bind the same source DB: trend inputs
  exclude observations whose feature/label interval crosses a
  `corporate_action_exception_ledger` or qfq-fallback window, and fundamental
  inputs exclude observations whose label interval crosses one

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
- the checked-in multi-year audit artifact reports no data-quality blockers:
  `corporate_action_exception_exposure_count = 0`,
  `qfq_fallback_price_exposure_count = 0`,
  `tradeability_fallback_exposure_count = 0`, and the audit-level
  signal-release gate is met
- the candidate remains blocked for probation capital by strategy-quality
  evidence: the promotion replay still fails OOS IR, OOS t-stat, drawdown,
  realized-versus-budget, and marginal-IR gates
- the checked-in shadow-live journal contains fewer than `12` consecutive weekly cycles

The remaining credibility work is tracked in
`docs/superpowers/plans/2026-04-28-trusted-backtest-strategy-generation-risk-roadmap.md`.
