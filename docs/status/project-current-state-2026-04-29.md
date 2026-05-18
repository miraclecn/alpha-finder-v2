# Project Current State - 2026-04-29

> Update on `2026-04-30`: the repository now contains machine-checkable
> strategy failure attribution, generated-strategy guardrails, and
> shadow-live admission evaluation. The current gap is no longer missing gate
> machinery; it is the absence of a new admissible research object after the
> frozen `trend_leadership_shadow_live_v1` candidate was rejected.

## Executive Judgment

`alpha-find-v2` has become a credible research and evidence framework for a
personal A-share long-only system. It is not yet a credible capital-deployment
strategy.

The important distinction is:

- The data and accounting gate is now mostly institutional in spirit: PIT
  benchmark membership, PIT SW2021 L1 industry labels, raw-OHLC execution
  accounting, explicit corporate-action booking, T+1 available-share logic,
  limit/suspension handling, and fallback exposure diagnostics are wired into
  the research-to-backtest path.
- The current `trend_leadership_shadow_live_v1` candidate fails strategy
  quality. The bound daily portfolio backtest is deeply negative, so no
  professional quant process should treat it as paper-trade or probation ready.

Current professional status:

| Area | Status | Judgment |
| --- | --- | --- |
| Data spine | usable with quarantines | Strong enough for candidate review, not perfect raw truth. |
| PIT benchmark and industry | usable | CSI 800 + SW2021 L1 coverage is no longer the live-readiness blocker. |
| Corporate actions | usable with exception ledger | `122` unresolved adjustment-factor jumps remain promotion-blocking unless excluded. |
| Tradeability | usable | Official rows cover nearly all `daily_bar_pit`; residual fallback is explicit. |
| Backtest ledger | improved | Raw fills/marks, explicit corporate actions, T+1, min trade, and fallback diagnostics exist. |
| Control truthfulness | improved | Unsupported size/beta controls are not marked enforced. |
| Diagnosis / guardrails / admission machinery | built | Failure attribution, generated-strategy guardrails, and shadow-live gate evaluation are now machine-checkable in repo. |
| Current trend candidate | rejected for capital | Data-quality gate is clean, but active returns, drawdown, and turnover are unacceptable. |
| Release-1 portfolio scope | inactive target recipe only | `a_share_core` remains a reference config, not an admitted capital path. |
| Shadow-live evidence | insufficient | Journal has `1` cycle; minimum policy target is `12` consecutive weekly cycles. |

## Current Evidence

Source artifacts:

- `output/audits/market_data_quality_20260429.json`
- `output/trend_live_candidate_portfolio_with_overlay_daily_backtest.json`
- `output/trend_only_portfolio_daily_backtest.json`
- `research/examples/deployment_minimal/trend_leadership_multi_year_validation_audit_v1.json`
- `research/examples/deployment_minimal/shadow_live_journal_trend_leadership_v1.json`

Market-data audit as of `2026-04-29`:

| Metric | Value |
| --- | ---: |
| `daily_bar_pit` rows | `11,655,309` |
| corporate-action ledger rows | `45,342` |
| significant adjustment-factor jumps | `35,814` |
| explained adjustment-factor jumps | `35,692` |
| unresolved adjustment-factor jumps | `122` |
| promotion-blocking unresolved jumps | `122` |
| tradeability rows | `11,655,309` |
| official tradeability rows | `11,653,846` |
| OHLC-fallback tradeability rows | `1,463` |
| qfq-fallback price rows in full data spine | `427,218` |

The full data spine still contains qfq-fallback rows, but the current candidate
generation and portfolio evidence exclude candidate/holding exposure to those
fallback windows.

Frozen candidate evidence:

| Portfolio backtest | Trend only | Trend + overlay |
| --- | ---: | ---: |
| Window | `2021-03-05` to `2026-03-19` | `2021-03-05` to `2026-03-19` |
| Total return | `-94.85%` | `-87.76%` |
| Annualized return | `-53.15%` | `-37.93%` |
| Benchmark annualized return | `3.09%` | `3.09%` |
| Active annualized return | `-56.24%` | `-41.02%` |
| Active IR | `-1.50` | `-1.33` |
| Max drawdown | `-95.39%` | `-89.20%` |
| Turnover | `87.88x` | `73.20x` |
| Market-data fallback exposure | `0` | `0` |
| Corporate-action exception exposure | `0` | `0` |
| qfq-fallback price exposure | `0` | `0` |
| Tradeability fallback exposure | `0` | `0` |

This is a clear research failure for the current candidate. The overlay reduces
the damage but does not rescue the thesis as currently parameterized.

## Object Chain State

The V2 object chain is intact:

`mandate -> thesis -> descriptor set -> sleeve -> portfolio recipe -> executable signal -> decay record`

Current live-candidate binding:

- thesis: `config/theses/trend_leadership.toml`
- descriptor set: `config/descriptor_sets/trend_leadership_core.toml`
- sleeve: `config/sleeves/trend_leadership_core.toml`
- target: `config/targets/open_t1_to_open_t20_net_cost.toml`
- portfolio: `research/examples/deployment_minimal/trend_live_candidate_portfolio_with_overlay.toml`
- audit case: `research/examples/deployment_minimal/trend_leadership_multi_year_validation_audit_v1.toml`
- daily backtest: `research/examples/deployment_minimal/trend_live_candidate_portfolio_backtest.toml`

Professional interpretation:

- The bundle is useful as a reproducible failure case and diagnostic baseline.
- It is not eligible for new paper-trade signal release under a finance-grade
  admission standard.
- It is not eligible for small-capital probation.
- `config/portfolio/a_share_core.toml` remains checked in, but only as an
  inactive reference recipe while the frozen trend candidate is rejected and
  the residual fundamental lane lacks audited inputs.

## Release Scope

The active release-1 scope is narrower than the checked-in two-sleeve
`a_share_core` recipe.

- `trend_leadership_shadow_live_v1` is a frozen diagnostic artifact, not an
  active capital candidate.
- `fundamental_rerating_core` remains paused until an audited
  `output/open_t1_to_open_t20_residual_component_snapshot.json` exists.
- `trend_resilience_core` remains a comparator lane, not an admitted second
  sleeve.
- `a_share_core` is therefore retained as an inactive target recipe rather than
  an active release portfolio.

## What Is Current

Current truth sources:

- Architecture: `docs/architecture/`
- Data boundary: `docs/data/v2-data-boundary-and-pit-audit.md`
- V1 reuse boundary: `docs/data/v1-duckdb-reuse-audit.md`
- Live-candidate operations: `docs/operations/trend-leadership-live-candidate-v1.md`
- Failure review: `docs/research/trend-leadership-failure-review-2026-04-29.md`
- Failure attribution artifact: `output/trend_leadership_failure_attribution_20260429.json`
- Strategy-generation guardrails: `docs/architecture/strategy-generation-guardrails.md`
- Release-scope review: `docs/research/release-1-scope-review-2026-04-29.md`
- Trusted backtest and strategy-generation roadmap:
  `docs/superpowers/plans/2026-04-28-trusted-backtest-strategy-generation-risk-roadmap.md`
- Forward professional roadmap:
  `docs/superpowers/plans/2026-04-29-professional-quant-roadmap.md`

Superseded implementation plans were moved to:

- `docs/archive/2026-04-29-superseded-plans/`

## Quant Finance Assessment

The next risk is no longer missing code for signal generation, failure
attribution, or admission gates. The next risk is false confidence about what
the completed machinery means.

The repo now answers the immediate professional-control questions for the
frozen candidate:

1. Is the loss explained by a missing data/accounting gate?
   Current answer: no; the attached candidate evidence is clean on the
   implemented data-quality blockers.
2. Is the current bundle admissible after strategy-quality review?
   Current answer: no; active IR, drawdown, and turnover fail the gate.
3. Is further parameter search on `trend_leadership_shadow_live_v1` justified?
   Current answer: no; the failure review rejects the frozen candidate version.

The remaining work is therefore narrower and more demanding:

1. Define a new versioned research object only if it has a specific economic
   mechanism, a descriptor set that actually binds that mechanism, and a target
   surface consistent with A-share execution reality.
2. Re-run the same data-quality, strategy-quality, promotion, and shadow-live
   gates on that new object instead of tuning around the rejected bundle.
3. Accumulate honest shadow-live evidence only after a new bundle passes the
   multi-year strategy-quality gate.

The right state is now:

`research framework ready -> admission machinery built -> current candidate rejected -> new research object required`
