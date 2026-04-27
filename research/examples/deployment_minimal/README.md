# Minimal Deployment Example

This directory is the first V2 deployment-layer example.
It now carries two lanes:

- the original persisted sample case for schema and test stability
- a real-output case that consumes generated `output/` benchmark and trend artifacts directly

Files:

- `executable_signal_case.toml`: binds a research-approved portfolio, sleeve artifacts, benchmark state, and an account-state snapshot into a next-open execution package
- `executable_signal_case_with_overlay.toml`: the same persisted deployment
  path plus a formal `regime_overlay` decision for the trade date
- `executable_signal_real_output_case.toml`: binds the generated `output/csi800_benchmark_state_history.json` and `output/trend_leadership_core_artifact.json` into a real-output next-open execution package
- `run_manifest_case.toml`: builds a first-class `run_manifest` ledger entry
  from the executable-signal case
- `run_manifest_2026_04_20.json`: persisted per-run ledger that binds the
  trade date, execution date, benchmark state, sleeve artifacts, account
  snapshot, and operator timestamp for one execution day
- `manual_execution_outcome_2026_04_21.json`: manual writeback artifact that
  records attempted orders, blocked trades, partial/manual deviations, cash
  drift, and post-trade holdings
- `realized_trading_window_2026_05_18.json`: realized post-trade observation
  window used for decay review, including realized holdings, realized costs,
  path points, and realized summary metrics
- `candidate_portfolio.toml`: deployment-only portfolio recipe used by the persisted executable-signal and decay-watch samples
- `candidate_portfolio_with_overlay.toml`: deployment-only portfolio recipe
  that declares `regime_overlay_id`
- `account_state_2026_04_20.json`: broker/account-facing live snapshot used to derive holdings, cash, and blocked exits
- `account_state_20260319_trend_real.json`: synthetic broker snapshot aligned to the real 20260319 decision date; holdings mirror the prior weekly trend sleeve so the case emits honest buys, sells, a blocked exit, and a blocked entry
- `portfolio_state_2026_04_20.json`: records the live book as of the decision date, including holdings, cash, and blocked exits
- `trend_real_output_portfolio.toml`: trend-only portfolio recipe used for the real-output executable-signal case
- `trend_live_candidate_portfolio_with_overlay.toml`: frozen trend-only
  shadow-live candidate recipe that keeps `trend_leadership_core` as the only
  release-1 alpha sleeve while binding `a_share_risk_overlay`
- `trend_live_candidate_overlay_observations_20260305_20260319.json`: retained
  short overlay-history sample from the earlier `20260305..20260319` smoke
  calendar
- `trend_live_candidate_overlay_smoke_replay.toml`: candidate-matched
  overlay-honest replay case for the checked-in smoke audit
- `trend_live_candidate_overlay_observation_build.toml`: reproducible builder
  case that turns PIT benchmark and daily-bar data into an overlay-observation
  history for the frozen live candidate
- `trend_live_candidate_overlay_replay.toml`: candidate-matched replay case
  that binds the generated overlay-observation history on the widest currently
  honest PIT-safe interval in the staged data spine
- `executable_signal_case_trend_live_candidate_with_overlay.toml`: persisted
  deployment case for the frozen trend-only live candidate; it carries a hard
  multi-year-audit gate and now builds because the checked-in audit passes
- `run_manifest_case_trend_live_candidate_2026_04_20.toml`: run-manifest
  build case for the frozen trend-only live candidate
- `trend_leadership_live_candidate_v1.toml`: versioned live-candidate bundle
  that freezes the thesis, descriptor set, sleeve, target, portfolio,
  cost-scenario set, operating docs, and the attached multi-year validation
  audit artifact
- `trend_leadership_multi_year_validation_audit_v1.toml`: reproducible
  multi-year-audit build case that points to the benchmark/trend evidence
  currently attached to the frozen candidate
- `trend_leadership_multi_year_validation_audit_v1.json`: current audit
  verdict for the frozen trend-only live candidate; it covers `2021-03-05`
  through `2026-03-19` and reports no release blockers
- `shadow_live_journal_trend_leadership_v1.json`: first shadow-live journal
  record that keeps the frozen candidate attached to one traced cycle
- `decay_watch_case.toml`: binds a promoted expectation snapshot and a recent realized window into a decay-watch record
- `decay_watch_case_with_realized_window.toml`: binds `run_manifest`,
  `manual_execution_outcome`, and `realized_trading_window` into a decay-watch
  record so the realized lane is no longer inline-only

Run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/fundamental_rerating_core.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-executable-signal --case research/examples/deployment_minimal/executable_signal_case.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-executable-signal --case research/examples/deployment_minimal/executable_signal_case_with_overlay.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-run-manifest --case research/examples/deployment_minimal/run_manifest_case.toml
PYTHONPATH=src python3 -m alpha_find_v2 evaluate-decay-watch --case research/examples/deployment_minimal/decay_watch_case.toml
PYTHONPATH=src python3 -m alpha_find_v2 evaluate-decay-watch --case research/examples/deployment_minimal/decay_watch_case_with_realized_window.toml
```

Real-output run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python3 -m alpha_find_v2 build-benchmark-state --case research/examples/benchmark_state_build_minimal/csi800.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-trend-research-input --case research/examples/trend_input_build_minimal/trend_leadership_core.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_output.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-regime-overlay-observations --case research/examples/deployment_minimal/trend_live_candidate_overlay_observation_build.toml
PYTHONPATH=src python3 -m alpha_find_v2 run-promotion-replay --case research/examples/deployment_minimal/trend_live_candidate_overlay_replay.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-multi-year-validation-audit --case research/examples/deployment_minimal/trend_leadership_multi_year_validation_audit_v1.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-executable-signal --case research/examples/deployment_minimal/executable_signal_real_output_case.toml
```

The example remains pre-broker and pre-QMT on purpose.
Its job is to prove that V2 now has a formal bridge from approved portfolio targets to executable signals and then to post-promotion decay monitoring.
The deployment case no longer injects inline weights.
It reads a formal account-state snapshot and adapts it into a portfolio-state snapshot so blocked live holdings are preserved without silently re-scaling other target names above their desired weights.
The overlay variant extends that bridge by attaching the evaluated
`regime_overlay` state to the package and now cuts the requested gross
deployment budget explicitly:
`normal -> 100%`, `de_risk -> 65%`, `cash_heavier -> 35%` of the base
investable budget before blocked-trade realism is applied.
The new `run_manifest -> manual_execution_outcome -> realized_trading_window`
lane closes the semi-auto trace loop: blocked trades, cash drift, and manual
override reasons now live in first-class artifacts and can flow into
`evaluate-decay-watch` instead of staying in chat notes or broker UI only.
The real-output case is the current honest end-to-end frontier.
It uses generated benchmark state and generated trend sleeve artifacts, but it still relies on a synthetic account snapshot because broker integration and tradeability reconstruction are not finished.
The frozen live-candidate paper-trade case is stricter:
it requires the attached multi-year validation audit gate before rebuilding a
fresh signal. The checked-in audit now passes, so the gated signal and manifest
cases build from the frozen candidate bundle.
The audit file is no longer intended to be hand-maintained:
`build-multi-year-validation-audit` now rebuilds it from the attached
benchmark-state, trend-observation inputs, the generated overlay-observation
history, and the candidate-matched replay case.

As of `2026-04-27`, the CSI 800 benchmark + `sw2021_l1` constituent coverage
blocker is closed for `2014-02-21` through `2026-04-23`; the fresh PIT audit
covers `2,364,800 / 2,364,800` staged constituent-days, and the benchmark-state
build succeeds across the widened live-candidate input window. The
`302132.SZ` PIT industry gap is resolved through `security_code_alias_backfill`
from legacy `300114.SZ`, Beijing-board names are excluded, and the frozen
live-candidate audit now covers `5.0404` calendar years with no blockers.

For live-readiness hardening, the frozen trend-only candidate bundle and its
shadow-live journal are now checked in alongside:

- [trend-leadership-paper-trade-signal-policy.md](/home/nan/alpha-find-v2/docs/operations/trend-leadership-paper-trade-signal-policy.md)
- [account-state-export-contract.md](/home/nan/alpha-find-v2/docs/operations/account-state-export-contract.md)
- [trend-leadership-live-candidate-v1.md](/home/nan/alpha-find-v2/docs/operations/trend-leadership-live-candidate-v1.md)
- [trend-leadership-small-capital-probation-policy.md](/home/nan/alpha-find-v2/docs/operations/trend-leadership-small-capital-probation-policy.md)
