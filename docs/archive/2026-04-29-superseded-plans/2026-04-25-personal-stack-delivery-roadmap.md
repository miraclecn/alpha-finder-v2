# Personal Stack V2 Delivery Roadmap

## Goal

Lock one first-release delivery path for the personal A-share V2 stack, then
split the work into bounded phase documents that can be implemented,
verified, and stopped independently without reopening the core product shape.

The first release is intentionally narrow:

- `Tushare 2000` is the production truth backbone
- `AKShare` stays supplemental, validation-only, or exploratory until a
  field-level audit upgrades it
- `Baostock` stays supplemental and cross-audit-only until a field-level audit
  upgrades it
- audited V1 DuckDB reuse is allowed only through isolated V2 build outputs
- `trend_leadership_core` is the only first-release alpha sleeve
- `risk_regime_filter` is the portfolio-level exposure governor
- execution remains semi-automatic and manual by design
- before any release-1 paper-trade signal is emitted, `trend_leadership_core`
  must first clear one audited multi-year validation gate

## Current Verified State

- `docs/architecture/a-share-personal-data-research-doctrine.md` already
  defines the `green / amber / experimental` data-tier doctrine and the first
  release portfolio shape.
- `docs/architecture/research-operating-model.md` already sets the research
  unit as the sleeve candidate and promotion as a portfolio-level decision.
- `docs/data/v1-duckdb-reuse-audit.md` already fixes the reuse boundary as
  `V1 audited DuckDB -> explicit V2 audit -> isolated V2 research source DB`.
- `research/examples/benchmark_state_build_minimal/README.md` already defines
  the standard `build-reference-staging-db -> build-research-source-db ->
  build-benchmark-state` chain. As of `2026-04-27`, the staged CSI 800 +
  `sw2021_l1` constituent coverage reaches `2014-02-21` through `2026-04-23`.
- `research/examples/promotion_replay_real_output/README.md` already defines a
  real-output trend replay lane on the widened `2021-03-05+` weekly decision
  calendar; the `302132.SZ` current-code industry gap is handled through the
  staged `security_code_alias_backfill`.
- `research/examples/deployment_minimal/README.md` already defines a formal
  bridge from generated sleeve artifacts to executable-signal and decay-watch
  examples, while remaining pre-broker and pre-QMT.
- `research/examples/fundamental_input_build_minimal/README.md` and
  `research/examples/promotion_replay_real_output_residual/README.md` already
  preserve the residual fundamental lane as a documented future option whose
  intake still depends on one external audited snapshot input.
- `docs/data/2026-04-26-sw-industry-pit-provider-audit.md` now records that
  the local official Shenwan packet
  (`StockClassifyUse_stock.xls`, `2014to2021.xlsx`,
  `SwClassCode_2021.xls`, and the official July-2021 cross-section) is strong
  enough to support a conservative derived `SW2021` PIT layer for the
  first-release trend lane, while same-day reclassification semantics remain
  quarantined.
- As of `2026-04-25`, the local `output/` directory already contains
  `pit_reference_staging.duckdb`, `research_source.duckdb`,
  `csi800_benchmark_state_history.json`, `trend_leadership_core_input.json`,
  `trend_leadership_core_artifact.json`,
  `trend_resilience_core_input.json`, and
  `trend_resilience_core_artifact.json`.
- As of `2026-04-25`, the local `output/` directory does not contain
  `open_t1_to_open_t20_residual_component_snapshot.json`.

## Execution Status (`2026-04-26`)

- Phase `1`: complete and re-verifiable on the current checked-in data spine
- Phase `2`: complete and re-verifiable on the widened `2021-03-05+` trend
  calendar; the frozen live-candidate audit now covers more than `5` calendar
  years after the T+20 exit horizon
- Phase `3`: complete and re-verifiable on the current replay / overlay
  surface
- Phase `4`: complete and re-verifiable on the current semi-auto deployment
  trace loop
- Phase `5`: intentionally paused for the current personal-investor stack;
  it is not an active blocker for phases `1 -> 4` and should resume only if
  the residual research question becomes worth extra infrastructure effort

## In Scope

- Release 1 is defined as phases `1 -> 4`.
- Phase `5` remains documented as a parked post-release residual option, not
  an active delivery obligation.
- Post-release trend deployment hardening should follow
  `docs/superpowers/plans/2026-04-26-trend-leadership-live-readiness.md`
  rather than reopening the paused residual lane by drift.
- All phase documents use the same plan shape:
  `Goal`, `Current Verified State`, `In Scope / Out of Scope`,
  `Dependencies`, `Execution Breakdown`, `Verification Matrix`,
  `Stop Conditions`, and `Exit Criteria`.
- Data, replay, and execution boundaries are described in document form before
  automation expands.

## Out of Scope

- broker-direct APIs, QMT routing, or unattended live order submission
- promoting `AKShare` to production truth without a separate audit
- promoting `Baostock` to production truth without a separate audit
- porting V1 `factor -> strategy -> promotion` logic into V2
- reopening an in-repo residual estimator design for the first release
- letting the slower `fundamental_rerating_core` lane block release-1
  delivery

## Dependencies

- `docs/architecture/a-share-personal-data-research-doctrine.md`
- `docs/architecture/research-operating-model.md`
- `docs/architecture/risk-model-and-simulation-loop.md`
- `docs/data/v1-duckdb-reuse-audit.md`
- `research/examples/benchmark_state_build_minimal/README.md`
- `research/examples/promotion_replay_real_output/README.md`
- `research/examples/deployment_minimal/README.md`
- `research/examples/fundamental_input_build_minimal/README.md`
- `research/examples/promotion_replay_real_output_residual/README.md`
- `docs/superpowers/plans/2026-04-26-trend-leadership-live-readiness.md`

## Execution Breakdown

### Phase 1. Data Truth And V1 Reuse

Freeze the only allowed data spine:

`Tushare 2000 -> output/pit_reference_staging.duckdb ->
output/research_source.duckdb -> benchmark / replay / deployment artifacts`

This phase also publishes the binding `green / amber / experimental` rules,
the `AKShare` / `Baostock` audit rules, and the V1 reuse whitelist /
denylist.

### Phase 2. Trend Production Lane

Turn `trend_leadership_core` into the only first-release alpha sleeve. The
current checked-in live-candidate chain begins on `2021-03-05`, while the data
spine supports CSI 800 + `sw2021_l1` constituent coverage beginning on
`2014-02-21`.

`trend_resilience_core` remains an evidence comparator only.
It is not promoted to a parallel first-release delivery lane.

### Phase 3. Regime Overlay And Promotion Discipline

Convert `risk_regime_filter` from doctrine into a first-class portfolio
object. The new boundary is a `regime_overlay` object that governs exposure,
not stock selection.

This phase also fixes the promotion rule: the overlay is judged by marginal
portfolio contribution, post-cost behavior, and regime robustness.

### Phase 4. Semi-Auto Deployment And Operations

Formalize the personal operating loop from generated signal to real execution
writeback. This phase introduces the planned run artifacts:

- `run_manifest`
- `manual_execution_outcome`
- `realized_trading_window`

The first release remains manual and traceable by design.

### Phase 5. Fundamental Residual Lane

Park `fundamental_rerating_core` as a post-release residual research option.
Keep the intake contract documented, but do not spend current delivery effort
trying to source or simulate the required external artifact:

- `output/open_t1_to_open_t20_residual_component_snapshot.json`

If this lane is explicitly resumed later, it should reuse the existing
residual replay scaffolding and stop immediately if the snapshot contract
fails.

## Verification Matrix

The roadmap is only credible if the repo keeps supporting the current
documented command chain while the phase documents are executed:

- `PYTHONPATH=src python3 -m unittest discover -s tests -v`
- `build-reference-staging-db -> build-research-source-db ->
  build-benchmark-state`
- `build-trend-research-input -> build-sleeve-artifact ->
  run-promotion-replay`
- `build-executable-signal -> evaluate-decay-watch`
- the residual `build-fundamental-research-input -> build-sleeve-artifact ->
  run-promotion-replay` chain stays out of current acceptance unless phase `5`
  is explicitly resumed and the residual snapshot passes intake checks

Release-1 acceptance is global only when all of the following are true:

- phases `1 -> 4` can be executed without invoking the residual blocker
- `trend_leadership_core` can run end-to-end on the widened `5` year candidate
  window without selecting securities that lack PIT industry labels
- the promotion surface can express regime-aware evidence instead of only one
  standalone sleeve chart
- the execution loop preserves manual deviations and realized outcomes in
  formal objects

## Stop Conditions

- Release-1 work starts depending on the missing residual snapshot.
- `AKShare` is silently reclassified as production truth.
- `Baostock` is silently reclassified as production truth.
- V1 factor, strategy, or promotion objects are proposed for direct reuse.
- Phase 3 turns the regime layer into a new stock-picking sleeve.
- Phase 4 expands into broker automation before the semi-auto trace loop is
  formally closed.
- Phase `5` is quietly pulled back into the active acceptance path without a
  new explicit resume decision.

## Exit Criteria

- The roadmap plus five phase documents exist under `docs/superpowers/plans/`.
- Phases `1 -> 4` are explicitly marked as the first-release mainline.
- Phase `5` is explicitly separated and currently paused as a post-release
  residual option.
- The data spine, promotion discipline, and semi-auto operating loop are all
  documented with concrete stop conditions.
- The repo can keep using one stable acceptance rule:
  honest first-release delivery does not depend on the slower residual lane.
