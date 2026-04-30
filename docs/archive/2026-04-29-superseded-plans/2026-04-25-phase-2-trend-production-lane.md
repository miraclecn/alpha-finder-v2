# Phase 2 Trend Production Lane

## Goal

Make `trend_leadership_core` the only first-release alpha sleeve and lock one
repeatable real-output chain from audited research data to promotion replay and
semi-auto deployment.

## Current Verified State

- `docs/architecture/a-share-personal-data-research-doctrine.md` already marks
  `trend_leadership` as the main production alpha sleeve.
- `research/examples/promotion_replay_real_output/README.md` already defines a
  real-output promotion replay lane; the frozen live-candidate lane now also has
  a widened 5-year audit path.
- Revalidated on `2026-04-27`, the normal
  `build-reference-staging-db -> build-research-source-db ->
  build-benchmark-state` path now supports CSI 800 + `sw2021_l1` constituent
  coverage from `2014-02-21` through `2026-04-23`.
- The widened trend replay blocker is no longer benchmark-state or
  trend-industry coverage. Beijing-board names are excluded from the
  live-tradable trend input, and `302132.SZ` is covered through
  `security_code_alias_backfill` from legacy `300114.SZ` industry intervals.
- `research/examples/trend_input_build_minimal/trend_leadership_core.toml`
  and `research/examples/trend_input_build_minimal/trend_resilience_core.toml`
  already exist.
- `research/examples/artifact_build_minimal/trend_leadership_core_output.toml`
  and `research/examples/artifact_build_minimal/trend_resilience_core_output.toml`
  already exist.
- `output/csi800_benchmark_state_history.json`,
  `output/trend_leadership_core_input.json`,
  `output/trend_leadership_core_artifact.json`,
  `output/trend_resilience_core_input.json`, and
  `output/trend_resilience_core_artifact.json` already exist locally as of
  `2026-04-25`.
- `research/examples/deployment_minimal/README.md` already defines a real-output
  executable-signal example driven by the generated trend artifact.

## In Scope

- lock `trend_leadership_core` as the only first-release alpha sleeve
- keep the checked-in real-output chain on the widened live-candidate calendar
  for `trend_leadership_core`
- lock the trend command chain from benchmark state to input build, artifact
  build, promotion replay, and executable-signal example
- keep `trend_resilience_core` only as a replay comparator on the same green
  stack
- document the accepted first-release gaps that do not invalidate the trend
  lane

## Out of Scope

- making `trend_resilience_core` a second first-release delivery requirement
- delaying release-1 acceptance until `fundamental_rerating_core` is runnable
- claiming a widened trend replay before selected securities have PIT industry
  labels on every decision date
- intraday, same-day event, or message-driven alpha work
- broker automation

## Dependencies

- Phase 1 data truth and V1 reuse rules
- `research/examples/benchmark_state_build_minimal/README.md`
- `research/examples/promotion_replay_real_output/README.md`
- `research/examples/deployment_minimal/README.md`
- `output/research_source.duckdb`
- `output/csi800_benchmark_state_history.json`

## Execution Breakdown

### 1. Freeze The Widened Trend Window

The checked-in trend live-candidate mainline starts on `2021-03-05`.

The replay validation window ends at `2026-03-19` because the sleeve target
requires a T+20 exit horizon. That produces `5.0404` calendar years of audited
history.

No document or example may treat the old `2026-03-05` smoke slice as current
release-grade evidence.

### 2. Freeze The Standard Trend Chain

The standard trend run order is:

1. `build-benchmark-state`
2. `build-trend-research-input` for `trend_leadership_core`
3. optional comparator build for `trend_resilience_core`
4. `build-sleeve-artifact`
5. `run-promotion-replay`
6. `build-executable-signal`

### 3. Freeze The Evidence Surface

Phase 2 should treat these replay outputs as required surfaces, not optional
extras:

- `decision`
- `snapshot`
- `research_evidence`
- `research_evidence.walk_forward`
- `research_evidence.regime_breakdown`

The phase is complete only if the trend lane is judged on those surfaces, not
on a single standalone return chart.

### 4. Keep The Comparator Narrow

`trend_resilience_core` stays a current non-residual replay comparator.
It can help judge regime behavior and portfolio incrementality, but it does
not expand release-1 scope into a new required alpha lane.

### 5. Publish Accepted Gaps

The document should keep these temporary first-release realities explicit:

- the executable-signal real-output case still uses a synthetic account
  snapshot
- broker / QMT integration is intentionally deferred
- exact limit-state reconstruction is still bounded by the audited data spine
- the slower residual fundamental lane is not required for release-1

## Verification Matrix

- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_trend_research_input_builder.py' -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_research_artifact_builder.py' -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_research_artifact_loader.py' -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_promotion_replay.py' -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_deployment.py' -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -v`
- `build-benchmark-state --case research/examples/benchmark_state_build_minimal/csi800.toml`
- `build-trend-research-input --case research/examples/trend_input_build_minimal/trend_leadership_core.toml`
- `build-trend-research-input --case research/examples/trend_input_build_minimal/trend_resilience_core.toml`
- `build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_output.toml`
- `build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_resilience_core_output.toml`
- `run-promotion-replay --case research/examples/promotion_replay_real_output/replay_case.toml`
- `build-executable-signal --case research/examples/deployment_minimal/executable_signal_real_output_case.toml`

## Stop Conditions

- the trend window is widened before the trend input builder can produce
  industry-labeled selected observations on every decision date
- the trend lane starts depending on `fundamental_rerating_core` to justify
  release-1 delivery
- non-green or unaudited inputs enter the main trend research chain
- `trend_resilience_core` is turned into a second release-1 scope lock

## Exit Criteria

- `trend_leadership_core` is explicitly fixed as the only first-release alpha
  sleeve
- the widened trend chain is documented and repeatable from research DB to
  executable signal
- replay evidence is defined on walk-forward and regime-aware surfaces
- accepted temporary gaps are documented instead of being hidden
- release-1 acceptance no longer depends on the slower residual lane
