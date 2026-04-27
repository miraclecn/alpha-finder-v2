# Phase 4 Semi-Auto Deployment And Ops

## Goal

Lift the current deployment boundary from a code example into a sustainable
personal operating loop that preserves the real execution trace.

The planned new run artifacts for this phase are:

- `run_manifest`
- `manual_execution_outcome`
- `realized_trading_window`

## Current Verified State

- `research/examples/deployment_minimal/README.md` already defines the current
  executable-signal and decay-watch examples.
- `research/examples/deployment_minimal/account_state_20260319_trend_real.json`
  already exists and is aligned to the real trend decision date.
- `research/examples/deployment_minimal/account_state_2026_04_20.json` and
  `research/examples/deployment_minimal/portfolio_state_2026_04_20.json`
  already exist as persisted deployment-facing examples.
- `research/examples/deployment_minimal/executable_signal_real_output_case.toml`
  already binds generated benchmark-state and trend artifacts into a real-output
  next-open execution package.
- `research/examples/deployment_minimal/decay_watch_case.toml` already exists.
- The deployment README explicitly states that the current frontier is still
  pre-broker and pre-QMT, and that the real-output case still uses a synthetic
  account snapshot.
- The current repo does not yet expose first-class
  `run_manifest`, `manual_execution_outcome`, or
  `realized_trading_window` objects.

## Implemented Minimal Closure (`2026-04-26`)

- `build-run-manifest` now turns an executable-signal case into a persisted
  `run_manifest` artifact.
- `manual_execution_outcome` and `realized_trading_window` now exist as
  first-class JSON artifacts under
  `research/examples/deployment_minimal/`.
- `evaluate-decay-watch` can now bind those realized execution artifacts
  instead of relying only on an inline `realized_summary`.
- decay records now carry execution-trace fields:
  `run_id`, `realized_execution_basis`, `blocked_trade_count`,
  `manual_override_count`, `exception_count`, and `cash_drift_weight`.
- The phase still remains explicitly pre-broker and pre-QMT.

## In Scope

- formalize the run chain:
  `account_state -> portfolio_state -> executable_signal ->
  manual_execution_outcome -> decay_record`
- define `run_manifest`
- define `manual_execution_outcome`
- define `realized_trading_window`
- define the daily operating flow:
  pre-open, order entry, post-close writeback, exception logging, review, and
  retirement / decay monitoring
- define an operations ledger that records data version, research result,
  manual deviations, and exception reasons

## Out of Scope

- broker-direct order routing
- full automation or unattended live trading
- hiding blocked trades, cash drift, or manual overrides outside the system
- expanding into OMS complexity before the semi-auto trace loop is formalized

## Dependencies

- Phase 2 trend production lane
- Phase 3 overlay state, if the first release chooses to apply it during
  deployment
- `research/examples/deployment_minimal/README.md`
- generated benchmark-state and trend artifact outputs

## Execution Breakdown

### 1. Freeze The Daily Runbook

The phase document should define one operating order:

1. pre-open artifact freeze
2. account snapshot ingestion
3. executable-signal generation
4. manual order execution
5. post-close writeback
6. realized-window capture
7. decay / review update

### 2. Define `run_manifest`

`run_manifest` should become the per-run ledger entry that binds:

- data version and build date
- benchmark-state artifact
- sleeve artifact paths
- portfolio recipe
- overlay state, if used
- account snapshot used for execution
- operator timestamp

Minimal implementation boundary:

- build from an existing `executable_signal_case`
- persist one ledger entry per trade date
- do not add a second portfolio construction engine or broker adapter here

### 3. Define `manual_execution_outcome`

This object should persist what the current deployment examples still leave
outside the system:

- attempted orders
- blocked trades
- partial fills
- manual overrides and reasons
- cash drift
- post-trade holdings deltas

Minimal implementation boundary:

- accept manual writeback as truth
- record blocked trades and manual overrides explicitly
- do not infer missing broker-side events from research artifacts

### 4. Define `realized_trading_window`

This object should capture the actual post-trade observation window used for
decay and review:

- realized execution basis
- realized holdings after manual execution
- realized slippage / cost record
- realized portfolio path over the review window

Minimal implementation boundary:

- allow decay-watch to read realized summary from this object
- keep the first version artifact-driven; do not add live OMS state machines

### 5. Close The Trace Loop

The document should require that blocked trades, cash drift, and manual
override reasons flow into decay and review instead of being left in chat
notes, spreadsheets, or broker UI only.

Minimal implementation closure:

- `run_manifest` shares one `run_id` with
  `manual_execution_outcome` and `realized_trading_window`
- `evaluate-decay-watch` validates the linked trace before producing a record
- mismatched run ids, portfolios, dates, or package references fail loudly

## Verification Matrix

- `PYTHONPATH=src python3 -m unittest tests.test_deployment -v`
- `PYTHONPATH=src python3 -m unittest tests.test_portfolio_constructor -v`
- `PYTHONPATH=src python3 -m unittest tests.test_portfolio_simulator -v`
- `PYTHONPATH=src python3 -m unittest tests.test_research_evaluator -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -v`
- `build-benchmark-state --case research/examples/benchmark_state_build_minimal/csi800.toml`
- `build-trend-research-input --case research/examples/trend_input_build_minimal/trend_leadership_core.toml`
- `build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_output.toml`
- `build-executable-signal --case research/examples/deployment_minimal/executable_signal_real_output_case.toml`
- `build-run-manifest --case research/examples/deployment_minimal/run_manifest_case.toml`
- `evaluate-decay-watch --case research/examples/deployment_minimal/decay_watch_case.toml`
- `evaluate-decay-watch --case research/examples/deployment_minimal/decay_watch_case_with_realized_window.toml`

Phase acceptance also requires the document to show how these real-world
deviations are written back:

- blocked trades
- cash drift
- manual overrides
- exception reasons
- realized post-trade outcomes

## Stop Conditions

- the same trading day cannot be reconstructed from manifest to decay watch
- manual execution differences remain outside formal objects
- Phase 4 expands to broker automation before the semi-auto loop is closed
- realized outcomes are still judged only from research artifacts rather than
  from actual execution writeback

## Exit Criteria

- the operating flow from account snapshot to decay record is documented
- `run_manifest`, `manual_execution_outcome`, and
  `realized_trading_window` are defined as planned first-class artifacts
- one trading day can be traced end-to-end without leaving execution drift
  outside the system
- decay monitoring is explicitly tied to realized execution outcomes, not only
  to the research path
