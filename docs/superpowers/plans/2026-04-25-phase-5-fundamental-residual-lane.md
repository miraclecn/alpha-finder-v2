# Phase 5 Fundamental Residual Lane

## Goal

Park `fundamental_rerating_core` as a separate same-target residual replay
lane without reopening the first-release scope or inventing a local residual
estimator.

For the current personal-investor stack, this phase is intentionally paused.
The document exists to preserve the exact intake contract and honest resume
conditions for the future, not to force immediate execution.

## Current Verified State

- `config/sleeves/fundamental_rerating_core.toml` already binds the slower
  sleeve to `open_t1_to_open_t20_residual_net_cost`.
- `config/sleeves/trend_leadership_core_residual.toml` already exists and binds
  the trend comparator to the same residual target.
- `config/risk_models/a_share_core_equity.toml` already defines the residual
  component set as `benchmark`, `industry`, `size`, and `beta`.
- `research/examples/fundamental_input_build_minimal/README.md` already states
  that the fundamental builder requires an external audited residual snapshot.
- `research/examples/promotion_replay_real_output_residual/README.md` already
  defines the residual same-target replay lane.
- `research/examples/promotion_replay_real_output_residual/residual_snapshot_required_coverage.json`
  already pins the minimal required coverage for the current checked-in cases.
- `build-residual-snapshot-required-coverage` can now rebuild that required
  coverage manifest directly from the checked-in residual build cases plus the
  current `output/research_source.duckdb` state.
- `validate-residual-snapshot` can now enforce the intake contract in one
  command once the external snapshot arrives.
- the residual replay lane still reuses `output/csi800_benchmark_state_history.json`,
  so it currently inherits the same honest benchmark window that now begins on
  `2026-03-05`.
- As of `2026-04-25`, the local `output/` directory does not contain
  `open_t1_to_open_t20_residual_component_snapshot.json`.
- As of `2026-04-26`, the personal-stack roadmap intentionally pauses this
  phase instead of treating that missing snapshot as an active delivery
  blocker.
- `docs/architecture/risk-model-and-simulation-loop.md` already keeps internal
  factor-return estimation out of scope for this stage.

## In Scope

- keep this lane separate from release-1 delivery
- record the explicit pause decision for the current roadmap cycle
- preserve the intake-contract definition for one audited upstream file
- reuse the existing residual replay scaffolding already present in the repo
- freeze the intake path:
  `output/open_t1_to_open_t20_residual_component_snapshot.json`
- require three intake checks:
  header validity, component completeness, and required coverage
- run same-target residual replay only after the phase is explicitly resumed
  and the snapshot passes intake checks

## Out of Scope

- building an in-repo residual estimator
- sourcing or approximating an external audited snapshot just to keep the phase
  nominally "moving"
- relabeling non-residual trend artifacts to the residual target
- zero-filling or inventing residual components
- letting this slower lane block phases `1 -> 4`
- reopening the release-1 product definition

## Dependencies

- Phase 1 data-truth and audit rules
- Phase 2 trend production lane
- Phase 3 replay / promotion discipline
- `config/risk_models/a_share_core_equity.toml`
- `research/examples/fundamental_input_build_minimal/README.md`
- `research/examples/promotion_replay_real_output_residual/README.md`
- `research/examples/promotion_replay_real_output_residual/residual_snapshot_required_coverage.json`

## Execution Breakdown

### 0. Pause The Lane

For the current roadmap cycle:

- do not treat this phase as an active blocker
- do not spend implementation effort chasing external audited exports
- do not relax the residual contract just to keep the lane alive on paper
- resume only through an explicit product decision, not by drift

### 1. Freeze The Intake Contract

If this phase is later resumed, the only acceptable upstream file is:

- `output/open_t1_to_open_t20_residual_component_snapshot.json`

Required contract:

- `schema_version = 1`
- `artifact_type = "residual_component_snapshot"`
- `target_id = "open_t1_to_open_t20_residual_net_cost"`
- `risk_model_id = "a_share_core_equity"` by meaning, even if carried in
  provenance metadata rather than one TOML field
- residual components on every required row:
  `benchmark`, `industry`, `size`, `beta`

### 2. Require Provenance

If resumed, the snapshot is not acceptable without plain-text provenance:

- benchmark definition
- industry schema
- generation date
- audited upstream export path

### 3. Require Three Intake Checks

If resumed, require three separate checks before any builder runs:

- rebuild the pinned required coverage manifest from the current checked-in
  residual build cases whenever the local DuckDB or either case changes
- header check
- component completeness check
- coverage check against
  `research/examples/promotion_replay_real_output_residual/residual_snapshot_required_coverage.json`

Concrete command:

- `PYTHONPATH=src python3 -m alpha_find_v2 build-residual-snapshot-required-coverage`
- `PYTHONPATH=src python3 -m alpha_find_v2 validate-residual-snapshot`

### 4. Freeze The Honest Run Order

Only after the phase is resumed and the snapshot passes intake may the
residual lane run:

1. `build-fundamental-research-input`
2. `build-sleeve-artifact` for `fundamental_rerating_core`
3. `build-trend-research-input` for `trend_leadership_core_residual`
4. `build-sleeve-artifact` for `trend_leadership_core_residual`
5. `run-promotion-replay`

### 5. Record One Explicit Decision

If replay is ever rerun, require one explicit outcome:

- admit
- reject
- hold for more data

Silently "keeping the option open" is not a valid completion state once the
same-target replay has run.

## Verification Matrix

While the phase is paused, document consistency is the only current acceptance
requirement. The commands below remain the future-resume verification path and
are not part of release-1 acceptance today.

- `PYTHONPATH=src python3 -m unittest tests.test_fundamental_research_input_builder -v`
- `PYTHONPATH=src python3 -m unittest tests.test_trend_research_input_builder -v`
- `PYTHONPATH=src python3 -m unittest tests.test_risk_model -v`
- `PYTHONPATH=src python3 -m unittest tests.test_target_risk_integration -v`
- `PYTHONPATH=src python3 -m unittest tests.test_research_artifact_builder -v`
- `PYTHONPATH=src python3 -m unittest tests.test_promotion_replay -v`
- `PYTHONPATH=src python3 -m unittest tests.test_residual_snapshot_required_coverage_builder -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -v`
- `PYTHONPATH=src python3 -m alpha_find_v2 build-residual-snapshot-required-coverage`
- `PYTHONPATH=src python3 -m alpha_find_v2 validate-residual-snapshot`
- `build-fundamental-research-input --case research/examples/fundamental_input_build_minimal/fundamental_rerating_core.toml`
- `build-sleeve-artifact --case research/examples/artifact_build_minimal/fundamental_rerating_core_output.toml`
- `build-trend-research-input --case research/examples/trend_input_build_minimal/trend_leadership_core_residual.toml`
- `build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_residual_output.toml`
- `run-promotion-replay --case research/examples/promotion_replay_real_output_residual/replay_case.toml`

If the phase is resumed, acceptance also requires the intake gate itself to be
satisfied:

- header valid
- all required components present
- required coverage manifest fully satisfied

## Stop Conditions

- the paused lane is treated as an active roadmap blocker again
- the snapshot file is missing
- snapshot header fields do not match the contract
- required residual components are incomplete on any required row
- required coverage is missing
- the residual lane is "unstuck" by target relabeling or a toy local estimator

## Exit Criteria

- the residual snapshot contract is documented and separate from release-1
- the pause decision is explicit for the current roadmap cycle
- the three intake checks remain explicit for any future resume
- the same-target residual replay order remains documented and honest
- the current valid state is:
  the lane is intentionally paused without blocking phases `1 -> 4`
