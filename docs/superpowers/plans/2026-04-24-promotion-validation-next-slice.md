# Promotion Validation Next Slice

**Goal:** turn the current honest dual-sleeve weekly replay from a credible example into anti-overfitting research evidence that can justify later paper-trading admission.

## Current Verified State

- `PYTHONPATH=src python3 -m unittest discover -s tests -v` now passes with `74` tests.
- `run-promotion-replay` is reproducible on the checked-in real-output case and now emits:
  - `decision` plus `snapshot` for the narrow placeholder gate
  - `research_evidence` for the broader validation surface
  - `research_evidence.walk_forward` with anchored split summaries and stability aggregates
  - `research_evidence.regime_breakdown` with regime buckets, stability tables, and weak sub-periods
- Replay-case loading now rejects mixed-target sleeves up front. Used sleeve artifacts must share one `target_id` before they can be compared on the same replay surface.
- The checked-in real-output replay lane is still honest but still narrow:
  - baseline: `trend_leadership_core`
  - candidate: `trend_leadership_core + trend_resilience_core`
  - shared target: `open_t1_to_open_t20_net_cost`
- The slower anchor lane is only partially opened:
  - `build-fundamental-research-input` now exists
  - `research/examples/fundamental_input_build_minimal/` now defines the real-output build contract
  - the repo still does **not** contain `output/open_t1_to_open_t20_residual_component_snapshot.json`
  - therefore `fundamental_rerating_core` still cannot be built into an honest real-output replay artifact

## Ordered Next Steps

### 1. Obtain an audited residual-component snapshot for the slower anchor target

Why first:

- The new fundamental input builder intentionally refuses to invent residualization inputs inside the repo.
- Without a real residual-component snapshot, the slower anchor cannot cross the build boundary honestly.

Deliverables:

- an audited JSON snapshot for `open_t1_to_open_t20_residual_net_cost`
- records keyed by `trade_date` plus `asset_id`
- residual components carried explicitly as `benchmark`, `industry`, `size`, and `beta`
- provenance notes that explain where the residual inputs came from and which benchmark / industry schema they assume

Success check:

- `PYTHONPATH=src python3 -m alpha_find_v2 build-fundamental-research-input --case research/examples/fundamental_input_build_minimal/fundamental_rerating_core.toml` succeeds
- the command writes `output/fundamental_rerating_core_input.json` without falling back to an internal toy estimator

### 2. Build a real `fundamental_rerating_core` sleeve artifact from generated input

Why second:

- The replay layer can only evaluate sleeves that already exist as artifacts.
- The current artifact build case for `fundamental_rerating_core` still points at a checked-in synthetic observation file, not the generated real-output input.

Deliverables:

- a sleeve-artifact build case that consumes `output/fundamental_rerating_core_input.json`
- a generated `fundamental_rerating_core` artifact built from the real-output observation input
- verification that the artifact preserves the same target label and weekly decision calendar expected by replay

Success check:

- `build-sleeve-artifact` succeeds from the generated fundamental input
- the artifact is no longer a checked-in synthetic placeholder for promotion-replay work

### 3. Create an honest same-target comparator lane for replay

Why third:

- Replay now correctly rejects mixed-target sleeves, so the current trend real-output artifacts cannot be combined with a residualized fundamental sleeve.
- The system needs one shared return basis before slower-anchor admission can be evaluated.

Deliverables:

- either a residualized trend real-output lane on `open_t1_to_open_t20_residual_net_cost`
- or another honest comparator sleeve already built on the same residual target
- updated replay example docs that state which sleeves are comparable and why

Success check:

- `run-promotion-replay` accepts the baseline and candidate artifacts without a target mismatch
- the resulting evidence surface compares like-for-like sleeves instead of mixed labels

### 4. Only then evaluate slower-anchor admission and overlap edge cases

Why last:

- The walk-forward, regime, and output-separation layers already exist; the missing piece is an honest slower-anchor replay lane.
- Constructor trade-state merging is now conservative, but deeper overlap issues should only be chased once the slower sleeve is actually running on the shared surface.

Deliverables:

- a real `fundamental_rerating_core` replay result on the existing walk-forward and regime evidence surface
- targeted constructor fixes only if the slower sleeve exposes a concrete overlap-state failure
- updated docs that record whether the slower anchor was admitted or rejected and on what evidence

Success check:

- the slower sleeve is admitted or rejected on the same evidence surface as the checked-in trend example
- no target relabeling, zero-filled residual terms, or hidden in-code factor-return estimation was required to make the comparison run

## Working Rule

Do not "complete" this slice by mixing residual and non-residual sleeves, relabeling `target_id`s, or adding a hidden in-code factor-return estimator.
The honest next move is to obtain the residual-component snapshot first, then build a same-target slower-anchor replay lane on top of the evidence surface that already exists.
