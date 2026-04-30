# Promotion Validation Next Slice

**Goal:** turn the current honest dual-sleeve weekly replay from a credible example into anti-overfitting research evidence that can justify later paper-trading admission.

## Current Verified State

- `PYTHONPATH=src python3 -m unittest discover -s tests -v` now passes with `77` tests.
- `run-promotion-replay` is reproducible on the checked-in real-output case and now emits:
  - `decision` plus `snapshot` for the narrow placeholder gate
  - `research_evidence` for the broader validation surface
  - `research_evidence.walk_forward` with anchored split summaries and stability aggregates
  - `research_evidence.regime_breakdown` with regime buckets, stability tables, and weak sub-periods
- Replay-case loading now rejects mixed-target sleeves up front. Used sleeve artifacts must share one `target_id` before they can be compared on the same replay surface.
- `build-trend-research-input` now supports a residual replay lane through `residualization_mode = "audited_residual_components"` plus `residual_component_snapshot_path`.
- Residual replay scaffolding is already checked in:
  - `config/descriptor_sets/trend_leadership_core_residual.toml`
  - `config/sleeves/trend_leadership_core_residual.toml`
  - `research/examples/trend_input_build_minimal/trend_leadership_core_residual.toml`
  - `research/examples/artifact_build_minimal/fundamental_rerating_core_output.toml`
  - `research/examples/artifact_build_minimal/trend_leadership_core_residual_output.toml`
  - `research/examples/promotion_replay_real_output_residual/`
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
- The real-output artifact build case now exists, but it still depends on generated input that cannot be produced until the audited residual snapshot arrives.

Deliverables:

- a sleeve-artifact build case that consumes `output/fundamental_rerating_core_input.json`
- a generated `fundamental_rerating_core` artifact built from the real-output observation input
- verification that the artifact preserves the same target label and weekly decision calendar expected by replay

Success check:

- `build-sleeve-artifact` succeeds from the generated fundamental input
- the artifact is no longer a checked-in synthetic placeholder for promotion-replay work

### 3. Run the honest same-target comparator lane for replay

Why third:

- Replay now correctly rejects mixed-target sleeves, so the current trend real-output artifacts cannot be combined with a residualized fundamental sleeve.
- The system needs one shared return basis before slower-anchor admission can be evaluated.

Deliverables:

- a generated residualized trend real-output artifact on `open_t1_to_open_t20_residual_net_cost`
- a generated slower-anchor artifact on the same residual target
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

## Continuation Decision

Recommended continuation: keep the current non-residual trend example intact and add a second, residualized real-output replay lane for slower-anchor admission.

Why this is the cleanest branch:

- `config/sleeves/fundamental_rerating_core.toml:1-21` already binds the slower anchor to `open_t1_to_open_t20_residual_net_cost`.
- `config/sleeves/trend_leadership_core.toml:1-21` still binds the established trend baseline to `open_t1_to_open_t20_net_cost`.
- `src/alpha_find_v2/research_artifact_loader.py:491-502` now explicitly rejects replay cases that mix target labels.
- `src/alpha_find_v2/trend_research_input_builder.py:197-204` now allows `residualization_mode='audited_residual_components'`, but only when a residual target and audited snapshot path are both supplied.
- `src/alpha_find_v2/fundamental_research_input_builder.py:183-213` and `:692-752` already enforce the audited residual-snapshot boundary for the slower sleeve.
- `research/examples/trend_input_build_minimal/trend_leadership_core_residual.toml:1-10` and `research/examples/promotion_replay_real_output_residual/replay_case.toml:1-41` already define the residual same-target replay lane.
- `research/examples/promotion_replay_real_output/replay_case.toml:1-41` is a verified non-residual trend-vs-resilience lane and should stay stable as a working example.

This means the next slice should not mutate the existing honest example into a different research question. It should add a parallel residual replay lane that answers the slower-anchor admission question directly.

## Current Workspace Preconditions

As of this planning pass:

- `output/research_source.duckdb` is present locally.
- `output/csi800_benchmark_state_history.json` is present locally.
- `output/open_t1_to_open_t20_residual_component_snapshot.json` is still missing.
- the residual real-output build and replay case files already exist locally, but none of them can run end-to-end until the missing residual snapshot is supplied.

Blocker audit update as of `2026-04-25`:

- `output/research_source.duckdb` exposes only PIT ranking / reference tables (`daily_bar_pit`, `fundamental_snapshot_pit`, `industry_classification_pit`, `benchmark_membership_pit`, `benchmark_weight_snapshot_pit`, and related calendar / security refs). It does not expose residual-component rows, target-return rows, or `benchmark` / `size` / `beta` component columns that could be exported directly into the required snapshot.
- `output/pit_reference_staging.duckdb` is even narrower: it only carries `benchmark_membership_pit`, `benchmark_weight_snapshot_pit`, and `industry_classification_pit`.
- A filesystem search under `/home/nan` confirms that `open_t1_to_open_t20_residual_component_snapshot.json` does not already exist elsewhere on the machine.
- Local legacy audited DuckDBs under `../alpha-find/output/` also do not expose a ready residual-component snapshot or direct `benchmark` / `industry` / `size` / `beta` contribution table that can be copied into V2 without a separate audited export step.
- Remote repo audit on `2026-04-25` shows `origin/main` at `5d32dec52e2c7c0873c485afe267dbe49e83b217` still tracks only the residual target / risk-model skeleton (`config/risk_models/a_share_core_equity.toml`, `config/targets/open_t1_to_open_t20_residual_net_cost.toml`, `src/alpha_find_v2/risk_model.py`, and `tests/test_risk_model.py`), not a checked-in residual-component snapshot or audited export command.
- Remote ref audit on `2026-04-25` also shows both V2 and legacy repos expose only `origin/main` and no remote tags, while reachable V2 history contains only the baseline rebuild commit plus the current evidence-boundary commit, so there is no hidden snapshot / export path in an alternate fetched ref.
- GitHub releases for `miraclecn/alpha-finder-v2` are also empty as of `2026-04-25`, so there is no published release asset carrying the missing snapshot either.
- The legacy repo's tracked `main` branch under `../alpha-find` also contains no checked-in path matching `residual_component`, `open_t1_to_open_t20_residual`, `factor_return`, `risk_model`, or `snapshot`, so there is no hidden copy source there either.
- `src/alpha_find_v2/risk_model.py` only exposes a decomposition interface that consumes already-audited factor-return snapshots; `docs/architecture/risk-model-and-simulation-loop.md` explicitly keeps internal factor-return estimation out of scope for this stage.

That narrows the real blocker to one external audited artifact. The residual comparator build lane is already defined in-repo, but it cannot execute honestly until that snapshot arrives. The honest next move is to obtain the snapshot from its audited upstream generation path, not to invent the residualization inputs inside this repo.

## Execution Breakdown

Implementation state as of `2026-04-25`:

- Task C code work is already landed in `src/alpha_find_v2/trend_research_input_builder.py` and `tests/test_trend_research_input_builder.py`.
- Task B and Task D config / example scaffolding is already landed in `research/examples/artifact_build_minimal/`, `research/examples/trend_input_build_minimal/`, and `research/examples/promotion_replay_real_output_residual/`.
- The remaining execution work is now operational rather than structural: obtain the audited snapshot, run the command chain, and document the residual replay verdict.

### Task A. Freeze the audited residual-snapshot contract before any builder changes

Files / surfaces involved:

- `research/examples/fundamental_input_build_minimal/fundamental_rerating_core.toml`
- `research/examples/fundamental_input_build_minimal/README.md`
- external artifact: `output/open_t1_to_open_t20_residual_component_snapshot.json`

- [ ] Confirm the incoming snapshot uses `schema_version = 1` and `artifact_type = "residual_component_snapshot"`, matching `src/alpha_find_v2/fundamental_research_input_builder.py:705-716`.
- [ ] Confirm the snapshot `target_id` is exactly `open_t1_to_open_t20_residual_net_cost`, matching the slower sleeve target and the build-case target check in `src/alpha_find_v2/fundamental_research_input_builder.py:717-721`.
- [ ] Confirm every selected `(trade_date, asset_id)` observation carries all four required components: `benchmark`, `industry`, `size`, and `beta`, matching `config/targets/open_t1_to_open_t20_residual_net_cost.toml:1-12` and the missing-component guard in `src/alpha_find_v2/fundamental_research_input_builder.py:740-751`.
- [ ] Record provenance in plain text before using the snapshot: source tables, benchmark definition, industry schema, and generation date.

Stop condition:

- If the residual snapshot is on a different return basis, omits component coverage for selected names, or is generated by an unaudited in-repo estimator, stop and fix the input contract first.

### Task B. Promote the slower sleeve from generated input to a real artifact without breaking the synthetic examples

Files to add or modify:

- Use existing `research/examples/artifact_build_minimal/fundamental_rerating_core_output.toml`
- Keep `research/examples/artifact_build_minimal/fundamental_rerating_core.toml` unchanged as the synthetic regression fixture
- Optionally update `research/examples/promotion_replay_real_output/README.md` to point to the new residual lane once it exists

- [ ] Use the existing real-output artifact build case that reads `output/fundamental_rerating_core_input.json` and writes `output/fundamental_rerating_core_artifact.json`.
- [ ] Do not repoint the checked-in synthetic case away from `research/examples/artifact_build_minimal/fundamental_rerating_core_input.json`; that file still serves schema and loader stability.
- [ ] Build the artifact only after Task A's snapshot passes contract review and `build-fundamental-research-input` produces generated output.

Success checks:

- `PYTHONPATH=src python3 -m alpha_find_v2 build-fundamental-research-input --case research/examples/fundamental_input_build_minimal/fundamental_rerating_core.toml`
- `PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/fundamental_rerating_core_output.toml`

Expected result:

- `output/fundamental_rerating_core_artifact.json` exists and carries `target_id = "open_t1_to_open_t20_residual_net_cost"`.

### Task C. Add one residualized trend comparator lane instead of trying to reuse the current non-residual lane

Recommended scope:

- residualize `trend_leadership_core` only
- keep `trend_resilience_core` and the current non-residual example unchanged
- use the same audited residual-snapshot mechanism as the slower sleeve, not a new estimator path

Files to add or modify:

- Use existing `src/alpha_find_v2/trend_research_input_builder.py`
- Use existing `tests/test_trend_research_input_builder.py`
- Use existing `research/examples/trend_input_build_minimal/trend_leadership_core_residual.toml`
- Use existing `research/examples/artifact_build_minimal/trend_leadership_core_residual_output.toml`

- [ ] Re-run the residual trend unit tests to keep the audited-snapshot contract pinned in place.
- [ ] Build the residual trend observation input from the existing audited residual mode and snapshot-path contract; do not reopen the builder design unless the snapshot itself reveals a real contract mismatch.
- [ ] Preserve the current non-residual path unchanged so `research/examples/promotion_replay_real_output/` remains reproducible.

Success checks:

- `PYTHONPATH=src python3 -m unittest tests.test_trend_research_input_builder -v`
- `PYTHONPATH=src python3 -m alpha_find_v2 build-trend-research-input --case research/examples/trend_input_build_minimal/trend_leadership_core_residual.toml`
- `PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_residual_output.toml`

Expected result:

- `output/trend_leadership_core_residual_artifact.json` exists and carries the same residual target id as the slower sleeve.

### Task D. Add a second honest replay case that answers the slower-anchor admission question directly

Files to add:

- existing `research/examples/promotion_replay_real_output_residual/README.md`
- existing `research/examples/promotion_replay_real_output_residual/baseline_portfolio.toml`
- existing `research/examples/promotion_replay_real_output_residual/candidate_portfolio.toml`
- existing `research/examples/promotion_replay_real_output_residual/replay_case.toml`

- [ ] Baseline recipe: residualized `trend_leadership_core` only.
- [ ] Candidate recipe: the same baseline plus `fundamental_rerating_core`.
- [ ] Replay case artifact paths: `output/trend_leadership_core_residual_artifact.json` and `output/fundamental_rerating_core_artifact.json`.
- [ ] Reuse the existing benchmark state artifact unless the residual lane exposes a calendar mismatch.
- [ ] Start with the same walk-forward anchors now used by `research/examples/promotion_replay_real_output/replay_case.toml:21-35`; only shift them if the generated residual lane lacks date coverage.

Success checks:

- `PYTHONPATH=src python3 -m alpha_find_v2 run-promotion-replay --case research/examples/promotion_replay_real_output_residual/replay_case.toml`

Expected result:

- replay loads cleanly without target mismatch
- output includes `decision`, `snapshot`, and `research_evidence`
- the slower anchor is evaluated on the same residual return basis as the baseline sleeve

### Task E. Only then evaluate admission and chase overlap bugs if they become concrete

Files / outputs involved:

- replay JSON emitted from the new residual replay case
- `docs/superpowers/plans/2026-04-24-promotion-validation-next-slice.md`
- residual replay README under `research/examples/promotion_replay_real_output_residual/`

- [ ] Review `research_evidence.walk_forward`, `research_evidence.regime_breakdown`, and incrementality diagnostics before looking at the gate verdict.
- [ ] Record one explicit decision: admit, reject, or hold for more data.
- [ ] If constructor overlap or trade-state bugs surface, add the narrowest failing regression first and only then patch the constructor.
- [ ] Update docs with the evidence that drove the decision, not just the decision label.

Completion condition:

- The slower sleeve is either rejected honestly or admitted honestly on a shared residual target surface, with no relabeled targets and no hidden residualization shortcut.

## Verification Matrix

Before claiming the slice is complete, the following should all be green:

- `PYTHONPATH=src python3 -m unittest tests.test_fundamental_research_input_builder -v`
- `PYTHONPATH=src python3 -m unittest tests.test_trend_research_input_builder -v`
- `PYTHONPATH=src python3 -m unittest tests.test_research_artifact_builder -v`
- `PYTHONPATH=src python3 -m unittest tests.test_research_artifact_loader -v`
- `PYTHONPATH=src python3 -m unittest tests.test_promotion_replay -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -v`

And the real-output command chain should run in this order:

1. `PYTHONPATH=src python3 -m alpha_find_v2 build-fundamental-research-input --case research/examples/fundamental_input_build_minimal/fundamental_rerating_core.toml`
2. `PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/fundamental_rerating_core_output.toml`
3. `PYTHONPATH=src python3 -m alpha_find_v2 build-trend-research-input --case research/examples/trend_input_build_minimal/trend_leadership_core_residual.toml`
4. `PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_residual_output.toml`
5. `PYTHONPATH=src python3 -m alpha_find_v2 run-promotion-replay --case research/examples/promotion_replay_real_output_residual/replay_case.toml`

## Rejected Shortcuts

- Do not relabel `trend_leadership_core` artifacts from `open_t1_to_open_t20_net_cost` to the residual target.
- Do not reintroduce empty or placeholder residual components in the trend lane and then treat them as audited evidence.
- Do not overwrite the checked-in synthetic minimal replay fixtures just to make the new lane look "real."
- Do not add a hidden in-code factor-return estimator to get around the missing residual snapshot.
- Do not reinterpret the existing trend-vs-resilience real-output example as the slower-anchor admission test.

## Immediate Unblock Runbook

The next session should treat the missing residual snapshot as an intake problem,
not a modeling problem. The goal is to accept one audited upstream export or
reject it quickly and honestly.

### 1. Upstream artifact request contract

Request exactly one file:

- path: `output/open_t1_to_open_t20_residual_component_snapshot.json`
- schema: `schema_version = 1`
- artifact type: `residual_component_snapshot`
- target id: `open_t1_to_open_t20_residual_net_cost`
- risk model: `a_share_core_equity`
- benchmark basis: `CSI 800`
- residual components on every selected row: `benchmark`, `industry`, `size`, `beta`
- coverage basis: weekly decision dates and selected names used by the
  `trend_leadership_core_residual` and `fundamental_rerating_core` build cases
- provenance note required alongside the file: source system, generation date,
  benchmark definition, industry schema, and the audited upstream code path

Derived minimal coverage from the current checked-in build cases plus local
`output/research_source.duckdb` as of `2026-04-25`:

- decision dates: `20260305`, `20260312`, `20260319`
- `fundamental_rerating_core` currently requests `3 * 18 = 54` selected
  observations across `21` unique asset ids
- `trend_leadership_core_residual` currently requests `3 * 16 = 48`
  selected observations across `26` unique asset ids
- minimal union coverage is `102` selected `(trade_date, asset_id)` rows
  across `47` unique asset ids
- per-date union coverage is `34` rows on each checked decision date
- the exact required union rows are pinned in
  `research/examples/promotion_replay_real_output_residual/residual_snapshot_required_coverage.json`
  so upstream export and local intake can compare against one machine-readable
  manifest instead of re-deriving the case union by hand

An upstream export may be broader than this minimal union, but it must at least
cover these case-derived observations on the same residual target surface. If
the local DuckDB or either build case changes, recompute this coverage before
requesting or accepting a fresh snapshot.

Plain-text handoff message:

```text
Please export `output/open_t1_to_open_t20_residual_component_snapshot.json`
for target `open_t1_to_open_t20_residual_net_cost`.

Required contract:
- schema_version: 1
- artifact_type: residual_component_snapshot
- target_id: open_t1_to_open_t20_residual_net_cost
- risk model: a_share_core_equity
- benchmark basis: CSI 800
- residual_components on every record: benchmark, industry, size, beta
- steps keyed by trade_date, records keyed by asset_id

Please include provenance notes for the benchmark definition, industry schema,
generation date, and the audited upstream export path. A V2-local estimator or
manually reconstructed snapshot is not acceptable.

Current minimal coverage for the checked-in residual replay lane is:
- 3 decision dates from 20260305 through 20260319
- 102 required selected observations in the union of the trend and
  fundamental build cases
- 47 unique asset ids across that union

A broader export is acceptable if it stays on the same target surface and
includes the required provenance.
```

Stop condition:

- If the provider cannot name the benchmark basis, industry schema, or audited
  export path, do not ingest the file.

### 2. On-arrival header and shape check

Run:

```bash
cd /home/nan/alpha-find-v2
python3 - <<'PY'
import json
from pathlib import Path

path = Path("output/open_t1_to_open_t20_residual_component_snapshot.json")
payload = json.loads(path.read_text(encoding="utf-8"))
print({
    "schema_version": payload.get("schema_version"),
    "artifact_type": payload.get("artifact_type"),
    "target_id": payload.get("target_id"),
    "step_count": len(payload.get("steps", [])),
})
PY
```

Expected:

- `schema_version` is `1`
- `artifact_type` is `residual_component_snapshot`
- `target_id` is `open_t1_to_open_t20_residual_net_cost`
- `step_count` is non-zero

If any field differs, stop before running the builders.

### 3. On-arrival component completeness check

Run:

```bash
cd /home/nan/alpha-find-v2
python3 - <<'PY'
import json
from pathlib import Path

required = {"benchmark", "industry", "size", "beta"}
path = Path("output/open_t1_to_open_t20_residual_component_snapshot.json")
payload = json.loads(path.read_text(encoding="utf-8"))
missing = []
for step in payload.get("steps", []):
    trade_date = step["trade_date"]
    for record in step.get("records", []):
        components = set(record.get("residual_components", {}).keys())
        absent = sorted(required - components)
        if absent:
            missing.append((trade_date, record.get("asset_id"), absent))
print({"missing_count": len(missing), "sample": missing[:5]})
PY
```

Expected:

- `missing_count` is `0`

If any rows are missing required components, reject the snapshot before trying
the research-input builders.

### 4. On-arrival required coverage check

Run:

```bash
cd /home/nan/alpha-find-v2
python3 - <<'PY'
import json
from pathlib import Path

snapshot_path = Path("output/open_t1_to_open_t20_residual_component_snapshot.json")
coverage_path = Path(
    "research/examples/promotion_replay_real_output_residual/"
    "residual_snapshot_required_coverage.json"
)
snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
available = {
    (step["trade_date"], record["asset_id"])
    for step in snapshot.get("steps", [])
    for record in step.get("records", [])
}
required = {
    (step["trade_date"], asset_id)
    for step in coverage.get("records_by_trade_date", [])
    for asset_id in step.get("required_union_asset_ids", [])
}
missing = sorted(required - available)
print({"missing_count": len(missing), "sample": missing[:5]})
PY
```

Expected:

- `missing_count` is `0`

If any required `(trade_date, asset_id)` rows are absent, reject the snapshot
before trying the research-input builders.

### 5. Honesty-preserving execution chain after snapshot intake

Run, in order:

1. `PYTHONPATH=src python3 -m alpha_find_v2 build-fundamental-research-input --case research/examples/fundamental_input_build_minimal/fundamental_rerating_core.toml`
2. `PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/fundamental_rerating_core_output.toml`
3. `PYTHONPATH=src python3 -m alpha_find_v2 build-trend-research-input --case research/examples/trend_input_build_minimal/trend_leadership_core_residual.toml`
4. `PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_residual_output.toml`
5. `PYTHONPATH=src python3 -m alpha_find_v2 run-promotion-replay --case research/examples/promotion_replay_real_output_residual/replay_case.toml`

Expected progression:

- step 1 proves the slower anchor can load audited residual components
- step 3 proves the residualized trend comparator uses the same target basis
- step 5 is the first point where admit / reject / hold should be discussed

### 6. If the snapshot still cannot be obtained

Record the blocker as external and stop the slice here:

- missing audited upstream residual export path
- no V2-local residual component source in `output/research_source.duckdb`
- no acceptable copy-in source under `../alpha-find/output/`
- no tracked export implementation on V2 `origin/main` or legacy `alpha-find` `main`
- no published GitHub release asset carrying the snapshot
- the checked-in V2 risk-model layer is intentionally an interface, not an estimator

Do not reopen target-building or trend-builder design unless a new audited
artifact source appears.
