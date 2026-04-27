# Residual Real-Output Promotion Replay Example

This directory defines the second honest promotion replay lane: a residualized
same-target comparison for slower-anchor admission.

Status as of `2026-04-26`: this lane is intentionally paused for the current
personal-investor roadmap. The files here preserve the resume path; they are
not part of the active release-1 acceptance chain.

It is configured to compare:

- baseline: `trend_leadership_core_residual`
- candidate: `trend_leadership_core_residual + fundamental_rerating_core`

Both sleeves are bound to `open_t1_to_open_t20_residual_net_cost`, so replay
can evaluate them on one residual return basis without relabeling targets.

If this lane is explicitly resumed in the future, run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python3 -m alpha_find_v2 build-residual-snapshot-required-coverage
PYTHONPATH=src python3 -m alpha_find_v2 validate-residual-snapshot
PYTHONPATH=src python3 -m alpha_find_v2 build-fundamental-research-input --case research/examples/fundamental_input_build_minimal/fundamental_rerating_core.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/fundamental_rerating_core_output.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-trend-research-input --case research/examples/trend_input_build_minimal/trend_leadership_core_residual.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_residual_output.toml
PYTHONPATH=src python3 -m alpha_find_v2 run-promotion-replay --case research/examples/promotion_replay_real_output_residual/replay_case.toml
```

This lane is currently paused. Even if resumed later, it is not runnable in
the checked-in workspace yet because
`output/open_t1_to_open_t20_residual_component_snapshot.json` is still
missing. The examples here preserve the honest same-target path once that
audited artifact is supplied.

It also inherits the same checked benchmark-state boundary as the non-residual
trend mainline: the current replayable benchmark calendar begins on
`2026-03-05`, not `2025-08-29`.

Minimal snapshot coverage for the current checked-in cases, derived from the
local `output/research_source.duckdb` as of `2026-04-26`:

- decision dates: `20260305`, `20260312`, `20260319`
- `fundamental_rerating_core` currently selects `54` observations
- `trend_leadership_core_residual` currently selects `48` observations
- the minimal union that the upstream residual snapshot must cover is `102`
  `(trade_date, asset_id)` rows across `47` unique asset ids
- the exact required union rows are pinned in
  `residual_snapshot_required_coverage.json`
- the pinned manifest is now rebuildable from the checked-in residual cases via
  `PYTHONPATH=src python3 -m alpha_find_v2 build-residual-snapshot-required-coverage`
- the two sleeves barely overlap on the current local data, so a full upstream
  export for all selected names on each listed date is safer than trying to
  hand-trim rows

Status as of `2026-04-26`:

- the checked-in V2 DuckDBs do not contain residual-component rows or direct
  `benchmark` / `industry` / `size` / `beta` contribution columns
- a filesystem search under `/home/nan` found no existing
  `open_t1_to_open_t20_residual_component_snapshot.json`
- local legacy audited DuckDBs under `../alpha-find/output/` also do not
  expose a ready snapshot / contribution table that can be copied into this
  lane without a separate audited export step
- remote `origin/main` as checked on `2026-04-25` still tracks only the
  residual target / risk-model skeleton, not a residual-component snapshot or
  audited export command
- both V2 and legacy remotes currently expose only `origin/main` and no remote
  tags, so there is no alternate fetched ref carrying the missing snapshot or
  export path
- GitHub releases for `miraclecn/alpha-finder-v2` are also empty as of
  `2026-04-25`, so there is no published release asset fallback
- the checked-in V2 `risk_model.py` layer is intentionally a decomposition
  interface, not an internal factor-return estimator

So this replay is intentionally parked for now. If it is resumed later, it
remains honestly blocked on one external artifact. Do not "unstick" it by
relabeling targets, zero-filling residual terms, or adding a hidden estimator
path inside V2.

If the lane is resumed and the audited snapshot arrives, first rebuild the
required coverage manifest if the local DuckDB or either residual build case
changed, then run
`validate-residual-snapshot` before any builders. The validation command
enforces the pinned target id, risk model, provenance fields, residual
component completeness, and required coverage manifest in one place.
