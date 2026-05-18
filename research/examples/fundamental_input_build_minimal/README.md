# Minimal Fundamental Input Build Example

This directory defines the build-case contract for the slower
`fundamental_rerating_core` sleeve on top of the isolated V2 DuckDB.

Status as of `2026-04-26`: this lane is intentionally paused for the current
personal-investor roadmap. The build case remains checked in as a future
resume contract, not an active release-1 requirement.

If phase `5` is explicitly resumed, run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python3 -m alpha_find_v2 validate-residual-snapshot
PYTHONPATH=src python3 -m alpha_find_v2 build-fundamental-research-input --case research/examples/fundamental_input_build_minimal/fundamental_rerating_core.toml
```

The builder now does two explicit jobs:

- rank names from `fundamental_snapshot_pit` plus PIT industry labels and
  next-open tradeability data
- join an external residual-component snapshot keyed to the exact residual
  target being evaluated

That second input is intentionally explicit. The checked-in repo does **not**
yet include `output/open_t1_to_open_t20_residual_component_snapshot.json`, so
this example is a build-boundary contract rather than a green end-to-end real
output lane.

As of `2026-04-26`, the bundled local DuckDBs were re-checked:

- `output/research_source.duckdb` contains the ranking inputs and PIT labels
  needed to select names, but it does not contain residual-component rows or
  direct `benchmark` / `industry` / `size` / `beta` contribution columns.
- `output/pit_reference_staging.duckdb` only carries PIT benchmark membership,
  benchmark weights, and industry labels.
- local legacy audited DuckDBs under `../alpha-find/output/` also do not
  expose a ready residual-component snapshot that can be copied straight into
  V2.

So the missing JSON is not just an omitted checked-in file. It must come from
an external audited export path with provenance notes, not from an ad hoc
in-repo estimator.

Until an audited residual-component snapshot exists, `fundamental_rerating_core`
must stay out of the honest real-output promotion replay example. For the
current roadmap cycle, that residual lane is paused rather than treated as a
live blocker.

If the phase is resumed and the snapshot arrives, `validate-residual-snapshot`
is the required first gate before the builder runs. It checks the header
contract, provenance metadata, residual component completeness, and the pinned
required coverage manifest.
