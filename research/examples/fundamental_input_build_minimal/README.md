# Minimal Fundamental Input Build Example

This directory defines the build-case contract for the slower
`fundamental_rerating_core` sleeve on top of the isolated V2 DuckDB.

Run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python -m alpha_find_v2 build-fundamental-research-input --case research/examples/fundamental_input_build_minimal/fundamental_rerating_core.toml
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

Until an audited residual-component snapshot exists, `fundamental_rerating_core`
must stay out of the honest real-output promotion replay example.
