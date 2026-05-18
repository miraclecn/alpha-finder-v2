# Minimal Artifact Build Example

This directory is the first V2 example for turning normalized research observations into persisted sleeve artifacts.

Files:

- `fundamental_rerating_core.toml`: build case for the slower quality/value anchor sleeve
- `fundamental_rerating_core_output.toml`: real build case that consumes `output/fundamental_rerating_core_input.json`
- `trend_leadership_core.toml`: sample build case for the medium-horizon trend-leadership sleeve
- `trend_leadership_core_output.toml`: real build case that consumes `output/trend_leadership_core_input.json`
- `trend_leadership_core_residual_output.toml`: real build case that consumes `output/trend_leadership_core_residual_input.json`
- `trend_resilience_core_output.toml`: real build case that consumes `output/trend_resilience_core_input.json`
- `*_input.json`: decision-calendar research observations bound to each sleeve

Run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/fundamental_rerating_core.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/fundamental_rerating_core_output.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_output.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_residual_output.toml
PYTHONPATH=src python3 -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_resilience_core_output.toml
```

The output paths deliberately point at the promotion-replay example artifact directory so the same generated artifacts can then be reused by replay and deployment examples.

The `trend_leadership_core` example is intentionally bound to the interim
`open_t1_to_open_t20_net_cost` target. It is honest about net-of-cost
forward returns while audited PIT residualization inputs are still pending.
For the real DuckDB-backed path, build the trend observation input first and
then use `trend_leadership_core_output.toml`.
`trend_leadership_core_residual_output.toml` is the same trend descriptor lane
rebuilt on `open_t1_to_open_t20_residual_net_cost`; that residual lane is
currently paused and, if resumed later, still requires the audited
`output/open_t1_to_open_t20_residual_component_snapshot.json` artifact before
the generated observation input can exist honestly.
`trend_resilience_core_output.toml` uses the same honest target clock but
leans harder on stability and liquidity so generated replay can compare two
weekly green-data sleeves on one calendar.
`fundamental_rerating_core_output.toml` is the real-output counterpart to the
synthetic slower-anchor fixture and is intended to write
`output/fundamental_rerating_core_artifact.json` once the generated
fundamental observation input has been built.
