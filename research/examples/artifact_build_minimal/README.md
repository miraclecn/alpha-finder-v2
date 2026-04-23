# Minimal Artifact Build Example

This directory is the first V2 example for turning normalized research observations into persisted sleeve artifacts.

Files:

- `fundamental_rerating_core.toml`: build case for the slower quality/value anchor sleeve
- `trend_leadership_core.toml`: build case for the medium-horizon trend-leadership sleeve
- `*_input.json`: decision-calendar research observations bound to each sleeve

Run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/fundamental_rerating_core.toml
PYTHONPATH=src python -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core.toml
```

The output paths deliberately point at the promotion-replay example artifact directory so the same generated artifacts can then be reused by replay and deployment examples.
