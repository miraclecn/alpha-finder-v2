# Minimal Deployment Example

This directory is the first V2 deployment-layer example.

Files:

- `executable_signal_case.toml`: binds a research-approved portfolio, sleeve artifacts, benchmark state, and an account-state snapshot into a next-open execution package
- `account_state_2026_04_20.json`: broker/account-facing live snapshot used to derive holdings, cash, and blocked exits
- `portfolio_state_2026_04_20.json`: records the live book as of the decision date, including holdings, cash, and blocked exits
- `decay_watch_case.toml`: binds a promoted expectation snapshot and a recent realized window into a decay-watch record

Run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/fundamental_rerating_core.toml
PYTHONPATH=src python -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core.toml
PYTHONPATH=src python -m alpha_find_v2 build-executable-signal --case research/examples/deployment_minimal/executable_signal_case.toml
PYTHONPATH=src python -m alpha_find_v2 evaluate-decay-watch --case research/examples/deployment_minimal/decay_watch_case.toml
```

The example remains pre-broker and pre-QMT on purpose.
Its job is to prove that V2 now has a formal bridge from approved portfolio targets to executable signals and then to post-promotion decay monitoring.
The deployment case no longer injects inline weights.
It reads a formal account-state snapshot and adapts it into a portfolio-state snapshot so blocked live holdings are preserved without silently re-scaling other target names above their desired weights.
