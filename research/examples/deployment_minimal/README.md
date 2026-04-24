# Minimal Deployment Example

This directory is the first V2 deployment-layer example.
It now carries two lanes:

- the original persisted sample case for schema and test stability
- a real-output case that consumes generated `output/` benchmark and trend artifacts directly

Files:

- `executable_signal_case.toml`: binds a research-approved portfolio, sleeve artifacts, benchmark state, and an account-state snapshot into a next-open execution package
- `executable_signal_real_output_case.toml`: binds the generated `output/csi800_benchmark_state_history.json` and `output/trend_leadership_core_artifact.json` into a real-output next-open execution package
- `candidate_portfolio.toml`: deployment-only portfolio recipe used by the persisted executable-signal and decay-watch samples
- `account_state_2026_04_20.json`: broker/account-facing live snapshot used to derive holdings, cash, and blocked exits
- `account_state_20260319_trend_real.json`: synthetic broker snapshot aligned to the real 20260319 decision date; holdings mirror the prior weekly trend sleeve so the case emits honest buys, sells, a blocked exit, and a blocked entry
- `portfolio_state_2026_04_20.json`: records the live book as of the decision date, including holdings, cash, and blocked exits
- `trend_real_output_portfolio.toml`: trend-only portfolio recipe used for the real-output executable-signal case
- `decay_watch_case.toml`: binds a promoted expectation snapshot and a recent realized window into a decay-watch record

Run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/fundamental_rerating_core.toml
PYTHONPATH=src python -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core.toml
PYTHONPATH=src python -m alpha_find_v2 build-executable-signal --case research/examples/deployment_minimal/executable_signal_case.toml
PYTHONPATH=src python -m alpha_find_v2 evaluate-decay-watch --case research/examples/deployment_minimal/decay_watch_case.toml
```

Real-output run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python -m alpha_find_v2 build-benchmark-state --case research/examples/benchmark_state_build_minimal/csi800.toml
PYTHONPATH=src python -m alpha_find_v2 build-trend-research-input --case research/examples/trend_input_build_minimal/trend_leadership_core.toml
PYTHONPATH=src python -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_output.toml
PYTHONPATH=src python -m alpha_find_v2 build-executable-signal --case research/examples/deployment_minimal/executable_signal_real_output_case.toml
```

The example remains pre-broker and pre-QMT on purpose.
Its job is to prove that V2 now has a formal bridge from approved portfolio targets to executable signals and then to post-promotion decay monitoring.
The deployment case no longer injects inline weights.
It reads a formal account-state snapshot and adapts it into a portfolio-state snapshot so blocked live holdings are preserved without silently re-scaling other target names above their desired weights.
The real-output case is the current honest end-to-end frontier.
It uses generated benchmark state and generated trend sleeve artifacts, but it still relies on a synthetic account snapshot because broker integration and tradeability reconstruction are not finished.
