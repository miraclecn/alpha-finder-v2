# Real-Output Promotion Replay Example

This directory is the first honest promotion replay lane that binds only
generated `output/` artifacts on the shared `2025-08-29+` weekly decision
calendar.

It compares:

- baseline: the existing `trend_leadership_core` sleeve on its own
- candidate: the same weekly trend book plus a second, more defensive
  `trend_resilience_core` sleeve built from the same green-data research stack

Run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python -m alpha_find_v2 build-benchmark-state --case research/examples/benchmark_state_build_minimal/csi800.toml
PYTHONPATH=src python -m alpha_find_v2 build-trend-research-input --case research/examples/trend_input_build_minimal/trend_leadership_core.toml
PYTHONPATH=src python -m alpha_find_v2 build-trend-research-input --case research/examples/trend_input_build_minimal/trend_resilience_core.toml
PYTHONPATH=src python -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_leadership_core_output.toml
PYTHONPATH=src python -m alpha_find_v2 build-sleeve-artifact --case research/examples/artifact_build_minimal/trend_resilience_core_output.toml
PYTHONPATH=src python -m alpha_find_v2 run-promotion-replay --case research/examples/promotion_replay_real_output/replay_case.toml
```

This is still an example lane, not a production promotion decision.
Its role is narrower:

- prove that V2 can compare two generated weekly sleeves on the exact same
  benchmark calendar
- keep the replay honest without falling back to synthetic sleeve artifacts
- establish the bridge that later allows a slower anchor sleeve to compete for
  admission on the same portfolio replay surface

Under the current generated artifacts and the placeholder
`research_example_replay_gate`, the candidate book improves return and IR but
does not yet pass the gate. That is acceptable here. The purpose of this lane
is to make the comparison real, not to force an approval outcome.

`run-promotion-replay` now also emits a compact `diagnostics` block so sleeve
admission work can be driven by evidence instead of cosmetic metric chasing:

- `incrementality`: signal overlap, candidate-only weight, and candidate-only
  return contribution versus the baseline book
- `concentration`: average live name count, effective names, and cash usage
- `best_periods` / `worst_periods`: the dates where the candidate most helped
  or hurt the baseline path

The current checked-in candidate uses a more stability-heavy 18-name
`trend_resilience_core` sleeve with a `50/50` trend-plus-resilience blend.
On the honest `2025-08-29+` window this still fails the placeholder gate, but
it improves the marginal IR delta materially while reducing drawdown and
raising candidate-only contribution.
