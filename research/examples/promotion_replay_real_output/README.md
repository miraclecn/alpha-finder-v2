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

`run-promotion-replay` now separates its JSON output into two top-level views:

- `decision` plus `snapshot`: the narrow placeholder admission readout for the
  current gate
- `research_evidence`: the broader anti-overfitting evidence surface that shows
  whether the candidate is economically incremental even when the placeholder
  gate still rejects it

Inside `research_evidence`, the replay emits a compact `diagnostics` block so
sleeve admission work can be driven by evidence instead of cosmetic metric
chasing:

- `incrementality`: signal overlap, candidate-only weight, and candidate-only
  return contribution versus the baseline book
- `concentration`: average live name count, effective names, and cash usage
- `best_periods` / `worst_periods`: the dates where the candidate most helped
  or hurt the baseline path

The checked-in real-output case now also defines anchored walk-forward splits
starting at `20250829`, `20251020`, `20251201`, and `20260114`. The replay
output exposes them under `research_evidence.walk_forward`:

- `splits`: one summary per anchor date, including baseline/candidate IR,
  drawdown, breadth, and marginal contribution
- `stability`: aggregate worst/average metrics across anchors so weak entry
  dates are visible instead of being washed out by the full-window average

The same replay output now also emits
`research_evidence.regime_breakdown` so the evidence
surface answers "when did the candidate help?" instead of only "what was the
full-window average?":

- `buckets`: fixed sub-views for `trend_up`, `trend_down`, `drawdown`, and
  `weak_breadth`
- `trend_up` / `trend_down` are defined from the candidate replay-period net
  return sign, not from an external market classifier
- `weak_breadth` isolates dates where the candidate live name count falls below
  the full-window average, while still carrying the same incrementality and
  concentration diagnostics as the full replay
- `stability`: worst/average IR, drawdown, and breadth across the non-empty
  buckets
- `weak_subperiods`: contiguous runs where the candidate stops helping on a
  marginal basis, remains underwater, or trades with weak breadth

On the current checked-in output this matters: the candidate still improves
the full-window IR and drawdown, but the bucket view shows that most of the
help comes from `trend_down` / `drawdown` periods while `trend_up` is weaker.
That is the kind of regime-fragility signal this slice was meant to expose
before pushing slower anchor sleeves harder.

The current checked-in candidate uses a more stability-heavy 18-name
`trend_resilience_core` sleeve with a `50/50` trend-plus-resilience blend.
On the honest `2025-08-29+` window this still fails the placeholder gate, but
it improves the marginal IR delta materially while reducing drawdown and
raising candidate-only contribution.

The slower `fundamental_rerating_core` anchor is not wired into this honest
real-output lane yet. Its sleeve target is currently
`open_t1_to_open_t20_residual_net_cost`, while the checked-in trend
real-output builder only supports explicit non-residual targets. The repo now
has a dedicated `build-fundamental-research-input` path plus a checked-in case
contract under `research/examples/fundamental_input_build_minimal/`, but that
path still requires an audited
`output/open_t1_to_open_t20_residual_component_snapshot.json` input. Until
that residual-component snapshot exists, a fundamental-vs-trend real-output
replay would either mix incompatible return labels or smuggle in an internal
toy factor-return estimator, and should not be treated as promotion evidence.
