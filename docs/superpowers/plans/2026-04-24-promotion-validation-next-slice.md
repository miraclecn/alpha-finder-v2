# Promotion Validation Next Slice

**Goal:** turn the current honest dual-sleeve weekly replay from a credible example into anti-overfitting research evidence that can justify later paper-trading admission.

## Current Verified State

- `PYTHONPATH=src python3 -m unittest discover -s tests -v` passes with `64` tests.
- `PYTHONPATH=src python3 -m alpha_find_v2 run-promotion-replay --case research/examples/promotion_replay_real_output/replay_case.toml` is reproducible on the checked-in real-output case.
- The candidate weekly book is now economically incremental instead of mostly re-buying the same names:
  - baseline IR: `0.3315`
  - candidate IR: `0.4290`
  - marginal IR delta: `0.0975`
  - candidate drawdown: `0.4896`
  - average signal-name Jaccard: `0.6279`
  - average candidate-only weight: `0.1019`
  - average candidate-only return contribution: `0.00192`
- The current example still does **not** pass the placeholder replay gate because:
  - `max_peak_to_trough_drawdown`
  - `minimum_marginal_ir_delta`

## Ordered Next Steps

### 1. Add anchored walk-forward validation on top of promotion replay

Why first:

- The current replay is honest, but it still lives on one short checked-in window.
- Before adding more sleeves, the system needs evidence that a candidate survives multiple entry dates rather than a single favorable path.

Deliverables:

- a walk-forward split definition for replay cases
- replay output that reports per-split IR, drawdown, breadth, and marginal contribution
- deterministic tests that lock the split logic and summary aggregation

Success check:

- the same candidate can be evaluated across multiple anchored out-of-sample slices from the checked-in replay calendar
- the report makes instability visible instead of hiding it in one aggregate number

### 2. Add regime and stability breakdown reporting

Why second:

- A-share sleeves often look acceptable in aggregate while being regime-fragile.
- The admission question is not only "did the sleeve help?" but also "when did it help, and when did it break?"

Deliverables:

- regime buckets and stability tables on top of replay periods
- concentration and overlap diagnostics carried through each bucket
- explicit identification of weak sub-periods instead of only best/worst single dates

Success check:

- the replay output shows whether the candidate is robust in trend, drawdown, and weak-breadth conditions

### 3. Separate example-gate output from research-evidence output

Why third:

- The current real-output example is useful, but `passed = false` is easy to misread as "the work failed".
- The repo needs a cleaner distinction between:
  - a narrow placeholder admission gate
  - a broader research evidence report

Deliverables:

- either a dedicated research-evidence command or a second report block from `run-promotion-replay`
- docs that explain the difference between replay evidence and promotion admission

Success check:

- users can see whether a sleeve is economically incremental even when the narrow demo gate still rejects it

### 4. Only then push slower anchor sleeves harder

Why last:

- Without the validation layer above, adding a slower anchor sleeve risks optimizing to the current window.
- More aggressive sleeve differentiation may also trigger overlapping-name trade-state conflicts in `portfolio_constructor.py`.

Deliverables:

- audited `fundamental_rerating_core` replay lane on the same validation surface
- investigation and repair of inconsistent overlapping trade-state handling if the slower sleeve exposes it

Success check:

- a slower sleeve is admitted or rejected on the same walk-forward and regime evidence surface as the current dual-trend example

## Working Rule

Do not spend the next slice chasing the current placeholder gate by threshold tweaking alone.
The correct next move is to improve the evidence surface first, then admit or reject new sleeves on that surface.
