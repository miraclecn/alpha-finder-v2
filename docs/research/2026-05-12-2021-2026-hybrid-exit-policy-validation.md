# 2021 / 2026 Hybrid Exit Policy Validation - 2026-05-12

## Object
- Test a hybrid exit structure on the already-selected names from the prior price-volume selector research.
- Validation years: `2021` and `2026`.
- Important date boundary: the `2026` sample uses only currently available rows through `2026-04-30`.
- Outer frame stays fixed:
  - intraday hard take-profit
  - intraday hard stop-loss
  - max hold cap
- Price-volume state logic is only allowed to accelerate the exit.
  It never widens stop, delays take-profit, or extends max hold.

## Hybrid Mechanics
- Order of decisions inside each future bar:
  1. `stop_loss` / `same_day_stop_first`
  2. `take_profit`
  3. if current step already reaches `max_hold_days`, exit at that day's close
  4. otherwise, if a bad price-volume state appears, exit at the next day's open
- State layer candidates:
  - `strict`
  - `aggressive`
- Fixed outer frames tested:
  - `15% / 10% / 10d`
  - `15% / 10% / 12d`
  - `20% / 10% / 5d`
- Replay cost: `30bp` round-trip.

## Validation Table
| policy | 2021 mean net | 2026 mean net | avg exit step | both years positive |
| --- | --- | --- | --- | --- |
| `fixed_tp0.15_sl0.10_hold10` | `+0.253%` | `+0.072%` | `5.79` | `Yes` |
| `hybrid_strict_fixed_15_10_10` | `+0.243%` | `+0.328%` | `5.52` | `Yes` |
| `hybrid_aggressive_fixed_15_10_10` | `+0.101%` | `+0.217%` | `5.37` | `Yes` |
| `fixed_tp0.15_sl0.10_hold12` | `+0.193%` | `+0.406%` | `6.27` | `Yes` |
| `hybrid_strict_fixed_15_10_12` | `+0.173%` | `+0.649%` | `5.86` | `Yes` |
| `hybrid_aggressive_fixed_15_10_12` | `+0.062%` | `+0.436%` | `5.73` | `Yes` |
| `fixed_tp0.20_sl0.10_hold5` | `+0.154%` | `+1.008%` | `4.12` | `Yes` |
| `hybrid_strict_fixed_20_10_5` | `-0.030%` | `+1.075%` | `3.99` | `No` |
| `hybrid_aggressive_fixed_20_10_5` | `-0.123%` | `+1.100%` | `3.89` | `No` |

## Direct Reading
- The state layer is not universally helpful.
  It helps some outer frames and harms others.
- The most reliable hybrid improvements come from the `strict` state layer, not the `aggressive` one.
- The best two hybrids here are:
  - `hybrid_strict_fixed_15_10_10`
  - `hybrid_strict_fixed_15_10_12`

## Best Candidate 1: `hybrid_strict_fixed_15_10_12`
- Versus base `fixed_tp0.15_sl0.10_hold12`:
  - `2021`: `+0.173%` vs `+0.193%`
  - `2026`: `+0.649%` vs `+0.406%`
- Interpretation:
  - It gives up only about `0.020` percentage points in `2021`
  - but gains about `0.243` percentage points in `2026`
- Break-even round-trip cost budget is about `47.3bp`, so it still survives both repo cost references:
  - base cost model around `24bp`
  - high cost model around `36bp`
- This is the clearest “small sacrifice in the easier year, larger protection in the harder year” candidate.

## Best Candidate 2: `hybrid_strict_fixed_15_10_10`
- Versus base `fixed_tp0.15_sl0.10_hold10`:
  - `2021`: `+0.243%` vs `+0.253%`
  - `2026`: `+0.328%` vs `+0.072%`
- Interpretation:
  - `2021` is almost unchanged
  - `2026` improves by about `0.257` percentage points
- Break-even cost budget is about `54.3bp`, the best cost cushion among the robust hybrids.
- If the objective is “keep the rule simple, keep cost tolerance high, but use states to cut weaker late paths,” this is the cleanest hybrid in this run.

## Why `strict` Works Better Than `aggressive`
- `aggressive` exits more often, but most of that extra activity is not high-value activity.
- On the `15/10/10` and `15/10/12` frames:
  - `strict` only uses state exits on about `7.8%-9.8%` of trades
  - `aggressive` uses them on about `12.2%-14.0%` of trades
- That extra `3%-5%` of forced exits hurts `2021` more than it helps `2026`.
- So the useful information in the price-volume layer appears to be sparse.
  It is better used as a selective rescue rule than as a frequent intervention rule.

## Why the `20 / 10 / 5` Frame Does Not Want a State Layer
- The `20% / 10% / 5d` outer frame is already very front-loaded.
- Once the holding horizon is that short, the state layer is mostly competing with the time cap rather than rescuing a late decay zone.
- Result:
  - both strict and aggressive hybrids improve `2026` a little
  - but both push `2021` below zero
- So for very short outer frames, adding post-entry state logic is more noise than edge.

## What the Hybrid Layer Is Actually Doing
- In the better hybrids, state exits are a minority:
  - `hybrid_strict_fixed_15_10_10`
    - `2021`: `9.1%`
    - `2026`: `7.8%`
  - `hybrid_strict_fixed_15_10_12`
    - `2021`: `9.8%`
    - `2026`: `7.8%`
- That is an important practical point.
  The state logic is not replacing the fixed frame.
  It is editing only a small subset of paths, and that is enough.

## Judgment
- The earlier conclusion still holds: price-volume state logic should not be the whole exit engine.
- But this validation adds a more useful refinement:
  - price-volume state logic *can* add value when it sits inside a slower, still-positive fixed outer frame
  - the added value comes from selective acceleration, not from aggressive constant intervention
- The best practical candidate from this run is `hybrid_strict_fixed_15_10_12`.
  It is not the highest-return rule overall, but it is the most balanced state-aware hybrid:
  - both validation years positive
  - stronger `2026` protection than the fixed baseline
  - still enough cost buffer for realistic A-share trading friction
- The second best practical candidate is `hybrid_strict_fixed_15_10_10`.
  It is slightly simpler, exits a bit faster, and has the best cost cushion among the successful hybrids.

## Practical Rule Reading
- If you want a real trading rule that uses these state findings, the cleaner form is:
  1. Keep `15% / 10% / 10-12d` as the outer risk frame.
  2. Only arm state-based acceleration after the trade has already reached at least `+10%` peak profit.
  3. Use the more selective `strict` state layer, not `aggressive`.
  4. Let the state layer remove only the small minority of trades that show clear post-profit deterioration.

## Artifacts
- Summary: `/tmp/price_volume_hybrid_policy_summary_2021_2026.csv`
- Replay rows: `/tmp/price_volume_hybrid_policy_replays_2021_2026.csv`
- Hybrid vs base comparison: `/tmp/price_volume_hybrid_policy_comparison_2021_2026.csv`
- Exit reasons: `/tmp/price_volume_hybrid_policy_reasons_2021_2026.csv`
