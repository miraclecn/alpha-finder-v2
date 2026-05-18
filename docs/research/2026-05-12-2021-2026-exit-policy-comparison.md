# 2021 / 2026 Exit Policy Comparison - 2026-05-12

## Object
- Compare post-entry exit policies on the already-selected names from the prior price-volume selector studies.
- Validation years: `2021` and `2026`.
- Important date boundary: the `2026` sample only uses currently available rows through `2026-04-30`.
- Sample sizes are asymmetric:
  - `2021`: `1215` trades
  - `2026`: `205` trades
- Because of that asymmetry, pooled average return is misleading. Robustness is judged year by year first, then by whether both validation years stay positive.

## Cost Assumption
- Replay cost for this comparison: `30bp` round-trip.
- This sits between the repo's two existing A-share cash models:
  - `config/cost_models/base_a_share_cash.toml`: about `24bp`
  - `config/cost_models/high_a_share_cash.toml`: about `36bp`

## Policies Compared
- State-only:
  - `strict`
  - `aggressive`
- State plus time cap:
  - `strict_hold15`
  - `strict_hold18`
- Fixed baselines:
  - `fixed_tp0.15_sl0.10_hold10`
  - `fixed_tp0.15_sl0.10_hold12`
  - `fixed_tp0.20_sl0.10_hold5`

## Validation Summary
| policy | 2021 mean net | 2026 mean net | 2021 win rate | 2026 win rate | avg exit step | both years positive | break-even cost |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `strict` | `+1.045%` | `-1.437%` | `52.8%` | `48.3%` | `17.72` | `No` | `-113.7bp` |
| `aggressive` | `+0.949%` | `-2.178%` | `56.4%` | `50.7%` | `17.06` | `No` | `-187.8bp` |
| `strict_hold15` | `+0.041%` | `+0.165%` | `48.7%` | `49.8%` | `11.11` | `Yes` | `34.1bp` |
| `strict_hold18` | `+0.171%` | `+0.002%` | `49.9%` | `50.2%` | `12.63` | `Yes` | `30.2bp` |
| `fixed_tp0.15_sl0.10_hold10` | `+0.253%` | `+0.072%` | `43.5%` | `41.5%` | `5.79` | `Yes` | `37.2bp` |
| `fixed_tp0.15_sl0.10_hold12` | `+0.193%` | `+0.406%` | `43.0%` | `43.9%` | `6.27` | `Yes` | `49.3bp` |
| `fixed_tp0.20_sl0.10_hold5` | `+0.154%` | `+1.008%` | `43.1%` | `46.8%` | `4.12` | `Yes` | `45.4bp` |

## What Changed After Adding a Time Cap
- The state-only rules were not failing because they never fired.
  They did fire on most trades, but they still exited too late:
  - `strict`: `65.3%` state exits in `2021`, `59.0%` in `2026`
  - `aggressive`: `66.0%` state exits in `2021`, `59.0%` in `2026`
- The problem is timing, not just recognition.
  Average exit step stays around `17` days, which leaves too much room for giveback after the trend has already matured.
- Adding a time cap materially changes the engine:
  - `strict_hold15`: state exits are only `48.1%` in `2021` and `43.9%` in `2026`; the other half comes from the time cap
  - `strict_hold18`: state exits are `53.1%` in `2021` and `48.8%` in `2026`
- So the time cap is not a cosmetic add-on.
  It is doing real rescue work that the state-only rules were not doing by themselves.

## Interpretation
- `strict` and `aggressive` are not deployable as standalone exit engines.
  They look good in `2021`, but both are already negative in `2026` before any realistic slippage upgrade.
- The key practical problem is that state-only exits have decent hit rates but poor path control.
  Even with roughly `48%-56%` positive-win-rate, they let too many trades stay alive into the later decay zone.
- `strict_hold15` and `strict_hold18` do recover sample-out-of-sample robustness.
  Both stay positive in `2021` and `2026`, but the edge is thin:
  - `strict_hold15` only has about `34.1bp` of cost budget
  - `strict_hold18` only has about `30.2bp`
- That means they survive the repo's `24bp` base cost model, but they do not survive the `36bp` high-cost model.
  So they are more plausible as a research overlay than as a robust standalone production rule.
- The fixed baselines are still stronger out of sample.
  In this comparison they have:
  - shorter holding time
  - larger cost buffer
  - better worst-year return
- The most balanced fixed rule here is `fixed_tp0.15_sl0.10_hold12`.
  It keeps both validation years positive and still has about `49.3bp` of cost budget.
- `fixed_tp0.20_sl0.10_hold5` is the strongest on average net return, but it is more front-loaded and clearly a faster-turn rule.

## Judgment
- The post-entry price-volume state logic is still useful, but not as a complete exit policy by itself.
- The evidence from `2021` and `2026-04-30` says:
  1. Pure state-based exits are too slow and unstable.
  2. State-based exits become more credible only after adding a separate time governor.
  3. Even after adding that governor, current state-based policies still trail the better fixed baselines on cost tolerance and worst-year stability.
- The cleaner practical reading is:
  - treat price-volume deterioration states as a sell accelerator or warning layer
  - do not let them be the only exit mechanism
  - pair them with an explicit holding-horizon constraint

## Practical Implication
- If the real objective is “sell based on state, not just hard TP/SL”, the next useful direction is not to remove hard structure entirely.
- The next useful direction is to test a hybrid:
  - fixed outer risk frame for time / cost control
  - state-based acceleration when the trade has already reached at least `+10%` peak profit and then falls into `expand_up` or `expand_down` with meaningful pullback
- That preserves the informative part of the state research while avoiding the main failure mode found here: exiting too late.

## Artifacts
- Summary: `/tmp/price_volume_exit_policy_summary_2021_2026.csv`
- Rollup: `/tmp/price_volume_exit_policy_rollup_2021_2026.csv`
- Exit reasons: `/tmp/price_volume_exit_policy_reasons_2021_2026.csv`
- Replay rows: `/tmp/price_volume_exit_policy_replays_2021_2026.csv`
