# Price-Volume Exit State Study - 2026-05-12

## Object
- Analysis years: `2021, 2026`
- Selection method: in-sample regime gate + regime-specific scorer, then `Top 5 / day`.
- Exit-state purpose: find post-entry price-volume states after which continuing to hold has weak expectancy or unfavorable risk/reward.

## Selected Sample
| year | predicted_regime | selected_rows |
| --- | --- | --- |
| 2021 | trend_continuation | 410 |
| 2021 | repair_retake | 305 |
| 2021 | attention_transition | 290 |
| 2021 | clean_breakout | 165 |
| 2026 | repair_retake | 125 |
| 2026 | clean_breakout | 40 |
| 2026 | trend_continuation | 40 |

## Forward State Coverage
| step | state | rows |
| --- | --- | --- |
| 1 | neutral | 330 |
| 1 | expand_up | 327 |
| 1 | expand_down | 304 |
| 2 | neutral | 390 |
| 2 | expand_up | 310 |
| 2 | expand_down | 252 |
| 3 | neutral | 400 |
| 3 | expand_up | 275 |
| 3 | expand_down | 225 |
| 4 | neutral | 439 |
| 4 | contract_down | 246 |
| 4 | expand_up | 235 |
| 5 | neutral | 466 |
| 5 | contract_down | 264 |
| 5 | expand_up | 207 |
| 6 | neutral | 451 |
| 6 | contract_down | 296 |
| 6 | contract_flat | 191 |
| 7 | neutral | 441 |
| 7 | contract_down | 328 |
| 7 | contract_flat | 195 |
| 8 | neutral | 432 |
| 8 | contract_down | 330 |
| 8 | contract_flat | 229 |
| 9 | neutral | 413 |
| 9 | contract_down | 347 |
| 9 | contract_flat | 230 |
| 10 | neutral | 392 |
| 10 | contract_down | 329 |
| 10 | contract_flat | 253 |
| 11 | neutral | 386 |
| 11 | contract_down | 351 |
| 11 | contract_flat | 254 |
| 12 | neutral | 392 |
| 12 | contract_down | 348 |
| 12 | contract_flat | 271 |
| 13 | neutral | 386 |
| 13 | contract_down | 357 |
| 13 | contract_flat | 273 |
| 14 | neutral | 384 |
| 14 | contract_down | 358 |
| 14 | contract_flat | 269 |
| 15 | neutral | 378 |
| 15 | contract_down | 367 |
| 15 | contract_flat | 256 |
| 16 | neutral | 399 |
| 16 | contract_down | 365 |
| 16 | contract_flat | 241 |
| 17 | neutral | 395 |
| 17 | contract_down | 367 |
| 17 | contract_flat | 246 |
| 18 | neutral | 392 |
| 18 | contract_down | 364 |
| 18 | contract_flat | 250 |
| 19 | neutral | 391 |
| 19 | contract_down | 368 |
| 19 | contract_flat | 252 |
| 20 | neutral | 380 |
| 20 | contract_down | 355 |
| 20 | contract_flat | 273 |
| 21 | neutral | 386 |
| 21 | contract_down | 348 |
| 21 | contract_flat | 252 |
| 22 | neutral | 399 |
| 22 | contract_down | 343 |
| 22 | contract_flat | 256 |
| 23 | neutral | 391 |
| 23 | contract_down | 341 |
| 23 | contract_flat | 255 |
| 24 | neutral | 398 |
| 24 | contract_down | 330 |
| 24 | contract_flat | 254 |
| 25 | neutral | 387 |
| 25 | contract_down | 323 |
| 25 | contract_flat | 259 |
| 26 | neutral | 383 |
| 26 | contract_down | 335 |
| 26 | contract_flat | 255 |
| 27 | neutral | 400 |
| 27 | contract_down | 324 |
| 27 | contract_flat | 245 |
| 28 | neutral | 414 |
| 28 | contract_down | 312 |
| 28 | contract_flat | 246 |
| 29 | neutral | 401 |
| 29 | contract_down | 310 |
| 29 | contract_flat | 243 |
| 30 | neutral | 426 |
| 30 | contract_down | 288 |
| 30 | contract_flat | 255 |

## Sell Candidates
- These rows are the strongest sell-state candidates in this study because remaining close expectancy is weak and the future drawdown probability dominates the rebound probability across multiple years.
_No rows._

## Continuation States
- These rows are the opposite: after they appear, continuing to hold still tends to have positive remaining expectancy.
| state | drawdown_bucket | peak_profit_bucket | count | remaining_close_ret_mean | future_max_ret_from_today_mean | future_min_ret_from_today_mean | future_drop_5_rate | future_rebound_5_rate | future_down5_first_rate | future_up5_first_rate | negative_remaining_years | drop_gt_rebound_years | downside_first_gt_upside_years |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| contract_flat | tight | 10_20 | 21 | 0.11719 | 0.232492 | -0.0603962 | 0.428571 | 0.809524 | 0.333333 | 0.666667 | 1 | 0 | 1 |
| contract_up | tight | gt_20 | 155 | 0.0776085 | 0.378014 | -0.124882 | 0.703226 | 0.903226 | 0.245161 | 0.729032 | 0 | 0 | 0 |
| contract_flat | tight | gt_20 | 17 | 0.0743622 | 0.152533 | -0.0207144 | 0.117647 | 0.647059 | 0.117647 | 0.647059 | 0 | 0 | 0 |
| contract_up | deep_pullback | 10_20 | 870 | 0.0252391 | 0.147544 | -0.0860283 | 0.616092 | 0.683908 | 0.409195 | 0.489655 | 0 | 0 | 0 |
| expand_flat | tight | 10_20 | 23 | 0.0165538 | 0.0994395 | -0.0528585 | 0.434783 | 0.434783 | 0.26087 | 0.347826 | 1 | 0 | 1 |
| contract_up | deep_pullback | gt_20 | 1075 | 0.00588198 | 0.129947 | -0.0881051 | 0.572093 | 0.60186 | 0.376744 | 0.462326 | 0 | 0 | 0 |

## Validation Judgment
- Out-of-sample, the bad states survive better than the good states.
- The most stable sample-out-of-sample sell signals are still the “already profitable, then high-attention deterioration” states:
  - `expand_up + mild_pullback + peak_profit 10_20`
  - `expand_up + mild_pullback + peak_profit gt_20`
  - `expand_down + deep_pullback + peak_profit 10_20`
  - `expand_down + deep_pullback + peak_profit gt_20`
- These states remain negative in both `2021` and `2026`, and in most cases `down5_first` stays above `up5_first`.
- The original “first strong sell warning” from the training sample, `expand_down + mild_pullback + peak_profit 10_20`, does not pass cleanly in `2026`.
  - `2021`: remaining expectancy `-4.07%`
  - `2026`: remaining expectancy `+10.67%`
  - but `2026` only has `20` rows, so this is too thin to trust as a universal rule
- Deep pullback is much more robust than mild pullback in the validation years.
  After peak profit has already reached at least `+10%`, once the trade slips into `deep_pullback`, continuing expectancy is usually weak whether the day itself is `expand_down`, `expand_up`, or just `neutral`.
- The continuation side is more selective than in `2022-2025`.
  The training-sample “`contract_up + mild_pullback` is usually a hold” conclusion weakens out of sample:
  - `contract_up + mild_pullback + peak_profit 10_20`
    - `2021`: `-1.03%`
    - `2026`: `+1.56%`
  This is no longer a clean universal hold state.
- The continuation states that do survive sample-out-of-sample are more mature trend states:
  - `contract_up + tight + peak_profit gt_20`
  - `contract_up + deep_pullback + peak_profit 10_20`
  - `contract_up + deep_pullback + peak_profit gt_20`
  These remain non-negative in both `2021` and `2026`.
- So the safer practical reading after validation is:
  1. Do not use `mild_pullback` alone as a universal sell trigger.
  2. Treat `high-turnover deterioration after prior profit` as a real warning, especially once the pullback is already deep.
  3. `contract_up` is only a reliable hold state when the trade is already mature enough, especially after `gt_20` peak profit or when the deep-pullback state still shows better upside-first behavior than downside-first behavior.
