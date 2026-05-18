# Price-Volume Exit State Study - 2026-05-12

## Object
- Analysis years: `2022, 2023, 2024, 2025`
- Selection method: in-sample regime gate + regime-specific scorer, then `Top 5 / day`.
- Exit-state purpose: find post-entry price-volume states after which continuing to hold has weak expectancy or unfavorable risk/reward.

## Selected Sample
| year | predicted_regime | selected_rows |
| --- | --- | --- |
| 2022 | attention_transition | 595 |
| 2022 | repair_retake | 385 |
| 2022 | trend_continuation | 185 |
| 2023 | clean_breakout | 925 |
| 2023 | trend_continuation | 160 |
| 2023 | repair_retake | 75 |
| 2023 | attention_transition | 50 |
| 2024 | repair_retake | 555 |
| 2024 | clean_breakout | 355 |
| 2024 | attention_transition | 235 |
| 2024 | trend_continuation | 65 |
| 2025 | trend_continuation | 560 |
| 2025 | clean_breakout | 285 |
| 2025 | attention_transition | 190 |
| 2025 | repair_retake | 180 |

## Forward State Coverage
| step | state | rows |
| --- | --- | --- |
| 1 | expand_down | 1215 |
| 1 | expand_up | 1097 |
| 1 | neutral | 1012 |
| 2 | neutral | 1175 |
| 2 | expand_down | 1004 |
| 2 | expand_up | 976 |
| 3 | neutral | 1315 |
| 3 | expand_up | 858 |
| 3 | contract_down | 792 |
| 4 | neutral | 1379 |
| 4 | contract_down | 946 |
| 4 | expand_up | 731 |
| 5 | neutral | 1442 |
| 5 | contract_down | 1066 |
| 5 | contract_flat | 656 |
| 6 | neutral | 1421 |
| 6 | contract_down | 1165 |
| 6 | contract_flat | 778 |
| 7 | neutral | 1378 |
| 7 | contract_down | 1233 |
| 7 | contract_flat | 822 |
| 8 | neutral | 1306 |
| 8 | contract_down | 1303 |
| 8 | contract_flat | 894 |
| 9 | contract_down | 1353 |
| 9 | neutral | 1229 |
| 9 | contract_flat | 941 |
| 10 | contract_down | 1349 |
| 10 | neutral | 1184 |
| 10 | contract_flat | 1010 |
| 11 | contract_down | 1379 |
| 11 | neutral | 1136 |
| 11 | contract_flat | 1036 |
| 12 | contract_down | 1408 |
| 12 | neutral | 1119 |
| 12 | contract_flat | 1059 |
| 13 | contract_down | 1418 |
| 13 | neutral | 1091 |
| 13 | contract_flat | 1071 |
| 14 | contract_down | 1371 |
| 14 | contract_flat | 1094 |
| 14 | neutral | 1083 |
| 15 | contract_down | 1336 |
| 15 | neutral | 1108 |
| 15 | contract_flat | 1070 |
| 16 | contract_down | 1314 |
| 16 | neutral | 1142 |
| 16 | contract_flat | 1090 |
| 17 | contract_down | 1288 |
| 17 | neutral | 1181 |
| 17 | contract_flat | 1072 |
| 18 | contract_down | 1288 |
| 18 | neutral | 1185 |
| 18 | contract_flat | 1059 |
| 19 | contract_down | 1250 |
| 19 | neutral | 1207 |
| 19 | contract_flat | 1049 |
| 20 | neutral | 1227 |
| 20 | contract_down | 1202 |
| 20 | contract_flat | 1047 |
| 21 | neutral | 1267 |
| 21 | contract_down | 1171 |
| 21 | contract_flat | 1037 |
| 22 | neutral | 1264 |
| 22 | contract_down | 1132 |
| 22 | contract_flat | 1040 |
| 23 | neutral | 1251 |
| 23 | contract_down | 1073 |
| 23 | contract_flat | 1026 |
| 24 | neutral | 1260 |
| 24 | contract_down | 1040 |
| 24 | contract_flat | 1013 |
| 25 | neutral | 1292 |
| 25 | contract_down | 1038 |
| 25 | contract_flat | 980 |
| 26 | neutral | 1320 |
| 26 | contract_down | 1021 |
| 26 | contract_flat | 963 |
| 27 | neutral | 1331 |
| 27 | contract_down | 1011 |
| 27 | contract_flat | 938 |
| 28 | neutral | 1351 |
| 28 | contract_down | 1005 |
| 28 | contract_flat | 940 |
| 29 | neutral | 1392 |
| 29 | contract_down | 998 |
| 29 | contract_flat | 914 |
| 30 | neutral | 1403 |
| 30 | contract_down | 949 |
| 30 | contract_flat | 926 |

## Sell Candidates
- These rows are the strongest sell-state candidates in this study because remaining close expectancy is weak and the future drawdown probability dominates the rebound probability across multiple years.
| state | drawdown_bucket | peak_profit_bucket | count | remaining_close_ret_mean | future_max_ret_from_today_mean | future_min_ret_from_today_mean | future_drop_5_rate | future_rebound_5_rate | future_down5_first_rate | future_up5_first_rate | negative_remaining_years | drop_gt_rebound_years | downside_first_gt_upside_years |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| expand_down | mild_pullback | 10_20 | 548 | -0.0640103 | 0.0932915 | -0.136557 | 0.760949 | 0.472628 | 0.565693 | 0.346715 | 4 | 4 | 4 |
| expand_up | mild_pullback | 10_20 | 1418 | -0.0503412 | 0.138987 | -0.146961 | 0.814528 | 0.631876 | 0.577574 | 0.363893 | 4 | 4 | 4 |
| expand_up | deep_pullback | gt_20 | 1323 | -0.0500391 | 0.110228 | -0.13516 | 0.708239 | 0.587302 | 0.535147 | 0.356765 | 4 | 4 | 4 |
| expand_down | deep_pullback | gt_20 | 2480 | -0.0468487 | 0.138842 | -0.151316 | 0.793952 | 0.569355 | 0.608468 | 0.315323 | 4 | 4 | 4 |
| expand_down | deep_pullback | 10_20 | 1264 | -0.0303634 | 0.141042 | -0.134523 | 0.731804 | 0.568038 | 0.551424 | 0.340981 | 4 | 4 | 4 |
| expand_up | deep_pullback | 10_20 | 875 | -0.0283326 | 0.102841 | -0.0995634 | 0.622857 | 0.524571 | 0.482286 | 0.326857 | 4 | 4 | 4 |
| neutral | deep_pullback | gt_20 | 6580 | -0.0256093 | 0.127016 | -0.121645 | 0.701824 | 0.565805 | 0.521733 | 0.357295 | 4 | 4 | 4 |
| neutral | deep_pullback | 10_20 | 5119 | -0.0152215 | 0.123394 | -0.102251 | 0.622583 | 0.583708 | 0.443055 | 0.404181 | 4 | 2 | 4 |
| expand_up | tight | 10_20 | 1617 | -0.0492307 | 0.176398 | -0.167529 | 0.846011 | 0.705009 | 0.555349 | 0.416821 | 4 | 4 | 3 |
| expand_down | mild_pullback | gt_20 | 419 | -0.0207814 | 0.142813 | -0.142637 | 0.789976 | 0.558473 | 0.630072 | 0.291169 | 3 | 4 | 4 |
| expand_flat | deep_pullback | gt_20 | 386 | -0.0447323 | 0.1347 | -0.135516 | 0.748705 | 0.57513 | 0.53886 | 0.349741 | 3 | 3 | 4 |
| expand_flat | mild_pullback | 10_20 | 335 | -0.0224883 | 0.125806 | -0.129889 | 0.761194 | 0.513433 | 0.591045 | 0.304478 | 3 | 3 | 4 |
| expand_flat | mild_pullback | gt_20 | 229 | -0.0367766 | 0.132752 | -0.152681 | 0.820961 | 0.58952 | 0.60262 | 0.31441 | 3 | 4 | 3 |
| expand_up | tight | gt_20 | 1945 | -0.0305876 | 0.212296 | -0.166639 | 0.791774 | 0.720308 | 0.503342 | 0.455527 | 3 | 3 | 3 |
| contract_flat | deep_pullback | 10_20 | 4307 | -0.00465934 | 0.0917833 | -0.0749376 | 0.534247 | 0.497562 | 0.403993 | 0.373114 | 3 | 2 | 3 |

## Continuation States
- These rows are the opposite: after they appear, continuing to hold still tends to have positive remaining expectancy.
| state | drawdown_bucket | peak_profit_bucket | count | remaining_close_ret_mean | future_max_ret_from_today_mean | future_min_ret_from_today_mean | future_drop_5_rate | future_rebound_5_rate | future_down5_first_rate | future_up5_first_rate | negative_remaining_years | drop_gt_rebound_years | downside_first_gt_upside_years |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| contract_up | mild_pullback | gt_20 | 217 | 0.0600355 | 0.244149 | -0.0807202 | 0.40553 | 0.834101 | 0.207373 | 0.718894 | 0 | 0 | 0 |
| contract_up | mild_pullback | 10_20 | 329 | 0.0416489 | 0.171903 | -0.0725626 | 0.504559 | 0.699088 | 0.285714 | 0.607903 | 0 | 1 | 0 |
| contract_flat | mild_pullback | 10_20 | 463 | 0.0143039 | 0.125071 | -0.0737747 | 0.507559 | 0.557235 | 0.38013 | 0.475162 | 1 | 2 | 2 |
| contract_up | deep_pullback | gt_20 | 2656 | 0.00662749 | 0.124517 | -0.0851423 | 0.560241 | 0.53012 | 0.395331 | 0.425828 | 1 | 2 | 1 |
| contract_up | deep_pullback | 10_20 | 2824 | 0.00135468 | 0.115737 | -0.0804861 | 0.552054 | 0.578966 | 0.382082 | 0.461048 | 1 | 1 | 1 |

## Judgment
- The strongest general sell pattern is not “price falls, so sell”, but “the trade has already floated at least `+10%`, and then a high-turnover down day appears while price has already pulled back from the peak”.
- The cleanest version is `expand_down + mild_pullback + peak_profit 10_20`:
  - sample count `548`
  - overall remaining close expectancy `-6.40%`
  - `down5_first` rate `56.6%` vs `up5_first` rate `34.7%`
  - all `2022-2025` years show negative remaining expectancy
- Once the pullback deepens to `>8%` from the running peak, even “noisy but not crashing” states become weak:
  - `expand_down + deep_pullback + peak_profit 10_20`
  - `neutral + deep_pullback + peak_profit 10_20`
  - `contract_flat + deep_pullback + peak_profit 10_20`
  These are not good “wait for a calm rebound” states in this sample.
- A second important result is that late-stage `expand_up` is often not a continuation confirmation anymore.
  After a trade has already reached `10%-20%` floating peak profit, `expand_up` with tight or mild pullback still has negative remaining close expectancy across all four years.
  This behaves more like exhaustion / final push than a fresh add-hold signal.
- The strongest continuation state is `contract_up + mild_pullback` after the trade has already worked:
  - `peak_profit 10_20`: remaining expectancy `+4.16%`
  - `peak_profit gt_20`: remaining expectancy `+6.00%`
  - `up5_first` clearly exceeds `down5_first`
  This means “light pullback, then price continues up on shrinking turnover” is often a hold state, not a sell state.
- So the practical sell logic suggested by this study is state-based:
  1. First ask whether the trade has already proven itself by reaching at least `+10%` peak profit.
  2. If yes, treat `expand_down` after `3%-8%` pullback as the first strong sell warning.
  3. If the pullback has already deepened beyond `8%`, do not assume shrinking volume means safety; deep pullback itself is already a weak state.
  4. Do not sell just because volume shrinks. If price still edges higher (`contract_up`) and the pullback stays mild, that is often the better hold state.
