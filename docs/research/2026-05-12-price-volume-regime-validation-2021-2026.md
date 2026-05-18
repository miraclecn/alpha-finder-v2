# Price-Volume Regime Validation Study - 2026-05-12

## Object
- Source DB: `output/research_source.duckdb`
- Training years: `2022, 2023, 2024, 2025`
- Validation years: `2021, 2026`
- Top N per day: `5`

## Validation Summary
- This is an event-level replay / ranking validation, not a broker-grade portfolio simulator.
| year | candidate_rows | candidate_event_rate | candidate_mean_close_ret30 | selected_rows | selected_signal_days | selected_event_rate | selected_mean_close_ret30 | selected_mean_max_ret30 | selected_mean_min_ret30 | predicted_regime_majority | predicted_regime_majority_share |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2021 | 659679 | 0.223559 | 0.0294037 | 1215 | 243 | 0.301235 | 0.000863488 | 0.246739 | -0.161501 | trend_continuation | 0.350427 |
| 2026 | 116079 | 0.172486 | -0.013599 | 205 | 41 | 0.341463 | -0.0506493 | 0.207152 | -0.181317 | repair_retake | 0.609756 |

## Predicted Regime Days
| year | predicted_regime | days |
| --- | --- | --- |
| 2021 | trend_continuation | 82 |
| 2021 | repair_retake | 61 |
| 2021 | attention_transition | 58 |
| 2021 | clean_breakout | 33 |
| 2026 | repair_retake | 25 |
| 2026 | clean_breakout | 8 |
| 2026 | trend_continuation | 8 |

## Top-5 First-Hit Cycle Study
- Scope: selected `Top 5 / day` rows only, because this is the closest sample to a tradable daily ranking output.
- Unit: trading days after entry day.
- Window cap: the longest observable cycle is `30` trading days because the label window itself is `30` days.
- `up20_any` / `loss10_any` means the level was touched at least once inside 30 days.
- `up20_first` / `loss10_first` means the level was the path-first touch under the study rule.

| year | metric | count | min | p25 | median | p75 | max | mean |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2021 | up20_any | 501 | 2 | 4 | 8 | 16 | 30 | 10.86 |
| 2021 | up20_first | 364 | 2 | 4 | 6 | 11 | 30 | 8.53 |
| 2021 | loss10_any | 826 | 1 | 3 | 7 | 13 | 30 | 9.10 |
| 2021 | loss10_first | 709 | 1 | 3 | 5 | 10 | 30 | 7.56 |
| 2026 | up20_any | 83 | 2 | 3 | 6 | 13 | 28 | 9.01 |
| 2026 | up20_first | 76 | 2 | 3 | 6 | 13 | 28 | 8.80 |
| 2026 | loss10_any | 148 | 1 | 4 | 9 | 15.25 | 30 | 10.57 |
| 2026 | loss10_first | 119 | 1 | 3 | 7 | 14 | 30 | 9.18 |

## Top-5 Cycle Buckets
- The key practical question is not just the min / max day, but whether the move usually resolves quickly enough to support a rules-based holding period.

| year | path-first bucket | 1-5d | 6-10d | 11-15d | 16-20d | 21-30d |
| --- | --- | --- | --- | --- | --- | --- |
| 2021 | up20_first | 164 | 101 | 43 | 23 | 34 |
| 2021 | loss10_first | 356 | 183 | 83 | 49 | 38 |
| 2026 | up20_first | 36 | 14 | 14 | 6 | 6 |
| 2026 | loss10_first | 47 | 32 | 19 | 11 | 10 |

## Reversal Gap
- `after up20 then loss10 gap` = rows that first touched `+20%` and still later touched `-10%` inside the same 30-day window; the number is the gap between those two touch days.
- `after loss10 then up20 gap` = rows that first touched `-10%` and later still managed to touch `+20%`.

| year | scenario | count | median gap | p75 gap | max gap |
| --- | --- | --- | --- | --- | --- |
| 2021 | after up20 then loss10 gap | 116 | 13.5 | 18 | 27 |
| 2021 | after loss10 then up20 gap | 135 | 10 | 15 | 27 |
| 2026 | after up20 then loss10 gap | 29 | 11 | 13 | 24 |
| 2026 | after loss10 then up20 gap | 7 | 6 | 9 | 13 |

## Practical Exit Rule Scan
- Matching assumption: daily-bar path replay, long-only, and conservative same-day conflict handling. If the same bar touches both take-profit and stop-loss, treat stop-loss as filled first.
- Scope: still only the selected `Top 5 / day` sample for `2021` and `2026`.
- Goal: find rules that keep both years positive while avoiding a very low win rate.

| rule | year | mean_ret | win_rate_pos | non_loss_rate | avg_hold | p10_ret |
| --- | --- | --- | --- | --- | --- | --- |
| baseline_hold30 | 2021 | 0.000863 | 0.367078 | 0.37037 | 30 | -0.222341 |
| baseline_hold30 | 2026 | -0.0506493 | 0.273171 | 0.273171 | 30 | -0.268187 |
| fixed_15_10_10 | 2021 | 0.005652 | 0.445267 | 0.451852 | 5.92593 | -0.1 |
| fixed_15_10_10 | 2026 | 0.011677 | 0.453659 | 0.453659 | 6.06829 | -0.1 |
| fixed_15_10_12 | 2021 | 0.005496 | 0.438683 | 0.441975 | 6.42469 | -0.1 |
| fixed_15_10_12 | 2026 | 0.015448 | 0.478049 | 0.487805 | 6.66341 | -0.1 |
| be_15_10_10_arm12 | 2021 | 0.006086 | 0.425514 | 0.479012 | 5.79918 | -0.1 |
| be_15_10_10_arm12 | 2026 | 0.013313 | 0.419512 | 0.502439 | 5.83902 | -0.1 |
| stag_15_08_12_chk5_gate3 | 2021 | 0.005488 | 0.393416 | 0.396708 | 5.10864 | -0.08 |
| stag_15_08_12_chk5_gate3 | 2026 | 0.015173 | 0.429268 | 0.439024 | 5.23902 | -0.08 |
| fixed_20_10_5 | 2021 | 0.004637 | 0.445267 | 0.44856 | 4.17613 | -0.1 |
| fixed_20_10_5 | 2026 | 0.020529 | 0.502439 | 0.507317 | 4.16585 | -0.1 |

## Candidate Reading
- `fixed_15_10_10`: the most balanced simple rule in this scan. Both years stay positive, positive-win-rate is around `45%`, and average holding time compresses to about `6` trading days.
- `fixed_15_10_12`: slightly weaker in `2021` but stronger in `2026`; this is the “let strong names breathe a bit longer” version.
- `be_15_10_10_arm12`: after the trade first reaches `+12%`, raise stop to break-even from the next day onward. This improves robustness against giveback and lifts the non-loss rate to roughly `48%-50%`, although strict positive-win-rate drops a bit because some winners become flat exits.
- `fixed_20_10_5`: attractive if the priority is shorter holding time and not letting positions linger. The trade-off is thinner `2021` edge; this rule has less room to absorb real friction.

## Cost Budget
- `fixed_15_10_10`: both validation years stay positive only if all-in round-trip friction is below about `56.5bp`.
- `fixed_15_10_12`: cost budget about `55.0bp`.
- `be_15_10_10_arm12`: cost budget about `60.9bp`, the best among the leading candidates here.
- `fixed_20_10_5`: cost budget about `46.4bp`, so it is more sensitive to slippage and execution quality.

## Judgment
- Use the classifier as a regime gate first, then score entries with the regime-specific evaluator.
- Treat the ranking summary as signal validation evidence, not deployable portfolio evidence.
- For the selected `Top 5 / day` basket, both winners and losers are front-loaded: most clean `up20_first` cases finish in the first `10` trading days, but clean `loss10_first` cases are even earlier.
- A practical implication is that a fixed `30`-day hold is too blunt for this selector. If the trade has not resolved by about day `10-15`, edge decays quickly and reversal risk rises.
- The giveback problem is real: a meaningful subset of names that first hit `+20%` still later touch `-10%` within the same `30`-day window, usually about `11-14` trading days after the first `+20%` touch.
- If the objective is “positive expectation, and win rate not too low,” the first practical candidate is `15%` take-profit, `10%` hard stop, `10` trading day max hold.
- If the objective is “positive expectation, and avoid turning many floated winners into losers,” the better candidate is `15%` take-profit, `10%` hard stop, `10` trading day max hold, then move stop to break-even after first reaching `+12%`.
