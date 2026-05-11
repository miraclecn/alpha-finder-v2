# V Shape First Break Confirmation Study - 2026-05-12

## Inputs
- events_csv: `/tmp/vshape_first_break_events.csv`
- source_db: `output/research_source.duckdb`

## Summary
- Year basis: `signal_date` (event-origin year).
| variant_name | year | candidate_rows | events | confirmation_pass_rate |
| --- | --- | --- | --- | --- |
| baseline_first_break | 2023 | 290 | 290 | 1 |
| baseline_first_break | 2024 | 2491 | 2491 | 1 |
| baseline_first_break | 2025 | 1282 | 1282 | 1 |
| confirm_2d | 2023 | 290 | 88 | 0.303448 |
| confirm_2d | 2024 | 2491 | 655 | 0.262947 |
| confirm_2d | 2025 | 1282 | 467 | 0.364275 |
| confirm_3d | 2023 | 290 | 70 | 0.241379 |
| confirm_3d | 2024 | 2491 | 530 | 0.212766 |
| confirm_3d | 2025 | 1282 | 413 | 0.322153 |

## Signal Density
- Year/day basis: `candidate_entry_date` (actual candidate entry timing).
- `signal_days` counts unique candidate entry dates among passed rows only.
| variant_name | year | events | signal_days | avg_per_day |
| --- | --- | --- | --- | --- |
| baseline_first_break | 2023 | 289 | 115 | 2.51304 |
| baseline_first_break | 2024 | 2490 | 151 | 16.4901 |
| baseline_first_break | 2025 | 1284 | 153 | 8.39216 |
| confirm_2d | 2023 | 85 | 61 | 1.39344 |
| confirm_2d | 2024 | 656 | 99 | 6.62626 |
| confirm_2d | 2025 | 469 | 97 | 4.83505 |
| confirm_3d | 2023 | 67 | 52 | 1.28846 |
| confirm_3d | 2024 | 532 | 91 | 5.84615 |
| confirm_3d | 2025 | 414 | 92 | 4.5 |
| confirm_3d | 2026 | 0 | 0 | 0 |

## Judgment
- `confirm_2d` still does not justify promotion: retained sample is `1210 / 4063 = 29.78%` of baseline, `up10` improves (`62.89%` vs `58.75%`), but 30-day terminal and downside profile are weaker (`mean close_ret30 0.005807` vs `0.006144`, median `-0.034557` vs `-0.025670`, `loss10 51.24%` vs `50.60%`).
- `confirm_3d` is weaker than `confirm_2d`: retained sample is `24.93%` of baseline with lower terminal return (`mean close_ret30 0.003939`, median `-0.035156`) and higher downside hit rate (`loss10 53.50%`).
- Entry-date density correction changes calendar allocation (for example `confirm_3d` now has a `2026` density bucket with `0` passed events), but the corrected density still shows material event loss without compensating distribution improvement.
- Decision for this cycle remains unchanged: keep delayed-confirmation variants at event-level research only and do not advance either variant to portfolio-level testing.
