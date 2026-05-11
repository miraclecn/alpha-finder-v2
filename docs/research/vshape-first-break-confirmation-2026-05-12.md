# V Shape First Break Confirmation Study - 2026-05-12

## Object
- Source events: `/tmp/vshape_first_break_events.csv`
- Source DB: `output/research_source.duckdb`
- Variants: `baseline_first_break`, `confirm_2d`, `confirm_3d`

## Summary
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
| variant_name | year | events | signal_days | avg_per_day |
| --- | --- | --- | --- | --- |
| baseline_first_break | 2023 | 290 | 116 | 2.5 |
| baseline_first_break | 2024 | 2491 | 151 | 16.4967 |
| baseline_first_break | 2025 | 1282 | 151 | 8.49007 |
| confirm_2d | 2023 | 88 | 63 | 1.39683 |
| confirm_2d | 2024 | 655 | 99 | 6.61616 |
| confirm_2d | 2025 | 467 | 94 | 4.96809 |
| confirm_3d | 2023 | 70 | 54 | 1.2963 |
| confirm_3d | 2024 | 530 | 90 | 5.88889 |
| confirm_3d | 2025 | 413 | 91 | 4.53846 |

## Judgment
- `confirm_2d` does not improve the 30-day distribution enough to justify sample loss: retention is 29.78% of baseline, mean/median `close_ret30` are slightly worse (`0.005807` vs `0.006144`, `-0.034557` vs `-0.025670`), and `loss10` is higher.
- `confirm_3d` does not improve the 30-day distribution enough to justify sample loss: retention is 24.96% of baseline and mean/median `close_ret30` are worse (`0.003939` vs `0.006144`, `-0.035156` vs `-0.025670`) with higher `loss10`.
- No delayed-confirmation variant should advance to portfolio-level testing in this cycle.
- Because both variants fail the risk/reward tradeoff at event level, this experiment should stop at event-level evaluation.
