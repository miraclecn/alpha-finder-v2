# 2022-2025 Full Portfolio Backtest - 2026-05-12

## Object
- Run a stricter portfolio-level replay on the `2022-2025` selected names from the prior price-volume selector research.
- Policies tested:
  - `fixed_tp0.15_sl0.10_hold10`
  - `hybrid_strict_fixed_15_10_10`
  - `fixed_tp0.15_sl0.10_hold12`
  - `hybrid_strict_fixed_15_10_12`
- Years are run as independent annual books:
  - each year starts with fresh capital
  - each year ends with forced close on the last trade date close

## Why This Replay Is Stricter
- This is not the earlier single-trade close-entry study.
- This replay uses a more realistic trade book:
  - decision at day `D` close
  - entry at day `D+1` open
  - A-share `T+1`, so no same-day sell after entry
  - intraday `TP/SL` only from the first sell-eligible day onward
  - state exit triggers at close and executes at the next open
  - lot size `100`
  - base A-share cash cost model from the repo, about `24bp` round-trip
- Portfolio construction is slot-based rather than daily full rebalance:
  - `Top 5 / day` entries
  - `50` slots for `hold10`
  - `60` slots for `hold12`
  - each new trade gets one equal slot of capital

## Portfolio Results
| year | policy | total return | max drawdown | annual vol | sharpe-like | avg gross exposure | avg active positions | closed trades | trade win rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `2022` | `fixed_tp0.15_sl0.10_hold10` | `-26.48%` | `-33.85%` | `19.96%` | `-1.50` | `51.40%` | `26.23` | `1167` | `35.05%` |
| `2022` | `hybrid_strict_fixed_15_10_10` | `-25.70%` | `-32.90%` | `18.88%` | `-1.54` | `49.27%` | `25.14` | `1168` | `35.27%` |
| `2022` | `fixed_tp0.15_sl0.10_hold12` | `-23.39%` | `-30.40%` | `17.71%` | `-1.48` | `47.13%` | `28.93` | `1153` | `34.69%` |
| `2022` | `hybrid_strict_fixed_15_10_12` | `-23.21%` | `-29.83%` | `16.69%` | `-1.56` | `44.79%` | `27.46` | `1154` | `34.49%` |
| `2023` | `fixed_tp0.15_sl0.10_hold10` | `-32.02%` | `-35.60%` | `17.32%` | `-2.23` | `52.93%` | `27.05` | `1176` | `32.48%` |
| `2023` | `hybrid_strict_fixed_15_10_10` | `-30.87%` | `-33.78%` | `15.99%` | `-2.32` | `50.24%` | `25.67` | `1176` | `33.08%` |
| `2023` | `fixed_tp0.15_sl0.10_hold12` | `-27.67%` | `-31.80%` | `15.31%` | `-2.13` | `49.30%` | `30.33` | `1176` | `32.82%` |
| `2023` | `hybrid_strict_fixed_15_10_12` | `-26.14%` | `-29.31%` | `13.93%` | `-2.19` | `46.49%` | `28.60` | `1176` | `33.84%` |
| `2024` | `fixed_tp0.15_sl0.10_hold10` | `-40.81%` | `-48.49%` | `20.92%` | `-2.50` | `42.91%` | `21.94` | `1185` | `31.98%` |
| `2024` | `hybrid_strict_fixed_15_10_10` | `-37.64%` | `-46.46%` | `19.91%` | `-2.37` | `40.78%` | `20.83` | `1185` | `32.57%` |
| `2024` | `fixed_tp0.15_sl0.10_hold12` | `-37.57%` | `-44.63%` | `18.24%` | `-2.60` | `38.59%` | `23.74` | `1185` | `31.65%` |
| `2024` | `hybrid_strict_fixed_15_10_12` | `-34.97%` | `-42.93%` | `17.33%` | `-2.50` | `36.40%` | `22.39` | `1185` | `31.90%` |
| `2025` | `fixed_tp0.15_sl0.10_hold10` | `-7.99%` | `-26.45%` | `18.20%` | `-0.38` | `49.03%` | `25.06` | `1209` | `41.11%` |
| `2025` | `hybrid_strict_fixed_15_10_10` | `-10.82%` | `-26.45%` | `16.78%` | `-0.62` | `46.34%` | `23.70` | `1209` | `40.61%` |
| `2025` | `fixed_tp0.15_sl0.10_hold12` | `-6.43%` | `-23.32%` | `15.88%` | `-0.35` | `45.13%` | `27.88` | `1209` | `41.11%` |
| `2025` | `hybrid_strict_fixed_15_10_12` | `-7.53%` | `-22.33%` | `14.64%` | `-0.48` | `42.47%` | `26.19` | `1209` | `41.44%` |

## Immediate Reading
- All four portfolio variants are negative in every year from `2022` to `2025`.
- `2025` is the least bad year.
- `2024` is the worst year.
- The `hold12` outer frame is consistently less bad than `hold10`.
- The `strict` hybrid layer helps in `2022-2024`, but not enough to flip the sign.
- In `2025`, the `strict` hybrid layer slightly hurts total return versus the plain fixed baseline.

## Hybrid vs Base
- `hybrid_strict_fixed_15_10_10` versus `fixed_tp0.15_sl0.10_hold10`:
  - `2022`: improves by about `+0.79` percentage points
  - `2023`: improves by about `+1.15` points
  - `2024`: improves by about `+3.18` points
  - `2025`: worsens by about `-2.83` points
- `hybrid_strict_fixed_15_10_12` versus `fixed_tp0.15_sl0.10_hold12`:
  - `2022`: improves by about `+0.17` points
  - `2023`: improves by about `+1.54` points
  - `2024`: improves by about `+2.60` points
  - `2025`: worsens by about `-1.10` points

## Sanity Check: This Is Not A Portfolio-Only Problem
- I also re-ran the older single-trade replay on the same `2022-2025` selected sample, still using these same exit rules.
- Result: even the trade-level close-entry replay is negative in all four years.
- So the bad result here is not caused mainly by slot sizing or capital overlap.
- The larger conclusion is simpler:
  - this selector can produce useful out-of-sample validation structure for `2021/2026`
  - but inside `2022-2025`, these particular fixed and hybrid exit rules are not enough to turn the selected stream into a positive annualized trade book

## What Seems To Be True
- The `strict` price-volume deterioration layer still has information.
  It usually reduces loss and drawdown in the harder years.
- But the information is not strong enough to rescue the base strategy by itself.
- The core issue is upstream:
  - the `Top 5 / day` selected stream under this execution timing and these exit frames is too weak
  - especially once realistic `next open` entry and `T+1` are enforced

## Practical Implication
- There is no evidence here that `hybrid_strict_fixed_15_10_10` or `hybrid_strict_fixed_15_10_12` is ready for live deployment on the `2022-2025` selected stream.
- If we continue, the next scientific step is not “polish exits a bit more”.
- The next step should be one of:
  1. tighten entry quality before testing exits again
  2. lower daily signal count below `Top 5 / day`
  3. add stronger pre-entry filters for liquidity / trend maturity / exhaustion risk
  4. re-test whether the state layer works better on a stricter entry sleeve than on this broader selected basket

## Artifacts
- Portfolio summary: `/tmp/price_volume_slot_portfolio_backtest_2022_2025_summary.csv`
- Daily curve: `/tmp/price_volume_slot_portfolio_backtest_2022_2025_daily_curve.csv`
- Trade log: `/tmp/price_volume_slot_portfolio_backtest_2022_2025_trades.csv`
- JSON summary: `/tmp/price_volume_slot_portfolio_backtest_2022_2025_summary.json`
