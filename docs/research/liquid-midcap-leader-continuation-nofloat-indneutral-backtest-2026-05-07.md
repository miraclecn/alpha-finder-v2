# Liquid Midcap Leader Continuation No-Float Ind-Neutral Backtest - 2026-05-07

## What Changed

This run reuses the strict restored strategy core and changes only the float
market-cap gate:

- removed `min_float_mcap_cny_bn = 5`
- removed `max_float_mcap_cny_bn = 30`

The new sleeve also declares:

- `neutralization = ["industry"]`

Important implementation note:

The prior strict run was already effectively industry-neutralized at scoring
time because the descriptor set contained only `weighted_momentum_quality`, and
the trend input builder applies `group_neutral_zscore_map(...)` when industry
labels exist but `industry_relative_strength` is absent. So the economic delta
in this run is primarily the removal of the float-market-cap filter, not the
addition of a new live neutralization engine.

## Window

- executed backtest window: `2020-01-02` to `2026-04-28`
- local green-data limit remains `2026-04-28`

## Build Artifacts

- research input: `output/liquid_midcap_leader_continuation_nofloat_indneutral_v1_input.json`
- sleeve artifact: `output/liquid_midcap_leader_continuation_nofloat_indneutral_v1_artifact.json`
- backtest: `output/liquid_midcap_leader_continuation_nofloat_indneutral_v1_daily_backtest.json`

Research input coverage:

- rebalance steps: `151`
- research records: `3,621`
- first trade date: `2020-01-02`
- last rebalance date: `2026-03-17`

## Backtest Summary

| Metric | Strict Base | No-Float + Ind-Neutral |
| --- | ---: | ---: |
| Final equity | CNY `8,870,123` | CNY `7,670,011` |
| Total return | `-11.30%` | `-23.30%` |
| CAGR | `-1.88%` | `-4.11%` |
| Annualized return | `1.83%` | `-0.15%` |
| Benchmark annualized return | `7.59%` | `7.59%` |
| Active annualized return | `-5.76%` | `-7.74%` |
| Information ratio | `-0.290` | `-0.356` |
| Annualized volatility | `27.57%` | `29.08%` |
| Sharpe | `0.066` | `-0.005` |
| Max drawdown | `-68.46%` | `-72.06%` |
| Beta | `1.00` | `1.02` |
| Turnover | `71.67x` | `72.74x` |
| Total costs | CNY `1,742,534` | CNY `1,531,042` |
| Blocked trade share | `20.98%` | `18.90%` |

Additional execution readout for the new variant:

- orders: `9,710`
- fills: `7,875`
- blocked orders: `1,835`
- average gross exposure: `96.08%`
- average cash: CNY `339,430`

Yearly returns for the new variant:

| Year | Return | Max Drawdown |
| --- | ---: | ---: |
| 2020 | `20.93%` | `-23.91%` |
| 2021 | `6.19%` | `-22.62%` |
| 2022 | `-36.68%` | `-38.59%` |
| 2023 | `-20.33%` | `-26.43%` |
| 2024 | `-26.55%` | `-40.09%` |
| 2025 | `46.60%` | `-19.58%` |
| 2026 YTD | `5.81%` | `-18.39%` |

## Judgment

Removing the float-market-cap filter makes the restored strategy worse across
nearly every economically important dimension:

- lower final equity
- deeper drawdown
- weaker active return
- worse information ratio
- higher volatility
- slightly higher turnover

The lower recorded total costs do not change the conclusion. Costs fell because
some trades were blocked less often and the book held less cash drag from the
mid-cap gate, but the underlying alpha quality deteriorated more than the cost
saving helped.

## Practical Conclusion

For this restored strategy family inside the current V2 stack:

- `industry-neutral scoring` is not the problem
- `removing the float-market-cap filter` is harmful

So if the goal is to salvage this sleeve, the float-market-cap filter should be
kept rather than removed.
