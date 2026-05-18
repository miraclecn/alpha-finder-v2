# Sector Gated Leader Pullback Backtest - 2026-05-07

## Object

- Thesis: `config/theses/sector_gated_leader_pullback.toml`
- Descriptor set: `config/descriptor_sets/sector_gated_leader_pullback_v1.toml`
- Sleeve: `config/sleeves/sector_gated_leader_pullback_v1.toml`
- Trend input case: `research/examples/trend_input_build_minimal/sector_gated_leader_pullback_v1.toml`
- Backtest case: `research/examples/deployment_minimal/sector_gated_leader_pullback_v1_portfolio_backtest.toml`

## Intended Mechanism

This object implements the previously proposed structure:

- biweekly rebalance (`rebalance_stride = 10`)
- free-float market cap between `5` and `30` billion CNY
- `leader_pullback_continuation` descriptor stack
- sector gating: only candidates from the top `5` industries are eligible for
  new entry
- industry score: average of the top `3` candidate scores inside each industry
- hold-band retention:
  - prior holdings may stay if their industry remains inside the top `8`
  - prior holdings may stay if their cross-sectional rank remains inside
    `2.0x` the target holding count

## Window

- executed backtest window: `2020-01-02` to `2026-04-28`
- local green-data limit remains `2026-04-28`

## Build Evidence

- research input: `output/sector_gated_leader_pullback_v1_input.json`
- sleeve artifact: `output/sector_gated_leader_pullback_v1_artifact.json`
- backtest: `output/sector_gated_leader_pullback_v1_daily_backtest.json`

Input coverage:

- rebalance steps: `151`
- research records: `2,459`
- first rebalance date: `2020-01-02`
- last rebalance date: `2026-03-17`

Compared with the prior strict restored strategy:

- strict research records: `3,588`
- new research records: `2,459`

So sector gating substantially narrowed the investable candidate surface.

## Backtest Summary

| Metric | Strict Base | Sector Gated Leader Pullback |
| --- | ---: | ---: |
| Final equity | CNY `8,870,123` | CNY `4,880,531` |
| Total return | `-11.30%` | `-51.19%` |
| CAGR | `-1.88%` | `-10.73%` |
| Annualized return | `1.83%` | `-6.69%` |
| Benchmark annualized return | `7.59%` | `7.59%` |
| Active annualized return | `-5.76%` | `-14.28%` |
| Information ratio | `-0.290` | `-0.536` |
| Annualized volatility | `27.57%` | `32.10%` |
| Sharpe | `0.066` | `-0.209` |
| Max drawdown | `-68.46%` | `-73.53%` |
| Beta | `1.00` | `0.94` |
| Turnover | `71.67x` | `44.42x` |
| Total costs | CNY `1,742,534` | CNY `762,185` |
| Blocked trade share | `20.98%` | `10.10%` |

Yearly returns for the new object:

| Year | Return | Max Drawdown |
| --- | ---: | ---: |
| 2020 | `2.06%` | `-27.71%` |
| 2021 | `6.13%` | `-24.99%` |
| 2022 | `-41.91%` | `-44.01%` |
| 2023 | `-9.07%` | `-25.95%` |
| 2024 | `-30.90%` | `-43.80%` |
| 2025 | `25.14%` | `-26.44%` |
| 2026 YTD | `-7.82%` | `-24.00%` |

## Turnover Readout

The new object did reduce churn materially:

- strict average name overlap between adjacent rebalances: `5.0%`
- new object average name overlap between adjacent rebalances: `33.8%`
- strict average live names per step: `23.76`
- new object average live names per step: `16.28`

Additional execution readout:

- orders: `4,741`
- fills: `4,262`
- blocked orders: `479`
- average gross exposure: `79.21%`
- average cash: CNY `1,549,810`

## Judgment

This object confirms one part of the hypothesis and rejects the other.

Confirmed:

- sector gating plus hold-band retention can reduce turnover materially
- the framework can support a board-first selection object without changing the
  overall V2 chain

Rejected:

- the specific `top-5 industry + hold-band` construction does not improve alpha
- lower turnover did not translate into better net returns

The failure mode is clear in the path:

- the object traded less
- costs fell sharply
- but the book became too narrow and too cash-heavy
- the gated industry signal did not identify durable early-stage leadership

So this version is not deployable and is worse than the already rejected strict
baseline.
