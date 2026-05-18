# Sector Gated Leader Pullback V2 Backtest - 2026-05-08

## Object

- Thesis: `config/theses/sector_gated_leader_pullback.toml`
- Descriptor set: `config/descriptor_sets/sector_gated_leader_pullback_v2.toml`
- Sleeve: `config/sleeves/sector_gated_leader_pullback_v2.toml`
- Trend input case: `research/examples/trend_input_build_minimal/sector_gated_leader_pullback_v2.toml`
- Backtest case: `research/examples/deployment_minimal/sector_gated_leader_pullback_v2_portfolio_backtest.toml`

## Intended Mechanism

This iteration kept the same `leader_pullback_continuation` descriptor stack and
the same free-float market-cap band (`5` to `30` billion CNY), but changed the
industry gate from a pure top-score filter to a wider start-of-move bias:

- biweekly rebalance (`rebalance_stride = 10`)
- top industries widened from `5` to `8`
- industry score window widened from top `3` names to top `5`
- industry ranking mode changed to `breadth_then_momentum`
- prior holdings may stay if their industry remains inside the top `12`
- prior holdings may stay if their cross-sectional rank remains inside
  `2.5x` the target holding count

The point of V2 was narrow: keep the sector-gate concept, but see whether a
broader "breadth improvement + recent momentum" industry start signal can
recover alpha without going back to the full-churn strict baseline.

## Window

- executed backtest window: `2020-01-02` to `2026-04-28`
- local green-data limit remains `2026-04-28`

## Build Evidence

- research input: `output/sector_gated_leader_pullback_v2_input.json`
- sleeve artifact: `output/sector_gated_leader_pullback_v2_artifact.json`
- backtest: `output/sector_gated_leader_pullback_v2_daily_backtest.json`

Input coverage:

- rebalance steps: `151`
- research records: `3,320`
- first rebalance date: `2020-01-02`
- last rebalance date: `2026-03-17`

Compared with V1:

- V1 research records: `2,459`
- V2 research records: `3,320`

So the breadth-first ranking did widen the candidate surface materially.

## Backtest Summary

| Metric | Strict Base | Sector Gated V1 | Sector Gated V2 |
| --- | ---: | ---: | ---: |
| Final equity | CNY `8,870,123` | CNY `4,880,531` | CNY `4,630,320` |
| Total return | `-11.30%` | `-51.19%` | `-53.70%` |
| Annualized return | `1.83%` | `-6.69%` | `-7.88%` |
| Benchmark annualized return | `7.59%` | `7.59%` | `7.59%` |
| Active annualized return | `-5.76%` | `-14.28%` | `-15.47%` |
| Information ratio | `-0.290` | `-0.536` | `-0.626` |
| Annualized volatility | `27.57%` | `32.10%` | `31.08%` |
| Sharpe | `0.066` | `-0.209` | `-0.254` |
| Max drawdown | `-68.46%` | `-73.53%` | `-74.03%` |
| Beta | `1.00` | `0.94` | `0.99` |
| Turnover | `71.67x` | `44.42x` | `62.12x` |
| Total costs | CNY `1,742,534` | CNY `762,185` | CNY `1,178,730` |
| Blocked trade share | `20.98%` | `10.10%` | `15.99%` |

Yearly returns for V2:

| Year | Return | Max Drawdown |
| --- | ---: | ---: |
| 2020 | `11.24%` | `-30.30%` |
| 2021 | `5.16%` | `-19.68%` |
| 2022 | `-33.40%` | `-33.64%` |
| 2023 | `-20.58%` | `-31.25%` |
| 2024 | `-27.44%` | `-37.56%` |
| 2025 | `-1.73%` | `-25.01%` |
| 2026 YTD | `-1.35%` | `-22.91%` |

## Turnover Readout

The V2 change widened exposure, but it did not preserve the main V1 benefit.

| Diagnostic | Strict Base | Sector Gated V1 | Sector Gated V2 |
| --- | ---: | ---: | ---: |
| Average adjacent rebalance overlap | `5.0%` | `33.8%` | `15.1%` |
| Average live names per step | `23.76` | `16.28` | `21.99` |
| Average gross exposure | `95.38%` | `79.21%` | `90.09%` |
| Average cash | CNY `458,868` | CNY `1,549,810` | CNY `785,974` |
| Orders | `9,629` | `4,741` | `7,847` |
| Fills | `7,609` | `4,262` | `6,592` |
| Blocked orders | `2,020` | `479` | `1,255` |

Interpretation:

- V1 was too narrow and too cash-heavy.
- V2 did fix part of that. Exposure rose and cash drag fell.
- But the breadth-first gate also gave back most of the turnover benefit.
- Costs rose sharply and alpha did not recover.

## Judgment

This second iteration rejects the current "industry start" salvage path.

What improved:

- candidate surface widened
- average gross exposure recovered from `79.21%` to `90.09%`
- cash drag fell from about `1.55m` to `0.79m`

What got worse:

- total return fell from `-51.19%` to `-53.70%`
- IR fell from `-0.536` to `-0.626`
- max drawdown worsened from `-73.53%` to `-74.03%`
- turnover jumped from `44.42x` to `62.12x`
- blocked orders jumped from `479` to `1,255`

So V2 confirms that the weakness is not just "top-5 industries was too tight."
Once the gate is widened enough to restore exposure, the churn comes back, and
the industry-start signal still does not identify durable leadership.

This object is not deployable and is worse than both:

- the already-rejected V1 sector-gated attempt, and
- the strict restored baseline.

The practical conclusion is to stop iterating on this exact sector-gated
branch and look elsewhere for turnover control.
