# Liquid Midcap Leader Continuation Backtest - 2026-05-06

## Object

- Research object: `liquid_midcap_leader_continuation_v1`
- Thesis: `config/theses/liquid_midcap_leader_continuation.toml`
- Descriptor set: `config/descriptor_sets/liquid_midcap_leader_continuation_v1.toml`
- Sleeve: `config/sleeves/liquid_midcap_leader_continuation_v1.toml`
- Portfolio case:
  `research/examples/deployment_minimal/liquid_midcap_leader_continuation_v1_portfolio_backtest.toml`

## Strategy Definition

The object adapts the ETF rotation idea to A-share stocks with:

- 60-day weighted log-price momentum multiplied by R-squared
- R-squared hard filter at `0.35`
- positive trend-line filter through a 20-day finite EMA/Laplace proxy
- 20-day median turnover floor of CNY `120mn`
- free-float market cap between CNY `5bn` and CNY `30bn`
- current turnover ratio below `1.8x` the prior five-day average
- no recent single-day loss below `-4%`
- close no more than `15%` above 20-day average
- RSI14 no greater than `80`
- SW2021 L1 industry-relative ranking and max three names per industry

## Generated Evidence

- Benchmark state:
  `output/csi800_benchmark_state_history_2020_20260428.json`
- Research input:
  `output/liquid_midcap_leader_continuation_v1_input.json`
- Sleeve artifact:
  `output/liquid_midcap_leader_continuation_v1_artifact.json`
- Daily portfolio backtest:
  `output/liquid_midcap_leader_continuation_v1_daily_backtest.json`

Evidence coverage:

| Artifact | Count / Window |
| --- | ---: |
| Benchmark state steps | `1,526` |
| Backtest window | `2020-01-02` to `2026-04-28` |
| Research signal steps | `151` |
| Research records | `3,460` |
| Backtest trading days | `1,526` |
| Orders | `8,767` |
| Fills | `7,028` |
| Blocked orders | `1,739` |
| Partial fills | `4` |

Quarantined before candidate selection:

- corporate-action exception windows: `22`
- qfq-fallback windows: `148`
- tradeability fallback windows: `1`

Final backtest exposure counts:

- corporate-action exception exposure: `0`
- qfq-fallback price exposure: `0`
- tradeability fallback exposure: `0`
- market-data fallback exposure: `0`

## Backtest Summary

| Metric | Value |
| --- | ---: |
| Initial capital | CNY `10,000,000` |
| Final equity | CNY `8,061,608` |
| Total return | `-19.38%` |
| CAGR | `-3.35%` |
| Annualized return | `0.47%` |
| Benchmark annualized return | `7.59%` |
| Active annualized return | `-7.12%` |
| Information ratio | `-0.35` |
| Annualized volatility | `28.28%` |
| Sharpe | `0.02` |
| Max drawdown | `-70.96%` |
| Beta | `1.02` |
| Turnover | `65.85x` |
| Total costs | CNY `1,531,375` |
| Blocked trade share | `19.84%` |

Yearly returns:

| Year | Return | Max Drawdown |
| --- | ---: | ---: |
| 2020 | `37.75%` | `-25.01%` |
| 2021 | `0.14%` | `-21.14%` |
| 2022 | `-32.86%` | `-34.24%` |
| 2023 | `-24.24%` | `-31.76%` |
| 2024 | `-23.80%` | `-32.37%` |
| 2025 | `45.30%` | `-27.08%` |
| 2026 YTD | `1.07%` | `-18.70%` |

## Judgment

This research object is implemented and reproducibly backtested, but it is not
admissible for capital deployment.

The object avoids known data-quality exposure in the final backtest, yet fails
strategy quality: active annualized return is negative, information ratio is
negative, drawdown is far too large, and realized turnover is excessive.

The main useful conclusion is negative: adding weighted momentum, R-squared,
trend filtering, mid-cap liquidity constraints, and volume overheat control did
not rescue a simple A-share continuation sleeve over this validation window.

