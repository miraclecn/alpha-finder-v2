# Liquid Midcap Leader Continuation Strict Backtest - 2026-05-06

## Window

- Requested window: `2020-01-02` to `2026-04-30`
- Verified local green-data limit: `2026-04-28`
- Executed backtest window: `2020-01-02` to `2026-04-28`

The local `output/research_source.duckdb` trade calendar and `daily_bar_pit`
both end on `2026-04-28`, so a `2026-04-30` portfolio backtest is not
available from the current workspace data snapshot.

## Strategy Shape

This strict A-share adaptation keeps the closest repo-native approximation to
the ETF source strategy:

- `25`-day recent-weighted log-price momentum
- hard `R^2 >= 0.4` path-quality filter
- current turnover ratio `< 1.8x` prior five-day average
- recent one-day loss floor at `-3%` across the latest three daily returns
- positive trend confirmation through `close > MA20` and rising `MA20`
- free-float market cap between `5` and `30` billion CNY

Removed from the earlier over-adapted object:

- `60`-day momentum lookback
- `20`-day median turnover floor at `120` million CNY
- `RSI14 <= 80`
- `close <= MA20 * 1.15`
- blended descriptor ranking across industry, trend stability, turnover, and
  volume-overheat components

The strict object ranks only by `weighted_momentum_quality`.

## Universe Clarification

The stock pool is not restricted to `CSI 800` constituents.

- candidate rows are loaded from `daily_bar_pit`
- the query requires `s.is_a_share`
- `CSI 800` is used only as the benchmark and overlay observation anchor

Verified local counts:

- full A-share daily-bar universe on `2026-04-28`: `5,488`
- `CSI 800` benchmark snapshot on `2026-03-31`: `800`

## Build Artifacts

- benchmark state: `output/csi800_benchmark_state_history_2020_20260428.json`
- strict research input: `output/liquid_midcap_leader_continuation_strict_v1_input.json`
- strict sleeve artifact: `output/liquid_midcap_leader_continuation_strict_v1_artifact.json`
- overlay observations: `output/liquid_midcap_leader_continuation_strict_v1_overlay_observations.json`
- base backtest: `output/liquid_midcap_leader_continuation_strict_v1_daily_backtest.json`
- overlay backtest: `output/liquid_midcap_leader_continuation_strict_v1_with_overlay_daily_backtest.json`

## Backtest Summary

### Strict Base

| Metric | Value |
| --- | ---: |
| Final equity | CNY `8,870,123` |
| Total return | `-11.30%` |
| CAGR | `-1.88%` |
| Annualized return | `1.83%` |
| Benchmark annualized return | `7.59%` |
| Active annualized return | `-5.76%` |
| Information ratio | `-0.290` |
| Annualized volatility | `27.57%` |
| Sharpe | `0.066` |
| Max drawdown | `-68.46%` |
| Beta | `1.00` |
| Turnover | `71.67x` |
| Total costs | CNY `1,742,534` |
| Blocked trade share | `20.98%` |

Yearly returns:

| Year | Return | Max Drawdown |
| --- | ---: | ---: |
| 2020 | `28.35%` | `-21.76%` |
| 2021 | `14.72%` | `-17.61%` |
| 2022 | `-29.89%` | `-32.22%` |
| 2023 | `-25.69%` | `-31.19%` |
| 2024 | `-26.15%` | `-31.70%` |
| 2025 | `38.66%` | `-20.30%` |
| 2026 YTD | `7.39%` | `-15.75%` |

### Strict Base + Overlay

| Metric | Value |
| --- | ---: |
| Final equity | CNY `8,916,704` |
| Total return | `-10.83%` |
| CAGR | `-1.80%` |
| Annualized return | `0.77%` |
| Benchmark annualized return | `7.59%` |
| Active annualized return | `-6.82%` |
| Information ratio | `-0.385` |
| Annualized volatility | `23.03%` |
| Sharpe | `0.033` |
| Max drawdown | `-63.40%` |
| Beta | `0.80` |
| Turnover | `61.75x` |
| Total costs | CNY `1,465,424` |
| Blocked trade share | `14.81%` |

Yearly returns:

| Year | Return | Max Drawdown |
| --- | ---: | ---: |
| 2020 | `23.79%` | `-18.46%` |
| 2021 | `6.49%` | `-17.21%` |
| 2022 | `-22.50%` | `-23.57%` |
| 2023 | `-23.28%` | `-26.69%` |
| 2024 | `-23.20%` | `-25.52%` |
| 2025 | `32.55%` | `-17.25%` |
| 2026 YTD | `7.61%` | `-12.86%` |

## Overlay Readout

The overlay observation history spans `151` rebalance dates from `2020-01-02`
through `2026-03-17`, using `CSI 800` only as the market-state anchor.

Input-state counts:

- `benchmark_trend`: supportive `54`, neutral `53`, risk_off `44`
- `market_breadth`: supportive `45`, neutral `33`, risk_off `73`
- `dispersion`: supportive `74`, neutral `76`, risk_off `1`
- `realized_volatility`: supportive `105`, neutral `41`, risk_off `5`
- `price_limit_stress`: supportive `150`, neutral `1`, risk_off `0`

Observed exposure effect in the backtest:

- base average gross exposure: `95.38%`
- overlay average gross exposure: `77.84%`
- base average cash: CNY `458,868`
- overlay average cash: CNY `2,170,228`

## Judgment

### Is the strict restored strategy effective?

No.

Even after removing the earlier over-adapted filters and restoring the core
momentum structure more faithfully, the strategy remains:

- negative in total return
- negative in CAGR
- negative in active annualized return
- negative in information ratio
- exposed to very deep drawdowns

So the original ETF logic does not translate into a deployable A-share stock
strategy in this strict form.

### Is the market-timing overlay worth adding?

Only as a risk reducer, not as a strategy rescuer.

The overlay helps on risk shape:

- final equity improves slightly
- max drawdown improves from `-68.46%` to `-63.40%`
- volatility drops from `27.57%` to `23.03%`
- beta drops from `1.00` to `0.80`
- turnover and total costs both decline

But it does not fix the economics:

- both variants still lose money over the full window
- overlay active annualized return is worse
- overlay information ratio is worse

So the right conclusion is:

- **worth adding only if the goal is to compress risk while accepting weaker upside**
- **not worth treating as the main fix**, because the underlying stock-selection
  alpha remains invalid
