# V1 DuckDB Reuse Audit

## Purpose

V2 reuses V1 market data only as an external source.

It does not reuse the V1 database as-is.

The correct boundary is:

`V1 audited DuckDB -> explicit V2 audit -> isolated V2 research source DB`

## Audited Files

- V1 source DB: `/home/nan/alpha-find/output/stock_data_audited.duckdb`
- V2 derived DB: `/home/nan/alpha-find-v2/output/research_source.duckdb`
- Audit/build date: `2026-04-29`
- Latest observed market date in the derived V2 DB: `2026-04-28`

## What Exists In V1

The populated V1 DuckDB does contain reusable market data from the personal Tushare stack:

- `raw_daily_basic`
- `raw_adj_factor`
- `raw_kline_unadj`
- `raw_kline_qfq`
- `pit_fina_indicator`
- `stock_basic_ref`
- `raw_namechange`

It does not contain the first-choice versions of several datasets that V2 originally wanted for the launch-grade green layer:

- no `suspend_d`
- no `stk_limit`
- no historical `index_member_all`
- no historical `index_classify`

This means V2 can start from real daily market and slow fundamental data immediately, but not from a fully reconstructed benchmark-membership or exchange-limit state.

## Derived V2 Dataset Registry

### `daily_bar_pit` `green`

- rows: `11,655,309`
- trade-date range: `2014-01-02` to `2026-04-28`
- distinct securities: `5,754`
- basis split:
  - `unadjusted`: `11,222,603`
  - `qfq_fallback`: `427,218`

Construction rule:

- use `raw_kline_unadj` for raw OHLC fields when available
- if `raw_kline_unadj` is missing but `raw_kline_qfq` exists, keep the row with `price_basis = qfq_fallback` so downstream consumers can exclude or sensitivity-test it
- populate adjusted OHLC diagnostic fields from `raw_kline_unadj * adj_factor` when unadjusted rows exist; `raw_kline_qfq` is labeled diagnostic fallback only
- standardize units:
  - `vol -> volume_shares`
  - `amount -> turnover_value_cny`
  - `free_share -> free_float_shares`
  - `circ_mv -> float_mcap_cny`

Interpretation:

- this is strong enough for medium-horizon trend and liquidity research
- portfolio-level research backtests use raw OHLC for fills, marks, and exchange-limit diagnostics
- signal returns use raw prices plus `adj_factor` for PIT economic return calculation, not static qfq bars
- downstream research must keep `price_basis` visible and run sensitivity checks on `qfq_fallback` rows
- `tradeability_state_daily` adds official suspension / limit-state rows when staged, with OHLC fallback diagnostics otherwise

### `market_data_quality_audit` `required`

Construction rule:

- run `audit-market-data-quality` against the isolated V2 research source after
  each `build-research-source-db` refresh
- persist the JSON artifact under `output/audits/`
- review the counts before treating a strategy backtest as promotion evidence

Required metrics:

- total `daily_bar_pit` rows
- `qfq_fallback` rows
- rows missing raw OHLC prices
- rows with missing or non-positive `adj_factor`
- `corporate_action_ledger` rows
- `tradeability_state_daily` rows
- significant adjustment-factor jumps, explained adjustment-factor jumps, and
  jumps that cannot be explained by a staged corporate-action window
- unresolved adjustment-factor jump distribution by year, magnitude bucket, and
  highest-frequency securities
- unresolved adjustment-factor jump proximity to staged `raw_dividend` rows,
  especially same-date non-implemented records, nearby implemented candidates,
  and rows with no implemented dividend within 30 calendar days
- top unresolved adjustment-factor jump examples with previous/current bar
  dates and nearest implemented dividend context

Current command:

```bash
PYTHONPATH=src python3 -m alpha_find_v2 audit-market-data-quality \
  --source-db output/research_source.duckdb \
  --output output/audits/market_data_quality_YYYYMMDD.json
```

Latest refreshed artifact: `output/audits/market_data_quality_20260429.json`.

Interpretation:

- unresolved adjustment-factor jumps are a data blocker, not an alpha parameter
  to tune around
- `qfq_fallback` rows remain diagnostic fallback rows and must not be promoted
  into PIT adjusted-price truth
- a missing `corporate_action_ledger` or `tradeability_state_daily` table means
  the research DB has not yet materialized the Batch 1 credibility surface

### `market_trade_calendar` `green`

- rows: `2,990`
- trade-date range: `2014-01-02` to `2026-04-28`

Construction rule:

- derive from observed A-share `raw_daily_basic` dates

### `security_master_ref` `green`

- rows: `5,835`
- list-date range: `1990-12-01` to `2026-04-27`

Construction rule:

- normalize exchange from `ts_code`
- normalize board into `main_board / chinext / star / beijing`

### `name_change_history` `green`

- rows: `4,786`
- effective-date range: `2010-06-29` to `2026-04-28`

Construction rule:

- retain historical name-change rows
- deduplicate overlapping ST windows when expanding daily ST state

### `fundamental_snapshot_pit` `amber`

- rows: `190,232`
- announcement-date range: `2007-01-24` to `2026-04-24`

Construction rule:

- source from `pit_fina_indicator`
- because intraday announcement timing is unavailable, set `available_date` to the next observed market trade date strictly after `ann_date`

Interpretation:

- acceptable for slow anchor and veto logic
- not acceptable for same-day event research

### `industry_classification_static` `amber`

- rows: `5,835`

Construction rule:

- use current `stock_basic_ref.industry`
- tag explicitly as `current_static`

Interpretation:

- usable only as a coarse temporary grouping
- not promotable as historical PIT industry truth

### `corporate_action_ledger` `green schema / high coverage`

- rows after historical `raw_dividend` backfill: `45,342`
- book-date range: `2014-01-06` to `2026-04-28`
- staged `raw_dividend` rows: `39,828`
- staged `raw_dividend` ex-date range: `2014-01-06` to `2026-04-28`

Construction rule:

- source from staged `raw_dividend`
- admit only implemented dividend rows with valid ex-dates
- book cash on `pay_date`
- book share changes on `ex_date`

Interpretation:

- the schema and derivation are now materialized in `output/research_source.duckdb`
- the historical dividend window is staged for 2014-2026
- unresolved adjustment-factor jumps dropped from `35,799` under the two-session
  sample to `920` after historical `raw_dividend` backfill
- after switching the reconciliation rule from same-day matching to the
  professional bar-window rule `(previous available bar date, current bar date]`,
  `35,692` of `35,814` significant adjustment-factor jumps are explained by
  `corporate_action_ledger`; the remaining `122` jumps are the targeted
  reconciliation queue

### `corporate_action_exception_ledger` `amber`

- rows: `122`
- trade-date range: `2014-01-14` to `2025-08-22`
- recommendation: `quarantine_security_window_from_promotion`

Construction rule:

- source from `daily_bar_pit.adj_factor` jumps not explained by
  `corporate_action_ledger`
- retain previous/current bar dates, factor ratio, magnitude, severity,
  suspension-window context, `pre_close` basis difference, and triage class
- never infer cash or share bookings from price/factor behavior alone

Triage:

| Class | Rows | Securities | Interpretation |
| --- | ---: | ---: | --- |
| `implemented_dividend_outside_factor_window` | 6 | 6 | nearby official dividend exists but not in the factor-jump bar window |
| `nonimplemented_dividend_same_date` | 1 | 1 | raw dividend exists but is not `div_proc='瀹炴柦'` |
| `daily_pre_close_ex_right_without_ledger` | 100 | 94 | Tushare daily `pre_close` and `adj_factor` agree on an ex-right adjustment, but no bookable action is staged |
| `low_materiality_provider_factor_noise` | 5 | 1 | sub-50bp factor-only source noise |
| `provider_factor_jump_without_event_evidence` | 10 | 4 | factor changed without dividend or `pre_close` support |

Severity:

| Severity | Magnitude | Rows | Securities |
| --- | --- | ---: | ---: |
| `low` | `<=50bp` | 7 | 3 |
| `medium` | `<=2pct` | 5 | 4 |
| `high` | `<=10pct` | 58 | 47 |
| `critical` | `>10pct` | 52 | 51 |

Interpretation:

- `100` of `122` rows fall in an official suspension/resumption window or are
  otherwise corroborated by the daily `pre_close` ex-right basis; they are not
  random missing dividends, but they still cannot be booked into the broker-like
  ledger without an official cash/share action source.
- all `122` rows remain promotion-blocking security windows until either a
  new official event source explains them or the affected security/date window
  is excluded from strategy evidence.

### `tradeability_state_daily` `green schema / near-complete official coverage`

- rows: `11,655,309`
- trade-date range: `2014-01-02` to `2026-04-28`
- official rows from staged `suspend_d` / `stk_limit`: `11,653,846`
- OHLC fallback rows: `1,463`
- official date range after staged backfill: `2014-01-02` to `2026-04-28`
- staged `raw_stk_limit` rows: `14,123,930`
- staged `raw_suspend_d` rows: `384,691`
- duplicate `(security_id, trade_date)` tradeability keys: `0`

Construction rule:

- use staged `raw_suspend_d` for official suspension state when present
- use staged `raw_stk_limit` for official up/down limit bands when present
- retain OHLC fallback rows for non-staged dates so downstream logic can keep
  running with explicit source priority

Interpretation:

- the surface is now materialized and usable for diagnostics and full-window
  official-first backtests
- 2014 through current-day official tradeability coverage is staged for nearly
  the full `daily_bar_pit` panel
- the remaining `1,463` fallback rows are source-level gaps, not missing annual
  backfill windows: `1,366` rows are `001914.SZ` before its staged
  `raw_stk_limit` begins on `2019-12-16`, and the rest are sparse Beijing-board
  / old-transfer rows on `2020-09-18` and `2021-08-26`
- promotion-grade evidence must still report or exclude
  `source_priority = 'ohlc_fallback'` rows, but the large 2014-2020
  tradeability staging blocker is closed

## Hard Data Risks Found During Audit

### 1. Missing unadjusted price coverage is real

The A-share-filtered `raw_daily_basic` spine has `11,650,120` rows.

Of those rows:

- `427,218` have no matching `raw_kline_unadj`, but do have `raw_kline_qfq`
- `299` have neither `raw_kline_unadj` nor `raw_kline_qfq`

This is not a V2 implementation bug.
It is a real gap in the V1 market-price history.

Practical implication:

- V2 should not pretend all daily rows are equally trustworthy
- `price_basis` and missing-price exclusions must remain explicit in research

### 2. Industry is not PIT-safe yet

The only currently reusable industry field is the current static industry from `stock_basic_ref`.

Practical implication:

- sector-relative normalization can be explored
- promotion-safe industry membership is still blocked

### 3. Tradeability realism is official-first with residual gaps

With staged `suspend_d` and `stk_limit`, V2 now has the cleanest first-source
way to reconstruct:

- official suspension flags
- official up-limit / down-limit bands
- exact next-session blocked-entry assumptions

Practical implication:

- V2 can already research medium-horizon price leadership
- V2 can run full-window official-first tradeability checks, subject to the
  remaining `1,463` explicit fallback rows
- `source_priority` must remain visible in promotion evidence so those rows are
  reported, excluded, or sensitivity-tested instead of silently treated as
  official exchange state
- `refresh-reference-market-events --mode append` remains the correct recovery
  command for future incremental event backfills; the default `replace` mode is
  only for a one-shot bounded refresh

### 4. Corporate-action coverage is mostly staged; residual jumps remain

The first real event refresh covered only `2026-04-01` through `2026-04-02` and
produced `35,799` unresolved adjustment-factor jumps.

After backfilling `raw_dividend` for `2014-01-01` through `2026-04-28` and
official tradeability for `2014-01-01` through `2026-04-28`, the market-data
quality audit at `output/audits/market_data_quality_20260429.json` reports:

- `corporate_action_rows`: `45,342`
- `tradeability_rows`: `11,655,309`
- `tradeability_official_rows`: `11,653,846`
- `tradeability_ohlc_fallback_rows`: `1,463`
- `adj_factor_jump_assessable`: `true`
- `missing_quality_tables`: `[]`
- `promotion_blocking_quality_state`: `blocked_unresolved_adj_factor_jumps`
- `qfq_fallback_rows`: `427,218`
- `missing_price_rows`: `0`
- `zero_or_missing_adj_factor_rows`: `0`
- `adj_factor_jump_rows`: `35,814`
- `explained_adj_factor_jump_rows`: `35,692`
- `unresolved_adj_factor_jump_rows`: `122`
- `promotion_blocking_unresolved_adj_factor_jump_rows`: `122`

The remaining unresolved jumps are now materialized in
`corporate_action_exception_ledger`. They are small enough to review directly,
but they remain hard security-window exclusions for promotion evidence.

The current frozen trend live-candidate daily backtest does not cross those
corporate-action exception windows, does not use tradeability OHLC fallback,
and no longer touches qfq fallback price rows after research-input generation
started quarantining qfq-fallback windows. The rebuilt multi-year validation
audit therefore reports zero corporate-action-exception, qfq-fallback, and
tradeability-fallback portfolio exposures. The global `427,218` qfq fallback
rows remain visible in the market-data audit; they are excluded from candidate
evidence rather than treated as trustworthy raw OHLC.

Unresolved adjustment-factor jumps by magnitude:

| Magnitude | Rows | Securities |
| --- | ---: | ---: |
| `<=50bp` | 7 | 3 |
| `<=2pct` | 5 | 4 |
| `<=10pct` | 58 | 47 |
| `>10pct` | 52 | 51 |

Unresolved adjustment-factor jumps by year:

| Year | Rows | Securities |
| --- | ---: | ---: |
| 2014 | 12 | 12 |
| 2015 | 7 | 7 |
| 2016 | 13 | 13 |
| 2017 | 10 | 10 |
| 2018 | 16 | 14 |
| 2019 | 11 | 11 |
| 2020 | 20 | 20 |
| 2021 | 10 | 10 |
| 2022 | 11 | 11 |
| 2023 | 8 | 8 |
| 2024 | 2 | 2 |
| 2025 | 2 | 2 |

Persisted audit reconciliation against staged `raw_dividend` shows:

- same-date non-implemented raw dividend only: `1` row
- implemented raw dividend within `卤30` calendar days: `6` rows
- no implemented raw dividend within `卤30` calendar days: `115` rows

Persisted exception triage shows:

- `daily_pre_close_ex_right_without_ledger`: `100` rows
- `implemented_dividend_outside_factor_window`: `6` rows
- `nonimplemented_dividend_same_date`: `1` row
- `low_materiality_provider_factor_noise`: `5` rows
- `provider_factor_jump_without_event_evidence`: `10` rows

Interpretation:

- the old `920` residual queue contained many false positives where the company
  action fell during a suspension/no-bar interval and the factor jump only
  appeared on the next available bar
- the remaining `122` rows are not explained by ordinary implemented dividend
  timing; `115` have no implemented dividend within `卤30` calendar days
- these `122` rows are now explicit `corporate_action_exception_ledger` rows,
  not hidden residuals; generated research inputs exclude affected
  security/windows before artifact creation, promotion/live-release gates block
  any remaining exposure, and the daily backtester reports held-position
  exposure diagnostics without inventing broker bookings

## Binding Decisions

1. V2 will not connect directly to the mixed V1 DuckDB for research.
2. V2 will use `/home/nan/alpha-find-v2/output/research_source.duckdb` as the isolated local research source.
3. The first honest V2 daily sample starts on `2014-01-02`, not earlier.
4. `fundamental_snapshot_pit` stays a slow `amber` layer.
5. `industry_classification_static` stays `amber` and must not be treated as PIT truth.
6. `trend_leadership` can start from `daily_bar_pit + market_trade_calendar + security_master_ref`, but its industry-relative branch still needs a separate honesty pass.

## Machine-Readable Phase 1 Boundary Outputs

The release-1 data spine is now emitted as explicit registry tables instead of
living only in prose:

- `output/pit_reference_staging.duckdb.reference_dataset_registry` records the
  staged PIT datasets, their source provider, and their row/date coverage.
- `output/research_source.duckdb.data_spine_registry` records the only allowed
  source, staging, and isolated V2 research surfaces for the Phase 1 chain.
- `output/research_source.duckdb.build_chain_registry` records the binding
  `build-reference-staging-db -> build-research-source-db ->
  build-benchmark-state` order.
- `output/research_source.duckdb.data_boundary_registry` records the
  release-1 tier rules, reusable surfaces, forbidden reuse objects, visible
  gaps, the `AKShare` audit rule, and explicit stop conditions.

## Immediate Next Step

The exception ledger is now wired into research input generation, promotion
evidence, live-release validation, and backtest diagnostics. Residual source
research can continue in parallel, but no row should move out of the exception
ledger unless an official source or reviewed source-specific rule is covered by
tests.

Do not tune or generate strategies from portfolio backtests until:

- `corporate_action_ledger` covers the target backtest window
- unresolved adjustment-factor exception windows are explicitly excluded or
  reported in the strategy evidence
- official tradeability coverage is measured separately from OHLC fallback
- the persisted market-data quality audit is attached to the strategy evidence
