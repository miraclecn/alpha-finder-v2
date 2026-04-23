# V1 DuckDB Reuse Audit

## Purpose

V2 reuses V1 market data only as an external source.

It does not reuse the V1 database as-is.

The correct boundary is:

`V1 audited DuckDB -> explicit V2 audit -> isolated V2 research source DB`

## Audited Files

- V1 source DB: `/home/nan/alpha-find/output/stock_data_audited.duckdb`
- V2 derived DB: `/home/nan/alpha-find-v2/output/research_source.duckdb`
- Audit/build date: `2026-04-23`
- Latest observed market date in the derived V2 DB: `2026-04-22`

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

- rows: `11,633,336`
- trade-date range: `2014-01-02` to `2026-04-22`
- distinct securities: `5,751`
- basis split:
  - `unadjusted`: `11,206,118`
  - `qfq_fallback`: `427,218`

Construction rule:

- use `raw_kline_unadj` when available
- if `raw_kline_unadj` is missing but `raw_kline_qfq` exists, keep the row with `price_basis = qfq_fallback`
- standardize units:
  - `vol -> volume_shares`
  - `amount -> turnover_value_cny`
  - `free_share -> free_float_shares`
  - `circ_mv -> float_mcap_cny`

Interpretation:

- this is strong enough for medium-horizon trend and liquidity research
- downstream research must keep `price_basis` visible and run sensitivity checks on `qfq_fallback` rows
- this table is not yet a complete substitute for official suspension / limit-state data

### `market_trade_calendar` `green`

- rows: `2,986`
- trade-date range: `2014-01-02` to `2026-04-22`

Construction rule:

- derive from observed A-share `raw_daily_basic` dates

### `security_master_ref` `green`

- rows: `5,832`
- list-date range: `1990-12-01` to `2026-04-21`

Construction rule:

- normalize exchange from `ts_code`
- normalize board into `main_board / chinext / star / beijing`

### `name_change_history` `green`

- rows: `4,736`
- effective-date range: `2010-06-29` to `2026-04-22`

Construction rule:

- retain historical name-change rows
- deduplicate overlapping ST windows when expanding daily ST state

### `fundamental_snapshot_pit` `amber`

- rows: `187,623`
- announcement-date range: `2007-01-24` to `2026-04-17`

Construction rule:

- source from `pit_fina_indicator`
- because intraday announcement timing is unavailable, set `available_date` to the next observed market trade date strictly after `ann_date`

Interpretation:

- acceptable for slow anchor and veto logic
- not acceptable for same-day event research

### `industry_classification_static` `amber`

- rows: `5,832`

Construction rule:

- use current `stock_basic_ref.industry`
- tag explicitly as `current_static`

Interpretation:

- usable only as a coarse temporary grouping
- not promotable as historical PIT industry truth

## Hard Data Risks Found During Audit

### 1. Missing unadjusted price coverage is real

`raw_daily_basic` has `11,638,622` rows.

Of those rows:

- `427,218` have no matching `raw_kline_unadj`, but do have `raw_kline_qfq`
- `5,286` have neither `raw_kline_unadj` nor `raw_kline_qfq`

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

### 3. Tradeability realism is still incomplete

Without `suspend_d` and `stk_limit`, V2 still lacks the cleanest first-source way to reconstruct:

- official suspension flags
- official up-limit / down-limit bands
- exact next-session blocked-entry assumptions

Practical implication:

- V2 can already research medium-horizon price leadership
- V2 should not yet claim full launch-grade tradeability realism from the current source DB alone

## Binding Decisions

1. V2 will not connect directly to the mixed V1 DuckDB for research.
2. V2 will use `/home/nan/alpha-find-v2/output/research_source.duckdb` as the isolated local research source.
3. The first honest V2 daily sample starts on `2014-01-02`, not earlier.
4. `fundamental_snapshot_pit` stays a slow `amber` layer.
5. `industry_classification_static` stays `amber` and must not be treated as PIT truth.
6. `trend_leadership` can start from `daily_bar_pit + market_trade_calendar + security_master_ref`, but its industry-relative branch still needs a separate honesty pass.

## Immediate Next Step

The next valid slice is not to fetch more random data.

It is to build the first real `trend_leadership` research input from:

- `daily_bar_pit`
- `market_trade_calendar`
- `security_master_ref`
- `name_change_history`
- `fundamental_snapshot_pit` only as a slow veto or anchor

and to keep the following still blocked until separately audited:

- benchmark-membership neutrality
- historical industry-neutral ranking
- exact price-limit trade blocking
