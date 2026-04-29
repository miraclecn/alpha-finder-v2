# A-share PIT Adjustment + Corporate-Action Ledger

Date: 2026-04-28

## Decision

V2 separates two price/accounting surfaces:

- Signal and research labels use decision-time visible PIT economic returns:
  `raw_price(t1) * adj_factor(t1) / (raw_price(t0) * adj_factor(t0)) - 1`.
- Execution, position sizing, tradeability checks, and mark-to-market accounting use raw unadjusted OHLC and explicit corporate-action booking.

Static qfq bars are diagnostic fallback data only. They must not become the PIT adjusted-price truth for research or the execution price for backtests.

## Tushare Interface Scope

Official interfaces checked for this slice:

- `daily`: unadjusted daily OHLC.
- `daily_basic`: daily liquidity and valuation spine.
- `adj_factor`: adjustment factors used for PIT economic returns.
- `stk_factor`: diagnostic adjusted-price comparison only.
- `dividend`: dividend and bonus-share events. V2 only ingests `div_proc='实施'` rows with valid `ex_date`.
- `stk_limit`: daily exchange limit prices.
- `suspend_d`: daily suspension records.
- `share_float`: staged for audit; not yet alpha or execution logic.
- `repurchase`: staged for audit; not yet alpha or execution logic.

Local permission test result from the previous research pass: the current token can read `daily`, `daily_basic`, `adj_factor`, `stk_factor`, `dividend`, `stk_limit`, `suspend_d`, `share_float`, and `repurchase`; `stk_premarket` is not available and is out of scope for phase 1.

## Data Model

New staged raw tables:

- `raw_dividend`
- `raw_stk_limit`
- `raw_suspend_d`
- `raw_share_float`
- `raw_repurchase`

Derived tables:

- `corporate_action_ledger`
  - `cash_dividend` rows book cash on `pay_date`.
  - `share_dividend` rows adjust share count on `ex_date`.
  - Rows require `div_proc='实施'` and a valid `ex_date`.
- `tradeability_state_daily`
  - Uses `raw_suspend_d` and `raw_stk_limit` when present.
  - Falls back to OHLC-derived suspension and limit-lock diagnostics when official rows are absent.

## Implementation Rules

- `daily_bar_pit.open/high/low/close/pre_close` are raw unadjusted transaction prices when `price_basis='unadjusted'`.
- `price_basis='qfq_fallback'` remains visible for rows lacking unadjusted OHLC.
- If adjusted OHLC columns are retained, their source must be explicit in `adjusted_price_source`.
- PIT adjusted-return helpers must not multiply `adj_factor` into `price_basis='qfq_fallback'` rows a second time.
- Trend, overlay, and benchmark return calculations use raw price plus `adj_factor`, not qfq price columns.
- Portfolio fills and marks use raw OHLC.
- Corporate actions are booked explicitly in the portfolio ledger, and only shares observed on the action record date are eligible for later cash/share booking.
- Adj-factor jumps without a matching ledger entry produce `unresolved_corporate_action` diagnostics.

## Acceptance Checks

- Staging tests cover pagination, de-duplication, empty results, and all new raw Tushare tables.
- Bootstrap tests cover `corporate_action_ledger`, `tradeability_state_daily`, raw OHLC semantics, and adjusted-source metadata.
- Trend and regime overlay tests prove PIT returns are calculated from raw price and `adj_factor`.
- Portfolio backtester tests prove raw execution, cash dividends, share dividends, official tradeability, and unresolved action diagnostics.
- Research artifact tests prove target labels can use broker-like holding return instead of naive `exit_open / entry_open - 1`.

## Out of Scope

- No new dependency.
- No mutation of the V1 DuckDB.
- No `stk_premarket` integration.
- `share_float` and `repurchase` are staged only.
- Rare actions not expressible by `dividend` are flagged through adj-factor reconciliation instead of guessed.
