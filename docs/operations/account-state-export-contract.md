# Account State Export Contract

## Purpose

Turn one manual broker/account export into the repository's
`account_state_snapshot` contract without spreadsheet-side repair.

## Required Fields

- `account_id`
- `as_of_date`
- `total_equity_cny`
- `cash_balance_cny`
- `available_cash_cny`
- `positions[]`
- `trade_restrictions[]`

Each position must preserve:

- `asset_id`
- `shares`
- `available_shares`
- `market_value_cny`

Each trade restriction must preserve:

- `asset_id`
- `can_enter`
- `can_exit`
- `reason`

## Operating Rule

1. Export broker/account holdings and cash after the decision-date close and before the next-open order entry.
2. Normalize the export into one JSON `account_state_snapshot`.
3. Record any suspension, limit lock, unavailable shares, or broker-side restriction explicitly in `trade_restrictions`.
4. Feed that snapshot into `build-executable-signal`.

## Hard Stops

- Do not edit holdings inside portfolio research objects.
- Do not net blocked shares away in spreadsheets.
- Do not drop unavailable shares; they are needed to preserve honest exit blocks.
