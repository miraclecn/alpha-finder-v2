# Trusted Backtest And Strategy Generation Risk Roadmap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Current steering note, 2026-04-29:** This document is retained as the
> technical backtest/data-quality roadmap and progress log. For project-level
> priority and capital-admission sequencing, use
> `docs/status/project-current-state-2026-04-29.md` and
> `docs/superpowers/plans/2026-04-29-professional-quant-roadmap.md`.
> Earlier unchecked checklist items below may be historical task text; prefer
> the progress log and current-state document when they conflict.

**Goal:** turn the current PIT-adjusted price and corporate-action repair into a staged path toward credible A-share portfolio backtests and constrained strategy generation.

**Architecture:** Treat credibility as a chain of gates: materialized data truth, data-quality audit, broker-like backtest ledger, risk-aware research labels, constrained strategy generation, then shadow-live evidence. Each batch must leave a testable artifact and must not weaken the V2 object chain: `mandate -> thesis -> descriptor set -> sleeve -> portfolio recipe -> executable signal -> decay record`.

**Tech Stack:** Python standard library, DuckDB, unittest, pytest, existing `alpha_find_v2` CLI, existing TOML/JSON research artifacts, no new dependencies.

---

## Current State On 2026-04-28

The current code slice has already made the most important accounting decision:

- research and signal returns use PIT economic returns from raw price plus `adj_factor`
- fills, marks, price-limit checks, and portfolio PnL use raw unadjusted OHLC
- corporate actions are booked explicitly through a derived ledger
- static qfq bars are diagnostic fallback only

Fresh local inspection now shows the Batch 1 surfaces are materialized:

- `output/research_source.duckdb` contains `daily_bar_pit`, `industry_classification_pit`, `benchmark_membership_pit`, `fundamental_snapshot_pit`, `corporate_action_ledger`, and `tradeability_state_daily`
- `output/pit_reference_staging.duckdb` contains `raw_dividend`, `raw_stk_limit`, `raw_suspend_d`, `raw_share_float`, and `raw_repurchase`
- `output/audits/market_data_quality_20260428.json` records the current data-quality audit

Therefore the next credible step is not another alpha tweak. It is to finish event coverage gaps, reconcile the remaining corporate-action anomalies, then harden the execution ledger and strategy-generation gates on top of the audited surfaces.

## Progress Log

### 2026-04-28 Batch 0 And Batch 1 Start

Completed:

- `docs/operations/trend-leadership-live-candidate-v1.md` now states that portfolio backtests use raw unadjusted `daily_bar_pit` OHLC plus explicit corporate-action booking, not adjusted OHLC fills or marks.
- `reference_data_staging` now fetches market-event interfaces with API-safe date scoping:
  - `dividend`: by `ex_date`
  - `stk_limit`: one `start_date=end_date` day at a time
  - `suspend_d`: one `start_date=end_date` day at a time
  - `share_float`: by `ann_date`
  - `repurchase`: by `ann_date`
- Unit coverage pins the parameter behavior in `tests/test_reference_data_staging.py`.

Observed live API behavior:

- A full historical event query using one large `start_date/end_date` plus offset fails near offset `102000` with Tushare parameter errors.
- `dividend` returns no useful full-range data with `start_date/end_date`, but returns rows when queried by `ex_date`.
- `share_float` and `repurchase` accept `ann_date` exact-date queries; this is safer than relying on broad ranges whose real pagination can exceed Tushare's offset tolerance.
- Direct two-day live event smoke for `20260401` through `20260402` returned:
  - `raw_dividend`: `2`
  - `raw_repurchase`: `143`
  - `raw_share_float`: `14885`
  - `raw_stk_limit`: `15050`
  - `raw_suspend_d`: `35`
- `build-research-source-db --target-db output/research_source_market_events_smoke.duckdb --supplemental-db output/pit_reference_staging.duckdb` completed and produced:
  - `corporate_action_ledger`: `0` rows because the supplemental DB used for this smoke did not yet contain real `raw_dividend`
  - `tradeability_state_daily`: `11,649,821` rows through OHLC fallback

Remaining in Batch 1:

- Run a bounded real staging build with event tables written to DuckDB without hanging on full historical API volume.
- Then rebuild the real `output/research_source.duckdb` using that supplemental DB.
- Add the formal market-data quality JSON audit command before treating the materialized DB as promotion evidence.

### 2026-04-28 Batch 1 Quality Audit CLI

Completed:

- Added `src/alpha_find_v2/market_data_quality.py`.
- Added `audit-market-data-quality` CLI with default output under `output/audits/market_data_quality_YYYYMMDD.json`.
- Added `build-reference-staging-db --market-event-start-date/--market-event-end-date`
  so Tushare market-event calls can be bounded independently from benchmark and
  industry staging windows.
- Added `refresh-reference-market-events` so bounded event refreshes can update
  existing supplemental DB raw event tables without replacing PIT benchmark or
  industry tables.
- Added `refresh-reference-market-events --mode append` so historical event
  coverage can be accumulated in recoverable batches instead of replacing the
  previous batch.
- Added `refresh-reference-market-events --event ...` so corporate actions and
  tradeability can be backfilled independently.
- Added `--request-interval-seconds` for Tushare frequency limits and
  `--skip-append-deduplicate` for known non-overlapping append windows.
- Added unit coverage in `tests/test_market_data_quality.py` for:
  - missing optional derived tables
  - `qfq_fallback` counts
  - missing raw OHLC counts
  - missing or non-positive adjustment factors
  - unresolved adjustment-factor jumps reconciled against `corporate_action_ledger.ex_date` / `book_date`
- Updated `docs/data/v1-duckdb-reuse-audit.md` to require the persisted JSON audit after each research-source refresh.

Remaining materialization command shape:

```bash
PYTHONPATH=src python3 -m alpha_find_v2 build-reference-staging-db \
  --market-event-start-date YYYYMMDD \
  --market-event-end-date YYYYMMDD
PYTHONPATH=src python3 -m alpha_find_v2 refresh-reference-market-events \
  --start-date YYYYMMDD \
  --end-date YYYYMMDD \
  --mode append \
  --event raw_dividend
PYTHONPATH=src python3 -m alpha_find_v2 refresh-reference-market-events \
  --start-date YYYYMMDD \
  --end-date YYYYMMDD \
  --mode append \
  --event raw_stk_limit \
  --event raw_suspend_d \
  --request-interval-seconds 0.35 \
  --skip-append-deduplicate
PYTHONPATH=src python3 -m alpha_find_v2 build-research-source-db \
  --supplemental-db output/pit_reference_staging.duckdb
PYTHONPATH=src python3 -m alpha_find_v2 audit-market-data-quality
```

### 2026-04-28 Batch 1 Real Two-Session Materialization

Completed:

- Ran `refresh-reference-market-events` for `2026-04-01` through `2026-04-02`
  against `output/pit_reference_staging.duckdb`.
- Rebuilt `output/research_source.duckdb` with
  `--supplemental-db output/pit_reference_staging.duckdb`.
- Wrote `output/audits/market_data_quality_20260428.json`.

Observed counts:

| Surface | Rows / Coverage |
| --- | ---: |
| `raw_dividend` | 2 |
| `raw_stk_limit` | 15,050 |
| `raw_suspend_d` | 35 |
| `raw_share_float` | 14,885 |
| `raw_repurchase` | 143 |
| `corporate_action_ledger` | 2 |
| `tradeability_state_daily` | 11,649,821 |
| `tradeability_state_daily.source_priority = official` | 5,485 |
| `tradeability_state_daily.source_priority = ohlc_fallback` | 11,644,336 |
| `unresolved_adj_factor_jump_rows` | 35,799 |

Interpretation:

- The Batch 1 schema is now materialized in the local real research DB.
- The current event data is a bounded smoke slice, not full historical coverage.
- The unresolved adjustment-factor jump count is a hard blocker for
  promotion-grade backtests over the full 2014-2026 window.
- Next Batch 1 work should run recoverable historical market-event slices, not
  strategy tuning.

### 2026-04-28 Batch 1 Historical Dividend And Tradeability Backfill

Completed:

- Backfilled `raw_dividend` by `ex_date` for `2014-01-01` through `2026-04-28`.
- Rebuilt `output/research_source.duckdb`; `corporate_action_ledger` now has
  `45,342` rows covering `2014-01-06` through `2026-04-28`.
- Re-ran `audit-market-data-quality`; unresolved adjustment-factor jumps fell
  from `35,799` to `920`.
- Backfilled `raw_stk_limit` and `raw_suspend_d` for `2023-01-01` through
  `2026-04-28`.
- Fixed duplicate official tradeability joins by deduplicating staged
  `raw_suspend_d` and aggregating staged `raw_stk_limit` to one key per
  `(ts_code, trade_date)` before joining `daily_bar_pit`.

Observed counts after rebuild:

| Surface | Rows / Coverage |
| --- | ---: |
| `raw_dividend` | 39,828 |
| `raw_stk_limit` | 5,540,647 |
| `raw_suspend_d` | 20,998 |
| `corporate_action_ledger` | 45,342 |
| `tradeability_state_daily` | 11,655,309 |
| `tradeability_state_daily.source_priority = official` | 4,254,772 |
| `tradeability_state_daily.source_priority = ohlc_fallback` | 7,400,537 |
| `unresolved_adj_factor_jump_rows` | 920 |

Interpretation:

- Corporate-action coverage is now strong enough for targeted reconciliation
  rather than broad blocker language.
- Official tradeability coverage is usable for recent windows beginning
  `2023-01-03`; 2014-2022 still relies on OHLC fallback until staged.
- Earlier official tradeability backfill should continue in smaller windows; a
  full 2024 annual append completed but spent most of its time in DB write and
  filesystem commit, so smaller windows are operationally safer.

### 2026-04-28 Batch 1 2022 Official Tradeability Backfill

Completed:

- Backfilled `raw_stk_limit` and `raw_suspend_d` for the full 2022 calendar
  year in quarterly append windows:
  - 2022 Q4: `374,477` `raw_stk_limit`, `5,703` `raw_suspend_d`
  - 2022 Q3: `395,301` `raw_stk_limit`, `6,446` `raw_suspend_d`
  - 2022 Q2: `352,821` `raw_stk_limit`, `4,590` `raw_suspend_d`
  - 2022 Q1: `340,398` `raw_stk_limit`, `2,450` `raw_suspend_d`
- Rebuilt `output/research_source.duckdb` with the updated supplemental DB.
- Re-ran `audit-market-data-quality`; unresolved adjustment-factor jumps remain
  `920`, which confirms tradeability backfill is orthogonal to the corporate
  action residual queue.
- Extended `market_data_quality` output so the persisted JSON audit now includes
  unresolved jump distribution by year, magnitude bucket, and top securities.

Observed counts after rebuild:

| Surface | Rows / Coverage |
| --- | ---: |
| `raw_stk_limit` | 7,003,644 |
| `raw_suspend_d` | 40,187 |
| `tradeability_state_daily` | 11,655,309 |
| `tradeability_state_daily.source_priority = official` | 5,425,809 |
| `tradeability_state_daily.source_priority = ohlc_fallback` | 6,229,500 |
| official tradeability date range | 2022-01-04 to 2026-04-28 |
| duplicate tradeability keys | 0 |
| `unresolved_adj_factor_jump_rows` | 920 |

Residual corporate-action triage:

- `322` unresolved jumps are `<=50bp`; `288` are `>10pct`.
- Exact same-date raw dividend explains only `1` unresolved row, and that row is
  `div_proc='预案'`, so it should not be booked as an implemented action.
- `84` unresolved rows have an implemented `raw_dividend` within `±5` calendar
  days. Treat these as date-alignment candidates, not resolved entries.
- `581` unresolved rows have no raw dividend within `±30` calendar days and
  remain hard unresolved corporate-action/source-history diagnostics.

Interpretation:

- 2022 through current-day tradeability is now official-first instead of OHLC
  fallback-first.
- 2014-2021 still requires staged `stk_limit` / `suspend_d` before full-window
  tradeability realism can be claimed.
- Because quarterly event windows still take roughly 15-20 minutes mostly in
  staging writes, 2021 and earlier should be processed monthly or quarterly
  with low-frequency polling and rebuilds after meaningful batches.

### 2026-04-28 Batch 1 2021 Official Tradeability Backfill

Completed:

- Backfilled `raw_stk_limit` and `raw_suspend_d` for the full 2021 calendar
  year in quarterly append windows:
  - 2021 Q4: `346,005` `raw_stk_limit`, `2,631` `raw_suspend_d`
  - 2021 Q3: `348,816` `raw_stk_limit`, `3,005` `raw_suspend_d`
  - 2021 Q2: `314,087` `raw_stk_limit`, `2,684` `raw_suspend_d`
  - 2021 Q1: `292,056` `raw_stk_limit`, `2,555` `raw_suspend_d`
- Rebuilt `output/research_source.duckdb` with the updated supplemental DB.
- Re-ran `audit-market-data-quality`; unresolved adjustment-factor jumps remain
  `920`, confirming again that tradeability coverage and corporate-action
  residuals are separate risk tracks.
- Extended `market_data_quality` output so the persisted JSON audit now includes
  unresolved jump proximity to staged `raw_dividend` records.

Observed counts after rebuild:

| Surface | Rows / Coverage |
| --- | ---: |
| `raw_stk_limit` | 8,304,608 |
| `raw_suspend_d` | 51,062 |
| `tradeability_state_daily` | 11,655,309 |
| `tradeability_state_daily.source_priority = official` | 6,487,407 |
| `tradeability_state_daily.source_priority = ohlc_fallback` | 5,167,902 |
| official tradeability date range | 2021-01-04 to 2026-04-28 |
| duplicate tradeability keys | 0 |
| `unresolved_adj_factor_jump_rows` | 920 |

Residual corporate-action proximity now persisted in the audit:

- same-date non-implemented raw dividend only: `1` row
- implemented `raw_dividend` within `±5` calendar days: `84` rows
- implemented `raw_dividend` within `±30` calendar days, excluding the `±5`
  bucket: `254` rows
- no implemented `raw_dividend` within `±30` calendar days: `581` rows

Interpretation:

- 2021 through current-day tradeability is now official-first instead of OHLC
  fallback-first.
- 2014-2020 still requires staged `stk_limit` / `suspend_d` before full-window
  tradeability realism can be claimed.
- The `84` near-date dividend candidates should not be auto-resolved until a
  tested date-alignment rule proves that the adjustment-factor effective date
  and `raw_dividend.ex_date` mismatch is systematic and safe to book.
- The `581` no-nearby-dividend rows remain hard
  `unresolved_corporate_action` diagnostics.

### 2026-04-29 Batch 1 2020-2014 Official Tradeability Backfill

Completed:

- Backfilled `raw_stk_limit` and `raw_suspend_d` for the full `2020-01-01`
  through `2014-01-01` window in quarterly append windows, one writer process
  at a time.
- Used low-frequency polling during the long Tushare and DuckDB write loop; the
  run log is `output/logs/tradeability_backfill_2020_2014_20260429_011334.log`.
- Rebuilt `output/research_source.duckdb` with the updated supplemental DB.
- Re-ran `audit-market-data-quality` and wrote
  `output/audits/market_data_quality_20260429.json`.

Backfilled rows by calendar year:

| Year | `raw_stk_limit` | `raw_suspend_d` |
| --- | ---: | ---: |
| 2020 | 1,176,519 | 9,418 |
| 2019 | 998,531 | 5,752 |
| 2018 | 856,589 | 42,976 |
| 2017 | 798,651 | 60,244 |
| 2016 | 704,154 | 64,668 |
| 2015 | 665,312 | 99,057 |
| 2014 | 619,566 | 51,514 |
| Total | 5,819,322 | 333,629 |

Observed counts after rebuild:

| Surface | Rows / Coverage |
| --- | ---: |
| `raw_stk_limit` | 14,123,930 |
| `raw_suspend_d` | 384,691 |
| `tradeability_state_daily` | 11,655,309 |
| `tradeability_state_daily.source_priority = official` | 11,653,846 |
| `tradeability_state_daily.source_priority = ohlc_fallback` | 1,463 |
| official tradeability date range | 2014-01-02 to 2026-04-28 |
| duplicate tradeability keys | 0 |
| `unresolved_adj_factor_jump_rows` | 920 |

Residual corporate-action proximity remains unchanged:

- same-date non-implemented raw dividend only: `1` row
- implemented `raw_dividend` within `+/-5` calendar days: `84` rows
- implemented `raw_dividend` within `+/-30` calendar days, excluding the `+/-5`
  bucket: `254` rows
- no implemented `raw_dividend` within `+/-30` calendar days: `581` rows

Residual tradeability fallback triage:

- `1,366` fallback rows are `001914.SZ` from `2014-01-02` through
  `2019-12-13`; staged `raw_stk_limit` for that code begins on `2019-12-16`.
- The remaining fallback rows are sparse Beijing-board / old-transfer rows on
  `2020-09-18` and `2021-08-26`.
- `1,457` fallback rows have unadjusted OHLC and `6` have `qfq_fallback`
  price basis.

Interpretation:

- The large 2014-2020 official tradeability coverage blocker is closed.
- Full-window research can now run official-first tradeability diagnostics, but
  promotion evidence must still report or exclude
  `source_priority = 'ohlc_fallback'` rows.
- Tradeability backfill did not change the `920` unresolved adjustment-factor
  jumps; the next risk track is still corporate-action reconciliation, not more
  `stk_limit` / `suspend_d` annual backfill.

### 2026-04-29 Batch 1 Corporate-Action Bar-Window Reconciliation

Completed:

- Replaced same-day-only adjustment-factor reconciliation with a bar-window
  rule: a company action explains a factor jump when its `ex_date` / `book_date`
  falls in `(previous available bar date, current bar date]` for that security.
- Applied the same rule to portfolio-backtester
  `unresolved_corporate_action` diagnostics so suspension/no-bar gaps do not
  create false unresolved events on the first later bar.
- Added persisted unresolved examples to
  `output/audits/market_data_quality_20260429.json` so the remaining queue can
  be reviewed security-by-security rather than as an aggregate count.

Observed counts after re-running the audit:

| Surface | Rows / Coverage |
| --- | ---: |
| significant `adj_factor` jumps | 35,814 |
| explained by corporate-action bar window | 35,692 |
| `unresolved_adj_factor_jump_rows` | 122 |
| `unresolved_adj_factor_jump_rows >10pct` | 52 |

Residual corporate-action proximity under the bar-window rule:

- same-date non-implemented raw dividend only: `1` row
- implemented `raw_dividend` within `+/-30` calendar days: `6` rows
- no implemented `raw_dividend` within `+/-30` calendar days: `115` rows

Interpretation:

- The old `920` residual queue contained many false positives where a stock had
  no bar on the company-action date, usually because of suspension; the factor
  update appeared on the next available bar.
- The remaining `122` rows are a much smaller hard residual queue. They should
  stay as `unresolved_corporate_action` diagnostics until another official
  source or a reviewed source-specific rule explains them.
- Strategy promotion evidence may use the improved audit, but must still report
  or exclude these residual securities/windows.

### 2026-04-29 Batch 1 Corporate-Action Exception Ledger

Completed:

- Added `corporate_action_exception_ledger` to
  `build-research-source-db`.
- The table materializes the remaining unresolved `adj_factor` jumps as
  explicit security/date quarantine windows instead of leaving them only in a
  JSON audit.
- Extended `audit-market-data-quality` with:
  - `promotion_blocking_unresolved_adj_factor_jump_rows`
  - unresolved triage buckets
  - per-example `has_suspend_window`, `factor_pre_close_basis_diff`,
    `triage_class`, and `recommended_action`
- Rebuilt `output/research_source.duckdb` and refreshed
  `output/audits/market_data_quality_20260429.json`.

Observed counts:

| Surface | Rows / Coverage |
| --- | ---: |
| `corporate_action_exception_ledger` | 122 |
| `promotion_blocking_unresolved_adj_factor_jump_rows` | 122 |
| exception date range | 2014-01-14 to 2025-08-22 |

Exception triage:

| Triage class | Rows | Securities |
| --- | ---: | ---: |
| `implemented_dividend_outside_factor_window` | 6 | 6 |
| `nonimplemented_dividend_same_date` | 1 | 1 |
| `daily_pre_close_ex_right_without_ledger` | 100 | 94 |
| `low_materiality_provider_factor_noise` | 5 | 1 |
| `provider_factor_jump_without_event_evidence` | 10 | 4 |

Severity:

| Severity | Rows | Securities |
| --- | ---: | ---: |
| `low` | 7 | 3 |
| `medium` | 5 | 4 |
| `high` | 58 | 47 |
| `critical` | 52 | 51 |

Interpretation:

- `100` of the `122` residuals are supported by the daily-bar `pre_close`
  ex-right basis, and many also occur across suspension/resumption windows.
  This explains why PIT adjusted returns are economically continuous.
- They still cannot be booked into the raw-OHLC broker ledger without an
  official cash/share event source. Treating the factor ratio itself as a
  share split would silently invent broker cash/share accounting.
- The professional handling is therefore quarantine: any strategy evidence that
  holds or labels a security across one of these windows must report and
  exclude that exposure unless a later official source-backed rule resolves it.

### 2026-04-29 Batch 1 Promotion Evidence Exception Gate

Completed:

- Added replay-case fields `market_data_source_db_path` and
  `market_data_quality_audit_path`.
- `load_portfolio_promotion_replay_case` now reads
  `corporate_action_exception_ledger` from the bound research-source DuckDB.
- `PortfolioPromotionReplay` now reports baseline/candidate decision-interval
  overlaps with corporate-action exception windows under
  `research_evidence.market_data_quality`.
- A replay with any exception exposure fails the promotion decision with
  `corporate_action_exception_exposure`.
- `build-multi-year-validation-audit` persists
  `corporate_action_exception_exposure_count` and blocks signal release when
  the count is nonzero.
- `run-portfolio-backtest` now reports actual daily holdings that cross
  `corporate_action_exception_ledger` windows under
  `diagnostics.corporate_action_exception_exposures`, and summary exposes the
  count.
- Bound the real-output and deployment replay cases to
  `output/research_source.duckdb` plus
  `output/audits/market_data_quality_20260429.json`.
- Added deployment CLI coverage proving `build-executable-signal` fails closed
  when the bound live-candidate multi-year audit reports corporate-action
  exception exposure.

Observed result:

- Rebuilt `trend_leadership_multi_year_validation_audit_v1.json`.
- The frozen `trend_leadership_shadow_live_v1` validation replay loads all
  `122` exception windows and reports `0` overlapping exposures. This keeps the
  corporate-action exception gate clean, but does not by itself make the audit
  release-ready.

### 2026-04-29 Batch 2 Research-Generation Exception Gate

Completed:

- `build-trend-research-input` now reads
  `corporate_action_exception_ledger` from the bound research-source DuckDB and
  excludes candidate observations whose trend feature/label interval intersects
  an exception window.
- `build-fundamental-research-input` now reads the same ledger and excludes
  candidate observations whose decision-to-exit label interval intersects an
  exception window.
- Both builders emit
  `corporate_action_exception_quarantine_excluded_count=<n>` in warnings when
  rows are removed by this gate.
- Added regression tests proving the top-ranked synthetic trend and
  fundamental candidates are removed when their label window crosses a
  quarantined exception.

Design constraint:

- These rows remain blocked data-quality windows, not inferred corporate
  actions. The builders remove contaminated research observations; they do not
  add cash dividends, split ratios, or share adjustments from the factor jump.

### 2026-04-29 Portfolio Evidence Fallback Gate

Completed:

- `run-portfolio-backtest` now carries `price_basis` and
  `tradeability_state_daily.source_priority` into order/holding diagnostics.
- The backtest summary now exposes:
  - `qfq_fallback_price_exposure_count`
  - `tradeability_fallback_exposure_count`
  - `market_data_fallback_exposure_count`
- `build-multi-year-validation-audit` can bind a persisted
  `portfolio_backtest_result_path` and blocks signal release on any qfq price
  fallback, tradeability fallback, or corporate-action exception exposure in
  the attached daily backtest.
- `audit-market-data-quality` now persists official versus OHLC-fallback
  tradeability row counts.
- Rebuilt the generated trend input, sleeve artifact, overlay observations,
  daily backtests, market-data audit, and multi-year validation audit.

Observed result:

- `build-trend-research-input` excluded `373` candidates through
  `corporate_action_exception_quarantine_excluded_count`.
- The first rebuilt daily portfolio evidence still touched `218` qfq-fallback
  price rows, so qfq fallback was promoted from a diagnostic to a hard release
  blocker.
- Research-input generation now also excludes qfq-fallback windows; the latest
  trend input excluded `5,663` candidates through
  `qfq_fallback_quarantine_excluded_count`.
- The frozen replay reports `0` corporate-action exception exposures.
- Both trend-only and overlay daily backtests report:
  - `corporate_action_exception_exposure_count`: `0`
  - `qfq_fallback_price_exposure_count`: `0`
  - `tradeability_fallback_exposure_count`: `0`
- The checked-in `trend_leadership_multi_year_validation_audit_v1.json` now
  reports no corporate-action-exception, qfq-fallback, or
  tradeability-fallback portfolio exposures; the audit-level data-quality gate
  passes for the frozen candidate.
- The promotion replay still fails strategy-quality gates including OOS IR,
  OOS t-stat, peak-to-trough drawdown, realized-versus-budget, and marginal IR.

Interpretation:

- The 122 corporate-action residual windows are now wired through research
  generation, replay, backtest diagnostics, and live-release gates.
- The qfq-fallback and tradeability-fallback hard gates are now wired through
  research generation, daily backtest diagnostics, and the multi-year audit.
- The next gating risk is no longer known market-data contamination for the
  frozen candidate; it is strategy quality and broker realism. The current
  evidence curve has unacceptable loss, drawdown, turnover, and promotion
  metric failures, so it must not be treated as a credible capital candidate.

## Batch Overview

| Batch | Objective | Promotion Impact |
| --- | --- | --- |
| 0 | Reconcile documentation and current build state | Removes stale instructions that still describe adjusted-price fills |
| 1 | Materialize Tushare event staging and data-quality audit | Proves the new corporate-action and tradeability surfaces exist in real data |
| 2 | Harden the daily backtest ledger | Makes portfolio PnL closer to broker-account accounting |
| 3 | Close alpha/risk-model contract gaps | Prevents strategy evidence from being mostly industry, size, or beta exposure |
| 4 | Add strategy-generation guardrails | Makes generated strategies obey V2 object and evidence constraints |
| 5 | Build shadow-live and monitoring evidence | Separates backtest credibility from live-operational readiness |

## Non-Negotiable Constraints

- Do not mutate the V1 DuckDB.
- Do not port V1 `factor -> strategy -> promotion` logic.
- Do not add dependencies.
- Do not treat qfq fallback rows as PIT adjusted-price truth.
- Do not let strategy generation optimize bare returns without costs, tradeability, turnover, and drawdown evidence.
- Do not admit a generated sleeve unless it emits a persisted sleeve artifact on the portfolio decision calendar.

---

## Batch 0: Documentation Reconciliation

**Purpose:** make the written operating contract match the new raw-price corporate-action architecture before further implementation.

**Files:**

- Modify: `docs/operations/trend-leadership-live-candidate-v1.md`
- Modify: `docs/audit/quantitative-finance-audit-2026-04-28.md`
- Modify: `docs/data/v1-duckdb-reuse-audit.md`
- Reference: `docs/superpowers/plans/2026-04-28-a-share-pit-adjustment-corporate-actions.md`

- [ ] **Step 1: Replace stale adjusted-fill wording**

  In `docs/operations/trend-leadership-live-candidate-v1.md`, replace the current statement that says historical proof uses adjusted daily OHLC for fills and marks with:

  ```markdown
  - before shadow-live, historical performance must be evidenced with
    `run-portfolio-backtest`, using raw unadjusted `daily_bar_pit` OHLC for
    fills, marks, and price-limit diagnostics, plus explicit staged corporate
    actions for cash and share adjustments; `run-promotion-replay` remains a
    candidate comparison and promotion-gate tool, not a real equity curve
  ```

- [ ] **Step 2: Add a cross-reference to this roadmap**

  Add this sentence near the current blocker list in `docs/operations/trend-leadership-live-candidate-v1.md`:

  ```markdown
  The remaining credibility work is tracked in
  `docs/superpowers/plans/2026-04-28-trusted-backtest-strategy-generation-risk-roadmap.md`.
  ```

- [ ] **Step 3: Verify no stale adjusted-fill language remains**

  Run:

  ```bash
  rg -n "adjusted daily OHLC|adjusted .*fills|qfq.*truth" docs src
  ```

  Expected: no remaining production guidance says portfolio backtests use adjusted OHLC for fills or marks. Mentions that qfq is not truth are allowed.

- [ ] **Step 4: Verify markdown whitespace**

  Run:

  ```bash
  git diff --check
  ```

  Expected: exit code `0`.

**Stop condition:** if any operations or architecture document still instructs an agent to fill or mark with adjusted OHLC, stop and fix the documentation before implementing later batches.

---

## Batch 1: Real Data Materialization And Data-Quality Audit

**Purpose:** prove the PIT adjustment and corporate-action code is not only unit-tested, but also materialized and measurable on real local Tushare-derived data.

**Files:**

- Modify: `src/alpha_find_v2/market_data_bootstrap.py`
- Modify: `src/alpha_find_v2/reference_data_staging.py`
- Modify: `src/alpha_find_v2/cli.py`
- Create: `src/alpha_find_v2/market_data_quality.py`
- Create: `tests/test_market_data_quality.py`
- Modify: `docs/data/v1-duckdb-reuse-audit.md`

- [ ] **Step 1: Run the staging chain on real data**

  Run the current CLI without changing code:

  ```bash
  PYTHONPATH=src python3 -m alpha_find_v2 build-reference-staging-db
  PYTHONPATH=src python3 -m alpha_find_v2 build-research-source-db
  ```

  Expected: both commands exit `0`; the resulting research source contains `corporate_action_ledger` and `tradeability_state_daily`.

- [x] **Step 2: Inspect required table coverage**

  Run:

  ```bash
  python3 - <<'PY'
  import duckdb

  checks = [
      ("output/pit_reference_staging.duckdb", "raw_dividend"),
      ("output/pit_reference_staging.duckdb", "raw_stk_limit"),
      ("output/pit_reference_staging.duckdb", "raw_suspend_d"),
      ("output/pit_reference_staging.duckdb", "raw_share_float"),
      ("output/pit_reference_staging.duckdb", "raw_repurchase"),
      ("output/research_source.duckdb", "corporate_action_ledger"),
      ("output/research_source.duckdb", "tradeability_state_daily"),
  ]

  for db_path, table in checks:
      con = duckdb.connect(db_path, read_only=True)
      exists = con.execute(
          "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
          [table],
      ).fetchone()[0]
      if not exists:
          raise SystemExit(f"missing {db_path}.{table}")
      count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
      print(f"{db_path}.{table}: {count}")
      con.close()
  PY
  ```

  Expected: all tables exist. Empty `raw_*` event tables are allowed only if the run documents the Tushare response and date range; derived tables must still exist.

- [x] **Step 3: Add a data-quality summary module**

  Create `src/alpha_find_v2/market_data_quality.py` with functions that compute these metrics from `output/research_source.duckdb`:

  ```python
  from __future__ import annotations

  from dataclasses import dataclass
  from pathlib import Path

  import duckdb


  @dataclass(frozen=True)
  class MarketDataQualitySummary:
      daily_bar_rows: int
      qfq_fallback_rows: int
      missing_price_rows: int
      zero_or_missing_adj_factor_rows: int
      corporate_action_rows: int
      tradeability_rows: int
      unresolved_adj_factor_jump_rows: int


  def summarize_market_data_quality(db_path: Path | str) -> MarketDataQualitySummary:
      with duckdb.connect(str(db_path), read_only=True) as conn:
          return MarketDataQualitySummary(
              daily_bar_rows=_count(conn, "daily_bar_pit"),
              qfq_fallback_rows=_scalar(
                  conn,
                  "SELECT count(*) FROM daily_bar_pit WHERE price_basis = 'qfq_fallback'",
              ),
              missing_price_rows=_scalar(
                  conn,
                  """
                  SELECT count(*)
                  FROM daily_bar_pit
                  WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
                  """,
              ),
              zero_or_missing_adj_factor_rows=_scalar(
                  conn,
                  "SELECT count(*) FROM daily_bar_pit WHERE adj_factor IS NULL OR adj_factor <= 0",
              ),
              corporate_action_rows=_count(conn, "corporate_action_ledger"),
              tradeability_rows=_count(conn, "tradeability_state_daily"),
              unresolved_adj_factor_jump_rows=_unresolved_adj_factor_jump_rows(conn),
          )


  def _count(conn: duckdb.DuckDBPyConnection, table: str) -> int:
      exists = conn.execute(
          "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
          [table],
      ).fetchone()[0]
      if not exists:
          return 0
      return _scalar(conn, f"SELECT count(*) FROM {table}")


  def _scalar(conn: duckdb.DuckDBPyConnection, sql: str) -> int:
      return int(conn.execute(sql).fetchone()[0] or 0)


  def _unresolved_adj_factor_jump_rows(conn: duckdb.DuckDBPyConnection) -> int:
      if not _count(conn, "corporate_action_ledger"):
          return 0
      return _scalar(
          conn,
          """
          WITH jumps AS (
              SELECT
                  security_id,
                  trade_date,
                  adj_factor / NULLIF(
                      lag(adj_factor) OVER (PARTITION BY security_id ORDER BY trade_date),
                      0
                  ) AS factor_ratio
              FROM daily_bar_pit
              WHERE price_basis = 'unadjusted'
          )
          SELECT count(*)
          FROM jumps AS j
          LEFT JOIN corporate_action_ledger AS c
            ON c.security_id = j.security_id
           AND c.action_date = j.trade_date
          WHERE j.factor_ratio IS NOT NULL
            AND abs(j.factor_ratio - 1.0) > 0.001
            AND c.security_id IS NULL
          """,
      )
  ```

- [x] **Step 4: Add tests for missing tables and normal summaries**

  Create `tests/test_market_data_quality.py` with a temporary DuckDB fixture that:

  - creates `daily_bar_pit`
  - creates `corporate_action_ledger`
  - creates `tradeability_state_daily`
  - asserts qfq fallback, missing price, missing adjustment factor, and unresolved jump counts

  Run:

  ```bash
  PYTHONPATH=src python3 -m unittest tests.test_market_data_quality -v
  ```

  Expected: all tests pass.

- [x] **Step 5: Add a CLI command for the summary**

  Add `audit-market-data-quality` to `src/alpha_find_v2/cli.py` with arguments:

  ```bash
  --source-db output/research_source.duckdb
  --output output/audits/market_data_quality_YYYYMMDD.json
  ```

  The command should write the dataclass as JSON and print the output path.

- [x] **Step 6: Run full verification**

  Run:

  ```bash
  PYTHONPATH=src python3 -m unittest discover -s tests -v
  PYTHONPATH=src python3 -m pytest -q
  git diff --check
  ```

  Expected: all commands exit `0`.

**Stop condition:** if real data produces many unresolved adjustment-factor jumps or missing official tradeability coverage, do not tune strategy parameters. First document the affected date/security coverage and either fix staging or mark the data window as non-promotable.

---

## Batch 2: Broker-Like Backtest Ledger

**Purpose:** make the daily portfolio backtester closer to actual A-share cash-account behavior.

**Files:**

- Modify: `src/alpha_find_v2/portfolio_backtester.py`
- Modify: `tests/test_portfolio_backtester.py`
- Modify: `src/alpha_find_v2/models.py`
- Modify: `config/cost_models/base_a_share_cash.toml`
- Modify: `config/cost_models/high_a_share_cash.toml`

- [ ] **Step 1: Add failing test for T+1 available shares**

  Add a test to `tests/test_portfolio_backtester.py` that buys `AAA` on one next-open execution date and attempts to sell those same shares before they become available. Expected behavior:

  - position shares exist
  - `available_shares` remains below total shares until the next eligible trading day
  - sell order is blocked with reason `t_plus_one_unavailable`

  Run:

  ```bash
  PYTHONPATH=src python3 -m unittest tests.test_portfolio_backtester.PortfolioBacktesterTest.test_t_plus_one_available_shares_block_same_day_sale -v
  ```

  Expected before implementation: fail because the backtester does not expose an available-share ledger.

- [ ] **Step 2: Implement available-share state**

  In `src/alpha_find_v2/portfolio_backtester.py`, add an internal ledger:

  ```python
  available_shares_by_asset: dict[str, float]
  pending_available_shares_by_date: dict[str, dict[str, float]]
  ```

  Booking rule:

  - buy fill increases total shares immediately
  - bought shares become available on the next trade date after fill date
  - sell fill cannot exceed available shares
  - share dividends increase both total shares and available shares only for eligible record-date holdings

- [ ] **Step 3: Add initial-holding corporate-action eligibility**

  Add a test where the backtest starts after the corporate-action record date but before `pay_date` or `ex_date`, with an initial holding present. Expected behavior:

  - cash dividend is booked if initial holdings were eligible through explicit case input
  - without explicit eligibility input, the action is diagnosed rather than guessed

  This prevents a hidden fallback from inventing record-date ownership.

- [ ] **Step 4: Add volume-sensitive slippage**

  Extend `CostModel` with optional fields:

  ```python
  impact_multiplier = 0.0
  spread_proxy_multiplier = 0.0
  ```

  Keep existing configs behavior-preserving by setting both to `0.0`. Add tests that prove a larger `order_value / turnover_value_cny` increases slippage only when `impact_multiplier > 0.0`.

- [ ] **Step 5: Verify portfolio tests**

  Run:

  ```bash
  PYTHONPATH=src python3 -m unittest tests.test_portfolio_backtester -v
  PYTHONPATH=src python3 -m pytest -q
  ```

  Expected: all tests pass.

**Stop condition:** if T+1 available-share modeling conflicts with the current next-open rebalance clock, keep the clock unchanged and expose the conflict as diagnostics. Do not silently allow same-day resale of new shares.

---

## Batch 3: Alpha And Risk-Model Contract Closure

**Purpose:** prevent strategy evidence from being an uncontrolled blend of market beta, industry rotation, size, and raw momentum.

**Files:**

- Modify: `src/alpha_find_v2/trend_research_input_builder.py`
- Modify: `tests/test_trend_research_input_builder.py`
- Modify: `src/alpha_find_v2/risk_model.py`
- Modify: `tests/test_risk_model.py`
- Modify: `config/sleeves/*.toml`
- Modify: `docs/architecture/risk-model-and-simulation-loop.md`

- [ ] **Step 1: Add explicit trend industry-neutral scoring switch**

  Add a config field to the trend input build case:

  ```toml
  industry_neutral_scoring = true
  ```

  In tests, create two industries with different raw momentum means and assert that z-scores are computed within industry when the switch is true.

- [ ] **Step 2: Refuse unsupported `Sleeve.neutralization` claims**

  Add validation that maps sleeve `neutralization` entries to implemented controls:

  - `industry`: implemented only when input scoring or constructor budget uses PIT industry
  - `size`: implemented only when risk exposure data exists
  - `beta`: implemented only when risk exposure data exists

  If a promotion-safe sleeve declares `size` or `beta` without a risk exposure snapshot, fail the build with a direct error message.

- [ ] **Step 3: Add residual-snapshot coverage reporting**

  Extend the risk-model residualization path to report:

  - required component count
  - covered `(trade_date, asset_id)` count
  - missing component rows
  - missing exposure rows

  The report should be written beside the generated artifact so promotion review can see whether residualization was real.

- [ ] **Step 4: Verify risk and trend tests**

  Run:

  ```bash
  PYTHONPATH=src python3 -m unittest tests.test_trend_research_input_builder tests.test_risk_model -v
  PYTHONPATH=src python3 -m pytest -q
  ```

  Expected: all tests pass.

**Stop condition:** do not mark a sleeve as industry/size/beta neutral if the implementation only caps final portfolio weights. Either implement the control or downgrade the claim.

---

## Batch 4: Strategy-Generation Guardrails

**Purpose:** allow strategy generation only inside the V2 research object chain and only with executable evidence.

**Files:**

- Create: `src/alpha_find_v2/strategy_generation_guardrails.py`
- Create: `tests/test_strategy_generation_guardrails.py`
- Modify: `src/alpha_find_v2/cli.py`
- Create: `docs/architecture/strategy-generation-guardrails.md`

- [ ] **Step 1: Define the generated-strategy manifest**

  Create a manifest schema represented with dataclasses:

  ```python
  @dataclass(frozen=True)
  class GeneratedStrategyManifest:
      mandate_id: str
      thesis_id: str
      descriptor_set_id: str
      sleeve_id: str
      target_id: str
      portfolio_recipe_id: str
      cost_model_id: str
      evidence_paths: list[str]
      rejected_objectives: list[str]
  ```

  The manifest is valid only if it references a persisted sleeve artifact and a portfolio backtest case.

- [ ] **Step 2: Reject bare-return optimization**

  Add tests that pass manifests with objectives such as `gross_return_only`, `ignore_costs`, or `ignore_tradeability`. Expected behavior: validation fails and records the rejected objective.

- [ ] **Step 3: Require executable evidence paths**

  Validation must require all of:

  - sleeve artifact path
  - daily portfolio backtest path or case path
  - market-data quality audit path
  - promotion replay path if the strategy is a candidate admission rather than a single-sleeve research run

- [ ] **Step 4: Add CLI validation command**

  Add:

  ```bash
  PYTHONPATH=src python3 -m alpha_find_v2 validate-generated-strategy --manifest path/to/manifest.json
  ```

  Expected output: JSON with `status`, `errors`, and `evidence_paths`.

- [ ] **Step 5: Verify generated-strategy guardrails**

  Run:

  ```bash
  PYTHONPATH=src python3 -m unittest tests.test_strategy_generation_guardrails -v
  PYTHONPATH=src python3 -m pytest -q
  ```

  Expected: all tests pass.

**Stop condition:** if a generated strategy cannot produce a persisted sleeve artifact and portfolio backtest case, it remains exploratory and cannot enter promotion replay.

---

## Batch 5: Shadow-Live Evidence And Monitoring

**Purpose:** prove that the historical backtest assumptions survive the operational path before any small-capital probation.

**Files:**

- Modify: `src/alpha_find_v2/deployment.py`
- Modify: `src/alpha_find_v2/live_readiness.py`
- Modify: `tests/test_live_readiness.py`
- Modify: `docs/operations/trend-leadership-live-candidate-v1.md`
- Modify: `docs/operations/trend-leadership-paper-trade-signal-policy.md`

- [ ] **Step 1: Add stale-data guard**

  Before building an executable signal, check the latest `daily_bar_pit.trade_date` against the expected research date. If data is older than one trading session, fail readiness with `stale_market_data`.

- [ ] **Step 2: Add runtime ST and delisting guard**

  Use `security_master_ref` and name-change/ST state to block new entries and flag forced exits for:

  - ST names when mandate `exclude_st = true`
  - delisted or delisting-effective names
  - suspended names when official tradeability data says suspended

- [ ] **Step 3: Require a 12-cycle shadow-live journal**

  Extend live readiness to count consecutive weekly cycles in:

  ```text
  research/examples/deployment_minimal/shadow_live_journal_trend_leadership_v1.json
  ```

  Small-capital probation remains blocked until the journal has at least `12` consecutive valid cycles.

- [ ] **Step 4: Add realized slippage comparison**

  For each manual or paper-trade execution outcome, compare:

  - expected next-open price
  - actual execution price if available
  - model slippage bps
  - realized slippage bps

  Flag `slippage_model_underestimated` when realized slippage exceeds model slippage by a configured threshold.

- [ ] **Step 5: Verify live-readiness tests**

  Run:

  ```bash
  PYTHONPATH=src python3 -m unittest tests.test_live_readiness -v
  PYTHONPATH=src python3 -m pytest -q
  ```

  Expected: all tests pass.

**Stop condition:** do not move from shadow-live to small-capital probation if the system cannot explain stale data, ST/delisting exposure, or realized-vs-modeled slippage.

---

## Final Promotion Gate For This Roadmap

The system is credible enough for strategy-level backtest review when all of these are true:

- real local research DB contains materialized `corporate_action_ledger` and `tradeability_state_daily`
- market-data quality audit has been written and reviewed
- daily portfolio backtester uses raw OHLC, explicit corporate actions, T+1 available-share accounting, participation limits, and documented slippage assumptions
- strategy evidence includes benchmark-relative metrics, turnover, drawdown, cost drag, blocked-trade counts, and unresolved-corporate-action diagnostics
- strategy generation refuses bare-return objectives and emits V2-native manifests
- shadow-live readiness still blocks small-capital probation until operational evidence exists

## Recommended Execution Order

Do not parallelize batches that depend on the output of a previous batch:

1. Batch 0 first, because stale docs can misdirect later agents.
2. Batch 1 second, because later work needs real table coverage and quality metrics.
3. Batch 2 and Batch 3 can proceed in parallel after Batch 1.
4. Batch 4 depends on the evidence fields stabilized by Batches 1 through 3.
5. Batch 5 can start once the backtest and generated-strategy evidence contracts are stable.

## Commit Guidance

Use the Lore Commit Protocol. Suggested commit grouping:

1. `Document the credible backtest risk roadmap`
2. `Materialize market-event quality gates`
3. `Audit T+1 and corporate-action backtest accounting`
4. `Enforce risk-model neutrality claims`
5. `Constrain generated strategy evidence`
6. `Gate shadow-live readiness on operational evidence`

Each commit should include:

```text
Constraint: no new deps; V2 object chain only; raw OHLC is execution truth
Rejected: static qfq as PIT truth | future-adjusted/static-adjustment ambiguity
Tested: <exact commands run>
Not-tested: <live Tushare or broker behavior not exercised>
```
