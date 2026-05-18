# SW Industry PIT Provider Audit Note

## Binding Conclusions

- The SW PIT industry provider-coverage blocker is closed for the frozen
  `trend_leadership` candidate after the `302132.SZ` current-code gap was
  resolved through `security_code_alias_backfill`; a wider `5` to `10` year
  validation window remains the preferred long-run target.
- `Tushare index_classify` is taxonomy metadata only. It does not provide
  per-stock PIT industry history.
- `stock_basic.industry` and similar static industry labels are not acceptable
  as PIT truth and must not be projected backward.
- `Tushare index_member_all` remains the first production-safe candidate source
  for staged `industry_classification_pit`, but the local staged copy can still
  lag the live provider surface if sync logic or interval handling is wrong.
- `AKShare stock_industry_clf_hist_sw()` exposes useful per-stock historical
  change nodes, including rows around the `2014-02-21` and `2021-07-30`
  schema-transition dates. It remains `amber` audit evidence only and is not
  allowed to become production truth without a separate field audit.
- The local Shenwan-research export
  `docs/data/StockClassifyUse_stock.xls` is a strong external change-node
  source. It contains `股票代码`, `计入日期`, `行业代码`, and `更新日期`, with
  `12,773` rows across `5,855` stocks and `553` industry codes, and it
  includes large node clusters on both `2014-02-21` and `2021-07-30`.
- The official local Shenwan 2021 support files materially strengthen that
  conclusion:
  - `docs/data/SwClassCode_2021.xls` provides the `2021` hierarchy code table
    with `511` rows
  - `docs/data/SwClassStd2021.pdf` documents the official `2021-07-30`
    launch date and the old/new taxonomy revision logic
  - `docs/data/2014to2021.xlsx` provides the official `2014 -> 2021`
    taxonomy crosswalk in both change-note and old/new comparison forms
  - `docs/data/最新个股申万行业分类(完整版-截至7月末).xlsx` provides a full
    official cross-section around the `2021-07-30` cutover, including
    `4,430` A-share rows
- `docs/data/2014to2021.xlsx` is taxonomy-transition evidence, not stock-level
  PIT interval evidence. It can explain code migration and alias behavior
  around the `2021-07-30` cutover, but it does not itself provide per-stock
  `in_date` / `out_date`.
- `Baostock query_stock_industry()` is not a valid replacement for this audit
  path. Live checks show it returns `证监会行业分类`, not `SW2021`, and the
  returned `updateDate` is a weekly snapshot date rather than a daily PIT
  interval surface.
- A `SW2021` row with an early `in_date` is useful continuity evidence for a
  specific stock, but it is not blanket proof that the entire pre-2021 history
  is PIT-complete. The provider surfaces still need cross-audit.
- Extra Tushare points may unlock complementary slow fields, but they do not by
  themselves prove that `5` to `10` years of PIT industry truth is already
  complete for the benchmark universe.

## One-Shot Audit Decision

The approved next step is a one-shot reconciliation script that compares three
surfaces for the same benchmark-member trading days:

1. local staged `industry_classification_pit`
2. live `Tushare index_member_all` fetched from both `is_new='N'` and
   `is_new='Y'`
3. `AKShare stock_industry_clf_hist_sw()` as an `amber` audit source

The script classifies missing industry coverage into four concrete buckets:

- `staging_sync_gap`
- `staging_end_date_boundary`
- `tushare_provider_gap_akshare_covers`
- `unresolved_gap`

This keeps the next decision honest:

- if live Tushare covers the date but staging does not, fix staging / sync
- if only inclusive end-date handling fixes the gap, fix interval semantics
- if Tushare still misses but AKShare covers, treat it as a provider-surface gap
- if both miss, the history is still unresolved and cannot be promoted

Important comparison limit:

- `AKShare` and `Tushare` are not assumed to share the same industry-code
  vocabulary in this audit. The one-shot script uses `AKShare` only as
  stock-date coverage evidence, not as direct code-equality truth.
- The Shenwan-research export does not expose explicit `out_date` rows. It is
  still useful because consecutive `计入日期` values can define change nodes,
  but interval end points must be derived conservatively.
- The Shenwan-research export also needs two explicit guards before it can
  participate in any production-truth path:
  - `2,173` rows carry non-midnight timestamps in `计入日期`, so same-day use
    would need an explicit `available_at` policy
  - `12` rows use a `1990-01-01` placeholder `计入日期`, so those left-edge
    starts are not automatically PIT-safe
- The Shenwan official packet is now strong enough to support a stricter
  conclusion:
  - using `StockClassifyUse_stock.xls`, every A-share row in the official
    `截至7月末` snapshot can be reconstructed by taking the latest node with
    `计入日期 <= 2021-07-30`
  - this yields `100%` stock-level coverage (`4,430 / 4,430`)
  - after normalizing one lowercase snapshot code (`300957.sz ->
    300957.SZ`), raw `行业代码` equality is `95.5079%` (`4,231 / 4,430`)
  - the new official crosswalk file `2014to2021.xlsx` explains most of the
    remaining raw-code disagreement:
    - `199` raw mismatches remain after the case normalization
    - `168` of those `199` mismatches come from the July-end official snapshot
      still using eight legacy codes not present in `SwClassCode_2021.xls`:
      `280102`, `280103`, `420101`, `610103`, `610203`, `620501`, `640301`,
      `640401`
    - all eight legacy codes are present on the old-code side of
      `2014to2021.xlsx`, while the paired `StockClassifyUse_stock.xls` values
      are valid `SW2021` codes
    - that makes those `168` rows official transition-alias evidence rather
      than random cross-file disagreement
    - if those legacy-code rows are normalized as cutover aliases, effective
      code-level agreement rises to `99.3002%` (`4,399 / 4,430`)
  - after decoding codes into hierarchy names with `SwClassCode_2021.xls`,
    the agreement rises to about `99.93%` at L1, `99.89%` at L2, and `99.16%`
    at L3
  - the residual unresolved set is now small and concrete:
    - `31` rows remain after removing official transition-alias mismatches
    - `18` are cement names with snapshot `610102` vs stock-file `610101`
    - `9` are glass names with snapshot `610202` vs stock-file `610201`
    - `4` are single-name outliers:
      `300395.SZ`, `002920.SZ`, `605300.SH`, `605339.SH`
  - the residual set still leans in favor of the change-node ledger:
    - for `29` of the `31` rows, `StockClassifyUse_stock.xls` never uses the
      snapshot code anywhere in that stock's full history
    - for the remaining `2` rows (`605300.SH`, `605339.SH`),
      `StockClassifyUse_stock.xls` explicitly switches away from snapshot code
      `340404` on `2021-07-30`
    - this makes the residual set look more like a stale / mixed snapshot
      artifact than a defect in the official change-node ledger
- The remaining blocker is therefore no longer "do we have an official SW
  source?" but "how do we operationalize it safely?":
  - normalize official code aliases across the packet
  - quarantine the small residual outlier set
  - define `available_at` for intraday `计入日期` timestamps
  - derive interval end points conservatively from the next `计入日期`
- The practical finance conclusion is now narrower and stronger:
  - for slow-moving uses such as benchmark grouping, industry exposure,
    neutralization, or the current `sw2021_l1` trend sleeve, the official
    Shenwan packet is strong enough to support a conservative derived PIT layer
  - it is still not enough to claim exact same-day reclassification-event
    truth without explicit `out_date` derivation and `available_at` rules
- `Baostock` may still be useful as an external sanity check for slow-moving
  industry continuity at the `证监会行业分类` level, but it cannot close
  `SW2021` PIT gaps and is therefore outside the primary reconciliation chain.

## Derived Bridge-Fill Policy

The approved compromise for residual live-provider gaps is now encoded as a
derived audit layer, not as a mutation of raw PIT truth.

A residual gap run may be marked `eligible_bridge_fill` only if all of the
following hold:

- `live_tushare_inclusive` is false for the entire run
- live `Tushare` shows the same industry code on both sides of the gap
- the missing interval does not cross `2014-02-21` or `2021-07-30`
- `AKShare` does not show multiple historical codes inside the gap

Blocked outcomes are explicit:

- `blocked_one_sided_gap`
- `blocked_schema_transition`
- `blocked_endpoint_mismatch`
- `blocked_counterevidence`

Important limit:

- this bridge-fill layer is acceptable only as a pragmatic derived series for
  slow-moving uses such as grouping, neutralization, or exposure controls
- it is not acceptable as proof that the raw provider already has complete PIT
  truth
- it is not acceptable for industry-change event research
- long gaps are not capped by length anymore, but they are still blocked if the
  evidence above is not present

## First Run On 2026-04-26

Using the cached/local build plus one live provider pull, the first full-window
run over `2021-04-26` to `2026-04-23` produced:

- `964,800` benchmark constituent-days across `1,177` names
- staged local coverage: `941,573` / `964,800` (`97.59%`)
- live Tushare inclusive coverage: `963,662` / `964,800` (`99.88%`)
- AKShare coverage evidence: `964,524` / `964,800` (`99.97%`)
- classified gaps:
  - `22,089` `staging_sync_gap`
  - `1,138` `tushare_provider_gap_akshare_covers`
  - `0` observed `staging_end_date_boundary`
  - `0` observed `tushare_end_date_boundary`

Interpretation:

- the current checked-in staging copy is still materially under-synced relative
  to live `Tushare index_member_all`
- after moving from local staging to live Tushare, the remaining uncovered
  window is small but not zero
- `AKShare` explains a meaningful residual share of the missing Tushare
  coverage, so the remaining blocker is not only local sync logic

## Bridge-Fill Result On The Same Window

After adding the derived bridge-fill classifier and re-running the same
`2021-04-26` to `2026-04-23` window from cache:

- residual live-provider gap days: `1,138`
- residual provider-gap runs: `10`
- `eligible_bridge_fill`: `0` runs / `0` days
- `blocked_one_sided_gap`: `6` runs / `603` days
- `blocked_schema_transition`: `4` runs / `535` days

Interpretation:

- the newly approved bridge-fill rule does not currently rescue any of the
  remaining `5`-year residual live-provider gaps
- several early-window gaps are open on the left edge, so they remain blocked
  even when `AKShare` shows a stable historical label
- the rest straddle the `2021-07-30` schema-transition boundary, so they remain
  blocked even when the endpoint codes match
- one representative blocked run (`603456.SH`, `2021-12-31` to `2023-07-03`)
  also shows multiple `AKShare` historical codes inside the gap, reinforcing
  that blanket long-gap filling would not be PIT-safe

## Execution Path

Run from repo root:

```bash
PYTHONPATH=src python3 -m alpha_find_v2.sw_industry_pit_audit \
  --start-date 20210426 \
  --end-date 20260423 \
  --output-json output/audits/sw_industry_pit_audit_20210426_20260423.json \
  --output-gap-csv output/audits/sw_industry_pit_gap_rows_20210426_20260423.csv
```

Important operating rules:

- The script caches normalized live-provider intervals under
  `output/audits/cache/` and should reuse them unless a fresh provider pull is
  explicitly required.
- `AKShare` is fetched at most once per refresh. Do not wrap it in per-symbol
  loops.
- If the audit is launched as a background job, inspect the log no more
  frequently than every `30` minutes.

## Superseding Verification On 2026-04-27

After the official L1 rebuild and manual adjudication layer were refreshed into
the default staging path, the full CSI 800 constituent audit was rerun over
`2014-02-21` through `2026-04-23`:

```bash
PYTHONPATH=src python3 -m alpha_find_v2.sw_industry_pit_audit \
  --reference-db output/pit_reference_staging.duckdb \
  --research-db output/research_source.duckdb \
  --benchmark-id "CSI 800" \
  --industry-schema sw2021_l1 \
  --industry-level L1 \
  --start-date 20140221 \
  --end-date 20260423 \
  --output-json output/audits/sw_industry_pit_audit_20140221_20260423.json \
  --output-gap-csv output/audits/sw_industry_pit_gap_rows_20140221_20260423.csv
```

Fresh result:

- constituent-days: `2,364,800`
- benchmark symbols: `1,609`
- staged exclusive coverage: `2,364,800 / 2,364,800` (`100%`)
- staged inclusive coverage: `2,364,800 / 2,364,800` (`100%`)
- `manual_adjudicated_provider_gap`: `25,229` days
- `tushare_end_date_boundary`: `207` days
- residual bridge-fill runs: `0`

Interpretation:

- `PIT` benchmark + `sw2021_l1` industry coverage for CSI 800 constituents is
  no longer the live-readiness blocker.
- The remaining `manual_adjudicated_provider_gap` rows are provider-comparison
  classifications, not staged coverage failures; staged effective rows exist
  for the constituent dates.
- Full trend validation is still a separate gate because the current trend
  builder selects from the broader A-share candidate universe, not only CSI 800
  constituents.
