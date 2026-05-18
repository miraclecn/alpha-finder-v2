# SW Industry PIT Adjudication Design

## Scope

This slice hardens `sw2021_l1` industry truth for the `2014-02-21` to current
research window.

It does not mutate raw truth with heuristic fills.
It does not back-port V1 logic.
It does not claim same-day reclassification-event precision until
`available_at` is explicitly defined for intraday node timestamps.

## Current Finding

- the rebuilt official staging database improved the `2014-02-21` to
  `2026-04-23` `10Y` audit, but it still covers only `2,183,429 / 2,364,800`
  benchmark constituent-days (`92.33%`)
- the missing official coverage is not a small interpolation problem:
  `175,391` rows of `staging_sync_gap` remain across `213` symbols
- all `213` affected symbols are left-edge history omissions:
  `200` symbols have every gap day before the first official staged row, and
  `13` symbols have no official staged row at all
- a direct L1 interval build from `docs/data/StockClassifyUse_stock.xls`
  changes the picture materially:
  `2,363,699 / 2,364,800` covered (`99.95%`)
- under that direct official-packet build, `staging_sync_gap` collapses from
  `175,391` rows to `4` rows on a single symbol: `001979.SZ`
- the remaining large gap bucket after the direct official-packet build is
  still `Tushare` residual provider absence:
  `25,229` rows across `35` symbols

Conclusion:

- the primary `10Y` problem is not missing SW history
- the primary `10Y` problem is that the current official packet is not yet
  being operationalized into the staged PIT table faithfully enough

## Data Layers

The industry stack should be split into three explicit layers.

### 1. Official Raw Node Ledger

Source:

- `docs/data/StockClassifyUse_stock.xls`

Rules:

- preserve stock-level node dates from the official packet
- preserve raw source timestamps and provenance
- quarantine `1990-01-01` placeholder starts as non-auto-trusted left edges
- preserve intraday `计入日期` values so downstream layers can define
  `available_at` honestly instead of silently flooring them away

This layer is the primary truth candidate for the `2014+` rebuild.

### 2. Derived Official PIT Intervals

Source:

- deterministic interval derivation from the official node ledger

Rules:

- derive `removed_at` from the next official node for the same stock
- project only the requested level such as `sw2021_l1`
- for slow-moving research uses, allow daily-date normalization
- do not claim event-time PIT precision when the source node is intraday

This layer is where the research-safe `2014+` L1 table should come from.

### 3. Manual Adjudication Layer

Source:

- narrow exception records only

Rules:

- never overwrite raw official rows
- store manual decisions separately with explicit evidence fields
- require `available_at` and evidence provenance for every manual row

This layer exists only for unresolved exceptions after the raw official import.

## Merge Policy

- if official raw intervals cover the date, use official
- if official raw is absent and live `Tushare index_member_all` has a real PIT
  interval, allow a derived fallback row tagged as `tushare_raw_fallback`
- if both raw sources are absent, the date stays unresolved unless it enters
  manual adjudication
- heuristic bridge-fill remains derived-only and must never be written back as
  raw truth

## Heuristic Policy

Approved:

- bridge-fill only in the derived layer
- only when both endpoints exist in live `Tushare`
- only when both endpoint industry codes match
- only when the gap does not cross `2014-02-21` or `2021-07-30`
- only when `AKShare` does not show multiple codes inside the gap

Rejected:

- filling a full gap from the front endpoint when back and front disagree
- one-sided left-edge or right-edge flattening
- mutating raw official truth with inferred rows

## Online Adjudication Policy

Online lookup is acceptable, but only for a narrow dispute pool.

### Tier A: Do Not Use Online Lookup First

These cases should be solved by the official raw packet itself:

- the `72` names with left-edge gaps ending at `2021-07-29`
- the full `200` left-edge `staging_sync_gap` names
- the `13` symbols with zero staged official rows

For these cases, the right action is raw official import, not online research.

### Tier B: Worth Manual Online Review

These cases are narrow enough to justify human adjudication:

- the `35` residual `tushare_provider_gap_akshare_covers` symbols
- the single `blocked_endpoint_mismatch` run:
  `000623.SZ`, `2014-07-01` to `2019-07-23`
- the `9` `blocked_schema_transition` runs, especially names spanning
  `2021-07-30`
- the residual `001979.SZ` `4`-day sync gap from `2015-12-31` to `2016-01-06`

Online evidence should be ranked in this order:

1. Shenwan official packet or official Shenwan release material
2. exchange / CNINFO legal disclosure that establishes business state and
   disclosure timing
3. company IR or other secondary material as supporting evidence only

## Manual Evidence Schema

Each adjudication record should include:

- `security_id`
- `start_date`
- `end_date`
- `industry_schema`
- `industry_level`
- `industry_code`
- `source_type`
- `evidence_url`
- `evidence_date`
- `available_at`
- `confidence`
- `adjudication_note`

## 2026-04-27 Progress Update

The default `sw2021_l1` reference path now materializes a separate
`industry_classification_pit_manual_adjudication` layer with `39` rows:

- `1` listing-lag effective exception for `001979.SZ`
- `37` provider-gap confirmation rows used only by the audit layer
- `1` external left-edge effective adjudication for `600651.SH`

After refreshing `output/pit_reference_staging.duckdb` and
`output/research_source.duckdb`, the full `2014-02-21` to `2026-04-23`
audit has no residual provider-gap runs:

- `600651.SH` is now resolved through the manual adjudication layer as an
  `external_left_edge_backfill` from `2014-02-21` to `2017-06-29`, while the
  `1990-01-01` source placeholder remains excluded from
  `industry_classification_pit_official_raw`
- external evidence: Securities Times listed `600651.SH` under other
  electronics on `2014-09-27`; a CNINFO disclosure dated `2016-09-28`
  uses Shenwan classification and includes `600651.SH` in the
  other-electronics peer set

The current staged coverage is `2,364,800 / 2,364,800` constituent-days.
`25,229` days are classified as `manual_adjudicated_provider_gap`; only
`207` `tushare_end_date_boundary` days remain as provider boundary noise.

Fresh command verification on `2026-04-27` wrote
`output/audits/sw_industry_pit_audit_20140221_20260423.json` and confirmed the
same staged coverage ratios:

- `staged_exclusive_covered`: `2,364,800 / 2,364,800`
- `staged_inclusive_covered`: `2,364,800 / 2,364,800`
- `derived_bridge_fill_summary.run_count`: `0`

A temporary full-window benchmark-state build using the checked builder also
succeeded over `2014-02-21` through `2026-04-23` with `2,956` trading steps and
`800` constituents per step.

This closes the benchmark constituent coverage question. The remaining
`trend_leadership` coverage gap was `302132.SZ`: the market-data spine carries
the current code, while the older official SW industry intervals were recorded
under legacy `300114.SZ`. The staged reference build now resolves that narrow
identity break with `security_code_alias_backfill`, after which the frozen
live-candidate audit rebuild covers `2021-03-05` through `2026-03-19` and
passes the 5-year release gate.

## Follow-Up

The official-packet rebuild and the narrow online adjudication slice are now
operational in the default database path. Future changes should focus on:

1. keeping `industry_classification_pit_official_raw` free of manual or
   heuristic fills
2. requiring explicit `evidence_url`, `evidence_date`, and `available_at` for
   every new manual adjudication
3. deciding whether downstream consumers should expose manual effective rows
   separately from official effective rows when stricter availability modeling
   is needed
