# Phase 1 Data Truth And V1 Reuse

## Goal

Turn the personal-data constraint into a hard system boundary instead of a
spoken assumption. Phase 1 fixes the only production-safe data spine for the
first release and records exactly what V1 can and cannot contribute to V2.

## Current Verified State

- `output/pit_reference_staging.duckdb` exists locally as of `2026-04-25`.
- `output/research_source.duckdb` exists locally as of `2026-04-25`.
- `docs/data/v1-duckdb-reuse-audit.md` already records the audited V1 source
  boundary and the derived V2 research DB.
- `research/examples/benchmark_state_build_minimal/README.md` already defines
  the only honest chain for benchmark / industry PIT reference staging and V2
  research DB construction.
- `docs/architecture/a-share-personal-data-research-doctrine.md` already
  classifies datasets into `green`, `amber`, and `experimental`.
- The checked local residual and replay docs already confirm that neither
  `output/research_source.duckdb` nor `output/pit_reference_staging.duckdb`
  contains audited residual-component rows.

## In Scope

- lock `Tushare 2000` as the first-release production source backbone
- lock `AKShare` as supplemental, validation-only, or exploratory until a
  field-level audit promotes a specific field
- lock `output/pit_reference_staging.duckdb` as the only PIT reference staging
  surface
- lock `output/research_source.duckdb` as the only isolated V2 research DB
- publish `green / amber / experimental` source-tier rules
- list directly reusable V1 market / PIT inputs, forbidden reuse surfaces, and
  known data gaps
- require benchmark and industry PIT references to enter V2 only through the
  staged Tushare path

## Out of Scope

- direct research against the mixed V1 DuckDB
- using `AKShare` as promotion-safe truth
- porting V1 factor, strategy, or promotion objects
- reopening residual-component snapshot generation inside V2
- broker or OMS integration

## Dependencies

- `docs/architecture/a-share-personal-data-research-doctrine.md`
- `docs/data/v1-duckdb-reuse-audit.md`
- `research/examples/benchmark_state_build_minimal/README.md`
- `output/pit_reference_staging.duckdb`
- `output/research_source.duckdb`

## Execution Breakdown

### 1. Freeze The Tier Rules

Document the release-1 rule set exactly:

- `green`: stable daily and structural truth used by benchmark state,
  trend research, tradeability checks, and regime monitoring
- `amber`: slower fundamental snapshots allowed only for lag-aware anchor or
  veto work
- `experimental`: everything else, including unaudited `AKShare`-only fields

### 2. Freeze The Only Standard Build Chain

The allowed first-release build entry is:

1. `build-reference-staging-db`
2. `build-research-source-db`
3. `build-benchmark-state`

No parallel side path is allowed for benchmark membership, benchmark weights,
or industry PIT truth.

### 3. Freeze Reuse Boundaries

Phase 1 should explicitly classify reusable surfaces from the current audit:

- acceptable V2 tables / surfaces:
  `daily_bar_pit`, `market_trade_calendar`, `security_master_ref`,
  `name_change_history`, `fundamental_snapshot_pit`,
  staged `benchmark_membership_pit`, staged `benchmark_weight_snapshot_pit`,
  and staged `industry_classification_pit`
- forbidden direct reuse:
  V1 factor outputs, V1 strategy outputs, V1 promotion artifacts, and any
  workflow that keeps V2 dependent on the mixed V1 DB at query time
- known gaps to keep visible:
  first-source suspension realism, exact limit-state reconstruction, and any
  field that only exists today through an unaudited `AKShare` path

### 4. Freeze The `AKShare` Audit Rule

`AKShare` may supplement, validate, or help explore coverage gaps. It may not
enter the production-safe research chain unless one field at a time is
promoted through an explicit V2 audit.

### 5. Publish Failure Conditions

The document should make failure explicit instead of letting truth boundaries
drift:

- stop if a production-safe path reads `AKShare`-only fields without audit
- stop if benchmark / industry PIT references bypass staging
- stop if any new work treats the mixed V1 DB as a live research dependency

## Verification Matrix

- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_reference_data_staging.py' -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_market_data_bootstrap.py' -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_benchmark_state_builder.py' -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -v`
- `build-reference-staging-db --target-db output/pit_reference_staging.duckdb`
- `build-research-source-db --supplemental-db output/pit_reference_staging.duckdb --target-db output/research_source.duckdb`
- `build-benchmark-state --case research/examples/benchmark_state_build_minimal/csi800.toml`

Phase completion is not just "the commands run." It also requires the document
to list:

- reusable surfaces
- prohibited reuse objects
- missing fields / realism gaps
- failure conditions that stop the build chain from drifting

## Stop Conditions

- an unaudited `AKShare` field enters a production-safe path
- benchmark or industry PIT data is loaded from anywhere except the staging
  chain
- the V1 factor / strategy / promotion stack is proposed as a reusable object
- Phase 1 tries to solve the residual snapshot problem instead of publishing
  the boundary

## Exit Criteria

- the tier rules are explicit and versioned in the phase document
- `output/pit_reference_staging.duckdb` and `output/research_source.duckdb`
  are locked as the only standard first-release data interfaces
- the standard command chain is documented and unique
- allowed V1 reuse, forbidden reuse, and known data gaps are all listed
- `AKShare` remains explicitly outside the promotion-safe truth layer
