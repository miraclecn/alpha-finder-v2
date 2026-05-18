# Minimal Benchmark-State Build Example

This directory defines the first production build case for turning audited PIT
benchmark weight snapshots plus PIT industry classification into a persisted
`benchmark_state_history` artifact.

Files:

- `csi800.toml`: build case for a CSI 800 benchmark-state history artifact

Run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python3 -m alpha_find_v2 build-reference-staging-db \
  --target-db output/pit_reference_staging.duckdb \
  --start-date 20140101 \
  --benchmark "CSI 800=000906.SH"
PYTHONPATH=src python3 -m alpha_find_v2 build-research-source-db \
  --source-db /home/nan/alpha-find/output/stock_data_audited.duckdb \
  --supplemental-db output/pit_reference_staging.duckdb \
  --target-db output/research_source.duckdb
PYTHONPATH=src python3 -m alpha_find_v2 build-benchmark-state \
  --case research/examples/benchmark_state_build_minimal/csi800.toml
```

Important:

- The base V1 audited DuckDB does not contain `benchmark_membership_pit`,
  `benchmark_weight_snapshot_pit`, or `industry_classification_pit`.
- Those PIT reference tables must be staged explicitly into the supplemental
  DuckDB passed to `build-research-source-db`.
- The checked-in CSI 800 build case now targets `provider_weight` with
  `sw2021_l1`, which means benchmark constituent weights come from staged
  official `index_weight` snapshots and industry labels come from staged
  `index_member_all` history.
- After the first two build steps run, the DuckDB outputs also expose
  machine-readable boundary metadata:
  `reference_dataset_registry` in `output/pit_reference_staging.duckdb`, plus
  `data_spine_registry`, `build_chain_registry`, and
  `data_boundary_registry` in `output/research_source.duckdb`.
- Rechecked on `2026-04-27`, the default staged reference path now covers CSI
  800 + `sw2021_l1` constituent-days from `2014-02-21` through `2026-04-23`.
  The audit artifact
  `output/audits/sw_industry_pit_audit_20140221_20260423.json` reports
  `2,364,800 / 2,364,800` staged constituent-days covered.
- A temporary full-window benchmark-state build over `2014-02-21` through
  `2026-04-23` succeeds with `2,956` trading steps, `800` constituents per
  step, and populated industry weights. The benchmark-state layer is therefore
  no longer the source of the `5` year live-readiness blocker.
- The checked-in build case now starts on `2021-03-05`, matching the widened
  trend live-candidate input and replay calendar. The replay audit window ends
  at `2026-03-19` because the sleeve target requires a T+20 exit horizon.
