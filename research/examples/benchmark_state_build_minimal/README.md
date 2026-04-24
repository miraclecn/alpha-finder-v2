# Minimal Benchmark-State Build Example

This directory defines the first production build case for turning audited PIT
benchmark weight snapshots plus PIT industry classification into a persisted
`benchmark_state_history` artifact.

Files:

- `csi800.toml`: build case for a CSI 800 benchmark-state history artifact

Run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python -m alpha_find_v2 build-reference-staging-db \
  --target-db output/pit_reference_staging.duckdb \
  --start-date 20140101 \
  --benchmark "CSI 800=000906.SH"
PYTHONPATH=src python -m alpha_find_v2 build-research-source-db \
  --source-db /home/nan/alpha-find/output/stock_data_audited.duckdb \
  --supplemental-db output/pit_reference_staging.duckdb \
  --target-db output/research_source.duckdb
PYTHONPATH=src python -m alpha_find_v2 build-benchmark-state \
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
- With the currently staged Tushare `sw2021_l1` history, full CSI 800 industry
  coverage begins on `2025-08-29`. The checked-in case therefore starts there
  instead of pretending earlier dates are fully classified.
