# Research Workspace

This directory is reserved for V2 research artifacts that are subordinate to the versioned mandate, thesis, sleeve, and portfolio registries.

Rules:

- notebooks and ad hoc analysis are exploratory only
- versioned config under `config/` is the source of truth
- sleeve research promoted into replay must emit a normalized decision-calendar artifact, not a sparse factor dump
- no legacy V1 factor ids should be promoted here without re-underwriting them as V2 theses or sleeves

Working structure:

- `examples/promotion_replay_minimal/` contains the first persisted end-to-end replay case
- `examples/deployment_minimal/` contains the first executable-signal and decay-watch cases
- persisted sleeve artifacts are JSON because replay artifacts are path-heavy, nested, and easier to audit in JSON than TOML
- artifact build cases are TOML because they bind sleeve config plus research input/output paths
- replay cases are TOML because they are mostly registry paths plus small scenario overrides
- deployment cases are TOML because they bind portfolio registries, sleeve artifacts, and narrow scenario state into one auditable file
