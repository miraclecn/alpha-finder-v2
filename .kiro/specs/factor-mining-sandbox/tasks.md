# Implementation Plan: Factor Mining Sandbox

## Overview

Implementation of the Factor Mining Sandbox (`factor_lab`) — a hard-isolated candidate-generation module that proposes new descriptor expressions for human review. Searches a closed DSL grammar via beam search (with a random-sampling control), evaluates survivors through the existing Stage 2 evaluation pipeline, and emits a machine-readable run directory under `output/factor_lab/runs/<run_id>/`. Implementation follows requirements R1–R13 and design.md.

## Tasks

- [x] 1. Create package scaffold and isolation guard
  - Create `src/alpha_find_v2/factor_lab/__init__.py` (empty package marker)
  - Create `src/alpha_find_v2/factor_lab/isolation.py` with `assert_output_root_safe(path: Path) -> None` that resolves symbolic links and `..`, exits with status code 6 if the absolute path is not under `output/factor_lab/`
  - Add `OUTPUT_ROOT: Path = Path("output/factor_lab")` constant
  - **Verification**: unit test asserts `assert_output_root_safe` exits 6 when given `/tmp/foo`; passes for `output/factor_lab/runs/abc`
  - _Requirements: R8.5_

- [x] 2. Implement DSL grammar constants and AST nodes
  - Create `src/alpha_find_v2/factor_lab/dsl/__init__.py`
  - Create `src/alpha_find_v2/factor_lab/dsl/grammar.py` with:
    - `TIME_SERIES_OPS: frozenset` = {`lag`, `delta`, `rolling_mean`, `rolling_std`, `rolling_max`, `rolling_min`}
    - `CROSS_SECTION_OPS: frozenset` = {`cs_rank`, `cs_zscore`, `cs_demean`, `cs_industry_demean`}
    - `ARITHMETIC_OPS: frozenset` = {`+`, `-`, `*`, `/`, `log`}
    - `LEAF_FIELDS: frozenset` = {`close_adj`, `open`, `turnover_value_cny`, `pe`, `pb`}
    - `WINDOW_WHITELIST: frozenset[int]` = {5, 10, 20, 60, 120, 250}
    - `MAX_DEPTH: int = 5`
    - Frozen dataclass AST: `Leaf`, `TSOp`, `CSOp`, `ArithOp` (binary + unary `log`)
    - `node_count(ast) -> int` per R2 clause 6
  - **Verification**: unit test confirms whitelist sizes (6/4/5/5/6); `node_count` excludes window literals
  - _Requirements: R2.1, R2.2, R2.3, R2.4, R2.5, R2.6_

- [x] 3. Implement DSL parser and validator with structured rejection
  - Create `src/alpha_find_v2/factor_lab/dsl/parser.py`: string → AST
  - Create `src/alpha_find_v2/factor_lab/dsl/validator.py` with `RejectionRecord` dataclass (clause_number, position, reason)
  - Reject: arity violations (R2.12), unknown operators/fields (R2.9), bad window values (R2.5), depth > 5 (R2.6), TS-wraps-CS composition (R2.7), conditionals/loops/UDFs (R2.9)
  - Allow: CS-wraps-TS direction (R2.8)
  - **Verification**: PBT generates random expressions; every rejection emits a `RejectionRecord` with correct clause number; valid expressions parse round-trip
  - _Requirements: R2.5, R2.6, R2.7, R2.8, R2.9, R2.10, R2.12_

- [x] 4. Implement DSL canonical string and evaluator
  - Create `src/alpha_find_v2/factor_lab/dsl/canonical.py`: AST → deterministic canonical string (used as cache key and dedup key)
  - Create `src/alpha_find_v2/factor_lab/dsl/evaluator.py`: `evaluate(ast, ctx) -> pd.DataFrame[trade_date, security_id, descriptor_value]`
  - Loads leaf data once per run from research_source.duckdb into wide panels (security_id × trade_date)
  - TS ops operate per-security column; CS ops operate per-date slice
  - `cs_industry_demean` joins `industry_cs1_member_pit` table; non-positive `pe`/`pb` treated as missing (R2.11)
  - **Verification**: unit test on synthetic 5-stock × 60-date panel: each operator produces expected values; cache hit on same canonical string returns identical frame
  - _Requirements: R2.11, R11.3_
  - _Dependencies: 2, 3_

- [x] 5. Implement family classifier
  - Create `src/alpha_find_v2/factor_lab/family.py` with `classify(ast) -> str | None` deterministic rule cascade per R4 clauses 2–7
  - Return one of `{trend, volatility, volume, value, cross_momentum}` or `None` (rejected)
  - Idempotent: same AST → same family every call
  - **Verification**: unit test covers all 5 families + reject case; PBT confirms idempotency over 1000 random ASTs; quality keyword never produced
  - _Requirements: R4.1, R4.2, R4.3, R4.4, R4.5, R4.6, R4.7, R4.9, R4.11, R4.12_

- [x] 6. Implement mining config TOML schema and loader
  - Create `src/alpha_find_v2/factor_lab/config.py` with `MiningConfig` frozen dataclass mirroring all six tables (`[search]`, `[fitness]`, `[family]`, `[walk_forward]`, `[dedup]`, `[universe]`)
  - `load_mining_config(path: Path) -> tuple[MiningConfig, list[str]]` returns config + list of `config_defaults_applied` key paths
  - Validation per R10: unknown keys → exit 2, out-of-range → exit 2, type mismatch → exit 2, missing → apply default + record, `quality` family identifier → exit 2 (R13.1), GP/RL search method → exit 2 (R13.2), unregistered data source → exit 2 (R13.3), output type other than run dir → exit 2 (R13.4)
  - `to_snapshot_dict()` produces verbatim resolved-config dict for `manifest.json` (R10.13)
  - **Verification**: unit test parses minimal config, defaults applied; out-of-range raises with status 2 and named key path; `quality` reference rejected; `gp`/`rl` rejected; round-trip via `to_snapshot_dict()` preserves values
  - _Requirements: R10.1–R10.13, R13.1, R13.2, R13.3, R13.4_

- [x] 7. Implement fitness function with complexity penalty
  - Create `src/alpha_find_v2/factor_lab/search/__init__.py`
  - Create `src/alpha_find_v2/factor_lab/search/fitness.py` with `fitness(train_ic_ir: float | None, node_count: int, lambda_: float) -> float | None`
  - NaN/inf `train_ic_ir` → `None`; record `status="rejected_oos"` reason `train_ic_ir_undefined` (R6.2)
  - Tie-breaking: ascending `node_count`, then ascending lexicographic canonical string (R6.3)
  - **Verification**: unit test boundary values (0, 1, NaN, inf); PBT confirms ordering invariants (higher fitness ranked first; ties broken by node_count then string)
  - _Requirements: R6.1, R6.2, R6.3_
  - _Dependencies: 4_

- [x] 8. Implement expression generator
  - Create `src/alpha_find_v2/factor_lab/search/expression_generator.py`
  - `expand_layer(parents: list[ast]) -> Iterator[ast]`: enumerate valid one-op extensions (used by beam)
  - `random_tree(rng, max_depth) -> ast`: uniform draw of complete tree from grammar (used by random sampler)
  - All stochastic choices derive from injected `numpy.random.Generator` seeded from `config.search.seed` (R3.8)
  - **Verification**: unit test enumerates all depth-1 expressions (60: 5 leaves × 12 ops with appropriate windows); PBT: same seed produces same random tree sequence
  - _Requirements: R3.8_
  - _Dependencies: 2, 3, 4_

- [x] 9. Implement beam search
  - Create `src/alpha_find_v2/factor_lab/search/beam.py` with `run_beam_search(config, ctx, fitness_fn, evaluator) -> list[Candidate]`
  - Layer-by-layer growth from depth 1 to `max_depth`
  - At each layer, evaluate via DSL evaluator → train-set IC_IR via Stage 2 forward returns
  - Retain top `beam_width` by fitness (descending); ties: ascending node_count, then lex (R3.1, R6.3)
  - NaN/non-finite IC_IR or evaluator errors → ineligible for retention (R3.2)
  - Each retained `Candidate` has canonical string, AST, fitness, train_ic_ir, sources=`["beam"]`
  - **Verification**: unit test 2-layer beam on synth fixture retains exactly `beam_width` candidates per layer; deterministic given seed
  - _Requirements: R3.1, R3.2, R3.5, R3.8_
  - _Dependencies: 7, 8_

- [x] 10. Implement random sampler
  - Create `src/alpha_find_v2/factor_lab/search/random_sampler.py` with `run_random_sampling(config, ctx, fitness_fn, evaluator) -> list[Candidate]`
  - Draw exactly `random_sample_size` expressions uniformly from grammar across depths 1..max_depth
  - Evaluate each on same train + OOS windows as beam (R3.3)
  - Sources=`["random"]` initially; merged later if same canonical string seen by beam (R3.4, R3.6)
  - **Verification**: unit test draws 1000 samples; coverage spans all depths and families; same seed → same sample set
  - _Requirements: R3.3, R3.4, R3.5, R3.6, R3.8_
  - _Dependencies: 8_

- [x] 11. Implement candidate merge and dedup-by-canonical
  - Create `src/alpha_find_v2/factor_lab/search/merge.py` with `merge_streams(beam: list[Candidate], random: list[Candidate]) -> list[Candidate]`
  - Same canonical string → single candidate with `sources=["beam","random"]` (R3.6)
  - Compute aggregate accepted-rates per stream; if random ≥ beam → emit `beam_underperforms_random` warning for manifest (R3.7)
  - **Verification**: unit test confirms duplicate canonical strings merge into one record with both sources; warning emitted when random outperforms
  - _Requirements: R3.4, R3.6, R3.7_
  - _Dependencies: 9, 10_

- [x] 12. Implement family quota enforcement
  - Create `src/alpha_find_v2/factor_lab/quota.py` with `apply_family_quota(candidates, quota_per_family) -> tuple[admitted, rejected_quota]`
  - Per family: select top `quota_per_family` by fitness descending, ties by lex ascending (R4.8)
  - Excluded candidates record `status="rejected_quota"` + family name in candidates.jsonl (R4.10)
  - Family `quality` never processed (R4.9) — enforced upstream by config validator (Task 6)
  - **Verification**: unit test 30 candidates across 5 families with quota=5 → exactly 25 admitted, 5 rejected per family overflow
  - _Requirements: R4.8, R4.9, R4.10_
  - _Dependencies: 5, 7_

- [x] 13. Implement walk-forward evaluator
  - Create `src/alpha_find_v2/factor_lab/walk_forward.py` with `run_walk_forward(candidate, config, research_db, universe_id) -> WalkForwardResult`
  - Split `[start, end]` into `segments` anchored windows per R5.1; precondition checks per R5.2 (min 24mo train, OOS within `--end`)
  - Per segment: construct ad-hoc `DescriptorComputeSpec` (NOT registered) wrapping `dsl.evaluator.evaluate`; pass to `evaluate_descriptor` from `factor_evaluation` (R5.3, R9.3, R9.4)
  - Forward returns at `primary_horizon_days` via Stage 2 `compute_forward_returns` (R5.7)
  - Universe via `resolver_for_universe(universe_id, conn, mandate)` (R5.8)
  - Tradeability filter applied automatically by `evaluate_descriptor` (R5.6, R9.2)
  - Cost model: `config/cost_models/base_a_share_cash.toml` (R9.6)
  - Acceptance: ALL segments OOS_IC_IR ≥ threshold AND OOS_IC_mean > 0 → `accepted_oos`; else `rejected_oos` with first failing segment index (R5.4, R5.5)
  - **Verification**: unit test on synth fixture: 3-segment split correct; precondition violation raises with offending segment index; ad-hoc spec never appears in `descriptor_compute.list_registered()`
  - _Requirements: R5.1–R5.8, R9.1, R9.2, R9.3, R9.4, R9.6_
  - _Dependencies: 4, 6_

- [x] 14. Implement correlation dedup stage
  - Create `src/alpha_find_v2/factor_lab/dedup.py` with `run_correlation_dedup(accepted, registered_descriptor_ids, train_window, ctx, dedup_rho, dedup_min_obs) -> tuple[admitted, rejected_correlation, matrix]`
  - Process candidates in fitness-descending order (ties: node_count asc, lex asc) (R6.7)
  - For each candidate: pairwise absolute Pearson r vs (a) every registered descriptor (loaded once via `descriptor_compute.get(id).fn(ctx)` and cached) (R6.4, R6.9) and (b) every already-admitted candidate
  - Restrict each pair to overlapping non-NaN `(trade_date, security_id)`; if overlap < `dedup_min_obs` or zero variance on either side → empty cell, no rejection (R6.5)
  - Reject if any defined |r| > `dedup_rho` → status `rejected_correlation` with highest correlated reference id and r rounded to 6 decimals (R6.6)
  - Build full correlation matrix (rows: every evaluated candidate; cols: registered descriptors then accepted candidates) (R6.8)
  - **Verification**: unit test two correlated candidates → lower-fitness rejected with correct reference; undefined cells written as empty strings; matrix contains all evaluated candidates including rejected
  - _Requirements: R6.4, R6.5, R6.6, R6.7, R6.8, R6.9_
  - _Dependencies: 13_

- [x] 15. Implement output writer
  - Create `src/alpha_find_v2/factor_lab/output.py` with five writers:
    - `write_manifest(run_dir, run_metadata) -> None`: all keys from R7.1 always present (zero/empty values written explicitly); UTC ISO-8601 with `Z` and ms precision; `duration_seconds` rounded to 3 decimals
    - `write_candidates_jsonl(run_dir, candidates) -> None`: one JSON per line, UTF-8, `\n`-terminated; all keys from R7.2 (R7.2)
    - `write_shortlist(run_dir, accepted_candidates) -> None`: only `accepted` status; ordered by fitness desc, expr_id asc; `family_rank` field added (R7.3)
    - `write_correlation_matrix(run_dir, matrix) -> None`: UTF-8 CSV, 6-decimal floats in [-1, 1], empty string for undefined cells (R7.4)
    - `write_audit_md(run_dir, accepted_candidates) -> None`: per-candidate template with TODO placeholders + Promotion Path section (R7.5, R8.6)
  - All paths emitted with POSIX forward slashes regardless of host OS (R7.10)
  - Atomic-ish writes (write to temp + rename) so partial run never corrupts registry (R7.7)
  - **Verification**: unit test schema-validates each artifact against fixture; round-trip JSON parse preserves config_snapshot exactly; Windows paths emit forward slashes
  - _Requirements: R7.1, R7.2, R7.3, R7.4, R7.5, R7.7, R7.10, R8.6_

- [x] 16. Implement registry append
  - Create `src/alpha_find_v2/factor_lab/registry.py` with `append_run_entry(run_id, run_at, run_dir, candidate_count, accepted_count, families_present) -> None`
  - Reads `output/factor_lab/registry.json`, appends one entry preserving prior order, writes atomically (R7.6)
  - If artifact write failed earlier → registry unchanged (R7.7)
  - `list_runs(family: str | None, min_ic_ir: float | None) -> list[dict]` for `list-factor-candidates` command:
    - sorted by `run_at` desc; case-sensitive family filter; min_ic_ir filters runs whose shortlist has any candidate with mean OOS IC_IR ≥ threshold; both filters → AND (R1.5, R1.6, R1.7)
    - missing/empty registry → return `[]` (R1.12)
  - **Verification**: unit test multi-run append preserves order; concurrent-write protection via file lock optional (single-user assumption); list filters work as AND
  - _Requirements: R1.5, R1.6, R1.7, R1.12, R7.6, R7.7_

- [x] 17. Implement git SHA resolver
  - Create `src/alpha_find_v2/factor_lab/git_meta.py` with `resolve_git_sha(repo_root: Path) -> tuple[str, bool]` returning `(sha, is_dirty)`
  - 40-character lowercase hex; `<sha>-dirty` if working tree has uncommitted changes (R12.4, R12.5)
  - Not in git repo → raise `GitMetadataError` causing caller to exit non-zero before writing artifacts (R12.6)
  - Dirty tree → emit `dirty_working_tree` warning to stderr (R12.5)
  - **Verification**: unit test mocks subprocess `git rev-parse HEAD` and `git status --porcelain`; clean/dirty/no-repo paths all return correct value or raise
  - _Requirements: R12.4, R12.5, R12.6_

- [x] 18. Implement top-level mining run orchestrator
  - Create `src/alpha_find_v2/factor_lab/run.py` with `execute_mining_run(research_db, start, end, config_path) -> dict` returning `{"run_id": ..., "run_dir": ...}`
  - Sequence: isolation check → config load → date validation → DB existence check → git SHA → generate `run_id` (UUID v4 hex, unique under runs/) → init RNG from `config.search.seed` (R12.2, R12.3) → load leaf panels → beam search → random sampling → merge → quota → walk-forward → dedup → output → registry append
  - Track timing → `duration_seconds` in manifest; if exceeds 2× budget add `time_budget_exceeded` warning, preserve artifacts (R11.6, R11.7)
  - Exit code mapping: 0 success, 2 config/arg, 4 missing DB, 5 missing expr_id, 6 isolation
  - Per-segment frame release after metrics recorded (R11.4)
  - Expression score cache (canonical string → series) shared across beam, random, walk-forward, dedup (R11.3)
  - **Verification**: integration test on synth fixture produces all 5 artifacts + registry entry; same seed → byte-identical candidates.jsonl modulo `run_id`/`run_at`/`duration_seconds`
  - _Requirements: R1.1, R1.2, R11.3, R11.4, R11.6, R11.7, R12.1, R12.2, R12.3_
  - _Dependencies: 6, 9, 10, 11, 12, 13, 14, 15, 16, 17_

- [x] 19. Implement inspect-candidate handler
  - Create `src/alpha_find_v2/factor_lab/inspect.py` with `run_inspection(run_id, expr_id) -> None`
  - Validate run_dir exists (else exit 4 with missing path), validate expr_id present in candidates.jsonl (else exit 5 with known ids list) (R1.9, R1.10)
  - Construct ad-hoc `DescriptorComputeSpec` from the stored canonical expression
  - Call Stage 2 `evaluate_descriptor` with full default horizons; pass output to `report_writer.write_report` targeted at `output/factor_lab/runs/<run_id>/inspections/<expr_id>/` (R1.8, R7.8)
  - Pipeline failure after validation → exit non-zero (not 2/4/5) with failing stage in stderr; no partial inspection dir left (R1.13)
  - **Verification**: integration test on synth fixture: inspect existing candidate writes report.json + report.md; missing run_id exits 4; missing expr_id exits 5; pipeline error exits non-zero with no partial dir
  - _Requirements: R1.8, R1.9, R1.10, R1.13, R7.8, R7.9_
  - _Dependencies: 16, 18_

- [x] 20. Wire CLI commands into existing argument parser
  - Edit `src/alpha_find_v2/cli.py` to add three subcommands:
    - `mine-factors --research-db --start --end --config` → `factor_lab.run.execute_mining_run`
    - `list-factor-candidates [--family] [--min-ic-ir]` → `factor_lab.registry.list_runs`
    - `inspect-candidate <run_id> <expr_id>` → `factor_lab.inspect.run_inspection`
  - Validate `--start`/`--end` are 8-digit YYYYMMDD with `start <= end`; else exit 2 with offending arg name (R1.11)
  - DB path missing/unreadable → exit 4 with path + reason (R1.3)
  - Config path missing/invalid TOML/schema fail → exit 2 with first violation (R1.4)
  - On success of `mine-factors`: print single JSON object `{"run_id": ..., "run_dir": ...}` to stdout (R1.2)
  - `list-factor-candidates` prints JSON array sorted by `run_at` desc (R1.5)
  - **Verification**: CLI integration test invokes each command via subprocess on synth fixture; exit codes and stdout/stderr contracts validated
  - _Requirements: R1.1, R1.2, R1.3, R1.4, R1.5, R1.6, R1.7, R1.8, R1.9, R1.10, R1.11, R1.12, R1.13_
  - _Dependencies: 18, 19_

- [x] 21. Add isolation enforcement test (no register, no out-of-tree writes)
  - Create `tests/factor_lab/test_isolation.py`
  - AST scan: walk all `.py` files under `src/alpha_find_v2/factor_lab/` and assert no import of `descriptor_compute.register` or any function that mutates the descriptor registry (R8.1, R8.8)
  - Filesystem mock: monkeypatch `pathlib.Path.write_text`, `open(..., "w")`, etc.; run sandbox end-to-end on synth fixture; assert all writes target paths under `output/factor_lab/` (R8.2, R8.3, R8.4, R8.8)
  - Misconfigured output root → exit 6 (R8.5)
  - **Verification**: test passes when isolation intact; fails with named clause + call site when violated
  - _Requirements: R8.1, R8.2, R8.3, R8.4, R8.5, R8.7, R8.8_
  - _Dependencies: 18_

- [x] 22. Add grammar rejection test
  - Create `tests/factor_lab/test_dsl_grammar.py`
  - Cover every banned composition in R2 clauses 5, 6, 7, 9, 12 with explicit string inputs and asserted `RejectionRecord` clause numbers
  - PBT generates random invalid expressions; every rejection produces a structured record naming the violated clause
  - Valid expression sample (one per family) parses without rejection
  - **Verification**: all listed banned patterns rejected; no false positives on valid expressions
  - _Requirements: R2.5, R2.6, R2.7, R2.9, R2.10, R2.12_
  - _Dependencies: 3_

- [x] 23. Add reproducibility test
  - Create `tests/factor_lab/test_reproducibility.py`
  - Run `mine-factors` twice on synth fixture with same config + seed
  - Assert `candidates.jsonl` and `shortlist.json` byte-identical after stripping `run_id`, `run_at`, `duration_seconds` from comparison
  - Assert config SHA-256 identical; `git_sha` identical
  - **Verification**: two runs produce byte-identical artifacts modulo excluded fields
  - _Requirements: R12.1, R12.2_
  - _Dependencies: 18, 20_

- [x] 24. Add walk-forward and OOS gate test
  - Create `tests/factor_lab/test_walk_forward.py`
  - Synthetic 4-year fixture; default config → exactly 3 anchored segments
  - Inject candidate that passes all segments → status `accepted_oos`
  - Inject candidate that fails segment 2 → status `rejected_oos` with `first_failing_segment=2`
  - Precondition test: 1-year fixture → run aborts with offending-segment error
  - **Verification**: all assertions hold; ad-hoc spec absent from descriptor registry after run
  - _Requirements: R5.1, R5.2, R5.4, R5.5, R9.4_
  - _Dependencies: 13, 18

- [x] 25. Add correlation dedup test
  - Create `tests/factor_lab/test_dedup.py`
  - Two candidates differing only by a no-op transform → |r|=1 → lower-fitness rejected
  - One candidate vs registered `medium_term_relative_strength` with high correlation → rejected with correct reference id
  - Insufficient overlap (< min_obs) → empty cell, candidate not rejected on that pair
  - **Verification**: matrix contains every evaluated candidate; rejected candidate carries correct reference + r value
  - _Requirements: R6.4, R6.5, R6.6, R6.7, R6.8_
  - _Dependencies: 14

- [x] 26. Add output schema and registry test
  - Create `tests/factor_lab/test_output_schema.py`
  - Run synth fixture end-to-end; validate each artifact against expected JSON schema / CSV layout
  - `manifest.json` has all R7.1 keys present; `candidates.jsonl` rows have all R7.2 keys; `shortlist.json` has `family_rank`; `correlation_matrix.csv` cells in [-1, 1] or empty; `audit.md` has Promotion Path section
  - All paths in JSON/JSONL emitted with forward slashes
  - Registry append preserves prior entries
  - **Verification**: every schema rule validated
  - _Requirements: R7.1, R7.2, R7.3, R7.4, R7.5, R7.6, R7.10, R8.6_
  - _Dependencies: 15, 16, 18

- [x] 27. Add config validation test
  - Create `tests/factor_lab/test_config.py`
  - Each of R10.1–R10.13 covered: defaults applied, unknown key rejected, out-of-range rejected, type mismatch rejected
  - R13 guardrails: `quality` family rejected before DB open (R13.1); `gp`/`rl` search method rejected (R13.2); unregistered data source rejected (R13.3); non-run-dir output rejected (R13.4)
  - `config_snapshot` round-trip preserves exact resolved config
  - **Verification**: every clause asserted via dedicated test
  - _Requirements: R10.1–R10.13, R13.1, R13.2, R13.3, R13.4_
  - _Dependencies: 6

- [x] 28. Add performance smoke test
  - Create `tests/factor_lab/test_performance.py` (marked `@pytest.mark.slow`, optional in CI)
  - Run synth fixture with default config; assert wall-clock < 300s; peak RSS < 4GB (via `psutil` if installed, else skip RSS check)
  - **Verification**: synth-fixture run finishes within budget on CI runner; if it exceeds 2× budget the manifest contains `time_budget_exceeded` warning
  - _Requirements: R11.1, R11.5, R11.6, R11.7_
  - _Dependencies: 18, 20

- [x] 29. Add CLI integration test
  - Create `tests/factor_lab/test_cli_integration.py`
  - Subprocess-invoke each of three commands on synth fixture; validate exit codes and stdout/stderr per R1
  - Cover: bad date → exit 2; missing DB → exit 4; missing run_id → exit 4; missing expr_id → exit 5; pipeline error in inspect → non-zero (not 2/4/5)
  - **Verification**: all CLI contracts validated
  - _Requirements: R1.1–R1.13_
  - _Dependencies: 20

- [x] 30. Document promotion workflow and update root README/docs
  - Add `docs/architecture/factor-mining-sandbox.md` summarizing: scope, isolation guarantees, promotion path (manual PR workflow), references to R8.6 audit.md template
  - Add a section to root `README.md` under "Stage 3" pointing at the new doc and CLI commands
  - **Verification**: doc links resolve; references match implemented behavior
  - _Requirements: R8.6, R8.7_
  - _Dependencies: 15
