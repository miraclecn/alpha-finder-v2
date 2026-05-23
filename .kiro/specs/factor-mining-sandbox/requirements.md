# Requirements Document

> Factor Mining Sandbox — Stage 3 of the V2 rebuild roadmap.
>
> Goal: Provide a hard-isolated candidate-generation sandbox that proposes new
> descriptor expressions for human review, without touching the production
> descriptor registry, without re-implementing Stage 2 evaluation math, and
> without violating the A-share execution-realism doctrine.

## Introduction

The Factor Mining Sandbox (`factor_lab`) is a candidate generator. It takes a
small, fixed grammar of operators and leaf fields, searches the resulting
expression space with beam search (and a random-sampling control), evaluates
surviving candidates through the existing Stage 2 evaluation pipeline using
anchored walk-forward, and emits a human-review packet.

What the sandbox **is**:

- a generator of expression candidates with measurable train and out-of-sample
  predictive quality
- a writer of run artifacts (manifest, candidates, shortlist, correlation
  matrix, audit template) under `output/factor_lab/runs/<run_id>/`
- a read-only consumer of Stage 2 evaluation primitives
  (`evaluate_descriptor`, `forward_returns`, `universe_resolver`,
  tradeability filter) and registered descriptor TOMLs

What the sandbox is **not**:

- it is **not** an alpha registry. Candidates are not descriptors.
- it does **not** auto-register, auto-promote, or auto-write to
  `config/descriptors/`.
- it is **not** a strategy. Factor-table-as-strategy auto-wrappers are banned
  by `docs/migration/legacy-boundary.md`.
- it is **not** a symbolic-miner replacement for descriptors. The legacy
  symbolic miner is the deny-listed pattern this sandbox explicitly avoids
  (narrow grammar, beam + random control, manual promotion).

Every promotion of a candidate to a registered descriptor is a manual workflow
that requires an economic story, a risk note, and a pull request. The sandbox
prepares the audit packet; humans make the call.

## Glossary

- **Expression DSL**: the closed grammar defined in R2 — 6 time-series ops,
  4 cross-section ops, 5 arithmetic ops, 5 leaf fields, fixed window-size
  whitelist, depth ≤ 5 nodes.
- **Family**: a deterministic rule-based class assigned to each expression by
  operator signature and leaf field. Stage 3 ships 5 families: `trend`,
  `volatility`, `volume`, `value`, `cross_momentum`. The `quality` family is
  out-of-scope until the Tushare credit prerequisites are met.
- **Beam Search**: layer-by-layer expression growth where, at each depth, only
  the top `beam_width` candidates by train-set IC_IR survive to the next
  depth. Default beam width is 20, default max depth is 5.
- **Random Sampling Baseline**: a parallel control that draws a fixed number
  of expressions uniformly from the same DSL grammar so beam-search yield can
  be compared to a chance baseline.
- **Anchored Walk-Forward**: a 3-segment evaluation protocol where each
  segment has a fixed train start (anchored), a growing train end, and a
  6-month out-of-sample window immediately after the train end. ALL segments
  must pass the OOS thresholds for the candidate to be accepted.
- **Complexity Penalty**: a fitness adjustment of the form
  `fitness = train_IC_IR − λ × node_count`, where `node_count` is the number
  of DSL nodes in the expression tree and `λ` defaults to 0.05.
- **Correlation Dedup**: rejection of any candidate whose absolute Pearson
  correlation against (a) any registered descriptor's score series or
  (b) any already-accepted candidate's score series exceeds `dedup_rho`
  (default 0.85).
- **Sandbox Isolation**: the hard rule that `factor_lab` may only write under
  `output/factor_lab/`, may only read Stage 2 evaluation primitives, and may
  never call `descriptor_compute.register()` or write to
  `config/descriptors/`.
- **Promotion Path**: the manual review workflow documented in each run's
  `audit.md` template. A candidate becomes a registered descriptor only via
  a human-authored PR that adds a new `config/descriptors/<id>.toml` and a
  new compute function in `factor_evaluation/descriptor_compute.py`. The
  sandbox never performs this step.
- **Run Directory**: `output/factor_lab/runs/<run_id>/` containing
  `manifest.json`, `candidates.jsonl`, `shortlist.json`,
  `correlation_matrix.csv`, and `audit.md`.
- **Mining Config TOML**: a single TOML file passed via `--config` whose
  schema (R10) controls beam width, max depth, complexity penalty λ,
  per-family quota, dedup ρ, walk-forward segment count, and OOS thresholds.

## Requirements

### Requirement 1: CLI commands

**User Story:** As a quant researcher, I want three CLI commands that drive
mining, listing, and inspection, so that I can run a sandbox session, browse
its outputs, and dive into a single candidate without writing scripts.

#### Acceptance Criteria

1. WHEN the user runs `alpha-find-v2 mine-factors --research-db <path> --start <YYYYMMDD> --end <YYYYMMDD> --config <toml>` with `--start` and `--end` as 8-digit calendar dates where `--start` is less than or equal to `--end`, THE Factor_Mining_Sandbox SHALL execute one mining run and write a run directory under `output/factor_lab/runs/<run_id>/` containing `manifest.json`, `candidates.jsonl`, `shortlist.json`, `correlation_matrix.csv`, and `audit.md`, where `<run_id>` is a non-empty identifier unique within `output/factor_lab/runs/`.
2. WHEN `mine-factors` finishes with all five required artifact files (`manifest.json`, `candidates.jsonl`, `shortlist.json`, `correlation_matrix.csv`, `audit.md`) written to the run directory and no unhandled error raised, THE Factor_Mining_Sandbox SHALL exit with status code 0 and print to stdout a single JSON object `{"run_id": "...", "run_dir": "..."}` where `run_dir` is the absolute path of the run directory.
3. IF the `--research-db` path does not exist or cannot be opened read-only, THEN THE Factor_Mining_Sandbox SHALL exit with status code 4, create no run directory, and print to stderr an error message that contains the supplied database path and indicates whether the path was missing or unreadable.
4. IF the `--config` path does not exist or fails schema validation against the mining-config TOML schema defined in R10, THEN THE Factor_Mining_Sandbox SHALL exit with status code 2, create no run directory, and print to stderr an error message containing the first schema violation found and the offending field path.
5. WHEN the user runs `alpha-find-v2 list-factor-candidates`, THE Factor_Mining_Sandbox SHALL scan `output/factor_lab/registry.json`, exit with status code 0, and print to stdout a JSON array sorted by `run_at` descending where each row carries the fields `run_id`, `run_at`, `candidate_count`, `accepted_count`, and `families_present`.
6. WHERE the user supplies `--family <name>` to `list-factor-candidates`, THE Factor_Mining_Sandbox SHALL filter rows to runs whose `families_present` contains `<name>` matched case-sensitively, and SHALL return an empty JSON array if no run matches.
7. WHERE the user supplies `--min-ic-ir <float>` to `list-factor-candidates`, THE Factor_Mining_Sandbox SHALL filter rows to runs whose shortlist contains at least one candidate with mean OOS IC_IR greater than or equal to the supplied threshold, and when `--family` is also supplied SHALL apply both filters as a logical AND.
8. WHEN the user runs `alpha-find-v2 inspect-candidate <run_id> <expr_id>` and `<run_id>` resolves to an existing run directory under `output/factor_lab/runs/` and `<expr_id>` is present in that run's `candidates.jsonl`, THE Factor_Mining_Sandbox SHALL invoke the Stage 2 `evaluate_descriptor` pipeline against the named expression as an ad-hoc descriptor, write the full set of artifact files that a Stage 2 evaluation produces into `output/factor_lab/runs/<run_id>/inspections/<expr_id>/`, and exit with status code 0.
9. IF the supplied `<run_id>` directory does not exist under `output/factor_lab/runs/`, THEN THE Factor_Mining_Sandbox SHALL exit with status code 4, create no inspection directory, and print to stderr the missing run directory path.
10. IF the supplied `<expr_id>` is not present in the run's `candidates.jsonl`, THEN THE Factor_Mining_Sandbox SHALL exit with status code 5, create no inspection directory, and print to stderr the list of candidate ids known to that run.
11. IF `--start` or `--end` is not a valid 8-digit calendar date in YYYYMMDD form, or `--start` is greater than `--end`, THEN THE Factor_Mining_Sandbox SHALL exit with status code 2, create no run directory, and print to stderr an error message naming the offending argument and its supplied value.
12. IF `output/factor_lab/registry.json` does not exist or contains zero registered runs when `list-factor-candidates` is invoked, THEN THE Factor_Mining_Sandbox SHALL exit with status code 0 and print an empty JSON array `[]` to stdout.
13. IF the Stage 2 `evaluate_descriptor` pipeline raises an error during `inspect-candidate` after the run directory and candidate id have been validated, THEN THE Factor_Mining_Sandbox SHALL exit with a non-zero status code other than 2, 4, and 5, create no partial inspection directory at `output/factor_lab/runs/<run_id>/inspections/<expr_id>/`, and print to stderr an error message naming the failing pipeline stage.

### Requirement 2: DSL grammar and operator whitelist

**User Story:** As a quant researcher, I want the expression DSL fixed to a
small, audited operator set with a window-size whitelist and a depth limit,
so that the search space stays interpretable, A-share-realistic, and free of
free-form symbolic-miner pathologies.

#### Acceptance Criteria

1. THE Expression_DSL SHALL accept exactly these 6 time-series operators and
   no others: `lag(x, N)`, `delta(x, N)`, `rolling_mean(x, N)`,
   `rolling_std(x, N)`, `rolling_max(x, N)`, `rolling_min(x, N)`.
2. THE Expression_DSL SHALL accept exactly these 4 cross-section operators
   and no others: `cs_rank(x)`, `cs_zscore(x)`, `cs_demean(x)`,
   `cs_industry_demean(x)`.
3. THE Expression_DSL SHALL accept exactly these 5 arithmetic operators and
   no others: `+`, `-`, `*`, `/`, `log`.
4. THE Expression_DSL SHALL accept exactly these 5 leaf fields and no
   others: `close_adj`, `open`, `turnover_value_cny`, `pe`, `pb`.
5. THE Expression_DSL SHALL accept a window-size argument `N` only when `N`
   is a positive integer literal drawn from the whitelist
   `{5, 10, 20, 60, 120, 250}`, and SHALL reject any expression whose `N`
   argument is a non-integer, a negative or zero value, a non-literal
   sub-expression, or any integer outside that whitelist.
6. THE Expression_DSL SHALL reject any expression whose abstract syntax tree
   contains more than 5 nodes, where one node is counted for each operator
   application from clauses 1, 2, and 3 (regardless of arity) and for each
   leaf field reference from clause 4, and where window-size literals `N`
   from clause 5 SHALL NOT be counted as nodes.
7. IF an expression contains a time-series operator (any of `lag`, `delta`,
   `rolling_mean`, `rolling_std`, `rolling_max`, `rolling_min`) whose direct
   or transitive argument contains a cross-section operator (any of
   `cs_rank`, `cs_zscore`, `cs_demean`, `cs_industry_demean`), THEN THE
   Expression_DSL SHALL reject the expression as a forbidden composition
   that would leak across dates.
8. THE Expression_DSL SHALL allow cross-section operators to wrap
   time-series operators (the `cs_*(time_series_op(...))` direction).
9. THE Expression_DSL SHALL reject any expression containing constructs
   outside the operator whitelist, including conditional branches, loops,
   user-defined functions, and operator names not enumerated in clauses 1
   through 4.
10. WHEN an expression is rejected by any of clauses 1 through 9 or
    clause 12, THE Expression_DSL SHALL emit a structured rejection record
    that names the violated clause number, identifies the offending
    sub-expression by its position in the input, and states a rejection
    reason describing which whitelist or rule was violated, without
    prescribing exact human-readable error message text.
11. WHEN the user supplies `pe` or `pb` as a leaf field, THE Expression_DSL
    SHALL bind the leaf to `daily_basic.pe` or `daily_basic.pb` respectively
    from the research database, treating non-positive values as missing.
12. IF an operator from clauses 1, 2, or 3 is applied with an argument count
    other than its declared arity (time-series operators in clause 1 require
    exactly 2 arguments, cross-section operators in clause 2 and the `log`
    operator in clause 3 require exactly 1 argument, and the binary
    arithmetic operators `+`, `-`, `*`, `/` in clause 3 require exactly 2
    operands), THEN THE Expression_DSL SHALL reject the expression as an
    arity violation.

### Requirement 3: Search algorithm

**User Story:** As a quant researcher, I want beam search over the DSL with a
random-sampling baseline running in parallel, so that I have a controlled
explore-vs-exploit balance and a chance-rate reference.

#### Acceptance Criteria

1. THE Factor_Mining_Sandbox SHALL grow expressions layer by layer from
   depth 1 through depth `max_depth` (default 5, allowed range 1 to 10),
   and at the end of each layer SHALL retain at most `beam_width`
   candidates (default 20, allowed range 1 to 1000) selected by train-set
   IC_IR in descending order, breaking ties first by ascending expression
   length and then by ascending lexicographic order of the expression
   string.
2. WHEN ranking candidates within a beam-search layer, THE
   Factor_Mining_Sandbox SHALL use train-set IC_IR computed by the Stage 2
   evaluation pipeline restricted to the configured train window, and
   SHALL treat candidates whose IC_IR is NaN, non-finite, or whose
   evaluation raised an error as ineligible for retention in the beam.
3. THE Factor_Mining_Sandbox SHALL also draw exactly `random_sample_size`
   expressions (default 1000, allowed range 1 to 100000) by uniform
   sampling with replacement over the same DSL grammar restricted to
   depths 1 through `max_depth`, and SHALL evaluate each drawn expression
   on the same train window and the same OOS window used for the
   beam-search candidates.
4. THE Factor_Mining_Sandbox SHALL persist beam-search candidates and
   random-sample candidates in `candidates.jsonl` such that each row
   carries a `sources` field whose value is a non-empty list containing
   one or both of the literal values `beam` and `random`.
5. THE Factor_Mining_Sandbox SHALL NOT use genetic-programming search,
   reinforcement-learning search, or any search method other than beam
   search and uniform random sampling.
6. WHEN the same expression is generated by both beam search and random
   sampling within a single run, THE Factor_Mining_Sandbox SHALL evaluate
   the expression exactly once and SHALL persist a single record in
   `candidates.jsonl` whose `sources` list contains both `beam` and
   `random`.
7. IF the random-sampling stream's accepted-rate over a run is greater
   than or equal to the beam-search stream's accepted-rate, where each
   stream's accepted-rate equals the count of that stream's expressions
   passing the Stage 2 acceptance gates divided by the count of that
   stream's evaluated expressions, THEN THE Factor_Mining_Sandbox SHALL
   include a top-level `beam_underperforms_random` warning in
   `manifest.json`.
8. WHEN drawing expressions for either the beam-search stream or the
   random-sampling stream, THE Factor_Mining_Sandbox SHALL derive all
   stochastic choices, including beam-search tie-breaking randomness if
   any, from the `seed` value recorded in `manifest.json` so that
   re-running the same `--config` against the same database produces the
   same set of evaluated expressions and the same per-record `sources`
   assignments.

### Requirement 4: Family classification and quota

**User Story:** As a quant researcher, I want every candidate assigned to one of five families with a per-family quota, so that the shortlist is diversified across mechanism types instead of being dominated by one family.

#### Acceptance Criteria

1. WHEN the Family_Classifier evaluates an expression, THE Family_Classifier SHALL assign exactly one family from the set `{trend, volatility, volume, value, cross_momentum}` to that expression and emit the assignment as a single string-valued `family` field.
2. WHEN the Family_Classifier evaluates an expression whose parsed tree contains an operator node named `cs_rank` or `cs_zscore`, THE Family_Classifier SHALL classify the expression as `cross_momentum`.
3. WHEN the Family_Classifier evaluates an expression to which clause 2 does not apply AND whose leaf set is a non-empty subset of `{pe, pb}` OR whose tree contains an operator node named `cs_industry_demean`, THE Family_Classifier SHALL classify the expression as `value`.
4. WHEN the Family_Classifier evaluates an expression to which clauses 2 and 3 do not apply AND whose leaf set contains `turnover_value_cny`, THE Family_Classifier SHALL classify the expression as `volume`.
5. WHEN the Family_Classifier evaluates an expression to which clauses 2, 3, and 4 do not apply AND whose tree contains an operator node named `rolling_std`, THE Family_Classifier SHALL classify the expression as `volatility`.
6. WHEN the Family_Classifier evaluates an expression to which clauses 2, 3, 4, and 5 do not apply AND whose tree contains an operator node named `delta` or `lag` AND whose leaf set contains `close_adj`, THE Family_Classifier SHALL classify the expression as `trend`.
7. IF none of clauses 2 through 6 apply to an expression, THEN THE Family_Classifier SHALL reject the expression without performing OOS evaluation and SHALL record a single record with `status = "rejected_family_unclassifiable"`, the original expression string, and `family = null` in `candidates.jsonl`.
8. WHEN train-set evaluation completes for a family, THE Factor_Mining_Sandbox SHALL admit at most `family_quota` candidates per family (default value 5, configurable integer in the closed range 1 to 50) into OOS evaluation, selected by train-set IC_IR in descending order, with ties broken by the lexicographic ascending order of the expression string.
9. WHILE Stage 3 is executing, THE Factor_Mining_Sandbox SHALL NOT register, evaluate, or admit any candidate whose assigned family is not in `{trend, volatility, volume, value, cross_momentum}`, and in particular SHALL NOT process any candidate whose family equals `quality`.
10. WHEN a candidate is excluded because its family's admitted count has already reached `family_quota`, THE Factor_Mining_Sandbox SHALL record a single record in `candidates.jsonl` containing the expression string, `status = "rejected_quota"`, and the family name that filled the quota, and SHALL NOT perform OOS evaluation on that candidate.
11. WHEN the Family_Classifier classifies the same expression string more than once within a run or across runs, THE Family_Classifier SHALL return the identical family assignment for every invocation, independent of evaluation order, candidate count, or system clock.
12. IF the Family_Classifier cannot parse an expression string into a tree, THEN THE Family_Classifier SHALL reject the expression without assigning a family and SHALL record a single record with `status = "rejected_family_unclassifiable"`, the original expression string, and `family = null` in `candidates.jsonl`.

### Requirement 5: Walk-forward evaluation protocol

**User Story:** As a quant researcher, I want every shortlisted candidate to
survive an anchored walk-forward with strict OOS thresholds, so that I do not
ship signals that only worked on a single training window.

#### Acceptance Criteria

1. THE Walk_Forward_Evaluator SHALL split the user-supplied `--start` to
   `--end` window into exactly `walk_forward_segments` anchored segments
   (default 3), where each segment k (with k starting at 1) has a fixed
   start date equal to `--start`, a train end date equal to
   `--start + (k × oos_window_months)`, and an OOS window of exactly
   `oos_window_months` trade-calendar months (default 6) immediately
   following the train end.
2. IF any segment's train window contains fewer than 24 trade-calendar
   months of data, OR IF the last segment's computed OOS window end date
   exceeds `--end`, THEN THE Walk_Forward_Evaluator SHALL abort the run
   without evaluating any candidate and SHALL emit an error indication
   identifying the offending segment index and the specific failing
   precondition.
3. WHEN evaluating a candidate on a segment, THE Walk_Forward_Evaluator
   SHALL invoke the Stage 2 `evaluate_descriptor` pipeline read-only on
   the segment's train and OOS sub-windows and SHALL record per-segment
   train IC_IR, OOS IC_IR, OOS IC mean, and OOS coverage to
   `candidates.jsonl`.
4. WHEN all segments for a candidate have been evaluated, IF for every
   segment OOS IC_IR is greater than or equal to `oos_ic_ir_threshold`
   (default 0.30) AND OOS IC mean is strictly greater than 0, THEN THE
   Walk_Forward_Evaluator SHALL record the candidate's status as
   `accepted_oos` in `candidates.jsonl` along with each segment's
   OOS IC_IR and OOS IC mean values.
5. IF a candidate fails any segment's OOS thresholds defined in
   criterion 4, THEN THE Walk_Forward_Evaluator SHALL record the
   candidate's status as `rejected_oos` in `candidates.jsonl` along with
   the index of the first failing segment and that segment's OOS IC_IR
   value and OOS IC mean value.
6. THE Walk_Forward_Evaluator SHALL apply the Stage 2 tradeability filter
   (suspend, limit-up, limit-down) to entry and exit observations on every
   segment using the existing `factor_evaluation` primitives, and SHALL
   NOT re-implement tradeability logic.
7. THE Walk_Forward_Evaluator SHALL use forward returns produced by the
   Stage 2 `forward_returns` SQL helper at the configured
   `primary_horizon` (default 20 trade-calendar days) on every segment.
8. THE Walk_Forward_Evaluator SHALL resolve the universe per segment
   using the Stage 2 `UniverseResolver` configured by the mining-config
   TOML (default `investable_a_share_core`).

### Requirement 6: Complexity penalty and correlation dedup

**User Story:** As a quant researcher, I want a complexity penalty on fitness and a correlation dedup pass against registered descriptors and earlier candidates, so that the shortlist favors parsimonious, non-redundant signals.

#### Acceptance Criteria

1. THE Factor_Mining_Sandbox SHALL compute each candidate's fitness as `fitness = train_IC_IR − λ × node_count`, where `node_count` is the integer count of nodes in the expression tree (operator applications plus leaf references, as defined in R2 clause 6) bounded by `1 ≤ node_count ≤ 5`, and `λ` is a non-negative real number in the closed interval [0.0, 1.0] with default 0.05.
2. IF a candidate's `train_IC_IR` is NaN, infinite, or otherwise not a finite real number, THEN THE Factor_Mining_Sandbox SHALL set that candidate's `fitness` to `null`, SHALL exclude that candidate from beam-search retention and from per-family quota selection, and SHALL record `status = "rejected_oos"` with the reason `train_ic_ir_undefined` in `candidates.jsonl`.
3. THE Factor_Mining_Sandbox SHALL use `fitness` (not raw `train_IC_IR`) as the ranking key for both beam-search layer pruning and per-family quota selection, breaking ties first by ascending `node_count` and then by ascending lexicographic order of the canonical expression string.
4. THE Correlation_Dedup_Stage SHALL run after walk-forward acceptance and SHALL compute the absolute Pearson correlation of each candidate's score series against (a) every registered descriptor's score series over the run's train window AND (b) every already-accepted candidate's score series over the run's train window, restricting each pairwise computation to overlapping non-NaN `(trade_date, security_id)` observations.
5. IF the overlap defined in clause 4 contains fewer than `dedup_min_obs` non-NaN observations (default 60, configurable integer ≥ 1) OR either side has zero variance over the overlap, THEN THE Correlation_Dedup_Stage SHALL treat that pair's correlation as undefined and SHALL write the empty string in the corresponding cell of `correlation_matrix.csv` without raising an error or rejecting the candidate on the basis of that pair alone.
6. IF any defined correlation computed in clause 4 is strictly greater than `dedup_rho` (default 0.85, configurable real number in the closed interval [0.0, 1.0]), THEN THE Correlation_Dedup_Stage SHALL reject the candidate, record its status as `rejected_correlation` in `candidates.jsonl`, and record both the highest correlated reference id and the correlation value rounded to 6 decimal places.
7. THE Correlation_Dedup_Stage SHALL process candidates in `fitness` descending order, breaking ties using the same rule as clause 3, so that higher-fitness candidates are accepted before lower-fitness candidates with which they correlate.
8. WHEN the Correlation_Dedup_Stage finishes processing all candidates, THE Factor_Mining_Sandbox SHALL write the full correlation matrix (rows: every evaluated candidate; columns: every registered descriptor followed by every accepted candidate) to `correlation_matrix.csv` regardless of which candidates were rejected.
9. THE Correlation_Dedup_Stage SHALL NOT re-evaluate registered descriptors. WHEN a registered descriptor's score series for the train window is not already cached, THE Correlation_Dedup_Stage SHALL compute it once via the Stage 2 `descriptor_compute` registry and cache it for the remainder of the run.

### Requirement 7: Output schema

**User Story:** As a quant researcher, I want every run to produce a predictable, machine-readable artifact set plus a human-review template, so that I can audit, diff, and promote candidates outside the tool.

#### Acceptance Criteria

1. WHEN a run completes successfully, THE Factor_Mining_Sandbox SHALL write `manifest.json` under the run directory containing the keys `run_id`, `run_at` (UTC ISO-8601 with timezone offset `Z`, millisecond precision), `seed` (non-negative integer), `git_sha` (40-character hex string, or the literal string `unknown` when the working tree has no git metadata), `config_snapshot`, `start_date` (date in `YYYY-MM-DD`), `end_date` (date in `YYYY-MM-DD`), `walk_forward_segments` (integer, 1 to 32 inclusive), `universe_id`, `random_sample_size` (integer, 0 or greater), `total_candidates_evaluated` (integer, 0 or greater), `accepted_count` (integer, 0 or greater), `rejected_oos_count` (integer, 0 or greater), `rejected_correlation_count` (integer, 0 or greater), `rejected_quota_count` (integer, 0 or greater), and `duration_seconds` (number, 0 or greater, rounded to 3 decimal places), with every listed key present in the file even when its value is zero or empty.
2. WHEN a run completes, THE Factor_Mining_Sandbox SHALL write `candidates.jsonl` containing exactly one JSON object per evaluated expression (one object per line, UTF-8 encoded, terminated by `\n`), and each object SHALL include the keys `expr_id`, `expression`, `node_count` (integer, 1 to 64 inclusive), `family`, `sources` (array of strings, may be empty), `train_ic_ir` (number or `null` when not computable), `fitness` (number or `null` when not computable), `oos_segments` (array containing one entry per walk-forward segment), and `status` (one of `accepted`, `rejected_oos`, `rejected_correlation`, `rejected_quota`, `rejected_family_unclassifiable`).
3. WHEN a run completes, THE Factor_Mining_Sandbox SHALL write `shortlist.json` containing only candidates whose `status` equals `accepted`, ordered by `fitness` descending with `expr_id` ascending as the tiebreaker, each row carrying every field defined for `candidates.jsonl` plus `family_rank` (integer, 1 or greater) giving its rank within its family by the same ordering.
4. WHEN a run completes, THE Factor_Mining_Sandbox SHALL write `correlation_matrix.csv` as UTF-8 text with one header row, one row per evaluated candidate keyed by `expr_id`, and one column per registered descriptor followed by one column per accepted candidate, where each cell holds the Pearson correlation over the train window formatted to 6 decimal places in the range -1.000000 to 1.000000 inclusive, and any cell whose Pearson correlation is undefined (NaN, insufficient overlap, or zero variance on either side) is written as the empty string.
5. WHEN a run completes, THE Factor_Mining_Sandbox SHALL write `audit.md` containing one section per accepted candidate with the template fields `expression`, `family`, `node_count`, per-segment OOS metrics for every segment listed in `oos_segments`, `economic_story` (placeholder string `TODO`), `risk_notes` (placeholder string `TODO`), and `suggested_promote` (placeholder restricted to one of the literal values `yes`, `no`, `needs_more_data`, defaulting to `needs_more_data`).
6. WHEN a run completes successfully, THE Factor_Mining_Sandbox SHALL append exactly one entry to `output/factor_lab/registry.json` containing `run_id`, `run_at`, `run_dir`, `candidate_count` (integer, 0 or greater), `accepted_count` (integer, 0 or greater), and `families_present` (array of family names, may be empty), preserving all pre-existing entries in their original order.
7. IF any artifact required by criteria 1 through 6 cannot be written (write failure, serialization error, or missing required field), THEN THE Factor_Mining_Sandbox SHALL abort the run, leave `output/factor_lab/registry.json` unchanged, surface an error indicating which artifact failed, and not leave a partial entry in the registry.
8. WHEN `inspect-candidate` is invoked, THE Factor_Mining_Sandbox SHALL write the Stage-2-style report under `output/factor_lab/runs/<run_id>/inspections/<expr_id>/` containing `report.json` and `report.md` produced by the Stage 2 report writer.
9. IF `inspect-candidate` is invoked with a `run_id` or `expr_id` that does not exist in the corresponding `candidates.jsonl`, THEN THE Factor_Mining_Sandbox SHALL exit without writing any inspection artifact and surface an error indicating which identifier was not found.
10. THE Factor_Mining_Sandbox SHALL emit POSIX-style path strings (forward-slash separators, no drive letters, no backslashes) in every JSON, JSONL, and TOML artifact regardless of host operating system.

### Requirement 8: Hard isolation from production

**User Story:** As the maintainer of the V2 doctrine, I want the sandbox
hard-isolated from the production descriptor registry, so that an automated
or accidental promotion is structurally impossible.

#### Acceptance Criteria

1. THE Factor_Mining_Sandbox SHALL NOT call `descriptor_compute.register()` or any function that mutates the Stage 2 descriptor registry, including any function whose fully qualified name resides under `src/alpha_find_v2/factor_evaluation/` and whose effect is to add, update, or remove a descriptor entry.
2. THE Factor_Mining_Sandbox SHALL NOT write, create, modify, rename, or delete any file or subdirectory under `config/descriptors/` or `config/descriptor_sets/`, including any file with a `.toml` extension or any other extension.
3. THE Factor_Mining_Sandbox SHALL NOT write, create, modify, rename, or delete any file under `src/alpha_find_v2/factor_evaluation/` or any other Stage 2 module directory.
4. THE Factor_Mining_Sandbox SHALL write all run output exclusively under `output/factor_lab/` and SHALL NOT write, create, modify, rename, or delete any file or subdirectory under `output/descriptor_evaluation/`, `output/research_source.duckdb`, `output/raw.duckdb`, or any path outside `output/factor_lab/`.
5. IF the sandbox detects, at startup, that its process working directory or its configured output root, after resolving symbolic links and `..` segments to an absolute path, is not equal to or a descendant of the absolute path of `output/factor_lab/`, THEN THE Factor_Mining_Sandbox SHALL exit with status code 6, write an error message to standard error indicating the resolved path and the expected path, and refuse to perform any further action.
6. WHEN the sandbox completes a run, THE Factor_Mining_Sandbox SHALL write to that run's `audit.md` a section titled "Promotion Path" that documents the promotion as a manual workflow consisting of, in order: (a) human authoring of the `economic_story` and `risk_notes` fields, (b) opening a pull request that adds a new `config/descriptors/<id>.toml` file and a new compute function in `factor_evaluation/descriptor_compute.py`, and (c) human review and merge of that pull request.
7. THE Factor_Mining_Sandbox SHALL NOT include any code path that, given any combination of CLI flags, environment variables, or configuration file inputs, automates any of the three steps described in clause 6.
8. IF an internal code path of the sandbox violates any of clauses 1 through 4 during execution of the sandbox test suite, THEN THE sandbox test suite SHALL fail with an isolation violation message that names the violated clause number, the violating call site as a fully qualified module path and line number, and the resource (file path or function name) that was accessed.

### Requirement 9: Reuse of Stage 2 evaluation pipeline

**User Story:** As a maintainer, I want the sandbox to reuse Stage 2's evaluation primitives read-only, so that IC, decile, walk-forward, universe, and tradeability math is computed in exactly one place.

#### Acceptance Criteria

1. THE Factor_Mining_Sandbox SHALL compute IC, IC_IR, decile returns, forward returns, universe membership, and tradeability filters by invoking the Stage 2 functions `evaluate_descriptor`, `forward_returns`, `UniverseResolver` (and its `BenchmarkUniverseResolver` and `InvestableCoreUniverseResolver` subclasses), and the Stage 2 tradeability helpers.
2. THE Factor_Mining_Sandbox SHALL NOT re-implement IC, IC_IR, decile, monotonicity, walk-forward, universe-resolution, or tradeability logic.
3. WHEN evaluating a candidate expression, THE Factor_Mining_Sandbox SHALL wrap that expression in an ad-hoc `DescriptorComputeSpec` whose `fn` returns the long-format frame produced by the DSL evaluator with exactly the columns `trade_date`, `security_id`, and `descriptor_value`, and SHALL pass that spec to `evaluate_descriptor`.
4. WHILE constructing an ad-hoc `DescriptorComputeSpec` for a candidate expression, THE Factor_Mining_Sandbox SHALL NOT register that spec in the global descriptor registry, and the spec SHALL remain in scope only for the current evaluation call.
5. IF a Stage 2 symbol that the sandbox imports from `factor_evaluation` (specifically `evaluate_descriptor`, `forward_returns`, `UniverseResolver`, `BenchmarkUniverseResolver`, `InvestableCoreUniverseResolver`, or any tradeability helper imported by the sandbox) is absent at import time, or has an incompatible call signature at the sandbox's call site, THEN THE Factor_Mining_Sandbox SHALL raise an exception at that import or call site, the raised exception SHALL include the name of the missing or incompatible symbol, the current sandbox operation SHALL abort without producing evaluation results, and THE Factor_Mining_Sandbox SHALL NOT execute any private re-implementation of the affected IC, IC_IR, decile, monotonicity, walk-forward, universe-resolution, or tradeability logic as a fallback.
6. THE Factor_Mining_Sandbox SHALL pass to `evaluate_descriptor` the same cost-model path that Stage 2 uses by default, which is `config/cost_models/base_a_share_cash.toml`.

### Requirement 10: Configurability

**User Story:** As a quant researcher, I want every numerical threshold, quota, and limit exposed in a single mining-config TOML file, so that I can tune a run without editing code.

#### Acceptance Criteria

1. THE Mining_Config_TOML SHALL define a `[search]` table with keys `beam_width` (positive integer, default 20, allowed range 1 to 1000), `max_depth` (positive integer, default 5, allowed range 1 to 5), `random_sample_size` (non-negative integer, default 1000, allowed range 0 to 100000), and `seed` (integer, default 42, allowed range 0 to 2^32 − 1).
2. THE Mining_Config_TOML SHALL define a `[fitness]` table with key `complexity_lambda` (real number, default 0.05, allowed range 0.0 to 1.0).
3. THE Mining_Config_TOML SHALL define a `[family]` table with key `quota_per_family` (positive integer, default 5, allowed range 1 to 50).
4. THE Mining_Config_TOML SHALL define a `[walk_forward]` table with keys `segments` (positive integer, default 3, allowed range 1 to 32), `oos_window_months` (positive integer, default 6, allowed range 1 to 60), `min_train_months` (positive integer, default 24, allowed range 6 to 120), `oos_ic_ir_threshold` (real number, default 0.30, allowed range 0.0 to 5.0), and `primary_horizon_days` (positive integer, default 20, allowed range 1 to 250).
5. THE Mining_Config_TOML SHALL define a `[dedup]` table with keys `rho_threshold` (real number, default 0.85, allowed range 0.0 to 1.0) and `min_obs` (positive integer, default 60, allowed range 1 to 5000).
6. THE Mining_Config_TOML SHALL define a `[universe]` table with key `id` (string, default `investable_a_share_core`).
7. WHEN the supplied TOML omits any required table or key listed in clauses 1 through 6, THE Factor_Mining_Sandbox SHALL substitute the default value for that key, SHALL include the substituted key path and applied default value in `manifest.json` under `config_defaults_applied`, and SHALL still complete the run.
8. IF the supplied TOML contains a key whose path is not listed in clauses 1 through 6, THEN THE Factor_Mining_Sandbox SHALL exit with status code 2, write to standard error a message naming the unknown key path including its enclosing table, and SHALL NOT create a run directory or write `manifest.json`.
9. IF any value supplied for a key in clauses 1 through 6 falls outside that key's allowed range, THEN THE Factor_Mining_Sandbox SHALL exit with status code 2, write to standard error a message naming the offending key path and the supplied value, and SHALL NOT create a run directory or write `manifest.json`.
10. IF any value supplied for a key in clauses 1 through 6 has a type incompatible with that key's declared type (for example a string supplied where an integer is required), THEN THE Factor_Mining_Sandbox SHALL exit with status code 2, write to standard error a message naming the offending key path, the supplied value, and the expected type, and SHALL NOT create a run directory or write `manifest.json`.
11. IF the supplied `[family]` table contains a family identifier whose value equals `quality` (case-insensitive), THEN THE Factor_Mining_Sandbox SHALL exit with status code 2 per R13 clause 1 and SHALL NOT create a run directory or write `manifest.json`.
12. IF the supplied `--config` path does not exist, cannot be opened for reading, or does not parse as valid TOML, THEN THE Factor_Mining_Sandbox SHALL exit with status code 2, write to standard error a message naming the supplied path and indicating which failure occurred, and SHALL NOT create a run directory or write `manifest.json`.
13. WHEN configuration validation completes successfully, THE Factor_Mining_Sandbox SHALL embed the resolved configuration (defaults plus user overrides, after type coercion) verbatim in `manifest.json` under `config_snapshot` such that re-parsing `config_snapshot` reproduces the resolved configuration exactly.

### Requirement 11: Performance and time budget

**User Story:** As a personal-stack user, I want a mining run on a synthetic fixture to complete quickly enough to use in tests, and a real research-database run to finish within a single coffee break, so that the sandbox stays usable on a developer laptop.

#### Acceptance Criteria

1. WHEN the sandbox is run against the synthetic fixture (`tests/_fixtures/synth_research_db.py`) with default config on a reference developer laptop with at least 4 physical CPU cores, 16 GB RAM, and a solid-state drive, THE Factor_Mining_Sandbox SHALL complete the run, measured as wall-clock elapsed time from CLI invocation to final artifact write, in under 5 minutes (300 seconds).
2. WHEN the sandbox is run against the full research database with default config and a 4-year window on a reference developer laptop with at least 4 physical CPU cores, 16 GB RAM, and a solid-state drive, THE Factor_Mining_Sandbox SHALL complete the run, measured as wall-clock elapsed time from CLI invocation to final artifact write, in under 30 minutes (1800 seconds).
3. THE Factor_Mining_Sandbox SHALL cache evaluated expression score series within a single run, keyed by a canonical string form of the expression, such that any two expressions with identical canonical form produced by either beam search or random sampling are computed at most once.
4. WHEN a segment's metrics are recorded during a run, THE Factor_Mining_Sandbox SHALL release that segment's intermediate frames before processing the next segment.
5. THE Factor_Mining_Sandbox SHALL keep peak resident set size of the sandbox process below 4 GB (4096 MB) when running against the synthetic fixture and below 8 GB (8192 MB) when running against the full research database.
6. IF a run exceeds 2x its declared time budget, THEN THE Factor_Mining_Sandbox SHALL emit a structured warning entry in `manifest.json` under `time_budget_exceeded` containing the declared budget value and the observed elapsed time.
7. IF a run exceeds 2x its declared time budget, THEN THE Factor_Mining_Sandbox SHALL preserve on disk all artifacts produced up to the point the budget violation is detected, without deletion or rollback.

### Requirement 12: Reproducibility

**User Story:** As a researcher, I want re-running the same config against the same database to produce the same shortlist, so that I can diff candidate sets across descriptor-registry changes.

#### Acceptance Criteria

1. WHEN the same `--config` file (compared by SHA-256 hash of its bytes) is executed twice against the same research database snapshot under the same recorded `git_sha` and `seed`, THE Factor_Mining_Sandbox SHALL produce `candidates.jsonl` and `shortlist.json` outputs that are byte-identical across the two runs when the fields `run_id`, `run_at`, and `duration_seconds` are excluded from the comparison.
2. THE Factor_Mining_Sandbox SHALL initialize every random draw from the integer `seed` value in the `[search]` section of the resolved config and SHALL record the resolved `seed` value in `manifest.json`.
3. IF the `seed` field is absent from the `[search]` section of the resolved config, or is not an integer in the inclusive range [0, 2^32 - 1], THEN THE Factor_Mining_Sandbox SHALL terminate with a non-zero exit code, emit an error indicating that `[search].seed` is missing or out of range, and SHALL NOT write `candidates.jsonl` or `shortlist.json`.
4. THE Factor_Mining_Sandbox SHALL record the 40-character lowercase hexadecimal git commit SHA of `HEAD` in the `git_sha` field of `manifest.json`.
5. IF the working tree contains uncommitted modifications, additions, deletions, or renames to tracked files, or contains untracked files that are not excluded by the repository's ignore rules, THEN THE Factor_Mining_Sandbox SHALL record `git_sha` in `manifest.json` as `"<sha>-dirty"` and SHALL emit a `dirty_working_tree` warning to standard error.
6. IF the working directory is not inside a git repository, THEN THE Factor_Mining_Sandbox SHALL terminate with a non-zero exit code, emit an error indicating that the git SHA cannot be resolved for reproducibility metadata, and SHALL NOT write `candidates.jsonl` or `shortlist.json`.

### Requirement 13: Out-of-scope guardrails

**User Story:** As the maintainer of the V2 doctrine, I want the sandbox to explicitly refuse known out-of-scope features, so that scope creep cannot silently slip in via a config flag.

#### Acceptance Criteria

1. THE Factor_Mining_Sandbox SHALL NOT support a `quality` family. IF a user-supplied config file or CLI flag contains a family identifier whose value matches `quality` (case-insensitive, after surrounding whitespace is trimmed), THEN THE Factor_Mining_Sandbox SHALL, before opening the Stage 1 audited research database, exit with status code 2 and print an error message that names the offending config key or CLI flag, states that the `quality` family requires `pit_fina_indicator`, and states that the family is deferred until the Tushare credit prerequisite is met; no run directory SHALL be created for the rejected invocation.
2. THE Factor_Mining_Sandbox SHALL NOT support genetic programming or reinforcement learning as search methods. IF a user-supplied config file or CLI flag contains a search-method identifier whose value names genetic programming or reinforcement learning (including common abbreviations such as `gp` or `rl`, matched case-insensitively after surrounding whitespace is trimmed), THEN THE Factor_Mining_Sandbox SHALL, before opening the Stage 1 audited research database, exit with status code 2 and print an error message that names the offending config key or CLI flag and the unsupported search method value; no run directory SHALL be created for the rejected invocation.
3. THE Factor_Mining_Sandbox SHALL NOT consume intraday bars, alternative-data feeds, news data, or any data source outside the Stage 1 audited research database. IF a user-supplied config file or CLI flag identifies a data source that is not registered in the Stage 1 audited research database, THEN THE Factor_Mining_Sandbox SHALL, before issuing any query against that data source, exit with status code 2 and print an error message that names the offending config key or CLI flag, names the rejected data source, and states that only Stage 1 audited research database sources are permitted; no run directory SHALL be created for the rejected invocation.
4. THE Factor_Mining_Sandbox SHALL NOT produce executable signals, sleeve artifacts, portfolio recipes, or any deliverable downstream of a registered descriptor, and THE Factor_Mining_Sandbox's only output SHALL be the run directory under `output/factor_lab/`. IF a user-supplied config file or CLI flag requests an output, artifact type, or downstream deliverable other than the run directory under `output/factor_lab/`, THEN THE Factor_Mining_Sandbox SHALL, before any output is written, exit with status code 2 and print an error message that names the offending config key or CLI flag, names the rejected output type, and states that the sandbox produces only the run directory under `output/factor_lab/`; no partial output SHALL be left on disk for the rejected invocation.

## Definition of Done

- All 13 requirements above pass automated tests against the synthetic
  research-DB fixture.
- A reference run against the synthetic fixture produces the full artifact
  set (`manifest.json`, `candidates.jsonl`, `shortlist.json`,
  `correlation_matrix.csv`, `audit.md`) and updates
  `output/factor_lab/registry.json`.
- An isolation-violation test asserts that no sandbox code path writes
  outside `output/factor_lab/` and no sandbox code path imports a mutating
  function from `factor_evaluation.descriptor_compute`.
- A grammar-rejection test asserts that every banned composition in R2
  (clauses 5, 6, 7, 9) is rejected with the expected structured rejection
  record.
- A reproducibility test asserts byte-identical `candidates.jsonl` (modulo
  `run_id`, `run_at`, `duration_seconds`) across two runs of the same
  config.
- The Stage 2 evaluation suite (≥280 tests after Stage 2 landing) stays
  green; new Stage 3 tests are additive only.
