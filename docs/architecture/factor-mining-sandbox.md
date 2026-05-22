# Factor Mining Sandbox

## Scope

The Factor Mining Sandbox (`factor_lab`) is a candidate generator for new descriptor expressions.

What it **is**:

- a read-only consumer of the Stage 2 evaluation primitives (`evaluate_descriptor`, `forward_returns`, `universe_resolver`, tradeability filter) and the registered descriptor config TOMLs
- a generator of expression candidates with measurable train and out-of-sample predictive quality, evaluated via anchored walk-forward over the research database
- a writer of one run directory under `output/factor_lab/runs/<run_id>/` per invocation

What it is **not**:

- not an alpha registry — candidates are proposals, not descriptors
- not a promoter — it never calls `descriptor_compute.register()` or writes to `config/descriptors/`
- not a strategy — factor-table-as-strategy auto-wrappers are banned by `docs/migration/legacy-boundary.md`

The sandbox sits before the descriptor registry in the V2 research object chain:

```
mandate → thesis → descriptor set → sleeve → portfolio recipe → executable signal → decay record
```

Its output is a human-review packet. Humans decide whether to promote a candidate into a registered descriptor via a PR.

## CLI Commands

### `mine-factors`

Executes one mining run and writes a run directory.

```bash
alpha-find-v2 mine-factors \
    --research-db output/research_source.duckdb \
    --start 20220101 \
    --end 20251231 \
    --config config/mining/default.toml
```

Exits 0 on success and prints `{"run_id": "...", "run_dir": "..."}` to stdout. Exit codes: 2 for config/date errors, 4 for missing database, 6 for isolation violation.

### `list-factor-candidates`

Lists all registered runs from `output/factor_lab/registry.json`, sorted by run date descending.

```bash
# All runs
alpha-find-v2 list-factor-candidates

# Filter by family
alpha-find-v2 list-factor-candidates --family trend

# Filter by minimum OOS IC_IR (both filters apply as AND)
alpha-find-v2 list-factor-candidates --family trend --min-ic-ir 0.35
```

Always exits 0. Prints an empty JSON array `[]` when no runs are registered.

### `inspect-candidate`

Re-runs the full Stage 2 `evaluate_descriptor` pipeline against a specific candidate from an existing run and writes the evaluation report under `output/factor_lab/runs/<run_id>/inspections/<expr_id>/`.

```bash
alpha-find-v2 inspect-candidate <run_id> <expr_id>
```

Exit codes: 4 if `<run_id>` does not exist, 5 if `<expr_id>` is not in that run's `candidates.jsonl`.

## Run Directory Structure

Each completed run produces five artifacts under `output/factor_lab/runs/<run_id>/`:

| Artifact | Contents |
|----------|----------|
| `manifest.json` | Run metadata: `run_id`, `run_at` (UTC ISO-8601), `seed`, `git_sha`, `config_snapshot`, date range, walk-forward parameters, candidate counts, timing, and any warnings |
| `candidates.jsonl` | One JSON object per evaluated expression: expression string, family, sources (`beam`, `random`, or both), per-segment OOS metrics, fitness, and status |
| `shortlist.json` | Accepted candidates only (`status = "accepted_oos"`), ordered by fitness descending with `family_rank` added |
| `correlation_matrix.csv` | Pairwise Pearson correlations of every evaluated candidate against every registered descriptor and every accepted candidate over the train window; undefined cells written as empty strings |
| `audit.md` | Human-review template: one section per accepted candidate with `economic_story` and `risk_notes` TODO placeholders, plus the Promotion Path section |

All paths in JSON and JSONL artifacts use POSIX forward slashes regardless of host OS.

## Isolation Guarantees

The sandbox enforces hard isolation at the implementation and test levels.

**Writes only to `output/factor_lab/`** — the output root is resolved to an absolute path at startup and asserted to be under `output/factor_lab/`. Any misconfiguration exits with code 6 before creating any files.

**Never calls `descriptor_compute.register()`** — the function is never imported inside `src/alpha_find_v2/factor_lab/`. The test suite in `tests/factor_lab/test_isolation.py` performs an AST scan of every `.py` file in the package and fails the test if any import or call to `register` is introduced.

**Read-only Stage 2 consumption** — the sandbox constructs ad-hoc `DescriptorComputeSpec` instances for walk-forward evaluation and inspection. These specs are never passed to `register()`; they exist only for the duration of the evaluation call.

These guarantees mean that running `mine-factors` cannot alter the descriptor registry, cannot write outside the sandbox output directory, and cannot create any object that affects downstream sleeve or portfolio construction.

## Promotion Path

Promoting a candidate from the sandbox to a registered descriptor is a **manual workflow**. The sandbox prepares the audit packet; humans make every decision.

The `audit.md` template generated for each run documents the three-step process:

### Step 1 — Human authoring

Open `output/factor_lab/runs/<run_id>/audit.md` and fill in the two TODO sections for each candidate you want to promote:

- `economic_story`: explain what inefficiency the expression captures, why it should exist in A-shares, and what holding horizon fits the mechanism.
- `risk_notes`: describe data risks (PIT safety of inputs), crowding risk, regime sensitivity, and any other concerns.

Change `suggested_promote` from `needs_more_data` to `yes` when satisfied.

### Step 2 — Open a pull request

The PR must include two additions:

1. `config/descriptors/<expr_id>.toml` — the descriptor configuration file, referencing the expression, family, target, and cost model.
2. A compute function in `factor_evaluation/descriptor_compute.py` — the Python function that evaluates the descriptor against the research database.

Reference the `audit.md` path and the `run_id` in the PR description so the evidence trail is clear.

### Step 3 — Human review and merge

A second researcher reviews the economic story, risk notes, and OOS metrics before approving and merging the PR. The registered descriptor enters the Stage 2 pipeline only after the merge.

The sandbox is not involved in any of these steps.

## audit.md Template

The `audit.md` generated automatically by each run contains:

- One section per accepted candidate with expression string, family, node count, and per-segment OOS metrics (IC_IR and IC mean for each walk-forward segment).
- `economic_story: TODO` and `risk_notes: TODO` placeholder lines that the researcher must fill before opening a promotion PR.
- `suggested_promote: needs_more_data` as the default placeholder (must be changed to `yes` or `no` by the researcher).
- A **Promotion Path** section at the end of the file that reproduces the three-step workflow described above.

The template is defined in `src/alpha_find_v2/factor_lab/output.py` (`write_audit_md`). Requirements R7.5 and R8.6 govern its contents.
