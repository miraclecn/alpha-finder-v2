# Sleeve Artifact And Promotion Replay

## Why This Layer Exists

V2 now has portfolio construction, simulation, and promotion gates.

What it still needed was a strict research artifact between sleeve research and portfolio replay.

Without that artifact, teams drift back into old habits:

- notebook-only picks with no stable replay contract
- factor-level validation that never becomes a portfolio path
- candidate sleeve claims that are not tested against the actual production book

The missing boundary is now explicit:

`sleeve research artifact -> portfolio construction replay -> executable portfolio path -> promotion decision`

## Standard Sleeve Artifact

The canonical V2 research output is not a factor id and not a loose csv dump.

It is a sleeve artifact with one snapshot per decision date.

Each decision-date snapshot must carry:

- ranked names
- research score
- target weight before portfolio combination
- realized forward return on the executable target horizon
- industry label used by portfolio limits
- cost-model binding
- trade-state binding such as blocked entry or blocked exit

This is intentionally strict.

For the first V2 release, artifacts must already be normalized onto the portfolio decision calendar.
If a sleeve trades weekly while another sleeve refreshes twice a week, the research pipeline must still emit a valid snapshot for every portfolio decision date.

The promotion replay layer does not guess missing states and does not silently forward-fill sparse artifacts.
If a sleeve artifact is missing a decision date, replay fails.

That is the right discipline for a live trading system because capital is allocated on a calendar, not on a notebook author's memory of when a sleeve "usually" refreshes.

## Promotion Replay Contract

A sleeve promotion test is defined as a comparison between two explicit portfolio recipes:

- baseline recipe: the current admissible production book
- proposed recipe: the same book after the candidate sleeve is admitted and capital is re-budgeted

This is important.

V2 does not treat promotion as "add one more signal and hope the optimizer handles it."
Promotion means proposing a new capital allocation policy for the live book.

The replay loop is:

`artifacts -> constructor -> simulator -> evaluator -> promotion gate`

For each decision date, the replay engine:

1. reads the sleeve artifact snapshots required by the recipe
2. combines them with the portfolio constructor
3. simulates the executable holdings path after A-share trade constraints and costs
4. evaluates baseline and proposed portfolio paths
5. auto-computes marginal portfolio contribution
6. injects those marginal metrics into the promotion snapshot
7. evaluates the portfolio promotion gate

## What Gets Auto-Populated

The replay layer now auto-populates the portfolio-level marginal fields that previously had to be typed in manually:

- marginal IR delta
- marginal drawdown increase

That matters because promotion should be driven by realized combined-book evidence, not by manual spreadsheet transcription.

Other promotion inputs can still come from adjacent research lanes for now, including:

- regime pass flags
- correlation diagnostics
- stressed cost scenarios

Those remain valid external inputs until the corresponding research modules are formalized inside V2.

## Finance Logic Behind The Strict Calendar

The strict decision-calendar rule is deliberate, not incidental.

In a real A-share book:

- the portfolio must know what it intends to hold on every rebalance date
- blocked exits and blocked entries depend on that date's actual target book
- turnover and cost are path-dependent
- a slower sleeve still occupies capital on dates when a faster sleeve wants to trade

That means sparse sleeve outputs are not just inconvenient. They are economically ambiguous.

The artifact must already answer:

- what the sleeve still wants to own on that date
- what return is realized over the next holding interval
- whether the position is actually enterable or exitable

Only then can the portfolio replay be finance-honest.

## Persisted Artifact Contract

The in-memory dataclass is not enough.
V2 now also standardizes the on-disk replay contract.

For the first release:

- sleeve artifacts are stored as JSON
- each file declares `schema_version = 1`
- each file declares `artifact_type = "sleeve_research_artifact"`
- each file carries `sleeve_id`, `mandate_id`, `target_id`, and `steps`
- each step carries `trade_date` plus the ordered name records used by replay

The record payload is intentionally close to the replay engine:

- `asset_id`
- `rank`
- `score`
- `target_weight`
- `realized_return`
- `cost_model_id`
- `industry`
- `trade_state.can_enter`
- `trade_state.can_exit`

This is deliberate.
The persisted artifact should already answer the capital-allocation question without notebook-only context.

## Artifact Build Boundary

Hand-written artifact JSON is not the long-term operating model.

V2 now also defines a narrow upstream build boundary:

- sleeve artifact build case: TOML binding the sleeve config, research input path, and output path
- sleeve research observation input: JSON carrying normalized decision-calendar observations

The observation input is intentionally one step earlier than the replay artifact.
It contains the research fields needed to derive the artifact rather than the finished replay payload:

- rank, score, and pre-combination target weight
- entry and exit opens for the executable target clock
- either residual components or factor exposures plus factor-return snapshots
- entry and exit trade-leg states
- industry and optional cost-model overrides

That keeps the audit trail honest:

`research observation input -> artifact build case -> persisted sleeve artifact -> replay`

Replay and deployment should consume the same persisted artifact.
They should not fork their own ad hoc label logic.

## Replay Case Contract

V2 also now defines a persisted replay case.

The replay case is a TOML file because it mostly binds existing versioned objects:

- baseline portfolio path
- candidate portfolio path
- default cost model path
- sleeve artifact paths
- benchmark state history path
- replay-scenario overrides such as cost/regime pass flags and turnover budget

The case file does not duplicate mandate or construction metadata that is already stored in the portfolio recipes.
Those are derived from the candidate portfolio and validated against the baseline portfolio so the replay stays tied to the same live-book frame.
The replay case also requires every used sleeve artifact to share the same
`target_id`. Promotion replay can compare baseline and candidate portfolios on
one common return basis, but it should reject cases that silently mix
incompatible target labels.

That gives V2 a stricter and cleaner handoff:

`persisted sleeve artifacts + persisted replay case -> loaded replay input -> portfolio replay -> promotion decision`

## Minimal Example

The first repository-level example now lives under:

- `research/examples/promotion_replay_minimal/`

It contains:

- one baseline portfolio recipe
- one candidate portfolio recipe
- two sleeve artifacts
- one benchmark state history artifact
- one replay-case TOML

This is not meant to be a production research archive.
It is the first audited trunk example proving that V2 can load research artifacts from disk and run a portfolio-level promotion replay without falling back to notebook state.

## Deliberate Non-Goals

This layer still does not do the following:

- infer missing sleeve states
- optimize reallocations after hard caps
- estimate benchmark industry weights internally
- compute sleeve correlation and regime diagnostics from first principles

Those are separate modules.

For now, the priority is narrower and more important:

- standardize the research handoff
- force candidate sleeves through a real baseline-versus-proposed portfolio replay
- make promotion depend on executable marginal value instead of standalone factor cosmetics
