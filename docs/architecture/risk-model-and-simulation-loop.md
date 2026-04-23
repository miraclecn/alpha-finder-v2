# Risk Model and Simulation Loop

## Why This Layer Exists

V2 cannot stop at descriptor definitions and target TOMLs.

A personal A-share trading system only becomes credible when the same object chain governs:

- research labels
- sleeve validation
- portfolio replay
- promotion decisions

That requires a formal bridge from `signal idea` to `executable portfolio path`.

## Required Object Chain

The next V2 execution loop is:

`descriptor set -> target -> risk model -> sleeve artifact -> portfolio construction -> combined target weights -> portfolio simulator -> research evaluator -> promotion gate`

Each object answers a different finance question:

- `target`: what return horizon is actually being judged
- `risk model`: which common return components are removed before calling something alpha
- `sleeve artifact`: what each sleeve actually wants to own on each decision date, with executable returns and trade-state bindings
- `portfolio construction`: how sleeve budgets and overlaps become one book
- `combined target weights`: what the portfolio is actually trying to own
- `portfolio simulator`: what can really be traded after A-share constraints and costs
- `research evaluator`: whether the realized path is good enough for promotion

## Risk Model Boundary

The V2 risk model is a versioned object, not an ad hoc regression snippet inside a notebook.

For the first release, its job is narrow:

- define the residualization components used by labels
- define how benchmark, industry, size, and beta components are named
- provide a stable interface from factor-return snapshots to stock-level common returns

The first skeleton does not pretend to be a full production Barra replacement.
It intentionally avoids embedding a toy estimator and instead makes the decomposition interface explicit.

That is the right sequencing for V2:

1. make residualization auditable and versioned
2. connect it to label construction and simulation
3. only then decide how to estimate factor returns internally

## Portfolio Simulator Boundary

The simulator is an execution layer, not an optimizer.

For now it assumes upstream research has already produced target weights. Its responsibilities are:

- apply the portfolio cash buffer
- block trades that are not executable
- carry forced holdings when exit is blocked
- charge buy and sell costs from versioned cost models
- generate realized portfolio return and turnover paths

This keeps the system honest. It separates:

- `what you want to own`
- `what you are allowed to own`
- `what you actually end up owning`

## Research Evaluator Boundary

The evaluator converts simulated paths into promotion-ready evidence.

The first summary layer should answer:

- did returns survive costs
- how much turnover was required
- how often execution was blocked
- how deep were realized drawdowns
- how broad was the realized opportunity set

Portfolio contribution, regime splits, and sleeve correlation remain promotion-layer inputs, but they now sit on top of a real executable path instead of factor cosmetics.

## Deliberate Non-Goals For This Skeleton

- no full internal factor-return estimator yet
- no portfolio optimizer yet
- no turnover-aware reallocator beyond the hard-cap combiner
- no live order router or QMT integration yet

Those are later layers. First, V2 needs a clean and testable research-to-execution spine.
