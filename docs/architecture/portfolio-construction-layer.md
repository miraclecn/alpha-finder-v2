# Portfolio Construction Layer

## Why This Layer Exists

V2 cannot jump directly from sleeve research to a tradable portfolio.

In a personal A-share system, multiple sleeves will often:

- want the same stock at the same time
- disagree on capital timing
- collide with single-name and industry limits
- leave part of the book uninvested when the portfolio budget cannot be filled honestly

That requires an explicit portfolio-construction object between sleeve research and the execution simulator.

## First V2 Construction Policy

The first production construction model is intentionally simple and auditable:

`sleeve targets -> budgeted overlap sum -> hard caps -> executable combined target`

Its finance logic is:

- sleeve capital is set by the portfolio recipe, not by whichever sleeve happens to emit more names
- overlapping names are summed, because cross-sleeve agreement is a real source of conviction
- single-name concentration is clipped at the portfolio cap
- industry concentration is clipped relative to benchmark industry weights
- any residual weight blocked by hard constraints remains cash

That last rule is deliberate. The first V2 constructor is not allowed to hide weak breadth or capital-allocation problems by quietly re-optimizing the book.

## Object Boundary

The first version adds a versioned `portfolio_construction` config object.

Its job is to define:

- how sleeve budgets enter the portfolio
- how overlap is combined
- how names are selected when capacity is limited
- whether overflow weight is redistributed or left in cash
- whether industry budgets are benchmark-relative or absolute

The portfolio recipe still owns the economic budget numbers such as:

- sleeve capital allocation
- max names
- max single-name weight
- max industry overweight

That split keeps the method versioned without duplicating the portfolio's actual risk budget.

## Why Hold Cash Instead Of Reallocating

For this stage, `hold_cash` is the right policy.

If a sleeve has no valid names, or if the combined book hits concentration limits, forcing the leftover capital into the remaining names would change the thesis budget ex post.

That is not honest research. A portfolio constructor should first reveal missing breadth and constraint pressure before an optimizer tries to repair them.

## Deliberate Non-Goals

The first constructor is not:

- a mean-variance optimizer
- a turnover-minimizing optimizer
- a risk-parity allocator
- a live execution scheduler

Those may come later. First, V2 needs a testable and finance-legible bridge from sleeve outputs to a real portfolio target.
