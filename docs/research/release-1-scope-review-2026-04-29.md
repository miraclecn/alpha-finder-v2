# Release-1 Scope Review - 2026-04-29

## Decision

Outcome: keep `config/portfolio/a_share_core.toml` as an inactive reference
portfolio recipe. Do not treat it as the active release-1 capital path while
the current trend candidate is rejected and the residual fundamental lane
remains paused.

Release-1 therefore stays narrower than the checked-in two-sleeve recipe:

- no active multi-sleeve release portfolio is admitted today
- `trend_leadership_shadow_live_v1` remains a frozen diagnostic failure bundle
- `fundamental_rerating_core` remains outside the capital path until audited
  residual inputs exist

## Evidence Reviewed

### 1. The current trend sleeve is rejected for capital use

`docs/research/trend-leadership-failure-review-2026-04-29.md` classifies the
frozen `trend_leadership_shadow_live_v1` candidate as `thesis_rejected`.

Strategy-quality evidence on the attached `2021-03-05` to `2026-03-19` window:

| Metric | Value |
| --- | ---: |
| Active IR | `-1.33` |
| Active annualized return | `-41.02%` |
| Max drawdown | `-89.20%` |
| Turnover | `73.20x` |

That sleeve is not eligible for paper-trade release, shadow-live catch-up, or
portfolio-scope expansion.

### 2. The fundamental anchor remains paused

`research/examples/fundamental_input_build_minimal/README.md` and
`research/examples/promotion_replay_real_output/README.md` still state that
`fundamental_rerating_core` depends on an audited
`output/open_t1_to_open_t20_residual_component_snapshot.json` input that does
not exist in the repo today.

That means the slower residual fundamental lane is still a resume contract, not
an active release-1 admission lane. Widening release scope around it would
either mix incompatible return labels or rely on an in-repo estimator that the
project explicitly rejects.

### 3. No second sleeve has earned release-scope expansion

The checked-in `trend_resilience_core` replay example is useful as a comparator,
but `research/examples/promotion_replay_real_output/README.md` is explicit that
it is still an example lane rather than a production promotion decision. The
same doc states that the candidate book improves return and IR, but does not
yet pass the gate, and that most of the help comes in `trend_down` /
`drawdown` periods while `trend_up` is weaker.

That is not enough evidence to broaden release-1 into a multi-sleeve capital
recipe.

## Options Considered

### Option A: Keep `a_share_core` as the active two-sleeve release portfolio

Rejected.

The current recipe is `70%` rejected trend plus `30%` paused residual
fundamental. That combination does not represent an admissible capital path.

### Option B: Swap in another second sleeve immediately

Rejected.

The current repo does not yet have a second sleeve that is both:

- honest on the full costs-and-constraints surface
- promoted through the relevant gate
- evidenced as a durable full-portfolio improvement rather than a comparator

### Option C: Keep the recipe for reference, but mark it inactive

Accepted.

This preserves the object-chain example and existing config references without
pretending that the current release scope has an approved two-sleeve book.

## Operating Consequences

- `config/portfolio/a_share_core.toml` remains checked in as a reference recipe
  and inactive target portfolio.
- The active release-1 capital scope is now "no admitted portfolio recipe"
  rather than "two sleeves waiting for operational polish."
- Any future second-sleeve admission must prove marginal improvement on the
  full portfolio path after costs, constraints, and promotion gates.
- Any future trend work must start as a new versioned research object rather
  than tuning the rejected frozen bundle.

## Follow-Through

Update `docs/status/project-current-state-2026-04-29.md` so the release scope
is explicit, and avoid describing `a_share_core` as an active production
portfolio until the strategy-quality and residual-input blockers are resolved.
