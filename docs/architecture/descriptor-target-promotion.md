# Descriptor, Target, and Promotion Objects

## Why These Objects Exist

V2 does not treat a "factor" as a tradable object.

The research path is:

`thesis -> descriptor set -> executable residual target -> sleeve -> portfolio promotion gate`

Each layer exists to solve a different quantitative finance problem:

- `descriptor`: an economically interpretable measurement, such as sector-relative cheapness or post-earnings drift confirmation
- `descriptor set`: a thesis-specific bundle of descriptors that jointly express one mechanism instead of mixing unrelated signals
- `executable residual target`: the return definition that matches the real trading clock, holding horizon, costs, and neutralization assumptions
- `promotion gate`: the portfolio-level test that decides whether a sleeve deserves capital after diversification and implementation frictions

## Descriptor Registry

Descriptors are the atomic research measurements.

They must be:

- economically interpretable
- point-in-time safe
- aligned to a plausible holding horizon
- diagnosable when they fail

The descriptor registry deliberately avoids free-form symbolic mining. A descriptor should answer a simple question, for example:

- is this company cheap relative to peers without being obviously broken?
- did earnings expectations improve broadly enough to imply underreaction rather than noise?
- did the event surprise arrive early enough to be tradable at the next session open?

## Descriptor Sets

A descriptor set is the first object that resembles alpha construction, but it is still thesis-constrained.

Its job is to specify:

- which descriptors belong to the thesis
- each descriptor's role in the mechanism
- the target definition the set is meant to predict
- the ranking or screening logic used before portfolio construction

This keeps V2 from mixing slow valuation logic with fast event logic in one undisciplined score.

## Executable Residual Targets

Most research pipelines quietly use unrealistic targets. V2 makes the target explicit.

For A-shares, the target must encode:

- when the signal is observed
- when the trade can actually be entered
- how long capital is held
- which costs and tradeability filters are applied
- which benchmark or style exposures are removed before judging alpha

That is why the first V2 targets are defined as `next-day-open` entry, `open-to-open` holding-period returns, net of A-share cash-equity cost assumptions, with residualization versus benchmark, industry, size, and beta.

The target is not complete until the system can resolve:

- the signal observation timestamp
- the entry and exit trade offsets implied by the trading calendar
- the versioned cost model used for label construction
- the exact tradeability rules that can exclude an observation from training
- the common return components removed before the sleeve is judged

In practice, V2 should build labels from `target + cost model + tradeability state + residual components`, not from the target TOML alone.

## Promotion Gates

A sleeve is not promoted because it has a pretty standalone IC plot.

A sleeve is promotable only if it improves the production portfolio after:

- costs
- turnover
- tradeability
- correlation to existing sleeves
- regime robustness

This is the core break from V1. Research wins only when it survives portfolio interaction, not when it looks elegant in isolation.

That means the promotion object should encode not only standalone thresholds, but also explicit marginal portfolio contribution requirements such as:

- minimum improvement in portfolio IR
- maximum increase in portfolio drawdown burden
- maximum correlation to the current live book
- turnover and implementation slippage staying inside the sleeve budget
