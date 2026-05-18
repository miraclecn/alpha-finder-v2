# Phase 3 Regime Overlay And Promotion

## Goal

Turn `risk_regime_filter` from doctrine into a formal portfolio object and fix
the promotion rule that governs overlay admission.

The new object boundary for this phase is `regime_overlay`.
It is an exposure governor, not a stock-picking sleeve.

## Current Verified State

- `docs/architecture/a-share-personal-data-research-doctrine.md` already
  defines `risk_regime_filter` as a portfolio overlay built from benchmark
  trend, market breadth, dispersion, realized volatility, and price-limit
  stress.
- `docs/architecture/research-operating-model.md` already requires promotion
  decisions to be judged at the portfolio level rather than from one sleeve
  chart.
- `research/examples/promotion_replay_real_output/README.md` already shows that
  replay emits `research_evidence.walk_forward` and
  `research_evidence.regime_breakdown`.
- `output/csi800_benchmark_state_history.json` already exists locally as of
  `2026-04-25`, so the repo already has one benchmark-state artifact that can
  feed overlay work.
- the repo now exposes a first-class `regime_overlay` config and evaluator
  boundary under `config/regime_overlays/` and
  `src/alpha_find_v2/regime_overlay.py`
- replay and deployment loaders now accept explicit
  `regime_overlay_observation_path` input when a portfolio declares
  `regime_overlay_id`
- the first executable contract is intentionally minimal:
  it reads explicit green-input states, enforces
  `normal / de_risk / cash_heavier`, and blocks or downgrades on incomplete
  input instead of inventing internal estimators

## In Scope

- define the `regime_overlay` config / object boundary
- define overlay input rules from `green` data only
- define replay and promotion interfaces for the overlay
- define the allowed portfolio states:
  `normal`, `de_risk`, and `cash_heavier`
- define explicit downgrade behavior when overlay inputs are incomplete or
  invalid
- fix overlay promotion criteria around marginal portfolio contribution,
  post-cost behavior, and regime robustness

## Out of Scope

- creating a new stock-picking sleeve
- using `amber` or `experimental` sources for overlay truth
- intraday macro timing or news-driven regime logic
- silently ignoring missing overlay inputs
- broker automation

## Dependencies

- Phase 1 data-tier and research-source boundaries
- Phase 2 trend production lane and replay evidence surfaces
- `docs/architecture/a-share-personal-data-research-doctrine.md`
- `docs/architecture/research-operating-model.md`
- `output/csi800_benchmark_state_history.json`

## Execution Breakdown

### 1. Promote The Overlay To A Formal Object

Replace the doctrine-only label with one explicit object boundary:

- `regime_overlay` inputs
- `regime_overlay` state output
- replay / promotion interface for overlay contribution

This phase is not complete if the overlay remains only an architectural idea.

### 2. Freeze The Input Boundary

Only `green` inputs are allowed for the first overlay contract:

- benchmark trend
- market breadth
- dispersion
- realized volatility
- price-limit stress

If `price-limit stress` is not yet auditable on the same standard as the other
green inputs, the phase document must say so explicitly and define the overlay
as incomplete or blocked rather than silently dropping the signal.

### 3. Freeze The State Machine

The overlay must at least support three explicit portfolio states:

- `normal`
- `de_risk`
- `cash_heavier`

The overlay acts on exposure and cash posture. It does not rank stocks or
replace sleeve selection logic.

### 4. Freeze Promotion Discipline

Overlay promotion is judged by portfolio-level evidence:

- marginal contribution to the combined book
- post-cost behavior
- behavior across stressed and weak regimes

Standalone overlay cosmetics or one flattering sub-period are not enough.

### 5. Freeze Failure Handling

Missing or invalid overlay inputs must not be ignored. The document should
require one of two paths:

- downgrade to a documented conservative state
- stop the overlay path and record the blocker

## Verification Matrix

- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_regime_overlay.py' -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_promotion_gate_evaluator.py' -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_research_artifact_loader.py' -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_deployment.py' -v`
- `PYTHONPATH=src python3 -m unittest discover -s tests -v`
- `run-promotion-replay --case research/examples/promotion_replay_real_output/replay_case.toml`
- `run-promotion-replay --case research/examples/promotion_replay_minimal/replay_case_with_overlay.toml`
- `build-executable-signal --case research/examples/deployment_minimal/executable_signal_case_with_overlay.toml`

Phase acceptance also requires the document to state:

- the `regime_overlay` object boundary
- the exact allowed input family
- the three exposure states
- the downgrade / stop rule when inputs are missing
- the portfolio-level promotion standard

## Stop Conditions

- the overlay is allowed to act as a stock selector
- non-green or unaudited data becomes overlay truth
- missing overlay inputs are silently ignored
- promotion falls back to a standalone sleeve chart instead of portfolio
  marginal evidence

## Exit Criteria

- `regime_overlay` is documented and implemented as a first-class object
- the overlay input family is frozen to `green` data only
- replay, promotion, and deployment are required to express
  `normal / de_risk / cash_heavier`
- downgrade and stop behavior is explicit
- the phase document fixes the overlay as a portfolio governor, not a new
  alpha sleeve
