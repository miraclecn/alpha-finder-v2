# Strategy Generation Guardrails

Generated strategies are not allowed to bypass the V2 research object chain.
Before a generated candidate can enter promotion review, it must provide a
machine-readable `generated_strategy_manifest` JSON file and pass
`validate-generated-strategy`.

## Manifest Contract

Schema version `1` binds one generated strategy to the same objects used by
hand-authored research:

```json
{
  "schema_version": 1,
  "artifact_type": "generated_strategy_manifest",
  "strategy_id": "generated_trend_strategy_v1",
  "objectives": ["active_net_information_ratio"],
  "promotion_review_requested": true,
  "mandate_path": "config/mandates/a_share_long_only_eod.toml",
  "thesis_path": "config/theses/trend_leadership.toml",
  "descriptor_set_path": "config/descriptor_sets/trend_leadership_core.toml",
  "sleeve_path": "config/sleeves/trend_leadership_core.toml",
  "target_path": "config/targets/open_t1_to_open_t20_net_cost.toml",
  "portfolio_path": "config/portfolio/a_share_core.toml",
  "cost_model_path": "config/cost_models/base_a_share_cash.toml",
  "data_quality_audit_path": "output/audits/market_data_quality_20260429.json",
  "daily_backtest_path": "output/trend_live_candidate_portfolio_with_overlay_daily_backtest.json",
  "promotion_replay_path": "research/examples/promotion_replay_real_output/replay_case.toml"
}
```

The validator checks that these references form one coherent chain:

`mandate -> thesis -> descriptor set -> sleeve -> portfolio recipe`

It also checks that the descriptor set and sleeve bind the same target, that
the target binds the declared cost model, and that the portfolio contains the
declared sleeve.

## Objective Guardrail

Generated strategy objectives must be implementation-aware. The validator
rejects bare-return or friction-ignoring objectives:

- `gross_return_only`
- `ignore_costs`
- `ignore_tradeability`

Those objective names are blocked because they produce candidates optimized
against surfaces that V2 will not trade. A generated strategy may optimize
active return or IR only when the target, cost model, and daily portfolio
backtest remain attached.

## Evidence Guardrail

Every manifest must bind:

- a `market_data_quality_audit` JSON artifact
- a `portfolio_backtest_result` daily backtest JSON artifact

If `promotion_review_requested` is true, the manifest must also bind a
`portfolio_promotion_replay_case`. This keeps promotion review connected to
executable evidence instead of standalone factor output.

The guardrail does not make a weak strategy good. It only decides whether a
generated candidate is structurally admissible for review. Strategy-quality,
data-quality, and live-readiness gates still decide whether capital can be
considered.

## CLI

```bash
PYTHONPATH=src python3 -m alpha_find_v2 validate-generated-strategy --manifest path/to/manifest.json
```

The command prints bound object ids, evidence paths, rejected objectives, and
whether promotion review is structurally allowed. Invalid manifests raise a
guardrail error and do not enter promotion review.
