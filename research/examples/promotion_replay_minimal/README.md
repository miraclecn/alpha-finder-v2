# Minimal Promotion Replay Example

This directory is the first persisted end-to-end V2 replay case.

The sleeve artifacts under `sleeve_artifacts/` are intentionally synthetic.
They keep the replay loader and replay report stable without depending on the
current `output/` tree, and the checked-in replay case now keeps both sleeves
on the same `target_id` so the evidence surface is not polluted by mixed
return labels.

Files:

- `baseline_portfolio.toml`: baseline production book before sleeve admission
- `candidate_portfolio.toml`: proposed book after sleeve admission
- `replay_case.toml`: scenario binding for replay
- `candidate_portfolio_with_overlay.toml`: candidate replay portfolio that also
  declares `regime_overlay_id`
- `replay_case_with_overlay.toml`: replay case that binds explicit overlay
  observations
- `regime_overlay_observations.json`: minimal green-input state history used by
  the formal overlay evaluator
- `benchmark_state_history.json`: PIT benchmark history used by the constructor, including per-date industry weights and constituent context
- `sleeve_artifacts/*.json`: decision-calendar research artifacts for each sleeve

The checked-in `benchmark_state_history.json` is still a hand-authored sample.
The production build path for the same artifact now lives under
`research/examples/benchmark_state_build_minimal/` and expects audited
`benchmark_weight_snapshot_pit` plus `industry_classification_pit` tables
inside the isolated V2 research DuckDB.
The persisted replay case remains intentionally synthetic for now.
That synthetic lane is still useful for schema and test stability, but the
honest generated comparison path now lives separately under
`research/examples/promotion_replay_real_output/`.
The deployment continuation still lives under
`research/examples/deployment_minimal/executable_signal_real_output_case.toml`.

Run:

```bash
cd /home/nan/alpha-find-v2
PYTHONPATH=src python3 -m alpha_find_v2 run-promotion-replay --case research/examples/promotion_replay_minimal/replay_case.toml
PYTHONPATH=src python3 -m alpha_find_v2 run-promotion-replay --case research/examples/promotion_replay_minimal/replay_case_with_overlay.toml
```

The example uses a narrow research-only promotion gate and an explicit turnover budget.
That is intentional.
Its purpose is to prove the persisted replay spine, not to stand in for the final production gate.
The overlay variant is also intentionally minimal.
It does not derive new market signals from partial artifacts.
It only proves that V2 can load a first-class `regime_overlay`, map explicit
green-input states into `normal / de_risk / cash_heavier`, and fail promotion
when overlay inputs are incomplete.
