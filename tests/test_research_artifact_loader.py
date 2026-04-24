import json
from pathlib import Path
import tempfile
import unittest

from alpha_find_v2.deployment_loader import load_portfolio_state_snapshot
from alpha_find_v2.live_state import (
    account_state_to_portfolio_state,
    load_account_state_snapshot,
    load_benchmark_state_artifact,
)
from alpha_find_v2.portfolio_promotion_replay import PortfolioPromotionReplay
from alpha_find_v2.research_artifact_loader import (
    load_portfolio_promotion_replay_case,
    load_sleeve_artifact,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_ROOT = PROJECT_ROOT / "research" / "examples" / "promotion_replay_minimal"
DEPLOYMENT_EXAMPLE_ROOT = PROJECT_ROOT / "research" / "examples" / "deployment_minimal"


class ResearchArtifactLoaderTest(unittest.TestCase):
    def test_load_sleeve_artifact_from_json(self) -> None:
        artifact = load_sleeve_artifact(
            EXAMPLE_ROOT / "sleeve_artifacts" / "fundamental_rerating_core.json"
        )

        self.assertEqual(artifact.sleeve_id, "fundamental_rerating_core")
        self.assertEqual(
            artifact.trade_dates(),
            ["2026-04-06", "2026-04-13", "2026-04-20"],
        )
        self.assertEqual(artifact.steps[0].records[0].cost_model_id, "base_a_share_cash")
        self.assertTrue(artifact.steps[0].records[0].trade_state.can_enter)
        self.assertTrue(artifact.steps[0].records[0].trade_state.can_exit)

    def test_load_benchmark_state_artifact(self) -> None:
        artifact = load_benchmark_state_artifact(
            EXAMPLE_ROOT / "benchmark_state_history.json"
        )

        step = artifact.step_for_date("2026-04-20")
        self.assertEqual(artifact.benchmark_id, "CSI 800")
        self.assertEqual(artifact.classification, "citics_l1")
        self.assertEqual(artifact.weighting_method, "manual_sample")
        self.assertEqual(step.trade_date, "2026-04-20")
        self.assertEqual(step.available_at, "2026-04-20T15:30:00+08:00")
        self.assertAlmostEqual(
            sum(step.industry_weights.values()),
            1.0,
        )
        self.assertEqual(step.constituents[0].industry, "bank")

    def test_load_account_state_snapshot_and_adapt_to_portfolio_state(self) -> None:
        account_state = load_account_state_snapshot(
            DEPLOYMENT_EXAMPLE_ROOT / "account_state_2026_04_20.json"
        )
        portfolio_state = account_state_to_portfolio_state(
            portfolio_id="research_example_candidate_portfolio",
            account_state=account_state,
        )

        self.assertEqual(account_state.account_id, "shadow_a_share_cash")
        self.assertEqual(account_state.as_of_date, "2026-04-20")
        self.assertAlmostEqual(account_state.available_cash_cny, 1_200_000.0)
        self.assertEqual(portfolio_state.blocked_exit_assets, ["DDD"])
        self.assertAlmostEqual(portfolio_state.cash_weight, 0.12)
        self.assertAlmostEqual(portfolio_state.current_weights()["AAA"], 0.30)

    def test_load_portfolio_state_snapshot(self) -> None:
        state = load_portfolio_state_snapshot(
            DEPLOYMENT_EXAMPLE_ROOT / "portfolio_state_2026_04_20.json"
        )

        self.assertEqual(state.portfolio_id, "research_example_candidate_portfolio")
        self.assertEqual(state.as_of_date, "2026-04-20")
        self.assertAlmostEqual(state.cash_weight, 0.12)
        self.assertEqual(state.blocked_exit_assets, ["DDD"])
        self.assertEqual(state.current_weights()["AAA"], 0.30)

    def test_load_portfolio_promotion_replay_case_runs_end_to_end(self) -> None:
        loaded_case = load_portfolio_promotion_replay_case(EXAMPLE_ROOT / "replay_case.toml")

        replay = PortfolioPromotionReplay(
            mandate=loaded_case.mandate,
            construction_model=loaded_case.construction_model,
            default_cost_model=loaded_case.default_cost_model,
            gate=loaded_case.gate,
            cost_models=loaded_case.cost_models,
        )
        result = replay.replay(loaded_case.replay_input)

        self.assertEqual(
            loaded_case.replay_input.baseline_portfolio.sleeves,
            ["fundamental_rerating_core"],
        )
        self.assertEqual(
            loaded_case.replay_input.candidate_portfolio.sleeves,
            ["fundamental_rerating_core", "trend_leadership_core"],
        )
        self.assertEqual(
            loaded_case.replay_input.artifacts[1].sleeve_id,
            "trend_leadership_core",
        )
        self.assertIsNotNone(loaded_case.benchmark_state_artifact)
        self.assertTrue(result.decision.passed)
        self.assertGreater(result.marginal.marginal_ir_delta, 0.0)

    def test_load_sleeve_artifact_rejects_unknown_schema_version(self) -> None:
        payload = {
            "schema_version": 99,
            "artifact_type": "sleeve_research_artifact",
            "sleeve_id": "fundamental_rerating_core",
            "mandate_id": "a_share_long_only_eod",
            "target_id": "open_t1_to_open_t20_residual_net_cost",
            "steps": [],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "bad_artifact.json"
            path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "Unsupported sleeve artifact schema version"):
                load_sleeve_artifact(path)


if __name__ == "__main__":
    unittest.main()
