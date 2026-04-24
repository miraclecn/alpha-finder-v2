import tomllib
import unittest

from alpha_find_v2.config_loader import (
    CONFIG_ROOT,
    PROJECT_ROOT,
    list_configs,
    load_cost_model,
    load_decay_monitor,
    load_descriptor_set,
    load_execution_policy,
    load_mandate,
    load_portfolio,
    load_portfolio_construction_model,
    load_promotion_gate,
    load_risk_model,
    load_sleeve,
    load_target,
    load_thesis,
)


class ConfigLoaderTest(unittest.TestCase):
    def test_mandate_loads_tradeability_constraints(self) -> None:
        mandate = load_mandate(CONFIG_ROOT / "mandates" / "a_share_long_only_eod.toml")

        self.assertEqual(mandate.id, "a_share_long_only_eod")
        self.assertTrue(mandate.filters["exclude_st"])
        self.assertTrue(mandate.risk["industry_neutral"])

    def test_core_thesis_registry_contains_current_research_lanes(self) -> None:
        thesis_ids = {
            load_thesis(path).id
            for path in list_configs("theses")
            if path.stem != "template"
        }

        self.assertEqual(
            thesis_ids,
            {
                "crowding_anti_consensus",
                "earnings_underreaction",
                "flow_liquidity_reversal",
                "fundamental_rerating",
                "trend_leadership",
            },
        )

    def test_portfolio_allocations_sum_to_one(self) -> None:
        portfolio = load_portfolio(CONFIG_ROOT / "portfolio" / "a_share_core.toml")

        self.assertEqual(
            portfolio.sleeves,
            ["fundamental_rerating_core", "trend_leadership_core"],
        )
        self.assertAlmostEqual(sum(portfolio.allocation.values()), 1.0)

    def test_sleeve_links_back_to_thesis_and_mandate(self) -> None:
        sleeve = load_sleeve(CONFIG_ROOT / "sleeves" / "trend_leadership_core.toml")

        self.assertEqual(sleeve.mandate_id, "a_share_long_only_eod")
        self.assertEqual(sleeve.thesis_id, "trend_leadership")
        self.assertIn("industry", sleeve.neutralization)

    def test_sleeve_references_descriptor_set_and_target(self) -> None:
        sleeve = load_sleeve(CONFIG_ROOT / "sleeves" / "trend_leadership_core.toml")
        descriptor_set = load_descriptor_set(
            CONFIG_ROOT / "descriptor_sets" / f"{sleeve.descriptor_set_id}.toml"
        )
        target = load_target(CONFIG_ROOT / "targets" / f"{sleeve.target_id}.toml")

        self.assertEqual(descriptor_set.thesis_id, sleeve.thesis_id)
        self.assertEqual(target.signal_observation, "trade_date_close")
        self.assertEqual(target.trade_entry, "next_day_open")

    def test_portfolio_links_to_promotion_gate(self) -> None:
        portfolio = load_portfolio(CONFIG_ROOT / "portfolio" / "a_share_core.toml")
        gate = load_promotion_gate(
            CONFIG_ROOT / "promotion_gates" / f"{portfolio.promotion_gate_id}.toml"
        )

        self.assertEqual(gate.scope, "portfolio")
        self.assertIn("max_component_correlation", gate.correlation_limits)
        self.assertIn("minimum_marginal_ir_delta", gate.portfolio_contribution)

    def test_portfolio_links_to_construction_model(self) -> None:
        portfolio = load_portfolio(CONFIG_ROOT / "portfolio" / "a_share_core.toml")
        construction_model = load_portfolio_construction_model(
            CONFIG_ROOT
            / "portfolio_construction"
            / f"{portfolio.construction_model_id}.toml"
        )

        self.assertEqual(construction_model.overlap_mode, "sum")
        self.assertEqual(construction_model.excess_weight_policy, "hold_cash")
        self.assertEqual(construction_model.industry_budget_mode, "benchmark_relative")

    def test_portfolio_links_to_execution_and_decay_policy(self) -> None:
        portfolio = load_portfolio(CONFIG_ROOT / "portfolio" / "a_share_core.toml")
        execution_policy = load_execution_policy(
            CONFIG_ROOT / "execution_policies" / f"{portfolio.execution_policy_id}.toml"
        )
        decay_monitor = load_decay_monitor(
            CONFIG_ROOT / "decay_monitors" / f"{portfolio.decay_monitor_id}.toml"
        )

        self.assertEqual(execution_policy.trade_timing, "next_day_open")
        self.assertEqual(execution_policy.blocked_trade_policy, "carry_positions")
        self.assertEqual(decay_monitor.comparison_mode, "promotion_snapshot")
        self.assertIn(20, decay_monitor.observation_windows)

    def test_descriptor_set_members_match_thesis_data_needs(self) -> None:
        descriptor_set = load_descriptor_set(
            CONFIG_ROOT / "descriptor_sets" / "trend_leadership_core.toml"
        )
        thesis = load_thesis(CONFIG_ROOT / "theses" / "trend_leadership.toml")

        self.assertTrue(set(descriptor_set.required_data).issubset(set(thesis.required_data)))
        self.assertEqual(descriptor_set.target_id, "open_t1_to_open_t20_net_cost")

    def test_cost_model_registry_is_positive_and_versioned(self) -> None:
        cost_model = load_cost_model(CONFIG_ROOT / "cost_models" / "base_a_share_cash.toml")

        self.assertEqual(cost_model.id, "base_a_share_cash")
        self.assertGreater(cost_model.round_trip_bps(), 0.0)

    def test_trend_interim_target_is_non_residual_and_has_no_risk_model(self) -> None:
        target = load_target(CONFIG_ROOT / "targets" / "open_t1_to_open_t20_net_cost.toml")

        self.assertEqual(target.id, "open_t1_to_open_t20_net_cost")
        self.assertEqual(target.label_kind, "net_return")
        self.assertEqual(target.residualization, [])
        self.assertEqual(target.risk_model_id, "")

    def test_secondary_real_trend_sleeve_stays_on_same_weekly_clock(self) -> None:
        sleeve = load_sleeve(CONFIG_ROOT / "sleeves" / "trend_resilience_core.toml")
        descriptor_set = load_descriptor_set(
            CONFIG_ROOT / "descriptor_sets" / f"{sleeve.descriptor_set_id}.toml"
        )

        weights = {
            component.descriptor_id: component.weight
            for component in descriptor_set.components
        }
        self.assertEqual(sleeve.thesis_id, "trend_leadership")
        self.assertEqual(sleeve.rebalance_frequency, "weekly")
        self.assertEqual(sleeve.target_id, "open_t1_to_open_t20_net_cost")
        self.assertGreaterEqual(
            sleeve.constraints["min_median_daily_turnover_cny_mn"],
            100,
        )
        self.assertGreater(
            weights["trend_stability"],
            weights["medium_term_relative_strength"],
        )

    def test_real_output_replay_examples_bind_generated_second_trend_lane(self) -> None:
        trend_input_case = tomllib.loads(
            (PROJECT_ROOT / "research/examples/trend_input_build_minimal/trend_resilience_core.toml").read_text(
                encoding="utf-8"
            )
        )
        artifact_case = tomllib.loads(
            (PROJECT_ROOT / "research/examples/artifact_build_minimal/trend_resilience_core_output.toml").read_text(
                encoding="utf-8"
            )
        )
        replay_case = tomllib.loads(
            (PROJECT_ROOT / "research/examples/promotion_replay_real_output/replay_case.toml").read_text(
                encoding="utf-8"
            )
        )
        baseline_portfolio = tomllib.loads(
            (PROJECT_ROOT / "research/examples/promotion_replay_real_output/baseline_portfolio.toml").read_text(
                encoding="utf-8"
            )
        )
        candidate_portfolio = tomllib.loads(
            (PROJECT_ROOT / "research/examples/promotion_replay_real_output/candidate_portfolio.toml").read_text(
                encoding="utf-8"
            )
        )

        self.assertEqual(
            trend_input_case["sleeve_path"],
            "config/sleeves/trend_resilience_core.toml",
        )
        self.assertEqual(
            artifact_case["input_path"],
            "output/trend_resilience_core_input.json",
        )
        self.assertEqual(
            artifact_case["output_path"],
            "output/trend_resilience_core_artifact.json",
        )
        self.assertEqual(
            replay_case["artifact_paths"],
            [
                "output/trend_leadership_core_artifact.json",
                "output/trend_resilience_core_artifact.json",
            ],
        )
        self.assertEqual(
            baseline_portfolio["sleeves"],
            ["trend_leadership_core"],
        )
        self.assertEqual(
            candidate_portfolio["sleeves"],
            ["trend_leadership_core", "trend_resilience_core"],
        )

    def test_target_references_versioned_risk_model(self) -> None:
        target = load_target(CONFIG_ROOT / "targets" / "open_t1_to_open_t20_residual_net_cost.toml")
        risk_model = load_risk_model(
            CONFIG_ROOT / "risk_models" / f"{target.risk_model_id}.toml"
        )

        self.assertEqual(risk_model.id, "a_share_core_equity")
        self.assertEqual(risk_model.residual_components, target.residualization)


if __name__ == "__main__":
    unittest.main()
