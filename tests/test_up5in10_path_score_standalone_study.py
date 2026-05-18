from __future__ import annotations

import unittest

import pandas as pd

from alpha_find_v2.up5in10_path_score_standalone_study import (
    attach_selection_context_to_observations,
    merge_scored_candidates,
    select_standalone_rows,
    summarize_replay_rows_by_context,
)


class Up5In10PathScoreStandaloneStudyTest(unittest.TestCase):
    def test_merge_scored_candidates_normalizes_trade_date_types(self) -> None:
        candidate_rows = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "year": 2024, "entry_close_adj": 10.0},
                {"security_id": "BBB.SZ", "trade_date": "20240103", "year": 2024, "entry_close_adj": 11.0},
            ]
        )
        scored_features = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "event_trade_date": 20240102, "event_year": 2024, "path_score": 2.0, "score_split": "loo", "th_p80": 1.0, "th_p90": 1.5},
                {"security_id": "BBB.SZ", "event_trade_date": 20240103, "event_year": 2024, "path_score": 3.0, "score_split": "loo", "th_p80": 1.0, "th_p90": 1.5},
            ]
        )

        merged = merge_scored_candidates(candidate_rows, scored_features)

        self.assertEqual(merged["security_id"].tolist(), ["AAA.SZ", "BBB.SZ"])
        self.assertEqual(merged["path_score"].tolist(), [2.0, 3.0])

    def test_select_standalone_rows_applies_thresholds_and_daily_top_n(self) -> None:
        scored_candidates = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "year": 2024, "path_score": 3.0, "th_p80": 1.5, "th_p90": 2.5},
                {"security_id": "BBB.SZ", "trade_date": "20240102", "year": 2024, "path_score": 2.0, "th_p80": 1.5, "th_p90": 2.5},
                {"security_id": "CCC.SZ", "trade_date": "20240102", "year": 2024, "path_score": 1.0, "th_p80": 1.5, "th_p90": 2.5},
                {"security_id": "DDD.SZ", "trade_date": "20240103", "year": 2024, "path_score": 2.6, "th_p80": 1.5, "th_p90": 2.5},
            ]
        )

        selected = select_standalone_rows(
            scored_candidates,
            threshold_names=("p80", "p90"),
            top_ns=(1, 2),
        )

        p80_top1 = selected.loc[
            (selected["threshold_name"] == "p80") & (selected["top_n"] == 1)
        ].reset_index(drop=True)
        p80_top2 = selected.loc[
            (selected["threshold_name"] == "p80") & (selected["top_n"] == 2)
        ].reset_index(drop=True)
        p90_top2 = selected.loc[
            (selected["threshold_name"] == "p90") & (selected["top_n"] == 2)
        ].reset_index(drop=True)

        self.assertEqual(p80_top1["security_id"].tolist(), ["AAA.SZ", "DDD.SZ"])
        self.assertEqual(p80_top2["security_id"].tolist(), ["AAA.SZ", "BBB.SZ", "DDD.SZ"])
        self.assertEqual(p90_top2["security_id"].tolist(), ["AAA.SZ", "DDD.SZ"])

    def test_attach_selection_context_duplicates_shared_observations_per_combo(self) -> None:
        observations = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "entry_trade_date": "20240102", "step": 1, "high_ret_from_entry": 0.03, "low_ret_from_entry": -0.01, "close_ret_from_entry": 0.01},
                {"security_id": "AAA.SZ", "entry_trade_date": "20240102", "step": 2, "high_ret_from_entry": 0.06, "low_ret_from_entry": -0.01, "close_ret_from_entry": 0.04},
                {"security_id": "BBB.SZ", "entry_trade_date": "20240103", "step": 1, "high_ret_from_entry": 0.02, "low_ret_from_entry": -0.03, "close_ret_from_entry": -0.02},
            ]
        )
        selected_rows = pd.DataFrame(
            [
                {"security_id": "AAA.SZ", "trade_date": "20240102", "score_split": "loo", "threshold_name": "p80", "top_n": 1},
                {"security_id": "AAA.SZ", "trade_date": "20240102", "score_split": "loo", "threshold_name": "p90", "top_n": 1},
                {"security_id": "BBB.SZ", "trade_date": "20240103", "score_split": "full", "threshold_name": "p80", "top_n": 3},
            ]
        )

        expanded = attach_selection_context_to_observations(
            observations,
            selected_rows=selected_rows,
        )

        aaa_rows = expanded.loc[expanded["security_id"] == "AAA.SZ"].copy()
        self.assertEqual(len(aaa_rows), 4)
        self.assertEqual(
            sorted(aaa_rows["threshold_name"].unique().tolist()),
            ["p80", "p90"],
        )
        self.assertEqual(
            expanded.groupby(["score_split", "threshold_name", "top_n"]).size().to_dict(),
            {("full", "p80", 3): 1, ("loo", "p80", 1): 2, ("loo", "p90", 1): 2},
        )

    def test_summarize_replay_rows_by_context_keeps_combo_dimensions(self) -> None:
        replay_rows = pd.DataFrame(
            [
                {"score_split": "loo", "threshold_name": "p80", "top_n": 1, "policy_name": "fixed_tp0.05_sl0.10_hold10", "year": 2024, "gross_ret": 0.05, "net_ret": 0.0476, "exit_step": 2},
                {"score_split": "loo", "threshold_name": "p80", "top_n": 1, "policy_name": "fixed_tp0.05_sl0.10_hold10", "year": 2024, "gross_ret": -0.10, "net_ret": -0.1024, "exit_step": 3},
                {"score_split": "loo", "threshold_name": "p90", "top_n": 1, "policy_name": "fixed_tp0.05_sl0.10_hold10", "year": 2024, "gross_ret": 0.05, "net_ret": 0.0476, "exit_step": 1},
            ]
        )

        summary = summarize_replay_rows_by_context(replay_rows)

        self.assertEqual(len(summary), 2)
        keyed = summary.set_index(["score_split", "threshold_name", "top_n", "policy_name", "year"])
        self.assertAlmostEqual(float(keyed.loc[("loo", "p80", 1, "fixed_tp0.05_sl0.10_hold10", 2024), "mean_net_ret"]), -0.0274)
        self.assertAlmostEqual(float(keyed.loc[("loo", "p90", 1, "fixed_tp0.05_sl0.10_hold10", 2024), "mean_net_ret"]), 0.0476)


if __name__ == "__main__":
    unittest.main()
