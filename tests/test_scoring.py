from __future__ import annotations

import unittest

from alpha_find_v2.scoring import (
    group_neutral_zscore_map,
    rank_then_cap_weights,
)


class ScoringTest(unittest.TestCase):
    def test_group_neutral_zscore_scores_each_industry_independently(self) -> None:
        scores = group_neutral_zscore_map(
            values_by_asset={
                "BANK_LEADER": 0.05,
                "BANK_LAGGARD": 0.04,
                "TECH_LEADER": 0.50,
                "TECH_LAGGARD": 0.49,
            },
            group_by_asset={
                "BANK_LEADER": "bank",
                "BANK_LAGGARD": "bank",
                "TECH_LEADER": "tech",
                "TECH_LAGGARD": "tech",
            },
        )

        self.assertGreater(scores["BANK_LEADER"], scores["BANK_LAGGARD"])
        self.assertGreater(scores["TECH_LEADER"], scores["TECH_LAGGARD"])
        self.assertAlmostEqual(scores["BANK_LEADER"], scores["TECH_LEADER"])
        self.assertAlmostEqual(scores["BANK_LAGGARD"], scores["TECH_LAGGARD"])

    def test_rank_then_cap_weights_are_monotonic_capped_and_fully_invested(self) -> None:
        weights = rank_then_cap_weights(
            ["AAA", "BBB", "CCC", "DDD"],
            weight_cap=0.40,
        )

        self.assertAlmostEqual(sum(weights.values()), 1.0)
        self.assertLessEqual(max(weights.values()), 0.40)
        self.assertGreater(weights["AAA"], weights["BBB"])
        self.assertGreater(weights["BBB"], weights["CCC"])
        self.assertGreater(weights["CCC"], weights["DDD"])


if __name__ == "__main__":
    unittest.main()
