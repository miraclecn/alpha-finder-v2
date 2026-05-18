from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


class SwIndustryPitAuditTest(unittest.TestCase):
    def test_build_akshare_intervals_reconstructs_change_nodes(self) -> None:
        from alpha_find_v2.sw_industry_pit_audit import (
            build_akshare_intervals,
        )

        intervals = build_akshare_intervals(
            [
                {
                    "symbol": "000001",
                    "start_date": "19910403",
                    "industry_code": "801010.SI",
                },
                {
                    "symbol": "000001",
                    "start_date": "20140221",
                    "industry_code": "801020.SI",
                },
                {
                    "symbol": "000001",
                    "start_date": "20210730",
                    "industry_code": "801030.SI",
                },
                {
                    "symbol": "000002",
                    "start_date": "19910129",
                    "industry_code": "801040.SI",
                },
            ]
        )

        self.assertEqual(
            intervals,
            [
                ("000001.SZ", "801010.SI", "19910403", "20140221"),
                ("000001.SZ", "801020.SI", "20140221", "20210730"),
                ("000001.SZ", "801030.SI", "20210730", None),
                ("000002.SZ", "801040.SI", "19910129", None),
            ],
        )

    def test_build_tushare_intervals_merges_both_is_new_branches(self) -> None:
        from alpha_find_v2.sw_industry_pit_audit import (
            build_tushare_intervals,
        )

        intervals = build_tushare_intervals(
            [
                {
                    "ts_code": "000001.SZ",
                    "l1_code": "801010.SI",
                    "in_date": "20140221",
                    "out_date": "20210730",
                    "is_new": "N",
                },
                {
                    "ts_code": "000001.SZ",
                    "l1_code": "801030.SI",
                    "in_date": "20210730",
                    "out_date": None,
                    "is_new": "Y",
                },
                {
                    "ts_code": "000001.SZ",
                    "l1_code": "801010.SI",
                    "in_date": "20140221",
                    "out_date": "20210730",
                    "is_new": "N",
                },
            ],
            industry_level="L1",
        )

        self.assertEqual(
            intervals,
            [
                ("000001.SZ", "801010.SI", "20140221", "20210730"),
                ("000001.SZ", "801030.SI", "20210730", None),
            ],
        )

    def test_audit_constituent_days_classifies_gap_sources(self) -> None:
        from alpha_find_v2.sw_industry_pit_audit import (
            ConstituentDay,
            CoverageSummary,
            audit_constituent_days,
        )

        summary = audit_constituent_days(
            constituent_days=[
                ConstituentDay(trade_date="20210730", security_id="000001.SZ"),
                ConstituentDay(trade_date="20210730", security_id="000002.SZ"),
                ConstituentDay(trade_date="20210730", security_id="000003.SZ"),
                ConstituentDay(trade_date="20210730", security_id="000004.SZ"),
            ],
            staged_intervals=[
                ("000002.SZ", "801020.SI", "20140221", "20210730"),
            ],
            live_tushare_intervals=[
                ("000001.SZ", "801010.SI", "20140221", None),
                ("000002.SZ", "801020.SI", "20140221", "20210730"),
            ],
            akshare_intervals=[
                ("000001.SZ", "801010.SI", "20140221", None),
                ("000002.SZ", "801020.SI", "20140221", "20210730"),
                ("000003.SZ", "801030.SI", "20140221", None),
            ],
        )

        self.assertIsInstance(summary, CoverageSummary)
        self.assertEqual(summary.constituent_days_total, 4)
        self.assertEqual(summary.staged_exclusive_covered, 0)
        self.assertEqual(summary.staged_inclusive_covered, 1)
        self.assertEqual(summary.live_tushare_exclusive_covered, 1)
        self.assertEqual(summary.live_tushare_inclusive_covered, 2)
        self.assertEqual(summary.akshare_covered, 2)
        self.assertEqual(
            summary.gap_classification_counts,
            {
                "staging_sync_gap": 1,
                "staging_end_date_boundary": 1,
                "tushare_provider_gap_akshare_covers": 1,
                "unresolved_gap": 1,
            },
        )
        self.assertEqual(
            [
                (row.security_id, row.classification)
                for row in summary.gap_rows
            ],
            [
                ("000001.SZ", "staging_sync_gap"),
                ("000002.SZ", "staging_end_date_boundary"),
                ("000003.SZ", "tushare_provider_gap_akshare_covers"),
                ("000004.SZ", "unresolved_gap"),
            ],
        )

    def test_audit_constituent_days_derives_bridge_fill_run_decisions(self) -> None:
        from alpha_find_v2.sw_industry_pit_audit import (
            BridgeFillRunDecision,
            ConstituentDay,
            audit_constituent_days,
        )

        summary = audit_constituent_days(
            constituent_days=[
                ConstituentDay(trade_date="20210105", security_id="000001.SZ"),
                ConstituentDay(trade_date="20210106", security_id="000001.SZ"),
                ConstituentDay(trade_date="20210107", security_id="000001.SZ"),
                ConstituentDay(trade_date="20210105", security_id="000002.SZ"),
                ConstituentDay(trade_date="20210106", security_id="000002.SZ"),
                ConstituentDay(trade_date="20210107", security_id="000002.SZ"),
                ConstituentDay(trade_date="20210729", security_id="000003.SZ"),
                ConstituentDay(trade_date="20210730", security_id="000003.SZ"),
                ConstituentDay(trade_date="20210802", security_id="000003.SZ"),
                ConstituentDay(trade_date="20210105", security_id="000004.SZ"),
                ConstituentDay(trade_date="20210106", security_id="000004.SZ"),
                ConstituentDay(trade_date="20210107", security_id="000004.SZ"),
                ConstituentDay(trade_date="20210105", security_id="000005.SZ"),
                ConstituentDay(trade_date="20210106", security_id="000005.SZ"),
                ConstituentDay(trade_date="20210107", security_id="000005.SZ"),
            ],
            staged_intervals=[],
            live_tushare_intervals=[
                ("000001.SZ", "801010.SI", "20200101", "20210104"),
                ("000001.SZ", "801010.SI", "20210107", None),
                ("000002.SZ", "801020.SI", "20210107", None),
                ("000003.SZ", "801030.SI", "20200101", "20210728"),
                ("000003.SZ", "801030.SI", "20210802", None),
                ("000004.SZ", "801040.SI", "20200101", "20210104"),
                ("000004.SZ", "801040.SI", "20210107", None),
                ("000005.SZ", "801050.SI", "20200101", "20210104"),
                ("000005.SZ", "801060.SI", "20210107", None),
            ],
            akshare_intervals=[
                ("000001.SZ", "220101", "20200101", None),
                ("000002.SZ", "220201", "20200101", None),
                ("000003.SZ", "220301", "20200101", None),
                ("000004.SZ", "220401", "20200101", "20210106"),
                ("000004.SZ", "220402", "20210106", None),
                ("000005.SZ", "220501", "20200101", None),
            ],
        )

        self.assertEqual(
            summary.bridge_fill_run_counts,
            {
                "eligible_bridge_fill": 1,
                "blocked_one_sided_gap": 1,
                "blocked_schema_transition": 1,
                "blocked_counterevidence": 1,
                "blocked_endpoint_mismatch": 1,
            },
        )
        self.assertEqual(
            summary.bridge_fill_day_counts,
            {
                "eligible_bridge_fill": 2,
                "blocked_one_sided_gap": 2,
                "blocked_schema_transition": 2,
                "blocked_counterevidence": 2,
                "blocked_endpoint_mismatch": 2,
            },
        )
        self.assertTrue(
            all(isinstance(run, BridgeFillRunDecision) for run in summary.bridge_fill_runs)
        )

        by_security = {
            run.security_id: run
            for run in summary.bridge_fill_runs
        }
        self.assertEqual(by_security["000001.SZ"].decision, "eligible_bridge_fill")
        self.assertEqual(by_security["000001.SZ"].imputed_industry_code, "801010.SI")
        self.assertEqual(by_security["000001.SZ"].gap_trade_days, 2)
        self.assertEqual(by_security["000002.SZ"].decision, "blocked_one_sided_gap")
        self.assertEqual(by_security["000003.SZ"].decision, "blocked_schema_transition")
        self.assertEqual(by_security["000004.SZ"].decision, "blocked_counterevidence")
        self.assertEqual(by_security["000004.SZ"].akshare_codes_in_gap, ["220401", "220402"])
        self.assertEqual(by_security["000005.SZ"].decision, "blocked_endpoint_mismatch")

    def test_audit_constituent_days_marks_manual_provider_gap_adjudications(self) -> None:
        from alpha_find_v2.sw_industry_pit_audit import (
            ConstituentDay,
            audit_constituent_days,
        )

        summary = audit_constituent_days(
            constituent_days=[
                ConstituentDay(trade_date="20210105", security_id="000003.SZ"),
            ],
            staged_intervals=[
                ("000003.SZ", "801030.SI", "20200101", None),
            ],
            live_tushare_intervals=[],
            akshare_intervals=[
                ("000003.SZ", "220301", "20200101", None),
            ],
            manual_adjudication_intervals=[
                ("000003.SZ", "801030.SI", "20210101", "20210106"),
            ],
        )

        self.assertEqual(
            summary.gap_classification_counts,
            {"manual_adjudicated_provider_gap": 1},
        )
        self.assertEqual(summary.gap_rows[0].classification, "manual_adjudicated_provider_gap")
        self.assertEqual(summary.bridge_fill_runs, [])

    def test_run_one_shot_payload_reports_bridge_fill_summary(self) -> None:
        from alpha_find_v2.sw_industry_pit_audit import (
            ConstituentDay,
            run_one_shot_sw_industry_pit_audit,
        )

        constituent_days = [
            ConstituentDay(trade_date="20210105", security_id="000001.SZ"),
            ConstituentDay(trade_date="20210106", security_id="000001.SZ"),
            ConstituentDay(trade_date="20210107", security_id="000001.SZ"),
        ]
        live_tushare_intervals = [
            ("000001.SZ", "801010.SI", "20200101", "20210104"),
            ("000001.SZ", "801010.SI", "20210107", None),
        ]
        akshare_intervals = [
            ("000001.SZ", "220101", "20200101", None),
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            output_json = Path(temp_dir) / "audit.json"
            output_gap_csv = Path(temp_dir) / "gaps.csv"
            with (
                patch(
                    "alpha_find_v2.sw_industry_pit_audit._load_staged_intervals",
                    return_value=[],
                ),
                patch(
                    "alpha_find_v2.sw_industry_pit_audit._load_constituent_days",
                    return_value=constituent_days,
                ),
                patch(
                    "alpha_find_v2.sw_industry_pit_audit._load_or_fetch_tushare_intervals",
                    return_value=live_tushare_intervals,
                ),
                patch(
                    "alpha_find_v2.sw_industry_pit_audit._load_or_fetch_akshare_intervals",
                    return_value=akshare_intervals,
                ),
                patch(
                    "alpha_find_v2.sw_industry_pit_audit._load_manual_adjudication_intervals",
                    return_value=[],
                ),
            ):
                payload = run_one_shot_sw_industry_pit_audit(
                    reference_db="ignored.duckdb",
                    research_db="ignored.duckdb",
                    benchmark_id="CSI 800",
                    industry_schema="sw2021_l1",
                    industry_level="L1",
                    start_date="20210105",
                    end_date="20210107",
                    output_json=output_json,
                    output_gap_csv=output_gap_csv,
                )
            persisted = json.loads(output_json.read_text(encoding="utf-8"))

        self.assertEqual(
            payload["derived_bridge_fill_summary"]["run_counts"],
            {"eligible_bridge_fill": 1},
        )
        self.assertEqual(
            payload["derived_bridge_fill_summary"]["day_counts"],
            {"eligible_bridge_fill": 2},
        )
        self.assertEqual(
            payload["derived_bridge_fill_examples"][0]["imputed_industry_code"],
            "801010.SI",
        )
        self.assertEqual(
            persisted["derived_bridge_fill_summary"]["eligible_day_count"],
            2,
        )


if __name__ == "__main__":
    unittest.main()
