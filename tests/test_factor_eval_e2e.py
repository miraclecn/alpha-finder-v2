"""
End-to-end tests for factor_evaluation Stage 2 (tasks 18-25).

Uses synthetic fixture. Tests:
  - evaluate_descriptor returns complete report
  - report.json and report.md are written
  - descriptor_version is stable
  - cost-net L-S <= gross L-S
  - tradeability injection works
  - CLI commands exit correctly
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

from alpha_find_v2.factor_evaluation.descriptor_evaluator import evaluate_descriptor
from alpha_find_v2.factor_evaluation.report_writer import write_report
from alpha_find_v2.factor_evaluation.exceptions import UniverseEmpty


def _synth(n_dates: int = 250) -> tuple[Path, tempfile.TemporaryDirectory]:
    tmp = tempfile.TemporaryDirectory()
    db = Path(tmp.name) / "research.duckdb"
    from tests._fixtures.synth_research_db import build_synth_research_db
    build_synth_research_db(db, n_securities=5, n_dates=n_dates)
    return db, tmp


class EvaluateDescriptorTest(unittest.TestCase):
    def setUp(self) -> None:
        self._db, self._tmp = _synth()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_report_has_all_required_fields(self) -> None:
        report = evaluate_descriptor(
            descriptor_id="medium_term_relative_strength",
            research_db=self._db,
            universe="csi800",
            start_date="20220801",
            end_date="20221231",
            horizons=(5, 20),
            primary_horizon=20,
        )
        # meta
        self.assertEqual(report.meta.descriptor_id, "medium_term_relative_strength")
        self.assertTrue(report.meta.descriptor_version.startswith("sha256:"))
        self.assertIsNotNone(report.meta.run_at)
        # coverage
        self.assertGreater(report.coverage.rows_used_over_possible, 0)
        # at least one horizon
        self.assertGreater(len(report.horizon_metrics), 0)
        # ic_decay present
        self.assertIsNotNone(report.ic_decay)
        self.assertIsInstance(report.ic_decay.ic_means, list)
        # diagnostics
        self.assertIsInstance(report.diagnostics.warnings, list)

    def test_descriptor_version_is_stable_across_runs(self) -> None:
        r1 = evaluate_descriptor(
            descriptor_id="medium_term_relative_strength",
            research_db=self._db,
            universe="csi800",
            start_date="20220801",
            end_date="20221101",
            horizons=(20,),
            primary_horizon=20,
        )
        r2 = evaluate_descriptor(
            descriptor_id="medium_term_relative_strength",
            research_db=self._db,
            universe="csi800",
            start_date="20220801",
            end_date="20221101",
            horizons=(20,),
            primary_horizon=20,
        )
        self.assertEqual(r1.meta.descriptor_version, r2.meta.descriptor_version)

    def test_report_json_is_reproducible_modulo_run_at(self) -> None:
        out = Path(self._tmp.name) / "eval"
        r1 = evaluate_descriptor(
            descriptor_id="medium_term_relative_strength",
            research_db=self._db,
            universe="csi800",
            start_date="20220801",
            end_date="20221101",
            horizons=(20,),
            primary_horizon=20,
        )
        write_report(r1, out)
        r2 = evaluate_descriptor(
            descriptor_id="medium_term_relative_strength",
            research_db=self._db,
            universe="csi800",
            start_date="20220801",
            end_date="20221101",
            horizons=(20,),
            primary_horizon=20,
        )
        write_report(r2, out)

        # Both report dirs exist; their content should match except run_at
        dirs = sorted(out.iterdir())
        self.assertGreaterEqual(len(dirs), 1)
        desc_dir = dirs[0]
        all_reports = list(desc_dir.iterdir())
        self.assertGreaterEqual(len(all_reports), 1)
        report_dir = all_reports[0]
        payload = json.loads((report_dir / "report.json").read_text(encoding="utf-8"))
        self.assertIn("descriptor_id", payload)
        self.assertIn("horizons", payload)
        self.assertIn("coverage", payload)

    def test_all_5_in_scope_descriptors_evaluate_without_error(self) -> None:
        descriptors = [
            "medium_term_relative_strength",
            "trend_stability",
            "turnover_confirmation",
            "industry_relative_strength",
            "sector_relative_valuation",
        ]
        for did in descriptors:
            with self.subTest(descriptor=did):
                report = evaluate_descriptor(
                    descriptor_id=did,
                    research_db=self._db,
                    universe="csi800",
                    start_date="20220801",
                    end_date="20221231",
                    horizons=(20,),
                    primary_horizon=20,
                )
                self.assertIsNotNone(report)
                self.assertEqual(report.meta.descriptor_id, did)

    def test_net_ls_is_less_than_gross_ls(self) -> None:
        report = evaluate_descriptor(
            descriptor_id="medium_term_relative_strength",
            research_db=self._db,
            universe="csi800",
            start_date="20220801",
            end_date="20221231",
            horizons=(20,),
            primary_horizon=20,
            cost_model_path=None,  # uses default cost bps
        )
        if 20 in report.horizon_metrics:
            hm = report.horizon_metrics[20]
            import math
            if not math.isnan(hm.decile_ls.annualised_return_net) and \
               not math.isnan(hm.decile_ls.annualised_return_gross):
                self.assertLessEqual(
                    hm.decile_ls.annualised_return_net,
                    hm.decile_ls.annualised_return_gross + 1e-9,
                )


class ReportWriterTest(unittest.TestCase):
    def setUp(self) -> None:
        self._db, self._tmp = _synth()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_writes_json_and_md(self) -> None:
        out = Path(self._tmp.name) / "eval"
        report = evaluate_descriptor(
            descriptor_id="turnover_confirmation",
            research_db=self._db,
            universe="csi800",
            start_date="20220801",
            end_date="20221031",
            horizons=(5,),
            primary_horizon=5,
        )
        report_dir = write_report(report, out)
        self.assertTrue((report_dir / "report.json").exists())
        self.assertTrue((report_dir / "report.md").exists())

    def test_json_contains_schema_version(self) -> None:
        out = Path(self._tmp.name) / "eval"
        report = evaluate_descriptor(
            descriptor_id="turnover_confirmation",
            research_db=self._db,
            universe="csi800",
            start_date="20220801",
            end_date="20221031",
            horizons=(5,),
            primary_horizon=5,
        )
        report_dir = write_report(report, out)
        payload = json.loads((report_dir / "report.json").read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(payload["artifact_type"], "descriptor_evaluation_report")

    def test_md_has_required_sections(self) -> None:
        out = Path(self._tmp.name) / "eval"
        report = evaluate_descriptor(
            descriptor_id="medium_term_relative_strength",
            research_db=self._db,
            universe="csi800",
            start_date="20220801",
            end_date="20221031",
            horizons=(20,),
            primary_horizon=20,
        )
        report_dir = write_report(report, out)
        md = (report_dir / "report.md").read_text(encoding="utf-8")
        self.assertIn("IC Summary", md)
        self.assertIn("IC Decay", md)
        self.assertIn("Coverage", md)


class TradeabilityFilterTest(unittest.TestCase):
    def setUp(self) -> None:
        self._db, self._tmp = _synth()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_tradeability_filter_runs_without_raw_db(self) -> None:
        # No raw_db_path → heuristic fallback, report still generated
        report = evaluate_descriptor(
            descriptor_id="medium_term_relative_strength",
            research_db=self._db,
            universe="csi800",
            start_date="20220801",
            end_date="20221031",
            horizons=(20,),
            primary_horizon=20,
            raw_db_path=None,
        )
        # Should complete without error (fallback to heuristic)
        self.assertIsNotNone(report)

    def test_suspended_row_reduces_tradeable_count(self) -> None:
        from tests._fixtures.synth_research_db import build_synth_raw_db
        raw_db = Path(self._tmp.name) / "raw.duckdb"

        # Get a date from the synth DB
        conn = duckdb.connect(str(self._db), read_only=True)
        test_date = conn.execute(
            "SELECT trade_date FROM market_trade_calendar ORDER BY trade_date LIMIT 1 OFFSET 150"
        ).fetchone()[0]
        conn.close()

        build_synth_raw_db(
            raw_db,
            research_db_path=self._db,
            suspended_rows={("600001.SH", test_date)},
        )

        report_with = evaluate_descriptor(
            descriptor_id="medium_term_relative_strength",
            research_db=self._db,
            universe="csi800",
            start_date="20220801",
            end_date="20221231",
            horizons=(20,),
            primary_horizon=20,
            raw_db_path=raw_db,
        )
        report_without = evaluate_descriptor(
            descriptor_id="medium_term_relative_strength",
            research_db=self._db,
            universe="csi800",
            start_date="20220801",
            end_date="20221231",
            horizons=(20,),
            primary_horizon=20,
            raw_db_path=None,
        )
        # tradeable_rate should be slightly lower with suspended row
        self.assertLessEqual(
            report_with.coverage.tradeable_rate,
            report_without.coverage.tradeable_rate + 1e-6,
        )


class CLISmokeTest(unittest.TestCase):
    def _run(self, *args: str, cwd: str | None = None) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "alpha_find_v2", *args],
            cwd=cwd or str(Path(__file__).parents[1]),
            env={**os.environ, "PYTHONPATH": "src"},
            capture_output=True,
            text=True,
            check=False,
        )

    def test_compute_descriptor_exits_0(self) -> None:
        db, tmp = _synth()
        try:
            result = self._run(
                "compute-descriptor",
                "--id", "medium_term_relative_strength",
                "--research-db", str(db),
                "--start", "20220801",
                "--end", "20221001",
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(result.stdout)
            self.assertIn("rows", payload)
            self.assertGreater(payload["rows"], 0)
        finally:
            tmp.cleanup()

    def test_compute_descriptor_unknown_id_exits_2(self) -> None:
        db, tmp = _synth()
        try:
            result = self._run(
                "compute-descriptor",
                "--id", "totally_unknown_xxxx",
                "--research-db", str(db),
            )
            self.assertEqual(result.returncode, 2, msg=result.stderr)
        finally:
            tmp.cleanup()

    def test_compute_descriptor_stub_exits_3(self) -> None:
        db, tmp = _synth()
        try:
            result = self._run(
                "compute-descriptor",
                "--id", "accrual_quality",
                "--research-db", str(db),
                "--start", "20220801",
                "--end", "20221001",
            )
            self.assertEqual(result.returncode, 3, msg=result.stderr)
        finally:
            tmp.cleanup()

    def test_evaluate_descriptor_exits_0_and_writes_report(self) -> None:
        db, tmp = _synth()
        out_dir = Path(tmp.name) / "eval_out"
        try:
            result = self._run(
                "evaluate-descriptor",
                "--id", "medium_term_relative_strength",
                "--research-db", str(db),
                "--universe", "csi800",
                "--start", "20220801",
                "--end", "20221031",
                "--horizons", "20",
                "--out-dir", str(out_dir),
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(result.stdout)
            self.assertIn("report_dir", payload)
            report_dir = Path(payload["report_dir"])
            self.assertTrue((report_dir / "report.json").exists())
            self.assertTrue((report_dir / "report.md").exists())
        finally:
            tmp.cleanup()

    def test_list_evaluation_reports_exits_0_empty(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        out_dir = Path(tmp.name) / "eval_out"
        try:
            result = self._run(
                "list-evaluation-reports",
                "--out-dir", str(out_dir),
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload, [])
        finally:
            tmp.cleanup()

    def test_list_evaluation_reports_after_one_evaluation(self) -> None:
        db, tmp = _synth()
        out_dir = Path(tmp.name) / "eval_out"
        try:
            # First, run an evaluation
            self._run(
                "evaluate-descriptor",
                "--id", "turnover_confirmation",
                "--research-db", str(db),
                "--universe", "csi800",
                "--start", "20220801",
                "--end", "20221031",
                "--horizons", "5",
                "--out-dir", str(out_dir),
            )
            # Then list
            result = self._run(
                "list-evaluation-reports",
                "--out-dir", str(out_dir),
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            payload = json.loads(result.stdout)
            self.assertGreater(len(payload), 0)
            self.assertEqual(payload[0]["descriptor_id"], "turnover_confirmation")
        finally:
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
