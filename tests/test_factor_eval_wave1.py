"""
Tests for factor_evaluation Wave 1:
  - exceptions.py
  - tests/_fixtures/synth_research_db.py
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from alpha_find_v2.factor_evaluation.exceptions import (
    DescriptorNotImplemented,
    EvaluationError,
    UniverseEmpty,
)


# ---------------------------------------------------------------------------
# Task 1: exceptions.py
# ---------------------------------------------------------------------------


class DescriptorNotImplementedTest(unittest.TestCase):
    def test_carries_descriptor_id(self) -> None:
        exc = DescriptorNotImplemented("accrual_quality", ("pit_fina_indicator",))
        self.assertEqual(exc.descriptor_id, "accrual_quality")

    def test_carries_requires_tuple(self) -> None:
        exc = DescriptorNotImplemented(
            "profitability_quality",
            ("pit_fina_indicator", "raw_income"),
        )
        self.assertIn("pit_fina_indicator", exc.requires)
        self.assertIn("raw_income", exc.requires)

    def test_default_message_contains_descriptor_and_datasets(self) -> None:
        exc = DescriptorNotImplemented(
            "leverage_conservatism",
            ("raw_balancesheet",),
        )
        self.assertIn("leverage_conservatism", str(exc))
        self.assertIn("raw_balancesheet", str(exc))

    def test_custom_message_overrides_default(self) -> None:
        exc = DescriptorNotImplemented("foo", ("bar",), message="Custom msg")
        self.assertEqual(str(exc), "Custom msg")

    def test_is_an_exception(self) -> None:
        with self.assertRaises(DescriptorNotImplemented):
            raise DescriptorNotImplemented("x", ("y",))

    def test_stringifies_cleanly(self) -> None:
        exc = DescriptorNotImplemented("foo", ("bar", "baz"))
        s = str(exc)
        self.assertIsInstance(s, str)
        self.assertGreater(len(s), 0)


class UniverseEmptyTest(unittest.TestCase):
    def test_carries_fields(self) -> None:
        exc = UniverseEmpty("csi800", "20240101", "20241231")
        self.assertEqual(exc.universe_id, "csi800")
        self.assertEqual(exc.start_date, "20240101")
        self.assertEqual(exc.end_date, "20241231")

    def test_is_an_exception(self) -> None:
        with self.assertRaises(UniverseEmpty):
            raise UniverseEmpty("csi800", "20240101", "20241231")


class EvaluationErrorTest(unittest.TestCase):
    def test_default_exit_code(self) -> None:
        exc = EvaluationError("something failed")
        self.assertEqual(exc.exit_code, 6)

    def test_custom_exit_code(self) -> None:
        exc = EvaluationError("research db missing", exit_code=4)
        self.assertEqual(exc.exit_code, 4)

    def test_message_preserved(self) -> None:
        exc = EvaluationError("msg xyz")
        self.assertIn("msg xyz", str(exc))


# ---------------------------------------------------------------------------
# Task 2: synthetic fixture
# ---------------------------------------------------------------------------


class SynthResearchDbTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._db_path = Path(self._tmp.name) / "research_source.duckdb"
        from tests._fixtures.synth_research_db import build_synth_research_db
        build_synth_research_db(self._db_path, n_securities=5, n_dates=250)
        self._conn = duckdb.connect(str(self._db_path), read_only=True)

    def tearDown(self) -> None:
        self._conn.close()
        self._tmp.cleanup()

    def test_trade_calendar_has_correct_row_count(self) -> None:
        count = self._conn.execute(
            "SELECT COUNT(*) FROM market_trade_calendar"
        ).fetchone()[0]
        self.assertEqual(count, 250)

    def test_daily_bar_pit_has_correct_row_count(self) -> None:
        count = self._conn.execute(
            "SELECT COUNT(*) FROM daily_bar_pit"
        ).fetchone()[0]
        self.assertEqual(count, 5 * 250)

    def test_daily_bar_pit_has_required_columns(self) -> None:
        cols = {
            r[0]
            for r in self._conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'daily_bar_pit'"
            ).fetchall()
        }
        required = {
            "security_id", "trade_date", "open", "close", "close_adj",
            "turnover_value_cny", "open_adj", "float_mcap_cny",
        }
        self.assertTrue(required.issubset(cols), msg=f"Missing: {required - cols}")

    def test_raw_daily_basic_has_pb_pe_free_share(self) -> None:
        cols = {
            r[0]
            for r in self._conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'raw_daily_basic'"
            ).fetchall()
        }
        self.assertIn("pb", cols)
        self.assertIn("pe", cols)
        self.assertIn("free_share", cols)

    def test_industry_classification_pit_populated(self) -> None:
        count = self._conn.execute(
            "SELECT COUNT(*) FROM industry_classification_pit"
        ).fetchone()[0]
        self.assertEqual(count, 5)

    def test_benchmark_membership_pit_populated(self) -> None:
        count = self._conn.execute(
            "SELECT COUNT(*) FROM benchmark_membership_pit"
        ).fetchone()[0]
        self.assertEqual(count, 5)

    def test_security_master_ref_populated(self) -> None:
        count = self._conn.execute(
            "SELECT COUNT(*) FROM security_master_ref"
        ).fetchone()[0]
        self.assertEqual(count, 5)

    def test_adj_factor_all_ones(self) -> None:
        min_af, max_af = self._conn.execute(
            "SELECT MIN(adj_factor), MAX(adj_factor) FROM raw_adj_factor"
        ).fetchone()
        self.assertAlmostEqual(min_af, 1.0)
        self.assertAlmostEqual(max_af, 1.0)

    def test_prices_are_monotone_per_security(self) -> None:
        # Each security's close_adj should be strictly increasing
        result = self._conn.execute(
            """
            SELECT security_id,
                   MIN(close_adj) AS min_p,
                   MAX(close_adj) AS max_p
            FROM daily_bar_pit
            GROUP BY security_id
            """
        ).fetchall()
        for sec_id, min_p, max_p in result:
            self.assertGreater(
                max_p, min_p,
                msg=f"{sec_id}: prices not monotone",
            )

    def test_no_null_close_adj(self) -> None:
        nulls = self._conn.execute(
            "SELECT COUNT(*) FROM daily_bar_pit WHERE close_adj IS NULL"
        ).fetchone()[0]
        self.assertEqual(nulls, 0)

    def test_deterministic_recreation(self) -> None:
        """Building the same DB twice gives identical results."""
        from tests._fixtures.synth_research_db import build_synth_research_db
        path2 = Path(self._tmp.name) / "research_source2.duckdb"
        build_synth_research_db(path2, n_securities=5, n_dates=250)
        conn2 = duckdb.connect(str(path2), read_only=True)
        p1 = self._conn.execute(
            "SELECT close_adj FROM daily_bar_pit WHERE security_id='600001.SH' ORDER BY trade_date LIMIT 5"
        ).fetchall()
        p2 = conn2.execute(
            "SELECT close_adj FROM daily_bar_pit WHERE security_id='600001.SH' ORDER BY trade_date LIMIT 5"
        ).fetchall()
        conn2.close()
        self.assertEqual(p1, p2)


class SynthRawDbTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self._research_db = Path(self._tmp.name) / "research_source.duckdb"
        self._raw_db = Path(self._tmp.name) / "raw.duckdb"
        from tests._fixtures.synth_research_db import (
            build_synth_research_db,
            build_synth_raw_db,
        )
        build_synth_research_db(self._research_db, n_securities=3, n_dates=30)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_empty_raw_db_has_tables(self) -> None:
        from tests._fixtures.synth_research_db import build_synth_raw_db
        build_synth_raw_db(self._raw_db, research_db_path=self._research_db)
        conn = duckdb.connect(str(self._raw_db), read_only=True)
        tables = {r[0] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()}
        self.assertIn("raw_suspend_d", tables)
        self.assertIn("raw_stk_limit", tables)
        conn.close()

    def test_suspended_row_present(self) -> None:
        from tests._fixtures.synth_research_db import build_synth_raw_db
        build_synth_raw_db(
            self._raw_db,
            research_db_path=self._research_db,
            suspended_rows={("600001.SH", "20220103")},
        )
        conn = duckdb.connect(str(self._raw_db), read_only=True)
        count = conn.execute(
            "SELECT COUNT(*) FROM raw_suspend_d WHERE ts_code='600001.SH'"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)

    def test_limit_locked_row_present(self) -> None:
        from tests._fixtures.synth_research_db import build_synth_raw_db
        build_synth_raw_db(
            self._raw_db,
            research_db_path=self._research_db,
            limit_locked_rows={("600003.SH", "20220105")},
        )
        conn = duckdb.connect(str(self._raw_db), read_only=True)
        count = conn.execute(
            "SELECT COUNT(*) FROM raw_stk_limit WHERE ts_code='600003.SH'"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
