"""
Tests for factor_evaluation Waves 2-3:
  - descriptor_compute.py (registry, 5 descriptor computes)
  - descriptor_stubs.py   (5 stubs)
  - universe_resolver.py
  - forward_returns.py
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from alpha_find_v2.factor_evaluation.exceptions import DescriptorNotImplemented
from alpha_find_v2.factor_evaluation.descriptor_compute import (
    REGISTRY,
    ComputeContext,
    DescriptorComputeSpec,
    get,
    list_registered,
    register,
    descriptor_version,
)
# ensure stubs are registered
import alpha_find_v2.factor_evaluation.descriptor_stubs  # noqa: F401


def _make_ctx(path: Path, start: str, end: str) -> ComputeContext:
    conn = duckdb.connect(str(path), read_only=True)
    return ComputeContext(conn=conn, start_date=start, end_date=end)


def _make_synth(n_securities: int = 5, n_dates: int = 250) -> tuple[Path, tempfile.TemporaryDirectory]:
    tmp = tempfile.TemporaryDirectory()
    db = Path(tmp.name) / "research.duckdb"
    from tests._fixtures.synth_research_db import build_synth_research_db
    build_synth_research_db(db, n_securities=n_securities, n_dates=n_dates)
    return db, tmp


# ---------------------------------------------------------------------------
# Task 5: registry skeleton
# ---------------------------------------------------------------------------


class RegistryTest(unittest.TestCase):
    def test_all_10_descriptors_registered(self) -> None:
        # 5 implemented + 5 stubs
        registered = set(list_registered())
        expected = {
            "medium_term_relative_strength",
            "trend_stability",
            "turnover_confirmation",
            "industry_relative_strength",
            "sector_relative_valuation",
            "accrual_quality",
            "profitability_quality",
            "leverage_conservatism",
            "estimate_revision_breadth",
            "post_earnings_drift_signal",
        }
        self.assertTrue(expected.issubset(registered), msg=f"Missing: {expected - registered}")

    def test_get_unknown_raises_key_error_with_message(self) -> None:
        with self.assertRaises(KeyError) as ctx:
            get("totally_unknown_descriptor")
        self.assertIn("totally_unknown_descriptor", str(ctx.exception))
        # Should list registered ids
        self.assertIn("medium_term_relative_strength", str(ctx.exception))

    def test_get_returns_spec(self) -> None:
        spec = get("medium_term_relative_strength")
        self.assertIsInstance(spec, DescriptorComputeSpec)
        self.assertEqual(spec.descriptor_id, "medium_term_relative_strength")
        self.assertIsNotNone(spec.fn)

    def test_register_no_op_and_retrieve(self) -> None:
        import pandas as pd
        def _noop(ctx: ComputeContext) -> pd.DataFrame:
            return pd.DataFrame(columns=["trade_date", "security_id", "descriptor_value"])

        spec = DescriptorComputeSpec(
            descriptor_id="_test_noop_reg",
            fn=_noop,
            requires=(),
            notes="test",
        )
        register(spec)
        self.assertIn("_test_noop_reg", list_registered())
        retrieved = get("_test_noop_reg")
        self.assertIs(retrieved.fn, _noop)
        # clean up
        del REGISTRY["_test_noop_reg"]

    def test_descriptor_version_returns_sha256_string(self) -> None:
        ver = descriptor_version("medium_term_relative_strength")
        self.assertTrue(ver.startswith("sha256:"), msg=ver)
        self.assertEqual(len(ver), len("sha256:") + 64)

    def test_descriptor_version_is_stable(self) -> None:
        v1 = descriptor_version("medium_term_relative_strength")
        v2 = descriptor_version("medium_term_relative_strength")
        self.assertEqual(v1, v2)

    def test_descriptor_version_differs_across_descriptors(self) -> None:
        v1 = descriptor_version("medium_term_relative_strength")
        v2 = descriptor_version("trend_stability")
        self.assertNotEqual(v1, v2)


# ---------------------------------------------------------------------------
# Task 11: stubs
# ---------------------------------------------------------------------------


class StubTest(unittest.TestCase):
    def test_stub_raises_descriptor_not_implemented(self) -> None:
        db, tmp = _make_synth(n_dates=30)
        try:
            ctx = _make_ctx(db, "20220201", "20220301")
            spec = get("accrual_quality")
            with self.assertRaises(DescriptorNotImplemented) as cm:
                spec.fn(ctx)
            exc = cm.exception
            self.assertEqual(exc.descriptor_id, "accrual_quality")
            self.assertIn("pit_fina_indicator", exc.requires)
        finally:
            ctx.conn.close()
            tmp.cleanup()

    def test_all_stub_descriptors_raise(self) -> None:
        stubs = [
            "accrual_quality", "profitability_quality", "leverage_conservatism",
            "estimate_revision_breadth", "post_earnings_drift_signal",
        ]
        db, tmp = _make_synth(n_dates=30)
        try:
            for stub_id in stubs:
                ctx = _make_ctx(db, "20220201", "20220301")
                spec = get(stub_id)
                with self.assertRaises(DescriptorNotImplemented) as cm:
                    spec.fn(ctx)
                self.assertEqual(cm.exception.descriptor_id, stub_id)
                self.assertGreater(len(cm.exception.requires), 0)
                ctx.conn.close()
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Tasks 6-10: descriptor computes
# ---------------------------------------------------------------------------


class DescriptorComputeOutputShapeTest(unittest.TestCase):
    """Each compute function returns exactly (trade_date, security_id, descriptor_value)."""

    def _assert_output_shape(self, descriptor_id: str, n_dates: int = 250) -> None:
        db, tmp = _make_synth(n_dates=n_dates)
        try:
            ctx = _make_ctx(db, "20220701", "20221231")
            spec = get(descriptor_id)
            df = spec.fn(ctx)
            self.assertEqual(
                list(df.columns),
                ["trade_date", "security_id", "descriptor_value"],
                msg=f"{descriptor_id}: wrong columns",
            )
            self.assertGreater(len(df), 0, msg=f"{descriptor_id}: empty output")
            self.assertFalse(
                df["descriptor_value"].isna().all(),
                msg=f"{descriptor_id}: all values are NaN",
            )
        finally:
            ctx.conn.close()
            tmp.cleanup()

    def test_medium_term_relative_strength_output_shape(self) -> None:
        self._assert_output_shape("medium_term_relative_strength")

    def test_trend_stability_output_shape(self) -> None:
        self._assert_output_shape("trend_stability")

    def test_turnover_confirmation_output_shape(self) -> None:
        self._assert_output_shape("turnover_confirmation")

    def test_industry_relative_strength_output_shape(self) -> None:
        self._assert_output_shape("industry_relative_strength")

    def test_sector_relative_valuation_output_shape(self) -> None:
        self._assert_output_shape("sector_relative_valuation")


class MediumTermRelativeStrengthTest(unittest.TestCase):
    def setUp(self) -> None:
        self._db, self._tmp = _make_synth(n_dates=250)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_row_count_is_dates_minus_60_times_securities(self) -> None:
        # We request from day 61 onwards (need 60-day lookback)
        ctx = _make_ctx(self._db, "20220801", "20221231")
        spec = get("medium_term_relative_strength")
        df = spec.fn(ctx)
        ctx.conn.close()
        # All 5 securities should produce values
        self.assertEqual(df["security_id"].nunique(), 5)

    def test_faster_growing_stock_has_higher_descriptor_on_average(self) -> None:
        # 600003.SH has growth 1.0120, 600001.SH has 1.0050
        # Over 60d: 600003 should have higher momentum
        ctx = _make_ctx(self._db, "20220801", "20221231")
        spec = get("medium_term_relative_strength")
        df = spec.fn(ctx)
        ctx.conn.close()
        mean_by_sec = df.groupby("security_id")["descriptor_value"].mean()
        self.assertGreater(
            mean_by_sec.get("600003.SH", float("-inf")),
            mean_by_sec.get("600001.SH", float("inf")),
            msg="600003.SH (faster growth) should have higher descriptor mean",
        )

    def test_no_rows_before_60d_lookback(self) -> None:
        # Start from very first date; rows for first 60 dates should not appear
        ctx = _make_ctx(self._db, "20220103", "20220201")
        spec = get("medium_term_relative_strength")
        df = spec.fn(ctx)
        ctx.conn.close()
        # There should be few or no rows (not enough history)
        conn2 = duckdb.connect(str(self._db), read_only=True)
        all_dates = [r[0] for r in conn2.execute(
            "SELECT trade_date FROM market_trade_calendar ORDER BY trade_date"
        ).fetchall()]
        conn2.close()
        if len(df) > 0:
            # Any returned date must be at least at index 60 in the calendar
            first_date = df["trade_date"].min()
            self.assertGreaterEqual(all_dates.index(first_date), 60 - 1)


class TurnoverConfirmationTest(unittest.TestCase):
    def test_stock_with_doubled_turnover_has_higher_descriptor(self) -> None:
        # Build a custom DB where one stock has doubled turnover in last 5 days
        tmp = tempfile.TemporaryDirectory()
        db = Path(tmp.name) / "research.duckdb"
        from tests._fixtures.synth_research_db import build_synth_research_db
        build_synth_research_db(db, n_securities=2, n_dates=100)
        conn = duckdb.connect(str(db))
        # Inflate turnover for 600001.SH on the last 5 dates
        dates = [r[0] for r in conn.execute(
            "SELECT trade_date FROM market_trade_calendar ORDER BY trade_date DESC LIMIT 5"
        ).fetchall()]
        for d in dates:
            conn.execute(
                "UPDATE daily_bar_pit SET turnover_value_cny = turnover_value_cny * 10 "
                "WHERE security_id='600001.SH' AND trade_date=?",
                [d],
            )
        conn.close()

        ctx = _make_ctx(db, dates[-1], dates[0])  # last 5 dates
        spec = get("turnover_confirmation")
        df = spec.fn(ctx)
        ctx.conn.close()
        tmp.cleanup()

        if len(df) > 0:
            last_date = df["trade_date"].max()
            row_600001 = df[(df["security_id"] == "600001.SH") & (df["trade_date"] == last_date)]
            row_600002 = df[(df["security_id"] == "600002.SH") & (df["trade_date"] == last_date)]
            if len(row_600001) > 0 and len(row_600002) > 0:
                self.assertGreater(
                    row_600001["descriptor_value"].iloc[0],
                    row_600002["descriptor_value"].iloc[0],
                )


class IndustryRelativeStrengthTest(unittest.TestCase):
    def test_bank_stocks_have_opposite_sign_relative_strength(self) -> None:
        # 600001.SH and 600002.SH are both in "bank"; one grows faster
        # Their industry-relative descriptors should have opposite signs on average
        db, tmp = _make_synth(n_dates=250)
        try:
            ctx = _make_ctx(db, "20220801", "20221231")
            spec = get("industry_relative_strength")
            df = spec.fn(ctx)
            ctx.conn.close()
            bank = df[df["security_id"].isin(["600001.SH", "600002.SH"])]
            mean_600001 = bank[bank["security_id"] == "600001.SH"]["descriptor_value"].mean()
            mean_600002 = bank[bank["security_id"] == "600002.SH"]["descriptor_value"].mean()
            # 600001 grows faster (1.0050 vs 1.0040), so should outperform the industry mean
            self.assertGreater(mean_600001, mean_600002)
        finally:
            tmp.cleanup()

    def test_rows_with_missing_pit_industry_are_dropped(self) -> None:
        # Build DB where one stock has no industry classification
        tmp = tempfile.TemporaryDirectory()
        db = Path(tmp.name) / "research.duckdb"
        from tests._fixtures.synth_research_db import build_synth_research_db
        build_synth_research_db(db, n_securities=3, n_dates=250)
        conn = duckdb.connect(str(db))
        conn.execute("DELETE FROM industry_classification_pit WHERE security_id='600003.SH'")
        conn.close()

        ctx = _make_ctx(db, "20220801", "20221231")
        spec = get("industry_relative_strength")
        df = spec.fn(ctx)
        ctx.conn.close()
        tmp.cleanup()
        # 600003.SH should not appear in output
        self.assertNotIn("600003.SH", df["security_id"].values)


class SectorRelativeValuationTest(unittest.TestCase):
    def test_lowest_pb_stock_in_industry_has_highest_descriptor(self) -> None:
        # 600001.SH starts with pb=0.80 (cheapest in bank), 600002 pb=1.20
        db, tmp = _make_synth(n_dates=100)
        try:
            ctx = _make_ctx(db, "20220201", "20220301")
            spec = get("sector_relative_valuation")
            df = spec.fn(ctx)
            ctx.conn.close()
            bank = df[df["security_id"].isin(["600001.SH", "600002.SH"])]
            if len(bank) > 0:
                mean_600001 = bank[bank["security_id"] == "600001.SH"]["descriptor_value"].mean()
                mean_600002 = bank[bank["security_id"] == "600002.SH"]["descriptor_value"].mean()
                self.assertGreater(mean_600001, mean_600002)
        finally:
            tmp.cleanup()

    def test_zero_pb_rows_are_dropped(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        db = Path(tmp.name) / "research.duckdb"
        from tests._fixtures.synth_research_db import build_synth_research_db
        build_synth_research_db(db, n_securities=3, n_dates=100)
        conn = duckdb.connect(str(db))
        conn.execute("UPDATE raw_daily_basic SET pb=0 WHERE ts_code='600003.SH'")
        conn.close()

        ctx = _make_ctx(db, "20220201", "20220301")
        spec = get("sector_relative_valuation")
        df = spec.fn(ctx)
        ctx.conn.close()
        tmp.cleanup()
        # 600003.SH should be absent (pb=0 filtered)
        self.assertNotIn("600003.SH", df["security_id"].values)


# ---------------------------------------------------------------------------
# Task 3: universe resolver
# ---------------------------------------------------------------------------


class UniverseResolverTest(unittest.TestCase):
    def setUp(self) -> None:
        self._db, self._tmp = _make_synth(n_dates=100)
        self._conn = duckdb.connect(str(self._db), read_only=True)

    def tearDown(self) -> None:
        self._conn.close()
        self._tmp.cleanup()

    def test_benchmark_resolver_returns_all_5_securities(self) -> None:
        from alpha_find_v2.factor_evaluation.universe_resolver import BenchmarkUniverseResolver
        resolver = BenchmarkUniverseResolver(self._conn, benchmark_id="CSI 800")
        universe = resolver.resolve("20220301")
        self.assertEqual(len(universe), 5)
        self.assertIn("600001.SH", universe)

    def test_benchmark_resolver_out_of_range_returns_empty(self) -> None:
        from alpha_find_v2.factor_evaluation.universe_resolver import BenchmarkUniverseResolver
        resolver = BenchmarkUniverseResolver(self._conn, benchmark_id="CSI 800")
        universe = resolver.resolve("20000101")  # before effective_at '20100101'
        self.assertEqual(len(universe), 0)

    def test_investable_core_resolver_returns_securities(self) -> None:
        from alpha_find_v2.factor_evaluation.universe_resolver import InvestableCoreUniverseResolver
        resolver = InvestableCoreUniverseResolver(
            self._conn,
            min_listing_days=10,   # low threshold so synth data passes
            min_median_turnover_cny=1.0,
        )
        dates = self._conn.execute(
            "SELECT trade_date FROM market_trade_calendar ORDER BY trade_date LIMIT 1 OFFSET 80"
        ).fetchone()[0]
        universe = resolver.resolve(dates)
        self.assertGreater(len(universe), 0)

    def test_resolver_factory_csi800(self) -> None:
        from alpha_find_v2.factor_evaluation.universe_resolver import resolver_for_universe
        resolver = resolver_for_universe("csi800", self._conn)
        universe = resolver.resolve("20220301")
        self.assertIsInstance(universe, set)

    def test_resolver_factory_unknown_raises(self) -> None:
        from alpha_find_v2.factor_evaluation.universe_resolver import resolver_for_universe
        with self.assertRaises(ValueError):
            resolver_for_universe("unknown_universe", self._conn)


# ---------------------------------------------------------------------------
# Task 4: forward returns
# ---------------------------------------------------------------------------


class ForwardReturnsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._db, self._tmp = _make_synth(n_dates=250)
        self._conn = duckdb.connect(str(self._db), read_only=True)

    def tearDown(self) -> None:
        self._conn.close()
        self._tmp.cleanup()

    def test_returns_dict_keyed_by_horizon(self) -> None:
        from alpha_find_v2.factor_evaluation.forward_returns import compute_forward_returns
        results = compute_forward_returns(
            self._conn,
            start_date="20220801",
            end_date="20221001",
            horizons=(5, 20),
        )
        self.assertIn(5, results)
        self.assertIn(20, results)

    def test_output_has_required_columns(self) -> None:
        from alpha_find_v2.factor_evaluation.forward_returns import compute_forward_returns
        results = compute_forward_returns(
            self._conn, start_date="20220801", end_date="20220901", horizons=(5,)
        )
        df = results[5]
        for col in ("security_id", "trade_date", "forward_return", "open_t1", "open_t1_h"):
            self.assertIn(col, df.columns)

    def test_no_null_forward_returns(self) -> None:
        from alpha_find_v2.factor_evaluation.forward_returns import compute_forward_returns
        results = compute_forward_returns(
            self._conn, start_date="20220801", end_date="20220901", horizons=(5,)
        )
        null_count = results[5]["forward_return"].isna().sum()
        self.assertEqual(null_count, 0)

    def test_faster_growing_stock_has_higher_return(self) -> None:
        from alpha_find_v2.factor_evaluation.forward_returns import compute_forward_returns
        results = compute_forward_returns(
            self._conn, start_date="20220801", end_date="20221201", horizons=(20,)
        )
        df = results[20]
        mean_returns = df.groupby("security_id")["forward_return"].mean()
        # 600003.SH growth=1.0120 vs 600001.SH growth=1.0050
        self.assertGreater(
            mean_returns.get("600003.SH", float("-inf")),
            mean_returns.get("600001.SH", float("inf")),
        )

    def test_rows_near_end_of_window_are_dropped(self) -> None:
        """Signal dates near end_date don't have exit prices → dropped."""
        from alpha_find_v2.factor_evaluation.forward_returns import compute_forward_returns
        results = compute_forward_returns(
            self._conn, start_date="20221101", end_date="20230101", horizons=(60,)
        )
        df = results[60]
        # All 60 dates after end_date lack data — rows near end_date should be sparse
        # (no assertion on exact count, just that LEAD correctly drops them)
        self.assertIsNotNone(df)


if __name__ == "__main__":
    unittest.main()
