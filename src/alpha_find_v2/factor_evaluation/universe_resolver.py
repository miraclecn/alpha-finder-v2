"""
Universe resolver: per-trade-date set of eligible security_ids.

Two strategies:
  BenchmarkUniverseResolver   — benchmark_membership_pit lookup
  InvestableCoreUniverseResolver — mandate-filter-based
"""
from __future__ import annotations

import functools
from pathlib import Path
from typing import Any


class UniverseResolver:
    """Abstract base for universe resolvers."""

    def resolve(self, trade_date: str) -> set[str]:
        raise NotImplementedError

    def resolve_batch(self, trade_dates: list[str]) -> dict[str, set[str]]:
        """Resolve for multiple dates. Default: call resolve() per date."""
        return {d: self.resolve(d) for d in trade_dates}


class BenchmarkUniverseResolver(UniverseResolver):
    """
    Universe = PIT benchmark membership at each trade date.

    Uses benchmark_membership_pit with effective_at / removed_at semantics.
    """

    def __init__(self, conn: Any, benchmark_id: str = "CSI 800") -> None:
        self._conn = conn
        self._benchmark_id = benchmark_id
        self._cache: dict[str, set[str]] = {}

    def resolve(self, trade_date: str) -> set[str]:
        if trade_date in self._cache:
            return self._cache[trade_date]
        rows = self._conn.execute(
            """
            SELECT security_id
            FROM benchmark_membership_pit
            WHERE benchmark_id = ?
              AND effective_at <= ?
              AND (removed_at IS NULL OR removed_at > ?)
            """,
            [self._benchmark_id, trade_date, trade_date],
        ).fetchall()
        result = {r[0] for r in rows}
        self._cache[trade_date] = result
        return result

    def resolve_batch(self, trade_dates: list[str]) -> dict[str, set[str]]:
        """Pull all membership rows in one query and bucket by date."""
        if not trade_dates:
            return {}
        min_d, max_d = min(trade_dates), max(trade_dates)
        rows = self._conn.execute(
            """
            SELECT effective_at, removed_at, security_id
            FROM benchmark_membership_pit
            WHERE benchmark_id = ?
              AND effective_at <= ?
              AND (removed_at IS NULL OR removed_at > ?)
            """,
            [self._benchmark_id, max_d, min_d],
        ).fetchall()

        result: dict[str, set[str]] = {d: set() for d in trade_dates}
        for effective_at, removed_at, security_id in rows:
            for d in trade_dates:
                if effective_at <= d and (removed_at is None or removed_at > d):
                    result[d].add(security_id)
        self._cache.update(result)
        return result


class InvestableCoreUniverseResolver(UniverseResolver):
    """
    Universe = A-share stocks passing mandate filters:
      - not ST
      - listed >= min_listing_days
      - median 60-day turnover >= min_median_daily_turnover_cny_mn * 1e6

    Reads filter parameters from a Mandate-shaped dict (or the Mandate dataclass).
    """

    def __init__(
        self,
        conn: Any,
        *,
        min_listing_days: int = 120,
        min_median_turnover_cny: float = 30e6,
        exclude_st: bool = True,
        exclude_suspended: bool = True,
    ) -> None:
        self._conn = conn
        self._min_listing_days = min_listing_days
        self._min_turnover = min_median_turnover_cny
        self._exclude_st = exclude_st
        self._exclude_suspended = exclude_suspended
        self._cache: dict[str, set[str]] = {}

    @classmethod
    def from_mandate(cls, conn: Any, mandate: Any) -> "InvestableCoreUniverseResolver":
        """Build from a Mandate dataclass or TOML-loaded dict."""
        filters = getattr(mandate, "filters", {})
        if not isinstance(filters, dict):
            filters = filters.__dict__ if hasattr(filters, "__dict__") else {}
        return cls(
            conn,
            min_listing_days=int(filters.get("min_listing_days", 120)),
            min_median_turnover_cny=float(
                filters.get("min_median_daily_turnover_cny_mn", 30)
            ) * 1e6,
            exclude_st=bool(filters.get("exclude_st", True)),
            exclude_suspended=bool(filters.get("exclude_suspended", True)),
        )

    def resolve(self, trade_date: str) -> set[str]:
        if trade_date in self._cache:
            return self._cache[trade_date]

        # Build SQL filter predicates
        st_filter = "AND COALESCE(st.is_st_flag, 0) = 0" if self._exclude_st else ""

        rows = self._conn.execute(
            f"""
            WITH listing_days AS (
                SELECT
                    s.security_id,
                    COUNT(*) OVER (PARTITION BY s.security_id) AS total_days,
                    ROW_NUMBER() OVER (PARTITION BY s.security_id ORDER BY s.trade_date) AS day_num
                FROM daily_bar_pit s
                WHERE s.trade_date <= ?
            ),
            earliest_listing AS (
                SELECT security_id,
                       SUM(CASE WHEN trade_date <= ? THEN 1 ELSE 0 END) AS listed_days
                FROM daily_bar_pit
                GROUP BY security_id
            ),
            turnover_60d AS (
                SELECT
                    security_id,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY turnover_value_cny)
                        AS median_turnover
                FROM (
                    SELECT security_id, trade_date, turnover_value_cny
                    FROM daily_bar_pit
                    WHERE trade_date <= ?
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY security_id ORDER BY trade_date DESC
                    ) <= 60
                )
                GROUP BY security_id
            ),
            is_st AS (
                SELECT security_id,
                       MAX(CASE WHEN is_st THEN 1 ELSE 0 END) AS is_st_flag
                FROM daily_bar_pit
                WHERE trade_date = ?
                GROUP BY security_id
            )
            SELECT b.security_id
            FROM daily_bar_pit b
            JOIN earliest_listing el ON el.security_id = b.security_id
            JOIN turnover_60d t60 ON t60.security_id = b.security_id
            LEFT JOIN is_st st ON st.security_id = b.security_id
            WHERE b.trade_date = ?
              AND el.listed_days >= ?
              AND t60.median_turnover >= ?
              {st_filter}
            """,
            [
                trade_date, trade_date,    # listing_days / earliest_listing
                trade_date,               # turnover_60d LAG window
                trade_date,               # is_st
                trade_date,               # main WHERE
                self._min_listing_days,
                self._min_turnover,
            ],
        ).fetchall()
        result = {r[0] for r in rows}
        self._cache[trade_date] = result
        return result


def resolver_for_universe(
    universe_id: str,
    conn: Any,
    mandate: Any | None = None,
) -> UniverseResolver:
    """
    Factory: return the appropriate UniverseResolver for a universe_id string.

    Supported ids:
      "csi800"                   → BenchmarkUniverseResolver("CSI 800")
      "investable_a_share_core"  → InvestableCoreUniverseResolver (from mandate)
    """
    if universe_id == "csi800":
        return BenchmarkUniverseResolver(conn, benchmark_id="CSI 800")
    if universe_id == "investable_a_share_core":
        if mandate is not None:
            return InvestableCoreUniverseResolver.from_mandate(conn, mandate)
        return InvestableCoreUniverseResolver(conn)
    raise ValueError(
        f"Unknown universe_id '{universe_id}'. "
        f"Supported: 'csi800', 'investable_a_share_core'."
    )
