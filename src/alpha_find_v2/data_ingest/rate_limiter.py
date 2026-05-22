"""
Token-bucket rate limiter for data source adapters.

Thread-safe, injectable clock and sleep for unit testing.
Supports per-minute rate and optional daily cap.
"""
from __future__ import annotations

import threading
import time
from typing import Callable


class RateLimitTimeout(Exception):
    """Raised when acquire() cannot obtain a token within the given timeout."""


class DailyCapExhausted(RateLimitTimeout):
    """Raised when the daily cap has been exhausted."""


class TokenBucket:
    """
    Token bucket with per-minute refill and optional daily cap.

    Token bucket refills at rate_per_minute / 60 tokens per second using a
    last-refill-timestamp approach: on acquire(), compute elapsed seconds since
    last check, add tokens = elapsed * (rate_per_minute/60), cap at
    rate_per_minute, then consume one token.

    Args:
        rate_per_minute: Maximum calls per minute. Must be > 0.
        daily_cap: Maximum calls per UTC calendar day. 0 = unlimited.
        _clock: Callable returning monotonic time in seconds. Injectable for
                tests (avoids real sleeping). Defaults to time.monotonic.
        _sleep: Callable to sleep for a given number of seconds. Injectable for
                tests. Defaults to time.sleep.
    """

    def __init__(
        self,
        rate_per_minute: int,
        daily_cap: int = 0,
        *,
        _clock: Callable[[], float] | None = None,
        _sleep: Callable[[float], None] | None = None,
        # Alias without underscore prefix (also accepted for compatibility)
        clock: Callable[[], float] | None = None,
    ) -> None:
        if rate_per_minute <= 0:
            raise ValueError("rate_per_minute must be > 0")
        if daily_cap < 0:
            raise ValueError("daily_cap must be >= 0")

        self._rate_per_minute = rate_per_minute
        self._daily_cap = daily_cap
        # Accept either _clock or clock; _clock takes precedence
        resolved_clock = _clock if _clock is not None else clock
        self._clock: Callable[[], float] = resolved_clock if resolved_clock is not None else time.monotonic
        self._sleep: Callable[[float], None] = _sleep if _sleep is not None else time.sleep

        # Token bucket state — starts full (pre-filled)
        self._tokens: float = float(rate_per_minute)
        self._max_tokens: float = float(rate_per_minute)
        self._refill_rate: float = rate_per_minute / 60.0  # tokens per second
        self._last_refill: float = self._clock()

        # Daily counter state
        self._daily_count: int = 0
        self._today_str: str = self._current_day_str()

        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self, timeout: float | None = None) -> None:
        """
        Block until a rate-limit token is available, then consume it.

        Records the call automatically (increments daily counter).

        Raises:
            DailyCapExhausted: if daily_cap > 0 and the daily cap is exhausted.
            RateLimitTimeout: if timeout (seconds) expires before a token is
                              available.
        """
        deadline = None if timeout is None else self._clock() + timeout

        while True:
            with self._lock:
                self._refill()
                self._roll_day()

                if self._daily_cap > 0 and self._daily_count >= self._daily_cap:
                    raise DailyCapExhausted(
                        f"Daily cap of {self._daily_cap} calls exhausted for today."
                    )

                # Check deadline before trying token (so timeout=0.0 raises
                # immediately when no token is available)
                if deadline is not None and self._clock() >= deadline:
                    if self._tokens < 1.0:
                        raise RateLimitTimeout(
                            f"Could not acquire a rate-limit token within {timeout}s."
                        )

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    self._daily_count += 1
                    return

                # Compute how long until we accumulate one token
                wait_seconds = (1.0 - self._tokens) / self._refill_rate

            # Outside lock: check timeout then sleep
            if deadline is not None:
                remaining = deadline - self._clock()
                if remaining <= 0:
                    raise RateLimitTimeout(
                        f"Could not acquire a rate-limit token within {timeout}s."
                    )
                wait_seconds = min(wait_seconds, remaining)

            if wait_seconds > 0:
                self._sleep(wait_seconds)

    def record_call(self) -> None:
        """Manually increment the daily counter by one.

        Called automatically by acquire(). Can also be called for calls
        made outside the token-bucket flow.
        """
        with self._lock:
            self._roll_day()
            self._daily_count += 1

    def daily_exhausted(self) -> bool:
        """Return True if daily_cap > 0 and the daily cap is exhausted."""
        with self._lock:
            self._roll_day()
            return self._daily_cap > 0 and self._daily_count >= self._daily_cap

    @property
    def rate_per_minute(self) -> int:
        return self._rate_per_minute

    @property
    def daily_cap(self) -> int:
        return self._daily_cap

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _refill(self) -> None:
        """Top up tokens based on elapsed time since last refill. Must hold lock."""
        now = self._clock()
        elapsed = now - self._last_refill
        self._tokens = min(self._max_tokens, self._tokens + elapsed * self._refill_rate)
        self._last_refill = now

    def _roll_day(self) -> None:
        """Reset daily counter if the UTC calendar day has changed. Must hold lock."""
        today = self._current_day_str()
        if today != self._today_str:
            self._today_str = today
            self._daily_count = 0

    @staticmethod
    def _current_day_str() -> str:
        """Current UTC date as YYYYMMDD string."""
        t = time.gmtime()
        return f"{t.tm_year:04d}{t.tm_mon:02d}{t.tm_mday:02d}"
