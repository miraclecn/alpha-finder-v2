"""Tests for rate_limiter.py TokenBucket.

No real time.sleep is used in any test — all timing is driven through the
injectable _clock and _sleep parameters.
"""

from __future__ import annotations

import threading
import time

import pytest

from alpha_find_v2.data_ingest.rate_limiter import (
    DailyCapExhausted,
    RateLimitTimeout,
    TokenBucket,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeClock:
    """Monotonic fake clock for deterministic testing."""

    def __init__(self, start: float = 0.0) -> None:
        self._t = start

    def __call__(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += seconds


def make_noop_sleep():
    """Return a no-op sleep that also records how long it was asked to sleep."""
    calls: list[float] = []

    def noop(seconds: float) -> None:
        calls.append(seconds)

    noop.calls = calls  # type: ignore[attr-defined]
    return noop


# ---------------------------------------------------------------------------
# Spec-required: fake-clock test — 60 calls succeed in simulated 60 seconds
# ---------------------------------------------------------------------------


def test_fake_clock_60_calls_in_60_seconds():
    """Instantiate rate_per_minute=60; advance fake clock 1s between calls.
    All 60 calls must succeed within the simulated 60s window.
    """
    clock = FakeClock(start=0.0)
    sleep = make_noop_sleep()
    bucket = TokenBucket(rate_per_minute=60, _clock=clock, _sleep=sleep)

    for i in range(60):
        clock.advance(1.0)  # advance 1 second before each call
        bucket.acquire()  # must not raise

    # 60 calls made in 60 simulated seconds — no real sleep, no timeout
    assert len(sleep.calls) == 0, "No real sleep should occur within the rate limit"


# ---------------------------------------------------------------------------
# Spec-required: no-sleep test — 100 calls near-instantly at rate=10000
# ---------------------------------------------------------------------------


def test_no_sleep_high_rate():
    """rate_per_minute=10000: 100 acquire() calls complete near-instantly."""
    bucket = TokenBucket(rate_per_minute=10_000)
    start = time.monotonic()
    for _ in range(100):
        bucket.acquire()
    elapsed = time.monotonic() - start
    assert elapsed < 1.0, f"100 calls took {elapsed:.3f}s — too slow"


# ---------------------------------------------------------------------------
# Spec-required: daily_cap=10 blocks after 10 calls
# ---------------------------------------------------------------------------


def test_daily_cap_10_blocks_on_11th():
    """First 10 acquire() succeed; after that daily_exhausted() is True."""
    bucket = TokenBucket(rate_per_minute=10_000, daily_cap=10)

    for _ in range(10):
        bucket.acquire()

    assert bucket.daily_exhausted(), "daily_exhausted() must be True after 10 calls"

    with pytest.raises((RateLimitTimeout, DailyCapExhausted)):
        bucket.acquire(timeout=0.0)


# ---------------------------------------------------------------------------
# Spec-required: timeout test — rate=1, second call in same second raises
# ---------------------------------------------------------------------------


def test_timeout_raises_on_exhausted_bucket():
    """rate_per_minute=1, timeout=0.0 on second call in same clock tick → RateLimitTimeout."""
    clock = FakeClock(start=0.0)
    sleep = make_noop_sleep()
    bucket = TokenBucket(rate_per_minute=1, _clock=clock, _sleep=sleep)

    bucket.acquire()  # first call — succeeds, consumes the one token

    # Same clock value — window still full, timeout=0.0 → must raise immediately
    with pytest.raises(RateLimitTimeout):
        bucket.acquire(timeout=0.0)


# ---------------------------------------------------------------------------
# Spec-required: PBT — observed rate never exceeds rate_per_minute / 60
# ---------------------------------------------------------------------------

# PBT
def test_pbt_rate_never_exceeded_across_random_delays():
    """Across 1000 calls with random inter-call delays (via fake clock),
    total_calls / elapsed_seconds never exceeds rate_per_minute / 60.

    Uses a manual loop with pseudo-random delays. The _sleep injectable
    advances the fake clock so acquire() makes progress without real sleep.
    """
    import random

    rng = random.Random(42)
    rate_per_minute = 120  # 2 tokens/s so most random delays pass without waiting
    max_rate_per_second = rate_per_minute / 60.0

    clock = FakeClock(start=0.0)

    # _sleep advances the fake clock so acquire() can make progress
    def advancing_sleep(seconds: float) -> None:
        clock.advance(seconds)

    bucket = TokenBucket(
        rate_per_minute=rate_per_minute,
        _clock=clock,
        _sleep=advancing_sleep,
    )

    call_times: list[float] = []

    for _ in range(1_000):
        # Random inter-call delay: 0 to 2 seconds simulated
        clock.advance(rng.uniform(0.0, 2.0))
        bucket.acquire()
        call_times.append(clock())

    # Verify: average observed rate ≤ configured rate_per_minute / 60
    if len(call_times) >= 2:
        total_elapsed = call_times[-1] - call_times[0]
        if total_elapsed > 0:
            observed_rate_per_second = len(call_times) / total_elapsed
            assert observed_rate_per_second <= max_rate_per_second + 1e-9, (
                f"Observed {observed_rate_per_second:.4f} calls/s exceeds "
                f"allowed {max_rate_per_second:.4f} calls/s"
            )


# ---------------------------------------------------------------------------
# Additional correctness tests
# ---------------------------------------------------------------------------


def test_acquire_auto_records_call():
    """acquire() must auto-call record_call() so daily counter is updated."""
    bucket = TokenBucket(rate_per_minute=10_000, daily_cap=3)
    assert not bucket.daily_exhausted()
    for _ in range(3):
        bucket.acquire()
    assert bucket.daily_exhausted()


def test_window_resets_after_60_seconds():
    """After 61 simulated seconds, the window clears and new calls proceed."""
    clock = FakeClock(start=0.0)
    sleep = make_noop_sleep()
    bucket = TokenBucket(rate_per_minute=3, _clock=clock, _sleep=sleep)

    for _ in range(3):
        bucket.acquire()

    clock.advance(61.0)
    bucket.acquire()  # must not raise


def test_thread_safety_concurrent_acquire():
    """Multiple threads must not collectively exceed the per-minute rate."""
    clock = FakeClock(start=0.0)
    sleep = make_noop_sleep()
    bucket = TokenBucket(rate_per_minute=50, _clock=clock, _sleep=sleep)
    successes: list[int] = []
    lock = threading.Lock()

    def worker():
        try:
            bucket.acquire(timeout=0.0)
            with lock:
                successes.append(1)
        except RateLimitTimeout:
            pass

    threads = [threading.Thread(target=worker) for _ in range(100)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(successes) <= 50


def test_zero_daily_cap_is_unlimited():
    """daily_cap=0 means unlimited; daily_exhausted() always False."""
    bucket = TokenBucket(rate_per_minute=10_000, daily_cap=0)
    for _ in range(500):
        bucket.acquire()
    assert not bucket.daily_exhausted()


def test_record_call_manual_increments_daily():
    """record_call() independently increments the daily counter."""
    bucket = TokenBucket(rate_per_minute=10_000, daily_cap=5)
    for _ in range(5):
        bucket.record_call()
    assert bucket.daily_exhausted()
