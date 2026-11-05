"""
LoadPilot — Adaptive concurrency controller.

Observes execution latency and queue pressure to dynamically adjust
the concurrency limit.  Inspired by Envoy's adaptive concurrency filter
but implements a gradient-based controller with damping to prevent
oscillations.
"""

from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum


class ConcurrencySignal(Enum):
    """Direction the controller wants to move."""

    INCREASE = "increase"
    DECREASE = "decrease"
    HOLD = "hold"


@dataclass(slots=True)
class ControllerSnapshot:
    """Point-in-time snapshot of controller state for telemetry."""

    timestamp: float
    concurrency_limit: int
    measured_latency_ms: float
    latency_target_ms: float
    latency_gradient: float
    queue_pressure: float
    signal: ConcurrencySignal
    system_pressure: float


class LatencyWindow:
    """
    Sliding window of latency measurements.

    Maintains a fixed-size deque of (timestamp, latency_ms) pairs and
    computes rolling statistics.
    """

    __slots__ = ("_window", "_max_size")

    def __init__(self, max_size: int = 50) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        self._window: deque[tuple[float, float]] = deque(maxlen=max_size)
        self._max_size = max_size

    def record(self, latency_ms: float, timestamp: float | None = None) -> None:
        if not math.isfinite(latency_ms) or latency_ms < 0:
            raise ValueError("latency_ms must be finite and non-negative")
        ts = time.monotonic() if timestamp is None else timestamp
        self._window.append((ts, latency_ms))

    @property
    def count(self) -> int:
        return len(self._window)

    @property
    def is_full(self) -> bool:
        return len(self._window) >= self._max_size

    def mean(self) -> float:
        if not self._window:
            return 0.0
        return sum(lat for _, lat in self._window) / len(self._window)

    def percentile(self, p: float) -> float:
        """Return the *p*-th percentile (p in 0..100)."""
        if not 0.0 <= p <= 100.0:
            raise ValueError(f"percentile must be in [0, 100], got {p}")
        if not self._window:
            return 0.0
        values = sorted(lat for _, lat in self._window)
        idx = (p / 100.0) * (len(values) - 1)
        lo = int(math.floor(idx))
        hi = int(math.ceil(idx))
        if lo == hi:
            return values[lo]
        frac = idx - lo
        return values[lo] * (1 - frac) + values[hi] * frac

    @property
    def p50(self) -> float:
        return self.percentile(50)

    @property
    def p95(self) -> float:
        return self.percentile(95)

    @property
    def p99(self) -> float:
        return self.percentile(99)

    def gradient(self) -> float:
        """
        Compute the latency gradient: ratio of recent-half mean to older-half mean.

        - gradient > 1.0 → latency is increasing
        - gradient < 1.0 → latency is decreasing
        - gradient ≈ 1.0 → latency is stable
        """
        n = len(self._window)
        if n < 4:
            return 1.0
        mid = n // 2
        items = list(self._window)
        old_half = [lat for _, lat in items[:mid]]
        new_half = [lat for _, lat in items[mid:]]
        old_mean = sum(old_half) / len(old_half)
        new_mean = sum(new_half) / len(new_half)
        if old_mean == 0:
            return 1.0
        return new_mean / old_mean

    def clear(self) -> None:
        self._window.clear()


class AdaptiveConcurrencyController:
    """
    Gradient-based adaptive concurrency controller.

    The controller periodically examines the latency gradient and queue
    pressure to decide whether to increase, decrease, or hold the
    concurrency limit.

    Design principles:
    - **Increase cautiously**: add ``step`` slots when latency is below
      target *and* the gradient is stable/decreasing.
    - **Decrease aggressively**: reduce by ``step * multiplier`` when
      latency is above target *and* gradient is rising.
    - **Damping**: after a decrease, the controller enters a cooldown
      period to let the system stabilise before making another change.
    - **Gradient threshold**: small fluctuations (gradient within ±5%)
      are treated as stable.
    """

    def __init__(
        self,
        initial_concurrency: int = 50,
        min_concurrency: int = 5,
        max_concurrency: int = 200,
        latency_target_ms: float = 500.0,
        window_size: int = 50,
        step: int = 5,
        decrease_multiplier: float = 2.0,
        cooldown_seconds: float = 2.0,
        gradient_threshold: float = 0.05,
    ) -> None:
        if min_concurrency < 1:
            raise ValueError("min_concurrency must be >= 1")
        if max_concurrency < min_concurrency:
            raise ValueError("max_concurrency must be >= min_concurrency")
        if not min_concurrency <= initial_concurrency <= max_concurrency:
            raise ValueError("initial_concurrency must be within configured bounds")
        if latency_target_ms <= 0 or not math.isfinite(latency_target_ms):
            raise ValueError("latency_target_ms must be finite and positive")
        if window_size < 1:
            raise ValueError("window_size must be >= 1")
        if step < 1:
            raise ValueError("step must be >= 1")
        if decrease_multiplier <= 0 or not math.isfinite(decrease_multiplier):
            raise ValueError("decrease_multiplier must be finite and positive")
        if cooldown_seconds < 0 or not math.isfinite(cooldown_seconds):
            raise ValueError("cooldown_seconds must be finite and non-negative")
        if gradient_threshold < 0 or not math.isfinite(gradient_threshold):
            raise ValueError("gradient_threshold must be finite and non-negative")
        self._limit = initial_concurrency
        self._min = min_concurrency
        self._max = max_concurrency
        self._target_ms = latency_target_ms
        self._step = step
        self._decrease_mult = decrease_multiplier
        self._cooldown = cooldown_seconds
        self._gradient_threshold = gradient_threshold

        self._latency_window = LatencyWindow(window_size)
        self._last_adjustment: float = 0.0
        self._history: deque[ControllerSnapshot] = deque(maxlen=200)

        # External pressure inputs (set by the engine)
        self._queue_pressure: float = 0.0   # 0.0 – 1.0

    # ---- public interface ----

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def system_pressure(self) -> float:
        """
        Composite pressure signal combining latency pressure and queue
        pressure.  Range 0.0 – 1.0.
        """
        latency_p = min(self._latency_window.p95 / max(self._target_ms, 1), 1.0)
        return max(latency_p, self._queue_pressure)

    def record_latency(self, latency_ms: float) -> None:
        """Record a job execution latency."""
        self._latency_window.record(latency_ms)

    def set_queue_pressure(self, pressure: float) -> None:
        """Update external queue-depth pressure signal (0.0 – 1.0)."""
        if not math.isfinite(pressure):
            raise ValueError("pressure must be finite")
        self._queue_pressure = max(0.0, min(1.0, pressure))

    def adjust(self) -> ConcurrencySignal:
        """
        Evaluate the current state and adjust the concurrency limit.

        Returns the direction the limit moved.
        """
        now = time.monotonic()

        # Respect cooldown after a decrease
        if now - self._last_adjustment < self._cooldown:
            signal = ConcurrencySignal.HOLD
            self._record_snapshot(signal)
            return signal

        if self._latency_window.count < 4:
            signal = ConcurrencySignal.HOLD
            self._record_snapshot(signal)
            return signal

        gradient = self._latency_window.gradient()
        current_p95 = self._latency_window.p95
        above_target = current_p95 > self._target_ms
        gradient_rising = gradient > (1.0 + self._gradient_threshold)
        gradient_falling = gradient < (1.0 - self._gradient_threshold)

        signal: ConcurrencySignal

        if above_target and gradient_rising:
            # Latency is high and getting worse → decrease aggressively
            reduction = int(self._step * self._decrease_mult)
            self._limit = max(self._min, self._limit - reduction)
            signal = ConcurrencySignal.DECREASE
            self._last_adjustment = now

        elif above_target and not gradient_rising:
            # Latency is high but stable/improving → gentle decrease
            self._limit = max(self._min, self._limit - self._step)
            signal = ConcurrencySignal.DECREASE
            self._last_adjustment = now

        elif not above_target and gradient_falling:
            # Latency is good and improving → cautious increase
            self._limit = min(self._max, self._limit + self._step)
            signal = ConcurrencySignal.INCREASE
            self._last_adjustment = now

        elif not above_target and not gradient_rising:
            # Latency is good and stable → try a small increase
            if self._queue_pressure > 0.3:
                # There's queued work — worth adding capacity
                self._limit = min(self._max, self._limit + self._step)
                signal = ConcurrencySignal.INCREASE
                self._last_adjustment = now
            else:
                signal = ConcurrencySignal.HOLD
        else:
            signal = ConcurrencySignal.HOLD

        self._record_snapshot(signal)
        return signal

    def reset(self) -> None:
        """Reset the controller to its initial state."""
        self._latency_window.clear()
        self._last_adjustment = 0.0
        self._queue_pressure = 0.0
        self._history.clear()

    @property
    def history(self) -> list[ControllerSnapshot]:
        return list(self._history)

    # ---- internals ----

    def _record_snapshot(self, signal: ConcurrencySignal) -> None:
        snap = ControllerSnapshot(
            timestamp=time.monotonic(),
            concurrency_limit=self._limit,
            measured_latency_ms=self._latency_window.p95 if self._latency_window.count else 0,
            latency_target_ms=self._target_ms,
            latency_gradient=self._latency_window.gradient(),
            queue_pressure=self._queue_pressure,
            signal=signal,
            system_pressure=self.system_pressure,
        )
        self._history.append(snap)
