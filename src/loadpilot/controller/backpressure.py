"""
LoadPilot — Backpressure and load shedding.

Provides signals and policies for managing overload conditions.
When the system is under pressure, the backpressure controller decides:
1. Which jobs to defer (delay acceptance)
2. Which jobs to shed (reject outright)
3. When to propagate pressure upstream
"""

from __future__ import annotations

import time
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

from loadpilot.workload.models import AdmissionDecision, Job, Priority


class PressureLevel(Enum):
    """Discrete pressure levels for easier reasoning."""

    NOMINAL = "nominal"       # < 50% — system is healthy
    ELEVATED = "elevated"     # 50-70% — worth monitoring
    HIGH = "high"             # 70-85% — start deferring low-priority
    CRITICAL = "critical"     # 85-95% — defer aggressively, shed low-priority
    OVERLOAD = "overload"     # > 95% — shed everything non-critical


def classify_pressure(pressure: float) -> PressureLevel:
    """Map a 0.0–1.0 pressure scalar to a discrete level."""
    if pressure < 0.50:
        return PressureLevel.NOMINAL
    if pressure < 0.70:
        return PressureLevel.ELEVATED
    if pressure < 0.85:
        return PressureLevel.HIGH
    if pressure < 0.95:
        return PressureLevel.CRITICAL
    return PressureLevel.OVERLOAD


@dataclass(slots=True)
class BackpressureConfig:
    """Configuration for the backpressure controller."""

    # Pressure thresholds
    defer_threshold: float = 0.70
    shed_low_threshold: float = 0.85
    shed_normal_threshold: float = 0.95
    shed_all_threshold: float = 0.99   # only CRITICAL survives

    # Queue-depth backpressure
    queue_soft_limit: int = 5_000
    queue_hard_limit: int = 10_000

    # Rate-based backpressure (per second)
    max_accept_rate: float | None = None   # None = unlimited

    # Callbacks
    on_pressure_change: Callable[[PressureLevel, PressureLevel], None] | None = None


@dataclass(slots=True)
class BackpressureState:
    """Snapshot of backpressure state."""

    pressure: float
    level: PressureLevel
    queue_depth: int
    accept_rate: float           # jobs/second over last window
    defer_count: int
    shed_count: int
    timestamp: float = field(default_factory=time.monotonic)


class BackpressureController:
    """
    Manages backpressure and load shedding decisions.

    The controller evaluates incoming jobs against the current system
    pressure and makes admit/defer/reject decisions based on:

    1. **System pressure** (from the adaptive concurrency controller)
    2. **Queue depth** (absolute backlog size)
    3. **Accept rate** (admission throughput over a sliding window)
    4. **Job priority** (higher priority jobs survive longer)

    Load shedding priority order (first to be shed):
      LOW → NORMAL → HIGH → CRITICAL (never shed unless system is dying)
    """

    def __init__(self, config: BackpressureConfig | None = None) -> None:
        self._config = config or BackpressureConfig()
        thresholds = (
            self._config.defer_threshold,
            self._config.shed_low_threshold,
            self._config.shed_normal_threshold,
            self._config.shed_all_threshold,
        )
        if any(not math.isfinite(value) or not 0.0 <= value <= 1.0 for value in thresholds):
            raise ValueError("pressure thresholds must be finite values in [0, 1]")
        if list(thresholds) != sorted(thresholds):
            raise ValueError("pressure thresholds must be in ascending order")
        if self._config.queue_soft_limit < 0 or self._config.queue_hard_limit < 1:
            raise ValueError("queue limits must be non-negative with a positive hard limit")
        if (
            self._config.max_accept_rate is not None
            and (
                not math.isfinite(self._config.max_accept_rate)
                or self._config.max_accept_rate < 0
            )
        ):
            raise ValueError("max_accept_rate must be finite and non-negative")
        self._pressure: float = 0.0
        self._level: PressureLevel = PressureLevel.NOMINAL
        self._queue_depth: int = 0

        # Rate tracking
        self._accept_timestamps: list[float] = []
        self._rate_window: float = 5.0  # seconds

        # Counters
        self._total_deferred: int = 0
        self._total_shed: int = 0

    # ---- public interface ----

    @property
    def pressure(self) -> float:
        return self._pressure

    @property
    def level(self) -> PressureLevel:
        return self._level

    def update(
        self,
        system_pressure: float,
        queue_depth: int,
    ) -> None:
        """
        Update the controller with the latest system state.

        Combines system pressure (from adaptive controller) with
        queue-depth pressure into a single composite signal.
        """
        old_level = self._level
        if not math.isfinite(system_pressure):
            raise ValueError("system_pressure must be finite")
        if queue_depth < 0:
            raise ValueError("queue_depth must be non-negative")

        # Queue-depth pressure
        if self._config.queue_hard_limit > 0:
            q_pressure = min(queue_depth / self._config.queue_hard_limit, 1.0)
        else:
            q_pressure = 0.0

        # Composite: take the maximum of system and queue pressure
        self._pressure = max(system_pressure, q_pressure)
        self._level = classify_pressure(self._pressure)
        self._queue_depth = queue_depth

        # Notify on level change
        if old_level != self._level and self._config.on_pressure_change:
            self._config.on_pressure_change(old_level, self._level)

    def evaluate(self, job: Job) -> AdmissionDecision:
        """
        Decide whether to accept, defer, or reject a job given current
        pressure.
        """
        cfg = self._config
        p = self._pressure
        prio = job.priority if isinstance(job.priority, Priority) else Priority(job.priority)

        # Hard queue limit
        if self._queue_depth >= cfg.queue_hard_limit:
            self._total_shed += 1
            job.trace(
                f"SHED: queue depth {self._queue_depth} >= hard limit {cfg.queue_hard_limit}"
            )
            return AdmissionDecision.REJECT

        # Rate limiting
        if cfg.max_accept_rate is not None:
            rate = self._current_rate()
            if rate >= cfg.max_accept_rate:
                self._total_deferred += 1
                job.trace(f"DEFER: accept rate {rate:.1f}/s >= limit {cfg.max_accept_rate:.1f}/s")
                return AdmissionDecision.DEFER

        # Priority-aware shedding based on pressure level
        if p >= cfg.shed_all_threshold:
            # Only CRITICAL jobs survive
            if prio < Priority.CRITICAL:
                self._total_shed += 1
                job.trace(f"SHED: pressure {p:.2f} >= {cfg.shed_all_threshold}, "
                          f"priority {prio.name} < CRITICAL")
                return AdmissionDecision.REJECT

        elif p >= cfg.shed_normal_threshold:
            # Shed LOW and NORMAL
            if prio <= Priority.NORMAL:
                self._total_shed += 1
                job.trace(f"SHED: pressure {p:.2f} >= {cfg.shed_normal_threshold}, "
                          f"priority {prio.name} <= NORMAL")
                return AdmissionDecision.REJECT
            elif prio == Priority.HIGH:
                self._total_deferred += 1
                job.trace(f"DEFER: pressure {p:.2f} >= {cfg.shed_normal_threshold}, "
                          f"priority HIGH")
                return AdmissionDecision.DEFER

        elif p >= cfg.shed_low_threshold:
            # Shed LOW, defer NORMAL
            if prio == Priority.LOW:
                self._total_shed += 1
                job.trace(f"SHED: pressure {p:.2f} >= {cfg.shed_low_threshold}, priority LOW")
                return AdmissionDecision.REJECT
            elif prio == Priority.NORMAL:
                self._total_deferred += 1
                job.trace(f"DEFER: pressure {p:.2f} >= {cfg.shed_low_threshold}, priority NORMAL")
                return AdmissionDecision.DEFER

        elif p >= cfg.defer_threshold:
            # Defer LOW priority only
            if prio == Priority.LOW:
                self._total_deferred += 1
                job.trace(f"DEFER: pressure {p:.2f} >= {cfg.defer_threshold}, priority LOW")
                return AdmissionDecision.DEFER

        # Soft queue limit — defer low priority
        if self._queue_depth >= cfg.queue_soft_limit and prio <= Priority.LOW:
            self._total_deferred += 1
            job.trace(f"DEFER: queue depth {self._queue_depth} >= soft limit {cfg.queue_soft_limit}")
            return AdmissionDecision.DEFER

        # Accept
        self._record_accept()
        return AdmissionDecision.ACCEPT

    def state(self) -> BackpressureState:
        """Return a snapshot of the current state."""
        return BackpressureState(
            pressure=self._pressure,
            level=self._level,
            queue_depth=self._queue_depth,
            accept_rate=self._current_rate(),
            defer_count=self._total_deferred,
            shed_count=self._total_shed,
        )

    def reset(self) -> None:
        """Reset all state."""
        self._pressure = 0.0
        self._level = PressureLevel.NOMINAL
        self._queue_depth = 0
        self._accept_timestamps.clear()
        self._total_deferred = 0
        self._total_shed = 0

    # ---- internals ----

    def _record_accept(self) -> None:
        now = time.monotonic()
        self._accept_timestamps.append(now)
        # Prune old timestamps
        cutoff = now - self._rate_window
        while self._accept_timestamps and self._accept_timestamps[0] < cutoff:
            self._accept_timestamps.pop(0)

    def _current_rate(self) -> float:
        """Compute the acceptance rate (jobs/second) over the rate window."""
        now = time.monotonic()
        cutoff = now - self._rate_window
        while self._accept_timestamps and self._accept_timestamps[0] < cutoff:
            self._accept_timestamps.pop(0)
        if not self._accept_timestamps:
            return 0.0
        window = now - self._accept_timestamps[0]
        if window <= 0:
            return float(len(self._accept_timestamps))
        return len(self._accept_timestamps) / window
