"""
LoadPilot — Telemetry metrics primitives.

Pure-Python, thread-safe metric types: Counter, Gauge, Histogram,
LabeledMetric, and a MetricRegistry to manage them.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Any


class Counter:
    """Thread-safe monotonic counter.

    The value can only increase (or be reset to zero).  Calling
    :meth:`inc` with a negative amount raises :class:`ValueError`.
    """

    __slots__ = ("name", "description", "_value", "_lock")

    def __init__(self, name: str, description: str = "") -> None:
        self.name = name
        self.description = description
        self._value: float = 0.0
        self._lock = threading.Lock()

    def inc(self, amount: float = 1.0) -> None:
        """Increment the counter by *amount* (must be ≥ 0)."""
        if amount < 0:
            raise ValueError(
                f"Counter.inc amount must be non-negative, got {amount}"
            )
        with self._lock:
            self._value += amount

    @property
    def value(self) -> float:
        """Current counter value."""
        with self._lock:
            return self._value

    def reset(self) -> None:
        """Reset the counter to zero."""
        with self._lock:
            self._value = 0.0

    def __repr__(self) -> str:  # pragma: no cover
        return f"Counter(name={self.name!r}, value={self.value})"


class Gauge:
    """Thread-safe gauge whose value can go up and down."""

    __slots__ = ("name", "description", "_value", "_lock")

    def __init__(self, name: str, description: str = "") -> None:
        self.name = name
        self.description = description
        self._value: float = 0.0
        self._lock = threading.Lock()

    def set(self, value: float) -> None:
        """Set the gauge to an arbitrary *value*."""
        with self._lock:
            self._value = value

    def inc(self, amount: float = 1.0) -> None:
        """Increment the gauge by *amount*."""
        with self._lock:
            self._value += amount

    def dec(self, amount: float = 1.0) -> None:
        """Decrement the gauge by *amount*."""
        with self._lock:
            self._value -= amount

    @property
    def value(self) -> float:
        """Current gauge value."""
        with self._lock:
            return self._value

    def reset(self) -> None:
        """Reset the gauge to zero."""
        with self._lock:
            self._value = 0.0

    def __repr__(self) -> str:  # pragma: no cover
        return f"Gauge(name={self.name!r}, value={self.value})"


class Histogram:
    """Records observations in a fixed-size sliding window and computes percentiles.

    Parameters
    ----------
    name:
        Metric name.
    description:
        Human-readable description.
    window_size:
        Maximum number of recent observations to retain for percentile
        calculations.  Older samples are evicted in FIFO order.
    """

    __slots__ = ("name", "description", "_values", "_lock", "_count", "_sum")

    def __init__(
        self,
        name: str,
        description: str = "",
        window_size: int = 1000,
    ) -> None:
        if window_size < 1:
            raise ValueError(
                f"window_size must be >= 1, got {window_size}"
            )
        self.name = name
        self.description = description
        self._values: deque[float] = deque(maxlen=window_size)
        self._lock = threading.Lock()
        self._count: int = 0
        self._sum: float = 0.0

    def observe(self, value: float) -> None:
        """Record an observation."""
        with self._lock:
            self._values.append(value)
            self._count += 1
            self._sum += value

    @property
    def count(self) -> int:
        """Total number of observations (including evicted ones)."""
        with self._lock:
            return self._count

    @property
    def mean(self) -> float:
        """Mean of all observations recorded so far.

        Returns ``0.0`` if no observations have been recorded.
        """
        with self._lock:
            if self._count == 0:
                return 0.0
            return self._sum / self._count

    def percentile(self, p: float) -> float:
        """Compute the *p*-th percentile over the current window.

        Parameters
        ----------
        p:
            Percentile in the range ``[0, 100]``.

        Returns
        -------
        float
            The percentile value.  Returns ``0.0`` when the window is empty.

        Raises
        ------
        ValueError
            If *p* is outside ``[0, 100]``.
        """
        if not 0.0 <= p <= 100.0:
            raise ValueError(f"Percentile must be in [0, 100], got {p}")
        with self._lock:
            n = len(self._values)
            if n == 0:
                return 0.0
            sorted_values = sorted(self._values)
        # Use nearest-rank method
        if p == 0.0:
            return sorted_values[0]
        rank = (p / 100.0) * (n - 1)
        lower = int(rank)
        upper = lower + 1
        if upper >= n:
            return sorted_values[-1]
        # Linear interpolation between the two nearest ranks
        frac = rank - lower
        return sorted_values[lower] + frac * (sorted_values[upper] - sorted_values[lower])

    @property
    def p50(self) -> float:
        """Median (50th percentile) of the current window."""
        return self.percentile(50.0)

    @property
    def p95(self) -> float:
        """95th percentile of the current window."""
        return self.percentile(95.0)

    @property
    def p99(self) -> float:
        """99th percentile of the current window."""
        return self.percentile(99.0)

    def reset(self) -> None:
        """Clear all observations."""
        with self._lock:
            self._values.clear()
            self._count = 0
            self._sum = 0.0

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"Histogram(name={self.name!r}, count={self.count}, mean={self.mean:.4f})"
        )


class LabeledMetric:
    """A metric family keyed by arbitrary string labels.

    Calling :meth:`labels` with a set of keyword arguments returns (or
    lazily creates) a child metric instance for that specific label
    combination.

    Parameters
    ----------
    metric_cls:
        One of :class:`Counter`, :class:`Gauge`, or :class:`Histogram`.
    name:
        Metric family name.
    description:
        Human-readable description.
    **kwargs:
        Extra keyword arguments forwarded to ``metric_cls.__init__``
        (e.g. ``window_size`` for :class:`Histogram`).
    """

    __slots__ = ("_cls", "_name", "_description", "_kwargs", "_metrics", "_lock")

    def __init__(
        self,
        metric_cls: type,
        name: str,
        description: str = "",
        **kwargs: Any,
    ) -> None:
        self._cls = metric_cls
        self._name = name
        self._description = description
        self._kwargs = kwargs
        self._metrics: dict[str, Counter | Gauge | Histogram] = {}
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        """Metric family name."""
        return self._name

    @property
    def description(self) -> str:
        """Metric family description."""
        return self._description

    def labels(self, **label_values: str) -> Counter | Gauge | Histogram:
        """Return the child metric for the given label set.

        Creates it on first access.

        Parameters
        ----------
        **label_values:
            Label key-value pairs, e.g. ``tenant_id="acme"``.
        """
        key = str(sorted(label_values.items()))
        with self._lock:
            metric = self._metrics.get(key)
            if metric is None:
                # Build a child name like "tenant_jobs_total{tenant_id=acme}"
                label_str = ",".join(
                    f"{k}={v}" for k, v in sorted(label_values.items())
                )
                child_name = f"{self._name}{{{label_str}}}"
                metric = self._cls(
                    name=child_name,
                    description=self._description,
                    **self._kwargs,
                )
                self._metrics[key] = metric
            return metric

    def all_metrics(self) -> dict[str, Counter | Gauge | Histogram]:
        """Return a snapshot of all child metrics keyed by label string."""
        with self._lock:
            return dict(self._metrics)

    def reset(self) -> None:
        """Reset every child metric and remove them from the family."""
        with self._lock:
            for metric in self._metrics.values():
                metric.reset()
            self._metrics.clear()

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"LabeledMetric(name={self._name!r}, "
            f"children={len(self._metrics)})"
        )


class MetricRegistry:
    """Central registry for all named metrics.

    Provides factory methods for the four metric types and a
    :meth:`snapshot` helper that serialises every registered metric
    into a plain :class:`dict`.
    """

    __slots__ = ("_metrics", "_lock")

    def __init__(self) -> None:
        self._metrics: dict[str, Counter | Gauge | Histogram | LabeledMetric] = {}
        self._lock = threading.Lock()

    # ---- factory methods ---------------------------------------------------

    def counter(self, name: str, description: str = "") -> Counter:
        """Create and register a :class:`Counter`."""
        return self._register(Counter(name=name, description=description))

    def gauge(self, name: str, description: str = "") -> Gauge:
        """Create and register a :class:`Gauge`."""
        return self._register(Gauge(name=name, description=description))

    def histogram(
        self,
        name: str,
        description: str = "",
        window_size: int = 1000,
    ) -> Histogram:
        """Create and register a :class:`Histogram`."""
        return self._register(
            Histogram(name=name, description=description, window_size=window_size)
        )

    def labeled_counter(self, name: str, description: str = "") -> LabeledMetric:
        """Create and register a labeled :class:`Counter` family."""
        return self._register(
            LabeledMetric(Counter, name=name, description=description)
        )

    def labeled_gauge(self, name: str, description: str = "") -> LabeledMetric:
        """Create and register a labeled :class:`Gauge` family."""
        return self._register(
            LabeledMetric(Gauge, name=name, description=description)
        )

    def labeled_histogram(
        self,
        name: str,
        description: str = "",
        window_size: int = 1000,
    ) -> LabeledMetric:
        """Create and register a labeled :class:`Histogram` family."""
        return self._register(
            LabeledMetric(
                Histogram,
                name=name,
                description=description,
                window_size=window_size,
            )
        )

    # ---- retrieval ---------------------------------------------------------

    def get(self, name: str) -> Counter | Gauge | Histogram | LabeledMetric | None:
        """Look up a metric by *name*."""
        with self._lock:
            return self._metrics.get(name)

    # ---- snapshot ----------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Serialise every registered metric into a nested dict.

        Returns a mapping from metric name to a dict of relevant
        values.  This is useful for logging, telemetry export, or
        human inspection.
        """
        with self._lock:
            metrics = dict(self._metrics)

        result: dict[str, Any] = {}
        for name, metric in metrics.items():
            if isinstance(metric, Counter):
                result[name] = {"type": "counter", "value": metric.value}
            elif isinstance(metric, Gauge):
                result[name] = {"type": "gauge", "value": metric.value}
            elif isinstance(metric, Histogram):
                result[name] = {
                    "type": "histogram",
                    "count": metric.count,
                    "mean": metric.mean,
                    "p50": metric.p50,
                    "p95": metric.p95,
                    "p99": metric.p99,
                }
            elif isinstance(metric, LabeledMetric):
                children: dict[str, Any] = {}
                for key, child in metric.all_metrics().items():
                    if isinstance(child, Counter):
                        children[key] = {"type": "counter", "value": child.value}
                    elif isinstance(child, Gauge):
                        children[key] = {"type": "gauge", "value": child.value}
                    elif isinstance(child, Histogram):
                        children[key] = {
                            "type": "histogram",
                            "count": child.count,
                            "mean": child.mean,
                            "p50": child.p50,
                            "p95": child.p95,
                            "p99": child.p99,
                        }
                result[name] = {"type": "labeled", "children": children}
        return result

    # ---- reset -------------------------------------------------------------

    def reset(self) -> None:
        """Reset every registered metric to its initial state."""
        with self._lock:
            for metric in self._metrics.values():
                metric.reset()

    # ---- internals ---------------------------------------------------------

    def _register(self, metric: Any) -> Any:
        """Register a metric by name, raising on duplicates."""
        name = metric.name
        with self._lock:
            if name in self._metrics:
                raise KeyError(
                    f"Metric {name!r} is already registered in this registry"
                )
            self._metrics[name] = metric
        return metric

    def __repr__(self) -> str:  # pragma: no cover
        with self._lock:
            names = list(self._metrics.keys())
        return f"MetricRegistry(metrics={names})"
