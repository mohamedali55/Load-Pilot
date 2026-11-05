"""
LoadPilot — Pre-defined metrics collector for the scheduling engine.

Provides :class:`MetricsCollector`, a facade that groups all standard
LoadPilot metrics (throughput, latency, resources, fairness, etc.) and
exposes convenience methods for common recording patterns.
"""

from __future__ import annotations

import time
from typing import Any

from loadpilot.telemetry.metrics import (
    Counter,
    Gauge,
    Histogram,
    LabeledMetric,
    MetricRegistry,
)
from loadpilot.workload.models import Job, JobStatus


class MetricsCollector:
    """Pre-defined metrics for the LoadPilot engine.

    Instantiating this class creates a full :class:`MetricRegistry`
    pre-populated with the standard counters, gauges, histograms, and
    labeled metrics that the engine needs.

    Thread Safety
    -------------
    All underlying metrics are individually thread-safe.  The collector
    itself is safe for concurrent ``record_*`` calls from multiple
    threads.
    """

    __slots__ = (
        "registry",
        # throughput counters
        "jobs_submitted",
        "jobs_completed",
        "jobs_failed",
        "jobs_rejected",
        "jobs_deferred",
        "jobs_timed_out",
        "jobs_cancelled",
        # per-tenant counters
        "tenant_jobs_completed",
        "tenant_jobs_rejected",
        "tenant_cpu_consumed",
        # latency histograms
        "job_wait_time",
        "job_execution_time",
        "scheduling_latency",
        # resource gauges
        "worker_utilization",
        "cpu_utilization",
        "memory_utilization",
        "queue_depth",
        # adaptive gauges
        "current_concurrency",
        "system_pressure",
        # fairness
        "tenant_fairness_debt",
        # deadline
        "deadline_misses",
        "tenant_deadline_misses",
    )

    def __init__(self) -> None:
        self.registry = MetricRegistry()

        # ---- Throughput counters -------------------------------------------
        self.jobs_submitted: Counter = self.registry.counter(
            "jobs_submitted_total",
            "Total number of jobs submitted to the engine",
        )
        self.jobs_completed: Counter = self.registry.counter(
            "jobs_completed_total",
            "Total number of jobs that completed successfully",
        )
        self.jobs_failed: Counter = self.registry.counter(
            "jobs_failed_total",
            "Total number of jobs that failed",
        )
        self.jobs_rejected: Counter = self.registry.counter(
            "jobs_rejected_total",
            "Total number of jobs rejected by admission control",
        )
        self.jobs_deferred: Counter = self.registry.counter(
            "jobs_deferred_total",
            "Total number of jobs deferred due to backpressure",
        )
        self.jobs_timed_out: Counter = self.registry.counter(
            "jobs_timed_out_total",
            "Total number of jobs that exceeded their deadline or timeout",
        )
        self.jobs_cancelled: Counter = self.registry.counter(
            "jobs_cancelled_total",
            "Total number of jobs cancelled before completion",
        )

        # ---- Per-tenant counters -------------------------------------------
        self.tenant_jobs_completed: LabeledMetric = self.registry.labeled_counter(
            "tenant_jobs_completed_total",
            "Jobs completed per tenant",
        )
        self.tenant_jobs_rejected: LabeledMetric = self.registry.labeled_counter(
            "tenant_jobs_rejected_total",
            "Jobs rejected per tenant",
        )
        self.tenant_cpu_consumed: LabeledMetric = self.registry.labeled_counter(
            "tenant_cpu_consumed_total",
            "Cumulative CPU seconds consumed per tenant",
        )

        # ---- Latency histograms -------------------------------------------
        self.job_wait_time: Histogram = self.registry.histogram(
            "job_wait_time_seconds",
            "Time a job waits from submission to execution start",
            window_size=2000,
        )
        self.job_execution_time: Histogram = self.registry.histogram(
            "job_execution_time_seconds",
            "Wall-clock execution time of a job",
            window_size=2000,
        )
        self.scheduling_latency: Histogram = self.registry.histogram(
            "scheduling_latency_seconds",
            "Time taken by a single scheduling round",
            window_size=500,
        )

        # ---- Resource gauges -----------------------------------------------
        self.worker_utilization: Gauge = self.registry.gauge(
            "worker_utilization_ratio",
            "Fraction of worker slots currently in use",
        )
        self.cpu_utilization: Gauge = self.registry.gauge(
            "cpu_utilization_ratio",
            "Fraction of CPU budget currently allocated",
        )
        self.memory_utilization: Gauge = self.registry.gauge(
            "memory_utilization_ratio",
            "Fraction of memory budget currently allocated",
        )
        self.queue_depth: Gauge = self.registry.gauge(
            "queue_depth",
            "Number of jobs currently waiting in the queue",
        )

        # ---- Adaptive controller gauges ------------------------------------
        self.current_concurrency: Gauge = self.registry.gauge(
            "current_concurrency_limit",
            "Current adaptive concurrency limit",
        )
        self.system_pressure: Gauge = self.registry.gauge(
            "system_pressure",
            "Current system pressure signal (0.0–1.0)",
        )

        # ---- Fairness gauges -----------------------------------------------
        self.tenant_fairness_debt: LabeledMetric = self.registry.labeled_gauge(
            "tenant_fairness_debt",
            "DRR fairness deficit per tenant",
        )

        # ---- Deadline counters ---------------------------------------------
        self.deadline_misses: Counter = self.registry.counter(
            "deadline_misses_total",
            "Total number of jobs that missed their deadline",
        )
        self.tenant_deadline_misses: LabeledMetric = self.registry.labeled_counter(
            "tenant_deadline_misses_total",
            "Deadline misses per tenant",
        )

    # ---- Convenience recording methods -------------------------------------

    def record_submission(self, job: Job) -> None:
        """Record a newly submitted job."""
        self.jobs_submitted.inc()

    def record_completion(self, job: Job) -> None:
        """Record all relevant metrics for a completed job.

        Handles both successful completions and deadline-miss detection
        (when the job has a deadline and its ``end_time`` exceeds it).
        """
        self.jobs_completed.inc()

        # Per-tenant completion
        if job.tenant_id:
            self.tenant_jobs_completed.labels(tenant_id=job.tenant_id).inc()

        # Latency observations
        wait = job.wait_time
        if wait > 0.0:
            self.job_wait_time.observe(wait)

        execution = job.execution_time
        if execution > 0.0:
            self.job_execution_time.observe(execution)

        # CPU consumed (actual if available, else estimate × duration)
        cpu = job.actual_cpu if job.actual_cpu > 0.0 else job.estimated_cpu
        cpu_seconds = cpu * (execution if execution > 0.0 else job.estimated_duration)
        if job.tenant_id and cpu_seconds > 0.0:
            self.tenant_cpu_consumed.labels(tenant_id=job.tenant_id).inc(cpu_seconds)

        # Deadline miss detection
        if job.deadline is not None and job.end_time > 0.0:
            if job.end_time > job.deadline:
                self.deadline_misses.inc()
                if job.tenant_id:
                    self.tenant_deadline_misses.labels(
                        tenant_id=job.tenant_id
                    ).inc()

    def record_failure(self, job: Job) -> None:
        """Record a failed job."""
        self.jobs_failed.inc()

        # Deadline miss — a failure past the deadline still counts as a miss
        if job.deadline is not None and job.end_time > 0.0:
            if job.end_time > job.deadline:
                self.deadline_misses.inc()
                if job.tenant_id:
                    self.tenant_deadline_misses.labels(
                        tenant_id=job.tenant_id
                    ).inc()

    def record_rejection(self, job: Job) -> None:
        """Record a rejected job."""
        self.jobs_rejected.inc()
        if job.tenant_id:
            self.tenant_jobs_rejected.labels(tenant_id=job.tenant_id).inc()

    def record_deferral(self, job: Job) -> None:
        """Record a deferred job."""
        self.jobs_deferred.inc()

    def record_timeout(self, job: Job) -> None:
        """Record a timed-out job."""
        self.jobs_timed_out.inc()
        if job.deadline is not None and job.end_time > job.deadline:
            self.deadline_misses.inc()
            if job.tenant_id:
                self.tenant_deadline_misses.labels(tenant_id=job.tenant_id).inc()

    def record_cancellation(self, job: Job) -> None:
        """Record a cancelled job."""
        self.jobs_cancelled.inc()

    def record_scheduling_round(self, duration: float, selected: int) -> None:
        """Record a completed scheduling round.

        Parameters
        ----------
        duration:
            Wall-clock seconds the scheduling round took.
        selected:
            Number of jobs selected for execution in this round.
        """
        self.scheduling_latency.observe(duration)

    # ---- Fairness ----------------------------------------------------------

    def compute_fairness_index(self) -> float:
        """Compute Jain's fairness index across tenants.

        .. math::

            J(x_1, ..., x_n) = \\frac{(\\sum x_i)^2}{n \\cdot \\sum x_i^2}

        where :math:`x_i` is the number of completed jobs for tenant *i*.

        Returns
        -------
        float
            A value in ``(0, 1]``.  ``1.0`` indicates perfect fairness.
            Returns ``1.0`` when there are zero or one tenants (trivially
            fair).
        """
        children = self.tenant_jobs_completed.all_metrics()
        if len(children) <= 1:
            return 1.0

        values: list[float] = [m.value for m in children.values()]
        n = len(values)
        total = sum(values)
        sum_squares = sum(v * v for v in values)

        if sum_squares == 0.0:
            # No completions at all — trivially fair
            return 1.0

        return (total * total) / (n * sum_squares)

    # ---- Snapshot / reset --------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Return the full registry snapshot as a dict."""
        return self.registry.snapshot()

    def reset(self) -> None:
        """Reset every metric to its initial state."""
        self.registry.reset()
