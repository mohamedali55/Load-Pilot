"""
LoadPilot — Simulation runner.

Drives a scenario through the engine, processing timed events (capacity
changes, worker failures) and collecting results.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from loadpilot.engine import Engine
from loadpilot.simulator.scenarios import Scenario, ScenarioEvent
from loadpilot.workload.generator import WorkloadGenerator
from loadpilot.workload.models import Job, JobStatus, ResourceVector, Tenant

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class SimulationResult:
    """Outcome of a simulation run."""

    scenario_name: str
    policy_name: str
    duration: float = 0.0

    # Job outcomes
    total_submitted: int = 0
    total_completed: int = 0
    total_failed: int = 0
    total_rejected: int = 0
    total_deferred: int = 0
    total_timed_out: int = 0
    total_cancelled: int = 0

    # Latency (milliseconds)
    wait_time_p50: float = 0.0
    wait_time_p95: float = 0.0
    wait_time_p99: float = 0.0
    exec_time_p50: float = 0.0
    exec_time_p95: float = 0.0
    exec_time_p99: float = 0.0

    # Fairness
    fairness_index: float = 0.0

    # Deadlines
    deadline_miss_count: int = 0
    deadline_miss_rate: float = 0.0

    # Resources
    peak_utilization: float = 0.0
    avg_utilization: float = 0.0

    # Per-tenant breakdown
    tenant_stats: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Scheduling overhead
    scheduling_overhead_ms: float = 0.0

    # Recovery time after overload
    recovery_time: float = 0.0

    # Raw job data for analysis
    jobs: list[Job] = field(default_factory=list, repr=False)


class SimulationRunner:
    """Runs a scenario against the LoadPilot engine with a specific policy."""

    def __init__(self, scenario: Scenario, policy: str = "loadpilot") -> None:
        self._scenario = scenario
        self._policy = policy
        self._capacity_restored_at: float | None = None

    async def run(self) -> SimulationResult:
        """Execute the simulation and return results."""
        scenario = self._scenario
        self._capacity_restored_at = None
        gen = WorkloadGenerator(seed=scenario.seed)

        # Create engine
        engine = Engine(config=scenario.config)

        # Set policy
        engine.set_policy(self._policy)

        # Register tenants
        for tenant in scenario.tenants:
            engine.register_tenant(tenant)

        # Generate jobs
        all_jobs: list[Job] = []
        for profile in scenario.profiles:
            jobs = gen.generate(profile)
            all_jobs.extend(jobs)

        # Submit all jobs
        for job in all_jobs:
            engine.submit(job)

        # Schedule scenario events relative to the beginning of the run.
        start = time.monotonic()
        event_tasks: list[asyncio.Task] = []
        for event in scenario.events:
            task = asyncio.create_task(
                self._apply_event(engine, event, start),
                name=f"event-{event.event_type}",
            )
            event_tasks.append(task)

        utilization_samples: list[float] = []
        sampler_stop = asyncio.Event()
        sampler_task = asyncio.create_task(
            self._sample_utilization(engine, utilization_samples, sampler_stop),
            name="simulation-utilization-sampler",
        )
        event_errors: list[Exception] = []

        # Run engine until drained
        try:
            await engine.run_until_drained(poll_interval=0.02)
        finally:
            duration = time.monotonic() - start
            sampler_stop.set()
            await sampler_task
            for task in event_tasks:
                task.cancel()
            if event_tasks:
                event_results = await asyncio.gather(
                    *event_tasks,
                    return_exceptions=True,
                )
                event_errors.extend(
                    result for result in event_results if isinstance(result, Exception)
                )

        if event_errors:
            raise event_errors[0]
        # Collect results
        return self._collect_results(
            engine,
            all_jobs,
            duration,
            utilization_samples,
        )

    async def _apply_event(
        self,
        engine: Engine,
        event: ScenarioEvent,
        start_time: float,
    ) -> None:
        """Wait for the event's time offset, then apply it."""
        await asyncio.sleep(max(0.0, start_time + event.time_offset - time.monotonic()))

        if event.event_type == "reduce_capacity":
            factor = event.params.get("factor", 0.5)
            cfg = engine.config
            new_workers = max(1, int(cfg.max_workers * factor))
            engine._pool.resize(new_workers)
            engine._resource_manager.update_capacity(
                ResourceVector(
                    cpu=cfg.cpu_budget,
                    memory=cfg.memory_budget,
                    slots=new_workers,
                )
            )
            logger.info("Event: capacity reduced to %d workers", new_workers)

        elif event.event_type == "restore_capacity":
            cfg = engine.config
            engine._pool.resize(cfg.max_workers)
            engine._resource_manager.update_capacity(cfg.total_capacity)
            self._capacity_restored_at = time.monotonic() - start_time
            logger.info("Event: capacity restored to %d workers", cfg.max_workers)

        elif event.event_type == "fail_workers":
            count = max(0, event.params.get("count", 5))
            new_max = max(1, engine._pool._max_workers - count)
            await engine._pool.cancel_all()
            engine._pool.resize(new_max)
            cfg = engine.config
            engine._resource_manager.update_capacity(
                ResourceVector(
                    cpu=cfg.cpu_budget,
                    memory=cfg.memory_budget,
                    slots=new_max,
                )
            )
            logger.info("Event: %d workers failed, pool reduced to %d", count, new_max)
        else:
            raise ValueError(f"Unknown scenario event: {event.event_type!r}")

    async def _sample_utilization(
        self,
        engine: Engine,
        samples: list[float],
        stop: asyncio.Event,
    ) -> None:
        while not stop.is_set():
            samples.append(engine._resource_manager.utilization)
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.005)
            except asyncio.TimeoutError:
                continue
        samples.append(engine._resource_manager.utilization)

    def _collect_results(
        self,
        engine: Engine,
        jobs: list[Job],
        duration: float,
        utilization_samples: list[float] | None = None,
    ) -> SimulationResult:
        """Gather metrics from the engine and job list."""
        metrics = engine.metrics

        completed = [j for j in jobs if j.status == JobStatus.COMPLETED]
        failed = [j for j in jobs if j.status == JobStatus.FAILED]
        rejected = [j for j in jobs if j.status == JobStatus.REJECTED]
        timed_out = [j for j in jobs if j.status == JobStatus.TIMED_OUT]
        cancelled = [j for j in jobs if j.status == JobStatus.CANCELLED]

        # Wait times
        wait_times = sorted([j.wait_time * 1000 for j in completed if j.wait_time > 0])
        exec_times = sorted([j.execution_time * 1000 for j in completed if j.execution_time > 0])

        def percentile(data: list[float], p: float) -> float:
            if not data:
                return 0.0
            idx = int(len(data) * p / 100.0)
            return data[min(idx, len(data) - 1)]

        # Deadline misses
        deadline_jobs = [j for j in jobs if j.deadline is not None]
        deadline_misses = sum(
            1 for j in deadline_jobs
            if j.end_time > 0 and j.end_time > j.deadline
        )
        deadline_miss_rate = (
            deadline_misses / len(deadline_jobs) if deadline_jobs else 0.0
        )

        # Per-tenant stats
        tenant_stats: dict[str, dict[str, Any]] = {}
        tenants = engine._tenants
        for tid, tenant in tenants.items():
            t_completed = [j for j in completed if j.tenant_id == tid]
            t_wait = sorted([j.wait_time * 1000 for j in t_completed if j.wait_time > 0])
            tenant_stats[tid] = {
                "submitted": tenant.total_submitted,
                "completed": tenant.total_completed,
                "rejected": tenant.total_rejected,
                "cpu_consumed": tenant.total_cpu_consumed,
                "wait_p50": percentile(t_wait, 50),
                "wait_p95": percentile(t_wait, 95),
                "completions_per_second": (
                    tenant.total_completed / duration if duration > 0 else 0
                ),
            }

        # Fairness index (Jain's)
        fairness_index = self._jains_fairness(tenants, duration)

        return SimulationResult(
            scenario_name=self._scenario.name,
            policy_name=self._policy,
            duration=duration,
            total_submitted=len(jobs),
            total_completed=len(completed),
            total_failed=len(failed),
            total_rejected=len(rejected),
            total_deferred=int(metrics.jobs_deferred.value),
            total_timed_out=len(timed_out),
            total_cancelled=len(cancelled),
            wait_time_p50=percentile(wait_times, 50),
            wait_time_p95=percentile(wait_times, 95),
            wait_time_p99=percentile(wait_times, 99),
            exec_time_p50=percentile(exec_times, 50),
            exec_time_p95=percentile(exec_times, 95),
            exec_time_p99=percentile(exec_times, 99),
            fairness_index=fairness_index,
            deadline_miss_count=deadline_misses,
            deadline_miss_rate=deadline_miss_rate,
            scheduling_overhead_ms=metrics.scheduling_latency.mean * 1000,
            peak_utilization=max(utilization_samples, default=0.0),
            avg_utilization=(
                sum(utilization_samples) / len(utilization_samples)
                if utilization_samples else 0.0
            ),
            recovery_time=(
                max(0.0, duration - self._capacity_restored_at)
                if self._capacity_restored_at is not None
                else 0.0
            ),
            tenant_stats=tenant_stats,
            jobs=jobs,
        )

    def _jains_fairness(
        self,
        tenants: dict[str, Tenant],
        duration: float,
    ) -> float:
        """Compute Jain's fairness index across tenant throughput shares.

        J(x) = (Σ x_i)² / (n · Σ x_i²)

        where x_i is tenant i's normalized throughput:
            x_i = (completed_i / weight_i)
        """
        if not tenants or duration <= 0:
            return 1.0

        normalized = []
        for tenant in tenants.values():
            weight = max(tenant.weight, 0.01)
            throughput = tenant.total_completed / weight
            normalized.append(throughput)

        n = len(normalized)
        if n == 0:
            return 1.0

        sum_x = sum(normalized)
        sum_x2 = sum(x * x for x in normalized)

        if sum_x2 == 0:
            return 1.0

        return (sum_x ** 2) / (n * sum_x2)
