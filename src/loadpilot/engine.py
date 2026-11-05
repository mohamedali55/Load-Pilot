"""
LoadPilot — Main Engine.

The top-level ``Engine`` class ties all modules together into the public API:

    from loadpilot import Engine, Job, Tenant

    engine = Engine(max_workers=50, cpu_budget=100, memory_budget=4096)
    engine.register_tenant(Tenant(id="customer-a", weight=2))
    engine.submit(Job(id="report-001", tenant_id="customer-a", estimated_cpu=10))
    await engine.start()
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from loadpilot.admission.controller import AdmissionController
from loadpilot.controller.adaptive import AdaptiveConcurrencyController
from loadpilot.controller.backpressure import BackpressureConfig, BackpressureController
from loadpilot.resources.manager import ResourceManager
from loadpilot.runtime.executor import AsyncExecutor, RetryPolicy
from loadpilot.runtime.worker import WorkerPool
from loadpilot.scheduler.engine import SchedulerEngine
from loadpilot.scheduler.policies import (
    DRRPolicy,
    FIFOPolicy,
    LoadPilotPolicy,
    PriorityPolicy,
    SchedulingPolicy,
)
from loadpilot.telemetry.collector import MetricsCollector
from loadpilot.workload.models import (
    AdmissionDecision,
    EngineConfig,
    Job,
    JobStatus,
    ResourceVector,
    SchedulingDecision,
    Tenant,
)

logger = logging.getLogger(__name__)


class Engine:
    """LoadPilot scheduling engine.

    Combines admission control, fair scheduling, resource management,
    async execution, and adaptive concurrency into a single runtime.
    """

    def __init__(
        self,
        max_workers: int = 50,
        cpu_budget: float = 100.0,
        memory_budget: float = 4096.0,
        config: EngineConfig | None = None,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = EngineConfig(
                max_workers=max_workers,
                cpu_budget=cpu_budget,
                memory_budget=memory_budget,
            )

        # ---- sub-systems ----
        self._resource_manager = ResourceManager(self._config.total_capacity)
        self._admission = AdmissionController(self._config, self._resource_manager)
        self._scheduler = SchedulerEngine(self._config)
        self._pool = WorkerPool(self._config.max_workers)
        self._executor = AsyncExecutor(
            self._pool,
            RetryPolicy(
                max_retries=self._config.default_max_retries,
                base_delay=self._config.retry_base_delay,
                max_delay=self._config.retry_max_delay,
            ),
        )
        self._concurrency_ctrl = AdaptiveConcurrencyController(
            initial_concurrency=self._config.initial_concurrency,
            min_concurrency=self._config.min_concurrency,
            max_concurrency=self._config.max_concurrency,
            latency_target_ms=self._config.latency_target_ms,
            window_size=self._config.latency_window_size,
            step=self._config.concurrency_step,
        )
        self._backpressure = BackpressureController(BackpressureConfig(
            defer_threshold=self._config.defer_threshold,
            shed_low_threshold=self._config.load_shed_threshold,
            queue_hard_limit=self._config.max_queue_size,
        ))
        self._metrics = MetricsCollector()

        # ---- tenant registry ----
        self._tenants: dict[str, Tenant] = {}

        # ---- job tracking ----
        self._all_jobs: dict[str, Job] = {}
        self._deferred: list[Job] = []

        # ---- internal state ----
        self._running = False
        self._scheduling_task: asyncio.Task | None = None
        self._controller_task: asyncio.Task | None = None
        self._retry_task: asyncio.Task | None = None

        # Completion is terminal only after retries are exhausted.
        self._executor.on_completion(self._on_job_completed)

    # ==================================================================
    # Public API
    # ==================================================================

    def register_tenant(self, tenant: Tenant) -> None:
        """Register a tenant with the engine."""
        self._tenants[tenant.id] = tenant
        self._scheduler.register_tenant(
            tenant.id,
            weight=tenant.weight,
            quantum=self._config.default_quantum * tenant.weight,
        )
        self._resource_manager.register_tenant(
            tenant.id,
            budget=tenant.resource_budget,
        )
        logger.info("Registered tenant %s (weight=%.1f)", tenant.id, tenant.weight)

    def submit(self, job: Job) -> AdmissionDecision:
        """Submit a job for processing.

        Returns the admission decision: ACCEPT, DEFER, or REJECT.
        """
        job.submit_time = time.monotonic()
        self._all_jobs[job.id] = job
        self._metrics.record_submission(job)

        tenant = self._tenants.get(job.tenant_id)
        if tenant is None:
            tenant = Tenant(id=job.tenant_id, weight=1.0)
            self.register_tenant(tenant)

        tenant.total_submitted += 1

        # Update backpressure state
        self._backpressure.update(
            system_pressure=self._concurrency_ctrl.system_pressure,
            queue_depth=self._scheduler.pending_count(),
        )

        # Admission control
        decision = self._evaluate_admission(job, tenant)

        if decision == AdmissionDecision.ACCEPT:
            job.status = JobStatus.ADMITTED
            self._scheduler.enqueue(job)
            tenant.current_queued += 1
        elif decision == AdmissionDecision.DEFER:
            job.status = JobStatus.DEFERRED
            self._deferred.append(job)
            self._metrics.record_deferral(job)
        else:  # REJECT
            job.status = JobStatus.REJECTED
            tenant.total_rejected += 1
            self._metrics.record_rejection(job)

        return decision

    def _evaluate_admission(self, job: Job, tenant: Tenant) -> AdmissionDecision:
        """Apply admission and backpressure policies to the current engine state."""
        queue_depth = self._scheduler.pending_count()
        self._backpressure.update(
            system_pressure=self._concurrency_ctrl.system_pressure,
            queue_depth=queue_depth,
        )
        decision = self._admission.evaluate(
            job,
            tenant,
            total_queue_size=queue_depth,
        )
        if decision is AdmissionDecision.ACCEPT:
            decision = self._backpressure.evaluate(job)
        return decision

    def submit_batch(self, jobs: list[Job]) -> list[AdmissionDecision]:
        """Submit multiple jobs. Returns a list of admission decisions."""
        return [self.submit(job) for job in jobs]

    async def start(self) -> None:
        """Start the engine's scheduling and control loops."""
        if self._running:
            return

        self._running = True
        logger.info("Engine starting (workers=%d)", self._config.max_workers)

        self._scheduling_task = asyncio.create_task(
            self._scheduling_loop(), name="loadpilot-scheduler"
        )
        self._controller_task = asyncio.create_task(
            self._controller_loop(), name="loadpilot-controller"
        )
        self._retry_task = asyncio.create_task(
            self._executor.process_retries(), name="loadpilot-retries"
        )

        try:
            await asyncio.gather(
                self._scheduling_task,
                self._controller_task,
                self._retry_task,
            )
        except asyncio.CancelledError:
            pass

    async def run_until_drained(self, poll_interval: float = 0.05) -> None:
        """Start the engine and run until all submitted jobs have completed."""
        if self._running:
            return

        self._running = True

        sched_task = asyncio.create_task(self._scheduling_loop())
        ctrl_task = asyncio.create_task(self._controller_loop())
        retry_task = asyncio.create_task(self._executor.process_retries())

        try:
            while self._running:
                pending = self._scheduler.pending_count()
                active = self._pool.active_count
                deferred = len(self._deferred)

                if (
                    pending == 0
                    and active == 0
                    and deferred == 0
                    and self._executor.pending_retry_count == 0
                ):
                    break

                await asyncio.sleep(poll_interval)
        finally:
            self._running = False
            sched_task.cancel()
            ctrl_task.cancel()
            retry_task.cancel()
            await asyncio.gather(
                sched_task, ctrl_task, retry_task, return_exceptions=True
            )
            await self._pool.shutdown(timeout=5.0)

    async def stop(self) -> None:
        """Gracefully stop the engine."""
        self._running = False
        for task in (self._scheduling_task, self._controller_task, self._retry_task):
            if task is not None:
                task.cancel()
        await self._executor.shutdown(timeout=10.0)
        logger.info("Engine stopped")

    # ==================================================================
    # Scheduling policy
    # ==================================================================

    def set_policy(self, policy: str | SchedulingPolicy) -> None:
        """Switch the scheduling policy.

        Accepts "fifo", "priority", "drr", "loadpilot", or a SchedulingPolicy.
        """
        if isinstance(policy, str):
            policy_map: dict[str, SchedulingPolicy] = {
                "fifo": FIFOPolicy(),
                "priority": PriorityPolicy(),
                "drr": DRRPolicy(self._scheduler._drr),
                "loadpilot": LoadPilotPolicy(self._config),
            }
            resolved = policy_map.get(policy.lower())
            if resolved is None:
                raise ValueError(
                    f"Unknown policy {policy!r}. "
                    f"Options: {', '.join(policy_map)}"
                )
            self._scheduler.set_policy(resolved)
        else:
            self._scheduler.set_policy(policy)

    # ==================================================================
    # Telemetry / Observability
    # ==================================================================

    @property
    def metrics(self) -> MetricsCollector:
        return self._metrics

    @property
    def config(self) -> EngineConfig:
        return self._config

    def snapshot(self) -> dict[str, Any]:
        """Return a comprehensive snapshot of the engine state."""
        return {
            "running": self._running,
            "pending_jobs": self._scheduler.pending_count(),
            "pending_by_tenant": self._scheduler.pending_count_by_tenant(),
            "active_jobs": self._pool.active_count,
            "deferred_jobs": len(self._deferred),
            "concurrency_limit": self._concurrency_ctrl.limit,
            "system_pressure": self._concurrency_ctrl.system_pressure,
            "resources": self._resource_manager.snapshot(),
            "pool": self._pool.snapshot(),
            "backpressure": {
                "level": self._backpressure.level.value,
                "pressure": self._backpressure.pressure,
            },
        }

    def get_job(self, job_id: str) -> Job | None:
        """Look up a job by ID."""
        return self._all_jobs.get(job_id)

    def get_all_jobs(self) -> dict[str, Job]:
        """Return all tracked jobs."""
        return dict(self._all_jobs)

    # ==================================================================
    # Internal loops
    # ==================================================================

    async def _scheduling_loop(self) -> None:
        while self._running:
            try:
                await self._schedule_round()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in scheduling loop")
            await asyncio.sleep(0.01)

    async def _schedule_round(self) -> None:
        available_slots = self._pool.available_count
        if available_slots <= 0:
            return

        start = time.monotonic()
        batch_size = min(available_slots, 20)

        decisions = self._scheduler.select_batch(
            n=batch_size,
            system_pressure=self._concurrency_ctrl.system_pressure,
            available_capacity=self._resource_manager.available,
        )

        sched_duration = time.monotonic() - start
        self._metrics.record_scheduling_round(sched_duration, len(decisions))

        execution_tasks: list[asyncio.Task[None]] = []
        for decision in decisions:
            if decision.action != "execute":
                continue

            job = self._all_jobs.get(decision.job_id)
            if job is None:
                continue

            if not self._resource_manager.reserve(job):
                job.trace("resource reservation failed — deferring")
                job.status = JobStatus.DEFERRED
                self._deferred.append(job)
                tenant = self._tenants.get(job.tenant_id)
                if tenant:
                    tenant.current_queued = max(0, tenant.current_queued - 1)
                continue

            tenant = self._tenants.get(job.tenant_id)
            if tenant:
                tenant.current_queued = max(0, tenant.current_queued - 1)
                tenant.current_running += 1

            execution_tasks.append(
                asyncio.create_task(
                    self._executor.execute(job),
                    name=f"exec-{job.id}",
                )
            )

        if execution_tasks:
            await asyncio.gather(*execution_tasks)

        # Re-attempt deferred jobs
        if self._deferred and self._pool.available_count > 0:
            self._retry_deferred()

    def _retry_deferred(self) -> None:
        still_deferred: list[Job] = []
        while self._deferred:
            job = self._deferred.pop(0)
            tenant = self._tenants.get(job.tenant_id)
            if tenant is None:
                tenant = Tenant(id=job.tenant_id)
                self.register_tenant(tenant)

            decision = self._evaluate_admission(job, tenant)

            if decision == AdmissionDecision.ACCEPT:
                job.status = JobStatus.ADMITTED
                self._scheduler.enqueue(job)
                tenant.current_queued += 1
            elif decision == AdmissionDecision.DEFER:
                still_deferred.append(job)
                self._metrics.record_deferral(job)
                break  # Stop retrying if still under pressure
            else:
                job.status = JobStatus.REJECTED
                tenant.total_rejected += 1
                self._metrics.record_rejection(job)

        self._deferred = still_deferred + self._deferred

    async def _controller_loop(self) -> None:
        while self._running:
            try:
                pending = self._scheduler.pending_count()
                max_q = self._config.max_queue_size

                self._concurrency_ctrl.set_queue_pressure(
                    pending / max_q if max_q > 0 else 0.0
                )

                signal = self._concurrency_ctrl.adjust()
                new_limit = self._concurrency_ctrl.limit
                self._pool.resize(new_limit)

                self._metrics.current_concurrency.set(new_limit)
                self._metrics.system_pressure.set(
                    self._concurrency_ctrl.system_pressure
                )
                self._metrics.queue_depth.set(pending)
                self._metrics.worker_utilization.set(self._pool.utilization)
                self._metrics.cpu_utilization.set(self._resource_manager.utilization)

                self._admission.set_pressure(
                    self._concurrency_ctrl.system_pressure
                )
                self._backpressure.update(
                    system_pressure=self._concurrency_ctrl.system_pressure,
                    queue_depth=pending,
                )

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in controller loop")

            await asyncio.sleep(self._config.adjustment_interval)

    # ==================================================================
    # Completion callback
    # ==================================================================

    async def _on_job_completed(self, job: Job) -> None:
        self._resource_manager.release(job)

        tenant = self._tenants.get(job.tenant_id)
        if tenant:
            tenant.current_running = max(0, tenant.current_running - 1)
            if job.status == JobStatus.COMPLETED:
                tenant.total_completed += 1
                tenant.total_cpu_consumed += job.actual_cpu
                tenant.total_memory_consumed += job.actual_memory

        if job.execution_time > 0:
            self._concurrency_ctrl.record_latency(job.execution_time * 1000)

        self._scheduler.update_fairness(job.tenant_id, job.estimated_cost)
        if job.status is JobStatus.COMPLETED:
            self._metrics.record_completion(job)
        elif job.status is JobStatus.TIMED_OUT:
            self._metrics.record_timeout(job)
        elif job.status is JobStatus.FAILED:
            self._metrics.record_failure(job)
        elif job.status is JobStatus.CANCELLED:
            self._metrics.record_cancellation(job)
