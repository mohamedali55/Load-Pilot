"""
LoadPilot — Scheduler engine.

Orchestrates Deficit Round Robin, Earliest Deadline First, and pluggable
scheduling policies to produce a prioritised batch of :class:`SchedulingDecision`
objects each scheduling round.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque

from loadpilot.scheduler.drr import DeficitRoundRobin
from loadpilot.scheduler.edf import EarliestDeadlineFirst
from loadpilot.scheduler.policies import (
    LoadPilotPolicy,
    SchedulerState,
    SchedulingPolicy,
)
from loadpilot.workload.models import (
    EngineConfig,
    Job,
    JobStatus,
    ResourceVector,
    SchedulingDecision,
)


class SchedulerEngine:
    """Central scheduler that ties DRR, EDF, and pluggable policies together.

    Parameters
    ----------
    config : EngineConfig
        Engine-wide configuration (weights, budgets, etc.).
    """

    __slots__ = (
        "_config",
        "_drr",
        "_edf",
        "_queues",
        "_policy",
        "_fairness_debt",
        "_tenant_weights",
        "_job_index",
    )

    def __init__(self, config: EngineConfig) -> None:
        self._config: EngineConfig = config
        self._drr: DeficitRoundRobin = DeficitRoundRobin({})
        self._edf: EarliestDeadlineFirst = EarliestDeadlineFirst()
        self._queues: dict[str, deque[Job]] = defaultdict(deque)
        self._policy: SchedulingPolicy = LoadPilotPolicy(config)
        self._fairness_debt: dict[str, float] = {}
        self._tenant_weights: dict[str, float] = {}
        # Secondary index: job_id → Job for O(1) look-ups and removals.
        self._job_index: dict[str, Job] = {}

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    def enqueue(self, job: Job) -> None:
        """Add *job* to the scheduling queues.

        The job is appended to its tenant's FIFO queue.  If the job carries a
        deadline, it is also pushed into the EDF priority queue.

        Raises
        ------
        ValueError
            If a job with the same ID is already enqueued.
        """
        if job.id in self._job_index:
            raise ValueError(f"Job {job.id!r} is already enqueued")

        if job.submit_time == 0.0:
            job.submit_time = time.monotonic()

        job.status = JobStatus.ADMITTED
        job.admit_time = time.monotonic()

        self._queues[job.tenant_id].append(job)
        self._job_index[job.id] = job

        if job.deadline is not None:
            self._edf.push(job)

        # Ensure the tenant has a fairness-debt entry.
        self._fairness_debt.setdefault(job.tenant_id, 0.0)

    def remove_job(self, job_id: str) -> Job | None:
        """Remove a job by *job_id* from all internal structures.

        Returns the removed :class:`Job`, or ``None`` if not found.
        """
        job = self._job_index.pop(job_id, None)
        if job is None:
            return None

        # Remove from the tenant queue.
        queue = self._queues.get(job.tenant_id)
        if queue is not None:
            try:
                queue.remove(job)
            except ValueError:
                pass  # Already consumed by DRR / select_batch
            if not queue:
                del self._queues[job.tenant_id]

        # Remove from EDF (lazy deletion — safe even if not present).
        self._edf.remove(job_id)

        return job

    # ------------------------------------------------------------------
    # Policy management
    # ------------------------------------------------------------------

    def set_policy(self, policy: SchedulingPolicy) -> None:
        """Replace the current scheduling policy."""
        self._policy = policy

    # ------------------------------------------------------------------
    # Tenant management
    # ------------------------------------------------------------------

    def register_tenant(
        self,
        tenant_id: str,
        weight: float = 1.0,
        quantum: float | None = None,
    ) -> None:
        """Register (or update) a tenant with the given *weight* and *quantum*.

        Parameters
        ----------
        tenant_id : str
            Unique tenant identifier.
        weight : float
            Relative scheduling weight.
        quantum : float | None
            DRR quantum.  Defaults to ``config.default_quantum``.
        """
        if quantum is None:
            quantum = self._config.default_quantum

        self._tenant_weights[tenant_id] = weight
        self._fairness_debt.setdefault(tenant_id, 0.0)
        self._drr.add_tenant(tenant_id, quantum)

    # ------------------------------------------------------------------
    # Main scheduling entry-point
    # ------------------------------------------------------------------

    def select_batch(
        self,
        n: int,
        system_pressure: float = 0.0,
        available_capacity: ResourceVector | None = None,
    ) -> list[SchedulingDecision]:
        """Select up to *n* jobs for execution.

        Algorithm
        ---------
        1. Check the EDF queue for deadline-urgent jobs (slack < 0).
        2. Gather remaining candidates from per-tenant FIFO queues.
        3. Use the active :class:`SchedulingPolicy` to score and rank.
        4. Greedily pack jobs that fit within *available_capacity*.
        5. Update fairness-debt counters for selected tenants.

        Returns a list of :class:`SchedulingDecision` objects that explain
        **why** each job was selected.
        """
        if n <= 0:
            return []

        if available_capacity is None:
            available_capacity = self._config.total_capacity

        now = time.monotonic()

        selected: list[SchedulingDecision] = []
        selected_ids: set[str] = set()
        remaining_capacity = available_capacity

        # ----------------------------------------------------------
        # Phase 1: Urgent deadline jobs (slack < 0 → already late)
        # ----------------------------------------------------------
        urgent_jobs = self._edf.get_urgent(now, threshold=0.0)
        for job in urgent_jobs:
            if len(selected) >= n:
                break
            if job.id in selected_ids:
                continue
            if not job.resource_estimate.fits_within(remaining_capacity):
                continue

            slack = job.deadline_slack(now)
            decision = SchedulingDecision(
                job_id=job.id,
                tenant_id=job.tenant_id,
                action="execute",
                score=float("inf"),
                reason="urgent-deadline",
                fairness_debt=self._fairness_debt.get(job.tenant_id, 0.0),
                deadline_slack=slack,
                estimated_cost=job.estimated_cost,
                system_pressure=system_pressure,
            )
            selected.append(decision)
            selected_ids.add(job.id)
            remaining_capacity = remaining_capacity - job.resource_estimate
            self._mark_scheduled(job)

        if len(selected) >= n:
            return selected

        # ----------------------------------------------------------
        # Phase 2: Gather non-urgent candidates
        # ----------------------------------------------------------
        candidates: list[Job] = []
        for queue in self._queues.values():
            for job in queue:
                if job.id not in selected_ids:
                    candidates.append(job)

        if not candidates:
            return selected

        # ----------------------------------------------------------
        # Phase 3: Score and rank via the active policy
        # ----------------------------------------------------------
        state = SchedulerState(
            now=now,
            fairness_debt=dict(self._fairness_debt),
            system_pressure=system_pressure,
            available_capacity=remaining_capacity,
            config=self._config,
        )
        ranked = self._policy.select(candidates, state)

        # ----------------------------------------------------------
        # Phase 4: Greedy packing
        # ----------------------------------------------------------
        for job in ranked:
            if len(selected) >= n:
                break
            if job.id in selected_ids:
                continue
            if not job.resource_estimate.fits_within(remaining_capacity):
                continue

            slack = job.deadline_slack(now)
            decision = SchedulingDecision(
                job_id=job.id,
                tenant_id=job.tenant_id,
                action="execute",
                score=self._compute_score_for_trace(job, state),
                reason="policy-selected",
                fairness_debt=self._fairness_debt.get(job.tenant_id, 0.0),
                deadline_slack=slack,
                estimated_cost=job.estimated_cost,
                system_pressure=system_pressure,
            )
            selected.append(decision)
            selected_ids.add(job.id)
            remaining_capacity = remaining_capacity - job.resource_estimate
            self._mark_scheduled(job)

        # ----------------------------------------------------------
        # Phase 5: Update fairness debt
        # ----------------------------------------------------------
        self._update_fairness_after_batch(selected)

        return selected

    # ------------------------------------------------------------------
    # Fairness accounting
    # ------------------------------------------------------------------

    def update_fairness(self, tenant_id: str, cost_consumed: float) -> None:
        """Update fairness debt after a tenant consumes resources.

        Positive debt means the tenant is *owed* resources (under-consumed).
        Consuming cost *decreases* the debt (or makes it negative if the
        tenant has over-consumed relative to its weight).
        """
        weight = self._tenant_weights.get(tenant_id, 1.0)
        # Compute the tenant's fair share fraction.
        total_weight = sum(self._tenant_weights.values()) or 1.0
        fair_fraction = weight / total_weight

        # Decrease debt by the cost consumed, normalised by fair share.
        self._fairness_debt.setdefault(tenant_id, 0.0)
        self._fairness_debt[tenant_id] -= cost_consumed / max(fair_fraction, 1e-9)

    # ------------------------------------------------------------------
    # Queue introspection
    # ------------------------------------------------------------------

    def pending_count(self) -> int:
        """Total number of jobs in all tenant queues."""
        return sum(len(q) for q in self._queues.values())

    def pending_count_by_tenant(self) -> dict[str, int]:
        """Number of pending jobs per tenant."""
        return {tid: len(q) for tid, q in self._queues.items() if q}

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _mark_scheduled(self, job: Job) -> None:
        """Transition *job* to SCHEDULED and remove from internal queues."""
        job.status = JobStatus.SCHEDULED
        job.schedule_time = time.monotonic()
        job.trace(f"scheduled at {job.schedule_time:.6f}")

        # Remove from the tenant queue and index.
        queue = self._queues.get(job.tenant_id)
        if queue is not None:
            try:
                queue.remove(job)
            except ValueError:
                pass
            if not queue:
                del self._queues[job.tenant_id]

        self._job_index.pop(job.id, None)
        self._edf.remove(job.id)

    def _update_fairness_after_batch(
        self,
        decisions: list[SchedulingDecision],
    ) -> None:
        """Adjust fairness debt for every tenant that had a job selected.

        Each selected job's ``estimated_cost`` is treated as a cost charge.
        All tenants (including those *not* selected) gain a small credit
        proportional to their weight.
        """
        if not decisions:
            return

        total_cost = sum(d.estimated_cost for d in decisions)
        total_weight = sum(self._tenant_weights.values()) or 1.0

        # Credit all tenants their fair-share entitlement.
        for tid, weight in self._tenant_weights.items():
            fair_share = (weight / total_weight) * total_cost
            self._fairness_debt.setdefault(tid, 0.0)
            self._fairness_debt[tid] += fair_share

        # Debit tenants that actually consumed.
        for decision in decisions:
            self._fairness_debt.setdefault(decision.tenant_id, 0.0)
            self._fairness_debt[decision.tenant_id] -= decision.estimated_cost

    def _compute_score_for_trace(self, job: Job, state: SchedulerState) -> float:
        """Compute a scalar score for inclusion in the decision trace.

        This delegates to :class:`LoadPilotPolicy._score` when the active
        policy is the unified policy; otherwise returns 0.0.
        """
        if isinstance(self._policy, LoadPilotPolicy):
            return self._policy._score(job, state)
        return 0.0
