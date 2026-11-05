"""
LoadPilot — Scheduling policies.

Defines the ``SchedulingPolicy`` protocol and four concrete implementations:

* **FIFOPolicy** — sort by submit time (earliest first).
* **PriorityPolicy** — sort by priority descending, then submit time.
* **DRRPolicy** — wraps :class:`DeficitRoundRobin`, groups candidates by tenant.
* **LoadPilotPolicy** — unified multi-signal scoring used by the engine.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from loadpilot.scheduler.drr import DeficitRoundRobin
from loadpilot.workload.models import (
    EngineConfig,
    Job,
    ResourceVector,
)


# ======================================================================
# Scheduler state — a snapshot passed to policies each scheduling round
# ======================================================================


@dataclass(slots=True)
class SchedulerState:
    """Contextual information provided to scheduling policies.

    Attributes
    ----------
    now : float
        Current monotonic timestamp.
    fairness_debt : dict[str, float]
        Per-tenant fairness debt.  Positive values indicate that the tenant
        has consumed *less* than its fair share (i.e., it is owed resources).
    system_pressure : float
        A value in ``[0.0, 1.0]`` indicating overall system load.
    available_capacity : ResourceVector
        Resources currently available for new jobs.
    config : EngineConfig
        Engine configuration (contains scoring weights).
    """

    now: float = 0.0
    fairness_debt: dict[str, float] = field(default_factory=dict)
    system_pressure: float = 0.0
    available_capacity: ResourceVector = field(default_factory=ResourceVector.zero)
    config: EngineConfig = field(default_factory=EngineConfig)


# ======================================================================
# Protocol
# ======================================================================


@runtime_checkable
class SchedulingPolicy(Protocol):
    """Interface that every scheduling policy must satisfy."""

    def select(
        self,
        candidates: list[Job],
        state: SchedulerState,
    ) -> list[Job]:
        """Return *candidates* sorted in order of decreasing priority.

        The scheduler will dispatch from the head of the returned list.
        """
        ...  # pragma: no cover


# ======================================================================
# Concrete policies
# ======================================================================


class FIFOPolicy:
    """First-In, First-Out — sort by submit time (earliest first)."""

    __slots__ = ()

    def select(
        self,
        candidates: list[Job],
        state: SchedulerState,
    ) -> list[Job]:
        return sorted(candidates, key=lambda j: j.submit_time)


class PriorityPolicy:
    """Priority-based scheduling.

    Jobs with higher :pyattr:`Priority` value sort first; ties are broken by
    submit time (earlier first).
    """

    __slots__ = ()

    def select(
        self,
        candidates: list[Job],
        state: SchedulerState,
    ) -> list[Job]:
        return sorted(
            candidates,
            key=lambda j: (-int(j.priority), j.submit_time),
        )


class DRRPolicy:
    """Wraps :class:`DeficitRoundRobin`.

    Groups candidate jobs by ``tenant_id`` and delegates scheduling to the
    DRR core.  The returned list may contain fewer elements than the input
    because only tenants with sufficient deficit are served.
    """

    __slots__ = ("_drr",)

    def __init__(self, drr: DeficitRoundRobin) -> None:
        self._drr = drr

    def select(
        self,
        candidates: list[Job],
        state: SchedulerState,
    ) -> list[Job]:
        # Build per-tenant queues from the candidate list (preserving order).
        per_tenant: dict[str, deque[Job]] = {}
        for job in candidates:
            per_tenant.setdefault(job.tenant_id, deque()).append(job)

        selected: list[Job] = []
        # Keep pulling until DRR returns None.
        while True:
            job = self._drr.select_next(per_tenant)
            if job is None:
                break
            selected.append(job)

        return selected


class LoadPilotPolicy:
    """Unified multi-signal scoring policy.

    Combines four components into a single scalar score per job:

    * **Fairness debt** — tenants that have consumed less than their share get
      a boost.
    * **Deadline urgency** — jobs with smaller deadline slack score higher.
    * **Cost efficiency** — cheaper jobs are preferred when the system is under
      pressure.
    * **Priority boost** — the job's static priority level.
    """

    __slots__ = ("_config",)

    def __init__(self, config: EngineConfig) -> None:
        self._config = config

    def select(
        self,
        candidates: list[Job],
        state: SchedulerState,
    ) -> list[Job]:
        scored: list[tuple[float, float, Job]] = []
        for job in candidates:
            s = self._score(job, state)
            # Negate score so that higher scores sort first.
            scored.append((-s, job.submit_time, job))
        scored.sort()
        return [job for _, _, job in scored]

    # ------------------------------------------------------------------
    # Scoring function
    # ------------------------------------------------------------------

    def _score(self, job: Job, state: SchedulerState) -> float:
        """Compute the composite scheduling score for *job*."""
        config = self._config

        # 1. Fairness debt: how much has this tenant under-consumed?
        fairness_component = (
            state.fairness_debt.get(job.tenant_id, 0.0) * config.fairness_weight
        )

        # 2. Deadline urgency: lower slack → higher urgency.
        slack = job.deadline_slack(state.now)
        if slack is not None:
            deadline_component = max(0.0, (1.0 / max(slack, 0.1))) * config.deadline_weight
        else:
            deadline_component = 0.0

        # 3. Cost efficiency: prefer cheaper jobs when system is under pressure.
        cost_component = (
            (1.0 / max(job.estimated_cost, 0.1))
            * config.cost_weight
            * state.system_pressure
        )

        # 4. Priority boost.
        priority_component = float(job.priority.value)

        return (
            fairness_component
            + deadline_component
            + cost_component
            + priority_component
        )
