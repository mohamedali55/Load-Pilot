"""
LoadPilot — Admission policies.

Each policy implements a simple ``evaluate(job, context) → AdmissionDecision``
protocol.  Policies are designed to be composed via :class:`CompositePolicy`
which applies the *most restrictive* decision across all members.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from loadpilot.workload.models import (
    AdmissionDecision,
    Job,
    Priority,
    ResourceVector,
)


# ---------------------------------------------------------------------------
# Context passed to every policy
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class AdmissionContext:
    """Snapshot of system and tenant state at the moment of evaluation.

    Parameters
    ----------
    system_pressure:
        Overall system pressure metric between 0.0 (idle) and 1.0 (overloaded).
    resource_available:
        Currently available global resources.
    resource_capacity:
        Total global capacity before active reservations, when available.
    tenant_queue_size:
        Number of jobs this tenant already has queued (admitted but not running).
    tenant_running_count:
        Number of jobs this tenant currently has running.
    tenant_max_queue:
        Per-tenant queue-size limit (``None`` → unlimited).
    tenant_max_concurrent:
        Per-tenant concurrent-running limit (``None`` → unlimited).
    total_queue_size:
        Total number of jobs across all tenants currently queued.
    max_queue_size:
        System-wide maximum queue size.
    """

    system_pressure: float
    resource_available: ResourceVector
    tenant_queue_size: int
    tenant_running_count: int
    tenant_max_queue: int | None
    tenant_max_concurrent: int | None
    total_queue_size: int
    max_queue_size: int
    resource_capacity: ResourceVector | None = None


# ---------------------------------------------------------------------------
# AdmissionPolicy protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class AdmissionPolicy(Protocol):
    """Protocol that all admission policies must satisfy."""

    def evaluate(self, job: Job, context: AdmissionContext) -> AdmissionDecision:
        """Evaluate whether *job* should be accepted, deferred, or rejected."""
        ...


# ---------------------------------------------------------------------------
# Concrete policies
# ---------------------------------------------------------------------------

class AlwaysAdmitPolicy:
    """Trivially accepts every job — useful for testing or bypass mode."""

    __slots__ = ()

    def evaluate(self, job: Job, context: AdmissionContext) -> AdmissionDecision:
        return AdmissionDecision.ACCEPT


class ResourceBasedPolicy:
    """Rejects a job if its resource estimate exceeds available capacity."""

    __slots__ = ()

    def evaluate(self, job: Job, context: AdmissionContext) -> AdmissionDecision:
        cost = job.resource_estimate
        if cost.fits_within(context.resource_available):
            return AdmissionDecision.ACCEPT
        if (
            context.resource_capacity is not None
            and cost.fits_within(context.resource_capacity)
        ):
            return AdmissionDecision.DEFER
        return AdmissionDecision.REJECT


class QueueLimitPolicy:
    """Enforces per-tenant and system-wide queue limits.

    Decision logic
    --------------
    1. If the system queue is at capacity → **REJECT**.
    2. If the tenant has a max-concurrent limit and is at it → **DEFER**.
    3. If the tenant has a max-queue limit:
       - At limit → **REJECT**.
       - At 80 % of limit → **DEFER** (early back-pressure signal).
    4. Otherwise → **ACCEPT**.
    """

    __slots__ = ("_near_limit_ratio",)

    def __init__(self, *, near_limit_ratio: float = 0.80) -> None:
        self._near_limit_ratio = near_limit_ratio

    def evaluate(self, job: Job, context: AdmissionContext) -> AdmissionDecision:
        # System-wide queue guard
        if context.total_queue_size >= context.max_queue_size:
            return AdmissionDecision.REJECT

        # Tenant concurrent-running cap
        if (
            context.tenant_max_concurrent is not None
            and context.tenant_running_count >= context.tenant_max_concurrent
        ):
            return AdmissionDecision.DEFER

        # Tenant queue cap
        if context.tenant_max_queue is not None:
            if context.tenant_queue_size >= context.tenant_max_queue:
                return AdmissionDecision.REJECT
            near_limit = int(context.tenant_max_queue * self._near_limit_ratio)
            if context.tenant_queue_size >= near_limit:
                return AdmissionDecision.DEFER

        return AdmissionDecision.ACCEPT


class PressureBasedPolicy:
    """Back-pressure policy driven by system load.

    * When pressure exceeds *defer_threshold*, low-priority jobs are deferred.
    * When pressure exceeds *shed_threshold*, only CRITICAL jobs are accepted;
      everything else is rejected.

    Parameters
    ----------
    defer_threshold:
        Pressure level (0.0–1.0) above which low-priority jobs are deferred.
    shed_threshold:
        Pressure level (0.0–1.0) above which non-critical jobs are rejected.
    """

    __slots__ = ("_defer_threshold", "_shed_threshold")

    def __init__(
        self,
        *,
        defer_threshold: float = 0.80,
        shed_threshold: float = 0.95,
    ) -> None:
        if defer_threshold >= shed_threshold:
            raise ValueError(
                f"defer_threshold ({defer_threshold}) must be less than "
                f"shed_threshold ({shed_threshold})"
            )
        self._defer_threshold = defer_threshold
        self._shed_threshold = shed_threshold

    def evaluate(self, job: Job, context: AdmissionContext) -> AdmissionDecision:
        pressure = context.system_pressure

        # Load-shedding zone: only CRITICAL survives
        if pressure >= self._shed_threshold:
            if _effective_priority(job) >= Priority.CRITICAL:
                return AdmissionDecision.ACCEPT
            return AdmissionDecision.REJECT

        # Deferral zone: defer low-priority
        if pressure >= self._defer_threshold:
            if _effective_priority(job) <= Priority.LOW:
                return AdmissionDecision.DEFER
            return AdmissionDecision.ACCEPT

        return AdmissionDecision.ACCEPT


class CompositePolicy:
    """Chains multiple policies, returning the *most restrictive* decision.

    Restrictiveness order: REJECT > DEFER > ACCEPT.

    Parameters
    ----------
    policies:
        Ordered sequence of admission policies.  Each is evaluated against
        the same job and context; the worst (most restrictive) decision wins.
    """

    __slots__ = ("_policies",)

    def __init__(self, policies: list[AdmissionPolicy]) -> None:
        if not policies:
            raise ValueError("CompositePolicy requires at least one sub-policy")
        self._policies: list[AdmissionPolicy] = list(policies)

    def evaluate(self, job: Job, context: AdmissionContext) -> AdmissionDecision:
        worst = AdmissionDecision.ACCEPT
        for policy in self._policies:
            decision = policy.evaluate(job, context)
            if _severity(decision) > _severity(worst):
                worst = decision
            # Short-circuit: REJECT is the most restrictive possible
            if worst is AdmissionDecision.REJECT:
                break
        return worst


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_SEVERITY_MAP: dict[AdmissionDecision, int] = {
    AdmissionDecision.ACCEPT: 0,
    AdmissionDecision.DEFER: 1,
    AdmissionDecision.REJECT: 2,
}


def _severity(decision: AdmissionDecision) -> int:
    """Return a numeric severity so decisions can be compared."""
    return _SEVERITY_MAP[decision]


def _effective_priority(job: Job) -> Priority:
    """Normalise the job's priority to a :class:`Priority` enum member."""
    p = job.priority
    if isinstance(p, Priority):
        return p
    return Priority(int(p))
