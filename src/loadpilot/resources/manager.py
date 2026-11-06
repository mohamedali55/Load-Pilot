"""
LoadPilot — ResourceManager.

Centralised resource tracking for the scheduling engine.  Maintains a global
capacity pool plus per-tenant budgets, and exposes reserve / release semantics
used by the admission controller and the scheduler.
"""

from __future__ import annotations

import logging
from typing import Any

from loadpilot.resources.budget import TenantBudget
from loadpilot.workload.models import Job, ResourceVector

_log = logging.getLogger(__name__)


class ResourceManager:
    """Manages global and per-tenant resource pools.

    Parameters
    ----------
    total_capacity:
        The system-wide resource capacity (cpu, memory, slots).
    """

    __slots__ = (
        "_total",
        "_reserved",
        "_tenant_budgets",
        "_reservations",
    )

    def __init__(self, total_capacity: ResourceVector) -> None:
        self._total: ResourceVector = total_capacity
        self._reserved: ResourceVector = ResourceVector.zero()
        self._tenant_budgets: dict[str, TenantBudget] = {}
        self._reservations: dict[str, ResourceVector] = {}  # job_id → reserved

    # ---- computed properties ---------------------------------------------

    @property
    def available(self) -> ResourceVector:
        """Return system-wide available resources (total − reserved).

        Values are clamped to zero for safety.
        """
        diff = self._total - self._reserved
        return ResourceVector(
            cpu=max(diff.cpu, 0.0),
            memory=max(diff.memory, 0.0),
            slots=max(diff.slots, 0.0),
        )

    @property
    def total_capacity(self) -> ResourceVector:
        """Return configured system capacity before active reservations."""
        return self._total

    @property
    def utilization(self) -> float:
        """Max utilisation ratio across all resource dimensions."""
        return self._reserved.utilization_ratio(self._total)

    # ---- tenant management -----------------------------------------------

    def register_tenant(self, tenant_id: str, budget: ResourceVector | None) -> None:
        """Register (or update) a tenant's resource budget.

        Parameters
        ----------
        tenant_id:
            Unique tenant identifier.
        budget:
            Per-tenant resource ceiling.  When ``None`` the tenant is given
            the full system capacity as its ceiling (i.e. no per-tenant cap).
        """
        ceiling = budget if budget is not None else self._total
        if tenant_id in self._tenant_budgets:
            existing = self._tenant_budgets[tenant_id]
            existing.ceiling = ceiling
            _log.debug("Updated budget for tenant %s: %s", tenant_id, ceiling)
        else:
            self._tenant_budgets[tenant_id] = TenantBudget(
                tenant_id=tenant_id,
                ceiling=ceiling,
                current_usage=ResourceVector.zero(),
            )
            _log.debug("Registered tenant %s with budget %s", tenant_id, ceiling)

    # ---- admission queries -----------------------------------------------

    def can_admit(self, job: Job) -> bool:
        """Check whether *job* can be admitted given global + tenant capacity.

        This is a read-only probe — no resources are reserved.
        """
        cost = job.resource_estimate

        # Global capacity check
        if not cost.fits_within(self.available):
            return False

        # Tenant budget check (if the tenant has a registered budget)
        tb = self._tenant_budgets.get(job.tenant_id)
        if tb is not None and not tb.can_afford(cost):
            return False

        return True

    # ---- reserve / release -----------------------------------------------

    def reserve(self, job: Job) -> bool:
        """Reserve resources for *job*.

        Returns ``True`` on success.  On failure (insufficient resources) the
        call is a no-op and ``False`` is returned.
        """
        cost = job.resource_estimate

        # Guard: prevent double reservation
        if job.id in self._reservations:
            _log.warning("Job %s already has a reservation — skipping", job.id)
            return True

        # Global capacity check
        if not cost.fits_within(self.available):
            _log.debug(
                "Reserve rejected for job %s: insufficient global resources", job.id
            )
            return False

        # Tenant budget check + reserve
        tb = self._tenant_budgets.get(job.tenant_id)
        if tb is not None:
            if not tb.reserve(cost):
                _log.debug(
                    "Reserve rejected for job %s: tenant %s over budget",
                    job.id,
                    job.tenant_id,
                )
                return False

        # Commit global reservation
        self._reserved = self._reserved + cost
        self._reservations[job.id] = cost
        _log.debug("Reserved %s for job %s", cost, job.id)
        return True

    def release(self, job: Job) -> None:
        """Release resources previously reserved for *job*.

        Safe to call even if the job has no active reservation (idempotent).
        """
        cost = self._reservations.pop(job.id, None)
        if cost is None:
            _log.debug("No reservation found for job %s — nothing to release", job.id)
            return

        # Release global
        diff = self._reserved - cost
        self._reserved = ResourceVector(
            cpu=max(diff.cpu, 0.0),
            memory=max(diff.memory, 0.0),
            slots=max(diff.slots, 0.0),
        )

        # Release tenant
        tb = self._tenant_budgets.get(job.tenant_id)
        if tb is not None:
            tb.release(cost)

        _log.debug("Released %s for job %s", cost, job.id)

    # ---- dynamic capacity ------------------------------------------------

    def update_capacity(self, new_capacity: ResourceVector) -> None:
        """Update the total system capacity (e.g. for adaptive concurrency control).

        If the new capacity is smaller than the current reservation the
        reservation is NOT forcibly reduced — the system is simply
        over-committed until jobs complete.
        """
        old = self._total
        self._total = new_capacity
        _log.info("System capacity updated from %s to %s", old, new_capacity)

    # ---- tenant observability --------------------------------------------

    def get_tenant_utilization(self, tenant_id: str) -> float:
        """Return the peak dimension utilisation for *tenant_id*.

        Returns 0.0 if the tenant has no registered budget.
        """
        tb = self._tenant_budgets.get(tenant_id)
        if tb is None:
            return 0.0
        return tb.utilization()

    # ---- telemetry -------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-friendly snapshot of the current resource state."""
        return {
            "total": {
                "cpu": self._total.cpu,
                "memory": self._total.memory,
                "slots": self._total.slots,
            },
            "reserved": {
                "cpu": self._reserved.cpu,
                "memory": self._reserved.memory,
                "slots": self._reserved.slots,
            },
            "available": {
                "cpu": self.available.cpu,
                "memory": self.available.memory,
                "slots": self.available.slots,
            },
            "utilization": self.utilization,
            "active_reservations": len(self._reservations),
            "tenants": {
                tid: {
                    "ceiling": {
                        "cpu": tb.ceiling.cpu,
                        "memory": tb.ceiling.memory,
                        "slots": tb.ceiling.slots,
                    },
                    "usage": {
                        "cpu": tb.current_usage.cpu,
                        "memory": tb.current_usage.memory,
                        "slots": tb.current_usage.slots,
                    },
                    "utilization": tb.utilization(),
                }
                for tid, tb in self._tenant_budgets.items()
            },
        }
