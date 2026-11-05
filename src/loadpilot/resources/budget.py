"""
LoadPilot — Per-tenant resource budgets.

Tracks resource ceilings and current usage for each tenant, enforcing that
no tenant can consume more than its allocated budget across all dimensions.
"""

from __future__ import annotations

from dataclasses import dataclass

from loadpilot.workload.models import ResourceVector


@dataclass(slots=True)
class TenantBudget:
    """Per-tenant resource budget with ceiling enforcement.

    Parameters
    ----------
    tenant_id:
        Unique identifier of the owning tenant.
    ceiling:
        Maximum resources this tenant is allowed to consume.
    current_usage:
        Resources currently reserved by running / admitted jobs of this tenant.
    """

    tenant_id: str
    ceiling: ResourceVector
    current_usage: ResourceVector

    # ---- computed helpers ------------------------------------------------

    @property
    def available(self) -> ResourceVector:
        """Return the remaining budget (ceiling − current_usage).

        Values are clamped to zero so callers never see negative availability
        even if usage has been (transiently) over-reported.
        """
        diff = self.ceiling - self.current_usage
        return ResourceVector(
            cpu=max(diff.cpu, 0.0),
            memory=max(diff.memory, 0.0),
            slots=max(diff.slots, 0.0),
        )

    # ---- query -----------------------------------------------------------

    def can_afford(self, cost: ResourceVector) -> bool:
        """Return ``True`` if *cost* fits within the remaining budget."""
        return cost.fits_within(self.available)

    # ---- mutations -------------------------------------------------------

    def reserve(self, cost: ResourceVector) -> bool:
        """Try to reserve *cost* from this budget.

        Returns ``True`` on success.  If the cost exceeds the available budget
        the call is a no-op and ``False`` is returned.
        """
        if not self.can_afford(cost):
            return False
        self.current_usage = self.current_usage + cost
        return True

    def release(self, cost: ResourceVector) -> None:
        """Release *cost* back into the budget.

        Usage is clamped at zero so a double-release cannot drive the
        counter negative.
        """
        diff = self.current_usage - cost
        self.current_usage = ResourceVector(
            cpu=max(diff.cpu, 0.0),
            memory=max(diff.memory, 0.0),
            slots=max(diff.slots, 0.0),
        )

    # ---- observability ---------------------------------------------------

    def utilization(self) -> float:
        """Return the ratio of the most-utilised resource dimension.

        A value of 1.0 means at least one dimension is fully consumed.
        Returns 0.0 when the ceiling is all-zero (no budget set).
        """
        return self.current_usage.utilization_ratio(self.ceiling)
