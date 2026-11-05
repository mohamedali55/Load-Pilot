"""
LoadPilot — Deficit Round Robin scheduler.

Implements weighted fair-share scheduling via per-tenant deficit counters.
Each tenant is assigned a quantum (token budget per round); jobs are dispatched
when the accumulated deficit is large enough to cover their estimated cost.
"""

from __future__ import annotations

from collections import deque
from typing import Iterator


from loadpilot.workload.models import Job


class DeficitRoundRobin:
    """Deficit Round Robin (DRR) scheduler.

    Parameters
    ----------
    quanta : dict[str, float]
        Mapping of ``tenant_id`` → ``quantum``.  The quantum is the number of
        cost units added to a tenant's deficit counter on each round.
    """

    __slots__ = ("_quanta", "_deficits", "_order")

    def __init__(self, quanta: dict[str, float]) -> None:
        self._quanta: dict[str, float] = dict(quanta)
        self._deficits: dict[str, float] = {tid: 0.0 for tid in quanta}
        # Maintain a stable iteration order (insertion order).
        self._order: list[str] = list(quanta.keys())

    # ------------------------------------------------------------------
    # Core scheduling
    # ------------------------------------------------------------------

    def select_next(self, queues: dict[str, deque[Job]]) -> Job | None:
        """Select the next job eligible for execution.

        The algorithm performs a single round-robin pass over all tenants:

        1. Add the tenant's quantum to its deficit counter.
        2. Pop jobs from the tenant's queue while the deficit can cover the
           job's ``estimated_cost``.
        3. If the queue becomes empty, reset the tenant's deficit to 0.
        4. Return the *first* eligible job found, or ``None`` if no tenant
           has an affordable job in this round.
        """
        if not self._order:
            return None

        # We iterate through all tenants starting from the current position.
        # A full pass is one "round".
        for tenant_id in self._round_robin():
            if tenant_id not in self._quanta:
                continue

            queue = queues.get(tenant_id)
            if queue is None or len(queue) == 0:
                # No work queued — reset deficit so this tenant does not
                # accumulate unbounded credit while idle.
                self._deficits[tenant_id] = 0.0
                continue

            # Credit the tenant.
            self._deficits[tenant_id] = self._deficits.get(tenant_id, 0.0) + self._quanta[tenant_id]

            # Try to dispatch the head-of-line job.
            head = queue[0]
            cost = head.estimated_cost
            if self._deficits[tenant_id] >= cost:
                job = queue.popleft()
                self._deficits[tenant_id] -= cost

                # If the queue is now empty, reset the deficit.
                if len(queue) == 0:
                    self._deficits[tenant_id] = 0.0

                return job

        return None

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Reset all deficit counters to zero."""
        for tid in self._deficits:
            self._deficits[tid] = 0.0

    def update_quantum(self, tenant_id: str, quantum: float) -> None:
        """Dynamically adjust the quantum for *tenant_id*.

        Raises
        ------
        KeyError
            If *tenant_id* is not registered.
        ValueError
            If *quantum* is not positive.
        """
        if tenant_id not in self._quanta:
            raise KeyError(f"Tenant {tenant_id!r} is not registered")
        if quantum <= 0:
            raise ValueError(f"quantum must be positive, got {quantum!r}")
        self._quanta[tenant_id] = quantum

    def add_tenant(self, tenant_id: str, quantum: float) -> None:
        """Register a new tenant with the given *quantum*.

        If the tenant already exists, its quantum is updated.

        Raises
        ------
        ValueError
            If *quantum* is not positive.
        """
        if quantum <= 0:
            raise ValueError(f"quantum must be positive, got {quantum!r}")
        if tenant_id not in self._quanta:
            self._order.append(tenant_id)
        self._quanta[tenant_id] = quantum
        self._deficits.setdefault(tenant_id, 0.0)

    def remove_tenant(self, tenant_id: str) -> None:
        """Unregister a tenant.

        Raises
        ------
        KeyError
            If *tenant_id* is not registered.
        """
        if tenant_id not in self._quanta:
            raise KeyError(f"Tenant {tenant_id!r} is not registered")
        del self._quanta[tenant_id]
        self._deficits.pop(tenant_id, None)
        try:
            self._order.remove(tenant_id)
        except ValueError:
            pass

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _round_robin(self) -> Iterator[str]:
        """Yield tenant IDs in round-robin order for one full pass."""
        yield from self._order

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def tenants(self) -> list[str]:
        """Return list of registered tenant IDs in iteration order."""
        return list(self._order)

    @property
    def deficits(self) -> dict[str, float]:
        """Return a snapshot of the current deficit counters."""
        return dict(self._deficits)
