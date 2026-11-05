"""
LoadPilot — AdmissionController.

Orchestrates admission decisions by combining system state, tenant metadata,
and the configured admission policy.
"""

from __future__ import annotations

import logging

from loadpilot.admission.policies import (
    AdmissionContext,
    AdmissionPolicy,
    CompositePolicy,
    PressureBasedPolicy,
    QueueLimitPolicy,
    ResourceBasedPolicy,
)
from loadpilot.resources.manager import ResourceManager
from loadpilot.workload.models import (
    AdmissionDecision,
    EngineConfig,
    Job,
    Tenant,
)

_log = logging.getLogger(__name__)


class AdmissionController:
    """Evaluates jobs against the configured admission policy.

    The controller builds an :class:`AdmissionContext` from live system state
    and delegates the actual decision to a pluggable :class:`AdmissionPolicy`.

    Parameters
    ----------
    config:
        Engine-wide configuration — provides queue limits and threshold knobs.
    resource_manager:
        The shared :class:`ResourceManager` used to query available resources.
    """

    __slots__ = (
        "_config",
        "_resource_manager",
        "_policy",
        "_system_pressure",
    )

    def __init__(
        self,
        config: EngineConfig,
        resource_manager: ResourceManager,
    ) -> None:
        self._config: EngineConfig = config
        self._resource_manager: ResourceManager = resource_manager
        self._policy: AdmissionPolicy = CompositePolicy([
            ResourceBasedPolicy(),
            QueueLimitPolicy(),
            PressureBasedPolicy(
                defer_threshold=config.defer_threshold,
                shed_threshold=config.load_shed_threshold,
            ),
        ])
        self._system_pressure: float = 0.0

    # ---- core entry point ------------------------------------------------

    def evaluate(
        self,
        job: Job,
        tenant: Tenant,
        total_queue_size: int = 0,
    ) -> AdmissionDecision:
        """Evaluate *job* for admission, given the owning *tenant*.

        The method:
        1. Constructs an :class:`AdmissionContext` from the resource manager
           and the tenant's counters.
        2. Delegates to the active policy.
        3. Appends a decision trace entry to the job.
        4. Returns the decision.
        """
        context = self._build_context(tenant, total_queue_size)
        decision = self._policy.evaluate(job, context)

        # Trace for observability
        job.trace(
            f"admission={decision.value} "
            f"pressure={context.system_pressure:.2f} "
            f"queue={context.total_queue_size}/{context.max_queue_size} "
            f"tenant_q={context.tenant_queue_size} "
            f"tenant_running={context.tenant_running_count}"
        )

        _log.debug(
            "Job %s (%s) → %s  [pressure=%.2f]",
            job.id,
            job.tenant_id,
            decision.value,
            self._system_pressure,
        )
        return decision

    # ---- control knobs ---------------------------------------------------

    def set_pressure(self, pressure: float) -> None:
        """Update the system-wide pressure metric.

        Parameters
        ----------
        pressure:
            A float clamped to [0.0, 1.0].
        """
        self._system_pressure = max(0.0, min(1.0, pressure))

    def set_policy(self, policy: AdmissionPolicy) -> None:
        """Hot-swap the active admission policy."""
        self._policy = policy
        _log.info("Admission policy replaced with %s", type(policy).__name__)

    # ---- internal --------------------------------------------------------

    def _build_context(self, tenant: Tenant, total_queue_size: int) -> AdmissionContext:
        """Construct an :class:`AdmissionContext` snapshot."""
        return AdmissionContext(
            system_pressure=self._system_pressure,
            resource_available=self._resource_manager.available,
            tenant_queue_size=tenant.current_queued,
            tenant_running_count=tenant.current_running,
            tenant_max_queue=tenant.max_queue_size,
            tenant_max_concurrent=tenant.max_concurrent,
            total_queue_size=total_queue_size,
            max_queue_size=self._config.max_queue_size,
        )
