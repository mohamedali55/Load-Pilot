"""Tests for admission policies and engine admission integration."""

from __future__ import annotations

from loadpilot.admission.controller import AdmissionController
from loadpilot.admission.policies import (
    AdmissionContext,
    CompositePolicy,
    PressureBasedPolicy,
    QueueLimitPolicy,
    ResourceBasedPolicy,
)
from loadpilot.engine import Engine
from loadpilot.resources.manager import ResourceManager
from loadpilot.workload.models import (
    AdmissionDecision,
    EngineConfig,
    Job,
    Priority,
    ResourceVector,
    Tenant,
)


def _context(**overrides) -> AdmissionContext:
    values = {
        "system_pressure": 0.0,
        "resource_available": ResourceVector(cpu=8, memory=256, slots=4),
        "tenant_queue_size": 0,
        "tenant_running_count": 0,
        "tenant_max_queue": None,
        "tenant_max_concurrent": None,
        "total_queue_size": 0,
        "max_queue_size": 10,
    }
    values.update(overrides)
    return AdmissionContext(**values)


class TestAdmissionPolicies:
    def test_resource_limit_rejects_job(self):
        job = Job(id="large", tenant_id="tenant", estimated_cpu=9)
        assert (
            ResourceBasedPolicy().evaluate(job, _context())
            is AdmissionDecision.REJECT
        )

    def test_queue_and_concurrency_limits(self):
        policy = QueueLimitPolicy()
        job = Job(id="job", tenant_id="tenant")

        assert (
            policy.evaluate(job, _context(total_queue_size=10))
            is AdmissionDecision.REJECT
        )
        assert (
            policy.evaluate(
                job,
                _context(tenant_running_count=2, tenant_max_concurrent=2),
            )
            is AdmissionDecision.DEFER
        )
        assert (
            policy.evaluate(
                job,
                _context(tenant_queue_size=4, tenant_max_queue=5),
            )
            is AdmissionDecision.DEFER
        )

    def test_pressure_policy_sheds_noncritical(self):
        policy = PressureBasedPolicy(defer_threshold=0.8, shed_threshold=0.95)
        low_priority = Job(id="low", tenant_id="tenant", priority=Priority.LOW)
        critical = Job(id="critical", tenant_id="tenant", priority=Priority.CRITICAL)

        assert (
            policy.evaluate(low_priority, _context(system_pressure=0.85))
            is AdmissionDecision.DEFER
        )
        assert (
            policy.evaluate(low_priority, _context(system_pressure=0.99))
            is AdmissionDecision.REJECT
        )
        assert (
            policy.evaluate(critical, _context(system_pressure=0.99))
            is AdmissionDecision.ACCEPT
        )

    def test_composite_returns_most_restrictive_decision(self):
        policy = CompositePolicy([
            ResourceBasedPolicy(),
            QueueLimitPolicy(),
        ])
        job = Job(id="job", tenant_id="tenant")

        assert (
            policy.evaluate(
                job,
                _context(tenant_running_count=1, tenant_max_concurrent=1),
            )
            is AdmissionDecision.DEFER
        )
        assert (
            policy.evaluate(job, _context(total_queue_size=10))
            is AdmissionDecision.REJECT
        )


class TestAdmissionController:
    def test_passes_total_queue_size_into_policy_context(self):
        config = EngineConfig(max_queue_size=2)
        manager = ResourceManager(config.total_capacity)
        manager.register_tenant("tenant", None)
        controller = AdmissionController(config, manager)
        job = Job(id="job", tenant_id="tenant")

        assert (
            controller.evaluate(job, Tenant(id="tenant"), total_queue_size=1)
            is AdmissionDecision.ACCEPT
        )
        assert (
            controller.evaluate(job, Tenant(id="tenant"), total_queue_size=2)
            is AdmissionDecision.REJECT
        )

    def test_engine_enforces_system_queue_limit(self):
        engine = Engine(config=EngineConfig(max_queue_size=1))

        first = engine.submit(Job(id="first", tenant_id="tenant"))
        second = engine.submit(Job(id="second", tenant_id="tenant"))

        assert first is AdmissionDecision.ACCEPT
        assert second is AdmissionDecision.REJECT
        assert engine.snapshot()["pending_jobs"] == 1
