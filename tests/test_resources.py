"""Tests for global resource tracking and per-tenant budgets."""

from __future__ import annotations

from loadpilot.resources.budget import TenantBudget
from loadpilot.resources.manager import ResourceManager
from loadpilot.workload.models import Job, ResourceVector


class TestTenantBudget:
    def test_reserve_and_release(self):
        budget = TenantBudget(
            tenant_id="tenant",
            ceiling=ResourceVector(cpu=4, memory=100, slots=2),
            current_usage=ResourceVector.zero(),
        )
        cost = ResourceVector(cpu=2, memory=40, slots=1)

        assert budget.can_afford(cost)
        assert budget.reserve(cost)
        assert budget.current_usage == cost
        assert budget.utilization() == 0.5

        budget.release(cost)
        assert budget.current_usage == ResourceVector.zero()

    def test_rejects_over_budget_without_changing_usage(self):
        budget = TenantBudget(
            tenant_id="tenant",
            ceiling=ResourceVector(cpu=1, memory=100, slots=1),
            current_usage=ResourceVector.zero(),
        )
        cost = ResourceVector(cpu=2, memory=10, slots=1)

        assert not budget.reserve(cost)
        assert budget.current_usage == ResourceVector.zero()


class TestResourceManager:
    def test_reservation_and_release_lifecycle(self):
        manager = ResourceManager(ResourceVector(cpu=8, memory=256, slots=2))
        manager.register_tenant(
            "tenant",
            ResourceVector(cpu=4, memory=128, slots=2),
        )
        job = Job(
            id="job",
            tenant_id="tenant",
            estimated_cpu=2,
            estimated_memory=64,
        )

        assert manager.can_admit(job)
        assert manager.reserve(job)
        assert manager.available == ResourceVector(cpu=6, memory=192, slots=1)
        assert manager.get_tenant_utilization("tenant") == 0.5
        assert manager.snapshot()["active_reservations"] == 1

        manager.release(job)
        manager.release(job)
        assert manager.available == ResourceVector(cpu=8, memory=256, slots=2)
        assert manager.get_tenant_utilization("tenant") == 0.0
        assert manager.snapshot()["active_reservations"] == 0

    def test_global_capacity_rejection_does_not_reserve(self):
        manager = ResourceManager(ResourceVector(cpu=1, memory=64, slots=1))
        job = Job(id="too-large", tenant_id="tenant", estimated_cpu=2)

        assert not manager.can_admit(job)
        assert not manager.reserve(job)
        assert manager.available == ResourceVector(cpu=1, memory=64, slots=1)

    def test_tenant_budget_rejection_does_not_reserve_globally(self):
        manager = ResourceManager(ResourceVector(cpu=8, memory=256, slots=4))
        manager.register_tenant(
            "tenant",
            ResourceVector(cpu=1, memory=256, slots=4),
        )
        job = Job(id="over-budget", tenant_id="tenant", estimated_cpu=2)

        assert not manager.can_admit(job)
        assert not manager.reserve(job)
        assert manager.available == ResourceVector(cpu=8, memory=256, slots=4)
        assert manager.get_tenant_utilization("tenant") == 0.0

    def test_capacity_reduction_preserves_existing_reservations(self):
        manager = ResourceManager(ResourceVector(cpu=4, memory=128, slots=2))
        job = Job(id="active", tenant_id="tenant", estimated_cpu=2)
        assert manager.reserve(job)

        manager.update_capacity(ResourceVector(cpu=1, memory=64, slots=1))

        assert manager.available == ResourceVector.zero()
        manager.release(job)
        assert manager.available == ResourceVector(cpu=1, memory=64, slots=1)
