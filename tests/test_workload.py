"""Tests for the workload models and generator."""

from __future__ import annotations

import time

import pytest

from loadpilot.workload.models import (
    AdmissionDecision,
    EngineConfig,
    Job,
    JobStatus,
    Priority,
    ResourceVector,
    SchedulingDecision,
    Tenant,
)
from loadpilot.workload.generator import (
    WorkloadGenerator,
    WorkloadProfile,
    default_tenants,
    scenario_burst_traffic,
    scenario_mixed_priority,
)


# ==================================================================
# ResourceVector
# ==================================================================


class TestResourceVector:
    def test_add(self):
        a = ResourceVector(cpu=10, memory=100, slots=2)
        b = ResourceVector(cpu=5, memory=50, slots=1)
        c = a + b
        assert c.cpu == 15
        assert c.memory == 150
        assert c.slots == 3

    def test_sub(self):
        a = ResourceVector(cpu=10, memory=100, slots=2)
        b = ResourceVector(cpu=3, memory=30, slots=1)
        c = a - b
        assert c.cpu == 7
        assert c.memory == 70
        assert c.slots == 1

    def test_fits_within(self):
        cost = ResourceVector(cpu=5, memory=50, slots=1)
        cap = ResourceVector(cpu=10, memory=100, slots=2)
        assert cost.fits_within(cap)

    def test_does_not_fit(self):
        cost = ResourceVector(cpu=15, memory=50, slots=1)
        cap = ResourceVector(cpu=10, memory=100, slots=2)
        assert not cost.fits_within(cap)

    def test_utilization_ratio(self):
        used = ResourceVector(cpu=8, memory=50, slots=1)
        cap = ResourceVector(cpu=10, memory=100, slots=2)
        ratio = used.utilization_ratio(cap)
        assert ratio == pytest.approx(0.8)  # 8/10 is the max

    def test_zero(self):
        z = ResourceVector.zero()
        assert z.cpu == 0.0
        assert z.memory == 0.0
        assert z.slots == 0.0


# ==================================================================
# Job
# ==================================================================


class TestJob:
    def test_auto_id(self):
        job = Job(tenant_id="t1")
        assert len(job.id) == 12

    def test_priority_coercion(self):
        job = Job(tenant_id="t1", priority=3)
        assert job.priority == Priority.HIGH

    def test_resource_estimate(self):
        job = Job(tenant_id="t1", estimated_cpu=5, estimated_memory=128)
        rv = job.resource_estimate
        assert rv.cpu == 5
        assert rv.memory == 128
        assert rv.slots == 1.0

    def test_estimated_cost(self):
        job = Job(tenant_id="t1", estimated_cpu=10, estimated_duration=2.0)
        assert job.estimated_cost == 20.0

    def test_is_terminal(self):
        job = Job(tenant_id="t1")
        assert not job.is_terminal
        job.status = JobStatus.COMPLETED
        assert job.is_terminal

    def test_deadline_slack(self):
        now = time.monotonic()
        job = Job(
            tenant_id="t1",
            deadline=now + 10,
            estimated_duration=2.0,
        )
        slack = job.deadline_slack(now)
        assert slack is not None
        assert slack == pytest.approx(8.0, abs=0.1)

    def test_no_deadline_slack(self):
        job = Job(tenant_id="t1")
        assert job.deadline_slack() is None

    def test_trace(self):
        job = Job(tenant_id="t1")
        job.trace("test message")
        assert "test message" in job._decision_trace


# ==================================================================
# Tenant
# ==================================================================


class TestTenant:
    def test_auto_id(self):
        t = Tenant()
        assert len(t.id) == 8

    def test_defaults(self):
        t = Tenant(id="t1")
        assert t.weight == 1.0
        assert t.max_concurrent is None
        assert t.total_submitted == 0


# ==================================================================
# EngineConfig
# ==================================================================


class TestEngineConfig:
    def test_total_capacity(self):
        cfg = EngineConfig(max_workers=50, cpu_budget=100, memory_budget=4096)
        cap = cfg.total_capacity
        assert cap.cpu == 100
        assert cap.memory == 4096
        assert cap.slots == 50


# ==================================================================
# WorkloadGenerator
# ==================================================================


class TestWorkloadGenerator:
    def test_deterministic(self):
        gen1 = WorkloadGenerator(seed=42)
        gen2 = WorkloadGenerator(seed=42)
        profile = WorkloadProfile(tenant_id="t1", num_jobs=50)
        jobs1 = gen1.generate(profile)
        jobs2 = gen2.generate(profile)
        assert len(jobs1) == len(jobs2) == 50
        for j1, j2 in zip(jobs1, jobs2):
            assert j1.id == j2.id
            assert j1.estimated_cpu == j2.estimated_cpu
            assert j1.priority == j2.priority

    def test_job_count(self):
        gen = WorkloadGenerator(seed=1)
        profile = WorkloadProfile(tenant_id="t1", num_jobs=100)
        jobs = gen.generate(profile)
        assert len(jobs) == 100

    def test_deadlines(self):
        gen = WorkloadGenerator(seed=1)
        profile = WorkloadProfile(
            tenant_id="t1",
            num_jobs=100,
            deadline_fraction=1.0,
        )
        jobs = gen.generate(profile)
        assert all(j.deadline is not None for j in jobs)

    def test_multi_tenant(self):
        gen = WorkloadGenerator(seed=1)
        profiles = [
            WorkloadProfile(tenant_id="a", num_jobs=50),
            WorkloadProfile(tenant_id="b", num_jobs=30),
        ]
        jobs = gen.generate_multi_tenant(profiles)
        assert len(jobs) == 80
        tenant_ids = {j.tenant_id for j in jobs}
        assert tenant_ids == {"a", "b"}

    def test_default_tenants(self):
        tenants = default_tenants()
        assert len(tenants) == 3
        assert tenants[0].id == "customer-a"

    def test_scenario_burst(self):
        tenants, jobs = scenario_burst_traffic()
        assert len(tenants) == 3
        assert len(jobs) == 12_500
