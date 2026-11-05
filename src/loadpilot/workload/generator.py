"""
LoadPilot — Synthetic workload generator.

Produces deterministic, reproducible workload traces for testing and benchmarking.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Iterator

from loadpilot.workload.models import Job, Priority, Tenant


@dataclass(slots=True)
class WorkloadProfile:
    """Describes the statistical shape of a synthetic workload."""

    tenant_id: str = "default"
    num_jobs: int = 100
    priority_distribution: dict[Priority, float] = field(default_factory=lambda: {
        Priority.LOW: 0.2,
        Priority.NORMAL: 0.5,
        Priority.HIGH: 0.2,
        Priority.CRITICAL: 0.1,
    })

    # cost distributions (uniform between min and max)
    cpu_range: tuple[float, float] = (1.0, 20.0)
    memory_range: tuple[float, float] = (32.0, 512.0)
    duration_range: tuple[float, float] = (0.1, 5.0)

    # deadline: fraction of jobs that have a deadline
    deadline_fraction: float = 0.3
    deadline_slack_range: tuple[float, float] = (5.0, 60.0)  # seconds of slack

    # burst: fraction of jobs submitted simultaneously (rest are spread)
    burst_fraction: float = 0.0
    inter_arrival_range: tuple[float, float] = (0.01, 0.1)  # seconds between jobs

    # retry
    max_retries: int = 1

    # dependencies: fraction of jobs that depend on a previous job
    dependency_fraction: float = 0.0


class WorkloadGenerator:
    """
    Deterministic workload generator.

    Uses a seeded RNG so that the same seed produces exactly the same trace.
    """

    def __init__(self, seed: int = 42) -> None:
        self._rng = random.Random(seed)

    def generate(self, profile: WorkloadProfile) -> list[Job]:
        """Generate a batch of jobs from a *profile*."""
        jobs: list[Job] = []
        generated_ids: list[str] = []
        priorities = list(profile.priority_distribution.keys())
        weights = list(profile.priority_distribution.values())

        for i in range(profile.num_jobs):
            job_id = f"{profile.tenant_id}-{i:06d}"
            priority = self._rng.choices(priorities, weights=weights, k=1)[0]

            cpu = self._rng.uniform(*profile.cpu_range)
            memory = self._rng.uniform(*profile.memory_range)
            duration = self._rng.uniform(*profile.duration_range)

            deadline = None
            if self._rng.random() < profile.deadline_fraction:
                slack = self._rng.uniform(*profile.deadline_slack_range)
                deadline = time.monotonic() + duration + slack

            depends_on: list[str] = []
            if (
                generated_ids
                and self._rng.random() < profile.dependency_fraction
            ):
                dep = self._rng.choice(generated_ids)
                depends_on = [dep]

            job = Job(
                id=job_id,
                tenant_id=profile.tenant_id,
                estimated_cpu=round(cpu, 2),
                estimated_memory=round(memory, 2),
                estimated_duration=round(duration, 3),
                priority=priority,
                deadline=deadline,
                max_retries=profile.max_retries,
                depends_on=depends_on,
            )
            jobs.append(job)
            generated_ids.append(job_id)

        return jobs

    def generate_burst(
        self,
        profile: WorkloadProfile,
        burst_size: int | None = None,
    ) -> list[Job]:
        """
        Generate jobs where *burst_size* arrive simultaneously and the rest
        are spaced out.
        """
        jobs = self.generate(profile)
        if burst_size is None:
            burst_size = int(len(jobs) * max(profile.burst_fraction, 0.5))

        now = time.monotonic()
        for i, job in enumerate(jobs):
            if i < burst_size:
                job.submit_time = now
            else:
                gap = self._rng.uniform(*profile.inter_arrival_range)
                now += gap
                job.submit_time = now
        return jobs

    def generate_multi_tenant(
        self,
        profiles: list[WorkloadProfile],
    ) -> list[Job]:
        """Generate interleaved jobs from multiple tenant profiles."""
        all_jobs: list[Job] = []
        for profile in profiles:
            all_jobs.extend(self.generate(profile))
        self._rng.shuffle(all_jobs)
        return all_jobs

    def stream(self, profile: WorkloadProfile) -> Iterator[Job]:
        """Yield jobs one at a time (for streaming simulation)."""
        yield from self.generate(profile)


def default_tenants() -> list[Tenant]:
    """Return a standard set of tenants for benchmarking."""
    return [
        Tenant(id="customer-a", weight=2.0, max_concurrent=30),
        Tenant(id="customer-b", weight=1.0, max_concurrent=20),
        Tenant(id="customer-c", weight=1.5, max_concurrent=25),
    ]


def scenario_burst_traffic(seed: int = 42) -> tuple[list[Tenant], list[Job]]:
    """Customer A submits a massive burst; B and C submit normally."""
    gen = WorkloadGenerator(seed)
    tenants = default_tenants()
    profiles = [
        WorkloadProfile(
            tenant_id="customer-a",
            num_jobs=10_000,
            cpu_range=(5.0, 30.0),
            memory_range=(128.0, 1024.0),
            duration_range=(1.0, 10.0),
            burst_fraction=0.8,
        ),
        WorkloadProfile(
            tenant_id="customer-b",
            num_jobs=500,
            cpu_range=(1.0, 5.0),
            memory_range=(32.0, 128.0),
            duration_range=(0.1, 1.0),
            priority_distribution={
                Priority.LOW: 0.0,
                Priority.NORMAL: 0.1,
                Priority.HIGH: 0.6,
                Priority.CRITICAL: 0.3,
            },
            deadline_fraction=0.7,
            deadline_slack_range=(2.0, 15.0),
        ),
        WorkloadProfile(
            tenant_id="customer-c",
            num_jobs=2_000,
            cpu_range=(3.0, 15.0),
            memory_range=(64.0, 512.0),
            duration_range=(0.5, 5.0),
        ),
    ]
    jobs = gen.generate_multi_tenant(profiles)
    return tenants, jobs


def scenario_mixed_priority(seed: int = 42) -> tuple[list[Tenant], list[Job]]:
    """All tenants submit mixed-priority work with deadlines."""
    gen = WorkloadGenerator(seed)
    tenants = default_tenants()
    profiles = [
        WorkloadProfile(
            tenant_id=t.id,
            num_jobs=1000,
            deadline_fraction=0.5,
            dependency_fraction=0.1,
        )
        for t in tenants
    ]
    jobs = gen.generate_multi_tenant(profiles)
    return tenants, jobs
