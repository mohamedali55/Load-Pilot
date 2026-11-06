"""
LoadPilot — Synthetic workload generator.

Produces deterministic, reproducible workload traces for testing and benchmarking.
"""

from __future__ import annotations

import math
import random
import time
from dataclasses import dataclass, field
from typing import Iterator, Literal

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
    max_retries: int | None = None

    # Arrival process for simulation traces.
    arrival_pattern: Literal["bulk", "uniform", "poisson", "burst"] = "bulk"
    arrival_rate: float = 10.0
    arrival_duration: float = 10.0

    # dependencies: fraction of jobs that depend on a previous job
    dependency_fraction: float = 0.0

    def __post_init__(self) -> None:
        if self.num_jobs < 0:
            raise ValueError("num_jobs must be non-negative")
        if self.max_retries is not None and self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.arrival_pattern not in {"bulk", "uniform", "poisson", "burst"}:
            raise ValueError(f"Unknown arrival pattern: {self.arrival_pattern!r}")
        if not math.isfinite(self.arrival_rate) or self.arrival_rate <= 0:
            raise ValueError("arrival_rate must be finite and positive")
        if not math.isfinite(self.arrival_duration) or self.arrival_duration < 0:
            raise ValueError("arrival_duration must be finite and non-negative")
        if not 0.0 <= self.burst_fraction <= 1.0:
            raise ValueError("burst_fraction must be between 0 and 1")


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
        arrival_offsets = self._arrival_offsets(profile)
        base_time = time.monotonic()
        priorities = list(profile.priority_distribution.keys())
        weights = list(profile.priority_distribution.values())

        for i in range(profile.num_jobs):
            job_id = f"{profile.tenant_id}-{i:06d}"
            priority = self._rng.choices(priorities, weights=weights, k=1)[0]

            cpu = self._rng.uniform(*profile.cpu_range)
            memory = self._rng.uniform(*profile.memory_range)
            duration = self._rng.uniform(*profile.duration_range)

            deadline = None
            deadline_slack = None
            if self._rng.random() < profile.deadline_fraction:
                deadline_slack = self._rng.uniform(*profile.deadline_slack_range)
                deadline = (
                    base_time
                    + arrival_offsets[i]
                    + duration
                    + deadline_slack
                )

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
                metadata={
                    "arrival_offset": arrival_offsets[i],
                    "deadline_slack": deadline_slack,
                },
            )
            job.submit_time = base_time + arrival_offsets[i]
            jobs.append(job)
            generated_ids.append(job_id)

        return jobs

    def _arrival_offsets(self, profile: WorkloadProfile) -> list[float]:
        """Generate deterministic arrival offsets for the selected process."""
        count = profile.num_jobs
        if profile.arrival_pattern == "bulk":
            return [0.0] * count
        if profile.arrival_pattern == "uniform":
            return sorted(
                self._rng.uniform(0.0, profile.arrival_duration)
                for _ in range(count)
            )
        if profile.arrival_pattern == "poisson":
            offsets: list[float] = []
            arrival = 0.0
            for _ in range(count):
                arrival += self._rng.expovariate(profile.arrival_rate)
                offsets.append(arrival)
            return offsets

        burst_count = min(count, int(count * profile.burst_fraction))
        offsets = [0.0] * burst_count
        arrival = 0.0
        for _ in range(count - burst_count):
            arrival += self._rng.uniform(*profile.inter_arrival_range)
            offsets.append(arrival)
        return offsets

    def generate_burst(
        self,
        profile: WorkloadProfile,
        burst_size: int | None = None,
    ) -> list[Job]:
        """
        Generate jobs where *burst_size* arrive simultaneously and the rest
        are spaced out.
        """
        burst_fraction = (
            max(profile.burst_fraction, 0.5)
            if burst_size is None
            else min(1.0, max(0.0, burst_size / max(profile.num_jobs, 1)))
        )
        burst_profile = WorkloadProfile(
            tenant_id=profile.tenant_id,
            num_jobs=profile.num_jobs,
            priority_distribution=profile.priority_distribution,
            cpu_range=profile.cpu_range,
            memory_range=profile.memory_range,
            duration_range=profile.duration_range,
            deadline_fraction=profile.deadline_fraction,
            deadline_slack_range=profile.deadline_slack_range,
            burst_fraction=burst_fraction,
            inter_arrival_range=profile.inter_arrival_range,
            max_retries=profile.max_retries,
            arrival_pattern="burst",
            arrival_rate=profile.arrival_rate,
            arrival_duration=profile.arrival_duration,
            dependency_fraction=profile.dependency_fraction,
        )
        return self.generate(burst_profile)

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
