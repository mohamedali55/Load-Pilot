"""
LoadPilot — Simulation scenarios.

Pre-built scenarios that stress-test the engine under specific conditions:
burst traffic, tenant monopolisation, capacity reduction, deadline pressure,
mixed workloads, and cascading failures.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loadpilot.workload.models import EngineConfig, Priority, Tenant
from loadpilot.workload.generator import WorkloadGenerator, WorkloadProfile


class ScenarioKind(Enum):
    BURST = "burst"
    MONOPOLISATION = "monopolisation"
    CAPACITY_REDUCTION = "capacity_reduction"
    DEADLINE_PRESSURE = "deadline_pressure"
    MIXED = "mixed"
    EXPENSIVE_JOBS = "expensive_jobs"


@dataclass(slots=True)
class ScenarioEvent:
    """
    A timed event injected during simulation.

    Examples: capacity reduction, tenant spike, worker failure.
    """

    time_offset: float               # seconds from simulation start
    event_type: str                   # e.g. "reduce_capacity", "add_burst", "fail_workers"
    params: dict[str, Any] = field(default_factory=dict)
    description: str = ""


@dataclass(slots=True)
class Scenario:
    """
    A complete simulation scenario.

    Contains tenant definitions, workload profiles, engine config overrides,
    and timed events.
    """

    name: str
    kind: ScenarioKind
    description: str
    tenants: list[Tenant]
    profiles: list[WorkloadProfile]
    config: EngineConfig
    events: list[ScenarioEvent] = field(default_factory=list)
    seed: int = 42
    duration: float = 60.0           # seconds


def scenario_burst_traffic() -> Scenario:
    """
    Customer A submits 10,000 expensive jobs in a burst.
    Customer B submits 500 small, high-priority, deadline-sensitive jobs.
    Customer C submits 2,000 medium jobs normally.

    Tests: fairness under burst, deadline handling, admission control.
    """
    tenants = [
        Tenant(id="customer-a", weight=2.0, max_concurrent=30),
        Tenant(id="customer-b", weight=1.0, max_concurrent=20),
        Tenant(id="customer-c", weight=1.5, max_concurrent=25),
    ]
    profiles = [
        WorkloadProfile(
            tenant_id="customer-a",
            num_jobs=10_000,
            cpu_range=(5.0, 30.0),
            memory_range=(128.0, 1024.0),
            duration_range=(1.0, 10.0),
            burst_fraction=0.8,
            priority_distribution={
                Priority.LOW: 0.3,
                Priority.NORMAL: 0.5,
                Priority.HIGH: 0.15,
                Priority.CRITICAL: 0.05,
            },
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
    return Scenario(
        name="burst_traffic",
        kind=ScenarioKind.BURST,
        description=(
            "Customer A submits a massive burst of expensive jobs while "
            "Customers B and C submit smaller, time-sensitive workloads."
        ),
        tenants=tenants,
        profiles=profiles,
        config=EngineConfig(max_workers=100, cpu_budget=200, memory_budget=8192),
        duration=120.0,
    )


def scenario_monopolisation() -> Scenario:
    """
    One tenant submits 50x more work than the others.

    Tests: DRR fairness, tenant isolation, starvation prevention.
    """
    tenants = [
        Tenant(id="monopolist", weight=1.0, max_concurrent=40),
        Tenant(id="small-a", weight=1.0, max_concurrent=20),
        Tenant(id="small-b", weight=1.0, max_concurrent=20),
    ]
    profiles = [
        WorkloadProfile(
            tenant_id="monopolist",
            num_jobs=50_000,
            cpu_range=(1.0, 10.0),
            memory_range=(32.0, 256.0),
            duration_range=(0.5, 3.0),
        ),
        WorkloadProfile(
            tenant_id="small-a",
            num_jobs=1_000,
            cpu_range=(1.0, 5.0),
            memory_range=(32.0, 128.0),
            duration_range=(0.1, 1.0),
        ),
        WorkloadProfile(
            tenant_id="small-b",
            num_jobs=1_000,
            cpu_range=(1.0, 5.0),
            memory_range=(32.0, 128.0),
            duration_range=(0.1, 1.0),
        ),
    ]
    return Scenario(
        name="monopolisation",
        kind=ScenarioKind.MONOPOLISATION,
        description="One tenant submits 50x more jobs than others with equal weight.",
        tenants=tenants,
        profiles=profiles,
        config=EngineConfig(max_workers=80, cpu_budget=160, memory_budget=4096),
        duration=180.0,
    )


def scenario_capacity_reduction() -> Scenario:
    """
    System starts with full capacity, then loses 50% of workers mid-run.

    Tests: adaptive concurrency, backpressure, recovery.
    """
    tenants = [
        Tenant(id="tenant-1", weight=1.0),
        Tenant(id="tenant-2", weight=1.0),
    ]
    profiles = [
        WorkloadProfile(
            tenant_id="tenant-1",
            num_jobs=5_000,
            cpu_range=(2.0, 10.0),
            memory_range=(64.0, 256.0),
            duration_range=(0.5, 3.0),
        ),
        WorkloadProfile(
            tenant_id="tenant-2",
            num_jobs=5_000,
            cpu_range=(2.0, 10.0),
            memory_range=(64.0, 256.0),
            duration_range=(0.5, 3.0),
        ),
    ]
    events = [
        ScenarioEvent(
            time_offset=30.0,
            event_type="reduce_capacity",
            params={"factor": 0.5},
            description="Lose 50% of worker capacity at t=30s",
        ),
        ScenarioEvent(
            time_offset=90.0,
            event_type="restore_capacity",
            params={"factor": 1.0},
            description="Restore full capacity at t=90s",
        ),
    ]
    return Scenario(
        name="capacity_reduction",
        kind=ScenarioKind.CAPACITY_REDUCTION,
        description="System loses 50% capacity mid-run, then recovers.",
        tenants=tenants,
        profiles=profiles,
        config=EngineConfig(max_workers=100, cpu_budget=200, memory_budget=8192),
        events=events,
        duration=120.0,
    )


def scenario_deadline_pressure() -> Scenario:
    """
    Many jobs with tight deadlines competing with bulk work.

    Tests: EDF scheduling, deadline miss rate, priority interaction.
    """
    tenants = [
        Tenant(id="deadline-heavy", weight=1.5),
        Tenant(id="bulk", weight=1.0),
    ]
    profiles = [
        WorkloadProfile(
            tenant_id="deadline-heavy",
            num_jobs=3_000,
            cpu_range=(1.0, 8.0),
            memory_range=(32.0, 256.0),
            duration_range=(0.2, 2.0),
            deadline_fraction=0.9,
            deadline_slack_range=(1.0, 10.0),
            priority_distribution={
                Priority.LOW: 0.0,
                Priority.NORMAL: 0.3,
                Priority.HIGH: 0.5,
                Priority.CRITICAL: 0.2,
            },
        ),
        WorkloadProfile(
            tenant_id="bulk",
            num_jobs=10_000,
            cpu_range=(2.0, 15.0),
            memory_range=(64.0, 512.0),
            duration_range=(1.0, 5.0),
            deadline_fraction=0.0,
        ),
    ]
    return Scenario(
        name="deadline_pressure",
        kind=ScenarioKind.DEADLINE_PRESSURE,
        description="Tight-deadline jobs compete with heavy bulk processing.",
        tenants=tenants,
        profiles=profiles,
        config=EngineConfig(max_workers=60, cpu_budget=120, memory_budget=4096),
        duration=90.0,
    )


def scenario_expensive_jobs() -> Scenario:
    """
    Mix of very cheap and very expensive jobs.

    Tests: cost-aware scheduling, preventing expensive job starvation.
    """
    tenants = [
        Tenant(id="heavy-hitter", weight=1.0),
        Tenant(id="light-user", weight=1.0),
    ]
    profiles = [
        WorkloadProfile(
            tenant_id="heavy-hitter",
            num_jobs=500,
            cpu_range=(50.0, 100.0),
            memory_range=(512.0, 2048.0),
            duration_range=(5.0, 30.0),
        ),
        WorkloadProfile(
            tenant_id="light-user",
            num_jobs=10_000,
            cpu_range=(0.5, 2.0),
            memory_range=(16.0, 64.0),
            duration_range=(0.05, 0.5),
        ),
    ]
    return Scenario(
        name="expensive_jobs",
        kind=ScenarioKind.EXPENSIVE_JOBS,
        description="500 very expensive jobs vs 10,000 cheap jobs, equal weights.",
        tenants=tenants,
        profiles=profiles,
        config=EngineConfig(max_workers=50, cpu_budget=100, memory_budget=4096),
        duration=120.0,
    )


def scenario_mixed() -> Scenario:
    """Balanced mix of short, long, deadline-bound, and bulk jobs."""
    tenants = [
        Tenant(id="interactive", weight=2.0),
        Tenant(id="batch", weight=1.0),
        Tenant(id="deadline", weight=1.5),
    ]
    profiles = [
        WorkloadProfile(
            tenant_id="interactive",
            num_jobs=1_000,
            cpu_range=(0.5, 3.0),
            memory_range=(32.0, 256.0),
            duration_range=(0.05, 0.5),
            deadline_fraction=0.25,
            deadline_slack_range=(1.0, 8.0),
        ),
        WorkloadProfile(
            tenant_id="batch",
            num_jobs=1_500,
            cpu_range=(2.0, 12.0),
            memory_range=(64.0, 512.0),
            duration_range=(0.5, 4.0),
            priority_distribution={
                Priority.LOW: 0.4,
                Priority.NORMAL: 0.5,
                Priority.HIGH: 0.1,
                Priority.CRITICAL: 0.0,
            },
            deadline_fraction=0.0,
        ),
        WorkloadProfile(
            tenant_id="deadline",
            num_jobs=500,
            cpu_range=(1.0, 6.0),
            memory_range=(32.0, 256.0),
            duration_range=(0.1, 1.0),
            priority_distribution={
                Priority.LOW: 0.0,
                Priority.NORMAL: 0.2,
                Priority.HIGH: 0.5,
                Priority.CRITICAL: 0.3,
            },
            deadline_fraction=0.8,
            deadline_slack_range=(1.0, 10.0),
        ),
    ]
    return Scenario(
        name="mixed",
        kind=ScenarioKind.MIXED,
        description="Interactive, batch, and deadline-sensitive workloads share capacity.",
        tenants=tenants,
        profiles=profiles,
        config=EngineConfig(max_workers=60, cpu_budget=120, memory_budget=4096),
        duration=90.0,
    )


ALL_SCENARIOS = {
    "burst_traffic": scenario_burst_traffic,
    "monopolisation": scenario_monopolisation,
    "capacity_reduction": scenario_capacity_reduction,
    "deadline_pressure": scenario_deadline_pressure,
    "expensive_jobs": scenario_expensive_jobs,
    "mixed": scenario_mixed,
}
