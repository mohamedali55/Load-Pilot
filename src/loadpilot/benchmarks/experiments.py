"""Paired, repeatable LoadPilot-versus-DRR scheduling experiments."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from statistics import median

from loadpilot.simulator.runner import SimulationResult, SimulationRunner
from loadpilot.simulator.scenarios import Scenario, ScenarioKind
from loadpilot.workload.generator import WorkloadProfile
from loadpilot.workload.models import EngineConfig, Priority, Tenant

DEFAULT_SEEDS = (11, 23, 37, 51, 79)
POLICIES = ("drr", "loadpilot")


@dataclass(frozen=True, slots=True)
class ExperimentSummary:
    scenario: str
    policy: str
    repetitions: int
    duration_s: float
    submitted: float
    completed: float
    failed: float
    rejected: float
    deferred: float
    timed_out: float
    cancelled: float
    wait_p50_ms: float
    wait_p95_ms: float
    wait_p99_ms: float
    exec_p50_ms: float
    exec_p95_ms: float
    exec_p99_ms: float
    fairness: float
    deadline_miss_count: float
    deadline_miss_rate: float
    peak_utilization: float
    avg_utilization: float
    scheduling_overhead_ms: float
    recovery_time_s: float
    throughput_per_second: float


@dataclass(frozen=True, slots=True)
class ComparisonResults:
    summaries: tuple[ExperimentSummary, ...]
    paired_runs: dict[str, dict[str, tuple[SimulationResult, ...]]]
    seeds: tuple[int, ...]
    arrival_time_scale: float


def build_scenarios(seed: int) -> tuple[Scenario, ...]:
    """Return the predeclared workload matrix for one paired random seed."""
    config = EngineConfig(
        max_workers=6,
        cpu_budget=6,
        memory_budget=256,
        initial_concurrency=6,
        min_concurrency=1,
        max_concurrency=6,
        default_max_retries=0,
        adjustment_interval=0.01,
    )
    tenants = [
        Tenant(id="alpha", weight=1.0),
        Tenant(id="beta", weight=1.0),
        Tenant(id="gamma", weight=1.0),
    ]
    balanced = Scenario(
        name="balanced_poisson",
        kind=ScenarioKind.MIXED,
        description="Three equal-weight tenants with independent Poisson arrivals.",
        tenants=tenants,
        profiles=[
            WorkloadProfile(
                tenant_id=tenant.id,
                num_jobs=36,
                priority_distribution={Priority.NORMAL: 1.0},
                cpu_range=(1.0, 1.0),
                memory_range=(8.0, 8.0),
                duration_range=(0.003, 0.008),
                deadline_fraction=0,
                arrival_pattern="poisson",
                arrival_rate=60,
            )
            for tenant in tenants
        ],
        config=config,
        seed=seed,
    )

    priority_tenants = [
        Tenant(id="urgent", weight=1.0),
        Tenant(id="bulk", weight=1.0),
    ]
    priority_deadline = Scenario(
        name="priority_deadlines",
        kind=ScenarioKind.DEADLINE_PRESSURE,
        description=(
            "High-priority deadline work competes with normal-priority bulk work."
        ),
        tenants=priority_tenants,
        profiles=[
            WorkloadProfile(
                tenant_id="urgent",
                num_jobs=60,
                priority_distribution={
                    Priority.HIGH: 0.7,
                    Priority.CRITICAL: 0.3,
                },
                cpu_range=(1.0, 1.0),
                memory_range=(8.0, 8.0),
                duration_range=(0.003, 0.008),
                deadline_fraction=1.0,
                deadline_slack_range=(0.02, 0.12),
                arrival_pattern="poisson",
                arrival_rate=80,
            ),
            WorkloadProfile(
                tenant_id="bulk",
                num_jobs=120,
                priority_distribution={Priority.NORMAL: 1.0},
                cpu_range=(1.0, 2.0),
                memory_range=(8.0, 16.0),
                duration_range=(0.003, 0.01),
                deadline_fraction=0,
                arrival_pattern="poisson",
                arrival_rate=120,
            ),
        ],
        config=config,
        seed=seed,
    )

    cost_tenants = [
        Tenant(id="compute-heavy", weight=1.0),
        Tenant(id="lightweight", weight=1.0),
    ]
    cost_skew = Scenario(
        name="resource_cost_skew",
        kind=ScenarioKind.MIXED,
        description=(
            "Equal-weight tenants with different estimated CPU-duration costs."
        ),
        tenants=cost_tenants,
        profiles=[
            WorkloadProfile(
                tenant_id="compute-heavy",
                num_jobs=60,
                priority_distribution={Priority.NORMAL: 1.0},
                cpu_range=(3.0, 3.0),
                memory_range=(8.0, 8.0),
                duration_range=(0.004, 0.01),
                deadline_fraction=0,
                arrival_pattern="poisson",
                arrival_rate=80,
            ),
            WorkloadProfile(
                tenant_id="lightweight",
                num_jobs=90,
                priority_distribution={Priority.NORMAL: 1.0},
                cpu_range=(1.0, 1.0),
                memory_range=(8.0, 8.0),
                duration_range=(0.002, 0.005),
                deadline_fraction=0,
                arrival_pattern="poisson",
                arrival_rate=120,
            ),
        ],
        config=config,
        seed=seed,
    )
    return balanced, priority_deadline, cost_skew


async def run_comparison(
    seeds: tuple[int, ...] = DEFAULT_SEEDS,
    *,
    arrival_time_scale: float = 0.25,
) -> ComparisonResults:
    """Run every policy on identical generated workloads for each seed."""
    if not seeds:
        raise ValueError("At least one seed is required")
    if len(set(seeds)) != len(seeds):
        raise ValueError("Seeds must be unique")

    paired: dict[str, dict[str, list[SimulationResult]]] = {}
    for seed in seeds:
        for scenario in build_scenarios(seed):
            policy_results = paired.setdefault(
                scenario.name,
                {policy: [] for policy in POLICIES},
            )
            for policy in POLICIES:
                result = await SimulationRunner(
                    deepcopy(scenario),
                    policy=policy,
                    arrival_time_scale=arrival_time_scale,
                ).run()
                policy_results[policy].append(result)

    paired_runs = {
        scenario: {
            policy: tuple(results)
            for policy, results in policy_results.items()
        }
        for scenario, policy_results in paired.items()
    }
    summaries = tuple(
        _summarize(scenario, policy, results)
        for scenario, policy_results in paired_runs.items()
        for policy, results in policy_results.items()
    )
    return ComparisonResults(summaries, paired_runs, seeds, arrival_time_scale)


def format_report(results: ComparisonResults) -> str:
    """Format all predeclared results and paired win counts as Markdown."""
    lines = [
        "# LoadPilot vs DRR: paired experiment results",
        "",
        (
            "Each seed runs both policies on the same generated jobs and arrival trace. "
            "Values below are medians across all reported repetitions. Every "
            "predeclared scenario and run-level metric is included."
        ),
        "",
        (
            f"Seeds: {', '.join(map(str, results.seeds))}. "
            f"Arrival time scale: {results.arrival_time_scale:g}."
        ),
        "",
        (
            "| Scenario | Policy | Runs | Duration s | Submitted | Completed | "
            "Failed | Rejected | Deferral events | Timed out | Cancelled "
            "| Wait p50/p95/p99 ms "
            "| Exec p50/p95/p99 ms | Fairness | Deadline misses count/rate "
            "| Peak/avg utilization | Scheduling overhead ms | Recovery s "
            "| Throughput/s |"
        ),
        (
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
            "---:|---:|---:|---:|---:|---:|---:|"
        ),
    ]
    for item in results.summaries:
        lines.append(
            f"| {item.scenario} | {item.policy} | {item.repetitions} "
            f"| {item.duration_s:.4f} "
            f"| {item.submitted:.0f} | {item.completed:.0f} | {item.failed:.0f} "
            f"| {item.rejected:.0f} | {item.deferred:.0f} | {item.timed_out:.0f} "
            f"| {item.cancelled:.0f} "
            f"| {item.wait_p50_ms:.3f}/{item.wait_p95_ms:.3f}/{item.wait_p99_ms:.3f} "
            f"| {item.exec_p50_ms:.3f}/{item.exec_p95_ms:.3f}/{item.exec_p99_ms:.3f} "
            f"| {item.fairness:.4f} "
            f"| {item.deadline_miss_count:.0f}/{item.deadline_miss_rate:.2%} "
            f"| {item.peak_utilization:.4f}/{item.avg_utilization:.4f} "
            f"| {item.scheduling_overhead_ms:.4f} | {item.recovery_time_s:.4f} "
            f"| {item.throughput_per_second:.2f} |"
        )

    lines.extend([
        "",
        "## Paired LoadPilot wins (out of the runs shown)",
        "",
        (
            "Each cell is LoadPilot wins/losses/ties against DRR. Higher completion, "
            "fairness, and throughput are better; lower wait, execution, deadline "
            "miss rate, and scheduling overhead are better."
        ),
        "",
        (
            "| Scenario | Completed | Wait p95 | Exec p95 | Fairness "
            "| Deadline misses | Throughput | Scheduling overhead |"
        ),
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for scenario, policy_results in results.paired_runs.items():
        loadpilot_runs = policy_results["loadpilot"]
        drr_runs = policy_results["drr"]
        wins = (
            _paired_outcomes(
                loadpilot_runs, drr_runs, lambda r: r.total_completed, True
            ),
            _paired_outcomes(
                loadpilot_runs, drr_runs, lambda r: r.wait_time_p95, False
            ),
            _paired_outcomes(
                loadpilot_runs, drr_runs, lambda r: r.exec_time_p95, False
            ),
            _paired_outcomes(
                loadpilot_runs, drr_runs, lambda r: r.fairness_index, True
            ),
            _paired_outcomes(
                loadpilot_runs, drr_runs, lambda r: r.deadline_miss_rate, False
            ),
            _paired_outcomes(
                loadpilot_runs,
                drr_runs,
                lambda r: r.total_completed / r.duration if r.duration else 0.0,
                True,
            ),
            _paired_outcomes(
                loadpilot_runs, drr_runs, lambda r: r.scheduling_overhead_ms, False
            ),
        )
        lines.append(
            f"| {scenario} | {wins[0]} | {wins[1]} | {wins[2]} | {wins[3]} "
            f"| {wins[4]} | {wins[5]} | {wins[6]} |"
        )
    return "\n".join(lines)


def _summarize(
    scenario: str,
    policy: str,
    results: tuple[SimulationResult, ...],
) -> ExperimentSummary:
    return ExperimentSummary(
        scenario=scenario,
        policy=policy,
        repetitions=len(results),
        duration_s=median(result.duration for result in results),
        submitted=median(result.total_submitted for result in results),
        completed=median(result.total_completed for result in results),
        failed=median(result.total_failed for result in results),
        rejected=median(result.total_rejected for result in results),
        deferred=median(result.total_deferred for result in results),
        timed_out=median(result.total_timed_out for result in results),
        cancelled=median(result.total_cancelled for result in results),
        wait_p50_ms=median(result.wait_time_p50 for result in results),
        wait_p95_ms=median(result.wait_time_p95 for result in results),
        wait_p99_ms=median(result.wait_time_p99 for result in results),
        exec_p50_ms=median(result.exec_time_p50 for result in results),
        exec_p95_ms=median(result.exec_time_p95 for result in results),
        exec_p99_ms=median(result.exec_time_p99 for result in results),
        fairness=median(result.fairness_index for result in results),
        deadline_miss_count=median(result.deadline_miss_count for result in results),
        deadline_miss_rate=median(result.deadline_miss_rate for result in results),
        peak_utilization=median(result.peak_utilization for result in results),
        avg_utilization=median(result.avg_utilization for result in results),
        scheduling_overhead_ms=median(
            result.scheduling_overhead_ms for result in results
        ),
        recovery_time_s=median(result.recovery_time for result in results),
        throughput_per_second=median(
            result.total_completed / result.duration if result.duration else 0.0
            for result in results
        ),
    )


def _paired_outcomes(
    left: tuple[SimulationResult, ...],
    right: tuple[SimulationResult, ...],
    metric: Callable[[SimulationResult], float],
    higher_is_better: bool,
) -> str:
    wins = losses = ties = 0
    for left_result, right_result in zip(left, right):
        left_value = metric(left_result)
        right_value = metric(right_result)
        if abs(left_value - right_value) <= 1e-9:
            ties += 1
        elif (left_value > right_value) == higher_is_better:
            wins += 1
        else:
            losses += 1
    return f"{wins}/{losses}/{ties}"


async def _main() -> None:
    results = await run_comparison()
    print(format_report(results))


if __name__ == "__main__":
    asyncio.run(_main())
