"""Tests for benchmark scenarios, metrics, and report generation."""

from __future__ import annotations

import pytest

from loadpilot.benchmarks.report import generate_report
from loadpilot.benchmarks.runner import BenchmarkRunner, BenchmarkSuite
from loadpilot.simulator import scenarios
from loadpilot.simulator.scenarios import Scenario, ScenarioEvent, ScenarioKind
from loadpilot.workload.generator import WorkloadProfile
from loadpilot.workload.models import EngineConfig, Tenant


def test_mixed_scenario_is_available():
    scenario = scenarios.ALL_SCENARIOS["mixed"]()
    assert scenario.kind is ScenarioKind.MIXED
    assert {profile.tenant_id for profile in scenario.profiles} == {
        tenant.id for tenant in scenario.tenants
    }


def test_benchmark_rejects_unknown_scenario_or_policy():
    with pytest.raises(ValueError, match="Unknown scenarios"):
        BenchmarkRunner(scenarios=["missing"])
    with pytest.raises(ValueError, match="Unknown policies"):
        BenchmarkRunner(policies=["missing"])


@pytest.mark.asyncio
async def test_small_simulation_reports_outcomes_and_utilization(monkeypatch):
    scenario = Scenario(
        name="tiny",
        kind=ScenarioKind.MIXED,
        description="Short deterministic benchmark fixture.",
        tenants=[Tenant(id="tenant")],
        profiles=[
            WorkloadProfile(
                tenant_id="tenant",
                num_jobs=3,
                cpu_range=(1, 1),
                memory_range=(1, 1),
                duration_range=(0.01, 0.01),
                deadline_fraction=0,
            )
        ],
        events=[
            ScenarioEvent(
                time_offset=0.005,
                event_type="reduce_capacity",
                params={"factor": 0.5},
            ),
            ScenarioEvent(
                time_offset=0.02,
                event_type="restore_capacity",
            ),
        ],
        config=EngineConfig(
            max_workers=2,
            cpu_budget=2,
            memory_budget=10,
            initial_concurrency=2,
            min_concurrency=1,
            max_concurrency=2,
            default_max_retries=0,
        ),
    )
    monkeypatch.setitem(scenarios.ALL_SCENARIOS, "tiny", lambda: scenario)

    suite = await BenchmarkRunner(
        scenarios=["tiny"],
        policies=["fifo", "loadpilot"],
    ).run()
    assert set(suite.results["tiny"]) == {"fifo", "loadpilot"}
    result = suite.results["tiny"]["fifo"]

    assert result.total_submitted == 3
    assert result.total_completed == 3
    assert result.total_failed == 0
    assert result.peak_utilization > 0
    assert result.avg_utilization > 0
    assert result.recovery_time > 0
    assert 0 < result.fairness_index <= 1

    report = generate_report(suite)
    assert "| Completed | 3 |" in report
    assert "| Cancelled | 0 |" in report
    assert "loadpilot" in report
