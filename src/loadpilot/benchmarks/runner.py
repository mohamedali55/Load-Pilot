"""
LoadPilot — Benchmark runner.

Runs identical workload traces against multiple scheduling policies and
produces comparable results.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from loadpilot.simulator.runner import SimulationResult, SimulationRunner
from loadpilot.simulator.scenarios import (
    ALL_SCENARIOS,
    Scenario,
)

logger = logging.getLogger(__name__)

POLICIES = ["fifo", "priority", "drr", "loadpilot"]


@dataclass(slots=True)
class BenchmarkSuite:
    """Results of running all policies against all scenarios."""

    results: dict[str, dict[str, SimulationResult]] = field(default_factory=dict)
    # results[scenario_name][policy_name] = SimulationResult


class BenchmarkRunner:
    """Runs benchmark comparisons across policies and scenarios."""

    def __init__(
        self,
        scenarios: list[str] | None = None,
        policies: list[str] | None = None,
    ) -> None:
        self._scenario_names = (
            list(ALL_SCENARIOS) if scenarios is None else list(scenarios)
        )
        self._policies = list(POLICIES) if policies is None else list(policies)
        unknown_policies = set(self._policies).difference(POLICIES)
        if unknown_policies:
            raise ValueError(
                f"Unknown policies: {', '.join(sorted(unknown_policies))}"
            )
        unknown_scenarios = set(self._scenario_names).difference(ALL_SCENARIOS)
        if unknown_scenarios:
            raise ValueError(
                f"Unknown scenarios: {', '.join(sorted(unknown_scenarios))}"
            )

    async def run(self) -> BenchmarkSuite:
        """Run all benchmarks and return a suite of results."""
        suite = BenchmarkSuite()

        for scenario_name in self._scenario_names:
            factory = ALL_SCENARIOS[scenario_name]

            suite.results[scenario_name] = {}

            for policy in self._policies:
                logger.info("Running %s / %s ...", scenario_name, policy)
                scenario = factory()
                runner = SimulationRunner(scenario, policy=policy)

                result = await runner.run()
                suite.results[scenario_name][policy] = result
                logger.info(
                    "  %s: completed=%d rejected=%d fairness=%.3f deadline_miss=%.1f%%",
                    policy,
                    result.total_completed,
                    result.total_rejected,
                    result.fairness_index,
                    result.deadline_miss_rate * 100,
                )

        return suite

    async def run_single(
        self,
        scenario_name: str,
        policy: str,
    ) -> SimulationResult:
        """Run a single scenario with a single policy."""
        factory = ALL_SCENARIOS.get(scenario_name)
        if factory is None:
            raise ValueError(f"Unknown scenario: {scenario_name}")

        scenario = factory()
        runner = SimulationRunner(scenario, policy=policy)
        return await runner.run()
