"""
LoadPilot — Benchmark report generation.

Produces human-readable comparison tables and summaries from benchmark results.
"""

from __future__ import annotations

import io
from typing import Any

from loadpilot.benchmarks.runner import BenchmarkSuite
from loadpilot.simulator.runner import SimulationResult


def generate_report(suite: BenchmarkSuite) -> str:
    """Generate a markdown-formatted benchmark report."""
    buf = io.StringIO()
    buf.write("# LoadPilot Benchmark Report\n\n")

    for scenario_name, policy_results in suite.results.items():
        buf.write(f"## Scenario: {scenario_name}\n\n")
        _write_comparison_table(buf, policy_results)
        buf.write("\n")
        _write_tenant_table(buf, policy_results)
        buf.write("\n---\n\n")

    # Summary
    buf.write("## Summary\n\n")
    _write_summary(buf, suite)

    return buf.getvalue()


def _write_comparison_table(
    buf: io.StringIO,
    results: dict[str, SimulationResult],
) -> None:
    """Write the main policy comparison table."""
    headers = [
        "Metric",
        *results.keys(),
    ]
    buf.write("| " + " | ".join(headers) + " |\n")
    buf.write("| " + " | ".join("---" for _ in headers) + " |\n")

    metrics = [
        ("Duration (s)", lambda r: f"{r.duration:.1f}"),
        ("Submitted", lambda r: f"{r.total_submitted}"),
        ("Completed", lambda r: f"{r.total_completed}"),
        ("Rejected", lambda r: f"{r.total_rejected}"),
        ("Deferred", lambda r: f"{r.total_deferred}"),
        ("Failed", lambda r: f"{r.total_failed}"),
        ("Timed Out", lambda r: f"{r.total_timed_out}"),
        ("Cancelled", lambda r: f"{r.total_cancelled}"),
        ("Throughput (jobs/s)", lambda r: f"{r.total_completed / max(r.duration, 0.001):.1f}"),
        ("Wait p50 (ms)", lambda r: f"{r.wait_time_p50:.1f}"),
        ("Wait p95 (ms)", lambda r: f"{r.wait_time_p95:.1f}"),
        ("Wait p99 (ms)", lambda r: f"{r.wait_time_p99:.1f}"),
        ("Exec p50 (ms)", lambda r: f"{r.exec_time_p50:.1f}"),
        ("Exec p95 (ms)", lambda r: f"{r.exec_time_p95:.1f}"),
        ("Exec p99 (ms)", lambda r: f"{r.exec_time_p99:.1f}"),
        ("Fairness Index", lambda r: f"{r.fairness_index:.4f}"),
        ("Deadline Misses", lambda r: f"{r.deadline_miss_count}"),
        ("Deadline Miss %", lambda r: f"{r.deadline_miss_rate * 100:.1f}%"),
        ("Sched Overhead (ms)", lambda r: f"{r.scheduling_overhead_ms:.3f}"),
    ]

    for name, extractor in metrics:
        row = [name]
        for result in results.values():
            row.append(extractor(result))
        buf.write("| " + " | ".join(row) + " |\n")


def _write_tenant_table(
    buf: io.StringIO,
    results: dict[str, SimulationResult],
) -> None:
    """Write per-tenant breakdown for each policy."""
    buf.write("\n### Per-Tenant Breakdown\n\n")

    for policy_name, result in results.items():
        if not result.tenant_stats:
            continue

        buf.write(f"**{policy_name}**\n\n")
        headers = ["Tenant", "Submitted", "Completed", "Rejected", "Wait p50 (ms)", "Wait p95 (ms)", "Jobs/s"]
        buf.write("| " + " | ".join(headers) + " |\n")
        buf.write("| " + " | ".join("---" for _ in headers) + " |\n")

        for tid, stats in result.tenant_stats.items():
            row = [
                tid,
                str(stats.get("submitted", 0)),
                str(stats.get("completed", 0)),
                str(stats.get("rejected", 0)),
                f"{stats.get('wait_p50', 0):.1f}",
                f"{stats.get('wait_p95', 0):.1f}",
                f"{stats.get('completions_per_second', 0):.1f}",
            ]
            buf.write("| " + " | ".join(row) + " |\n")

        buf.write("\n")


def _write_summary(buf: io.StringIO, suite: BenchmarkSuite) -> None:
    """Write a high-level summary comparing LoadPilot vs baselines."""
    buf.write("### LoadPilot vs Baselines\n\n")

    for scenario_name, results in suite.results.items():
        lp = results.get("loadpilot")
        fifo = results.get("fifo")
        if lp is None:
            continue

        buf.write(f"**{scenario_name}**:\n")

        if fifo:
            fairness_improvement = (
                (lp.fairness_index - fifo.fairness_index) / max(fifo.fairness_index, 0.001) * 100
            )
            buf.write(f"- Fairness improvement over FIFO: {fairness_improvement:+.1f}%\n")

            if fifo.deadline_miss_rate > 0:
                deadline_improvement = (
                    (fifo.deadline_miss_rate - lp.deadline_miss_rate)
                    / fifo.deadline_miss_rate * 100
                )
                buf.write(f"- Deadline miss reduction vs FIFO: {deadline_improvement:+.1f}%\n")

        buf.write(f"- Jain's Fairness Index: {lp.fairness_index:.4f}\n")
        buf.write(f"- Deadline miss rate: {lp.deadline_miss_rate * 100:.1f}%\n")
        buf.write(f"- Wait time p95: {lp.wait_time_p95:.1f}ms\n")
        buf.write("\n")
