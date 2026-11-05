# LoadPilot

LoadPilot is a Python runtime for **cost-aware, multi-tenant job scheduling**. It combines admission control, scheduling policies, resource accounting, asynchronous execution, and adaptive concurrency in one engine.

It is designed to explore how shared workloads can be scheduled when jobs differ in priority, estimated cost, and deadline.

## Why LoadPilot?

In a shared service, a large burst of expensive work can compete with smaller, time-sensitive jobs for the same workers and resources. LoadPilot provides a place to experiment with decisions such as:

- Should a job be accepted, deferred, or rejected?
- Which tenant's work should run next?
- Does a job fit within the available CPU, memory, and worker capacity?
- Should concurrency change as latency and queue pressure change?

## Requirements

- Python 3.12 or newer

## Installation

Install LoadPilot with its development dependencies:

```bash
pip install -e ".[dev]"
```

## Quick start

```python
import asyncio

from loadpilot import Engine, Job, Tenant


async def main():
    engine = Engine(
        max_workers=50,
        cpu_budget=100,
        memory_budget=4096,
    )

    engine.register_tenant(Tenant(id="customer-a", weight=2))
    engine.register_tenant(Tenant(id="customer-b", weight=1))

    engine.submit(
        Job(
            id="report-001",
            tenant_id="customer-a",
            estimated_cpu=10,
            estimated_memory=256,
            priority=2,
        )
    )
    engine.submit(
        Job(
            id="quick-task",
            tenant_id="customer-b",
            estimated_cpu=1,
            estimated_memory=32,
            priority=3,
        )
    )

    # Process accepted work and return once the engine has drained.
    await engine.run_until_drained()

    snapshot = engine.snapshot()
    print(f"Completed jobs: {int(engine.metrics.jobs_completed.value)}")
    print(f"Pending jobs: {snapshot['pending_jobs']}")
    print(f"Active jobs: {snapshot['active_jobs']}")


asyncio.run(main())
```

Jobs can provide an asynchronous `payload` to execute. If no payload is supplied, the worker simulates work by sleeping for the job's `estimated_duration`.

## How it works

The engine processes work through these stages:

1. **Admission:** Resource availability, queue limits, tenant limits, and system pressure determine whether a job is accepted, deferred, or rejected.
2. **Scheduling:** Accepted jobs wait in per-tenant queues. The scheduler first considers jobs with negative estimated deadline slack, then ranks remaining jobs with the configured scheduling policy.
3. **Reservation:** Before execution, the resource manager reserves the job's estimated CPU, memory, and worker slot against global capacity and the tenant's budget.
4. **Execution:** An asynchronous worker runs the payload or simulated work. Timeouts cancel the running coroutine; failed and timed-out jobs may be retried with exponential backoff.
5. **Completion and feedback:** When a job succeeds or reaches a final failure state, resources are released and metrics are updated. Latency and queue pressure inform the adaptive concurrency and backpressure controllers.

## Scheduling policies

| Policy | Behavior |
|---|---|
| FIFO | Runs jobs in submission order. |
| Priority | Ranks higher-priority jobs first. |
| DRR | Uses per-tenant deficit round robin to share service according to configured quanta. |
| LoadPilot | Scores jobs using priority, fairness debt, deadline slack, estimated cost, and system pressure. This is the default policy. |

Change the policy on an engine with:

```python
engine.set_policy("fifo")  # "priority", "drr", or "loadpilot"
```

Jobs with negative estimated deadline slack are selected before normally ranked work when they fit the remaining capacity. Slack is estimated as:

```text
deadline - current_time - estimated_duration
```

## Configuration and observability

`Engine` accepts worker, CPU, and memory limits directly, or an `EngineConfig` for additional settings such as queue limits, scoring weights, retry behavior, and adaptive concurrency bounds.

Use `engine.snapshot()` to inspect pending, active, and deferred work, resource reservations, worker-pool state, and pressure. The `engine.metrics` collector exposes counters and latency/resource measurements.

## Benchmarks

The benchmark runner compares policies against repeatable workload scenarios and generates a Markdown report:

```python
import asyncio

from loadpilot.benchmarks import BenchmarkRunner, generate_report


async def main():
    runner = BenchmarkRunner(
        scenarios=["burst_traffic", "monopolisation"],
        policies=["fifo", "priority", "drr", "loadpilot"],
    )
    suite = await runner.run()
    print(generate_report(suite))


asyncio.run(main())
```

Available scenarios include burst traffic, tenant monopolisation, capacity reduction, deadline pressure, expensive jobs, and mixed workloads. Reports compare throughput, latency percentiles, fairness, deadline misses, and scheduling overhead.

## Simulation scope

The simulator is intended for comparing scheduling behavior; it is not a model of real hardware or a live traffic replay:

- Synthetic jobs sleep for their estimated duration. CPU and memory are tracked from estimates and are not physically consumed.
- Scenario jobs are submitted in bulk; generated inter-arrival times are not replayed as a live stream.
- `Job.depends_on` is represented in the model, but dependency completion is not enforced by the scheduler.
- Retries use the engine-wide settings in `EngineConfig`; the per-job `max_retries` field does not currently set the retry limit.

## Tests

Run the test suite from the project root:

```bash
pytest tests/ -v
```

## Further reading

- [Google SRE: Handling Overload](https://sre.google/sre-book/handling-overload/)
- [Envoy: Adaptive Concurrency](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter.html)
- [Apache Flink: Backpressure](https://nightlies.apache.org/flink/flink-docs-master/ops/monitoring/back_pressure.html)
