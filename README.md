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

For a focused, paired comparison of LoadPilot and DRR, run the predeclared experiment matrix:

```bash
python -m loadpilot.benchmarks.experiments
```

It uses the same generated jobs and arrival trace for both policies across five fixed seeds and three workloads: balanced Poisson arrivals, priority/deadline pressure, and skewed resource costs. The report includes predeclared outcomes and performance metrics for every scenario, plus paired win/loss/tie counts; it does not select only favorable workloads.

### LoadPilot vs DRR results

Results from the five fixed seeds (`11, 23, 37, 51, 79`) with arrival-time scale `0.25`. Latency is milliseconds; utilization is a ratio; throughput is completed jobs per simulated second. Deferral is reported as events (a job may be deferred more than once). The first table shows medians across repetitions for all predeclared run-level outcome and performance metrics.

| Scenario | Policy | Runs | Duration s | Submitted | Completed | Failed | Rejected | Deferral events | Timed out | Cancelled | Wait p50/p95/p99 ms | Exec p50/p95/p99 ms | Fairness | Deadline misses count/rate | Peak/avg utilization | Scheduling overhead ms | Recovery s | Throughput/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Balanced Poisson | DRR | 5 | 0.5894 | 108 | 108 | 0 | 0 | 50 | 0 | 0 | 188.339/388.559/388.849 | 14.823/15.780/16.016 | 1.0000 | 0/0.00% | 1.0000/0.4500 | 0.2746 | 0.0000 | 183.23 |
| Balanced Poisson | LoadPilot | 5 | 0.5889 | 108 | 108 | 0 | 0 | 53 | 0 | 0 | 200.708/375.935/403.711 | 14.986/15.965/16.049 | 1.0000 | 0/0.00% | 1.0000/0.4500 | 0.2484 | 0.0000 | 183.39 |
| Priority/deadlines | DRR | 5 | 1.3386 | 180 | 180 | 0 | 0 | 79 | 0 | 0 | 483.546/985.398/1031.070 | 14.784/15.776/16.012 | 0.9000 | 40/66.67% | 1.0000/0.4480 | 0.3679 | 0.0000 | 134.47 |
| Priority/deadlines | LoadPilot | 5 | 1.3461 | 180 | 180 | 0 | 0 | 83 | 0 | 0 | 486.054/1017.954/1050.067 | 14.914/15.776/16.255 | 0.9000 | 42/70.00% | 1.0000/0.4480 | 0.3272 | 0.0000 | 133.72 |
| Resource-cost skew | DRR | 5 | 1.4315 | 150 | 150 | 0 | 0 | 76 | 0 | 0 | 966.801/1167.322/1198.061 | 14.810/15.705/15.953 | 0.9615 | 0/0.00% | 1.0000/0.4787 | 0.4487 | 0.0000 | 104.78 |
| Resource-cost skew | LoadPilot | 5 | 1.4339 | 150 | 150 | 0 | 0 | 69 | 0 | 0 | 363.944/1121.059/1184.007 | 14.907/15.859/16.118 | 0.9615 | 0/0.00% | 1.0000/0.4787 | 0.2461 | 0.0000 | 104.61 |

The next table gives LoadPilot wins/losses/ties against DRR for each paired seed. Higher completion, fairness, and throughput are better; lower wait, execution, deadline-miss rate, and scheduling overhead are better.

| Scenario | Completed | Wait p95 | Exec p95 | Fairness | Deadline misses | Throughput | Scheduling overhead |
|---|---:|---:|---:|---:|---:|---:|---:|
| Balanced Poisson | 0/0/5 | 2/3/0 | 2/3/0 | 0/0/5 | 0/0/5 | 4/1/0 | 5/0/0 |
| Priority/deadlines | 0/0/5 | 2/3/0 | 2/3/0 | 0/0/5 | 2/3/0 | 2/3/0 | 5/0/0 |
| Resource-cost skew | 0/0/5 | 4/1/0 | 1/4/0 | 0/0/5 | 0/0/5 | 0/5/0 | 5/0/0 |

Both policies completed all jobs and had identical fairness in every paired run. LoadPilot had lower scheduling overhead in all 15 comparisons and lower p95 wait in four of five resource-cost-skew runs, but it did not consistently win on wait, execution time, or throughput. For deadline misses, LoadPilot won two paired runs and lost three; its median miss rate was higher (70% vs. 66.67%). These small synthetic experiments show tradeoffs, not a universal winner; conclusions are limited to the disclosed workload matrix, seeds, simulator timing, and current policy parameters.

## Simulation scope

The simulator is intended for comparing scheduling behavior; it is not a model of real hardware or a live traffic replay:

- Synthetic jobs sleep for their estimated duration. CPU and memory are tracked from estimates and are not physically consumed.
- Workload profiles can use bulk, uniform, Poisson, or burst arrivals. Simulations submit jobs at the generated offsets; `arrival_time_scale` can compress or expand those offsets.
- A job with `depends_on` is held until every prerequisite completes successfully. Missing prerequisites and dependency cycles fail once the input stream has ended and no runnable work remains; a failed prerequisite fails its dependents.
- `Job.max_retries` overrides the engine-wide retry limit when set. `None` inherits `EngineConfig.default_max_retries`; `0` disables retries for that job.
- A job that fits total resource capacity but not currently available resources is deferred rather than rejected. Jobs whose estimates exceed total capacity are rejected.

## Tests

Run the test suite from the project root:

```bash
pytest tests/ -v
```

## Further reading

- [Google SRE: Handling Overload](https://sre.google/sre-book/handling-overload/)
- [Envoy: Adaptive Concurrency](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter.html)
- [Apache Flink: Backpressure](https://nightlies.apache.org/flink/flink-docs-master/ops/monitoring/back_pressure.html)
