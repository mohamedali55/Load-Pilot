"""Tests for async execution, worker management, timeouts, and retries."""

from __future__ import annotations

import asyncio

import pytest

from loadpilot.engine import Engine
from loadpilot.runtime.executor import AsyncExecutor, RetryPolicy
from loadpilot.runtime.worker import Worker, WorkerPool
from loadpilot.workload.models import EngineConfig, Job, JobStatus


@pytest.mark.asyncio
async def test_worker_executes_payload_and_records_result():
    async def payload():
        return "done"

    job = Job(id="result", tenant_id="tenant", payload=payload)
    worker = Worker(0)

    await worker.execute(job)

    assert job.status is JobStatus.COMPLETED
    assert job.result == "done"
    assert job.attempt == 1
    assert worker.stats.total_executed == 1


@pytest.mark.asyncio
async def test_timeout_cancels_payload():
    cancelled = asyncio.Event()

    async def payload():
        try:
            await asyncio.sleep(10)
        finally:
            cancelled.set()

    job = Job(id="timeout", tenant_id="tenant", payload=payload, timeout=0.01)

    await Worker(0).execute(job)

    assert job.status is JobStatus.TIMED_OUT
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_pool_resize_wakes_waiting_jobs():
    pool = WorkerPool(max_workers=1)
    started = 0
    started_twice = asyncio.Event()
    release = asyncio.Event()

    async def payload():
        nonlocal started
        started += 1
        if started >= 2:
            started_twice.set()
        await release.wait()

    tasks = [
        await pool.submit(Job(id=f"resize-{i}", tenant_id="tenant", payload=payload))
        for i in range(3)
    ]
    await asyncio.sleep(0)
    pool.resize(2)
    await asyncio.wait_for(started_twice.wait(), timeout=1)
    assert started == 2

    release.set()
    await asyncio.gather(*tasks)
    assert pool.active_count == 0


@pytest.mark.asyncio
async def test_cancelled_job_fires_completion_once():
    pool = WorkerPool(max_workers=1)
    started = asyncio.Event()
    completions: list[JobStatus] = []

    async def payload():
        started.set()
        await asyncio.sleep(10)

    pool.on_completion(lambda job: completions.append(job.status))
    job = Job(id="cancel", tenant_id="tenant", payload=payload)
    await pool.submit(job)
    await started.wait()

    assert await pool.cancel(job.id)
    assert job.status is JobStatus.CANCELLED
    assert completions == [JobStatus.CANCELLED]


@pytest.mark.asyncio
async def test_retry_policy_allows_configured_retry_count_and_notifies_once():
    pool = WorkerPool(max_workers=1)
    executor = AsyncExecutor(
        pool,
        RetryPolicy(max_retries=2, base_delay=0, max_delay=0),
    )
    attempts = 0
    completed = asyncio.Event()
    notifications: list[JobStatus] = []

    async def payload():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("temporary failure")
        return "success"

    async def on_completion(job: Job):
        notifications.append(job.status)
        completed.set()

    executor.on_completion(on_completion)
    retry_task = executor.start_retry_processor()
    job = Job(id="retry", tenant_id="tenant", payload=payload)
    try:
        await executor.execute(job)
        await asyncio.wait_for(completed.wait(), timeout=1)
        assert job.status is JobStatus.COMPLETED
        assert attempts == 3
        assert notifications == [JobStatus.COMPLETED]
        assert executor.pending_retry_count == 0
    finally:
        await executor.shutdown(timeout=1)
        await asyncio.gather(retry_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_engine_runs_jobs_concurrently_and_releases_resources():
    config = EngineConfig(
        max_workers=2,
        initial_concurrency=2,
        min_concurrency=1,
        max_concurrency=2,
        cpu_budget=2,
        memory_budget=256,
        adjustment_interval=0.01,
        default_max_retries=0,
    )
    engine = Engine(config=config)
    started = 0
    both_started = asyncio.Event()
    release = asyncio.Event()

    async def payload():
        nonlocal started
        started += 1
        if started == 2:
            both_started.set()
        await release.wait()

    engine.submit(
        Job(id="concurrent-1", tenant_id="tenant", estimated_cpu=1, payload=payload)
    )
    engine.submit(
        Job(id="concurrent-2", tenant_id="tenant", estimated_cpu=1, payload=payload)
    )

    drain_task = asyncio.create_task(engine.run_until_drained(poll_interval=0.005))
    try:
        await asyncio.wait_for(both_started.wait(), timeout=1)
        assert engine._tenants["tenant"].current_running == 2
        release.set()
        await asyncio.wait_for(drain_task, timeout=1)
    finally:
        release.set()
        if not drain_task.done():
            await engine.stop()
            await asyncio.gather(drain_task, return_exceptions=True)

    assert engine._tenants["tenant"].current_running == 0
    assert engine.snapshot()["resources"]["active_reservations"] == 0
