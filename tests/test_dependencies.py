"""Tests for dependency-aware scheduling."""

from __future__ import annotations

import asyncio

import pytest

from loadpilot.engine import Engine
from loadpilot.workload.models import EngineConfig, Job, JobStatus


def _engine() -> Engine:
    return Engine(
        config=EngineConfig(
            max_workers=2,
            cpu_budget=2,
            memory_budget=64,
            initial_concurrency=2,
            min_concurrency=1,
            max_concurrency=2,
            default_max_retries=0,
            adjustment_interval=0.005,
        )
    )


@pytest.mark.asyncio
async def test_dependency_can_be_submitted_before_its_prerequisite():
    engine = _engine()
    prerequisite_finished = asyncio.Event()

    async def prerequisite():
        await asyncio.sleep(0.01)
        prerequisite_finished.set()

    async def dependent():
        assert prerequisite_finished.is_set()

    child = Job(
        id="child",
        tenant_id="tenant",
        depends_on=["parent"],
        payload=dependent,
    )
    parent = Job(id="parent", tenant_id="tenant", payload=prerequisite)
    engine.submit(child)
    engine.submit(parent)

    await asyncio.wait_for(engine.run_until_drained(poll_interval=0.002), timeout=1)

    assert parent.status is JobStatus.COMPLETED
    assert child.status is JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_failed_prerequisite_fails_dependent_without_running_it():
    engine = _engine()
    dependent_called = False

    async def prerequisite():
        raise RuntimeError("prerequisite failed")

    async def dependent():
        nonlocal dependent_called
        dependent_called = True

    child = Job(
        id="child",
        tenant_id="tenant",
        depends_on=["parent"],
        payload=dependent,
    )
    parent = Job(id="parent", tenant_id="tenant", payload=prerequisite)
    engine.submit(child)
    engine.submit(parent)

    await asyncio.wait_for(engine.run_until_drained(poll_interval=0.002), timeout=1)

    assert parent.status is JobStatus.FAILED
    assert child.status is JobStatus.FAILED
    assert not dependent_called


@pytest.mark.asyncio
async def test_missing_dependency_fails_when_input_stream_finishes():
    engine = _engine()
    job = Job(id="orphan", tenant_id="tenant", depends_on=["missing"])
    engine.submit(job)

    await asyncio.wait_for(engine.run_until_drained(poll_interval=0.002), timeout=1)

    assert job.status is JobStatus.FAILED
    assert "missing" in str(job.error)


@pytest.mark.asyncio
async def test_dependency_cycle_fails_instead_of_hanging():
    engine = _engine()
    first = Job(id="first", tenant_id="tenant", depends_on=["second"])
    second = Job(id="second", tenant_id="tenant", depends_on=["first"])
    engine.submit(first)
    engine.submit(second)

    await asyncio.wait_for(engine.run_until_drained(poll_interval=0.002), timeout=1)

    assert first.status is JobStatus.FAILED
    assert second.status is JobStatus.FAILED


def test_engine_rejects_duplicate_job_ids():
    engine = _engine()
    engine.submit(Job(id="duplicate", tenant_id="tenant"))

    with pytest.raises(ValueError, match="already been submitted"):
        engine.submit(Job(id="duplicate", tenant_id="tenant"))
