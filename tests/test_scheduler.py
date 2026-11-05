"""Tests for the scheduler module."""

from __future__ import annotations

import time
from collections import deque

import pytest

from loadpilot.scheduler.drr import DeficitRoundRobin
from loadpilot.scheduler.edf import EarliestDeadlineFirst
from loadpilot.scheduler.engine import SchedulerEngine
from loadpilot.scheduler.policies import (
    FIFOPolicy,
    LoadPilotPolicy,
    PriorityPolicy,
    SchedulerState,
)
from loadpilot.workload.models import (
    EngineConfig,
    Job,
    Priority,
    ResourceVector,
)


# ==================================================================
# DRR
# ==================================================================


class TestDeficitRoundRobin:
    def test_basic_round_robin(self):
        drr = DeficitRoundRobin({"a": 10, "b": 10})
        queues: dict[str, deque[Job]] = {
            "a": deque([Job(id="a1", tenant_id="a", estimated_cpu=5, estimated_duration=1)]),
            "b": deque([Job(id="b1", tenant_id="b", estimated_cpu=5, estimated_duration=1)]),
        }
        j1 = drr.select_next(queues)
        assert j1 is not None
        assert j1.id == "a1"

        j2 = drr.select_next(queues)
        assert j2 is not None
        assert j2.id == "b1"

    def test_expensive_job_needs_deficit(self):
        drr = DeficitRoundRobin({"a": 10})
        expensive_job = Job(
            id="exp", tenant_id="a",
            estimated_cpu=50, estimated_duration=1.0,  # cost = 50
        )
        queues = {"a": deque([expensive_job])}

        # First call: deficit = 10, cost = 50 → can't afford
        j = drr.select_next(queues)
        assert j is None

    def test_add_remove_tenant(self):
        drr = DeficitRoundRobin({})
        drr.add_tenant("x", 20)
        assert "x" in drr.tenants

        drr.remove_tenant("x")
        assert "x" not in drr.tenants

    def test_update_quantum(self):
        drr = DeficitRoundRobin({"a": 10})
        drr.update_quantum("a", 50)
        queues = {"a": deque([Job(id="a1", tenant_id="a", estimated_cpu=30, estimated_duration=1)])}
        j = drr.select_next(queues)
        assert j is not None  # quantum=50 >= cost=30

    def test_empty_queue_resets_deficit(self):
        drr = DeficitRoundRobin({"a": 100})
        queues: dict[str, deque[Job]] = {"a": deque()}
        drr.select_next(queues)
        assert drr.deficits["a"] == 0.0


# ==================================================================
# EDF
# ==================================================================


class TestEarliestDeadlineFirst:
    def test_pop_earliest(self):
        edf = EarliestDeadlineFirst()
        now = time.monotonic()
        j1 = Job(id="late", tenant_id="t", deadline=now + 100)
        j2 = Job(id="early", tenant_id="t", deadline=now + 10)
        edf.push(j1)
        edf.push(j2)
        result = edf.pop()
        assert result is not None
        assert result.id == "early"

    def test_no_deadline_raises_value_error(self):
        edf = EarliestDeadlineFirst()
        j = Job(id="no-dl", tenant_id="t")
        with pytest.raises(ValueError, match="has no deadline"):
            edf.push(j)
        assert edf.size == 0

    def test_remove(self):
        edf = EarliestDeadlineFirst()
        j = Job(id="r1", tenant_id="t", deadline=time.monotonic() + 10)
        edf.push(j)
        assert edf.size == 1
        assert edf.remove("r1")
        assert edf.size == 0

    def test_get_urgent(self):
        edf = EarliestDeadlineFirst()
        now = time.monotonic()
        j1 = Job(id="urgent", tenant_id="t", deadline=now + 1, estimated_duration=2.0)  # slack = -1
        j2 = Job(id="ok", tenant_id="t", deadline=now + 100, estimated_duration=1.0)
        edf.push(j1)
        edf.push(j2)
        urgent = edf.get_urgent(now=now, threshold=0)
        assert len(urgent) == 1
        assert urgent[0].id == "urgent"


# ==================================================================
# Policies
# ==================================================================


class TestSchedulingPolicies:
    def _make_state(self, **kwargs) -> SchedulerState:
        return SchedulerState(
            now=time.monotonic(),
            system_pressure=0.0,
            config=EngineConfig(),
            **kwargs,
        )

    def test_fifo(self):
        policy = FIFOPolicy()
        now = time.monotonic()
        j1 = Job(id="first", tenant_id="t")
        j1.submit_time = now
        j2 = Job(id="second", tenant_id="t")
        j2.submit_time = now + 1
        result = policy.select([j2, j1], self._make_state())
        assert result[0].id == "first"

    def test_priority(self):
        policy = PriorityPolicy()
        j_low = Job(id="low", tenant_id="t", priority=Priority.LOW)
        j_high = Job(id="high", tenant_id="t", priority=Priority.HIGH)
        result = policy.select([j_low, j_high], self._make_state())
        assert result[0].id == "high"

    def test_loadpilot_prefers_deadline(self):
        config = EngineConfig(deadline_weight=10.0)
        policy = LoadPilotPolicy(config)
        now = time.monotonic()

        j_deadline = Job(
            id="dl", tenant_id="t", priority=Priority.NORMAL,
            deadline=now + 2, estimated_duration=1.0,
        )
        j_normal = Job(id="norm", tenant_id="t", priority=Priority.NORMAL)

        state = self._make_state(
            fairness_debt={"t": 0.0},
        )
        result = policy.select([j_normal, j_deadline], state)
        assert result[0].id == "dl"


# ==================================================================
# SchedulerEngine
# ==================================================================


class TestSchedulerEngine:
    def test_enqueue_and_pending(self):
        engine = SchedulerEngine(EngineConfig())
        engine.register_tenant("t1")
        j = Job(id="j1", tenant_id="t1")
        engine.enqueue(j)
        assert engine.pending_count() == 1

    def test_select_batch(self):
        engine = SchedulerEngine(EngineConfig())
        engine.register_tenant("t1")
        for i in range(5):
            engine.enqueue(Job(id=f"j{i}", tenant_id="t1", estimated_cpu=1, estimated_memory=10))

        decisions = engine.select_batch(n=3)
        assert len(decisions) == 3
        assert all(d.action == "execute" for d in decisions)

    def test_remove_job(self):
        engine = SchedulerEngine(EngineConfig())
        engine.register_tenant("t1")
        j = Job(id="rm1", tenant_id="t1")
        engine.enqueue(j)
        removed = engine.remove_job("rm1")
        assert removed is not None
        assert engine.pending_count() == 0

    def test_duplicate_enqueue_raises(self):
        engine = SchedulerEngine(EngineConfig())
        engine.register_tenant("t1")
        j = Job(id="dup", tenant_id="t1")
        engine.enqueue(j)
        with pytest.raises(ValueError, match="already enqueued"):
            engine.enqueue(j)
