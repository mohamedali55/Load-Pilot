"""Tests for adaptive concurrency and backpressure control."""

from __future__ import annotations

import pytest

from loadpilot.controller.adaptive import (
    AdaptiveConcurrencyController,
    ConcurrencySignal,
    LatencyWindow,
)
from loadpilot.controller.backpressure import (
    BackpressureConfig,
    BackpressureController,
    PressureLevel,
    classify_pressure,
)
from loadpilot.engine import Engine
from loadpilot.workload.models import (
    AdmissionDecision,
    EngineConfig,
    Job,
    Priority,
)


def test_latency_window_percentiles_and_gradient():
    window = LatencyWindow(max_size=8)
    for latency in (10, 10, 10, 10, 20, 20, 20, 20):
        window.record(latency)

    assert window.count == 8
    assert window.p50 == 15
    assert window.p95 == 20
    assert window.gradient() == 2
    with pytest.raises(ValueError, match="percentile"):
        window.percentile(101)


def test_adaptive_controller_decreases_on_rising_high_latency():
    controller = AdaptiveConcurrencyController(
        initial_concurrency=20,
        min_concurrency=5,
        max_concurrency=30,
        latency_target_ms=100,
        window_size=8,
        step=2,
        decrease_multiplier=2,
        cooldown_seconds=0,
    )
    for latency in (100, 100, 100, 100, 200, 200, 200, 200):
        controller.record_latency(latency)

    assert controller.adjust() is ConcurrencySignal.DECREASE
    assert controller.limit == 16
    assert controller.history[-1].latency_gradient == 2


def test_adaptive_controller_increases_bounded_by_maximum():
    controller = AdaptiveConcurrencyController(
        initial_concurrency=9,
        min_concurrency=1,
        max_concurrency=10,
        latency_target_ms=100,
        window_size=8,
        step=5,
        cooldown_seconds=0,
    )
    for _ in range(8):
        controller.record_latency(50)
    controller.set_queue_pressure(0.5)

    assert controller.adjust() is ConcurrencySignal.INCREASE
    assert controller.limit == 10
    assert controller.system_pressure == 0.5


def test_adaptive_controller_validates_configuration_and_pressure():
    with pytest.raises(ValueError, match="within configured bounds"):
        AdaptiveConcurrencyController(initial_concurrency=20, max_concurrency=10)

    controller = AdaptiveConcurrencyController()
    with pytest.raises(ValueError, match="finite"):
        controller.set_queue_pressure(float("nan"))


def test_backpressure_classification_and_priority_shedding():
    assert classify_pressure(0.69) is PressureLevel.ELEVATED
    controller = BackpressureController(
        BackpressureConfig(
            defer_threshold=0.5,
            shed_low_threshold=0.7,
            shed_normal_threshold=0.85,
            shed_all_threshold=0.95,
            queue_soft_limit=10,
            queue_hard_limit=20,
        )
    )
    controller.update(system_pressure=0.75, queue_depth=0)

    assert (
        controller.evaluate(Job(id="low", tenant_id="t", priority=Priority.LOW))
        is AdmissionDecision.REJECT
    )
    assert (
        controller.evaluate(Job(id="high", tenant_id="t", priority=Priority.HIGH))
        is AdmissionDecision.ACCEPT
    )
    assert controller.state().shed_count == 1


def test_engine_applies_backpressure_policy_to_admission():
    engine = Engine(config=EngineConfig(max_queue_size=10))
    engine._backpressure = BackpressureController(
        BackpressureConfig(queue_soft_limit=1, queue_hard_limit=10)
    )

    first = engine.submit(Job(id="first", tenant_id="t", priority=Priority.NORMAL))
    second = engine.submit(Job(id="second", tenant_id="t", priority=Priority.LOW))

    assert first is AdmissionDecision.ACCEPT
    assert second is AdmissionDecision.DEFER
    assert engine.metrics.jobs_deferred.value == 1
