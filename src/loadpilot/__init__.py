"""LoadPilot — A unified cost-aware scheduling engine."""

from loadpilot.workload.models import (
    AdmissionDecision,
    EngineConfig,
    Job,
    JobStatus,
    Priority,
    ResourceVector,
    SchedulingDecision,
    Tenant,
)

__all__ = [
    "AdmissionDecision",
    "Engine",
    "EngineConfig",
    "Job",
    "JobStatus",
    "Priority",
    "ResourceVector",
    "SchedulingDecision",
    "Tenant",
]

__version__ = "0.1.0"


def __getattr__(name: str):
    # Lazy import Engine to avoid circular imports during module init
    if name == "Engine":
        from loadpilot.engine import Engine
        return Engine
    raise AttributeError(f"module 'loadpilot' has no attribute {name!r}")
