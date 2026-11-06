"""
LoadPilot — Workload models.

Core data structures: Job, Tenant, ResourceVector, and related enums/types.
"""

from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine


class JobStatus(enum.Enum):
    """Lifecycle states for a job."""

    PENDING = "pending"          # Submitted, not yet admitted
    ADMITTED = "admitted"        # Passed admission control, waiting to be scheduled
    SCHEDULED = "scheduled"     # Selected by scheduler, waiting for a worker
    RUNNING = "running"         # Executing on a worker
    COMPLETED = "completed"     # Finished successfully
    FAILED = "failed"           # Finished with an error
    REJECTED = "rejected"       # Denied by admission control
    DEFERRED = "deferred"       # Temporarily postponed (backpressure)
    CANCELLED = "cancelled"     # Cancelled by caller or timeout
    TIMED_OUT = "timed_out"     # Exceeded its deadline or execution timeout


class AdmissionDecision(enum.Enum):
    """Result of admission control evaluation."""

    ACCEPT = "accept"
    DEFER = "defer"
    REJECT = "reject"


class Priority(enum.IntEnum):
    """Job priority levels.  Higher numeric value = higher priority."""

    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


@dataclass(frozen=True, slots=True)
class ResourceVector:
    """
    Multi-dimensional resource requirement or capacity.

    Each dimension is a float so that fractional resources (e.g. 0.5 CPU) are
    supported.  A zero value means the dimension is unconstrained.
    """

    cpu: float = 0.0
    memory: float = 0.0      # megabytes
    slots: float = 1.0       # worker-slot cost (default: 1 slot per job)

    # ---- arithmetic helpers used by admission & resource manager ----

    def __add__(self, other: ResourceVector) -> ResourceVector:
        return ResourceVector(
            cpu=self.cpu + other.cpu,
            memory=self.memory + other.memory,
            slots=self.slots + other.slots,
        )

    def __sub__(self, other: ResourceVector) -> ResourceVector:
        return ResourceVector(
            cpu=self.cpu - other.cpu,
            memory=self.memory - other.memory,
            slots=self.slots - other.slots,
        )

    def fits_within(self, capacity: ResourceVector) -> bool:
        """Return True if *self* can be satisfied by *capacity*."""
        return (
            self.cpu <= capacity.cpu
            and self.memory <= capacity.memory
            and self.slots <= capacity.slots
        )

    def utilization_ratio(self, capacity: ResourceVector) -> float:
        """Return the maximum utilization ratio across all dimensions."""
        ratios: list[float] = []
        if capacity.cpu > 0:
            ratios.append(self.cpu / capacity.cpu)
        if capacity.memory > 0:
            ratios.append(self.memory / capacity.memory)
        if capacity.slots > 0:
            ratios.append(self.slots / capacity.slots)
        return max(ratios) if ratios else 0.0

    @classmethod
    def zero(cls) -> ResourceVector:
        return cls(cpu=0.0, memory=0.0, slots=0.0)


@dataclass(slots=True)
class Job:
    """
    A unit of work submitted to the engine.

    Parameters
    ----------
    id : str
        Unique identifier (auto-generated if empty).
    tenant_id : str
        Owning tenant.
    estimated_cpu : float
        Estimated CPU cost.
    estimated_memory : float
        Estimated memory cost (MB).
    priority : Priority | int
        Scheduling priority.
    deadline : float | None
        Absolute UNIX timestamp by which the job should complete.
        ``None`` means no deadline.
    timeout : float | None
        Maximum wall-clock seconds the job may execute.
    depends_on : list[str]
        Job IDs that must complete before this job can run.
    payload : Callable[..., Coroutine[Any, Any, Any]] | None
        The actual async callable to execute.  May be ``None`` for
        simulated / synthetic workloads.
    metadata : dict[str, Any]
        Arbitrary user-supplied metadata.
    """

    # ---- identity ----
    id: str = ""
    tenant_id: str = ""

    # ---- cost estimates ----
    estimated_cpu: float = 1.0
    estimated_memory: float = 64.0
    estimated_duration: float = 1.0   # seconds

    # ---- scheduling hints ----
    priority: Priority | int = Priority.NORMAL
    deadline: float | None = None
    timeout: float | None = None
    depends_on: list[str] = field(default_factory=list)

    # ---- execution ----
    payload: Callable[..., Coroutine[Any, Any, Any]] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    # ---- internal state (managed by the engine) ----
    status: JobStatus = field(default=JobStatus.PENDING, repr=False)
    submit_time: float = field(default=0.0, repr=False)
    admit_time: float = field(default=0.0, repr=False)
    schedule_time: float = field(default=0.0, repr=False)
    start_time: float = field(default=0.0, repr=False)
    end_time: float = field(default=0.0, repr=False)
    actual_cpu: float = field(default=0.0, repr=False)
    actual_memory: float = field(default=0.0, repr=False)
    result: Any = field(default=None, repr=False)
    error: BaseException | None = field(default=None, repr=False)
    attempt: int = field(default=0, repr=False)
    max_retries: int | None = field(default=None, repr=False)
    _decision_trace: list[str] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        if not self.id:
            self.id = uuid.uuid4().hex[:12]
        if isinstance(self.priority, int) and not isinstance(self.priority, Priority):
            self.priority = Priority(self.priority)
        if self.max_retries is not None and self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.submit_time == 0.0:
            self.submit_time = time.monotonic()

    # ---- convenience properties ----

    @property
    def resource_estimate(self) -> ResourceVector:
        return ResourceVector(
            cpu=self.estimated_cpu,
            memory=self.estimated_memory,
            slots=1.0,
        )

    @property
    def estimated_cost(self) -> float:
        """Scalar cost used by DRR deficit accounting."""
        return self.estimated_cpu * self.estimated_duration

    @property
    def wait_time(self) -> float:
        """Seconds between submission and execution start (0 if not started)."""
        if self.start_time > 0:
            return self.start_time - self.submit_time
        return 0.0

    @property
    def execution_time(self) -> float:
        """Seconds between start and end (0 if not finished)."""
        if self.end_time > 0 and self.start_time > 0:
            return self.end_time - self.start_time
        return 0.0

    @property
    def is_terminal(self) -> bool:
        return self.status in (
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.REJECTED,
            JobStatus.CANCELLED,
            JobStatus.TIMED_OUT,
        )

    def deadline_slack(self, now: float | None = None) -> float | None:
        """
        Remaining slack: ``deadline − now − estimated_duration``.

        Returns ``None`` if no deadline is set.
        """
        if self.deadline is None:
            return None
        now = now or time.monotonic()
        return self.deadline - now - self.estimated_duration

    def trace(self, message: str) -> None:
        """Append a decision trace entry."""
        self._decision_trace.append(message)


@dataclass(slots=True)
class Tenant:
    """
    A customer / workload owner.

    Parameters
    ----------
    id : str
        Unique tenant identifier.
    weight : float
        Relative scheduling weight (higher = larger share of capacity).
    max_concurrent : int | None
        Optional hard cap on concurrent running jobs for this tenant.
    max_queue_size : int | None
        Optional cap on queued (admitted but not running) jobs.
    resource_budget : ResourceVector | None
        Optional per-tenant resource ceiling.
    metadata : dict[str, Any]
        Arbitrary metadata.
    """

    id: str = ""
    weight: float = 1.0
    max_concurrent: int | None = None
    max_queue_size: int | None = None
    resource_budget: ResourceVector | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    # ---- internal accounting ----
    total_submitted: int = field(default=0, repr=False)
    total_completed: int = field(default=0, repr=False)
    total_rejected: int = field(default=0, repr=False)
    total_cpu_consumed: float = field(default=0.0, repr=False)
    total_memory_consumed: float = field(default=0.0, repr=False)
    current_running: int = field(default=0, repr=False)
    current_queued: int = field(default=0, repr=False)

    def __post_init__(self) -> None:
        if not self.id:
            self.id = uuid.uuid4().hex[:8]


@dataclass(frozen=True, slots=True)
class SchedulingDecision:
    """
    Captures *why* a job was selected (or skipped) during a scheduling round.
    """

    job_id: str
    tenant_id: str
    action: str                     # "execute", "defer", "skip", "reject"
    score: float = 0.0
    reason: str = ""
    fairness_debt: float = 0.0
    deadline_slack: float | None = None
    estimated_cost: float = 0.0
    system_pressure: float = 0.0


@dataclass(slots=True)
class EngineConfig:
    """Top-level configuration for the LoadPilot engine."""

    max_workers: int = 50
    cpu_budget: float = 100.0
    memory_budget: float = 4096.0

    # admission
    max_queue_size: int = 10_000
    admission_pressure_threshold: float = 0.85

    # scheduling
    default_quantum: float = 100.0
    deadline_weight: float = 2.0
    fairness_weight: float = 1.5
    cost_weight: float = 1.0
    pressure_weight: float = 1.0

    # adaptive controller
    initial_concurrency: int = 50
    min_concurrency: int = 5
    max_concurrency: int = 200
    latency_window_size: int = 50
    latency_target_ms: float = 500.0
    concurrency_step: int = 5
    adjustment_interval: float = 2.0     # seconds

    # retry
    default_max_retries: int = 2
    retry_base_delay: float = 0.5        # seconds
    retry_max_delay: float = 30.0        # seconds

    # backpressure
    load_shed_threshold: float = 0.95
    defer_threshold: float = 0.80

    @property
    def total_capacity(self) -> ResourceVector:
        return ResourceVector(
            cpu=self.cpu_budget,
            memory=self.memory_budget,
            slots=float(self.max_workers),
        )
