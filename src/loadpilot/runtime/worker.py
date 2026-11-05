"""
LoadPilot — Worker and WorkerPool.

Provides async workers that execute jobs and a pool that manages concurrency.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from loadpilot.workload.models import Job, JobStatus

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WorkerStats:
    """Cumulative statistics for a single worker."""

    total_executed: int = 0
    total_failed: int = 0
    total_cpu_time: float = 0.0
    last_job_id: str | None = None


class Worker:
    """A single async worker that executes jobs.

    Parameters
    ----------
    worker_id:
        Unique integer identifying this worker within the pool.
    """

    __slots__ = ("id", "busy", "current_job", "stats")

    def __init__(self, worker_id: int) -> None:
        self.id: int = worker_id
        self.busy: bool = False
        self.current_job: Job | None = None
        self.stats: WorkerStats = WorkerStats()

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def execute(self, job: Job) -> None:
        """Run *job* on this worker.

        * If ``job.payload`` is a callable returning a coroutine, invoke it
          and await the result with an optional ``job.timeout``.
        * If ``job.payload`` is ``None`` the worker *simulates* work by
          sleeping for ``job.estimated_duration`` seconds.

        The method updates ``job.status``, ``job.actual_cpu``,
        ``job.actual_memory``, and ``job.error`` accordingly.
        """
        self.busy = True
        self.current_job = job
        job.status = JobStatus.RUNNING
        job.start_time = time.monotonic()
        job.attempt += 1

        exec_start = time.monotonic()
        try:
            if job.payload is not None:
                coro = job.payload()
                if job.timeout is not None and job.timeout > 0:
                    job.result = await asyncio.wait_for(
                        coro, timeout=job.timeout
                    )
                else:
                    job.result = await coro
            else:
                # Simulated workload — honour timeout if set.
                sleep_duration = job.estimated_duration
                if job.timeout is not None and job.timeout > 0:
                    await asyncio.wait_for(
                        asyncio.sleep(sleep_duration), timeout=job.timeout
                    )
                else:
                    await asyncio.sleep(sleep_duration)

            elapsed = time.monotonic() - exec_start
            job.actual_cpu = elapsed * job.estimated_cpu
            job.actual_memory = job.estimated_memory
            job.status = JobStatus.COMPLETED
            job.end_time = time.monotonic()

            self.stats.total_executed += 1
            self.stats.total_cpu_time += job.actual_cpu

        except asyncio.TimeoutError:
            job.status = JobStatus.TIMED_OUT
            job.end_time = time.monotonic()
            job.error = TimeoutError(
                f"Job {job.id} timed out after {job.timeout}s"
            )
            self.stats.total_failed += 1
            logger.warning("Job %s timed out on worker %d", job.id, self.id)

        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
            job.end_time = time.monotonic()
            self.stats.total_failed += 1
            logger.info("Job %s cancelled on worker %d", job.id, self.id)
            raise  # Re-raise so the asyncio task is properly cancelled.

        except Exception as exc:  # noqa: BLE001
            job.status = JobStatus.FAILED
            job.end_time = time.monotonic()
            job.error = exc
            self.stats.total_failed += 1
            logger.error(
                "Job %s failed on worker %d: %s", job.id, self.id, exc
            )

        finally:
            self.stats.last_job_id = job.id
            self.busy = False
            self.current_job = None


class WorkerPool:
    """Pool of async workers with concurrency control.

    Parameters
    ----------
    max_workers:
        Maximum number of concurrent job executions.
    """

    __slots__ = (
        "_max_workers",
        "_running_count",
        "_condition",
        "_workers",
        "_active_tasks",
        "_completion_callbacks",
        "_next_worker_id",
        "_lock",
    )

    def __init__(self, max_workers: int) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        self._max_workers: int = max_workers
        self._running_count: int = 0
        self._condition: asyncio.Condition = asyncio.Condition()
        self._workers: dict[int, Worker] = {}
        self._active_tasks: dict[str, asyncio.Task[None]] = {}
        self._completion_callbacks: list[Callable[[Job], Any]] = []
        self._next_worker_id: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def active_count(self) -> int:
        """Number of currently executing jobs."""
        return len(self._active_tasks)

    @property
    def available_count(self) -> int:
        """Number of free worker slots."""
        return max(0, self._max_workers - self.active_count)

    @property
    def utilization(self) -> float:
        """Fraction of the pool that is busy, in ``[0.0, 1.0]``."""
        if self._max_workers == 0:
            return 0.0
        return min(1.0, self.active_count / self._max_workers)

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------

    async def submit(self, job: Job) -> asyncio.Task[None]:
        """Submit a *job* for execution.

        Launches the job asynchronously and returns its ``asyncio.Task``.
        The task waits until a worker slot is available before executing.
        """
        if job.id in self._active_tasks:
            raise ValueError(f"Job {job.id!r} is already active")
        task = asyncio.create_task(self._run(job), name=f"job-{job.id}")
        self._active_tasks[job.id] = task
        task.add_done_callback(lambda completed: self._finalize_unstarted(job, completed))
        return task

    async def _run(self, job: Job) -> None:
        """Wait for a slot, execute, then release it."""
        acquired = False
        try:
            async with self._condition:
                await self._condition.wait_for(
                    lambda: self._running_count < self._max_workers
                )
                self._running_count += 1
                acquired = True
            worker = self._acquire_worker()
            try:
                await worker.execute(job)
            finally:
                self._release_worker(worker)
        except asyncio.CancelledError:
            if job.status not in (
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.TIMED_OUT,
                JobStatus.CANCELLED,
            ):
                job.status = JobStatus.CANCELLED
                job.end_time = time.monotonic()
            raise
        finally:
            if acquired:
                async with self._condition:
                    self._running_count -= 1
                    self._condition.notify_all()
            if self._active_tasks.pop(job.id, None) is not None:
                self._fire_completion(job)

    def _finalize_unstarted(self, job: Job, task: asyncio.Task[None]) -> None:
        """Clean up a task cancelled before its coroutine had a chance to run."""
        if self._active_tasks.get(job.id) is not task:
            return
        self._active_tasks.pop(job.id)
        if task.cancelled() and job.status not in (
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.TIMED_OUT,
            JobStatus.CANCELLED,
        ):
            job.status = JobStatus.CANCELLED
            job.end_time = time.monotonic()
        self._fire_completion(job)

    def _acquire_worker(self) -> Worker:
        """Find an idle worker or create a new one."""
        for w in self._workers.values():
            if not w.busy:
                return w
        wid = self._next_worker_id
        self._next_worker_id += 1
        worker = Worker(wid)
        self._workers[wid] = worker
        return worker

    def _release_worker(self, worker: Worker) -> None:
        """Mark a worker as idle (the Worker.execute finally block does
        this too, but we keep explicit bookkeeping here)."""
        worker.busy = False

    def _fire_completion(self, job: Job) -> None:
        """Invoke registered completion callbacks."""
        for cb in self._completion_callbacks:
            try:
                cb(job)
            except Exception:  # noqa: BLE001
                logger.exception("Completion callback error for job %s", job.id)

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    async def cancel(self, job_id: str) -> bool:
        """Cancel a running job by its *job_id*.

        Returns ``True`` if the task was found and cancelled.
        """
        task = self._active_tasks.get(job_id)
        if task is None:
            return False
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        return True

    async def cancel_all(self) -> int:
        """Cancel every active task.  Returns the number cancelled."""
        tasks = list(self._active_tasks.values())
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        return len(tasks)

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    def on_completion(self, callback: Callable[[Job], None]) -> None:
        """Register a synchronous callback invoked when a job finishes."""
        self._completion_callbacks.append(callback)

    # ------------------------------------------------------------------
    # Dynamic resizing
    # ------------------------------------------------------------------

    def resize(self, new_max: int) -> None:
        """Adjust the maximum concurrency of the pool.

        Running jobs are *not* interrupted — the new limit only affects
        future submissions.
        """
        if new_max < 1:
            raise ValueError("new_max must be >= 1")
        old_max = self._max_workers
        self._max_workers = new_max
        async def notify_waiters() -> None:
            async with self._condition:
                self._condition.notify_all()

        try:
            asyncio.get_running_loop().create_task(notify_waiters())
        except RuntimeError:
            pass
        logger.info("WorkerPool resized: %d -> %d", old_max, new_max)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Graceful shutdown.

        1. Wait up to *timeout* seconds for running jobs to finish.
        2. Cancel any that remain.
        """
        if not self._active_tasks:
            return

        tasks = list(self._active_tasks.values())
        _done, pending = await asyncio.wait(tasks, timeout=timeout)

        if pending:
            logger.warning(
                "Shutdown: %d tasks still running after %.1fs — cancelling",
                len(pending),
                timeout,
            )
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    # ------------------------------------------------------------------
    # Telemetry
    # ------------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Return a dictionary describing the current pool state."""
        return {
            "max_workers": self._max_workers,
            "active_count": self.active_count,
            "available_count": self.available_count,
            "utilization": round(self.utilization, 4),
            "total_workers_created": len(self._workers),
            "active_job_ids": list(self._active_tasks.keys()),
            "workers": {
                wid: {
                    "busy": w.busy,
                    "total_executed": w.stats.total_executed,
                    "total_failed": w.stats.total_failed,
                    "total_cpu_time": round(w.stats.total_cpu_time, 4),
                    "last_job_id": w.stats.last_job_id,
                }
                for wid, w in self._workers.items()
            },
        }
