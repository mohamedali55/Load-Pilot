"""
LoadPilot — AsyncExecutor with retry support.

Manages the full job execution lifecycle: submission, timeout handling,
exponential-backoff retry, and completion notification.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable

from loadpilot.runtime.worker import WorkerPool
from loadpilot.workload.models import Job, JobStatus

logger = logging.getLogger(__name__)


class RetryPolicy:
    """Exponential-backoff retry policy with jitter.

    Parameters
    ----------
    max_retries:
        Maximum number of retry attempts (excludes the initial attempt).
    base_delay:
        Base delay in seconds for the first retry.
    max_delay:
        Upper bound on the computed delay.
    """

    __slots__ = ("max_retries", "base_delay", "max_delay")

    def __init__(
        self,
        max_retries: int = 2,
        base_delay: float = 0.5,
        max_delay: float = 30.0,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if base_delay < 0:
            raise ValueError("base_delay must be >= 0")
        if max_delay < 0:
            raise ValueError("max_delay must be >= 0")
        self.max_retries: int = max_retries
        self.base_delay: float = base_delay
        self.max_delay: float = max_delay

    def should_retry(self, job: Job) -> bool:
        """Return ``True`` if the job still has retry budget remaining."""
        return job.attempt <= self.max_retries

    def delay(self, attempt: int) -> float:
        """Compute delay for the given *attempt* (0-indexed).

        Uses exponential backoff with full jitter::

            min(base_delay * 2^attempt + random(0, base_delay), max_delay)
        """
        raw = self.base_delay * (2 ** attempt) + random.uniform(0, self.base_delay)
        return min(raw, self.max_delay)


class AsyncExecutor:
    """Manages job execution lifecycle with retry and timeout.

    Parameters
    ----------
    pool:
        The ``WorkerPool`` to submit jobs to.
    retry_policy:
        The ``RetryPolicy`` governing retries on failure.
    """

    __slots__ = (
        "_pool",
        "_retry",
        "_pending_retries",
        "_retry_count",
        "_completion_handlers",
        "_retry_task",
        "_shutdown_event",
    )

    def __init__(self, pool: WorkerPool, retry_policy: RetryPolicy) -> None:
        self._pool: WorkerPool = pool
        self._retry: RetryPolicy = retry_policy
        self._pending_retries: asyncio.Queue[tuple[float, Job]] = asyncio.Queue()
        self._retry_count: int = 0
        self._completion_handlers: list[Callable[[Job], Awaitable[None]]] = []
        self._retry_task: asyncio.Task[None] | None = None
        self._shutdown_event: asyncio.Event = asyncio.Event()

    @property
    def pending_retry_count(self) -> int:
        """Number of retries queued or currently waiting to run."""
        return self._retry_count

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def execute(self, job: Job) -> None:
        """Execute a single *job* through the worker pool.

        1. Mark status as ``RUNNING`` and record ``start_time``.
        2. Submit to the pool and await completion.
        3. On failure, enqueue for retry if the policy permits.
        4. Invoke completion handlers when the job reaches a terminal state.
        """
        job.status = JobStatus.RUNNING
        if job.start_time == 0.0:
            job.start_time = time.monotonic()

        task = await self._pool.submit(job)

        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            # If the executor itself is cancelled, let the task keep
            # running but propagate cancellation upward.
            if not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
            if job.status in (
                JobStatus.FAILED,
                JobStatus.TIMED_OUT,
                JobStatus.CANCELLED,
            ):
                await self._notify_handlers(job)
            raise

        # --- post-execution logic ---
        await self._handle_result(job)

    async def _handle_result(self, job: Job, *, retrying: bool = False) -> None:
        """Decide whether to retry or finalize a completed job."""
        if job.status == JobStatus.COMPLETED:
            await self._notify_handlers(job)
            if retrying:
                self._retry_count -= 1
            return

        if job.status in (JobStatus.FAILED, JobStatus.TIMED_OUT):
            if self._retry.should_retry(job):
                delay = self._retry.delay(job.attempt - 1)
                job.trace(
                    f"Retry scheduled (attempt {job.attempt + 1}/"
                    f"{self._retry.max_retries + 1}) after {delay:.2f}s"
                )
                logger.info(
                    "Job %s queued for retry (attempt %d) after %.2fs",
                    job.id,
                    job.attempt + 1,
                    delay,
                )
                await self._pending_retries.put((delay, job))
                if not retrying:
                    self._retry_count += 1
                return

            # Exhausted retries — finalise.
            job.trace("Retries exhausted")
            logger.warning(
                "Job %s exhausted retries (%d attempts)", job.id, job.attempt
            )

        # Terminal states (FAILED, TIMED_OUT, CANCELLED) without retry.
        await self._notify_handlers(job)
        if retrying:
            self._retry_count -= 1

    # ------------------------------------------------------------------
    # Batch execution
    # ------------------------------------------------------------------

    async def execute_batch(self, jobs: list[Job]) -> list[asyncio.Task[None]]:
        """Execute multiple *jobs* concurrently.

        Returns a list of ``asyncio.Task`` objects — one per job.
        """
        tasks: list[asyncio.Task[None]] = [
            asyncio.create_task(self.execute(job), name=f"exec-{job.id}")
            for job in jobs
        ]
        return tasks

    # ------------------------------------------------------------------
    # Completion handlers
    # ------------------------------------------------------------------

    def on_completion(self, handler: Callable[[Job], Awaitable[None]]) -> None:
        """Register an async handler invoked when a job reaches a terminal
        state (after retries are exhausted or on success)."""
        self._completion_handlers.append(handler)

    async def _notify_handlers(self, job: Job) -> None:
        """Call every registered completion handler."""
        for handler in self._completion_handlers:
            try:
                await handler(job)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Completion handler error for job %s", job.id
                )

    # ------------------------------------------------------------------
    # Retry processor
    # ------------------------------------------------------------------

    async def process_retries(self) -> None:
        """Background loop that dequeues and re-executes failed jobs.

        Runs until ``shutdown()`` is called or the task is cancelled.
        """
        logger.info("Retry processor started")
        while not self._shutdown_event.is_set():
            try:
                item = await asyncio.wait_for(
                    self._pending_retries.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            delay, job = item
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                if self._shutdown_event.is_set():
                    job.status = JobStatus.FAILED
                    job.error = RuntimeError("Retry abandoned during shutdown")
                    job.end_time = time.monotonic()
                    job.trace("Abandoned during shutdown")
                    await self._notify_handlers(job)
                    self._retry_count -= 1
                else:
                    self._pending_retries.put_nowait(item)
                raise

            # Reset transient state for the new attempt.
            job.status = JobStatus.RUNNING
            job.error = None
            job.result = None
            job.start_time = time.monotonic()
            job.end_time = 0.0

            logger.info("Retrying job %s (attempt %d)", job.id, job.attempt + 1)
            try:
                task = await self._pool.submit(job)
                await task
            except asyncio.CancelledError:
                if job.status == JobStatus.CANCELLED:
                    await self._notify_handlers(job)
                    self._retry_count -= 1
                break
            except Exception:  # noqa: BLE001
                logger.exception("Unexpected error retrying job %s", job.id)

            await self._handle_result(job, retrying=True)

        logger.info("Retry processor stopped")

    def start_retry_processor(self) -> asyncio.Task[None]:
        """Launch the retry processor as a background task and return it."""
        if self._retry_task is not None and not self._retry_task.done():
            return self._retry_task
        self._retry_task = asyncio.create_task(
            self.process_retries(), name="retry-processor"
        )
        return self._retry_task

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Gracefully shut down the executor.

        1. Signal the retry processor to stop.
        2. Drain remaining retry queue entries (mark them as FAILED).
        3. Delegate to the worker pool's shutdown.
        """
        self._shutdown_event.set()

        # Stop the retry processor task.
        if self._retry_task is not None and not self._retry_task.done():
            self._retry_task.cancel()
            try:
                await self._retry_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass

        # Drain the retry queue — anything still pending is abandoned.
        while not self._pending_retries.empty():
            try:
                _, job = self._pending_retries.get_nowait()
                job.status = JobStatus.FAILED
                job.end_time = time.monotonic()
                job.trace("Abandoned during shutdown")
                await self._notify_handlers(job)
                self._retry_count -= 1
            except asyncio.QueueEmpty:
                break

        await self._pool.shutdown(timeout=timeout)
