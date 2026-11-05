"""
LoadPilot — Earliest Deadline First scheduler.

Maintains a min-heap of jobs keyed by their absolute deadline so that
the most time-critical work can be dispatched first.
"""

from __future__ import annotations

import heapq
from typing import Final

from loadpilot.workload.models import Job


# Tombstone sentinel: deleted entries stay in the heap but are skipped.
_REMOVED: Final[str] = "__REMOVED__"


class EarliestDeadlineFirst:
    """Earliest Deadline First (EDF) priority queue.

    Only jobs that have a non-``None`` deadline are accepted.  Internally the
    heap stores ``(deadline, insertion_counter, job)`` triples; the counter
    breaks ties in FIFO order.
    """

    __slots__ = ("_heap", "_entry_map", "_counter", "_size")

    def __init__(self) -> None:
        self._heap: list[list] = []                 # min-heap of [deadline, seq, job | _REMOVED]
        self._entry_map: dict[str, list] = {}       # job_id → heap entry
        self._counter: int = 0                      # tie-breaker, monotonically increasing
        self._size: int = 0                         # live (non-tombstoned) count

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def push(self, job: Job) -> None:
        """Add a job to the EDF queue.

        Parameters
        ----------
        job : Job
            Must have a non-``None`` ``deadline``.

        Raises
        ------
        ValueError
            If the job has no deadline.
        """
        if job.deadline is None:
            raise ValueError(
                f"Job {job.id!r} has no deadline; cannot add to EDF queue"
            )

        # If this job_id is already tracked, remove the old entry first.
        if job.id in self._entry_map:
            self._remove_entry(job.id)

        entry: list = [job.deadline, self._counter, job]
        self._counter += 1
        self._entry_map[job.id] = entry
        heapq.heappush(self._heap, entry)
        self._size += 1

    def pop(self) -> Job | None:
        """Remove and return the job with the earliest deadline.

        Returns ``None`` if the queue is empty.
        """
        while self._heap:
            _deadline, _seq, job = heapq.heappop(self._heap)
            if job is not _REMOVED:
                del self._entry_map[job.id]
                self._size -= 1
                return job
        return None

    def peek(self) -> Job | None:
        """Return the job with the earliest deadline without removing it.

        Returns ``None`` if the queue is empty.
        """
        while self._heap:
            entry = self._heap[0]
            if entry[2] is not _REMOVED:
                return entry[2]
            # Discard stale tombstone at the top.
            heapq.heappop(self._heap)
        return None

    def remove(self, job_id: str) -> bool:
        """Remove the job with *job_id* from the queue.

        Returns ``True`` if the job was found and removed, ``False`` otherwise.
        """
        if job_id not in self._entry_map:
            return False
        self._remove_entry(job_id)
        return True

    def get_urgent(self, now: float, threshold: float) -> list[Job]:
        """Return all jobs whose deadline slack is below *threshold*.

        Slack is defined as ``deadline − now − estimated_duration``.  Jobs
        with slack below *threshold* are considered urgent.

        The returned list is sorted by deadline (earliest first).  The jobs
        are **not** removed from the queue.
        """
        urgent: list[tuple[float, Job]] = []
        for entry in self._heap:
            deadline, _seq, job = entry
            if job is _REMOVED:
                continue
            slack = deadline - now - job.estimated_duration
            if slack < threshold:
                urgent.append((deadline, job))
        # Sort by deadline ascending.
        urgent.sort(key=lambda t: t[0])
        return [job for _, job in urgent]

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def size(self) -> int:
        """Number of live jobs in the queue."""
        return self._size

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _remove_entry(self, job_id: str) -> None:
        """Mark an entry as removed (lazy deletion)."""
        entry = self._entry_map.pop(job_id)
        entry[2] = _REMOVED
        self._size -= 1
