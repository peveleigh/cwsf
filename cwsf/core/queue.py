"""Priority Job Queue for CWSF.

This module implements a thread-safe, priority-based job queue for managing
scraping tasks.
"""

import heapq
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from cwsf.core.job import Job, JobStatus

logger = logging.getLogger(__name__)


class PriorityJobQueue:
    """A thread-safe priority queue for Job objects.
    
    Jobs are ordered by priority (lower number = higher priority).
    Jobs with the same priority are ordered by their creation time (FIFO).
    """

    def __init__(self):
        self._lock = threading.Lock()
        # _heap stores (priority, created_at, job_id) to maintain priority and FIFO order
        self._heap: List[Tuple[int, datetime, str]] = []
        # _jobs maps job_id to the actual Job object
        self._jobs: Dict[str, Job] = {}
        # _running_jobs maps job_id to Job objects currently in RUNNING state
        self._running_jobs: Dict[str, Job] = {}

    def enqueue(self, job: Job) -> None:
        """Adds a job to the queue.
        
        If a job with the same job_id already exists in PENDING state, it is replaced.
        If it exists in RUNNING state, it is updated (but remains RUNNING).
        """
        with self._lock:
            if job.job_id in self._running_jobs:
                logger.info(f"Updating RUNNING job: {job.job_id}")
                self._running_jobs[job.job_id] = job
                return

            if job.job_id in self._jobs:
                logger.info(f"Updating PENDING job: {job.job_id}")
                # We don't remove from heap because it's inefficient.
                # Instead, we update the _jobs dict. dequeue() will handle stale heap entries.
                self._jobs[job.job_id] = job
            else:
                logger.info(f"Enqueuing new job: {job.job_id} (priority={job.priority})")
                self._jobs[job.job_id] = job
                heapq.heappush(self._heap, (job.priority, job.created_at, job.job_id))

    def dequeue(self) -> Optional[Job]:
        """Returns the highest-priority PENDING job and transitions it to RUNNING.
        
        Returns None if no PENDING jobs are available.
        """
        with self._lock:
            while self._heap:
                priority, created_at, job_id = heapq.heappop(self._heap)
                
                # Check if the job still exists and is still PENDING
                # (It might have been removed or updated)
                job = self._jobs.get(job_id)
                if job and job.status == JobStatus.PENDING:
                    # Transition to RUNNING
                    running_job = job.with_status(JobStatus.RUNNING)
                    del self._jobs[job_id]
                    self._running_jobs[job_id] = running_job
                    return running_job
            
            return None

    def remove(self, job_id: str) -> None:
        """Removes a PENDING job from the queue.
        
        If the job is currently RUNNING, it is marked CANCELLED.
        """
        with self._lock:
            if job_id in self._jobs:
                logger.info(f"Removing PENDING job: {job_id}")
                del self._jobs[job_id]
                # Note: stale entry remains in heap, handled by dequeue()
            elif job_id in self._running_jobs:
                logger.info(f"Cancelling RUNNING job: {job_id}")
                job = self._running_jobs[job_id]
                self._running_jobs[job_id] = job.with_status(JobStatus.CANCELLED)

    def update(self, job_id: str, new_config: Dict) -> None:
        """Replaces the config on a PENDING job.
        
        If the job is RUNNING, the update is deferred (the job object is updated
        in _running_jobs, but it's up to the orchestrator to check it).
        """
        with self._lock:
            if job_id in self._jobs:
                logger.info(f"Updating config for PENDING job: {job_id}")
                old_job = self._jobs[job_id]
                new_job = old_job.with_config(new_config)
                self._jobs[job_id] = new_job
                
                # If priority changed, we need to push a new entry to the heap
                if new_job.priority != old_job.priority:
                    heapq.heappush(self._heap, (new_job.priority, new_job.created_at, job_id))
            
            elif job_id in self._running_jobs:
                logger.info(f"Updating config for RUNNING job: {job_id} (deferred)")
                old_job = self._running_jobs[job_id]
                self._running_jobs[job_id] = old_job.with_config(new_config)

    def list_jobs(self) -> List[Job]:
        """Returns all jobs with their current status and priority."""
        with self._lock:
            return list(self._jobs.values()) + list(self._running_jobs.values())

    def size(self) -> int:
        """Returns the count of PENDING jobs."""
        with self._lock:
            # We can't just use len(self._heap) because of stale entries
            return len(self._jobs)

    def complete(self, job_id: str, success: bool = True) -> None:
        """Marks a RUNNING job as COMPLETED or FAILED."""
        with self._lock:
            if job_id in self._running_jobs:
                job = self._running_jobs.pop(job_id)
                new_status = JobStatus.COMPLETED if success else JobStatus.FAILED
                # In a real system, we might move this to a history list
                logger.info(f"Job {job_id} finished with status {new_status}")
