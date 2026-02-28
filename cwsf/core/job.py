"""Job data model for CWSF.

This module defines the Job data model and JobStatus enum used by the
queue and orchestrator to manage scraping tasks.
"""

import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional


class JobStatus(Enum):
    """Status of a scraping job."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class FailureContext:
    """Context for a failed request after retry exhaustion."""
    site_name: str
    url: str
    http_status: Optional[int]
    error_type: str
    error_message: str
    retries_attempted: int
    timestamp: str  # ISO 8601


@dataclass(frozen=True)
class Job:
    """Represents a scraping job derived from a site configuration.
    
    Two jobs are considered equal if their job_id values match.
    """
    site_name: str
    config: Dict[str, Any]
    job_id: str = field(default_factory=lambda: "")  # Will be set in __post_init__
    priority: int = 10
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self):
        """Initialize job_id and priority if not provided."""
        # job_id is derived from site_name as per Story 2.4
        if not self.job_id:
            # We use object.__setattr__ because the dataclass is frozen
            object.__setattr__(self, 'job_id', self.site_name)
        
        # Priority from config if present, otherwise use default
        config_priority = self.config.get('priority')
        if config_priority is not None:
            object.__setattr__(self, 'priority', int(config_priority))

    def __eq__(self, other):
        if not isinstance(other, Job):
            return False
        return self.job_id == other.job_id

    def __hash__(self):
        return hash(self.job_id)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the job to a dictionary."""
        data = asdict(self)
        data['status'] = self.status.value
        data['created_at'] = self.created_at.isoformat()
        data['updated_at'] = self.updated_at.isoformat()
        return data

    def with_status(self, status: JobStatus) -> 'Job':
        """Return a new Job instance with the updated status."""
        return Job(
            site_name=self.site_name,
            config=self.config,
            job_id=self.job_id,
            priority=self.priority,
            status=status,
            created_at=self.created_at,
            updated_at=datetime.now(timezone.utc)
        )

    def with_config(self, config: Dict[str, Any]) -> 'Job':
        """Return a new Job instance with the updated config."""
        priority = config.get('priority', self.priority)
        return Job(
            site_name=self.site_name,
            config=config,
            job_id=self.job_id,
            priority=int(priority),
            status=self.status,
            created_at=self.created_at,
            updated_at=datetime.now(timezone.utc)
        )
