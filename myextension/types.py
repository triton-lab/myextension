import json
from datetime import datetime
from enum import Enum, auto
from typing import NamedTuple


class JobStatus(Enum):
    OPENED = auto()
    PREPARING = auto()
    RUNNING = auto()
    STOPPING = auto()
    TERMINATED = auto()
    EMPTY = auto()  # either just started or after termination
    STALE = auto()  # when instance ID is no longer found
    UNKNOWN = auto()


class JobStatusEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, JobStatus):
            return obj.name
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class JobMetadata(NamedTuple):
    job_id: str
    name: str
    file_path: str
    timestamp: datetime
    request_id: str
    instance_id: str
    instance_type: str
    extra: str


class JobInfo(NamedTuple):
    job_id: str
    name: str
    file_path: str
    timestamp: datetime
    request_id: str
    instance_id: str
    instance_type: str
    extra: str
    status: JobStatus
    console_output: str


def to_status(request_state: str, instance_state: str, console: str) -> JobStatus:
    if request_state == "open":
        status = JobStatus.OPENED
    elif instance_state == "pending":
        status = JobStatus.PREPARING
    elif instance_state == "running":
        status = JobStatus.RUNNING
    elif instance_state in ("shutting-down", "stopping"):
        status = JobStatus.STOPPING
    elif instance_state in ("terminated", "stopped"):
        status = JobStatus.TERMINATED
    elif instance_state == "info-empty" and console:
        status = JobStatus.TERMINATED
    elif instance_state == "info-empty":
        # Either (1) right after instance creation
        #    or  (2) long after instance termination
        status = JobStatus.EMPTY
    elif instance_state == "notfound-id":
        status = JobStatus.STALE
    else:
        # TODO: Add log saying something is wrong
        status = JobStatus.UNKNOWN
    return status