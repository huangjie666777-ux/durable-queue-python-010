from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Job:
    id: int
    tenant: str
    queue: str
    key: str
    payload: dict[str, Any]
    idempotency_key: str
    available_at: float
    attempts: int
    max_attempts: int
    state: str
    created_at: float
    updated_at: float


@dataclass(frozen=True)
class Lease:
    job: Job
    worker_id: str
    token: str
    lease_until: float