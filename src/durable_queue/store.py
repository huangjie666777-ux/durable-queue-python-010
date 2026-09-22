from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable, Mapping
from typing import Any

from .models import Job, Lease


class JobStore:
    """SQLite-backed task store. The model must complete the state machine."""

    def __init__(self, db_path: str, clock: Callable[[], float] | None = None) -> None:
        self.db_path = db_path
        self.clock = clock or time.time
        self._conn = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant TEXT NOT NULL,
                queue TEXT NOT NULL,
                task_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                available_at REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                state TEXT NOT NULL,
                lease_worker TEXT,
                lease_token TEXT,
                lease_until REAL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(tenant, queue, idempotency_key)
            );
            """
        )

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "JobStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def enqueue(self, tenant: str, queue: str, key: str, payload: Mapping[str, Any], idempotency_key: str, max_attempts: int = 3, available_at: float | None = None) -> Job:
        raise NotImplementedError

    def claim(self, worker_id: str, queue: str, limit: int = 1, lease_seconds: float = 30.0) -> list[Lease]:
        raise NotImplementedError

    def ack(self, job_id: int, worker_id: str, token: str) -> Job:
        raise NotImplementedError

    def fail(self, job_id: int, worker_id: str, token: str, error: str = "") -> Job:
        raise NotImplementedError

    def recover_expired(self, now: float | None = None) -> int:
        raise NotImplementedError

    def get(self, job_id: int) -> Job | None:
        raise NotImplementedError

    def pending_count(self, queue: str | None = None) -> int:
        if queue is None:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM jobs WHERE state = 'pending'").fetchone()
        else:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM jobs WHERE state = 'pending' AND queue = ?", (queue,)).fetchone()
        return int(row["n"])