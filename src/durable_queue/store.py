from __future__ import annotations

import json
import sqlite3
import time
import secrets
from collections.abc import Callable, Mapping
from typing import Any

from .errors import IdempotencyConflict, InvalidTransition, LeaseConflict
from .models import Job, Lease


def _canonical_payload(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _row_to_job(row: sqlite3.Row) -> Job:
    return Job(
        id=int(row["id"]),
        tenant=row["tenant"],
        queue=row["queue"],
        key=row["task_key"],
        payload=json.loads(row["payload_json"]),
        idempotency_key=row["idempotency_key"],
        available_at=float(row["available_at"]),
        attempts=int(row["attempts"]),
        max_attempts=int(row["max_attempts"]),
        state=row["state"],
        created_at=float(row["created_at"]),
        updated_at=float(row["updated_at"]),
    )


class JobStore:
    """SQLite-backed task store. The model must complete the state machine."""

    def __init__(self, db_path: str, clock: Callable[[], float] | None = None) -> None:
        self.db_path = db_path
        self.clock = clock or time.time
        self._wake_callbacks: list[Callable[[], None]] = []
        self._conn = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA busy_timeout = 5000")
        self._conn.execute("PRAGMA journal_mode = WAL")
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
                last_fail_token TEXT,
                UNIQUE(tenant, queue, idempotency_key)
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_claim
                ON jobs(queue, state, available_at);
            CREATE INDEX IF NOT EXISTS idx_jobs_ordering
                ON jobs(tenant, queue, task_key, id);
            """
        )
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(jobs)")}
        if "last_fail_token" not in cols:
            self._conn.execute("ALTER TABLE jobs ADD COLUMN last_fail_token TEXT")

    def close(self) -> None:
        self._wake_callbacks.clear()
        self._conn.close()

    def __enter__(self) -> "JobStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def add_wake_callback(self, callback: Callable[[], None]) -> None:
        """Register a callback fired when new work may be available."""
        self._wake_callbacks.append(callback)

    def remove_wake_callback(self, callback: Callable[[], None]) -> None:
        try:
            self._wake_callbacks.remove(callback)
        except ValueError:
            pass

    def _notify_waiters(self) -> None:
        for callback in list(self._wake_callbacks):
            try:
                callback()
            except Exception:
                pass

    def enqueue(self, tenant: str, queue: str, key: str, payload: Mapping[str, Any], idempotency_key: str, max_attempts: int = 3, available_at: float | None = None) -> Job:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        now = self.clock()
        if available_at is None:
            available_at = now
        payload_json = _canonical_payload(payload)
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE tenant = ? AND queue = ? AND idempotency_key = ?",
                (tenant, queue, idempotency_key),
            ).fetchone()
            if row is not None:
                existing = _row_to_job(row)
                if existing.key != key or row["payload_json"] != payload_json:
                    raise IdempotencyConflict(
                        f"idempotency_key {idempotency_key!r} already exists with different business fields"
                    )
                job = existing
                self._conn.execute("COMMIT")
                return job
            cur = self._conn.execute(
                """
                INSERT INTO jobs (
                    tenant, queue, task_key, payload_json, idempotency_key,
                    available_at, attempts, max_attempts, state,
                    lease_worker, lease_token, lease_until,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, 'pending', NULL, NULL, NULL, ?, ?)
                """,
                (
                    tenant, queue, key, payload_json, idempotency_key,
                    available_at, max_attempts, now, now,
                ),
            )
            job_id = cur.lastrowid
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        self._notify_waiters()
        result = self.get(int(job_id))
        assert result is not None
        return result

    def claim(self, worker_id: str, queue: str, limit: int = 1, lease_seconds: float = 30.0) -> list[Lease]:
        if limit < 1:
            return []
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be > 0")
        now = self.clock()
        lease_until = now + lease_seconds
        leases: list[Lease] = []
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            rows = self._conn.execute(
                """
                SELECT * FROM jobs j
                WHERE j.queue = ?
                  AND j.state = 'pending'
                  AND j.available_at <= ?
                  AND NOT EXISTS (
                      SELECT 1 FROM jobs p
                      WHERE p.tenant = j.tenant
                        AND p.queue = j.queue
                        AND p.task_key = j.task_key
                        AND p.id < j.id
                        AND p.state IN ('pending', 'running')
                  )
                ORDER BY j.available_at ASC, j.id ASC
                LIMIT ?
                """,
                (queue, now, limit),
            ).fetchall()
            for row in rows:
                token = secrets.token_urlsafe(32)
                self._conn.execute(
                    """
                    UPDATE jobs
                       SET state = 'running', lease_worker = ?, lease_token = ?,
                           lease_until = ?, updated_at = ?
                     WHERE id = ? AND state = 'pending'
                    """,
                    (worker_id, token, lease_until, now, row["id"]),
                )
                updated = self._conn.execute(
                    "SELECT * FROM jobs WHERE id = ?", (row["id"],)
                ).fetchone()
                assert updated is not None
                job = _row_to_job(updated)
                leases.append(Lease(job=job, worker_id=worker_id, token=token, lease_until=lease_until))
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return leases

    def _load_running(self, job_id: int, worker_id: str, token: str) -> Job | None:
        row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        job = _row_to_job(row)
        if row["lease_worker"] != worker_id or row["lease_token"] != token:
            raise LeaseConflict(f"worker {worker_id!r} does not hold the current lease for job {job_id}")
        if job.state != "running":
            raise InvalidTransition(f"job {job_id} is {job.state}, not running")
        return job

    def ack(self, job_id: int, worker_id: str, token: str) -> Job:
        now = self.clock()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                raise InvalidTransition(f"job {job_id} does not exist")
            same_lease = row["lease_worker"] == worker_id and row["lease_token"] == token
            if row["state"] == "succeeded":
                job = _row_to_job(row)
                self._conn.execute("COMMIT")
                if not same_lease:
                    raise LeaseConflict(f"worker {worker_id!r} does not hold the lease for job {job_id}")
                return job
            if row["state"] == "dead":
                self._conn.execute("COMMIT")
                raise InvalidTransition(f"job {job_id} is dead and cannot be acknowledged")
            if not same_lease:
                self._conn.execute("ROLLBACK")
                raise LeaseConflict(f"worker {worker_id!r} does not hold the current lease for job {job_id}")
            job = self._load_running(job_id, worker_id, token)
            assert job is not None
            self._conn.execute(
                "UPDATE jobs SET state = 'succeeded', updated_at = ? WHERE id = ?",
                (now, job_id),
            )
            self._conn.execute("COMMIT")
        except Exception:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            raise
        result = self.get(job_id)
        assert result is not None
        self._notify_waiters()
        return result

    def fail(self, job_id: int, worker_id: str, token: str, error: str = "", retry_base_delay: float = 1.0) -> Job:
        now = self.clock()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                raise InvalidTransition(f"job {job_id} does not exist")
            if row["state"] == "pending":
                self._conn.execute("COMMIT")
                if row["last_fail_token"] != token:
                    # Leased work was recovered after expiry; stale callers
                    # must not be able to mutate the job.
                    raise LeaseConflict(
                        f"worker {worker_id!r} no longer holds the lease for job {job_id}"
                    )
                return _row_to_job(row)
            if row["state"] == "dead":
                self._conn.execute("COMMIT")
                return _row_to_job(row)
            same_lease = row["lease_worker"] == worker_id and row["lease_token"] == token
            if not same_lease:
                self._conn.execute("ROLLBACK")
                raise LeaseConflict(f"worker {worker_id!r} does not hold the current lease for job {job_id}")
            job = self._load_running(job_id, worker_id, token)
            assert job is not None
            attempts = job.attempts + 1
            if attempts >= job.max_attempts:
                new_state = "dead"
                available_at = job.available_at
            else:
                new_state = "pending"
                available_at = now + retry_base_delay * (2 ** (attempts - 1))
            self._conn.execute(
                """
                UPDATE jobs
                   SET state = ?, attempts = ?, available_at = ?,
                       lease_worker = NULL, lease_token = NULL, lease_until = NULL,
                       updated_at = ?, last_fail_token = ?
                WHERE id = ?
                """,
                (new_state, attempts, available_at, now, token, job_id),
            )
            self._conn.execute("COMMIT")
        except Exception:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            raise
        result = self.get(job_id)
        assert result is not None
        self._notify_waiters()
        return result

    def recover_expired(self, now: float | None = None) -> int:
        if now is None:
            now = self.clock()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            cur = self._conn.execute(
                """
                UPDATE jobs
                   SET state = 'pending',
                       lease_worker = NULL, lease_token = NULL, lease_until = NULL,
                       last_fail_token = NULL,
                       available_at = MIN(available_at, ?),
                       updated_at = ?
                WHERE state = 'running' AND lease_until IS NOT NULL AND lease_until <= ?
                """,
                (now, now, now),
            )
            recovered = cur.rowcount if cur.rowcount is not None else 0
            self._conn.execute("COMMIT")
        except Exception:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
            raise
        if recovered:
            self._notify_waiters()
        return int(recovered)

    def get(self, job_id: int) -> Job | None:
        row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return _row_to_job(row) if row is not None else None

    def pending_count(self, queue: str | None = None) -> int:
        if queue is None:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM jobs WHERE state = 'pending'").fetchone()
        else:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM jobs WHERE state = 'pending' AND queue = ?", (queue,)).fetchone()
        return int(row["n"])
