from __future__ import annotations

import json
import secrets
import sqlite3
import time
from collections.abc import Callable, Mapping
from typing import Any

from .errors import IdempotencyConflict, InvalidTransition, LeaseConflict
from .models import Job, Lease


def _canonical_payload(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


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

    def __init__(
        self,
        db_path: str,
        clock: Callable[[], float] | None = None,
        *,
        backoff_base: float = 1.0,
        on_change: Callable[[], None] | None = None,
    ) -> None:
        self.db_path = db_path
        self.clock = clock or time.time
        self.backoff_base = backoff_base
        self._on_change = on_change
        self._listeners: set[Callable[[], None]] = set()
        self._conn = sqlite3.connect(
            db_path,
            isolation_level=None,
            check_same_thread=False,
            timeout=10.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout = 10000")
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

            CREATE INDEX IF NOT EXISTS idx_jobs_due
                ON jobs(queue, state, available_at);

            CREATE INDEX IF NOT EXISTS idx_jobs_key_order
                ON jobs(tenant, queue, task_key, id, state);
            """
        )

    def _notify(self) -> None:
        if self._on_change is not None:
            self._on_change()
        for listener in tuple(self._listeners):
            listener()

    def subscribe(self, listener: Callable[[], None]) -> Callable[[], None]:
        """Register a change listener; return an unsubscribe callable."""
        self._listeners.add(listener)

        def unsubscribe() -> None:
            self._listeners.discard(listener)

        return unsubscribe

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "JobStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def enqueue(
        self,
        tenant: str,
        queue: str,
        key: str,
        payload: Mapping[str, Any],
        idempotency_key: str,
        max_attempts: int = 3,
        available_at: float | None = None,
    ) -> Job:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        now = self.clock()
        if available_at is None:
            available_at = now
        payload_json = _canonical_payload(payload)
        conn = self._conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM jobs WHERE tenant = ? AND queue = ? AND idempotency_key = ?",
                (tenant, queue, idempotency_key),
            ).fetchone()
            if existing is not None:
                if (
                    existing["task_key"] != key
                    or existing["payload_json"] != payload_json
                    or int(existing["max_attempts"]) != max_attempts
                ):
                    raise IdempotencyConflict(
                        f"idempotency_key {idempotency_key!r} was already submitted "
                        "with different business fields"
                    )
                conn.execute("COMMIT")
                return _row_to_job(existing)
            cur = conn.execute(
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
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        self._notify()
        return _row_to_job(row)

    def claim(
        self,
        worker_id: str,
        queue: str,
        limit: int = 1,
        lease_seconds: float = 30.0,
        tenant: str | None = None,
    ) -> list[Lease]:
        if limit < 1:
            return []
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be > 0")
        now = self.clock()
        lease_until = now + lease_seconds
        conn = self._conn
        leases: list[Lease] = []
        tenant_clause = "AND j.tenant = ?" if tenant is not None else ""
        params: list[Any] = [now, queue]
        if tenant is not None:
            params.append(tenant)
        try:
            conn.execute("BEGIN IMMEDIATE")
            for _ in range(limit):
                row = conn.execute(
                    f"""
                    SELECT j.* FROM jobs j
                    WHERE j.state = 'pending'
                      AND j.available_at <= ?
                      AND j.queue = ?
                      {tenant_clause}
                      AND NOT EXISTS (
                          SELECT 1 FROM jobs p
                          WHERE p.tenant = j.tenant
                            AND p.queue = j.queue
                            AND p.task_key = j.task_key
                            AND p.id < j.id
                            AND p.state IN ('pending', 'running')
                      )
                    ORDER BY j.id, j.available_at
                    LIMIT 1
                    """,
                    params,
                ).fetchone()
                if row is None:
                    break
                token = secrets.token_hex(24)
                conn.execute(
                    """
                    UPDATE jobs
                    SET state = 'running', lease_worker = ?, lease_token = ?,
                        lease_until = ?, updated_at = ?
                    WHERE id = ? AND state = 'pending'
                    """,
                    (worker_id, token, lease_until, now, row["id"]),
                )
                updated = conn.execute(
                    "SELECT * FROM jobs WHERE id = ?", (row["id"],)
                ).fetchone()
                job = _row_to_job(updated)
                leases.append(
                    Lease(job=job, worker_id=worker_id, token=token, lease_until=lease_until)
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        if leases:
            self._notify()
        return leases

    def _locked_job_with_valid_lease(
        self, job_id: int, worker_id: str, token: str
    ) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        if row is None:
            raise InvalidTransition(f"job {job_id} does not exist")
        if row["state"] != "running":
            raise InvalidTransition(
                f"job {job_id} is in state {row['state']!r}, expected 'running'"
            )
        if row["lease_worker"] != worker_id or row["lease_token"] != token:
            raise LeaseConflict(
                f"lease for job {job_id} does not belong to worker {worker_id!r} "
                "or token is stale"
            )
        if float(row["lease_until"]) <= self.clock():
            raise LeaseConflict(f"lease for job {job_id} has expired")
        return row

    def ack(self, job_id: int, worker_id: str, token: str) -> Job:
        now = self.clock()
        conn = self._conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                raise InvalidTransition(f"job {job_id} does not exist")
            if row["state"] == "succeeded":
                if row["lease_worker"] != worker_id or row["lease_token"] != token:
                    raise LeaseConflict(
                        f"job {job_id} already succeeded under a different lease"
                    )
                conn.execute("COMMIT")
                return _row_to_job(row)
            self._locked_job_with_valid_lease(job_id, worker_id, token)
            conn.execute(
                """
                UPDATE jobs
                SET state = 'succeeded', lease_until = NULL, updated_at = ?
                WHERE id = ?
                """,
                (now, job_id),
            )
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        self._notify()
        return _row_to_job(row)

    def fail(self, job_id: int, worker_id: str, token: str, error: str = "") -> Job:
        now = self.clock()
        conn = self._conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                raise InvalidTransition(f"job {job_id} does not exist")
            if row["state"] == "dead":
                if row["lease_worker"] != worker_id or row["lease_token"] != token:
                    raise LeaseConflict(
                        f"job {job_id} is dead under a different lease"
                    )
                conn.execute("COMMIT")
                return _row_to_job(row)
            self._locked_job_with_valid_lease(job_id, worker_id, token)
            attempts = int(row["attempts"]) + 1
            max_attempts = int(row["max_attempts"])
            if attempts >= max_attempts:
                conn.execute(
                    """
                    UPDATE jobs
                    SET state = 'dead', attempts = ?, lease_until = NULL, updated_at = ?
                    WHERE id = ?
                    """,
                    (attempts, now, job_id),
                )
            else:
                retry_at = now + self.backoff_base * (2 ** (attempts - 1))
                conn.execute(
                    """
                    UPDATE jobs
                    SET state = 'pending', attempts = ?,
                        available_at = ?,
                        lease_worker = NULL, lease_token = NULL, lease_until = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (attempts, retry_at, now, job_id),
                )
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        self._notify()
        return _row_to_job(row)

    def recover_expired(self, now: float | None = None) -> int:
        if now is None:
            now = self.clock()
        conn = self._conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """
                UPDATE jobs
                SET state = 'pending',
                    lease_worker = NULL, lease_token = NULL, lease_until = NULL,
                    available_at = MIN(available_at, ?),
                    updated_at = ?
                WHERE state = 'running' AND lease_until <= ?
                """,
                (now, now, now),
            )
            recovered = cur.rowcount or 0
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        if recovered:
            self._notify()
        return recovered

    def get(self, job_id: int) -> Job | None:
        row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return _row_to_job(row) if row is not None else None

    def pending_count(self, queue: str | None = None) -> int:
        if queue is None:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM jobs WHERE state = 'pending'").fetchone()
        else:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM jobs WHERE state = 'pending' AND queue = ?", (queue,)).fetchone()
        return int(row["n"])
