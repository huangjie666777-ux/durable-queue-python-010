"""End-to-end demo: enqueue, claim, ack and exponential-backoff retry.

Run with:

    PYTHONPATH=src python examples/demo.py
"""
from __future__ import annotations

import asyncio
import os
import tempfile

from durable_queue import AsyncWorker, Job, JobStore


async def main() -> None:
    fd, db_path = tempfile.mkstemp(prefix="durable-queue-demo-", suffix=".db")
    os.close(fd)
    try:
        with JobStore(db_path) as store:
            print(f"[demo] using database {db_path}")

            email_ok = store.enqueue(
                "tenant-acme", "emails", "user-42",
                {"to": "ada@example.com", "subject": "welcome"},
                idempotency_key="welcome-ada-v1",
            )
            email_fail = store.enqueue(
                "tenant-acme", "emails", "user-7",
                {"to": "grace@example.com", "subject": "retry me"},
                idempotency_key="welcome-grace-v1",
            )
            duplicate = store.enqueue(
                "tenant-acme", "emails", "user-42",
                {"to": "ada@example.com", "subject": "welcome"},
                idempotency_key="welcome-ada-v1",
            )
            print(f"[demo] enqueued jobs {email_ok.id} and {email_fail.id}; "
                  f"duplicate submit returned job {duplicate.id} (same id: {duplicate.id == email_ok.id})")

            attempts = {email_fail.id: 0}

            async def handler(job: Job) -> None:
                if job.id == email_fail.id:
                    attempts[job.id] += 1
                    if attempts[job.id] < 2:
                        print(f"[demo] attempt {attempts[job.id]} for job {job.id} failed")
                        raise RuntimeError("temporary SMTP outage")
                    print(f"[demo] attempt {attempts[job.id]} for job {job.id} succeeded")
                else:
                    print(f"[demo] job {job.id} ({job.payload['to']}) sent on first try")

            worker = AsyncWorker(
                store, "demo-worker", "emails",
                concurrency=2, lease_seconds=10.0, retry_base_delay=0.1,
            )

            async def stopper() -> None:
                while store.pending_count("emails") or any(
                    store.get(jid).state == "running" for jid in (email_ok.id, email_fail.id)
                ):
                    await asyncio.sleep(0.02)
                await asyncio.sleep(0.05)
                worker.stop()

            stop_task = asyncio.create_task(stopper())
            await worker.run(handler, poll_interval=0.02)
            await stop_task

            print(f"[demo] final states: {email_ok.id}={store.get(email_ok.id).state}, "
                  f"{email_fail.id}={store.get(email_fail.id).state}, "
                  f"attempts={store.get(email_fail.id).attempts}")
    finally:
        for suffix in ("", "-wal", "-shm"):
            path = db_path + suffix
            if os.path.exists(path):
                os.unlink(path)


if __name__ == "__main__":
    asyncio.run(main())
