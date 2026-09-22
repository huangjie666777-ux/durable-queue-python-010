"""End-to-end demo on a real temporary SQLite file.

Shows idempotent enqueue, atomic claim, ack, exponential-backoff retry,
dead-lettering, and a bounded AsyncWorker with graceful shutdown.
"""
from __future__ import annotations

import asyncio
import os
import tempfile

from durable_queue import AsyncWorker, JobStore


def synchronous_part(db_path: str) -> None:
    clock_value = 1_000_000.0
    store = JobStore(db_path, clock=lambda: clock_value, backoff_base=1.0)

    print("== enqueue (idempotent) ==")
    job = store.enqueue(
        "tenant-a", "emails", "user:42",
        {"to": "alice@example.com", "subject": "welcome"},
        idempotency_key="email-42-welcome",
    )
    duplicate = store.enqueue(
        "tenant-a", "emails", "user:42",
        {"subject": "welcome", "to": "alice@example.com"},
        idempotency_key="email-42-welcome",
    )
    print(f"job id={job.id}; duplicate submission returned same id={duplicate.id}")

    print("\n== claim + ack ==")
    lease = store.claim("worker-1", "emails", lease_seconds=30)[0]
    print(f"leased job {lease.job.id} token={lease.token[:8]}... until={lease.lease_until:.0f}")
    done = store.ack(lease.job.id, "worker-1", lease.token)
    print(f"acked -> state={done.state}")

    print("\n== failure retry with exponential backoff ==")
    flaky = store.enqueue(
        "tenant-a", "emails", "user:43", {"to": "bob@example.com"},
        idempotency_key="email-43-welcome", max_attempts=3,
    )
    for attempt in range(1, 4):
        lease = store.claim("worker-1", "emails", lease_seconds=30)[0]
        result = store.fail(lease.job.id, "worker-1", lease.token, error="smtp down")
        print(f"attempt {attempt}: state={result.state} attempts={result.attempts}", end="")
        if result.state == "dead":
            print()
            break
        print(f" next_available_in={result.available_at - clock_value:.0f}s")
        clock_value = result.available_at

    print("\n== expired lease recovery ==")
    crashed = store.enqueue(
        "tenant-a", "emails", "user:44", {"to": "carol@example.com"},
        idempotency_key="email-44-welcome",
    )
    lease = store.claim("worker-crashed", "emails", lease_seconds=30)[0]
    clock_value = lease.lease_until + 1
    recovered = store.recover_expired(clock_value)
    print(f"recovered {recovered} expired job(s); job {crashed.id} "
          f"is now {store.get(crashed.id).state} with attempts={store.get(crashed.id).attempts}")
    store.close()


async def asynchronous_part(db_path: str) -> None:
    print("\n== AsyncWorker: bounded concurrency, handler errors, graceful stop ==")
    import time

    started = time.monotonic()
    store = JobStore(
        db_path,
        clock=lambda: 1_000_000.0 + time.monotonic() - started,
        backoff_base=0.01,
    )
    good = store.enqueue("tenant-b", "webhooks", "hook-1", {"url": "/ok"}, "h1")
    bad = store.enqueue(
        "tenant-b", "webhooks", "hook-2", {"url": "/bad"}, "h2", max_attempts=2,
    )
    outcomes: list[str] = []

    async def handler(job) -> None:
        if job.id == bad.id:
            raise RuntimeError(f"webhook {job.payload['url']} returned 500")
        outcomes.append(f"delivered {job.payload['url']} (attempt {job.attempts + 1})")

    worker = AsyncWorker(store, "async-worker", "webhooks", concurrency=4)
    run_task = asyncio.create_task(worker.run(handler, poll_interval=0.02))
    await asyncio.sleep(0.01)
    # Late submission wakes an idle poller immediately (no lost wakeup).
    late = store.enqueue("tenant-b", "webhooks", "hook-3", {"url": "/late"}, "h3")
    for _ in range(200):
        await asyncio.sleep(0.02)
        if store.get(good.id).state == "succeeded" and store.get(late.id).state == "succeeded":
            if store.get(bad.id).state == "dead":
                break
    for line in outcomes:
        print(f"  {line}")
    print(f"  bad job {bad.id} -> {store.get(bad.id).state} after {store.get(bad.id).attempts} attempts")
    worker.stop()
    await run_task
    print("worker stopped gracefully with no background tasks left")
    store.close()


def main() -> None:
    tmp_dir = tempfile.mkdtemp(prefix="durable-queue-demo-")
    db_path = os.path.join(tmp_dir, "demo.db")
    print(f"using sqlite database: {db_path}")
    synchronous_part(db_path)
    asyncio.run(asynchronous_part(db_path))


if __name__ == "__main__":
    main()
