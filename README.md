# durable-queue

This is a Python3.10 standard-library skeleton for a durable SQLite task queue.

Public API names are `Job`, `Lease`, `JobStore`, `AsyncWorker`, and `QueueError`. The implementation must support idempotent enqueue, ordered leasing, acknowledgement, retry, expired-lease recovery, and a bounded asyncio worker. The final project must keep all state in a caller-provided SQLite file and include runnable tests plus `examples/demo.py`.

The initial compatibility test can be run with:

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

## Features

- Durable SQLite storage on a caller-provided file path; an injectable monotonic-style `clock` supplies all timestamps (no local-time or process-global dependencies).
- Idempotent `enqueue(tenant, queue, key, payload, idempotency_key, ...)`: the same `(tenant, queue, idempotency_key)` returns the original job; conflicting `key`/payload raises `IdempotencyConflict`. Payloads are stored as stable, sorted JSON.
- Atomic `claim(worker_id, queue, limit, lease_seconds)` using `BEGIN IMMEDIATE`: each lease carries an unguessable token and an expiry. Tasks stay strictly ordered per `(tenant, queue, key)` while predecessors are `pending` or `running`.
- `ack` / `fail` only succeed for the current lease holder; stale workers/tokens raise `LeaseConflict` without mutating state. Failures reschedule with deterministic exponential backoff (`retry_base_delay * 2**(attempts-1)`); exhausting `max_attempts` marks the job `dead`.
- `recover_expired` returns crashed `running` jobs to `pending`, preserving attempts and ordering.
- `AsyncWorker` offers bounded asyncio concurrency, automatic ack/fail on handler outcome, event-driven wakeups (no lost empty polls), graceful `stop()` that drains in-flight tasks, and external cancellation support.

## Demo

```bash
PYTHONPATH=src python examples/demo.py
```
