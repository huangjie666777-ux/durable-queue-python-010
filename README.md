# durable-queue

This is a Python3.10 standard-library skeleton for a durable SQLite task queue.

Public API names are `Job`, `Lease`, `JobStore`, `AsyncWorker`, and `QueueError`. The implementation must support idempotent enqueue, ordered leasing, acknowledgement, retry, expired-lease recovery, and a bounded asyncio worker. The final project must keep all state in a caller-provided SQLite file and include runnable tests plus `examples/demo.py`.

The initial compatibility test can be run with:

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

## Features

- Persistent jobs in a caller-provided SQLite file; every write runs in a
  `BEGIN IMMEDIATE` transaction with rollback on failure.
- Idempotent enqueue scoped by `(tenant, queue, idempotency_key)`; payload is
  stored as stable, key-sorted JSON. Conflicting business fields raise
  `IdempotencyConflict`, a subclass of `QueueError`.
- Atomic claim with `secrets`-generated tokens and expiry timestamps; jobs are
  ordered by database id and `available_at`, and a key is strictly serialised
  (no follower is claimed while an earlier job of the same key is pending or
  running).
- Ack requires the current, unexpired lease; stale workers, tokens, or expired
- leases raise `LeaseConflict` without changing state. Expired leases are
  recovered to `pending` via `recover_expired()` (also called each poll).
- Failure schedules deterministic exponential backoff
  `backoff_base * 2 ** (attempts - 1)`; after `max_attempts` the job becomes
  `dead`. Ack and terminal fail are idempotent for the owning lease; illegal
  transitions raise `InvalidTransition`.
- `AsyncWorker` provides bounded concurrency, exception-to-retry handling,
  event-driven wakeups (no lost wakeup on empty polls), graceful `stop()`, and
  cancellation that leaves no background tasks behind.
- Time comes from an injectable `clock` callable returning epoch seconds; the
  library never touches local time or process-global state.

## Run

```bash
PYTHONPATH=src python3 examples/demo.py
PYTHONPATH=src python3 -m unittest tests.test_compat tests.test_integration -v
```
