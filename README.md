# durable-queue

This is a Python3.10 standard-library skeleton for a durable SQLite task queue.

Public API names are `Job`, `Lease`, `JobStore`, `AsyncWorker`, and `QueueError`. The implementation must support idempotent enqueue, ordered leasing, acknowledgement, retry, expired-lease recovery, and a bounded asyncio worker. The final project must keep all state in a caller-provided SQLite file and include runnable tests plus `examples/demo.py`.

The initial compatibility test can be run with:

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```