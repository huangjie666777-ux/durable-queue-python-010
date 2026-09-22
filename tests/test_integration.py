from __future__ import annotations

import asyncio
import concurrent.futures
import os
import tempfile
import unittest

from durable_queue import AsyncWorker, Job, JobStore, QueueError
from durable_queue.errors import IdempotencyConflict, InvalidTransition, LeaseConflict


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class StoreTestBase(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.clock = FakeClock()
        self.store = JobStore(self.db_path, clock=self.clock)

    def tearDown(self) -> None:
        self.store.close()
        for suffix in ("", "-wal", "-shm"):
            path = self.db_path + suffix
            if os.path.exists(path):
                os.unlink(path)


class IdempotencyTests(StoreTestBase):
    def test_duplicate_idempotency_key_returns_same_job(self) -> None:
        j1 = self.store.enqueue("t1", "q1", "k1", {"a": 1}, "idem-1")
        j2 = self.store.enqueue("t1", "q1", "k1", {"a": 1}, "idem-1")
        self.assertEqual(j1, j2)
        self.assertEqual(self.store.pending_count(), 1)

    def test_conflicting_fields_raise_distinct_error(self) -> None:
        self.store.enqueue("t1", "q1", "k1", {"a": 1}, "idem-1")
        with self.assertRaises(IdempotencyConflict):
            self.store.enqueue("t1", "q1", "k1", {"a": 2}, "idem-1")
        with self.assertRaises(IdempotencyConflict):
            self.store.enqueue("t1", "q1", "other-key", {"a": 1}, "idem-1")
        self.assertIsInstance(IdempotencyConflict("x"), QueueError)

    def test_same_key_allowed_in_other_tenant_or_queue(self) -> None:
        j1 = self.store.enqueue("t1", "q1", "k", {"a": 1}, "idem")
        j2 = self.store.enqueue("t2", "q1", "k", {"a": 1}, "idem")
        j3 = self.store.enqueue("t1", "q2", "k", {"a": 1}, "idem")
        self.assertEqual(len({j1.id, j2.id, j3.id}), 3)

    def test_payload_uses_stable_json(self) -> None:
        j1 = self.store.enqueue("t1", "q1", "k", {"b": 2, "a": 1}, "idem")
        j2 = self.store.enqueue("t1", "q1", "k", {"a": 1, "b": 2}, "idem")
        self.assertEqual(j1.id, j2.id)
        row = self.store._conn.execute(
            "SELECT payload_json FROM jobs WHERE id = ?", (j1.id,)
        ).fetchone()
        self.assertEqual(row["payload_json"], '{"a":1,"b":2}')

    def test_transaction_rollback_on_conflict(self) -> None:
        self.store.enqueue("t1", "q1", "k1", {"a": 1}, "idem-1")
        with self.assertRaises(IdempotencyConflict):
            self.store.enqueue("t1", "q1", "k2", {"a": 2}, "idem-1")
        self.assertEqual(self.store.pending_count(), 1)
        self.assertFalse(self.store._conn.in_transaction)


class PersistenceTests(StoreTestBase):
    def test_jobs_survive_reopen(self) -> None:
        j1 = self.store.enqueue(
            "t1", "q1", "k1", {"x": 1}, "i1", available_at=self.clock() + 10
        )
        j2 = self.store.enqueue("t1", "q1", "k1", {"x": 2}, "i2")
        self.assertEqual(self.store.claim("w1", "q1"), [])
        self.clock.advance(10)
        leases = self.store.claim("w1", "q1")
        self.assertEqual([l.job.id for l in leases], [j1.id])
        self.store.close()

        reopened = JobStore(self.db_path, clock=self.clock)
        self.assertEqual(reopened.get(j2.id).state, "pending")
        running = reopened.get(j1.id)
        self.assertEqual(running.state, "running")
        self.assertEqual(running.attempts, 0)
        self.assertEqual(reopened.pending_count("q1"), 1)
        reopened.close()

    def test_context_manager_closes_connection(self) -> None:
        with JobStore(self.db_path, clock=self.clock) as store:
            store.enqueue("t", "q", "k", {}, "i")
        with JobStore(self.db_path, clock=self.clock) as store:
            self.assertEqual(store.pending_count(), 1)


class ClaimOrderingTests(StoreTestBase):
    def test_strict_per_key_ordering(self) -> None:
        j1 = self.store.enqueue("t1", "q1", "k1", {}, "i1")
        j2 = self.store.enqueue("t1", "q1", "k1", {}, "i2")
        other = self.store.enqueue("t1", "q1", "k2", {}, "i3")
        leases = self.store.claim("w1", "q1", limit=10)
        self.assertEqual({l.job.id for l in leases}, {j1.id, other.id})
        self.assertNotIn(j2.id, [l.job.id for l in leases])

        token1 = next(l.token for l in leases if l.job.id == j1.id)
        self.store.ack(j1.id, "w1", token1)
        leases = self.store.claim("w1", "q1", limit=10)
        self.assertEqual([l.job.id for l in leases], [j2.id])

    def test_claim_order_by_available_at_then_id(self) -> None:
        later = self.store.enqueue("t", "q", "ka", {}, "ia", available_at=self.clock() + 10)
        now1 = self.store.enqueue("t", "q", "kb", {}, "ib")
        now2 = self.store.enqueue("t", "q", "kc", {}, "ic")
        self.clock.advance(5)
        leases = self.store.claim("w", "q", limit=10)
        self.assertEqual([l.job.id for l in leases], [now1.id, now2.id])
        self.clock.advance(6)
        leases = self.store.claim("w", "q", limit=10)
        self.assertEqual([l.job.id for l in leases], [later.id])

    def test_future_dated_predecessor_blocks_successor_until_due(self) -> None:
        first = self.store.enqueue("t", "q", "k", {}, "i1", available_at=self.clock() + 100)
        second = self.store.enqueue("t", "q", "k", {}, "i2")
        leases = self.store.claim("w", "q", limit=10)
        self.assertEqual(leases, [])
        self.clock.advance(100)
        leases = self.store.claim("w", "q", limit=10)
        self.assertEqual([l.job.id for l in leases], [first.id])

    def test_concurrent_workers_do_not_double_claim(self) -> None:
        ids = [self.store.enqueue("t", "q", f"k{i}", {}, f"i{i}").id for i in range(6)]

        def claim(worker: str) -> list[int]:
            local = JobStore(self.db_path, clock=self.clock)
            try:
                return [l.job.id for l in local.claim(worker, "q", limit=3)]
            finally:
                local.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            results = [job_id for got in pool.map(claim, ["w1", "w2", "w3"]) for job_id in got]
        self.assertEqual(sorted(results), ids)

    def test_lease_fields_are_set(self) -> None:
        self.store.enqueue("t", "q", "k", {}, "i")
        [lease] = self.store.claim("w", "q")
        self.assertGreaterEqual(len(lease.token), 32)
        self.assertEqual(lease.worker_id, "w")
        self.assertGreater(lease.lease_until, self.clock())
        job = self.store.get(lease.job.id)
        self.assertEqual(job.state, "running")
        self.assertEqual(self.store.pending_count(), 0)


class AckFailRetryTests(StoreTestBase):
    def _claim_one(self, worker: str = "w"):
        return self.store.claim(worker, "q", limit=1)[0]

    def test_ack_success_is_idempotent(self) -> None:
        j = self.store.enqueue("t", "q", "k", {}, "i")
        lease = self._claim_one()
        done = self.store.ack(j.id, "w", lease.token)
        self.assertEqual(done.state, "succeeded")
        again = self.store.ack(j.id, "w", lease.token)
        self.assertEqual(again.state, "succeeded")
        self.assertEqual(again.updated_at, done.updated_at)

    def test_stale_worker_and_token_cannot_ack(self) -> None:
        j = self.store.enqueue("t", "q", "k", {}, "i")
        lease = self._claim_one("w1")
        with self.assertRaises(LeaseConflict):
            self.store.ack(j.id, "w2", lease.token)
        with self.assertRaises(LeaseConflict):
            self.store.ack(j.id, "w1", "bogus-token")
        self.assertEqual(self.store.get(j.id).state, "running")
        self.clock.advance(31)
        self.assertEqual(self.store.recover_expired(), 1)
        with self.assertRaises(LeaseConflict):
            self.store.ack(j.id, "w1", lease.token)
        self.assertEqual(self.store.get(j.id).state, "pending")

    def test_fail_with_stale_token_after_expiry_fails(self) -> None:
        j = self.store.enqueue("t", "q", "k", {}, "i")
        lease = self._claim_one("w1")
        self.clock.advance(31)
        self.store.recover_expired()
        with self.assertRaises(LeaseConflict):
            self.store.fail(j.id, "w1", lease.token)
        self.assertEqual(self.store.get(j.id).attempts, 0)

    def test_exponential_backoff_retry_then_dead(self) -> None:
        j = self.store.enqueue("t", "q", "k", {}, "i", max_attempts=3)
        lease = self._claim_one()
        failed = self.store.fail(j.id, "w", lease.token, error="boom", retry_base_delay=2.0)
        self.assertEqual(failed.state, "pending")
        self.assertEqual(failed.attempts, 1)
        self.assertAlmostEqual(failed.available_at, self.clock() + 2.0)
        self.assertEqual(self.store.claim("w", "q"), [])

        self.clock.advance(2.0)
        lease = self._claim_one()
        failed = self.store.fail(j.id, "w", lease.token, retry_base_delay=2.0)
        self.assertEqual(failed.state, "pending")
        self.assertEqual(failed.attempts, 2)
        self.assertAlmostEqual(failed.available_at, self.clock() + 4.0)

        same = self.store.fail(j.id, "w", lease.token, retry_base_delay=2.0)
        self.assertEqual(same, failed)

        self.clock.advance(4.0)
        lease = self._claim_one()
        dead = self.store.fail(j.id, "w", lease.token, retry_base_delay=2.0)
        self.assertEqual(dead.state, "dead")
        self.assertEqual(dead.attempts, 3)
        with self.assertRaises(InvalidTransition):
            self.store.ack(j.id, "w", lease.token)
        again = self.store.fail(j.id, "w", lease.token)
        self.assertEqual(again.state, "dead")
        self.assertEqual(again.attempts, 3)

    def test_invalid_transitions_raise_queue_error(self) -> None:
        j = self.store.enqueue("t", "q", "k", {}, "i")
        with self.assertRaises(LeaseConflict):
            self.store.ack(j.id, "w", "x")
        with self.assertRaises(QueueError):
            self.store.ack(j.id + 999, "w", "x")
        with self.assertRaises(QueueError):
            self.store.fail(j.id + 999, "w", "x")

    def test_recovery_preserves_attempts_and_order(self) -> None:
        j1 = self.store.enqueue("t", "q", "k", {}, "i1")
        j2 = self.store.enqueue("t", "q", "k", {}, "i2")
        l1 = self._claim_one()
        self.store.fail(j1.id, "w", l1.token, retry_base_delay=5.0)
        self.clock.advance(6)
        l1b = self.store.claim("w", "q")[0]
        self.assertEqual(l1b.job.id, j1.id)
        self.assertEqual(l1b.job.attempts, 1)
        self.assertEqual(self.store.claim("w", "q"), [])
        self.clock.advance(31)
        self.assertEqual(self.store.recover_expired(), 1)
        recovered = self.store.get(j1.id)
        self.assertEqual(recovered.state, "pending")
        self.assertEqual(recovered.attempts, 1)
        l1c = self.store.claim("w", "q")[0]
        self.assertEqual(l1c.job.id, j1.id)
        self.store.ack(j1.id, "w", l1c.token)
        l2 = self.store.claim("w", "q")[0]
        self.assertEqual(l2.job.id, j2.id)

    def test_recover_expired_is_idempotent(self) -> None:
        self.store.enqueue("t", "q", "k", {}, "i")
        self._claim_one()
        self.clock.advance(31)
        self.assertEqual(self.store.recover_expired(), 1)
        self.assertEqual(self.store.recover_expired(), 0)


class AsyncWorkerTests(StoreTestBase):
    def test_run_once_success_and_failure(self) -> None:
        async def scenario() -> None:
            ok = self.store.enqueue("t", "q", "ok", {}, "i1")
            bad = self.store.enqueue("t", "q", "bad", {}, "i2")

            async def handler(job: Job) -> None:
                if job.id == bad.id:
                    raise RuntimeError("handler exploded")

            worker = AsyncWorker(
                self.store, "worker-1", "q", concurrency=2, retry_base_delay=0.01
            )
            count = await worker.run_once(handler)
            self.assertEqual(count, 2)
            self.assertEqual(self.store.get(ok.id).state, "succeeded")
            failed_job = self.store.get(bad.id)
            self.assertEqual(failed_job.state, "pending")
            self.assertEqual(failed_job.attempts, 1)

        asyncio.run(scenario())

    def test_graceful_stop_waits_for_inflight(self) -> None:
        async def scenario() -> None:
            self.store.enqueue("t", "q", "k", {}, "i1")
            started = asyncio.Event()
            release = asyncio.Event()

            async def slow_handler(job: Job) -> None:
                started.set()
                await release.wait()

            worker = AsyncWorker(self.store, "worker-1", "q")
            task = asyncio.create_task(worker.run(slow_handler, poll_interval=0.02))
            await asyncio.wait_for(started.wait(), timeout=2.0)
            worker.stop()
            await asyncio.sleep(0.05)
            self.assertFalse(task.done())
            release.set()
            await asyncio.wait_for(task, timeout=2.0)
            self.assertIsNone(task.exception())

        asyncio.run(scenario())

    def test_wakeup_on_enqueue_no_lost_notification(self) -> None:
        async def scenario() -> None:
            worker = AsyncWorker(self.store, "worker-1", "q", concurrency=1)
            claimed: list[object] = []

            async def real_handler(job: Job) -> None:
                claimed.append(job.id)

            task = asyncio.create_task(worker.run(real_handler, poll_interval=5.0))
            await asyncio.sleep(0.1)
            self.store.enqueue("t", "q", "k", {}, "i")
            deadline = asyncio.get_running_loop().time() + 2.0
            while not claimed and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.02)
            self.assertEqual(len(claimed), 1)
            worker.stop()
            await asyncio.wait_for(task, timeout=2.0)

        asyncio.run(scenario())

    def test_cancel_run_does_not_leak_tasks(self) -> None:
        async def scenario() -> None:
            self.store.enqueue("t", "q", "k", {}, "i1")
            block = asyncio.Event()

            async def handler(job: Job) -> None:
                await block.wait()

            worker = AsyncWorker(self.store, "worker-1", "q")
            task = asyncio.create_task(worker.run(handler, poll_interval=0.02))
            await asyncio.sleep(0.1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(worker._inflight, set())
            block.set()

        asyncio.run(scenario())

    def test_bounded_concurrency(self) -> None:
        async def scenario() -> None:
            for i in range(5):
                self.store.enqueue("t", "q", f"k{i}", {}, f"i{i}")
            active = 0
            max_active = 0

            async def handler(job: Job) -> None:
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                await asyncio.sleep(0.05)
                active -= 1

            worker = AsyncWorker(self.store, "worker-1", "q", concurrency=2)
            task = asyncio.create_task(worker.run(handler, poll_interval=0.01))
            await asyncio.sleep(0.4)
            worker.stop()
            await task
            self.assertLessEqual(max_active, 2)

        asyncio.run(scenario())

    def test_handler_exception_does_not_kill_worker(self) -> None:
        async def scenario() -> None:
            bad = self.store.enqueue("t", "q", "bad", {}, "i1")
            good = self.store.enqueue("t", "q", "good", {}, "i2")

            async def handler(job: Job) -> None:
                if job.id == bad.id:
                    raise ValueError("boom")

            worker = AsyncWorker(self.store, "worker-1", "q", concurrency=2, retry_base_delay=100)
            await worker.run_once(handler)
            self.assertEqual(self.store.get(bad.id).state, "pending")
            self.assertEqual(self.store.get(good.id).state, "succeeded")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
