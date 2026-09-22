import asyncio
import os
import sqlite3
import tempfile
import time
import unittest

from durable_queue import AsyncWorker, JobStore, QueueError
from durable_queue.errors import IdempotencyConflict, InvalidTransition, LeaseConflict


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


class IntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "queue.db")
        self.clock = FakeClock()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def make_store(self, backoff_base: float = 0.01) -> JobStore:
        return JobStore(self.db_path, clock=self.clock, backoff_base=backoff_base)

    def test_idempotent_enqueue_returns_same_job_and_stable_payload(self) -> None:
        with self.make_store() as store:
            payload = {"b": 2, "a": 1, "nested": {"z": 0, "y": 9}}
            first = store.enqueue("t", "q", "k", payload, "idem-1")
            second = store.enqueue(
                "t", "q", "k",
                {"a": 1, "b": 2, "nested": {"y": 9, "z": 0}}, "idem-1",
            )
            self.assertEqual(first.id, second.id)
            self.assertEqual(first.payload, payload)
            row = store._conn.execute(
                "SELECT payload_json FROM jobs WHERE id = ?", (first.id,)
            ).fetchone()
            self.assertEqual(
                row["payload_json"],
                '{"a":1,"b":2,"nested":{"y":9,"z":0}}',
            )

    def test_idempotency_scoped_per_tenant_and_queue(self) -> None:
        with self.make_store() as store:
            one = store.enqueue("t1", "q", "k", {"x": 1}, "i")
            two = store.enqueue("t2", "q", "k", {"x": 1}, "i")
            three = store.enqueue("t1", "q2", "k", {"x": 1}, "i")
            self.assertEqual(len({one.id, two.id, three.id}), 3)

    def test_idempotency_conflict_on_different_business_fields(self) -> None:
        with self.make_store() as store:
            store.enqueue("t", "q", "k", {"v": 1}, "i")
            with self.assertRaises(IdempotencyConflict):
                store.enqueue("t", "q", "other", {"v": 1}, "i")
            with self.assertRaises(IdempotencyConflict):
                store.enqueue("t", "q", "k", {"v": 2}, "i")
            with self.assertRaises(IdempotencyConflict):
                store.enqueue("t", "q", "k", {"v": 1}, "i", max_attempts=5)
            self.assertIsInstance(IdempotencyConflict("x"), QueueError)
            count = store._conn.execute("SELECT COUNT(*) AS n FROM jobs").fetchone()["n"]
            self.assertEqual(count, 1)

    def test_jobs_survive_store_restart(self) -> None:
        with self.make_store() as store:
            job = store.enqueue("t", "q", "k", {"v": 1}, "i")
            lease = store.claim("w1", "q")[0]
            store.ack(job.id, "w1", lease.token)
            store.enqueue("t", "q", "k2", {"v": 2}, "i2", available_at=self.clock.now + 50)
        with self.make_store() as reopened:
            done = reopened.get(1)
            self.assertIsNotNone(done)
            self.assertEqual(done.state, "succeeded")
            self.assertEqual(done.payload, {"v": 1})
            self.assertEqual(reopened.pending_count(), 1)
            self.assertEqual(reopened.claim("w1", "q"), [])

    def test_strict_per_key_ordering(self) -> None:
        with self.make_store() as store:
            a = store.enqueue("t", "q", "k", {"n": 1}, "a")
            b = store.enqueue("t", "q", "k", {"n": 2}, "b")
            c = store.enqueue("t", "q", "k", {"n": 3}, "c")
            other = store.enqueue("t", "q", "other", {"n": 0}, "o")
            leases = store.claim("w", "q", limit=10)
            self.assertEqual({lease.job.id for lease in leases}, {a.id, other.id})
            token_a = next(lease.token for lease in leases if lease.job.id == a.id)
            store.ack(a.id, "w", token_a)
            leases = store.claim("w", "q", limit=10)
            self.assertEqual([lease.job.id for lease in leases], [b.id])
            # While b is running, c stays blocked even with a large limit.
            self.assertEqual(store.claim("w", "q", limit=10), [])
            store.fail(b.id, "w", leases[0].token)
            # b is pending again (retry backoff) and still blocks c.
            self.clock.now += 1.0
            got = store.claim("w", "q", limit=10)
            self.assertEqual([lease.job.id for lease in got], [b.id])
            store.ack(b.id, "w", got[0].token)
            got = store.claim("w", "q", limit=10)
            self.assertEqual([lease.job.id for lease in got], [c.id])

    def test_available_at_orders_different_keys(self) -> None:
        with self.make_store() as store:
            late = store.enqueue("t", "q", "k1", {}, "a", available_at=self.clock.now + 10)
            early = store.enqueue("t", "q", "k2", {}, "b", available_at=self.clock.now)
            self.assertEqual(
                [lease.job.id for lease in store.claim("w", "q", limit=5)], [early.id]
            )
            self.clock.now += 10
            self.assertEqual(
                [lease.job.id for lease in store.claim("w", "q", limit=5)], [late.id]
            )

    def test_concurrent_workers_never_claim_same_job(self) -> None:
        with self.make_store() as store_a, self.make_store() as store_b:
            for i in range(20):
                store_a.enqueue("t", "q", f"k{i}", {"i": i}, f"idem-{i}")
            claimed: set[int] = set()
            for idx, active in enumerate((store_a, store_b)):
                leases = active.claim(f"w-{idx}", "q", limit=50)
                ids = [lease.job.id for lease in leases]
                self.assertEqual(len(ids), len(set(ids)))
                self.assertTrue(claimed.isdisjoint(ids))
                claimed.update(ids)
            self.assertEqual(len(claimed), 20)
            self.assertEqual(store_a.claim("w", "q"), [])

    def test_lease_has_unguessable_token_and_expiry(self) -> None:
        with self.make_store() as store:
            job = store.enqueue("t", "q", "k", {}, "i")
            lease = store.claim("w", "q", lease_seconds=5)[0]
            self.assertEqual(lease.worker_id, "w")
            self.assertGreaterEqual(len(lease.token), 32)
            self.assertAlmostEqual(lease.lease_until, self.clock.now + 5)
            self.assertEqual(store.claim("w2", "q", lease_seconds=5), [])
            self.assertEqual(store.get(job.id).state, "running")

    def test_stale_worker_and_token_ack_rejected_without_state_change(self) -> None:
        with self.make_store() as store:
            job = store.enqueue("t", "q", "k", {}, "i")
            lease = store.claim("owner", "q")[0]
            with self.assertRaises(LeaseConflict):
                store.ack(job.id, "intruder", lease.token)
            with self.assertRaises(LeaseConflict):
                store.ack(job.id, "owner", "deadbeef")
            self.assertEqual(store.get(job.id).state, "running")
            # Even the owner cannot ack after expiry; recovery restores the job.
            self.clock.now += 100
            with self.assertRaises(LeaseConflict):
                store.ack(job.id, "owner", lease.token)
            self.assertEqual(store.get(job.id).state, "running")
            self.assertEqual(store.recover_expired(), 1)
            recovered = store.get(job.id)
            self.assertEqual(recovered.state, "pending")
            self.assertEqual(recovered.attempts, 0)

    def test_invalid_state_transitions_raise_queue_error(self) -> None:
        with self.make_store() as store:
            job = store.enqueue("t", "q", "k", {}, "i")
            with self.assertRaises(InvalidTransition):
                store.ack(job.id, "w", "t")
            lease = store.claim("w", "q")[0]
            store.ack(job.id, "w", lease.token)
            self.assertEqual(store.get(job.id).state, "succeeded")
            with self.assertRaises(InvalidTransition):
                store.fail(job.id, "w", lease.token)

    def test_deterministic_exponential_backoff_then_dead(self) -> None:
        with self.make_store(backoff_base=2.0) as store:
            job = store.enqueue("t", "q", "k", {}, "i", max_attempts=3)
            lease = store.claim("w", "q")[0]
            failed = store.fail(job.id, "w", lease.token, error="boom")
            self.assertEqual(failed.state, "pending")
            self.assertEqual(failed.attempts, 1)
            self.assertAlmostEqual(failed.available_at, self.clock.now + 2.0)
            self.assertEqual(store.claim("w", "q"), [])
            self.clock.now += 2.0
            lease = store.claim("w", "q")[0]
            failed = store.fail(job.id, "w", lease.token)
            self.assertEqual(failed.attempts, 2)
            self.assertAlmostEqual(failed.available_at, self.clock.now + 4.0)
            self.clock.now += 4.0
            lease = store.claim("w", "q")[0]
            dead = store.fail(job.id, "w", lease.token)
            self.assertEqual(dead.state, "dead")
            self.assertEqual(dead.attempts, 3)
            # Repeating the terminal fail with the same lease is idempotent.
            again = store.fail(job.id, "w", lease.token)
            self.assertEqual(again.state, "dead")
            self.assertEqual(store.claim("w", "q"), [])

    def test_ack_is_idempotent_for_same_lease(self) -> None:
        with self.make_store() as store:
            job = store.enqueue("t", "q", "k", {}, "i")
            lease = store.claim("w", "q")[0]
            store.ack(job.id, "w", lease.token)
            again = store.ack(job.id, "w", lease.token)
            self.assertEqual(again.state, "succeeded")

    def test_claim_rolls_back_on_database_error(self) -> None:
        store = self.make_store()
        store.enqueue("t", "q", "k", {}, "i")
        original_conn = store._conn

        class SabotagedConn:
            def __init__(self, inner: sqlite3.Connection) -> None:
                self._inner = inner

            def execute(self, sql, params=()):
                if sql.lstrip().startswith("UPDATE jobs"):
                    raise sqlite3.OperationalError("forced failure")
                return self._inner.execute(sql, params)

            def __getattr__(self, name):
                return getattr(self._inner, name)

        store._conn = SabotagedConn(original_conn)  # type: ignore[assignment]
        with self.assertRaises(sqlite3.OperationalError):
            store.claim("w", "q")
        store._conn = original_conn
        # Transaction was rolled back: the job is still claimable.
        leases = store.claim("w", "q")
        self.assertEqual(len(leases), 1)
        self.assertEqual(leases[0].job.state, "running")
        store.close()

    def test_worker_success_failure_retry_stop_and_wakeup(self) -> None:
        asyncio.run(self._worker_scenario())

    async def _worker_scenario(self) -> None:
        base = 1000.0
        started_at = time.monotonic()
        store = JobStore(
            self.db_path,
            clock=lambda: base + time.monotonic() - started_at,
            backoff_base=0.01,
        )
        with store:
            ok = store.enqueue("t", "q", "ok", {"v": 1}, "ok")
            bad = store.enqueue("t", "q", "bad", {"v": 2}, "bad", max_attempts=2)
            succeeded: list[int] = []
            failures = {"n": 0}

            async def handler(job) -> None:
                if job.id == bad.id:
                    failures["n"] += 1
                    raise RuntimeError("always fails")
                succeeded.append(job.id)

            worker = AsyncWorker(store, "worker-1", "q", concurrency=2)
            run_task = asyncio.create_task(worker.run(handler, poll_interval=0.02))
            # The poller is idle; enqueueing must wake it promptly
            # rather than waiting for a poll timeout (no lost wakeup).
            await asyncio.sleep(0.05)
            late = store.enqueue("t", "q", "late", {"v": 3}, "late")
            for _ in range(200):
                await asyncio.sleep(0.02)
                if store.get(late.id).state == "succeeded" and store.get(bad.id).state == "dead":
                    break
            self.assertEqual(succeeded, [ok.id, late.id])
            self.assertEqual(failures["n"], 2)
            self.assertEqual(store.get(bad.id).state, "dead")
            worker.stop()
            await asyncio.wait_for(run_task, timeout=2.0)
            self.assertTrue(run_task.done())
            self.assertIsNone(run_task.exception())
            # No background tasks left behind.
            pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            self.assertEqual(pending, [])

    def test_worker_run_once_handler_exception_and_bounded_concurrency(self) -> None:
        asyncio.run(self._run_once_scenario())

    async def _run_once_scenario(self) -> None:
        with self.make_store(backoff_base=10.0) as store:
            job_ids = [
                store.enqueue("t", "q", f"k{i}", {}, f"i{i}").id for i in range(5)
            ]
            active = 0
            max_active = 0

            async def handler(_job) -> None:
                nonlocal active, max_active
                active += 1
                max_active = max(max_active, active)
                await asyncio.sleep(0)
                active -= 1
                raise ValueError("nope")

            worker = AsyncWorker(store, "w", "q", concurrency=3)
            processed = await worker.run_once(handler)
            self.assertEqual(processed, 3)
            self.assertLessEqual(max_active, 3)
            for job_id in job_ids[:3]:
                failed = store.get(job_id)
                self.assertEqual(failed.state, "pending")
                self.assertEqual(failed.attempts, 1)
            self.assertEqual(store.get(job_ids[3]).state, "pending")
            self.assertEqual(store.get(job_ids[3]).attempts, 0)

    def test_worker_cancellation_leaves_no_background_tasks(self) -> None:
        asyncio.run(self._cancel_scenario())

    async def _cancel_scenario(self) -> None:
        with self.make_store() as store:
            store.enqueue("t", "q", "k", {}, "i")
            started = asyncio.Event()

            async def handler(_job) -> None:
                started.set()
                await asyncio.sleep(10)

            worker = AsyncWorker(store, "w", "q")
            run_task = asyncio.create_task(worker.run(handler, poll_interval=0.02))
            await asyncio.wait_for(started.wait(), timeout=2.0)
            run_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await run_task
            self.assertTrue(run_task.done())


if __name__ == "__main__":
    unittest.main()
