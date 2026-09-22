import tempfile
import unittest

from durable_queue import AsyncWorker, Job, JobStore, Lease, QueueError


class CompatibilityTests(unittest.TestCase):
    def test_public_symbols_and_sqlite_constructor(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db") as handle:
            store = JobStore(handle.name)
            self.assertEqual(store.pending_count(), 0)
            store.close()
        self.assertTrue(Job)
        self.assertTrue(Lease)
        self.assertTrue(AsyncWorker)
        self.assertTrue(QueueError)


if __name__ == "__main__":
    unittest.main()