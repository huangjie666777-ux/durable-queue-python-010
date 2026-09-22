from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from .errors import LeaseConflict
from .store import JobStore
from .models import Lease


Handler = Callable[[Any], Awaitable[None]]


class AsyncWorker:
    """Bounded asyncio worker with polling, retries and graceful shutdown."""

    def __init__(
        self,
        store: JobStore,
        worker_id: str,
        queue: str,
        concurrency: int = 1,
        lease_seconds: float = 30.0,
        retry_base_delay: float = 1.0,
    ) -> None:
        self.store = store
        self.worker_id = worker_id
        self.queue = queue
        self.concurrency = max(1, concurrency)
        self.lease_seconds = lease_seconds
        self.retry_base_delay = retry_base_delay
        self._stop = asyncio.Event()
        self._wake = asyncio.Event()
        self._inflight: set[asyncio.Task[None]] = set()
        self._loop: asyncio.AbstractEventLoop | None = None

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def _on_store_wake(self) -> None:
        loop = self._loop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(self._wake.set)

    def _free_slots(self) -> int:
        return self.concurrency - len(self._inflight)

    async def _process(self, lease: Lease, handler: Handler) -> None:
        try:
            try:
                await handler(lease.job)
            except asyncio.CancelledError:
                raise
            except Exception:
                try:
                    self.store.fail(
                        lease.job.id,
                        self.worker_id,
                        lease.token,
                        retry_base_delay=self.retry_base_delay,
                    )
                except (LeaseConflict, asyncio.CancelledError):
                    pass
                except Exception:
                    pass
            else:
                try:
                    self.store.ack(lease.job.id, self.worker_id, lease.token)
                except LeaseConflict:
                    pass
        except asyncio.CancelledError:
            raise

    def _spawn(self, lease: Lease, handler: Handler) -> None:
        task = asyncio.create_task(self._process(lease, handler))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    async def run_once(self, handler: Handler) -> int:
        self.store.recover_expired()
        leases = self.store.claim(
            self.worker_id,
            self.queue,
            limit=self.concurrency,
            lease_seconds=self.lease_seconds,
        )
        for lease in leases:
            self._spawn(lease, handler)
        if self._inflight:
            await asyncio.gather(*list(self._inflight))
        return len(leases)

    async def run(self, handler: Handler, poll_interval: float = 0.05) -> None:
        self._loop = asyncio.get_running_loop()
        self.store.add_wake_callback(self._on_store_wake)
        try:
            while not self._stop.is_set():
                self._wake.clear()
                if not self._inflight:
                    self.store.recover_expired()
                free = self._free_slots()
                leases: list[Lease] = []
                if free > 0:
                    leases = self.store.claim(
                        self.worker_id,
                        self.queue,
                        limit=free,
                        lease_seconds=self.lease_seconds,
                    )
                for lease in leases:
                    self._spawn(lease, handler)
                if self._stop.is_set():
                    break
                if not leases:
                    try:
                        await asyncio.wait_for(self._wake.wait(), timeout=poll_interval)
                    except asyncio.TimeoutError:
                        pass
        finally:
            self.store.remove_wake_callback(self._on_store_wake)
            inflight = list(self._inflight)
            if inflight:
                cancelling = self._stop.is_set() is False
                if cancelling:
                    for task in inflight:
                        task.cancel()
                await asyncio.gather(*inflight, return_exceptions=True)
