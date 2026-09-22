from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from .store import JobStore


Handler = Callable[[Any], Awaitable[None]]


class AsyncWorker:
    """Bounded asyncio worker. The model must implement polling and shutdown."""

    def __init__(
        self,
        store: JobStore,
        worker_id: str,
        queue: str,
        concurrency: int = 1,
        *,
        tenant: str | None = None,
        lease_seconds: float = 30.0,
    ) -> None:
        self.store = store
        self.worker_id = worker_id
        self.queue = queue
        self.concurrency = concurrency
        self.tenant = tenant
        self.lease_seconds = lease_seconds
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run_once(self, handler: Handler) -> int:
        self.store.recover_expired()
        leases = self.store.claim(
            self.worker_id,
            self.queue,
            limit=self.concurrency,
            lease_seconds=self.lease_seconds,
            tenant=self.tenant,
        )
        if not leases:
            return 0

        async def handle(lease: Any) -> None:
            try:
                await handler(lease.job)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.store.fail(
                    lease.job.id,
                    self.worker_id,
                    lease.token,
                    error=f"{type(exc).__name__}: {exc}",
                )
            else:
                self.store.ack(lease.job.id, self.worker_id, lease.token)

        tasks = [asyncio.create_task(handle(lease)) for lease in leases]
        if tasks:
            await asyncio.gather(*tasks)
        return len(tasks)

    async def run(self, handler: Handler, poll_interval: float = 0.05) -> None:
        wake = asyncio.Event()
        unsubscribe = self.store.subscribe(wake.set)
        try:
            while not self._stop.is_set():
                processed = await self.run_once(handler)
                if processed == 0 and not self._stop.is_set():
                    wake.clear()
                    # Re-poll once after clearing so a commit racing with the
                    # previous empty poll cannot be missed (no lost wakeup).
                    processed = await self.run_once(handler)
                if processed == 0 and not self._stop.is_set():
                    try:
                        await asyncio.wait_for(wake.wait(), timeout=poll_interval)
                    except asyncio.TimeoutError:
                        pass
        finally:
            unsubscribe()
