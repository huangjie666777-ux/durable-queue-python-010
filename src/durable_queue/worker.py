from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from .store import JobStore


Handler = Callable[[Any], Awaitable[None]]


class AsyncWorker:
    """Bounded asyncio worker. The model must implement polling and shutdown."""

    def __init__(self, store: JobStore, worker_id: str, queue: str, concurrency: int = 1) -> None:
        self.store = store
        self.worker_id = worker_id
        self.queue = queue
        self.concurrency = concurrency
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run_once(self, handler: Handler) -> int:
        raise NotImplementedError

    async def run(self, handler: Handler, poll_interval: float = 0.05) -> None:
        raise NotImplementedError