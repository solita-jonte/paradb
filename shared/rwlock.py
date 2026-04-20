import asyncio
from contextlib import asynccontextmanager


class RWLock:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._read_ready = asyncio.Condition(self._lock)
        self._readers = 0
        self._writer = False
        self._writers_waiting = 0

    async def acquire_read(self):
        async with self._lock:
            while self._writer or self._writers_waiting > 0:
                await self._read_ready.wait()
            self._readers += 1

    async def release_read(self):
        async with self._lock:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    @asynccontextmanager
    async def for_reading(self):
        try:
            await self.acquire_read()
            yield
        finally:
            await self.release_read()

    async def acquire_write(self):
        async with self._lock:
            self._writers_waiting += 1
            while self._writer or self._readers > 0:
                await self._read_ready.wait()
            self._writers_waiting -= 1
            self._writer = True

    async def release_write(self):
        async with self._lock:
            self._writer = False
            self._read_ready.notify_all()

    async def try_release_write(self):
        if self._writer:
            await self.release_write()

    @asynccontextmanager
    async def for_writing(self):
        try:
            await self.acquire_write()
            yield
        finally:
            await self.release_write()
