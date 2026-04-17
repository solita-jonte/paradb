"""Tests for the shared RWLock implementation."""

import asyncio

import pytest

from shared.rwlock import RWLock


class TestMultipleReadersConcurrent:
    @pytest.mark.asyncio
    async def test_readers_do_not_block_each_other(self):
        # given an RWLock
        lock = RWLock()
        acquired = []
        barrier = asyncio.Barrier(3)

        async def reader(idx):
            async with lock.for_reading():
                acquired.append(idx)
                await barrier.wait()

        # when three tasks acquire for reading simultaneously
        tasks = [asyncio.create_task(reader(i)) for i in range(3)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)

        # then all three succeed
        assert sorted(acquired) == [0, 1, 2]


class TestWriterBlocksNewReaders:
    @pytest.mark.asyncio
    async def test_reader_blocked_while_writer_holds_lock(self):
        # given an RWLock held by a writer
        lock = RWLock()
        await lock.acquire_write()

        reader_acquired = asyncio.Event()

        async def reader():
            async with lock.for_reading():
                reader_acquired.set()

        # when a reader tries to acquire
        task = asyncio.create_task(reader())

        # then the reader is blocked
        await asyncio.sleep(0.3)
        assert not reader_acquired.is_set()

        # when the writer releases
        await lock.release_write()

        # then the reader proceeds
        await asyncio.wait_for(reader_acquired.wait(), timeout=5)
        await asyncio.wait_for(task, timeout=5)


class TestWriterWaitsForReaders:
    @pytest.mark.asyncio
    async def test_writer_blocked_until_readers_finish(self):
        # given an RWLock held by a reader
        lock = RWLock()
        await lock.acquire_read()

        writer_acquired = asyncio.Event()

        async def writer():
            async with lock.for_writing():
                writer_acquired.set()

        # when a writer tries to acquire
        task = asyncio.create_task(writer())

        # then the writer is blocked
        await asyncio.sleep(0.3)
        assert not writer_acquired.is_set()

        # when the reader releases
        await lock.release_read()

        # then the writer proceeds
        await asyncio.wait_for(writer_acquired.wait(), timeout=5)
        await asyncio.wait_for(task, timeout=5)


class TestContextManagersReleaseOnException:
    @pytest.mark.asyncio
    async def test_for_reading_releases_on_exception(self):
        # given an RWLock
        lock = RWLock()

        # when an exception is raised inside for_reading
        try:
            async with lock.for_reading():
                raise ValueError("test error")
        except ValueError:
            pass

        # then the lock is released (a writer can acquire)
        acquired = asyncio.Event()

        async def writer():
            async with lock.for_writing():
                acquired.set()

        task = asyncio.create_task(writer())
        await asyncio.wait_for(acquired.wait(), timeout=2)
        await asyncio.wait_for(task, timeout=5)

    @pytest.mark.asyncio
    async def test_for_writing_releases_on_exception(self):
        # given an RWLock
        lock = RWLock()

        # when an exception is raised inside for_writing
        try:
            async with lock.for_writing():
                raise ValueError("test error")
        except ValueError:
            pass

        # then the lock is released (a reader can acquire)
        acquired = asyncio.Event()

        async def reader():
            async with lock.for_reading():
                acquired.set()

        task = asyncio.create_task(reader())
        await asyncio.wait_for(acquired.wait(), timeout=2)
        await asyncio.wait_for(task, timeout=5)
