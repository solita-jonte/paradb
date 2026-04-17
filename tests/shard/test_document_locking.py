"""Tests for document locking — per-document write lock and partition halt lock."""

import asyncio
import os
import pytest
import pytest_asyncio
import uuid

from shard.document_storage import write_document, partition_index_for_id
from shard.document_locking import get_partition_lock, get_document_lock, reset as reset_locks


@pytest.fixture
def data_dir(tmp_path):
    return str(tmp_path / "data")


@pytest_asyncio.fixture(autouse=True)
async def clean_locks():
    await reset_locks()
    yield
    await reset_locks()


class TestConcurrentWritesSameDocumentSerialized:
    @pytest.mark.asyncio
    async def test_both_writes_complete_without_corruption(self, data_dir):
        # given a known document ID
        doc_id = str(uuid.uuid4())
        results = []
        errors = []

        async def write_payload(payload):
            try:
                lock = get_document_lock(doc_id)
                async with lock:
                    result = await write_document(data_dir, {"_id": doc_id, **payload})
                    results.append(result)
            except Exception as e:
                errors.append(e)

        # when two tasks write concurrently to the same document ID
        t1 = asyncio.create_task(write_payload({"v": 1}))
        t2 = asyncio.create_task(write_payload({"v": 2}))
        await asyncio.wait_for(asyncio.gather(t1, t2), timeout=5)

        # then both complete without errors
        assert not errors
        assert len(results) == 2

        # and the file on disk is valid JSON with one of the two payloads
        import json
        uid = uuid.UUID(doc_id)
        from shard.document_storage import partition_dir_name
        p_idx = partition_index_for_id(uid)
        d_name = partition_dir_name(p_idx)
        file_path = os.path.join(data_dir, d_name, f"{uid}.json")
        with open(file_path) as f:
            on_disk = json.load(f)
        assert on_disk["v"] in (1, 2)


class TestConcurrentWritesDifferentDocumentsParallel:
    @pytest.mark.asyncio
    async def test_both_files_exist(self, data_dir):
        # given two different document IDs
        id1 = str(uuid.uuid4())
        id2 = str(uuid.uuid4())
        errors = []

        async def write_doc(doc_id, value):
            try:
                lock = get_document_lock(doc_id)
                async with lock:
                    await write_document(data_dir, {"_id": doc_id, "v": value})
            except Exception as e:
                errors.append(e)

        # when two tasks write to different documents
        t1 = asyncio.create_task(write_doc(id1, 1))
        t2 = asyncio.create_task(write_doc(id2, 2))
        await asyncio.wait_for(asyncio.gather(t1, t2), timeout=5)

        # then both complete and both files exist
        assert not errors
        from shard.document_storage import read_document
        assert read_document(data_dir, id1) is not None
        assert read_document(data_dir, id2) is not None


class TestPartitionHaltBlocksWrites:
    @pytest.mark.asyncio
    async def test_write_blocked_until_halt_released(self, data_dir):
        # given a partition with a halt (write lock) active
        doc_id = uuid.uuid4()
        partition_idx = partition_index_for_id(doc_id)
        rw_lock = await get_partition_lock(partition_idx)

        # acquire the write lock to simulate halt
        await rw_lock.acquire_write()

        write_completed = asyncio.Event()
        errors = []

        async def do_write():
            try:
                # this should block on for_reading until halt is released
                async with rw_lock.for_reading():
                    await write_document(data_dir, {"_id": str(doc_id), "v": 1})
                write_completed.set()
            except Exception as e:
                errors.append(e)
                write_completed.set()

        # when a write is attempted in a background task
        task = asyncio.create_task(do_write())

        # then the write does not complete while halt is active
        await asyncio.sleep(0.3)
        assert not write_completed.is_set()

        # when we release the halt
        await rw_lock.release_write()

        # then the write completes
        await asyncio.wait_for(write_completed.wait(), timeout=5)
        await asyncio.wait_for(task, timeout=5)
        assert not errors


class TestMultipleReadsDuringHaltConcurrent:
    @pytest.mark.asyncio
    async def test_readers_do_not_block_each_other(self):
        # given a partition lock
        rw_lock = await get_partition_lock(0)

        acquired = []
        barrier = asyncio.Barrier(3)

        async def reader(idx):
            async with rw_lock.for_reading():
                acquired.append(idx)
                await barrier.wait()  # all readers must reach this concurrently

        # when three readers acquire the lock simultaneously
        tasks = [asyncio.create_task(reader(i)) for i in range(3)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)

        # then all three acquired the lock
        assert sorted(acquired) == [0, 1, 2]
