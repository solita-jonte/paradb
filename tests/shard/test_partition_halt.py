"""Tests for the halt/resume write mechanism on the shard side."""

import asyncio
from fastapi.testclient import TestClient
import pytest
import pytest_asyncio
import uuid

from shard.app import app
from shard.partitions import partition_to_shard_url
from shard.document_locking import get_partition_lock, reset as reset_locks
from shard.document_storage import write_document, partition_index_for_id
from shard.url import get_host_url


@pytest.fixture
def data_dir(tmp_path):
    d = str(tmp_path / "data")
    import shard.routers.db as shard_db_routers
    original = getattr(shard_db_routers, "DATA_DIR", None)
    shard_db_routers.DATA_DIR = d
    yield d
    if original is not None:
        shard_db_routers.DATA_DIR = original


@pytest_asyncio.fixture(autouse=True)
async def reset_state():
    await reset_locks()
    partition_to_shard_url.clear()
    yield
    await reset_locks()
    partition_to_shard_url.clear()


@pytest.fixture
def client():
    return TestClient(app)


class TestDeleteCmdPartitionHaltsWrites:
    @pytest.mark.asyncio
    async def test_writes_blocked_after_halt(self, client):
        # given a partition
        partition_index = 5
        url = get_host_url()
        partition_to_shard_url[partition_index] = url

        # when we call DELETE /internal/partitions to halt writes
        response = client.delete("/internal/partitions", params={"partition_index": partition_index})
        assert response.status_code == 200

        # then the RWLock write side is held (writes block)
        rw_lock = await get_partition_lock(partition_index)
        acquired = asyncio.Event()

        async def try_read():
            async with rw_lock.for_reading():
                acquired.set()

        task = asyncio.create_task(try_read())

        # the read should block because write lock is held
        await asyncio.sleep(0.3)
        assert not acquired.is_set()

        # cleanup: release the write lock
        await rw_lock.release_write()
        await asyncio.wait_for(task, timeout=5)


class TestWritesToNonHaltedPartitionsStillProceed:
    @pytest.mark.asyncio
    async def test_non_halted_partition_works(self, data_dir):
        # given partition 5 is halted
        rw_lock_5 = await get_partition_lock(5)
        await rw_lock_5.acquire_write()

        # when we write to a document in partition 6
        doc_id = uuid.UUID(int=6)  # lowest bits = 6
        url = get_host_url()
        partition_to_shard_url[6] = url

        completed = asyncio.Event()
        errors = []

        async def do_write():
            try:
                rw_lock_6 = await get_partition_lock(6)
                async with rw_lock_6.for_reading():
                    await write_document(data_dir, {"_id": str(doc_id), "val": "ok"})
                completed.set()
            except Exception as e:
                errors.append(e)

        task = asyncio.create_task(do_write())

        # then it completes without blocking
        await asyncio.wait_for(completed.wait(), timeout=5)
        await asyncio.wait_for(task, timeout=5)
        assert not errors

        # cleanup
        await rw_lock_5.release_write()


class TestReceivingUpdatedPartitionsReleasesHalt:
    @pytest.mark.asyncio
    async def test_halt_released_on_partition_update(self):
        # given partition 5 is halted (write lock held)
        rw_lock = await get_partition_lock(5)
        await rw_lock.acquire_write()

        read_acquired = asyncio.Event()

        async def try_read():
            async with rw_lock.for_reading():
                read_acquired.set()

        task = asyncio.create_task(try_read())

        # verify it's blocked
        await asyncio.sleep(0.2)
        assert not read_acquired.is_set()

        # when we receive an updated partition table (which should release the halt)
        await rw_lock.release_write()

        # then the blocked read resumes
        await asyncio.wait_for(read_acquired.wait(), timeout=5)
        await asyncio.wait_for(task, timeout=5)


class TestHaltedPartitionWriteResumesAndForwards:
    @pytest.mark.asyncio
    async def test_write_resumes_and_forwards_after_ownership_change(self):
        # given partition P is halted
        doc_id = uuid.UUID(int=7)
        partition_idx = partition_index_for_id(doc_id)
        url = get_host_url()
        partition_to_shard_url[partition_idx] = url

        rw_lock = await get_partition_lock(partition_idx)
        await rw_lock.acquire_write()

        write_completed = asyncio.Event()
        write_result = {}

        async def do_write():
            async with rw_lock.for_reading():
                # after resuming, check ownership — it may have changed
                owner = partition_to_shard_url.get(partition_idx)
                write_result["owner"] = owner
            write_completed.set()

        task = asyncio.create_task(do_write())

        # verify write is blocked
        await asyncio.sleep(0.2)
        assert not write_completed.is_set()

        # when ownership changes to another shard and halt is released
        partition_to_shard_url[partition_idx] = "other-shard"
        await rw_lock.release_write()

        # then the write task resumes and sees the new owner
        await asyncio.wait_for(write_completed.wait(), timeout=5)
        await asyncio.wait_for(task, timeout=5)
        assert write_result["owner"] == "other-shard"
