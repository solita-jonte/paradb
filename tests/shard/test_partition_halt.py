"""Tests for the halt/resume write mechanism on the shard side."""

import threading
import uuid

import pytest
from fastapi.testclient import TestClient

from shard.app import app, partition_to_shard_host
from shard.document_locking import get_partition_lock, reset as reset_locks
from shard.document_storage import write_document, partition_index_for_id


@pytest.fixture
def data_dir(tmp_path):
    d = str(tmp_path / "data")
    import shard.app as shard_app
    original = getattr(shard_app, "DATA_DIR", None)
    shard_app.DATA_DIR = d
    yield d
    if original is not None:
        shard_app.DATA_DIR = original


@pytest.fixture(autouse=True)
def reset_state():
    reset_locks()
    partition_to_shard_host.clear()
    yield
    reset_locks()
    partition_to_shard_host.clear()


@pytest.fixture
def client(data_dir):
    return TestClient(app)


class TestDeleteCmdPartitionHaltsWrites:
    def test_writes_blocked_after_halt(self, client, data_dir):
        # given a partition
        partition_index = 5
        import socket
        hostname = socket.gethostname()
        partition_to_shard_host[partition_index] = hostname

        # when we call DELETE /cmd/partition to halt writes
        response = client.delete("/cmd/partition", params={"partition_index": partition_index})
        assert response.status_code == 200

        # then the RWLock write side is held (writes block)
        rw_lock = get_partition_lock(partition_index)
        acquired = threading.Event()

        def try_read():
            with rw_lock.for_reading():
                acquired.set()

        t = threading.Thread(target=try_read)
        t.start()

        # the read should block because write lock is held
        assert not acquired.wait(timeout=0.3)

        # cleanup: release the write lock
        rw_lock.release_write()
        t.join(timeout=5)


class TestWritesToNonHaltedPartitionsStillProceed:
    def test_non_halted_partition_works(self, data_dir):
        # given partition 5 is halted
        rw_lock_5 = get_partition_lock(5)
        rw_lock_5.acquire_write()

        # when we write to a document in partition 6
        doc_id = uuid.UUID(int=6)  # lowest bits = 6
        import socket
        hostname = socket.gethostname()
        partition_to_shard_host[6] = hostname

        completed = threading.Event()
        errors = []

        def do_write():
            try:
                rw_lock_6 = get_partition_lock(6)
                with rw_lock_6.for_reading():
                    write_document(data_dir, {"_id": str(doc_id), "val": "ok"})
                completed.set()
            except Exception as e:
                errors.append(e)

        t = threading.Thread(target=do_write)
        t.start()

        # then it completes without blocking
        assert completed.wait(timeout=5)
        t.join(timeout=5)
        assert not errors

        # cleanup
        rw_lock_5.release_write()


class TestReceivingUpdatedPartitionsReleasesHalt:
    def test_halt_released_on_partition_update(self, client, data_dir):
        # given partition 5 is halted (write lock held)
        rw_lock = get_partition_lock(5)
        rw_lock.acquire_write()

        read_acquired = threading.Event()

        def try_read():
            with rw_lock.for_reading():
                read_acquired.set()

        t = threading.Thread(target=try_read)
        t.start()

        # verify it's blocked
        assert not read_acquired.wait(timeout=0.2)

        # when we receive an updated partition table (which should release the halt)
        rw_lock.release_write()

        # then the blocked read resumes
        assert read_acquired.wait(timeout=5)
        t.join(timeout=5)


class TestHaltedPartitionWriteResumesAndForwards:
    def test_write_resumes_and_forwards_after_ownership_change(self, client, data_dir):
        # given partition P is halted
        doc_id = uuid.UUID(int=7)
        partition_idx = partition_index_for_id(doc_id)
        import socket
        hostname = socket.gethostname()
        partition_to_shard_host[partition_idx] = hostname

        rw_lock = get_partition_lock(partition_idx)
        rw_lock.acquire_write()

        write_completed = threading.Event()
        write_result = {}

        def do_write():
            with rw_lock.for_reading():
                # after resuming, check ownership — it may have changed
                owner = partition_to_shard_host.get(partition_idx)
                write_result["owner"] = owner
            write_completed.set()

        t = threading.Thread(target=do_write)
        t.start()

        # verify write is blocked
        assert not write_completed.wait(timeout=0.2)

        # when ownership changes to another shard and halt is released
        partition_to_shard_host[partition_idx] = "other-shard"
        rw_lock.release_write()

        # then the write thread resumes and sees the new owner
        assert write_completed.wait(timeout=5)
        t.join(timeout=5)
        assert write_result["owner"] == "other-shard"
