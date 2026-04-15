"""Tests for document locking — per-document write lock and partition halt lock."""

import threading
import time
import uuid
import os

import pytest

from shard.document_storage import write_document, partition_index_for_id
from shard.document_locking import get_partition_lock, get_document_lock, reset as reset_locks


@pytest.fixture
def data_dir(tmp_path):
    return str(tmp_path / "data")


@pytest.fixture(autouse=True)
def clean_locks():
    reset_locks()
    yield
    reset_locks()


class TestConcurrentWritesSameDocumentSerialized:
    def test_both_writes_complete_without_corruption(self, data_dir):
        # given a known document ID
        doc_id = str(uuid.uuid4())
        results = []
        errors = []

        def write_payload(payload):
            try:
                lock = get_document_lock(doc_id)
                with lock:
                    result = write_document(data_dir, {"_id": doc_id, **payload})
                    results.append(result)
            except Exception as e:
                errors.append(e)

        # when two threads write concurrently to the same document ID
        t1 = threading.Thread(target=write_payload, args=({"v": 1},))
        t2 = threading.Thread(target=write_payload, args=({"v": 2},))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

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
    def test_both_files_exist(self, data_dir):
        # given two different document IDs
        id1 = str(uuid.uuid4())
        id2 = str(uuid.uuid4())
        errors = []

        def write_doc(doc_id, value):
            try:
                lock = get_document_lock(doc_id)
                with lock:
                    write_document(data_dir, {"_id": doc_id, "v": value})
            except Exception as e:
                errors.append(e)

        # when two threads write to different documents
        t1 = threading.Thread(target=write_doc, args=(id1, 1))
        t2 = threading.Thread(target=write_doc, args=(id2, 2))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        # then both complete and both files exist
        assert not errors
        from shard.document_storage import read_document
        assert read_document(data_dir, id1) is not None
        assert read_document(data_dir, id2) is not None


class TestPartitionHaltBlocksWrites:
    def test_write_blocked_until_halt_released(self, data_dir):
        # given a partition with a halt (write lock) active
        doc_id = uuid.uuid4()
        partition_idx = partition_index_for_id(doc_id)
        rw_lock = get_partition_lock(partition_idx)

        # acquire the write lock to simulate halt
        rw_lock.acquire_write()

        write_completed = threading.Event()
        errors = []

        def do_write():
            try:
                # this should block on for_reading until halt is released
                with rw_lock.for_reading():
                    write_document(data_dir, {"_id": str(doc_id), "v": 1})
                write_completed.set()
            except Exception as e:
                errors.append(e)
                write_completed.set()

        # when a write is attempted in a background thread
        t = threading.Thread(target=do_write)
        t.start()

        # then the write does not complete while halt is active
        assert not write_completed.wait(timeout=0.3)

        # when we release the halt
        rw_lock.release_write()

        # then the write completes
        assert write_completed.wait(timeout=5)
        t.join(timeout=5)
        assert not errors


class TestMultipleReadsDuringHaltConcurrent:
    def test_readers_do_not_block_each_other(self):
        # given a partition lock
        rw_lock = get_partition_lock(0)

        acquired = []
        barrier = threading.Barrier(3, timeout=5)

        def reader(idx):
            with rw_lock.for_reading():
                acquired.append(idx)
                barrier.wait()  # all readers must reach this concurrently

        # when three readers acquire the lock simultaneously
        threads = [threading.Thread(target=reader, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        # then all three acquired the lock
        assert sorted(acquired) == [0, 1, 2]
