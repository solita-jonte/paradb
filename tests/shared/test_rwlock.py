"""Tests for the shared RWLock implementation."""

import threading
import time

from shared.rwlock import RWLock


class TestMultipleReadersConcurrent:
    def test_readers_do_not_block_each_other(self):
        # given an RWLock
        lock = RWLock()
        acquired = []
        barrier = threading.Barrier(3, timeout=5)

        def reader(idx):
            with lock.for_reading():
                acquired.append(idx)
                barrier.wait()

        # when three threads acquire for reading simultaneously
        threads = [threading.Thread(target=reader, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        # then all three succeed
        assert sorted(acquired) == [0, 1, 2]


class TestWriterBlocksNewReaders:
    def test_reader_blocked_while_writer_holds_lock(self):
        # given an RWLock held by a writer
        lock = RWLock()
        lock.acquire_write()

        reader_acquired = threading.Event()

        def reader():
            with lock.for_reading():
                reader_acquired.set()

        # when a reader tries to acquire
        t = threading.Thread(target=reader)
        t.start()

        # then the reader is blocked
        assert not reader_acquired.wait(timeout=0.3)

        # when the writer releases
        lock.release_write()

        # then the reader proceeds
        assert reader_acquired.wait(timeout=5)
        t.join(timeout=5)


class TestWriterWaitsForReaders:
    def test_writer_blocked_until_readers_finish(self):
        # given an RWLock held by a reader
        lock = RWLock()
        lock.acquire_read()

        writer_acquired = threading.Event()

        def writer():
            with lock.for_writing():
                writer_acquired.set()

        # when a writer tries to acquire
        t = threading.Thread(target=writer)
        t.start()

        # then the writer is blocked
        assert not writer_acquired.wait(timeout=0.3)

        # when the reader releases
        lock.release_read()

        # then the writer proceeds
        assert writer_acquired.wait(timeout=5)
        t.join(timeout=5)


class TestContextManagersReleaseOnException:
    def test_for_reading_releases_on_exception(self):
        # given an RWLock
        lock = RWLock()

        # when an exception is raised inside for_reading
        try:
            with lock.for_reading():
                raise ValueError("test error")
        except ValueError:
            pass

        # then the lock is released (a writer can acquire)
        acquired = threading.Event()

        def writer():
            with lock.for_writing():
                acquired.set()

        t = threading.Thread(target=writer)
        t.start()
        assert acquired.wait(timeout=2)
        t.join(timeout=5)

    def test_for_writing_releases_on_exception(self):
        # given an RWLock
        lock = RWLock()

        # when an exception is raised inside for_writing
        try:
            with lock.for_writing():
                raise ValueError("test error")
        except ValueError:
            pass

        # then the lock is released (a reader can acquire)
        acquired = threading.Event()

        def reader():
            with lock.for_reading():
                acquired.set()

        t = threading.Thread(target=reader)
        t.start()
        assert acquired.wait(timeout=2)
        t.join(timeout=5)
