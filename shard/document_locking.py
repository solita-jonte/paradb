"""Document write locking — per-partition RWLock and per-document-ID lock."""

import threading
from shared.rwlock import RWLock


# Per-partition halt locks (partition_index -> RWLock).
# Normal writes acquire for_reading; orchestrator halt acquires for_writing.
_partition_locks: dict[int, RWLock] = {}
_partition_locks_mutex = threading.Lock()

# Per-document-ID write locks to serialize concurrent writes to the same document.
_document_locks: dict[str, threading.Lock] = {}
_document_locks_mutex = threading.Lock()


def get_partition_lock(partition_index: int) -> RWLock:
    """Get or create the RWLock for a partition."""
    with _partition_locks_mutex:
        if partition_index not in _partition_locks:
            _partition_locks[partition_index] = RWLock()
        return _partition_locks[partition_index]


def get_document_lock(doc_id: str) -> threading.Lock:
    """Get or create a lock for a specific document ID."""
    with _document_locks_mutex:
        if doc_id not in _document_locks:
            _document_locks[doc_id] = threading.Lock()
        return _document_locks[doc_id]


def reset():
    """Reset all locks (for testing)."""
    with _partition_locks_mutex:
        _partition_locks.clear()
    with _document_locks_mutex:
        _document_locks.clear()
