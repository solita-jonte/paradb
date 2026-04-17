"""Document storage layer — handles reading, writing, and deleting JSON documents on disk."""

import glob
import json
import os
import uuid
from typing import Any

from shared.file_utils import file_replace_retry
from shared.types.partition_bits import PARTITION_MASK


def partition_index_for_id(doc_id: uuid.UUID) -> int:
    """Return the partition index (0–1023) for a given document UUID."""
    return doc_id.int & PARTITION_MASK


def partition_dir_name(partition_index: int) -> str:
    """Return the hex directory name for a partition index."""
    return format(partition_index, "x")


def _doc_path(data_dir: str, doc_id: uuid.UUID) -> str:
    """Return the full file path for a document."""
    partition_idx = partition_index_for_id(doc_id)
    dir_name = partition_dir_name(partition_idx)
    return os.path.join(data_dir, dir_name, f"{doc_id}.json")


async def write_document(data_dir: str, document: dict[str, Any]) -> dict[str, Any]:
    """Write (upsert) a document to disk. Generates _id if missing, for unit testing. Returns the full document."""
    doc = dict(document)
    if "_id" not in doc:  # for testing only
        doc["_id"] = str(uuid.uuid4())
    doc_id = uuid.UUID(doc["_id"])

    try:
        file_path = _doc_path(data_dir, doc_id)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        tmp_path = os.path.join(data_dir, f".tmp_{doc_id}.tmp-json")
        print("writing tmp:", tmp_path)
        with open(tmp_path, "w") as f:
            json.dump(doc, f)
        await file_replace_retry(tmp_path, file_path, 5)
    except OSError as ex:
        print("replace failed:", ex)
        print("tmp:", tmp_path, "exists:", os.path.exists(tmp_path))
        print("dst:", file_path, "exists:", os.path.exists(file_path))
        if os.path.exists(tmp_path) and os.path.exists(os.path.dirname(file_path)):
            print("same device:", os.stat(tmp_path).st_dev == os.stat(os.path.dirname(file_path)).st_dev)
        raise ex

    return doc


def read_document(data_dir: str, doc_id: str) -> dict[str, Any] | None:
    """Read a document by its _id string. Returns None if not found."""
    uid = uuid.UUID(doc_id)
    file_path = _doc_path(data_dir, uid)
    if not os.path.isfile(file_path):
        return None
    with open(file_path) as f:
        return json.load(f)


def delete_document(data_dir: str, doc_id: str) -> bool:
    """Delete a document by its _id string. Returns True if deleted, False if not found."""
    uid = uuid.UUID(doc_id)
    file_path = _doc_path(data_dir, uid)
    if not os.path.isfile(file_path):
        return False
    os.remove(file_path)
    return True


def scan_all_documents(data_dir: str) -> list[dict[str, Any]]:
    """Read and return every document on disk."""
    results = []
    for entry in glob.glob(os.path.join(data_dir, "*", "*.json")):
        try:
            with open(entry) as f:
                results.append(json.load(f))
        except (json.JSONDecodeError, OSError):
            pass
    return results
