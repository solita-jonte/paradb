"""Tests for document storage — writing, reading, and deleting JSON documents on disk."""

import glob
import json
import os
import pytest
import uuid

from shard.document_storage import (
    write_document,
    read_document,
    delete_document,
    partition_index_for_id,
    partition_dir_name,
)


@pytest.fixture
def data_dir(tmp_path):
    """Provide a fresh temporary DATA_DIR for each test."""
    return str(tmp_path / "data")


class TestWriteNewDocumentWithGeneratedId:
    @pytest.mark.asyncio
    async def test_returns_document_with_uuid_id(self, data_dir):
        # given a document body with no _id
        body = {"name": "Alice"}

        # when we write it
        result = await write_document(data_dir, body)

        # then the result contains a valid UUID _id
        assert "_id" in result
        uuid.UUID(result["_id"])  # raises if not valid

    @pytest.mark.asyncio
    async def test_file_exists_at_correct_path(self, data_dir):
        # given a document body with no _id
        body = {"name": "Alice"}

        # when we write it
        result = await write_document(data_dir, body)

        # then a JSON file exists at the correct partition path
        doc_id = uuid.UUID(result["_id"])
        partition_idx = partition_index_for_id(doc_id)
        dir_name = partition_dir_name(partition_idx)
        expected_path = os.path.join(data_dir, dir_name, f"{doc_id}.json")
        assert os.path.isfile(expected_path)

    @pytest.mark.asyncio
    async def test_file_content_matches_returned_document(self, data_dir):
        # given a document body with no _id
        body = {"name": "Alice"}

        # when we write it
        result = await write_document(data_dir, body)

        # then the file content matches
        doc_id = uuid.UUID(result["_id"])
        partition_idx = partition_index_for_id(doc_id)
        dir_name = partition_dir_name(partition_idx)
        file_path = os.path.join(data_dir, dir_name, f"{doc_id}.json")
        with open(file_path) as f:
            on_disk = json.load(f)
        assert on_disk == result


class TestWriteNewDocumentWithSupplierId:
    @pytest.mark.asyncio
    async def test_file_in_correct_partition_dir(self, data_dir):
        # given a document with a known UUID
        doc_id = uuid.UUID("eeb2f00c-d6e6-464d-b692-09e5ca759be9")
        body = {"_id": str(doc_id), "name": "test"}

        # when we write it
        await write_document(data_dir, body)

        # then the file is in the correct partition directory
        partition_idx = partition_index_for_id(doc_id)
        dir_name = partition_dir_name(partition_idx)
        expected_path = os.path.join(data_dir, dir_name, f"{doc_id}.json")
        assert os.path.isfile(expected_path)

    @pytest.mark.asyncio
    async def test_on_disk_contains_supplied_id(self, data_dir):
        # given a document with a known UUID
        doc_id = uuid.UUID("eeb2f00c-d6e6-464d-b692-09e5ca759be9")
        body = {"_id": str(doc_id), "name": "test"}

        # when we write it
        await write_document(data_dir, body)

        # then on-disk JSON contains the supplied _id
        partition_idx = partition_index_for_id(doc_id)
        dir_name = partition_dir_name(partition_idx)
        file_path = os.path.join(data_dir, dir_name, f"{doc_id}.json")
        with open(file_path) as f:
            on_disk = json.load(f)
        assert on_disk["_id"] == str(doc_id)


class TestUpsertOverwritesDocument:
    @pytest.mark.asyncio
    async def test_full_replacement(self, data_dir):
        # given an existing document
        doc_id = str(uuid.uuid4())
        await write_document(data_dir, {"_id": doc_id, "a": 1})

        # when we upsert with a different body
        result = await write_document(data_dir, {"_id": doc_id, "b": 2})

        # then the result has the new field and not the old
        assert result["b"] == 2
        assert "a" not in result
        assert result["_id"] == doc_id


class TestAtomicWriteViaTemporaryFile:
    @pytest.mark.asyncio
    async def test_no_temp_files_remain(self, data_dir):
        # given a fresh data dir
        # when we write a document
        await write_document(data_dir, {"name": "test"})

        # then no .tmp_ files remain
        tmp_files = glob.glob(os.path.join(data_dir, "**", ".tmp_*.tmp-json"), recursive=True)
        assert tmp_files == []

    @pytest.mark.asyncio
    async def test_target_file_is_valid_json(self, data_dir):
        # given we write a document
        result = await write_document(data_dir, {"name": "test"})

        # when we read the file back
        doc_id = uuid.UUID(result["_id"])
        partition_idx = partition_index_for_id(doc_id)
        dir_name = partition_dir_name(partition_idx)
        file_path = os.path.join(data_dir, dir_name, f"{doc_id}.json")

        # then it's valid JSON
        with open(file_path) as f:
            data = json.load(f)
        assert isinstance(data, dict)


class TestPartitionDirectoryCreatedOnFirstWrite:
    @pytest.mark.asyncio
    async def test_directory_auto_created(self, data_dir):
        # given a fresh DATA_DIR with no subdirectories
        assert not os.path.exists(data_dir)

        # when we write a document
        result = await write_document(data_dir, {"name": "first"})

        # then the partition directory was created
        doc_id = uuid.UUID(result["_id"])
        partition_idx = partition_index_for_id(doc_id)
        dir_name = partition_dir_name(partition_idx)
        assert os.path.isdir(os.path.join(data_dir, dir_name))


class TestReadDocumentById:
    @pytest.mark.asyncio
    async def test_read_returns_written_document(self, data_dir):
        # given a written document
        original = await write_document(data_dir, {"name": "Alice", "age": 30})

        # when we read it by _id
        result = read_document(data_dir, original["_id"])

        # then it matches the original
        assert result == original


class TestReadNonExistentDocument:
    def test_returns_none(self, data_dir):
        # given a random UUID that was never written
        os.makedirs(data_dir, exist_ok=True)
        random_id = str(uuid.uuid4())

        # when we read it
        result = read_document(data_dir, random_id)

        # then the result is None
        assert result is None


class TestDeleteDocument:
    @pytest.mark.asyncio
    async def test_file_removed_from_disk(self, data_dir):
        # given a written document
        original = await write_document(data_dir, {"name": "delete-me"})
        doc_id = original["_id"]

        # when we delete it
        delete_document(data_dir, doc_id)

        # then the file no longer exists
        uid = uuid.UUID(doc_id)
        partition_idx = partition_index_for_id(uid)
        dir_name = partition_dir_name(partition_idx)
        file_path = os.path.join(data_dir, dir_name, f"{uid}.json")
        assert not os.path.exists(file_path)

    @pytest.mark.asyncio
    async def test_read_after_delete_returns_none(self, data_dir):
        # given a written and then deleted document
        original = await write_document(data_dir, {"name": "delete-me"})
        doc_id = original["_id"]
        delete_document(data_dir, doc_id)

        # when we read it
        result = read_document(data_dir, doc_id)

        # then it returns None
        assert result is None


class TestDeleteNonExistentDocument:
    def test_no_error(self, data_dir):
        # given a random UUID that was never written
        os.makedirs(data_dir, exist_ok=True)
        random_id = str(uuid.uuid4())

        # when we delete it — then no exception is raised
        result = delete_document(data_dir, random_id)
        assert result is False


class TestDocumentIdDeterminesPartitionDirectory:
    @pytest.mark.asyncio
    async def test_multiple_uuids_land_in_correct_dirs(self, data_dir):
        # given several UUIDs with known partition indices
        test_cases = []
        for target_partition in [0, 1, 0x1FF, 0x3FF]:
            # construct a UUID whose lowest 10 bits equal target_partition
            doc_id = uuid.UUID(int=target_partition | (0xABCDEF << 10))
            test_cases.append((doc_id, target_partition))

        for doc_id, expected_partition in test_cases:
            # when we write the document
            await write_document(data_dir, {"_id": str(doc_id), "val": expected_partition})

            # then the file is in the correctly-named hex directory
            expected_dir = partition_dir_name(expected_partition)
            file_path = os.path.join(data_dir, expected_dir, f"{doc_id}.json")
            assert os.path.isfile(file_path), (
                f"Expected {file_path} for partition {expected_partition}"
            )
