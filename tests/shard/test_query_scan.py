"""Tests for query scanning — reading all documents from disk and filtering."""

import os
import uuid

import pytest

from shard.document_storage import write_document
from shard.query_engine import execute_query


@pytest.fixture
def data_dir(tmp_path):
    return str(tmp_path / "data")


class TestScanReturnsAllForEmptyQuery:
    def test_all_documents_returned(self, data_dir):
        # given 3 documents on disk
        docs = []
        for i in range(3):
            docs.append(write_document(data_dir, {"val": i}))

        # when we execute an empty query
        results = execute_query(data_dir, {})

        # then all 3 are returned
        result_ids = {r["_id"] for r in results}
        expected_ids = {d["_id"] for d in docs}
        assert result_ids == expected_ids


class TestScanFiltersDocuments:
    def test_matching_subset_returned(self, data_dir):
        # given documents with various field values
        write_document(data_dir, {"name": "Alice", "age": 30})
        write_document(data_dir, {"name": "Bob", "age": 25})
        write_document(data_dir, {"name": "Charlie", "age": 35})

        # when we query for age > 28
        results = execute_query(data_dir, {"age": {"$gt": 28}})

        # then only Alice and Charlie are returned
        names = {r["name"] for r in results}
        assert names == {"Alice", "Charlie"}


class TestScanOnEmptyDataDir:
    def test_empty_list_returned(self, data_dir):
        # given an empty data directory
        os.makedirs(data_dir, exist_ok=True)

        # when we execute a query
        results = execute_query(data_dir, {})

        # then an empty list is returned
        assert results == []


class TestScanByIdFilter:
    def test_single_document_by_id(self, data_dir):
        # given a document with a known ID
        doc = write_document(data_dir, {"name": "target"})
        write_document(data_dir, {"name": "other"})

        # when we query by _id
        results = execute_query(data_dir, {"_id": doc["_id"]})

        # then only that document is returned
        assert len(results) == 1
        assert results[0]["_id"] == doc["_id"]
