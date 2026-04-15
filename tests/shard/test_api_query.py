"""Tests for the POST /query REST endpoint."""

import uuid

import pytest
from fastapi.testclient import TestClient

from shard.app import app


@pytest.fixture
def data_dir(tmp_path):
    d = str(tmp_path / "data")
    import shard.app as shard_app
    original = getattr(shard_app, "DATA_DIR", None)
    shard_app.DATA_DIR = d
    yield d
    if original is not None:
        shard_app.DATA_DIR = original


@pytest.fixture
def client(data_dir):
    return TestClient(app)


class TestPostQueryEmptyBody:
    def test_returns_all_documents(self, client):
        # given several documents
        client.post("/document", json={"name": "Alice"})
        client.post("/document", json={"name": "Bob"})
        client.post("/document", json={"name": "Charlie"})

        # when we query with empty filter
        response = client.post("/query", json={})

        # then all documents are returned
        assert response.status_code == 200
        results = response.json()
        assert len(results) == 3


class TestPostQueryWithFilter:
    def test_returns_matching_subset(self, client):
        # given documents with various ages
        client.post("/document", json={"name": "Alice", "age": 30})
        client.post("/document", json={"name": "Bob", "age": 25})
        client.post("/document", json={"name": "Charlie", "age": 35})

        # when we query for age > 28
        response = client.post("/query", json={"age": {"$gt": 28}})

        # then only matching documents are returned
        results = response.json()
        names = {r["name"] for r in results}
        assert names == {"Alice", "Charlie"}


class TestPostQueryEmptyDatabase:
    def test_returns_empty_list(self, client):
        # given no documents
        # when we query
        response = client.post("/query", json={})

        # then we get an empty list
        assert response.status_code == 200
        assert response.json() == []


class TestPostQueryByIdFilter:
    def test_returns_single_document(self, client):
        # given a document with a known ID
        create_resp = client.post("/document", json={"name": "target"})
        doc_id = create_resp.json()["_id"]
        client.post("/document", json={"name": "other"})

        # when we query by _id
        response = client.post("/query", json={"_id": doc_id})

        # then only that document is returned
        results = response.json()
        assert len(results) == 1
        assert results[0]["_id"] == doc_id
