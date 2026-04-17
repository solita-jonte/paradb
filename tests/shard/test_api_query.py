"""Tests for the POST /db/query REST endpoint."""

from fastapi.testclient import TestClient
import pytest

from shard.app import app


@pytest.fixture
def data_dir(tmp_path):
    d = str(tmp_path / "data")
    import shard.routers.db as shard_db_router
    original = getattr(shard_db_router, "DATA_DIR", None)
    shard_db_router.DATA_DIR = d
    yield d
    if original is not None:
        shard_db_router.DATA_DIR = original


@pytest.fixture
def client(data_dir):
    return TestClient(app)


class TestPostQueryEmptyBody:
    def test_returns_all_documents(self, client):
        # given several documents
        client.post("/db/document", json={"name": "Alice"})
        client.post("/db/document", json={"name": "Bob"})
        client.post("/db/document", json={"name": "Charlie"})

        # when we query with empty filter
        response = client.post("/db/query", json={})

        # then all documents are returned
        assert response.status_code == 200
        results = response.json()
        assert len(results) == 3


class TestPostQueryWithFilter:
    def test_returns_matching_subset(self, client):
        # given documents with various ages
        client.post("/db/document", json={"name": "Alice", "age": 30})
        client.post("/db/document", json={"name": "Bob", "age": 25})
        client.post("/db/document", json={"name": "Charlie", "age": 35})

        # when we query for age > 28
        response = client.post("/db/query", json={"age": {"$gt": 28}})

        # then only matching documents are returned
        results = response.json()
        names = {r["name"] for r in results}
        assert names == {"Alice", "Charlie"}


class TestPostQueryEmptyDatabase:
    def test_returns_empty_list(self, client):
        # given no documents
        # when we query
        response = client.post("/db/query", json={})

        # then we get an empty list
        assert response.status_code == 200
        assert response.json() == []


class TestPostQueryByIdFilter:
    def test_returns_single_document(self, client):
        # given a document with a known ID
        create_resp = client.post("/db/document", json={"name": "target"})
        doc_id = create_resp.json()["_id"]
        client.post("/db/document", json={"name": "other"})

        # when we query by _id
        response = client.post("/db/query", json={"_id": doc_id})

        # then only that document is returned
        results = response.json()
        assert len(results) == 1
        assert results[0]["_id"] == doc_id
