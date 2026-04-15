"""Tests for the POST /document and DELETE /document REST endpoints."""

import uuid

import pytest
from fastapi.testclient import TestClient

from shard.app import app


@pytest.fixture
def data_dir(tmp_path):
    """Provide a fresh DATA_DIR and configure the app to use it."""
    d = str(tmp_path / "data")
    # Set the data dir on the app/module so the endpoints use it
    import shard.app as shard_app
    original = getattr(shard_app, "DATA_DIR", None)
    shard_app.DATA_DIR = d
    yield d
    if original is not None:
        shard_app.DATA_DIR = original


@pytest.fixture
def client(data_dir):
    """Provide a TestClient for the shard FastAPI app."""
    return TestClient(app)


class TestPostDocumentWithoutId:
    def test_creates_document_with_generated_id(self, client):
        # given a document body with no _id
        body = {"name": "Alice"}

        # when we POST it
        response = client.post("/document", json=body)

        # then we get a 201 with a generated UUID _id
        assert response.status_code == 201
        data = response.json()
        assert "_id" in data
        uuid.UUID(data["_id"])  # valid UUID
        assert data["name"] == "Alice"


class TestPostDocumentWithId:
    def test_creates_document_with_supplied_id(self, client, data_dir):
        # given a document with a known _id
        doc_id = str(uuid.uuid4())
        body = {"_id": doc_id, "name": "Bob"}

        # when we POST it
        response = client.post("/document", json=body)

        # then the document is created
        assert response.status_code in (200, 201)
        data = response.json()
        assert data["_id"] == doc_id
        assert data["name"] == "Bob"


class TestPostDocumentUpsert:
    def test_full_replacement_on_upsert(self, client):
        # given an existing document
        doc_id = str(uuid.uuid4())
        client.post("/document", json={"_id": doc_id, "a": 1})

        # when we POST again with a different body
        response = client.post("/document", json={"_id": doc_id, "b": 2})

        # then the response has the new field and not the old
        data = response.json()
        assert data["b"] == 2
        assert "a" not in data
        assert data["_id"] == doc_id


class TestPostDocumentReturnsFullDocument:
    def test_includes_all_fields_plus_id(self, client):
        # given a body with no _id
        body = {"name": "Alice", "age": 30}

        # when we POST it
        response = client.post("/document", json=body)

        # then the response contains all fields plus _id
        data = response.json()
        assert "_id" in data
        assert data["name"] == "Alice"
        assert data["age"] == 30


class TestDeleteDocument:
    def test_removes_document(self, client):
        # given a created document
        create_resp = client.post("/document", json={"name": "delete-me"})
        doc_id = create_resp.json()["_id"]

        # when we DELETE it
        response = client.delete(f"/document/{doc_id}")

        # then it succeeds
        assert response.status_code in (200, 204)


class TestDeleteNonExistentDocument:
    def test_returns_not_found_or_ok(self, client):
        # given a random UUID that was never created
        doc_id = str(uuid.uuid4())

        # when we DELETE it
        response = client.delete(f"/document/{doc_id}")

        # then we get 404 or 200
        assert response.status_code in (200, 404)


class TestPostDocumentInvalidJson:
    def test_returns_422(self, client):
        # given an invalid body
        # when we POST it
        response = client.post(
            "/document",
            content="not valid json",
            headers={"Content-Type": "application/json"},
        )

        # then we get 422
        assert response.status_code == 422
