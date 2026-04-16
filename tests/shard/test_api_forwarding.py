"""Tests for write forwarding when a shard does not own the target partition."""

from fastapi.testclient import TestClient
import pytest
from unittest.mock import patch, MagicMock
import uuid

from shard.app import app
from shard.document_storage import partition_index_for_id
from shard.url import get_host_url


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


class TestWriteToNonOwnedPartitionForwards:
    def test_forwards_to_correct_shard(self, client):
        # given the partition table assigns the target partition to another shard
        doc_id = str(uuid.uuid4())
        partition_idx = partition_index_for_id(uuid.UUID(doc_id))

        import shard.app as shard_app
        original_mapping = shard_app.partition_to_shard_url.copy()
        # set up: this partition is owned by "other-shard"
        shard_app.partition_to_shard_url[partition_idx] = "other-shard"

        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"_id": doc_id, "name": "forwarded"}

        try:
            # when we POST a document whose ID maps to that partition
            with patch("shard.app.httpx.post", return_value=mock_response) as mock_post:
                response = client.post("/document", json={"_id": doc_id, "name": "test"})

            # then the mock was called with the forwarded flag
            assert mock_post.called
            call_args = mock_post.call_args
            assert "other-shard" in call_args[0][0] or "other-shard" in str(call_args)

            # and the response is proxied back
            assert response.status_code == 201
        finally:
            shard_app.partition_to_shard_url.clear()
            shard_app.partition_to_shard_url.update(original_mapping)


class TestForwardedWriteToNonOwnedReturnsRetry:
    def test_returns_error_not_forward(self, client):
        # given a forwarded request that arrives at a shard that doesn't own the partition
        doc_id = str(uuid.uuid4())
        partition_idx = partition_index_for_id(uuid.UUID(doc_id))

        import shard.app as shard_app
        original_mapping = shard_app.partition_to_shard_url.copy()
        shard_app.partition_to_shard_url[partition_idx] = "yet-another-shard"

        try:
            # when we POST with the forwarded flag set
            response = client.post(
                "/document",
                json={"_id": doc_id, "name": "test"},
                headers={"X-Forwarded": "true"},
            )

            # then we get an error response (not another forward)
            assert response.status_code in (409, 503, 307)
        finally:
            shard_app.partition_to_shard_url.clear()
            shard_app.partition_to_shard_url.update(original_mapping)


class TestForwardedWriteToOwnedPartitionSucceeds:
    def test_writes_document_normally(self, client):
        # given a forwarded request for a partition this shard owns
        doc_id = str(uuid.uuid4())
        partition_idx = partition_index_for_id(uuid.UUID(doc_id))

        import shard.app as shard_app
        url = get_host_url()
        original_mapping = shard_app.partition_to_shard_url.copy()
        shard_app.partition_to_shard_url[partition_idx] = url

        try:
            # when we POST with the forwarded flag
            response = client.post(
                "/document",
                json={"_id": doc_id, "name": "test"},
                headers={"X-Forwarded": "true"},
            )

            # then the document is written successfully
            assert response.status_code in (200, 201)
            data = response.json()
            assert data["_id"] == doc_id
        finally:
            shard_app.partition_to_shard_url.clear()
            shard_app.partition_to_shard_url.update(original_mapping)
