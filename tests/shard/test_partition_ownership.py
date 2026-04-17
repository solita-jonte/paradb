"""Tests for shard-side partition ownership updates."""

import pytest
from fastapi.testclient import TestClient

from shard.app import app
from shard.partitions import partition_to_shard_url
from shard.url import get_host_url


@pytest.fixture(autouse=True)
def reset_state():
    partition_to_shard_url.clear()
    yield
    partition_to_shard_url.clear()


@pytest.fixture
def client():
    return TestClient(app)


class TestPostCmdPartitionsUpdatesTable:
    def test_partition_table_updated(self, client):
        # given a list of shard partition infos
        payload = [
            {"url": "http://shard-1:12345", "load": 0.0, "partitions": [0, 1, 2]},
            {"url": "http://shard-2:23456", "load": 0.0, "partitions": [3, 4, 5]},
        ]

        # when we POST /internal/partitions
        response = client.post("/internal/partitions", json=payload)

        # then the mapping is updated correctly
        assert response.status_code == 200
        assert partition_to_shard_url[0] == "http://shard-1:12345"
        assert partition_to_shard_url[3] == "http://shard-2:23456"


class TestPartitionTableReplacesState:
    def test_latest_mapping_active(self, client):
        # given an initial partition table
        client.post("/internal/partitions", json=[
            {"url": "http://shard-1:12345", "load": 0.0, "partitions": [0, 1, 2]},
        ])

        # when we send a different table
        client.post("/internal/partitions", json=[
            {"url": "http://shard-2:23456", "load": 0.0, "partitions": [0, 1, 2]},
        ])

        # then only the latest mapping is active
        assert partition_to_shard_url[0] == "http://shard-2:23456"


class TestLookUpOwningHost:
    def test_returns_correct_hostname(self, client):
        # given a partition table
        client.post("/internal/partitions", json=[
            {"url": "http://shard-1:12345", "load": 0.0, "partitions": [10, 20]},
            {"url": "http://shard-2:23456", "load": 0.0, "partitions": [30, 40]},
        ])

        # when we look up partition 30
        # then it returns shard-2
        assert partition_to_shard_url[30] == "http://shard-2:23456"


class TestShardRecognizesOwnPartitions:
    def test_own_vs_other(self, client):
        # given the local host's url
        url = get_host_url()

        # and a partition table with some local and some remote partitions
        client.post("/internal/partitions", json=[
            {"url": url, "load": 0.0, "partitions": [0, 1]},
            {"url": "http://other-shard", "load": 0.0, "partitions": [2, 3]},
        ])

        # then the shard identifies its own partitions
        assert partition_to_shard_url[0] == url
        assert partition_to_shard_url[1] == url
        assert partition_to_shard_url[2] == "http://other-shard"
