"""Tests for shard-side partition ownership updates."""

import socket

import pytest
from fastapi.testclient import TestClient

from shard.app import app, partition_to_shard_host, shards
from shared.types.shard import ShardPartitionInfo


@pytest.fixture(autouse=True)
def reset_state():
    partition_to_shard_host.clear()
    shards.clear()
    yield
    partition_to_shard_host.clear()
    shards.clear()


@pytest.fixture
def client():
    return TestClient(app)


class TestPostCmdPartitionsUpdatesTable:
    def test_partition_table_updated(self, client):
        # given a list of shard partition infos
        payload = [
            {"hostname": "shard-1", "load": 0.0, "partitions": [0, 1, 2]},
            {"hostname": "shard-2", "load": 0.0, "partitions": [3, 4, 5]},
        ]

        # when we POST /cmd/partitions
        response = client.post("/cmd/partitions", json=payload)

        # then the mapping is updated correctly
        assert response.status_code == 200
        assert partition_to_shard_host[0] == "shard-1"
        assert partition_to_shard_host[3] == "shard-2"


class TestPartitionTableReplacesState:
    def test_latest_mapping_active(self, client):
        # given an initial partition table
        client.post("/cmd/partitions", json=[
            {"hostname": "shard-1", "load": 0.0, "partitions": [0, 1, 2]},
        ])

        # when we send a different table
        client.post("/cmd/partitions", json=[
            {"hostname": "shard-2", "load": 0.0, "partitions": [0, 1, 2]},
        ])

        # then only the latest mapping is active
        assert partition_to_shard_host[0] == "shard-2"


class TestLookUpOwningHost:
    def test_returns_correct_hostname(self, client):
        # given a partition table
        client.post("/cmd/partitions", json=[
            {"hostname": "shard-1", "load": 0.0, "partitions": [10, 20]},
            {"hostname": "shard-2", "load": 0.0, "partitions": [30, 40]},
        ])

        # when we look up partition 30
        # then it returns shard-2
        assert partition_to_shard_host[30] == "shard-2"


class TestShardRecognizesOwnPartitions:
    def test_own_vs_other(self, client):
        # given the local hostname
        hostname = socket.gethostname()

        # and a partition table with some local and some remote partitions
        client.post("/cmd/partitions", json=[
            {"hostname": hostname, "load": 0.0, "partitions": [0, 1]},
            {"hostname": "other-shard", "load": 0.0, "partitions": [2, 3]},
        ])

        # then the shard identifies its own partitions
        assert partition_to_shard_host[0] == hostname
        assert partition_to_shard_host[1] == hostname
        assert partition_to_shard_host[2] == "other-shard"
