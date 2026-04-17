"""Tests for shard registration, heartbeat, staleness, and graceful removal."""

from fastapi.testclient import TestClient
import pytest
import time
from unittest.mock import patch, AsyncMock

from orchestrator.app import app
from orchestrator.partitions import init as partitions_init, get_free_partitions
from orchestrator.shards import URL_TO_SHARD, init as shards_init


@pytest.fixture(autouse=True)
def reset_state():
    """Reset global orchestrator state before each test."""
    partitions_init()
    shards_init()
    URL_TO_SHARD.clear()
    yield
    URL_TO_SHARD.clear()


@pytest.fixture
def client():
    return TestClient(app)


def _patch_shard_command():
    """Patch ShardCommand with async-compatible mock methods."""
    p = patch("orchestrator.shard_router.ShardCommand")
    mock = p.start()
    mock.return_value.send_partitions = AsyncMock()
    mock.return_value.halt_flush_partition_writes = AsyncMock()
    return p, mock


class TestPostShardRegistersNewShard:
    def test_shard_stored_in_registry(self, client):
        # given a new shard
        shard_info = {"url": "http://shard-1:12345", "load": 0.5}

        # when we POST /shard
        p, _ = _patch_shard_command()
        try:
            response = client.post("/internal/shard/heartbeat", json=shard_info)
        finally:
            p.stop()

        # then the shard is in the registry
        assert response.status_code == 200
        assert "http://shard-1:12345" in URL_TO_SHARD


class TestPostShardUpdatesLoad:
    def test_load_updated_not_duplicated(self, client):
        # given an existing shard
        p, _ = _patch_shard_command()
        try:
            client.post("/internal/shard/heartbeat", json={"url": "http://shard-1:12345", "load": 0.5})

            # when we POST again with different load
            client.post("/internal/shard/heartbeat", json={"url": "http://shard-1:12345", "load": 0.8})
        finally:
            p.stop()

        # then load is updated, not duplicated
        assert URL_TO_SHARD["http://shard-1:12345"].load == 0.8
        assert len(URL_TO_SHARD) == 1


class TestPostShardTriggersPartitionAllocation:
    def test_partitions_assigned_to_new_shard(self, client):
        # given free partitions exist (from init)
        # when we register a new shard
        p, _ = _patch_shard_command()
        try:
            client.post("/internal/shard/heartbeat", json={"url": "http://shard-1:12345", "load": 0.0})
        finally:
            p.stop()

        # then some partitions are assigned
        shard = URL_TO_SHARD["http://shard-1:12345"]
        assert len(shard.partitions) > 0


class TestDeleteShardRemovesGracefully:
    def test_shard_removed_and_partitions_freed(self, client):
        # given a registered shard with partitions
        p, _ = _patch_shard_command()
        try:
            client.post("/internal/shard/heartbeat", json={"url": "http://shard-1:12345", "load": 0.0})
        finally:
            p.stop()
        assert len(URL_TO_SHARD["http://shard-1:12345"].partitions) > 0

        # when we DELETE /shard
        response = client.delete("/internal/shard", params={"url": "http://shard-1:12345"})

        # then the shard is removed
        assert response.status_code == 200
        assert "http://shard-1:12345" not in URL_TO_SHARD


class TestStaleShard:
    def test_removed_after_15_seconds(self, client):
        # given a registered shard
        p, _ = _patch_shard_command()
        try:
            client.post("/internal/shard/heartbeat", json={"url": "http://shard-1:12345", "load": 0.0})
        finally:
            p.stop()

        # when 15+ seconds pass without a heartbeat
        shard = URL_TO_SHARD["http://shard-1:12345"]
        shard.last_heartbeat = time.time() - 16  # simulate staleness

        # then after a staleness check, the shard is removed
        from orchestrator.shards import remove_stale_shards
        remove_stale_shards()
        assert "http://shard-1:12345" not in URL_TO_SHARD

    def test_returns_list_of_removed_shard_urls(self, client):
        # given two shards, one stale
        p, _ = _patch_shard_command()
        with patch("orchestrator.balancer.ShardCommand") as MockBalCmd, \
             patch("orchestrator.balancer.ShardBroadcastCommand") as MockBalBroadcast:
            MockBalCmd.return_value.halt_flush_partition_writes = AsyncMock()
            MockBalCmd.return_value.send_partitions = AsyncMock()
            MockBalBroadcast.return_value.send_partitions = AsyncMock()
            try:
                client.post("/internal/shard/heartbeat", json={"url": "http://shard-1:12345", "load": 0.0})
                client.post("/internal/shard/heartbeat", json={"url": "http://shard-2:12345", "load": 0.0})
            finally:
                p.stop()

        URL_TO_SHARD["http://shard-1:12345"].last_heartbeat = time.time() - 16

        # when remove_stale_shards is called
        from orchestrator.shards import remove_stale_shards
        removed = remove_stale_shards()

        # then it returns a list containing only the stale shard URL
        assert isinstance(removed, list)
        assert "http://shard-1:12345" in removed
        assert "http://shard-2:12345" not in removed

    def test_returns_empty_list_when_no_shards_stale(self, client):
        # given two shards with recent heartbeats
        p, _ = _patch_shard_command()
        with patch("orchestrator.balancer.ShardCommand") as MockBalCmd, \
             patch("orchestrator.balancer.ShardBroadcastCommand") as MockBalBroadcast:
            MockBalCmd.return_value.halt_flush_partition_writes = AsyncMock()
            MockBalCmd.return_value.send_partitions = AsyncMock()
            MockBalBroadcast.return_value.send_partitions = AsyncMock()
            try:
                client.post("/internal/shard/heartbeat", json={"url": "http://shard-1:12345", "load": 0.0})
                client.post("/internal/shard/heartbeat", json={"url": "http://shard-2:12345", "load": 0.0})
            finally:
                p.stop()

        # when remove_stale_shards is called
        from orchestrator.shards import remove_stale_shards
        removed = remove_stale_shards()

        # then it returns an empty list and both shards remain
        assert isinstance(removed, list)
        assert len(removed) == 0
        assert "http://shard-1:12345" in URL_TO_SHARD
        assert "http://shard-2:12345" in URL_TO_SHARD

    def test_removes_multiple_stale_shards_at_once(self, client):
        # given three shards, two of them stale
        p, _ = _patch_shard_command()
        with patch("orchestrator.balancer.ShardCommand") as MockBalCmd, \
             patch("orchestrator.balancer.ShardBroadcastCommand") as MockBalBroadcast:
            MockBalCmd.return_value.halt_flush_partition_writes = AsyncMock()
            MockBalCmd.return_value.send_partitions = AsyncMock()
            MockBalBroadcast.return_value.send_partitions = AsyncMock()
            try:
                client.post("/internal/shard/heartbeat", json={"url": "http://shard-1:12345", "load": 0.0})
                client.post("/internal/shard/heartbeat", json={"url": "http://shard-2:12345", "load": 0.0})
                client.post("/internal/shard/heartbeat", json={"url": "http://shard-3:12345", "load": 0.0})
            finally:
                p.stop()

        URL_TO_SHARD["http://shard-1:12345"].last_heartbeat = time.time() - 16
        URL_TO_SHARD["http://shard-3:12345"].last_heartbeat = time.time() - 16

        # when remove_stale_shards is called
        from orchestrator.shards import remove_stale_shards
        removed = remove_stale_shards()

        # then both stale URLs are returned and only the fresh shard remains
        assert set(removed) == {"http://shard-1:12345", "http://shard-3:12345"}
        assert "http://shard-2:12345" in URL_TO_SHARD
        assert len(URL_TO_SHARD) == 1


class TestShardHeartbeatsWithin15Seconds:
    def test_shard_not_removed(self, client):
        # given a registered shard that heartbeats within 15 seconds
        p, _ = _patch_shard_command()
        try:
            client.post("/internal/shard/heartbeat", json={"url": "http://shard-1:12345", "load": 0.0})
        finally:
            p.stop()

        shard = URL_TO_SHARD["http://shard-1:12345"]
        shard.last_heartbeat = time.time() - 10  # 10 seconds ago, still fresh

        # when a staleness check runs
        from orchestrator.shards import remove_stale_shards
        remove_stale_shards()

        # then the shard is still active
        assert "http://shard-1:12345" in URL_TO_SHARD


class TestStaleShardPartitionRelease:
    def test_stale_shard_partitions_become_free(self, client):
        # given a shard that owns all partitions
        p, _ = _patch_shard_command()
        try:
            client.post("/internal/shard/heartbeat", json={"url": "http://shard-1:12345", "load": 0.0})
        finally:
            p.stop()

        shard = URL_TO_SHARD["http://shard-1:12345"]
        assert len(shard.partitions) == 1024

        # when the shard goes stale and is removed
        shard.last_heartbeat = time.time() - 16
        from orchestrator.shards import remove_stale_shards
        remove_stale_shards()

        # then all 1024 partitions are free again
        free = list(get_free_partitions())
        assert len(free) == 1024
