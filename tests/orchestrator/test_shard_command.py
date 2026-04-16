"""Tests for the HTTP commands the orchestrator sends to shards."""

import pytest
from unittest.mock import patch, MagicMock

from orchestrator.shard_command import ShardCommand, ShardBroadcastCommand
from orchestrator.shards import Shard
from orchestrator.partitions import init as partitions_init, INDEX_TO_PARTITION


@pytest.fixture(autouse=True)
def reset():
    partitions_init()
    yield


class TestSendPartitions:
    def test_sends_post_to_shard(self):
        # given a shard with partitions
        shard = Shard("shard-1")
        shard.add_partitions([INDEX_TO_PARTITION[0], INDEX_TO_PARTITION[1]])
        cmd = ShardCommand(shard)

        # when we call send_partitions
        with patch("orchestrator.shard_command.httpx.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            cmd.send_partitions()

        # then an HTTP POST was made to the shard's /cmd/partitions endpoint
        assert mock_post.called
        url = mock_post.call_args[0][0]
        assert "shard-1" in url
        assert "/cmd/partitions" in url


class TestHaltFlushPartitionWrites:
    def test_sends_delete_to_shard(self):
        # given a shard
        shard = Shard("shard-1")
        cmd = ShardCommand(shard)

        # when we call halt_flush_partition_writes
        with patch("orchestrator.shard_command.httpx.delete") as mock_delete:
            mock_delete.return_value = MagicMock(status_code=200)
            cmd.halt_flush_partition_writes(42)

        # then an HTTP DELETE was made with the partition index
        assert mock_delete.called
        url = mock_delete.call_args[0][0]
        assert "shard-1" in url
        assert "/cmd/partition" in url


class TestShardBroadcastCommand:
    def test_sends_to_all_shards(self):
        # given 3 shards
        shards = [Shard(f"shard-{i}") for i in range(3)]
        for shard in shards:
            shard.add_partitions([INDEX_TO_PARTITION[shards.index(shard)]])
        broadcast = ShardBroadcastCommand(shards)

        # when we call send_partitions
        with patch("orchestrator.shard_command.httpx.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            broadcast.send_partitions()

        # then 3 HTTP calls were made
        assert mock_post.call_count == 3
