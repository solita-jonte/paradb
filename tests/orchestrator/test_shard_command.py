"""Tests for the HTTP commands the orchestrator sends to shards."""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from orchestrator.shard_command import ShardCommand, ShardBroadcastCommand
from orchestrator.shards import Shard
from orchestrator.partitions import init as partitions_init, INDEX_TO_PARTITION


@pytest.fixture(autouse=True)
def reset():
    partitions_init()
    yield


def _mock_async_client():
    """Create a mock httpx.AsyncClient with async context manager support."""
    mock_client = AsyncMock()
    mock_client.post.return_value = MagicMock(status_code=200)
    mock_client.delete.return_value = MagicMock(status_code=200)
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_client)
    mock_cm.__aexit__ = AsyncMock(return_value=False)
    return mock_cm, mock_client


class TestSendPartitions:
    @pytest.mark.asyncio
    async def test_sends_post_to_shard(self):
        # given a shard with partitions
        shard = Shard("shard-1")
        shard.add_partitions([INDEX_TO_PARTITION[0], INDEX_TO_PARTITION[1]])
        cmd = ShardCommand(shard)

        # when we call send_partitions
        mock_cm, mock_client = _mock_async_client()
        with patch("orchestrator.shard_command.httpx.AsyncClient", return_value=mock_cm):
            await cmd.send_partitions()

        # then an HTTP POST was made to the shard's /internal/partitions endpoint
        assert mock_client.post.called
        url = mock_client.post.call_args[0][0]
        assert "shard-1" in url
        assert "/internal/partitions" in url


class TestHaltFlushPartitionWrites:
    @pytest.mark.asyncio
    async def test_sends_delete_to_shard(self):
        # given a shard
        shard = Shard("shard-1")
        cmd = ShardCommand(shard)

        # when we call halt_flush_partition_writes
        mock_cm, mock_client = _mock_async_client()
        with patch("orchestrator.shard_command.httpx.AsyncClient", return_value=mock_cm):
            await cmd.halt_flush_partition_writes(42)

        # then an HTTP DELETE was made with the partition index
        assert mock_client.delete.called
        url = mock_client.delete.call_args[0][0]
        assert "shard-1" in url
        assert "/internal/partition" in url


class TestShardBroadcastCommand:
    @pytest.mark.asyncio
    async def test_sends_to_all_shards(self):
        # given 3 shards
        shards = [Shard(f"shard-{i}") for i in range(3)]
        for shard in shards:
            shard.add_partitions([INDEX_TO_PARTITION[shards.index(shard)]])
        broadcast = ShardBroadcastCommand(shards)

        # when we call send_partitions
        mock_cm, mock_client = _mock_async_client()
        with patch("orchestrator.shard_command.httpx.AsyncClient", return_value=mock_cm):
            await broadcast.send_partitions()

        # then 3 HTTP calls were made
        assert mock_client.post.call_count == 3
