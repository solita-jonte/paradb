import httpx

from .shards import Shard, all_shards


class ShardCommand:
    """Sends HTTP commands from the orchestrator to a single shard."""

    def __init__(self, shard: Shard):
        self.shard = shard

    def halt_flush_partition_writes(self, partition_index: int):
        """Tell the shard to halt writes on a specific partition."""
        print('sending halt_flush_partition_writes -> shard', self.shard.url)
        url = f"{self.shard.url}/cmd/partition"
        httpx.delete(url, params={"partition_index": partition_index})

    def send_partitions(self):
        """Send the current partition table to this shard."""
        print('sending partitions -> shard', self.shard.url)
        payload = []
        for s in all_shards():
            payload.append({
                "url": s.url,
                "load": s.load,
                "partitions": [p.index for p in s.partitions],
            })
        url = f"{self.shard.url}/cmd/partitions"
        httpx.post(url, json=payload)


class ShardBroadcastCommand:
    """Broadcasts partition updates to all shards."""

    def __init__(self, shards: list[Shard]):
        self.shards = shards

    def send_partitions(self):
        for shard in self.shards:
            ShardCommand(shard).send_partitions()
