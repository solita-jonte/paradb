import requests

from .shards import Shard, all_shards

SHARD_PORT = 3357


class ShardCommand:
    """Sends HTTP commands from the orchestrator to a single shard."""

    def __init__(self, shard: Shard):
        self.shard = shard

    def halt_flush_partition_writes(self, partition_index: int):
        """Tell the shard to halt writes on a specific partition."""
        url = f"http://{self.shard.hostname}:{SHARD_PORT}/cmd/partition"
        requests.delete(url, params={"partition_index": partition_index})

    def send_partitions(self):
        """Send the current partition table to this shard."""
        payload = []
        for s in all_shards():
            payload.append({
                "hostname": s.hostname,
                "load": s.load,
                "partitions": [p.index for p in s.partitions],
            })
        url = f"http://{self.shard.hostname}:{SHARD_PORT}/cmd/partitions"
        requests.post(url, json=payload)


class ShardBroadcastCommand:
    """Broadcasts partition updates to all shards."""

    def __init__(self, shards: list[Shard]):
        self.shards = shards

    def send_partitions(self):
        for shard in self.shards:
            ShardCommand(shard).send_partitions()
