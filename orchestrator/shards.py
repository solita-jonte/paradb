import time

from .partitions import Partition
from shared.types.shard import ShardInfo

STALE_TIMEOUT = 15  # seconds


class Shard:
    def __init__(self, hostname: str):
        self.hostname: str = hostname
        self.partitions: list[Partition] = []
        self.load: float = 0.0
        self.last_heartbeat: float = time.time()

    def add_partitions(self, partitions: list[Partition]):
        for partition in partitions:
            partition.owner = self.hostname
            self.partitions.append(partition)

    def remove_partition(self, partition: Partition):
        self.partitions.remove(partition)
        partition.owner = ""

    def release(self):
        for partition in self.partitions:
            partition.release()
        self.partitions.clear()
        self.load = 0.0


NAME_TO_SHARD: dict[str, Shard] = {}


def init():
    pass


def all_shards():
    return NAME_TO_SHARD.values()


def fetch_shard(shard_info: ShardInfo):
    shard = NAME_TO_SHARD.get(shard_info.hostname)
    if not shard:
        shard = NAME_TO_SHARD[shard_info.hostname] = Shard(shard_info.hostname)
    shard.load = shard_info.load
    shard.last_heartbeat = time.time()
    return shard


def release_shard(name: str):
    shard = NAME_TO_SHARD.get(name)
    if shard:
        shard.release()
        del NAME_TO_SHARD[name]


def remove_stale_shards():
    """Remove shards that haven't sent a heartbeat within STALE_TIMEOUT seconds."""
    now = time.time()
    stale = [name for name, shard in NAME_TO_SHARD.items()
             if now - shard.last_heartbeat > STALE_TIMEOUT]
    for name in stale:
        release_shard(name)
