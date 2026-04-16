import time

from .partitions import Partition
from shared.types.shard import ShardInfo

STALE_TIMEOUT = 15  # seconds


class Shard:
    def __init__(self, url: str):
        self.url: str = url
        self.partitions: list[Partition] = []
        self.load: float = 0.0
        self.last_heartbeat: float = time.time()

    def add_partitions(self, partitions: list[Partition]):
        for partition in partitions:
            partition.owner = self.url
            self.partitions.append(partition)

    def remove_partition(self, partition: Partition):
        self.partitions.remove(partition)
        partition.owner = ""

    def release(self):
        for partition in self.partitions:
            partition.release()
        self.partitions.clear()
        self.load = 0.0


URL_TO_SHARD: dict[str, Shard] = {}


def init():
    pass


def all_shards():
    return URL_TO_SHARD.values()


def fetch_shard(shard_info: ShardInfo):
    shard = URL_TO_SHARD.get(shard_info.url)
    if not shard:
        shard = URL_TO_SHARD[shard_info.url] = Shard(shard_info.url)
    shard.load = shard_info.load
    shard.last_heartbeat = time.time()
    return shard


def release_shard(url: str):
    shard = URL_TO_SHARD.get(url)
    if shard:
        shard.release()
        del URL_TO_SHARD[url]


def remove_stale_shards() -> list[str]:
    """Remove shards that haven't sent a heartbeat within STALE_TIMEOUT seconds.
    Returns the list of removed shard URLs."""
    now = time.time()
    stale = [url for url, shard in URL_TO_SHARD.items()
             if now - shard.last_heartbeat > STALE_TIMEOUT]
    for url in stale:
        release_shard(url)
    return stale
