import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from orchestrator.partitions import get_free_partitions
from orchestrator.shards import all_shards, remove_stale_shards
from orchestrator.shard_command import ShardBroadcastCommand


MONITOR_INTERVAL = 5  # seconds


async def _monitor_stale_shards():
    """Background coroutine that periodically checks for stale shards and redistributes."""
    while True:
        await asyncio.sleep(MONITOR_INTERVAL)
        removed = remove_stale_shards()
        if removed:
            await _redistribute_free_partitions()


async def _redistribute_free_partitions():
    """Distribute free partitions round-robin across remaining shards and broadcast."""
    shards = list(all_shards())
    if not shards:
        return
    free = list(get_free_partitions())
    if not free:
        return
    for partition in free:
        smallest = min(shards, key=lambda s: len(s.partitions))
        smallest.add_partitions([partition])
    await ShardBroadcastCommand(shards).send_partitions()


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_monitor_stale_shards())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
