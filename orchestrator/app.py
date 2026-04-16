#!/usr/bin/env python3

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .balancer import balance_shards
from .partitions import get_free_partitions
from .shards import all_shards, fetch_shard, release_shard, remove_stale_shards
from .shard_command import ShardCommand, ShardBroadcastCommand
from shared.types.shard import ShardInfo, ShardPartitionInfo


MONITOR_INTERVAL = 5  # seconds


async def _monitor_stale_shards():
    """Background coroutine that periodically checks for stale shards and redistributes."""
    while True:
        await asyncio.sleep(MONITOR_INTERVAL)
        removed = remove_stale_shards()
        if removed:
            _redistribute_free_partitions()


def _redistribute_free_partitions():
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
    ShardBroadcastCommand(shards).send_partitions()


@asynccontextmanager
async def _lifespan(app: FastAPI):
    task = asyncio.create_task(_monitor_stale_shards())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=_lifespan)


@app.post("/shard")
def update_shard(shard_info: ShardInfo):
    shard = fetch_shard(shard_info)
    free_partitions = list(get_free_partitions())
    shard.add_partitions(free_partitions)
    if free_partitions:
        ShardCommand(shard).send_partitions()
    balance_shards()
    return {"status": 0}


@app.delete("/shard")
def delete_shard(url: str):
    release_shard(url)
    return {"status": 0}


@app.get("/shard")
def get_shards():
    shards = []
    for shard in all_shards():
        pis = [p.index for p in shard.partitions]
        shard_partitions = ShardPartitionInfo.model_construct(url=shard.url, load=shard.load, partitions=pis)
        shards.append(shard_partitions)
    return {'status': 0, 'shards': shards}
