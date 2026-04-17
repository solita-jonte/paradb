from fastapi import APIRouter

from orchestrator.balancer import balance_shards
from orchestrator.partitions import get_free_partitions
from orchestrator.shards import all_shards, fetch_shard, release_shard
from orchestrator.shard_command import ShardCommand
from shared.types.shard import ShardInfo, ShardPartitionInfo


shard_router = APIRouter(prefix="/internal/shard", tags=["shard-internal"])


@shard_router.post("/heartbeat")
async def update_shard(shard_info: ShardInfo):
    shard = fetch_shard(shard_info)
    free_partitions = list(get_free_partitions())
    shard.add_partitions(free_partitions)
    if free_partitions:
        await ShardCommand(shard).send_partitions()
    await balance_shards()
    return {"status": 0}


@shard_router.delete("")
def delete_shard(url: str):
    release_shard(url)
    return {"status": 0}


@shard_router.get("")
def get_shards():
    shards = []
    for shard in all_shards():
        pis = [p.index for p in shard.partitions]
        shard_partitions = ShardPartitionInfo.model_construct(url=shard.url, load=shard.load, partitions=pis)
        shards.append(shard_partitions)
    return {'status': 0, 'shards': shards}
