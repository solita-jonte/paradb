from fastapi import APIRouter

from shard.document_locking import get_partition_lock
from shard.partitions import partition_to_shard_url
from shared.types.shard import ShardPartitionInfo


internal_router = APIRouter(prefix="/internal", tags=["shard-internal"])


@internal_router.delete("/partitions")
async def halt_flush_partition_writes(partition_index: int):
    """Halt writes to a partition by acquiring the write lock."""
    print("shard got halt_flush_partition_writes for", partition_index)
    rw_lock = await get_partition_lock(partition_index)
    await rw_lock.acquire_write()
    return {"status": 0}


@internal_router.post("/partitions")
def update_shards(shard_partition_infos: list[ShardPartitionInfo]):
    print("shard got new partition table")
    for shard in shard_partition_infos:
        for partition in shard.partitions:
            partition_to_shard_url[partition] = shard.url
    return {"status": 0}
