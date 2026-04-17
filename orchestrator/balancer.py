import asyncio

from orchestrator.shards import all_shards
from orchestrator.shard_command import ShardCommand, ShardBroadcastCommand


try_lock = asyncio.Lock()


async def balance_shards():
    """Balance so each shard have equal number of partitions."""

    if try_lock.locked():
        print("Skipping balancing, someone already at it!")
        return

    # only one thread at a time
    async with try_lock:
        print("Re-balancing shards...")
        # sort based on how many partitions they each have
        shards_balance = sorted(all_shards(), key=lambda sh: len(sh.partitions))
        # check if we move a partition
        if shards_balance and len(shards_balance[0].partitions) <= len(shards_balance[-1].partitions) - 2:
            # yep, move partition
            smallest = shards_balance[0]
            biggest = shards_balance[-1]
            partition = biggest.partitions[-1]
            print(f"Moving partition {partition.index} from shard {biggest.url} to {smallest.url}")
            # stop writes from this shard
            await ShardCommand(biggest).halt_flush_partition_writes(partition.index)
            biggest.remove_partition(partition)
            smallest.add_partitions([partition])
            # enable writes from all other shards, and finally
            await ShardBroadcastCommand(shards_balance).send_partitions()
