#!/usr/bin/env python3

from fastapi import FastAPI
import uvicorn

from .balancer import balance_shards
from .partitions import init as partitions_init, get_free_partitions
from .shards import init as shards_init, fetch_shard, release_shard
from .shard_command import ShardCommand
from shared.types.shard import ShardInfo


app = FastAPI()


@app.post('/shard')
def update_shard(shard_info: ShardInfo):
    shard = fetch_shard(shard_info)
    free_partitions = list(get_free_partitions())
    shard.add_partitions(free_partitions)
    if free_partitions:
        ShardCommand(shard).send_partitions()
    balance_shards()
    return {'status': 0}


@app.delete('/shard')
def delete_shard(hostname: str):
    release_shard(hostname)
    return {'status': 0}


if __name__ == "__main__":
    partitions_init()
    shards_init()
    uvicorn.run(app, host="0.0.0.0", port=3356)
