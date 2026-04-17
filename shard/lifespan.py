import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI

from shard.orchestrator_command import OrchestratorCommand
from shard.url import get_host_url
from shared.types.shard import ShardInfo


HEARTBEAT_INTERVAL = 5  # seconds


async def update_shard():
    url = get_host_url()
    shard_info = ShardInfo(url=url, load=0.0)
    await OrchestratorCommand().update_shard(shard_info)


async def _heartbeat_loop():
    """Background coroutine that sends periodic heartbeats to the orchestrator."""
    while True:
        try:
            print('sending update -> orchestrator')
            await update_shard()
        except Exception as ex:
            print("Shard wasn't updated:", ex)
        await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_heartbeat_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
