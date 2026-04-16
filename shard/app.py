#!/usr/bin/env python3

import asyncio
import json
import os
import socket
import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import httpx

from .document_storage import partition_index_for_id, write_document, read_document, delete_document
from .query_engine import execute_query
from .document_locking import get_partition_lock, get_document_lock
from .orchestrator_command import OrchestratorCommand
from .url import get_host_url
from shared.file_utils import osfunc_retry
from shared.types.shard import ShardInfo, ShardPartitionInfo


HEARTBEAT_INTERVAL = 5  # seconds
DATA_DIR = os.environ.get("DATA_DIR", "./data/")

app = FastAPI()
shards: list[ShardPartitionInfo] = []
partition_to_shard_url: dict[int, str] = {}


@app.post("/document", status_code=201)
async def create_or_upsert_document(request: Request):
    """Create or upsert a document. Forwards to owning shard if needed."""
    raw = await request.body()
    try:
        body = json.loads(raw)
    except (ValueError, json.JSONDecodeError):
        return JSONResponse(status_code=422, content={"error": "invalid JSON"})
    is_forwarded = request.headers.get("X-Forwarded", "").lower() == "true"

    doc_id_str = body.get("_id")
    if doc_id_str:
        doc_id = uuid.UUID(doc_id_str)
    else:
        doc_id = uuid.uuid4()
        body["_id"] = str(doc_id)

    partition_idx = partition_index_for_id(doc_id)
    owner_url = partition_to_shard_url.get(partition_idx)
    url = get_host_url()

    if owner_url and owner_url != url:
        if is_forwarded:
            return JSONResponse(status_code=503, content={"error": "retry", "detail": "partition not owned"})
        resp = httpx.post(
            f"{owner_url}/document",
            json=body,
            headers={"X-Forwarded": "true"},
        )
        return JSONResponse(status_code=resp.status_code, content=resp.json())

    try:
        rw_lock = get_partition_lock(partition_idx)
        with rw_lock.for_reading():
            doc_lock = get_document_lock(str(doc_id))
            with doc_lock:
                result = osfunc_retry(lambda: write_document(DATA_DIR, body), retries=3)
    except TimeoutError:
        return JSONResponse(status_code=500, content=dict(error="Unable to write document"))

    return JSONResponse(status_code=201, content=result)


@app.delete("/document/{doc_id}")
async def remove_document(doc_id: str):
    """Delete a document by ID. Treated as a write operation with ownership checks."""
    try:
        doc_id_uuid = uuid.UUID(doc_id)
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "invalid document ID"})

    partition_idx = partition_index_for_id(doc_id_uuid)
    owner_url = partition_to_shard_url.get(partition_idx)
    url = get_host_url()

    if owner_url and owner_url != url:
        resp = httpx.delete(f"{owner_url}/document/{doc_id}")
        return JSONResponse(status_code=resp.status_code, content=resp.json())

    rw_lock = get_partition_lock(partition_idx)
    with rw_lock.for_reading():
        doc_lock = get_document_lock(doc_id)
        with doc_lock:
            deleted = delete_document(DATA_DIR, doc_id)

    if not deleted:
        return JSONResponse(status_code=404, content={"error": "not found"})
    return JSONResponse(status_code=200, content={"status": "deleted"})


@app.post("/query")
async def query_documents(request: Request):
    """Query documents with MongoDB-like filter."""
    body = await request.json()
    results = execute_query(DATA_DIR, body)
    return JSONResponse(status_code=200, content=results)


@app.delete("/cmd/partition")
def halt_flush_partition_writes(partition_index: int):
    """Halt writes to a partition by acquiring the write lock."""
    print("shard got halt_flush_partition_writes for", partition_index)
    rw_lock = get_partition_lock(partition_index)
    rw_lock.acquire_write()
    return {"status": 0}


@app.post("/cmd/partitions")
def update_shards(shard_partition_infos: list[ShardPartitionInfo]):
    print("shard got new partition table")
    global shards
    shards = shard_partition_infos
    for shard in shard_partition_infos:
        for partition in shard.partitions:
            partition_to_shard_url[partition] = shard.url
    return {"status": 0}


def update_shard():
    url = get_host_url()
    shard_info = ShardInfo(url=url, load=0.0)
    OrchestratorCommand().update_shard(shard_info)


async def _heartbeat_loop():
    """Background coroutine that sends periodic heartbeats to the orchestrator."""
    while True:
        try:
            print('sending update -> orchestrator')
            update_shard()
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


app.router.lifespan_context = lifespan
