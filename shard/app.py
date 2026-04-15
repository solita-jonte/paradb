#!/usr/bin/env python3

import json
import os
import socket
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import requests
import uvicorn

from .document_storage import partition_index_for_id, write_document, read_document, delete_document
from .query_engine import execute_query
from .document_locking import get_partition_lock, get_document_lock
from .orchestrator_command import OrchestratorCommand
from shared.types.shard import ShardInfo, ShardPartitionInfo


DATA_DIR = os.environ.get("DATA_DIR", "./data/")
SHARD_PORT = int(os.environ.get("SHARD_PORT", "3357"))

app = FastAPI()
shards: list[ShardPartitionInfo] = []
partition_to_shard_host: dict[int, str] = {}


@app.post('/document', status_code=201)
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
    owner = partition_to_shard_host.get(partition_idx)
    hostname = socket.gethostname()

    if owner and owner != hostname:
        if is_forwarded:
            return JSONResponse(status_code=503, content={"error": "retry", "detail": "partition not owned"})
        resp = requests.post(
            f"http://{owner}:{SHARD_PORT}/document",
            json=body,
            headers={"X-Forwarded": "true"},
        )
        return JSONResponse(status_code=resp.status_code, content=resp.json())

    rw_lock = get_partition_lock(partition_idx)
    with rw_lock.for_reading():
        doc_lock = get_document_lock(str(doc_id))
        with doc_lock:
            result = write_document(DATA_DIR, body)

    return JSONResponse(status_code=201, content=result)


@app.delete('/document/{doc_id}')
async def remove_document(doc_id: str):
    """Delete a document by ID. Treated as a write operation with ownership checks."""
    try:
        doc_id_uuid = uuid.UUID(doc_id)
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "invalid document ID"})

    partition_idx = partition_index_for_id(doc_id_uuid)
    owner = partition_to_shard_host.get(partition_idx)
    hostname = socket.gethostname()

    if owner and owner != hostname:
        resp = requests.delete(f"http://{owner}:{SHARD_PORT}/document/{doc_id}")
        return JSONResponse(status_code=resp.status_code, content=resp.json())

    rw_lock = get_partition_lock(partition_idx)
    with rw_lock.for_reading():
        doc_lock = get_document_lock(doc_id)
        with doc_lock:
            deleted = delete_document(DATA_DIR, doc_id)

    if not deleted:
        return JSONResponse(status_code=404, content={"error": "not found"})
    return JSONResponse(status_code=200, content={"status": "deleted"})


@app.post('/query')
async def query_documents(request: Request):
    """Query documents with MongoDB-like filter."""
    body = await request.json()
    results = execute_query(DATA_DIR, body)
    return JSONResponse(status_code=200, content=results)


@app.delete('/cmd/partition')
def halt_flush_partition_writes(partition_index: int):
    """Halt writes to a partition by acquiring the write lock."""
    rw_lock = get_partition_lock(partition_index)
    rw_lock.acquire_write()
    return {'status': 0}


@app.post('/cmd/partitions')
def update_shards(shard_partition_infos: list[ShardPartitionInfo]):
    global shards
    shards = shard_partition_infos
    for shard in shard_partition_infos:
        for partition in shard.partitions:
            partition_to_shard_host[partition] = shard.hostname
    return {'status': 0}


@asynccontextmanager
async def lifespan(app: FastAPI):
    hostname = socket.gethostname()
    shard_info = ShardInfo(hostname=hostname, load=0.0)
    OrchestratorCommand().update_shard(shard_info)
    yield


app.router.lifespan_context = lifespan


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=SHARD_PORT)
