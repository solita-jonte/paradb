import json
import os
import uuid
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
import httpx

from shard.document_locking import get_partition_lock, get_document_lock
from shard.document_storage import partition_index_for_id, write_document, delete_document
from shard.partitions import partition_to_shard_url
from shard.query_engine import execute_query
from shard.url import get_host_url
from shared.file_utils import osfunc_retry


DATA_DIR = os.environ.get("DATA_DIR", "./data/")
db_router = APIRouter(prefix="/db", tags=["shard-db"])


@db_router.post("/document", status_code=201)
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
        async with httpx.AsyncClient() as http_client:
            resp = await http_client.post(
                f"{owner_url}/db/document",
                json=body,
                headers={"X-Forwarded": "true"},
            )
        return JSONResponse(status_code=resp.status_code, content=resp.json())

    try:
        rw_lock = await get_partition_lock(partition_idx)
        async with rw_lock.for_reading():
            doc_lock = get_document_lock(str(doc_id))
            async with doc_lock:
                result = await osfunc_retry(lambda: write_document(DATA_DIR, body), retries=3)
    except TimeoutError:
        return JSONResponse(status_code=500, content=dict(error="Unable to write document"))

    return JSONResponse(status_code=201, content=result)


@db_router.delete("/document/{doc_id}")
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
        async with httpx.AsyncClient() as http_client:
            resp = await http_client.delete(f"{owner_url}/db/document/{doc_id}")
        return JSONResponse(status_code=resp.status_code, content=resp.json())

    rw_lock = await get_partition_lock(partition_idx)
    async with rw_lock.for_reading():
        doc_lock = get_document_lock(doc_id)
        async with doc_lock:
            deleted = delete_document(DATA_DIR, doc_id)

    if not deleted:
        return JSONResponse(status_code=404, content={"error": "not found"})
    return JSONResponse(status_code=200, content={"status": "deleted"})


@db_router.post("/query")
async def query_documents(request: Request):
    """Query documents with MongoDB-like filter."""
    body = await request.json()
    results = execute_query(DATA_DIR, body)
    return JSONResponse(status_code=200, content=results)
