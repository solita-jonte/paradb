#!/usr/bin/env python3

from fastapi import FastAPI

from orchestrator.lifespan import lifespan
from orchestrator.shard_router import shard_router


app = FastAPI(lifespan=lifespan)
app.include_router(shard_router)
