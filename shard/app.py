#!/usr/bin/env python3

from fastapi import FastAPI

from shard.lifespan import lifespan
from shard.routers.db import db_router
from shard.routers.internal import internal_router


app = FastAPI(lifespan=lifespan)
app.include_router(db_router)
app.include_router(internal_router)
