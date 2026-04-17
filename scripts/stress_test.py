#!/usr/bin/env python3
"""Minimal ParaDB stress test."""

import argparse, asyncio, random, time, uuid, httpx, random


doc_ids: list[str] = []


async def writer(client, url, stop):
    wrote = 0
    while time.monotonic() < stop:
        doc_id = random.choice(doc_ids)
        doc = {"_id": doc_id, "v": random.randint(0, 1_000_000)}
        r = await client.post(f"{url}/db/document", json=doc)
        r.raise_for_status()
        wrote += 1
    return wrote

async def reader(client, url, stop):
    read = 0
    while time.monotonic() < stop:
        r = await client.post(f"{url}/db/query", json={"v": {"$gt": 500_000}})
        r.raise_for_status()
        read += len(r.json())
    return read

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://localhost:3357")
    p.add_argument("--writers", type=int, default=10)
    p.add_argument("--readers", type=int, default=5)
    p.add_argument("--duration", type=int, default=30, help="seconds")
    p.add_argument("--docs", type=int, default=10, help="number of documents")
    args = p.parse_args()

    doc_ids.extend([str(uuid.uuid4()) for _ in range(args.docs)])

    stop = time.monotonic() + args.duration
    async with httpx.AsyncClient(timeout=30) as c:
        tasks  = [writer(c, args.url, stop) for _ in range(args.writers)]
        tasks += [reader(c, args.url, stop) for _ in range(args.readers)]
        results = await asyncio.gather(*tasks)

    writes = sum(results[:args.writers])
    reads  = sum(results[args.writers:])
    print(f"Done in {args.duration}s — {writes} writes, {reads} docs read")


if __name__ == "__main__":
    asyncio.run(main())
