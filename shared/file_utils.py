import asyncio
import os


async def file_replace_retry(src: str, dst: str, retries: int = 5):
    for loop in range(retries):
        try:
            os.replace(src, dst)
            break
        except PermissionError as ex:
            if loop >= retries - 1:
                raise ex
            await asyncio.sleep(0.001)


async def osfunc_retry(func, retries: int = 3):
    for loop in range(retries):
        try:
            return await func()
        except OSError as ex:
            if loop >= retries - 1:
                raise ex
            await asyncio.sleep(0.001)
