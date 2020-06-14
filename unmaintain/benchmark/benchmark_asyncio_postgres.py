import time
import asyncio
import aiopg
import uvloop
import asyncpg


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


dsn = 'dbname=rssant user=rssant password=rssant host=127.0.0.1'


async def run_aiopg():
    pool = await aiopg.create_pool(dsn, minsize=5, maxsize=5)
    t0 = time.time()
    for i in range(1000):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                ret = []
                async for row in cur:
                    ret.append(row)
                assert ret == [(1,)]
    print('run_aiopg', time.time() - t0)
    pool.close()
    await pool.wait_closed()


async def run_asyncpg():
    async with asyncpg.create_pool(
            user='rssant', password='rssant',
            database='rssant', host='127.0.0.1',
            command_timeout=60, min_size=5, max_size=5
    ) as pool:
        t0 = time.time()
        for i in range(1000):
            async with pool.acquire() as conn:
                values = await conn.fetch("SELECT 1")
                assert values == [(1,)]
        print('run_asyncpg', time.time() - t0)
    await pool.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(run_aiopg())

loop = asyncio.get_event_loop()
loop.run_until_complete(run_asyncpg())
