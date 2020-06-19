import time
import asyncio
import asyncpg
import os
import contextlib
import random
import sys


async def init_table(connection: asyncpg.Connection):
    query = """
    CREATE TABLE IF NOT EXISTS STORY (
        id BIGINT PRIMARY KEY,
        content bytea NOT NULL
    );
    """
    await connection.execute(query)
    query = "DELETE FROM STORY;"
    await connection.execute(query)


def random_content():
    return os.urandom(random.randint(1, 10) * 1024)


def random_fid():
    return random.randint(0, 1 << 24)


class Reporter:
    def __init__(self):
        self.items = []

    @contextlib.contextmanager
    def report(self):
        t0 = time.monotonic()
        yield
        value = time.monotonic() - t0
        self.items.append(value * 1000)

    def summary(self, concurrency):
        n = len(self.items)
        s = sum(self.items)
        avg = s / n if n > 0 else 0
        s_items = list(sorted(self.items))
        result = [f'avg={avg:.1f}']
        p_s = [0.5, 0.8, 0.9, 0.95, 0.99]
        if n > 0:
            for p in p_s:
                v = s_items[int(n * p)]
                result.append('p{}={:.1f}'.format(int(p * 100), v))
        qps = (1000 / avg) * concurrency
        result.append(f'qps={qps:.0f}')
        print(' '.join(result))
        self.items = []


READER_REPORTER = Reporter()
WRITER_REPORTER = Reporter()


async def put(pool: asyncpg.pool.Pool, fid: int, content: bytes):
    sql = """
    insert into story (id, content) values ($1, $2)
    ON CONFLICT (id) DO UPDATE SET content = excluded.content
    """
    async with pool.acquire() as conn:
        await conn.execute(sql, fid, content)


async def get(pool: asyncpg.pool.Pool, fid: int):
    sql = "SELECT * FROM story where id = $1"
    async with pool.acquire() as conn:
        _, content = await conn.fetchrow(sql, fid)
        assert content


async def reader_task(pool: asyncpg.pool.Pool, fid_s, n):
    fid_s = list(fid_s)
    random.shuffle(fid_s)
    for fid in fid_s:
        with READER_REPORTER.report():
            await get(pool, fid)


async def writer_task(pool: asyncpg.pool.Pool, fid_s, n):
    fid_s = list(fid_s)
    random.shuffle(fid_s)
    for fid in fid_s:
        content = random_content()
        with WRITER_REPORTER.report():
            await put(pool, fid, content)


async def benchmark(pool: asyncpg.pool.Pool, num_fid, num_round, concurrency):
    fid_s_s = []
    for i in range(concurrency):
        fid_s = [random_fid() for _ in range(num_fid // concurrency)]
        fid_s_s.append(fid_s)
    loop = asyncio.get_event_loop()
    for n in range(num_round):
        print(f'{n} ' + '-' * 60)
        writer_tasks = []
        for i in range(concurrency):
            t = writer_task(pool, fid_s_s[i], num_round)
            writer_tasks.append(loop.create_task(t))
        await asyncio.gather(*writer_tasks)
        WRITER_REPORTER.summary(concurrency)
        reader_tasks = []
        for i in range(concurrency):
            t = reader_task(pool, fid_s_s[i], num_round)
            reader_tasks.append(loop.create_task(t))
        await asyncio.gather(*reader_tasks)
        READER_REPORTER.summary(concurrency)


async def async_main(concurrency):
    print(f'concurrency={concurrency}')
    async with asyncpg.create_pool(
            user='rssant', password='rssant',
            database='rssant', host='127.0.0.1',
            command_timeout=60, min_size=5, max_size=5
    ) as pool:
        async with pool.acquire() as conn:
            await init_table(conn)
        await benchmark(
            pool,
            num_fid=1000,
            num_round=3,
            concurrency=concurrency,
        )
    await pool.close()


def main():
    concurrency = int(sys.argv[1])
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_main(concurrency))


if __name__ == "__main__":
    main()
