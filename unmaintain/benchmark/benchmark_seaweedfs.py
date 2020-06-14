import aiohttp
import asyncio
import os
import sys
import time
import random
import contextlib


seaweedfs_url = 'http://127.0.0.1:9081'


def random_content():
    return os.urandom(random.randint(1, 10) * 1024)


def random_fid(volumes):
    volume_id = random.choice(volumes)
    file_key = random.randint(0, 1 << 24)
    file_key_hex = '%x' % file_key
    cookie_hex = '00000000'
    return f'{volume_id},{file_key_hex}{cookie_hex}'


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


async def put(session, fid: str, content: bytes):
    url = f'{seaweedfs_url}/{fid}'
    data = aiohttp.FormData()
    data.add_field(
        'file',
        content,
        content_type='application/gzip'
    )
    async with session.put(url, data=data) as response:
        result = await response.read()
        return response.status, result


async def get(session, fid: str):
    url = f'{seaweedfs_url}/{fid}'
    async with session.get(url) as response:
        result = await response.read()
        return response.status, result


async def reader_task(session, fid_s, n):
    fid_s = list(fid_s)
    random.shuffle(fid_s)
    for fid in fid_s:
        with READER_REPORTER.report():
            status, r = await get(session, fid)
            assert status == 200, (status, r)


async def writer_task(session, fid_s, n):
    fid_s = list(fid_s)
    random.shuffle(fid_s)
    for fid in fid_s:
        content = random_content()
        with WRITER_REPORTER.report():
            status, r = await put(session, fid, content)
            assert status in (200, 201, 204), (status, r)


async def benchmark(session, num_volume, num_fid, num_round, concurrency):
    volumes = list(range(20, 20 + num_volume))
    fid_s_s = []
    for i in range(concurrency):
        fid_s = [random_fid(volumes) for _ in range(num_fid // concurrency)]
        fid_s_s.append(fid_s)
    loop = asyncio.get_event_loop()
    for n in range(num_round):
        print(f'{n} ' + '-' * 60)
        writer_tasks = []
        for i in range(concurrency):
            t = writer_task(session, fid_s_s[i], num_round)
            writer_tasks.append(loop.create_task(t))
        await asyncio.gather(*writer_tasks)
        WRITER_REPORTER.summary(concurrency)
        reader_tasks = []
        for i in range(concurrency):
            t = reader_task(session, fid_s_s[i], num_round)
            reader_tasks.append(loop.create_task(t))
        await asyncio.gather(*reader_tasks)
        READER_REPORTER.summary(concurrency)


async def async_main(num_volume, concurrency):
    print(f'num_volume={num_volume} concurrency={concurrency}')
    async with aiohttp.ClientSession() as session:
        await benchmark(
            session,
            num_fid=1000,
            num_round=3,
            num_volume=num_volume,
            concurrency=concurrency,
        )


def main():
    num_volume = int(sys.argv[1])
    concurrency = int(sys.argv[2])
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_main(num_volume, concurrency))


if __name__ == "__main__":
    main()
