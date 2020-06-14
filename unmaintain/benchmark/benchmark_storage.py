import requests
import time
import asyncio
import os
import random
import sqlite3
from rssant_common.helper import aiohttp_client_session
import rssant_common.django_setup
from rssant_common.unionid import decode
from concurrent.futures import ThreadPoolExecutor
from rssant_api.models.story_sharding import seaweed_fid_for

url = 'http://127.0.0.1:9080'

data = {"storys": [{"feed_id": "02404h", "offset": 10, "limit": 1}, {"feed_id": "022m", "offset": 10, "limit": 1}, {"feed_id": "02404v", "offset": 261, "limit": 1}, {"feed_id": "0234", "offset": 10, "limit": 1}, {"feed_id": "021k", "offset": 10, "limit": 1}, {"feed_id": "02404y", "offset": 29, "limit": 1}, {"feed_id": "020w", "offset": 4, "limit": 1}, {"feed_id": "0224", "offset": 6, "limit": 1}, {"feed_id": "02404t", "offset": 4, "limit": 1}, {"feed_id": "024045", "offset": 298, "limit": 1}, {"feed_id": "02404r", "offset": 9, "limit": 1}, {"feed_id": "0235", "offset": 21, "limit": 1}, {"feed_id": "020e", "offset": 20, "limit": 1}, {
    "feed_id": "020q", "offset": 15, "limit": 1}, {"feed_id": "0201", "offset": 13, "limit": 1}, {"feed_id": "02404w", "offset": 19, "limit": 1}, {"feed_id": "0203", "offset": 31, "limit": 1}, {"feed_id": "022v", "offset": 10, "limit": 1}, {"feed_id": "024044", "offset": 124, "limit": 1}, {"feed_id": "023r", "offset": 119, "limit": 1}, {"feed_id": "022g", "offset": 11, "limit": 1}, {"feed_id": "021h", "offset": 4, "limit": 1}, {"feed_id": "020c", "offset": 14, "limit": 1}, {"feed_id": "0206", "offset": 7, "limit": 1}, {"feed_id": "020t", "offset": 9, "limit": 1}, {"feed_id": "0222", "offset": 29, "limit": 1}, {"feed_id": "02404g", "offset": 9, "limit": 1}]}

fid_s = []
for s in data['storys']:
    feed_id = decode(s['feed_id'])[1]
    fid_s.append(seaweed_fid_for(feed_id, s['offset'], 1))

feed_id = decode('02404d')[1]
for offset in range(131, 131 + 15):
    fid_s.append(seaweed_fid_for(feed_id, offset, 1))

fid_s = random.sample(fid_s * 10, 30)

session = requests.Session()
pool = ThreadPoolExecutor(10)


def task(fid):
    t0 = time.time()
    r = session.get(url + f'/{fid}')
    r.content
    cost = time.time() - t0
    return fid, r.status_code, cost


def main():
    for i in range(10):
        cost_s = list(sorted(pool.map(task, fid_s), key=lambda x: x[2]))
        for fid, status, cost in cost_s:
            cost_ms = int(cost * 1000)
            print(status, cost_ms, fid)
        print('-' * 60)


async def async_main():
    sess = aiohttp_client_session()

    async def _task(fid):
        t0 = time.time()
        async with sess.get(url + f'/{fid}') as res:
            await res.read()
        cost = time.time() - t0
        return fid, res.status, cost
    for i in range(10):
        cost_s = await asyncio.gather(*[_task(fid) for fid in fid_s])
        cost_s = list(sorted(cost_s, key=lambda x: x[2]))
        for fid, status, cost in cost_s:
            cost_ms = int(cost * 1000)
            print(status, cost_ms, fid)
        print('*' * 60)
    await sess.close()


def sqlite_init(filepath):
    sql = '''
    create table if not exists story (
        feed_id INTEGER NOT NULL,
        offset INTEGER NOT NULL,
        header BLOB,
        PRIMARY KEY (feed_id, offset)
    );
    '''
    conn = sqlite3.connect(filepath)
    cursor = conn.cursor()
    cursor.execute(sql)

    def storys_gen():
        for feed_id in range(10000, 20000):
            for offset in range(random.randint(200, 490), 500):
                data = os.urandom(random.randint(500, 1000))
                yield feed_id, offset, data

    sql_insert = '''
    insert into story (feed_id, offset, header) values (?, ?, ?);
    '''
    cursor.executemany(sql_insert, storys_gen())
    conn.commit()
    conn.close()


def sqlite_main(filepath_s):
    conn_cursor_s = []
    sql = '''
    select * from story where feed_id=? and offset=?
    '''
    t0 = time.time()
    for filepath in filepath_s:
        conn = sqlite3.connect(filepath)
        cursor = conn.cursor()
        conn_cursor_s.append((conn, cursor))
    print('open         ', (time.time() - t0) * 1000)
    for i in range(100):
        t0 = time.time()
        for conn, cursor in conn_cursor_s:
            cursor.execute(sql, [random.randint(10000, 20000), random.randint(480, 500)])
            cursor.fetchall()
        print(f'scan-{i}        ', (time.time() - t0) * 1000)


if __name__ == "__main__":
    filepath_s = [f'examples/{i}.db' for i in range(10)]
    # for filepath in filepath_s:
    #     print('init', filepath)
    #     sqlite_init(filepath)
    sqlite_main(filepath_s)
    # main()
    # asyncio.get_event_loop().run_until_complete(async_main())
