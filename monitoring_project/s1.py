import asyncio
import aiohttp
from aiohttp_socks import ProxyConnector
import time
from collections import deque

import psycopg2
from psycopg2.extras import execute_values

task_status = 0
indata_max_size = 0
threadpool_indata = deque()
threadpool_outdata = deque()
proxies = deque()
TIMEDELTA = 60 * 60 * 4  # 4 hours

POSTGRESQL_DB_NAME = 'db_name'
POSTGRESQL_USER = 'postgres'
POSTGRESQL_PASSWORD = 'pass'
POSTGRESQL_HOST = 'localhost'

def sql_put(sqlite_request, data_tuple):
    try:
        connection = psycopg2.connect(dbname=POSTGRESQL_DB_NAME, user=POSTGRESQL_USER, password=POSTGRESQL_PASSWORD, host=POSTGRESQL_HOST, connect_timeout=5)
        cursor = connection.cursor()
        cursor.execute(sqlite_request, data_tuple)
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as error:
        print(error)

def sql_get(sqlite_request, data_tuple):
    results = []
    try:
        connection = psycopg2.connect(dbname=POSTGRESQL_DB_NAME, user=POSTGRESQL_USER, password=POSTGRESQL_PASSWORD, host=POSTGRESQL_HOST, connect_timeout=5)
        cursor = connection.cursor()
        cursor.execute(sqlite_request, data_tuple)
        results = cursor.fetchall()
        cursor.close()
        connection.close()
    except Exception as error:
        print(error)
    return results

def sql_execute_values(sql_request, data):
    try:
        connection = psycopg2.connect(dbname=POSTGRESQL_DB_NAME, user=POSTGRESQL_USER, password=POSTGRESQL_PASSWORD, host=POSTGRESQL_HOST, connect_timeout=5)
        cursor = connection.cursor()
        execute_values(cursor, sql_request, data)
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as error:
        print(error)

async def aiohttp_get(url, proxy, time_request):
    connector = ProxyConnector.from_url(proxy)
    headers = {
        'Accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,es;q=0.8',
        'Connection': 'keep-alive',
        'DNT': '1',
        'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Mobile/9A5313e',
    }
    connector = ProxyConnector.from_url(proxy)
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        start_time = time.time()
        try:
            async with session.get(url=url, timeout=5) as response:
                status = response.status
                response = await response.json()
                if response.get('code') == 510:
                    status = 510
                elif response.get('code') == 1001:
                    status = 1001
        except Exception as error:
            status = 0
            response = None
    if time.time() - start_time < time_request:
        await asyncio.sleep(time_request - time.time() + start_time)
    return {'status': status, 'response': response}


async def update_proxies():
    global proxies
    proxies.clear()
    proxies.append('')
    proxies.append('')
    proxies.append('')
    proxies.append('')

# NEED
async def one_thread(indata, proxy):
    outdata = {'coin': indata['coin']}
    res = await aiohttp_get(indata['url'], proxy, 2.5)
    if res['status'] == 200:
        outdata.update({'data': res['response']['data']})
    result = {'proxy': proxy, 'status': res['status'], 'indata': indata, 'outdata': outdata}
    asyncio.create_task(worker_result(result))

# FIRST
async def start_workers(thread_per_proxy):
    global proxies, indata_max_size, threadpool_indata, threadpool_outdata, task_status
    now = time.time()
    cur = int(now - now % TIMEDELTA)
    start = cur - TIMEDELTA * 60

    print('start_workers')
    sql_put('DELETE FROM fourh_futures WHERE time<%s', (start,))
    # sql_put('DELETE FROM futures WHERE time<?', (time.time() - 1000,))
    await update_proxies()
    proxy = proxies.popleft()
    result = await aiohttp_get('https://www.mexc.com/api/platform/spot/market-v2/web/symbols', proxy, 3.0)
    proxies.append(proxy)
    if result['status'] != 200:
        task_status = 0
        return

    threadpool_indata.clear()
    threadpool_outdata.clear()

    bad_coins = sql_get('SELECT * FROM bad_coins', ())
    bad_coins = set([i[0] for i in bad_coins])
    for i in result['response']['data']['USDT']:
        coin = i['vn'] + '_USDT'
        if not coin in bad_coins:
            # TODO ADD ONLY NEW VALUES
            task = {
                'url': f'https://futures.mexc.com/api/v1/contract/kline/{coin}?&interval=Hour4&start={start}'}
            task.update({'coin': coin})
            threadpool_indata.append(task)
    indata_max_size = len(threadpool_indata)
    while len(proxies) > 0 and len(threadpool_indata) > 0:
        tmp_proxy = proxies.popleft()
        for i in range(thread_per_proxy):
            asyncio.create_task(one_thread(threadpool_indata.popleft(), tmp_proxy))
            if len(threadpool_indata) == 0:
                break

async def start_workers(thread_per_proxy):
    global proxies, indata_max_size, threadpool_indata, threadpool_outdata, task_status
    now = time.time()
    cur = int(now - now % TIMEDELTA)
    start = cur - TIMEDELTA * 60

    print('start_workers')
    sql_put('DELETE FROM fourh_futures WHERE time<%s', (start,))
    # sql_put('DELETE FROM futures WHERE time<?', (time.time() - 1000,))
    await update_proxies()
    proxy = proxies.popleft()
    result = await aiohttp_get('https://www.mexc.com/api/platform/spot/market-v2/web/symbols', proxy, 3.0)
    proxies.append(proxy)
    if result['status'] != 200:
        task_status = 0
        return

    threadpool_indata.clear()
    threadpool_outdata.clear()

    bad_coins = sql_get('SELECT * FROM bad_coins', ())
    bad_coins = set([i[0] for i in bad_coins])
    for i in result['response']['data']['USDT']:
        coin = i['vn'] + '_USDT'
        if not coin in bad_coins:
            task = {
                'url': f'https://futures.mexc.com/api/v1/contract/kline/{coin}?&interval=Hour4&start={start}'
            }
            task.update({'coin': coin})
            threadpool_indata.append(task)
    indata_max_size = len(threadpool_indata)
    while len(proxies) > 0 and len(threadpool_indata) > 0:
        tmp_proxy = proxies.popleft()
        for i in range(thread_per_proxy):
            asyncio.create_task(one_thread(threadpool_indata.popleft(), tmp_proxy))
            if len(threadpool_indata) == 0:
                break

async def worker_result(result):
    global proxies, indata_max_size, threadpool_indata, threadpool_outdata, task_status
    # есть монета
    if result['status'] == 200:
        item = result['outdata']
        sql_data = []
        for k in range(len(item['data']['time'])):
            sql_data.append((
                item['coin'],
                item['data']['time'][k],
                item['data']['realOpen'][k],
                item['data']['realClose'][k],
                item['data']['realHigh'][k],
                item['data']['realLow'][k],
            ))
        sql_request = 'INSERT INTO fourh_futures (coin, time, realOpen, realClose, realHigh, realLow) VALUES %s ON CONFLICT DO NOTHING'
        sql_execute_values(sql_request, sql_data)
        threadpool_outdata.append(result['outdata'])
    # ошибка сети
    elif result['status'] == 0 or result['status'] == 510:
        threadpool_indata.append(result['indata'])
    # нет монеты
    elif result['status'] == 1001:
        sql_put('INSERT INTO bad_coins VALUES (%s) ON CONFLICT DO NOTHING', (result['outdata']['coin'],))
        # sql_put('INSERT OR IGNORE INTO bad_coins VALUES (?)', (result['outdata']['coin'], ))
        threadpool_outdata.append(result['outdata'])
    # другое
    else:
        threadpool_outdata.append(result['outdata'])
    proxies.append(result['proxy'])
    # print('outdata', len(threadpool_outdata), 'indata', len(threadpool_indata), 'proxies', len(proxies), 'indata_max_size', indata_max_size)
    if len(threadpool_indata) > 0 and len(proxies) > 0:
        asyncio.create_task(one_thread(threadpool_indata.popleft(), proxies.popleft()))
    elif len(threadpool_outdata) == indata_max_size:
        asyncio.create_task(end_workers())

async def end_workers():
    global proxies, indata_max_size, threadpool_indata, threadpool_outdata, task_status
    threadpool_indata.clear()
    threadpool_outdata.clear()
    indata_max_size = 0
    task_status = 0

# INIT
# многопоточное обновление курса
async def coins_update():
    global task_status
    while True:
        print(
            f"coins_update task_status: {task_status} |"
            f" indata: {len(threadpool_indata)} |"
            f" outdata: {len(threadpool_outdata)} |"
            f" indata_max_size: {indata_max_size}")
        if task_status == 0:
            task_status = 1
            await start_workers(10)
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(coins_update())
