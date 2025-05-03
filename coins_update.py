# coins_update.py
# https://futures.mexc.com/api/v1/contract/deals/BTC_USDT
# https://futures.mexc.com/api/v1/contract/depth_step/BTC_USDT?step=0.1
# https://futures.mexc.com/api/v1/contract/kline/BTC_USDT?end=1737877398&interval=Min15&start=1736977398
# https://futures.mexc.com/api/v1/contract/funding_rate/BTC_USDT
# https://www.mexc.com/api/activity/contract/stimulate_config/profits?lang=ru-RU&type=INCOME
# https://www.mexc.com/api/platform/spot/market-v2/web/symbols
# https://www.mexc.com/api/platform/spot/market-v2/web/hidden/symbols

import asyncio
import aiohttp
from aiohttp_socks import ProxyType, ProxyConnector, ChainProxyConnector
import sqlite3
import time
from collections import deque
import datetime

from database_tools import (sql_put, sql_get, sql_execute_values)

task_status = 0
indata_max_size = 0
threadpool_indata = deque()
threadpool_outdata = deque()
proxies = deque()

def reduce_value(value):
    """
    Represent big values in short terms. Convert numeric to str
    """
    if value < 1000:
        return str(value)
    elif value < 1_000_000:
        return f"{value / 1000:.1f}K"
    elif value < 1_000_000_000:
        return f"{value / 1_000_000:.1f}M"
    else:
        return f"{value / 1_000_000_000:.1f}B"

def parse_contract_details(coin_name: str, details: list):
    """
    Parameters:
    -----------
    details: list[dict, dict, ...]
        isNew: bool
        maxLeverage: int
        limitMaxVol: int
    Returns:
    --------
    contract_data: dict
    """
    match_contract = [x for x in details if x['symbol'] == coin_name]
    if match_contract:
        contract_data = {
            "isNew": match_contract[0]['isNew'],
            "maxLeverage": match_contract[0]['maxLeverage'],
            "limitMaxVol": match_contract[0]['limitMaxVol'],
            "contractSize": match_contract[0]['contractSize'],
        }
    else:
        contract_data = {
            "isNew": False,
            "maxLeverage": "N/A",
            "limitMaxVol": "N/A",
            "contractSize": "N/A",
        }
    return contract_data

def parse_ticker_details(coin_name: str, details: list):
    """
    Parameters:
    -----------
    details: list[dict, dict, ...]
        amount24: float

    Returns:
    --------
    ticker_data: dict
    """
    match_ticker = [x for x in details if x['symbol'] == coin_name]
    # print("match_ticker", match_ticker)
    try:
    # if match_ticker:
        ticker_data = {
            "amount24": match_ticker[0]['amount24'],
            "lastPrice": match_ticker[0]['lastPrice'],
            "fundingRate": match_ticker[0]['fundingRate'],
        }
    except:
        ticker_data = {
            "amount24": "N/A",
            "lastPrice": "N/A",
            "fundingRate": 'N/A'
        }
    return ticker_data

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
    proxies.append('socks5://UrYitU5Z6:buuQKE5gv@142.252.171.241:62403')
    proxies.append('socks5://AsyKEKUp2:XiD4C1WhN@142.252.132.205:63145')
    proxies.append('socks5://wdiVwFp9M:a2NHWzs4x@185.128.42.8:62689')
    proxies.append('socks5://RmWEhzw7A:PP6Zudupc@212.193.136.67:64845')

    proxies.append('http://jyaMmjyRF:SfYP48GZH@213.139.192.12:64568')  # DO NOT SHARE
    proxies.append('http://nLudUShiL:D3CYJT1rd@91.247.78.46:63504')  # DO NOT SHARE
    proxies.append('http://K3Wz1u33U:xpPQteSUM@46.3.5.35:63292')  # DO NOT SHARE
    proxies.append('http://2THcCbihU:p2R3ucwbZ@46.232.34.78:64532')  # DO NOT SHARE
    proxies.append('http://A7TH9auxY:pD7vqegUg@46.232.42.35:64454')  # DO NOT SHARE
    proxies.append('http://cf4e6QvwS:y2B9qsvBt@45.192.46.129:63088')  # DO NOT SHARE

async def one_thread(indata, proxy):
    outdata = {'coin': indata['coin'], 'contract_data': indata['contract_data'], 'ticker_data': indata['ticker_data']}
    res = await aiohttp_get(indata['url'], proxy, 2.5)
    if res['status'] == 200:
        outdata.update({'data': res['response']['data']})
    result = {'proxy': proxy, 'status': res['status'], 'indata': indata, 'outdata': outdata}
    asyncio.create_task(worker_result(result))

async def start_workers(thread_per_proxy):
    print('start_workers')
    global proxies, indata_max_size, threadpool_indata, threadpool_outdata, task_status
    sql_put('DELETE FROM futures WHERE time<%s', (time.time() - 60 * 30,))
    # sql_put('DELETE FROM futures WHERE time<?', (time.time() - 1000,))
    await update_proxies()
    proxy = proxies.popleft()
    result = await aiohttp_get('https://www.mexc.com/api/platform/spot/market-v2/web/symbols', proxy, 3.0)
    # get contract details
    result_contract = await aiohttp_get('https://futures.mexc.com/api/v1/contract/detail', proxy, 3.0)
    # get ticker details
    results_ticker = await aiohttp_get('https://contract.mexc.com/api/v1/contract/ticker', proxy, 3.0)
    proxies.append(proxy)
    if result['status'] != 200 or result_contract['status'] != 200 or results_ticker['status'] != 200:
        task_status = 0
        return
    threadpool_indata.clear()
    threadpool_outdata.clear()
    bad_coins = sql_get('SELECT * FROM bad_coins', ())
    bad_coins = set([i[0] for i in bad_coins])
    for i in result['response']['data']['USDT']:
        coin = i['vn'] + '_USDT'
        if not coin in bad_coins:
            task = {'url': f'https://futures.mexc.com/api/v1/contract/kline/{coin}?&interval=Min1&start=&start={int(time.time() - 200)}'}
            # update task with contract data
            contract_data = parse_contract_details(coin, result_contract['response']['data'])
            ticker_data = parse_ticker_details(coin, results_ticker['response']['data'])
            task.update({'coin': coin, 'contract_data': contract_data, 'ticker_data': ticker_data})
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
        try:  # Replaces "N/A" value checkup
            max_leverage_enter_amt = reduce_value(round(
                item['contract_data']['limitMaxVol'] * item['ticker_data']['lastPrice'] *
                item['contract_data']['contractSize'] / item['contract_data']['maxLeverage'], 1))
            amount24 = reduce_value(item['ticker_data']['amount24'])
            fundingRate = str(round(item['ticker_data']['fundingRate']*100, 4))
               
        except Exception as e:
            max_leverage_enter_amt = "N/A"
            amount24 = "N/A"
            fundingRate = "N/A"

        # ADD METADATA TO COIN
        sql_data = [(item['coin'],
                    item['contract_data']['isNew'],
                    str(item['contract_data']['maxLeverage']),
                    max_leverage_enter_amt,
                    amount24,
                    fundingRate
                    )]

        sql_request = """
                    INSERT INTO coins (coin, isNew, maxLeverage, limitMaxVol, amount24, fundingRate)
                    VALUES %s
                    ON CONFLICT (coin) DO UPDATE SET
                        isNew = EXCLUDED.isNew,
                        maxLeverage = EXCLUDED.maxLeverage,
                        limitMaxVol = EXCLUDED.limitMaxVol,
                        amount24 = EXCLUDED.amount24,
                        fundingRate = EXCLUDED.fundingRate
                """

        # sql_request = 'INSERT INTO coins (coin, isNew, maxLeverage, limitMaxVol, amount24, fundingRate) VALUES %s ON CONFLICT DO NOTHING'
        sql_execute_values(sql_request, sql_data)


        # sql_put(
        #     'INSERT OR REPLACE INTO coins (coin, isNew, maxLeverage, limitMaxVol, amount24, fundingRate) VALUES (?,?,?,?,?,?)',
        #     sql_data)
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
        sql_request = 'INSERT INTO futures (coin, time, realOpen, realClose, realHigh, realLow) VALUES %s ON CONFLICT DO NOTHING'
        sql_execute_values(sql_request, sql_data)

        # sql_request = 'INSERT OR IGNORE INTO futures (coin_time, coin, time, mean) VALUES (?,?,?,?)'
        # sql_executemany(sql_request, sql_data)
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
    #print('outdata', len(threadpool_outdata), 'indata', len(threadpool_indata), 'proxies', len(proxies), 'indata_max_size', indata_max_size)
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

# многопоточное обновление курса
async def coins_update():
    global task_status
    while True:
        print(f"coins_update task_status: {task_status} | indata: {len(threadpool_indata)} | outdata: {len(threadpool_outdata)} | indata_max_size: {indata_max_size}")
        if task_status == 0:
            task_status = 1
            await start_workers(10)
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(coins_update())
