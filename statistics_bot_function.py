import os
import asyncio
import time
import threading
from asgiref.sync import sync_to_async
import pandas as pd
import re

from database_tools import (sql_put, sql_get, sql_execute_values)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'monitoring_project.settings')

import django
django.setup()

from monitoring_app.models import TradingPair, Event, TradingPairState

price_thr = 7.0                    # Порог изменения цены в процентах
end_price_thr = price_thr * 0.85   # Порог изменения цены для закрытия пампа (для статистики)

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

# Функция для экранирования символов MarkdownV2
def escape_md(text: str) -> str:
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

@sync_to_async
def get_or_create_trading_pair(symbol):
    pair, created = TradingPair.objects.get_or_create(symbol=symbol, defaults={'exchange': 'MEXC'})
    if created:
        print(f"[DB] Создана новая торговая пара: {symbol}")
    return pair

@sync_to_async
def record_event(pair, event_type, event_price, description=""):
    obj = Event.objects.create(
        trading_pair=pair,
        event_type=event_type,
        event_time=time.time(),
        event_price=event_price,
        description=description
    )
    print(f"[DB] Записано событие: {event_type} для {pair.symbol} по цене {event_price} с описанием: {description}")
    return obj

@sync_to_async
def get_pump_starts(pair):
    """Список всех pump_start-событий для пары."""
    return list(Event.objects.filter(trading_pair=pair, event_type="pump_start").order_by('event_time'))

@sync_to_async
def get_pump_ends(pair):
    """Список всех pump_end-событий для пары."""
    return list(Event.objects.filter(trading_pair=pair, event_type="pump_end").order_by('event_time'))


async def get_pump_stats(symbol: str, cur_price: float):
    """
    Подсчет статистики завершённых отработок (pump_start → pump_end) за 15 минут, 3 часа, сутки,
    а также время с момента последнего pump_end.

    # TODO: Integrate and merge DB'S metadata
    """
    now = time.time()
    fifteen_minutes_ago = 15 * 60
    three_hours_ago = 3 * 3600
    one_day_ago = 24 * 3600

    pair = await get_or_create_trading_pair(symbol)
    pump_starts = await get_pump_starts(pair)
    pump_ends = await get_pump_ends(pair)

    stats = {
        "fifteen_minutes": 0,
        "three_hours": 0,
        "one_day": 0,
        "more_one_day": 0,

        "profit_deals_cnt": 0,   # int: кол-во сделок в плюс
        "loss_deals_cnt": 0,     # int: кол-во сделок в минус
        "profit_percentage": 0,  # float: накопление профита в % чистого движения
        "loss_percentage": 0,    # float: накопление лосса в % чистого движения
        "in_deal_cnt": 0,        # int: кол-во сделок в отработке
        "in_deal_percentage": 0, # int: баланс сделки в отработке
    }

    for pstart in pump_starts:
        is_positive_deal = True
        diff_percent = 0
        next_pend = None
        non_closed = False
        loss_day_closed_price = 0
        for pend in pump_ends:
            if pend.event_time > pstart.event_time:
                # check if price lower thr
                if pend.event_price <= pstart.event_price * (1 - (end_price_thr / 100)):
                    next_pend = pend
                    break
                # check if price higher thr and got more one day
                elif now - pend.event_time >= one_day_ago:
                    loss_day_closed_price = pend.event_price
                    break
        # got win deal or got more one day loss deal
        if next_pend:
            # deals emulation stats
            diff_percent = round((next_pend.event_price - pstart.event_price) / pstart.event_price * 100, 1)
            # timing stats
            if next_pend.event_time - pstart.event_time >= one_day_ago:
                stats["more_one_day"] += 1
                is_positive_deal = False
            elif next_pend.event_time - pstart.event_time >= three_hours_ago:
                stats["one_day"] += 1
            elif next_pend.event_time - pstart.event_time >= fifteen_minutes_ago:
                stats["three_hours"] += 1
            elif next_pend.event_time - pstart.event_time < fifteen_minutes_ago:
                stats["fifteen_minutes"] += 1
        # got current deal or current deal lasts more one day
        else:
            #  loss deal (lasts more one day and closed in minus for damp price)
            if loss_day_closed_price:
                is_positive_deal = False
                diff_percent = round((loss_day_closed_price - pstart.event_price) / pstart.event_price * 100, 1)
                stats["more_one_day"] += 1
            # deal lasts less one day and not closed
            else:
                non_closed = True
                # calculate difference of current price
                diff_percent = round((cur_price - pstart.event_price) / pstart.event_price * 100, 1)
                # adjust relevant value of percentage
                stats["in_deal_cnt"] += 1
                # pump loss deal
        if non_closed:
            stats["in_deal_percentage"] += diff_percent
        # profit deal
        elif is_positive_deal:
            stats['profit_deals_cnt'] += 1
            stats['profit_percentage'] += abs(diff_percent)
        # loss deal (lasts more one)
        elif not is_positive_deal:
            stats['loss_deals_cnt'] += 1
            stats['loss_percentage'] += abs(diff_percent)
    return stats

async def scheduler(): # 1 итерация проверки

    print("[scheduler] Начинаем проверку...")
    # fetch coins along with meta-data
    coins_rows = sql_get('SELECT coin, isNew, maxLeverage, limitMaxVol, amount24, fundingRate FROM coins', ())
    # coins_rows = sql_get('SELECT coin, isNew, maxLeverage, limitMaxVol, amount24, fundingRate FROM coins')
    coins = [r[0] for r in coins_rows]
    # vip_rows = sql_get('SELECT coin FROM vip_coins', ())
    # vip_coins = {r[0] for r in vip_rows}

    for ci, coin in enumerate(coins):

        futures_rows = sql_get(
            'SELECT time,realClose FROM futures WHERE coin=%s ORDER BY time',
            (coin,)
        )
        graph_rows = sql_get(
            'SELECT time,realOpen,realClose,realHigh,realLow FROM fourh_futures WHERE coin=%s ORDER BY time',
            (coin,)
        )
        # futures_rows = sql_get('SELECT time, mean FROM futures WHERE coin=? ORDER BY time', (coin,))
        if not futures_rows:
            continue

        futures_times = [row[0] for row in futures_rows]

        cp = sql_get('SELECT time FROM checkpoints WHERE coin=%s', (coin, ))
        # cp = sql_get('SELECT time FROM checkpoints WHERE coin=?', (coin,))
        if cp and cp[0][0] in futures_times:
            start_index = futures_times.index(cp[0][0])
        else:
            start_index = 0

        current_futures = futures_rows[start_index:]
        if not current_futures:
            print(f"[scheduler] Нет новых данных для {coin} после чекпоинта. Пропуск.")
            continue

        found_event = False
        for i in range(len(current_futures) - 1, -1, -1):
            tmp_interval = current_futures[i:]
            if len(tmp_interval) < 2:
                continue

            price_start = float(tmp_interval[0][1])
            price_end = float(tmp_interval[-1][1])
            diff_percent = (price_end - price_start) / price_start * 100.0
            # print("diff_percent", diff_percent)
            if abs(diff_percent) >= price_thr:
                """ - - - """
                df = pd.DataFrame(graph_rows)
                df.columns = ['time', 'realOpen', 'realClose', 'realHigh', 'realLow']
                df['dt'] = pd.to_datetime(df['time'], unit='s', utc=True).dt.tz_convert(tz='Europe/Moscow')
                """ - - - """
                # Not enough data for hour timeframe
                if df.shape[0] < 2:
                    continue
                found_event = True

                # image = get_image(coin, df)
                # rsi = escape_md(f"{get_rsi(df)}%")

                if diff_percent > 0:
                    event_type = "pump_start"
                    direction_label = "🟢 Pump"
                else:
                    event_type = "pump_end"
                    direction_label = "🔻 Dump"

                pair = await get_or_create_trading_pair(coin)
                description = f"{direction_label}: изменение цены на {diff_percent:.2f}%"
                await record_event(pair, event_type, price_end, description)

                stats = await get_pump_stats(coin, price_end)

                try:
                    winrate_stat = round(
                        stats['profit_deals_cnt'] / (stats['profit_deals_cnt'] + stats['loss_deals_cnt']) * 100, 1
                    )
                except:
                    winrate_stat = 0.0
                value_profit_text = f"{reduce_value(round(coins_rows[ci][2] * 10 * stats['profit_percentage'] / 100, 1))}$"
                value_loss_text = f"{reduce_value(round(coins_rows[ci][2] * 10 * stats['loss_percentage'] / 100, 1))}$"
                in_deal_balance_text = f"{reduce_value(round(coins_rows[ci][2] * 10 * abs(stats['in_deal_percentage']) / 100, 1))}$"
                sign = ''
                if stats['in_deal_percentage'] < 0:
                    sign = '-'
                perc_text = (
                    f"Профит: x{round(stats['profit_percentage'] / 100, 1)} ({value_profit_text})\n"
                    f"Лосс: -x{round(stats['loss_percentage'] / 100, 1)} ({value_loss_text})\n"
                    f"Баланс в отработке: {sign}x{round(abs(stats['in_deal_percentage']) / 100, 1)} ({sign}{in_deal_balance_text})"
                )
                deals_stats = escape_md(f"Винрейт: {winrate_stat}\n{perc_text}")

                stats_text = (
                    f"За 15 мин: {stats['fifteen_minutes']}\n"
                    f"За 3 часа: {stats['three_hours']}\n"
                    f"За сутки: {stats['one_day']}\n"
                    f"В отработке: {stats['in_deal_cnt']}\n"
                    f"Без отработки: {stats['more_one_day']}\n\n"
                    f"{deals_stats}"
                )

                end_time = tmp_interval[-1][0]
                if cp:
                    sql_put('UPDATE checkpoints SET time=%s WHERE coin=%s', (int(tmp_interval[-1][0]), coin))
                    print(f"[scheduler] Обновлён чекпоинт для {coin} на {end_time}")
                else:
                    sql_put('INSERT INTO checkpoints values (%s,%s) ON CONFLICT DO NOTHING',
                            (coin, int(tmp_interval[-1][0])))
                    print(f"[scheduler] Чекпоинт создан для {coin} со временем {end_time}")
                break

    # --- ДОБАВЛЯЕМ В КОНЦЕ БЛОК проверки неотработанных pump_start ---
    try:
        await check_not_worked_out()
    except Exception as e:
        print(f"[scheduler] Ошибка в check_not_worked_out: {e}")
    print("[scheduler] Проверка окончена, ждем 10 сек.\n")
    await asyncio.sleep(10)

""" Устаревшая функция, пишущая в базу (db.sqlite3) неотработанное событие """

@sync_to_async
def check_not_worked_out():
    now = time.time()
    cutoff = now - 24 * 3600  # 24 часа назад
    pump_starts = Event.objects.filter(
        event_type="pump_start",
        event_time__lt=cutoff
    ).order_by('event_time')

    for ps in pump_starts:
        # Проверяем, был ли pump_end после этого pump_start
        has_pump_end = Event.objects.filter(
            trading_pair=ps.trading_pair,
            event_type="pump_end",
            event_time__gt=ps.event_time
        ).exists()

        # Дополнительно проверяем, не создали ли мы уже not_worked_out по этому pump_start
        # (чтобы не дублировать событие при каждом цикле)
        has_not_worked_out = Event.objects.filter(
            trading_pair=ps.trading_pair,
            event_type="not_worked_out",
            event_time__gt=ps.event_time  # было бы логично сравнить, но можно и хитрее
        ).exists()

        if not has_pump_end and not has_not_worked_out:
            # Создаем новое событие "not_worked_out" со временем = (ps.event_time + 24ч) или now
            # на ваш вкус; часто берут просто текущее время.
            Event.objects.create(
                trading_pair=ps.trading_pair,
                event_type="not_worked_out",
                event_time=now,
                event_price=ps.event_price,  # можно взять начальную цену пампа
                description="Памп так и не отработался за 24ч."
            )
            print(f"[scheduler] not_worked_out: {ps.trading_pair.symbol} (pump_start={ps.id})")



async def main():
    print("[main] Запуск бота и шедулера.")
    # Checkup with 10 seconds sleep
    asyncio.create_task(scheduler())

if __name__ == "__main__":
    asyncio.run(main())