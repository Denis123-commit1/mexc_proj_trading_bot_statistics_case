import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
import schedule
import time
import threading

API_TOKEN = 'YOUR_TELEGRAM_BOT_TOKEN'
CHAT_ID = 'YOUR_CHAT_ID'

# Initialize bot and dispatcher
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)


async def send_daily_message():
    await bot.send_message(chat_id=CHAT_ID, text='This is your daily message!')


def schedule_daily_message(loop):
    schedule.every().day.at("14:00").do(run_send_daily_message, loop)


def run_send_daily_message(loop):
    asyncio.run_coroutine_threadsafe(send_daily_message(), loop)


def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(1)


@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    await message.reply("Welcome! The bot is running.")


async def periodic_task():
    while True:
        print("Running every 10 seconds...")
        await asyncio.sleep(10)


if __name__ == '__main__':
    # Get the current event loop
    loop = asyncio.get_event_loop()

    # Start the daily message scheduler
    threading.Thread(target=schedule_daily_message, args=(loop,), daemon=True).start()
    # Start the schedule runner in a separate thread
    threading.Thread(target=run_schedule, daemon=True).start()

    # Start the periodic task
    asyncio.create_task(periodic_task())

    # Start the bot
    executor.start_polling(dp, skip_updates=True)
