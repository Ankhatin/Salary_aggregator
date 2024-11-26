import asyncio
import os
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
import json
import datetime

from dotenv import load_dotenv

from mongodb import do_aggregate

TOKEN = os.getenv("TELEGRAM_TOKEN")

dp = Dispatcher()

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)
# @dp.message(CommandStart())
# async def command_start_handler(message) -> None:
#     """
#     This handler receives messages with `/start` command
#     """
#     # Most event objects have aliases for API methods that can be called in events' context
#     # For example if you want to answer to incoming message you can use `message.answer(...)` alias
#     # and the target chat will be passed to :ref:`aiogram.methods.send_message.SendMessage`
#     # method automatically or call API method directly via
#     # Bot instance: `bot.send_message(chat_id=message.chat.id, ...)`
#     await message.answer(f"Hello, {message.from_user.full_name}!")


@dp.message(CommandStart())
async def command_start_handler(message) -> None:
    await message.reply('Введите данные в формате JSON, пример: '
                        '{"dt_from": "2022-09-01T00:00:00",'
                        '"dt_upto": "2022-12-31T23:59:00",'
                        '"group_type": "month"}')


@dp.message()
async def message_handler(message) -> None:
    if message.content_type == "text":
        text = message.text.split()
        text = ''.join(text)
        try:
            data = json.loads(text)
        except json.decoder.JSONDecodeError:
            data = None
            await message.answer("Некорректная структура данных JSON")
        if data:
            dt_from = datetime.datetime.fromisoformat(data['dt_from'])
            dt_upto = datetime.datetime.fromisoformat(data['dt_upto'])
            group_type = data['group_type']
            print(dt_from, dt_upto)
            dataset, labels = await do_aggregate(dt_from, dt_upto, group_type)
            message_text = str({"dataset": dataset, "labels": labels})
            try:
                index = message_text.index("'labels'")
                message_text = message_text[:index] + "\n" + message_text[index:]
            except ValueError:
                await message.answer(f"Возможно некорректная структура выходных данных:\n {message_text}")
            await message.answer(message_text)


async def run_telebot():
    bot = Bot(token=os.getenv("TELEGRAM_TOKEN"))
    await dp.start_polling(bot)
