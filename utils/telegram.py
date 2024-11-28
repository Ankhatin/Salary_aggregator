import asyncio
import os
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
import json
import datetime

from dotenv import load_dotenv

from utils.mongodb import do_aggregate, compile_intervals

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

dp = Dispatcher()


async def get_input_vars(message):
    '''
    Функция принимает на вход текстовое сообщение от пользователя в json строке,
    преобразует в формат данных Python и возвращает список переменных в вызывающую функцию.
    В случае некорректных данных возвращается ответ с текстом ошибки.
    :param message: json row
    :return: Union[list, str]
    '''
    text = message.split()
    text = ''.join(text)
    try:
        data = json.loads(text)
    except json.decoder.JSONDecodeError:
        return "Некорректная структура данных JSON"
    if data:
        if data.get('dt_from') and data.get('dt_upto') and data.get('group_type'):
            if data['group_type'] in ['month', 'day', 'hour']:
                # все входные данные в наличии, устанавливаем значения переменных
                dt_from = datetime.datetime.fromisoformat(data['dt_from'])
                dt_upto = datetime.datetime.fromisoformat(data['dt_upto'])
                group_type = data['group_type']
                return [dt_from, dt_upto, group_type]
            else:
                return f"Неподдерживаемый тип интервала данных: {data['group_type']}"
        else:
            return "Недостаточно данных для запроса"


@dp.message(CommandStart())
async def command_start_handler(message) -> None:
    """
    Функция обрабатывает команду "/start", введенную пользователем
    в Телеграм-боте. Выводит в чат сообщение с предложением ввести
    данные для запроса в базу данных.
    :param message:
    :return:
    """
    await message.reply('Введите данные в формате JSON, пример: '
                        '{"dt_from": "2022-09-01T00:00:00",'
                        '"dt_upto": "2022-12-31T23:59:00",'
                        '"group_type": "month"}')


@dp.message()
async def message_handler(message) -> None:
    """
    Функция обрабатывает текстовые сообщения от пользователя в Телеграм-боте,
    проверяет корректность входных данных, вызывает функцию, которая
    агрегирует данные из базы данных MongoDB и возвращает ответ пользователю
    в чат Телеграм-бота
    :param message: сообщение от пользователя
    :return: None
    """
    if message.content_type == "text":
        input_data = await get_input_vars(message.text) # получим переменные для запроса в базу данных
        if type(input_data) is list:
            # если данные в сообщении корректные, делаем запрос в базу данных
            dt_from = input_data[0]
            dt_upto = input_data[1]
            group_type = input_data[2]
            dataset, labels = await do_aggregate(dt_from, dt_upto, group_type) # получим выходные данные
            message_text = str({"dataset": dataset, "labels": labels}) # строковое представление словаря для отображения
            index = message_text.index("'labels'")
            message_text = message_text[:index] + "\n" + message_text[index:]
            await message.answer(message_text) # отправим ответ пользователю
        else:
            # выводим сообщение об ошибке
            await message.answer(input_data)
    else:
        await message.answer("Некорректные входные данные")


async def run_telebot():
    """
    Функция создает экземпляр Телеграм-бота и запускает цикл взаимодействия
    с сервером Телеграм
    """
    bot = Bot(token=TELEGRAM_TOKEN)
    await dp.start_polling(bot)

# asyncio.run(run_telebot())