import datetime
import json

import aiogram
import pytest

from utils.telegram import *

input_message = '{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}'
wrong_json = '{"dt_from" "2022-09-01T00:00:00" "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}'
missed_arg = '{"dt_upto": "2022-12-31T23:59:00", "group_type": "month"}'


@pytest.mark.asyncio()
async def test_async_run_telebot():
    '''
    Функция проверяет зарегистрирован ли в системе Телеграм бот
    с таким токеном. Если инициализация экземпляра бота прошла успешно,
    то в свойстве bot.token мы прочитаем токен созданного экземпляра
    :return:
    '''
    bot = Bot(token=TELEGRAM_TOKEN)
    assert bot.token == TELEGRAM_TOKEN
    with pytest.raises(Exception) as excinfo:
        Bot(token="1ii23kganuuewww32fwe")
    assert str(excinfo) == "<ExceptionInfo TokenValidationError('Token is invalid!') tblen=3>"


@pytest.mark.asyncio()
async def test_async_message_handler():
    '''
    Функция отправляет тестовое сообщение пользователю по его ID в Телеграм.
    В случае успеха возвращается отправленное сообщение
    :return:
    '''
    bot = Bot(token=TELEGRAM_TOKEN)
    message = await bot.send_message(chat_id=738543210, text='test')
    assert message.text == 'test'


@pytest.mark.asyncio()
async def test_async_get_input_vars():
    dt_from, dt_upto, group_type = await get_input_vars(input_message)
    assert [dt_from, dt_upto, group_type] == [datetime.datetime.fromisoformat('2022-09-01T00:00:00'),
                                              datetime.datetime.fromisoformat('2022-12-31T23:59:00'),
                                              "month"]
    error = await get_input_vars(wrong_json)
    assert error == "Некорректная структура данных JSON"
    error = await get_input_vars(missed_arg)
    assert error == "Недостаточно данных для запроса"
