import pytest

from app import get_data

dt_from = "2022-09-01T00:00:00"
dt_upto = "2022-12-31T23:59:00"
group_type = "month"


@pytest.mark.asyncio
async def test_async_get_data():
    '''
    Функция проверяет зарегистрирован ли в системе Телеграм бот
    с таким токеном. Если инициализация экземпляра бота прошла успешно,
    то в свойстве bot.token мы прочитаем токен созданного экземпляра
    :return:
    '''
    response = await get_data(dt_from, dt_upto, group_type)
    assert response == {"dataset": [5906586,5515874,5889803,6092634],
                        "labels": ["2022-09-01T00:00:00",
                                   "2022-10-01T00:00:00",
                                   "2022-11-01T00:00:00",
                                   "2022-12-01T00:00:00"]
                       }


