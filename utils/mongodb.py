import datetime
from dateutil.relativedelta import relativedelta
import os
from motor import motor_asyncio

URI = os.getenv("MONGODB_URI")
client = motor_asyncio.AsyncIOMotorClient(URI)
db = client.get_database("salary_box")
salaries = db.get_collection("salaries")


async def compile_intervals(dt_from, dt_upto, group_type):
    '''
    Функция на основании входных данных от пользователя формирует список
    интервалов для последующего получения из базы даннных статистических данных
    :param dt_from: дата начала сбора статистики
    :param dt_upto: дата окончания сбора статистики
    :param group_type: тип периода сбора статистики
    :return: список временных интервалов для запроса к бд
    '''
    intervals = []
    if group_type == 'month':
        months, days, hours = (1, 0, 0)
        start_of_period = dt_from + relativedelta(day=1, hour=0, minute=0)
        loop_count = (relativedelta(dt_upto, start_of_period).years * 12 +
                      relativedelta(dt_upto, start_of_period).months + 1)
    elif group_type == 'day':
        months, days, hours = (0, 1, 0)
        start_of_period = dt_from + relativedelta(hour=0, minute=0)
        loop_count = (dt_upto - start_of_period).days + 1
    else:
        months, days, hours = (0, 0, 1)
        start_of_period = dt_from + relativedelta(minute=0)
        delta = dt_upto - start_of_period
        loop_count = (delta.days * 24) + (delta.seconds // 3600) + 1
    end_of_period = start_of_period + relativedelta(months=+months, days=+days, hours=+hours, minutes=-1)
    intervals.append((start_of_period, end_of_period))
    for index in range(2, loop_count+1):
        start_of_period += relativedelta(months=+months, days=+days, hours=+hours)
        end_of_period = start_of_period + relativedelta(months=+months, days=+days, hours=+hours, minutes=-1)
        intervals.append((start_of_period, end_of_period))
    return intervals


async def do_aggregate(dt_from, dt_upto, group_type):
    '''
    Функция принимает на вход: начальное время, конечное время агрегации данных,
    тип периода агрегации (месяц, день, час), по каждому интервалу производит запрос
    к базе данных и суммирует размер зарплат в каждом интервале.
    Возвращает два списка данных: суммы зарплат в каждом временном интервале и список
    временных меток для каждого интервала
    :param dt_from:
    :param dt_upto:
    :param group_type:
    :return: dataset, labels - наботы выходных данных
    '''
    dataset = []
    labels = []
    intervals = await compile_intervals(dt_from, dt_upto, group_type)
    for interval in intervals:
        cursor = db.salaries.aggregate([
            {"$match": {"dt": {"$gte": interval[0], "$lte": interval[1]}}},
            {"$group": {"_id": "null", "total": {"$sum": "$value"}}},
        ])
        result = await cursor.to_list()
        try:
            result_sum = result[0]['total']
            dataset.append(result_sum)
        except IndexError:
            dataset.append(0)
        labels.append(datetime.datetime.isoformat(interval[0]))
    return dataset, labels

