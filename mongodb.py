import datetime
from dateutil.relativedelta import relativedelta
import os
from motor import motor_asyncio

URI = os.getenv("MONGODB_URI")
client = motor_asyncio.AsyncIOMotorClient(URI)
db = client.get_database("salary_box")
salaries= db.get_collection("salaries")


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
    :return: dataset, labels
    '''
    intervals = []
    dataset = []
    labels = []
    if group_type == 'month': # период агрегации - месяц
        # устанавливаем начало первого интервала на первый день месяца
        start_of_period = dt_from + relativedelta(day=1, hour=0, minute=0)
        # устанавливаем конец первого периода на последний день месяца, время 23:59
        end_of_period = start_of_period + relativedelta(days=+30, minutes=-1)
        intervals.append((dt_from, end_of_period))
        # получим количество месяцев в периоде + 1, чтобы включить последний месяц
        number_month = relativedelta(dt_upto, dt_from).months + 1
        # цикл со второго по предпоследний месяц
        for index in range(2, number_month):
            start_of_period += relativedelta(months=+1)
            end_of_period += relativedelta(months=+1)
            intervals.append((start_of_period, end_of_period))
        start_of_period += relativedelta(months=+1)
        intervals.append((start_of_period, dt_upto))
    elif group_type == 'day': # период агрегации - день
        # устанавливаем начало первого интервала на нулевой час дня
        start_of_period = dt_from + relativedelta(hour=0, minute=0)
        # устанавливаем конец первого интервала на первый день, время 23:59
        end_of_period = start_of_period + relativedelta(days=+1, minutes=-1)
        intervals.append((start_of_period, end_of_period))
        # Получим количество дней в запрашиваемом периоде + 1, чтобы включить последний день
        number_days = (dt_upto - dt_from).days + 1
        # цикл со второго по последний день
        for index in range(2, number_days+1):
            start_of_period += relativedelta(days=+1)
            end_of_period += relativedelta(days=+1)
            intervals.append((start_of_period, end_of_period))
    elif group_type == 'hour': # период агрегации - час
        start_of_period = dt_from + relativedelta(minute=0)
        # устанавливаем конец первого интервала на последнюю минуту первого часа
        end_of_period = start_of_period + relativedelta(hours=+1, minutes=-1)
        intervals.append((start_of_period, end_of_period))
        delta = relativedelta(dt_upto, dt_from)
        # Получим количество часов в запрашиваемом периоде + 1, чтобы включить последний час
        number_hours = delta.days * 24 + delta.hours + 1
        # цикл со второго по последний час
        for index in range(2, number_hours + 1):
            start_of_period += relativedelta(hours=+1)
            end_of_period += relativedelta(hours=+1)
            intervals.append((start_of_period, end_of_period))
    for interval in intervals:
        cursor = db.salaries.aggregate([
            {"$match": {"dt": {"$gte": interval[0], "$lte": interval[1]}}},
            {"$group": {"_id": "null", "total": {"$sum": "$value"}}},
            # {"$out": "result"}
        ])

        result = await cursor.to_list()
        try:
            result_sum = result[0]['total']
            dataset.append(result_sum)
        except IndexError:
            dataset.append(0)
        labels.append(datetime.datetime.isoformat(interval[0]))
    return dataset, labels

