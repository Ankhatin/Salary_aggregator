import datetime
import pytest
import asyncio

from utils.mongodb import do_aggregate, compile_intervals

test_input_data_month = {
    "dt_from": "2022-09-01T00:00:00",
    "dt_upto": "2022-12-31T23:59:00",
    "group_type": "month"
}
test_output_data_month = {
    "dataset": [5906586, 5515874, 5889803, 6092634],
    "labels": ["2022-09-01T00:00:00", "2022-10-01T00:00:00", "2022-11-01T00:00:00", "2022-12-01T00:00:00"]
}

test_input_data_day = {
    "dt_from": "2022-11-01T00:00:00",
    "dt_upto": "2022-11-04T23:59:00",
    "group_type": "day"
}
test_output_data_day = {
    "dataset": [195279, 191601, 201722, 207361],
    "labels": ["2022-11-01T00:00:00", "2022-11-02T00:00:00", "2022-11-03T00:00:00", "2022-11-04T00:00:00"]
}

test_input_data_hour = {
    "dt_from": "2022-02-01T00:00:00",
    "dt_upto": "2022-02-01T03:59:00",
    "group_type": "hour"
}
test_output_data_hour = {
    "dataset": [8177, 8407, 4868, 7706],
    "labels": ["2022-02-01T00:00:00",
               "2022-02-01T01:00:00",
               "2022-02-01T02:00:00",
               "2022-02-01T03:00:00",]
}


@pytest.mark.asyncio()
async def test_async_compile_intervals():
    # тестовый случай с периодом в месяц
    dt_from = datetime.datetime.fromisoformat(test_input_data_month['dt_from'])
    dt_upto = datetime.datetime.fromisoformat(test_input_data_month['dt_upto'])
    group_type = test_input_data_month['group_type']
    intervals = await compile_intervals(dt_from, dt_upto, group_type)
    assert intervals == [(datetime.datetime(2022, 9, 1, 0, 0),
                          datetime.datetime(2022, 9, 30, 23, 59)),
                         (datetime.datetime(2022, 10, 1, 0, 0),
                          datetime.datetime(2022, 10, 31, 23, 59)),
                         (datetime.datetime(2022, 11, 1, 0, 0),
                          datetime.datetime(2022, 11, 30, 23, 59)),
                         (datetime.datetime(2022, 12, 1, 0, 0),
                          datetime.datetime(2022, 12, 31, 23, 59))]

    # тестовый случай с периодом в день
    dt_from = datetime.datetime.fromisoformat(test_input_data_day['dt_from'])
    dt_upto = datetime.datetime.fromisoformat(test_input_data_day['dt_upto'])
    group_type = test_input_data_day['group_type']
    intervals = await compile_intervals(dt_from, dt_upto, group_type)
    assert intervals == [(datetime.datetime(2022, 11, 1, 0, 0),
                          datetime.datetime(2022, 11, 1, 23, 59)),
                         (datetime.datetime(2022, 11, 2, 0, 0),
                          datetime.datetime(2022, 11, 2, 23, 59)),
                         (datetime.datetime(2022, 11, 3, 0, 0),
                          datetime.datetime(2022, 11, 3, 23, 59)),
                         (datetime.datetime(2022, 11, 4, 0, 0),
                          datetime.datetime(2022, 11, 4, 23, 59))]

    # тестовый случай с периодом в час
    dt_from = datetime.datetime.fromisoformat(test_input_data_hour['dt_from'])
    dt_upto = datetime.datetime.fromisoformat(test_input_data_hour['dt_upto'])
    group_type = test_input_data_hour['group_type']
    intervals = await compile_intervals(dt_from, dt_upto, group_type)
    assert intervals == [(datetime.datetime(2022, 2, 1, 0, 0),
                          datetime.datetime(2022, 2, 1, 0, 59)),
                         (datetime.datetime(2022, 2, 1, 1, 0),
                          datetime.datetime(2022, 2, 1, 1, 59)),
                         (datetime.datetime(2022, 2, 1, 2, 0),
                          datetime.datetime(2022, 2, 1, 2, 59)),
                         (datetime.datetime(2022, 2, 1, 3, 0),
                          datetime.datetime(2022, 2, 1, 3, 59))]


# @pytest.yield_fixture
# def event_loop():
#     policy = asyncio.get_event_loop_policy()
#     res = policy.new_event_loop()
#     asyncio.set_event_loop(res)
#     res._close = res.close
#     res.close = lambda: None
#
#     yield res
#
#     res._close()


# @pytest.mark.asyncio(loop_scope="function")
async def test_async_do_aggregate():
    dt_from = datetime.datetime.fromisoformat(test_input_data_month['dt_from'])
    dt_upto = datetime.datetime.fromisoformat(test_input_data_month['dt_upto'])
    group_type = test_input_data_month['group_type']
    event_loop = asyncio.get_running_loop()
    task = event_loop.create_task(do_aggregate(dt_from, dt_upto, group_type))
    dataset, labels = await task # do_aggregate(dt_from, dt_upto, group_type)
    assert dataset == test_output_data_month['dataset']
    assert labels == test_output_data_month['labels']

