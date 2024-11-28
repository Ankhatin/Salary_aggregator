import asyncio
import datetime
import json
import os

from pathlib import Path
from fastapi import Request, FastAPI, HTTPException
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from starlette import status
from fastapi.responses import HTMLResponse
from utils.mongodb import do_aggregate
from utils.telegram import run_telebot


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    bot_task = loop.create_task(run_telebot())
    yield
    bot_task.cancel()

app = FastAPI(lifespan=lifespan)


@app.get("/", response_description="Instructions")
async def get_root():
    '''
    Функция отображает страницу с ознакомительной информацией о работе сервиса
    при обращении к корневому маршруту 'http://0.0.0.0:8000/'
    :return:
    '''
    html_content = """
    <html>
        <head>
            <h3>Сервис по получению статистических данных о зарплатах сотрудников</h3>
        </head>
        <body>
            <p>Для получения статистических данных используйте GET или POST запрос к нашему серверу.</p>
            <p>
                Параметр "dt_from" в формате ISO - дата и время начала периода агрегации данных.<br>
                Параметр "dt_upto" в формате ISO - дата и время конца периода агрегации данных.
            </p>
            <p>Допустимые виды интервалов: "month", "day", "hour".</p>
            <p>
                Пример отправки GET запроса:<br>
                "http://127.0.0.1:8000/aggregated_data/?dt_from=2022-10-01T00:00:00&dt_upto=2022-11-30T23:59:00&group_type=day"<br><br>
                Для отправки POST запроса в теле запроса (body) отправьте строку в формате JSON.<br>
                Пример:<br>
                {<br>
                "dt_from": "2022-09-01T00:00:00",<br> 
                "dt_upto": "2022-12-31T23:59:00",<br>
                "group_type": "month"<br>
                }<br><br>
                Кроме того, вы можете воспользоваться нашим Телеграм-ботом для отправки запроса и получения ответа.<br>
                Пример запроса в Телеграм-боте:<br>
                {<br>
                "dt_from": "2022-02-01T00:00:00",<br>
                "dt_upto": "2022-02-02T00:00:00",<br>
                "group_type": "hour"<br>
                } 
            </p>
            <p>
                Образец выходных данных:<br>
                {<br>
                'dataset': [5906586, 5327636, 5889803, 6092634],<br>
                'labels': ['2022-09-01T00:00:00', '2022-10-01T00:00:00', '2022-11-01T00:00:00', '2022-12-01T00:00:00']<br>
                }<br>
            </p>
        </body>
    """
    return HTMLResponse(content=html_content, status_code=200)


@app.post("/aggregated_data/",
          response_description="Aggregated data",
          status_code=status.HTTP_200_OK,
          openapi_extra={
              "requestBody": {
                  "content": {
                      "application/json": {
                          "schema": {
                              "required": ["dt_from", "dt_upto", "group_type"],
                              "properties": {
                                  "dt_from": {"type": "string"},
                                  "dt_upto": {"type": "string"},
                                  "group_type": {"type": "string"}
                              },
                              "example": {
                                  "dt_from": "2022-10-01T00:00:00",
                                  "dt_upto": "2022-11-30T23:59:00",
                                  "group_type": "day"
                              }
                          }
                      }
                  },
                  "required": True,
              },
          },
          )
async def get_data_through_post(request: Request):
    '''
    Функция FAST API при обращении к маршруту 'http://0.0.0.0:8000/aggregated_data/'
    посредством отправки POST запроса. Получает входные данные из тела (body)
    запроса (request) в виде строки json.
    В ответ возвращает агрегированные данные в формате json
    :param request:
    :return: json
    '''
    raw_body = await request.body()
    try:
        data = json.loads(raw_body)
    except json.decoder.JSONDecodeError:
        raise HTTPException(status_code=422, detail="Invalid JSON")
    if data:
        dt_from = datetime.datetime.fromisoformat(data['dt_from'])
        dt_upto = datetime.datetime.fromisoformat(data['dt_upto'])
        group_type = data['group_type']
        if dt_from > dt_upto:
            raise HTTPException(status_code=422, detail=f"Invalid dates of period: {dt_from} > {dt_upto}")
        elif group_type not in ['month', 'day', 'hour']:
            raise HTTPException(status_code=422, detail=f"Invalid type of interval: '{group_type}'")

        dataset, labels = await do_aggregate(dt_from, dt_upto, group_type)
        return {"dataset": dataset, "labels": labels}


@app.get("/aggregated_data/",
          response_description="Aggregated data",
          status_code=status.HTTP_200_OK)
async def get_data(dt_from: str, dt_upto: str, group_type: str):
    '''
    Функция FAST API для маршрута 'http://0.0.0.0:8000/aggregated_data/'
    посредством отправки GET запроса.
    Получает значения параметров из адресной строки браузера (url)
    :params dt_from, dt_upto, group_type
    :return: json
    '''
    dt_from = datetime.datetime.fromisoformat(dt_from)
    dt_upto = datetime.datetime.fromisoformat(dt_upto)
    group_type = group_type
    dataset, labels = await do_aggregate(dt_from, dt_upto, group_type)
    return {"dataset": dataset, "labels": labels}
