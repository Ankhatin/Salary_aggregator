import asyncio
import os
from typing import Union

from fastapi import BackgroundTasks, FastAPI
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from starlette import status

# from mongodb import do_find
from telegram import run_telebot


dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    bot_task = loop.create_task(run_telebot())
    yield
    bot_task.cancel()


app = FastAPI(lifespan=lifespan)

# loop = client.get_io_loop()
# loop.run_until_complete(do_find())

# try:
#     client.admin.command('ping')
#     print("Pinged your deployment. You successfully connected to MongoDB!")
# except Exception as e:
#     print(e)


@app.get("/")
async def read_root():
    return {"Hello": "World"}
    # cursor = collection.find()
    # data = await cursor.to_list()
    # list_data = [item['dt'] for item in data]
    # return list_data


# @app.post("/",
#           response_description="Send request",
#           status_code=status.HTTP_201_CREATED)
# async def read_aggregated_data


# async def main():
#     # await asyncio.gather(read_root(), run_telebot())
#     task_telegram = asyncio.create_task(run_telebot())
#     await task_telegram

# try:
#     loop = asyncio.get_running_loop()
#     task_telegram = asyncio.create_task(run_telebot())
#     print(loop)
# except RuntimeError:
#     task_telegram = asyncio.run(run_telebot())
# print("Loop: ", loop)
# if loop:
# task = asyncio.create_task(main())
# else:
# asyncio.run(main(), debug=True)

