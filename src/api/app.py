from fastapi import FastAPI, Query
import aiosqlite
import json
from data_collection.db_manager import DBManager

app = FastAPI()
db_manager = DBManager()

# Cargar el archivo de configuración
with open('src/config/config.json') as config_file:
    config = json.load(config_file)


@app.on_event("startup")
async def startup_event():
    for exchange, settings in config["exchanges"].items():
        await db_manager.init_db(settings["db_path"])


@app.get("/api/klines/{exchange}")
async def get_data(exchange: str, ticker: str = Query(...), timeframe: str = Query(...), startTime: int = Query(None), endTime: int = Query(None), limit: int = Query(100, gt=0, le=1000)):
    db_path = config["exchanges"][exchange]["db_path"]
    query = f"SELECT * FROM {ticker}_{timeframe} WHERE 1=1"
    if startTime:
        query += f" AND open_time >= {startTime}"
    if endTime:
        query += f" AND close_time <= {endTime}"

    query += " ORDER BY open_time DESC"
    query += f" LIMIT {limit}"

    async with aiosqlite.connect(db_path) as db:
        async with db.execute(query) as cursor:
            data = await cursor.fetchall()
            return data
