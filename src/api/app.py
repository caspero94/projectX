from fastapi import FastAPI, Query
import json
import asyncpg
from data_collection.db_manager import DBManager

# Cargar el archivo de configuración
with open('src/config/config.json') as config_file:
    config = json.load(config_file)

app = FastAPI()
db_manager = DBManager(config["exchanges"]["binance"]["db_path"])


@app.on_event("startup")
async def startup_event():
    for exchange, settings in config["exchanges"].items():
        await db_manager.init_db()


@app.get("/api/klines/{exchange}")
async def get_data(exchange: str, ticker: str = Query(...), timeframe: str = Query(...), startTime: int = Query(None), endTime: int = Query(None), limit: int = Query(100, gt=0, le=1000)):
    db_path = "postgresql://pedro:dolar816.@server-data.postgres.database.azure.com:5432/postgres"
    query = f'SELECT * FROM "{exchange}_{ticker}_{timeframe}" WHERE 1=1'
    if startTime:
        query += f" AND open_time >= {startTime}"
    if endTime:
        query += f" AND close_time <= {endTime}"

    query += " ORDER BY open_time DESC"
    query += f" LIMIT {limit}"

    conn = await asyncpg.connect(dsn=db_path)
    try:
        data = await conn.fetch(query)
    finally:
        await conn.close()

    return data
    # async with asyncpg.connect(db_path) as db:
    #     async with db.execute(query) as cursor:
    #         data = await cursor.fetchall()
    #         return data
