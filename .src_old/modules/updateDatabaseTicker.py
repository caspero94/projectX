import pandas as pd
import sqlite3
import time
from modules.database.createTable import createTable
from modules.database.insertData import insertData
from modules.database.checkTickerTable import checkTickerTable
from modules.database.getDataTicker import getDataTicker
from modules.requestData.requestsTickerData import requestsTickerData


def updateDatabaseTicker(symbol: str = "BTCUSDT", timeframe: str = "1h", limit: int = 1000, buffer: int = 1) -> None:
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()
    # set TickerName
    tickerName: str = f"{symbol}_{timeframe}"
    n_reg = 2
    # Comprobamos si existe tabla para el ticker, sino procedemos a crearla
    if not checkTickerTable(tickerName, cursor):
        createTable(conn, cursor, tickerName)
        # Descargamos datos nuevos
        dataDowload = requestsTickerData(tickerName, symbol, timeframe, limit)
        # Insertamos datos en db
        n_reg = insertData(conn, cursor, dataDowload, tickerName)

    # Obtenemos datos de la tabla
    data = getDataTicker(tickerName, buffer, conn)

    # Actualizamos lastDate
    lastDate = data['open_time'].iloc[-1]

    while True:  # n_reg > 1
        if timeframe in ["1h", "30m", "15m", "5m", "3m"]:
            time.sleep(5)
        else:
            time.sleep(0.3)

        # Descargamos datos nuevos
        dataDowload = requestsTickerData(
            tickerName, symbol, timeframe, limit, lastDate)
        # Insertamos datos en db
        n_reg = insertData(conn, cursor, dataDowload, tickerName)
        # Obtenemos datos db
        data = getDataTicker(tickerName, buffer, conn)
        # Actualizamos lastDate
        lastDate = data['open_time'].iloc[-1]
        print(f"Actualizado {tickerName}")

    # Cerramos conexion base de datos
    conn.close()
    print(f"--> {tickerName}: DB ACTUALIZADA <--")
    return None
