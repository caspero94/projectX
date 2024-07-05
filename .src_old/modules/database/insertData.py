import sqlite3
import requests
# from modules.database.connectDatabase import conn, cursor


def insertData(conn: sqlite3.Connection, cursor: sqlite3.Cursor, data: requests.Response, tickerName: str) -> int:

    if data is None:
        return 0
    # Preparar la instrucción SQL de inserción
    insert_query = f'''
    INSERT INTO {tickerName} (
        open_time, open, high, low, close, volume, close_time,
        quote_asset_volume, number_of_trades, taker_buy_base_asset_volume,
        taker_buy_quote_asset_volume, ignore
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(open_time) DO UPDATE SET
    open = excluded.open,
    high = excluded.high,
    low = excluded.low,
    close = excluded.close,
    volume = excluded.volume,
    close_time = excluded.close_time,
    quote_asset_volume = excluded.quote_asset_volume,
    number_of_trades = excluded.number_of_trades,
    taker_buy_base_asset_volume = excluded.taker_buy_base_asset_volume,
    taker_buy_quote_asset_volume = excluded.taker_buy_quote_asset_volume,
    ignore = excluded.ignore
    '''

    # Insertar cada registro del JSON en la tabla
    dataJson = data.json()
    for record in dataJson:
        cursor.execute(insert_query, record)
    conn.commit()
    # print(f"--> {tickerName}: {len(dataJson)} REGISTROS AGREGADOS A DB")

    return len(dataJson)
