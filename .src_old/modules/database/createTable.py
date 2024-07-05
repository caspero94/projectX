import sqlite3


def createTable(conn: sqlite3.Connection, cursor: sqlite3.Cursor, tickerName: str) -> None:
    # Crear la tabla
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {tickerName} (
        open_time TIMESTAMP PRIMARY KEY,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        close_time TIMESTAMP,
        quote_asset_volume REAL,
        number_of_trades INTEGER,
        taker_buy_base_asset_volume REAL,
        taker_buy_quote_asset_volume REAL,
        ignore REAL
    )
    ''')
    conn.commit()

    # print(f"-> {tickerName}: TABLA CREADA EN DB")

    return None
