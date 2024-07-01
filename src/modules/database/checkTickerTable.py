import sqlite3


def checkTickerTable(tickerName: str, cursor: sqlite3.Cursor) -> bool:
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tickerName,))
    # print(f"-> {tickerName}: COMPROBANDO SI EXISTE TABLA EN DB")
    return cursor.fetchone() is not None
