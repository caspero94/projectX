import pandas as pd
import sqlite3


def getDataTicker(tickerName: str, buffer: int, conn: sqlite3.Connection) -> pd.DataFrame:
    # Obtener datos del ticker
    query = f"""
    SELECT * FROM (
    SELECT * FROM {tickerName}
    ORDER BY open_time DESC
    LIMIT {buffer})
    ORDER BY open_time ASC
    """

    # print(f"--> {tickerName}: DATOS OBTENIDOS DE DB")
    return pd.read_sql_query(query, conn)
