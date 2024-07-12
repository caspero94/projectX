import pandas as pd
import asyncio
import sqlite3
import json
from datetime import datetime

from lightweight_charts import Chart

with open('config/config.json') as config_file:
    config = json.load(config_file)
for exchange, settings in config["exchanges"].items():
    tickers = settings["tickers"]

# CONECT TO DATABASE


def connetDatabase(databaseName) -> sqlite3.Connection:
    return sqlite3.connect(databaseName)


def cursorCreate(conn: sqlite3.Connection) -> sqlite3.Cursor:
    return conn.cursor()


databaseName = "database/binance_data.db"
conn = connetDatabase(databaseName)
cursor = cursorCreate(conn)

# GET TICKER DATABASE


def getDataTicker(tickerName: str, buffer: int, conn: sqlite3.Connection) -> pd.DataFrame:

    query = f"""
    SELECT * FROM (
    SELECT * FROM {tickerName}
    ORDER BY open_time DESC
    LIMIT {buffer})
    ORDER BY open_time ASC
    """

    return pd.read_sql_query(query, conn)


def get_bar_data(symbol, timeframe, buffer=5000):
    if symbol not in tickers:
        return pd.DataFrame()
    data = getDataTicker(f"{symbol}_{timeframe}", buffer, conn)
    data['open_time'] = pd.to_datetime(data['open_time'], unit='ms')
    data['open_time'] = data['open_time'].dt.tz_localize(
        'UTC').dt.tz_convert('Europe/Madrid')

    data.set_index('open_time', inplace=True)
    return data

# FUCTIONS CHART


def on_new_bar(chart):
    print('New bar event!')


def on_search(chart, searched_string="None"):
    if searched_string != "None":
        chart.topbar["tickers"].value = (searched_string)  # CORREGIR
    else:
        searched_string = chart.topbar["tickers"].value
    new_data = get_bar_data(searched_string, chart.topbar['timeframe'].value)

    if new_data.empty:
        chart.set(new_data, True)
        return chart.watermark(f'NO HAY DATOS PARA {searched_string} {chart.topbar['timeframe'].value}', color='rgba(180, 180, 240, 0.3)')

    chart.set(new_data)
    return chart.watermark(f'{searched_string} {chart.topbar['timeframe'].value}', color='rgba(180, 180, 240, 0.3)')


def on_timeframe_selection(chart):
    new_data = get_bar_data(
        chart.topbar["tickers"].value, chart.topbar["timeframe"].value, 1000)
    if new_data.empty:
        return
    chart.set(new_data, True)
    chart.watermark(f'''{chart.topbar["tickers"].value} {
                    chart.topbar["timeframe"].value}''', color='rgba(180, 180, 240, 0.3)')


async def update_clock(chart):
    while chart.is_alive:
        await asyncio.sleep(1-(datetime.now().microsecond/1_000_000))
        chart.topbar['clock'].set(datetime.now().strftime('%H:%M:%S'))


async def data_loop(chart):
    while True:
        data = get_bar_data(
            chart.topbar["tickers"].value, chart.topbar["timeframe"].value, 1)

        for i, series in data.iterrows():
            if not chart.is_alive:
                return
            chart.update(series)
            await asyncio.sleep(0.05)


async def start_chart():
    chart = Chart(toolbox=True)
    chart.legend(visible=True)
    chart.events.new_bar += on_new_bar
    chart.events.search += on_search
    chart.topbar.menu('tickers', options=tickers, default="BTCUSDT",
                      separator=False, align="left", func=on_search)
    chart.topbar.textbox('clock', align="right")
    # chart.topbar.textbox('symbol', 'BTCUSDT')
    chart.topbar.switcher('timeframe', ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"), default='5m',
                          func=on_timeframe_selection)  # , "1w", "1M"
    chart.watermark(f'''{chart.topbar["tickers"].value} {
                    chart.topbar["timeframe"].value}''', color='rgba(180, 180, 240, 0.3)')
    data = get_bar_data(
        chart.topbar["tickers"].value, chart.topbar["timeframe"].value, 1000)

    chart.set(data)
    await asyncio.gather(chart.show_async(), data_loop(chart), update_clock(chart))


# def main():
#    asyncio.run(start_chart())

if __name__ == '__main__':
    asyncio.run(start_chart())
