import pandas as pd
import asyncio
from datetime import datetime

from lightweight_charts import Chart
from modules.database.getDataTicker import getDataTicker
from modules.database.connectDatabase import conn


def get_bar_data(symbol, timeframe, buffer=2000):
    data = getDataTicker(f"{symbol}_{timeframe}", buffer, conn)
    data['open_time'] = pd.to_datetime(data['open_time'], unit='ms')
    data['open_time'] = data['open_time'].dt.tz_localize(
        'UTC').dt.tz_convert('Europe/Madrid')

    data.set_index('open_time', inplace=True)
    return data


def menu(chart):
    chart.topbar.switcher()


def exitProcess():
    KeyboardInterrupt


async def update_clock(chart):
    while chart.is_alive:
        await asyncio.sleep(1-(datetime.now().microsecond/1_000_000))
        chart.topbar['clock'].set(datetime.now().strftime('%H:%M:%S'))


async def data_loop(chart):
    while True:
        data = get_bar_data(
            chart.topbar["symbol"].value, chart.topbar["timeframe"].value, 1)

        for i, series in data.iterrows():
            # if not chart.is_alive:
            #    return
            chart.update(series)
            await asyncio.sleep(0.03)


def on_new_bar(chart):
    print('New bar event!')


def on_search(chart, searched_string):
    data = get_bar_data(searched_string, chart.topbar['timeframe'].value)
    chart.topbar['symbol'].set(searched_string)
    chart.set(data)


def on_timeframe_selection(chart):
    data = get_bar_data(
        chart.topbar["symbol"].value, chart.topbar["timeframe"].value, 1000)
    chart.set(data, True)


async def main():
    chart = Chart(toolbox=True)
    chart.legend(visible=True)
    chart.events.new_bar += on_new_bar
    chart.events.search += on_search
    chart.topbar.menu('exit', options=(
        "opcion1", "2", "opt3"), separator=False, func=menu)
    chart.topbar.textbox('clock', align="right")
    chart.topbar.button('exit', button_text="X",
                        align="right", func=exitProcess)
    chart.topbar.textbox('symbol', 'BTCUSDT')
    chart.topbar.switcher('timeframe', ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"), default='5m',
                          func=on_timeframe_selection)  # , "1w", "1M"

    data = get_bar_data(
        chart.topbar["symbol"].value, chart.topbar["timeframe"].value, 1000)

    chart.set(data)
    await asyncio.gather(chart.show_async(), data_loop(chart), update_clock(chart))


def start_chart():
    asyncio.run(main())


##########################################################
"""

# def calculate_sma(df, period: int = 50):
#     return pd.DataFrame({
#         'time': df.index,
#         f'SMA {period}': df['close'].rolling(window=period).mean()
#     }).dropna()


def createChart() -> Chart:

    # INICIAMOS INDICADOR
    # line = chart.create_line('SMA 50')
    # sma_data = calculate_sma(data, period=50)
    # line.set(sma_data)

"""
