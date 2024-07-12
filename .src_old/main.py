import concurrent.futures
from modules.updateDatabaseTicker import updateDatabaseTicker
from modules.plot.plotTickerTimeframe import start_chart
from multiprocessing import Process


def updateAllSymbolTimeframe():
    list_symbol = ["BTCUSDT", "ETHUSDT"]
    list_timeframe = ["1h", "30m", "15m", "5m", "3m", "1m"]
    # ["1M", "1w", "3d", "1d", "12h", "8h","6h","4h","2h",
    limit = 1000
    buffer = 1
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for symbol in list_symbol:
            for timeframe in list_timeframe:
                futures.append(executor.submit(
                    updateDatabaseTicker, symbol, timeframe, limit, buffer))

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error ocurrido: {e}")


def main():
    # Actualizar base de datos
    proceso1 = Process(target=updateAllSymbolTimeframe)
    # Grafico
    proceso2 = Process(target=start_chart)
    # multiprocessing
    proceso1.start()
    proceso2.start()

    proceso1.join()
    proceso2.join()


if __name__ == '__main__':

    main()
