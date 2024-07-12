import requests
import time


def requestsTickerData(tickerName: str, symbol: str, timeframe: str, limit: int = 1000, lastDate: str = "0000000000000") -> requests.Response:

    url = f"https://api.binance.com/api/v3/klines?symbol={
        symbol}&interval={timeframe}&startTime={lastDate}&limit={limit}"
    # print(f"--> {tickerName}: DATOS DESCARGADOS DEL EXCHANGE")

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response

    except requests.exceptions.RequestException as e:
        print(f"Error obteniendo datos para {tickerName}: {e}")
        return None
