import asyncio
import json
from .data_fetcher import DataFetcher
from .db_manager import DBManager
import datetime
#
with open('config/config.json') as config_file:
    config = json.load(config_file)


class TaskManager:

    def __init__(self):
        self.data_fetcher = DataFetcher()
        self.db_manager = DBManager(config["exchanges"]["binance"]["db_path"])

    async def collect_data(self, exchange, ticker, timeframe, limit, api_url):
        last_time = await self.db_manager.get_last_time_from_db(ticker, timeframe)

        while True:
            new_data = await self.data_fetcher.fetch_ticker_data(exchange, ticker, timeframe, last_time, limit, api_url)
            await self.db_manager.save_to_db(new_data, ticker, timeframe)
            last_time = await self.db_manager.get_last_time_from_db(ticker, timeframe)
            if last_time:
                timestamp_seconds = last_time.timestamp()
                date_time = datetime.datetime.fromtimestamp(timestamp_seconds)
                formatted_date_time = date_time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"{ticker}_{timeframe} -> {formatted_date_time}")
            await asyncio.sleep(10)

    async def start_data_collection(self):
        tasks = []
        for exchange, settings in config["exchanges"].items():
            limit = settings["data_limit"]
            api_url = settings["api_url"]
            for ticker in settings["tickers"]:
                for timeframe in settings["timeframes"]:
                    tasks.append(asyncio.create_task(self.collect_data(
                        exchange, ticker, timeframe, limit, api_url)))
            await asyncio.sleep(10)
        await asyncio.gather(*tasks)
