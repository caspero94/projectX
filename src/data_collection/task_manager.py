import asyncio
import json
import logging
from .data_fetcher import DataFetcher
from .db_manager import DBManager
from datetime import datetime

logger = logging.getLogger(__name__)

with open('src/config/config.json') as config_file:
    config = json.load(config_file)


class TaskManager:

    def __init__(self):
        self.data_fetcher = DataFetcher()
        self.db_manager = DBManager(config["exchanges"]["binance"]["db_path"])
        self.semaphore = asyncio.Semaphore(50)

    async def collect_data(self, exchange, ticker, timeframe, limit, api_url):

        try:
            while True:
                async with self.semaphore:
                    logging.ERROR(F"INICIO TAREA {ticker}__{timeframe}")
                    last_time = await self.db_manager.get_last_time_from_db(ticker, timeframe, exchange)
                    if int(last_time) < 1720562000000:

                        new_data = await self.data_fetcher.fetch_ticker_data(exchange, ticker, timeframe, last_time, limit, api_url)
                        await self.db_manager.save_to_db(new_data, ticker, timeframe, exchange)
                        lastdata = datetime.fromtimestamp(int(last_time)/1000)
                        logger.info(f"""Collect_data --> {exchange} --> {ticker} --> {
                            timeframe} -> {lastdata}""")

                await asyncio.sleep(80)
        except Exception as e:
            logger.error(f"""Error en collect_data: {
                e}, ticker {ticker} - {timeframe}""")
            raise

    async def start_data_collection(self):
        try:
            await self.db_manager.init_db()
            tasks = []
            for exchange, settings in config["exchanges"].items():
                limit = settings["data_limit"]
                api_url = settings["api_url"]
                for ticker in settings["tickers"]:
                    for timeframe in settings["timeframes"]:
                        tasks.append(asyncio.create_task(self.collect_data(
                            exchange, ticker, timeframe, limit, api_url)))
                # await asyncio.sleep(15)
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Error en start_data_collection: {e}")
            raise
