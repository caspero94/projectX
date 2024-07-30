import asyncio
import json
import logging
from .data_fetcher import DataFetcher
from .db_manager import DBManager

logger = logging.getLogger(__name__)

with open('src/config/config.json') as config_file:
    config = json.load(config_file)


class TaskManager:

    def __init__(self):

        logger.info("RECOLECTOR GENERAL - PREPARADO")
        self.data_fetcher = DataFetcher()
        self.db_manager = DBManager(config["exchanges"]["binance"]["db_path"])

    async def start_data_collection(self):

        try:
            logger.info("RECOLECTOR GENERAL - INICIADO")
            tickers_master = []

            for exchange, settings in config["exchanges"].items():
                limit = settings["data_limit"]
                api_url = settings["api_url"]

                for ticker in settings["tickers"]:

                    for timeframe in settings["timeframes"]:
                        tickers_master.append(
                            [f"{exchange}_{ticker}_{timeframe}", exchange, ticker, timeframe, limit, api_url, 0])

            await self.db_manager.init_db()
            q_data = asyncio.Queue(maxsize=30)
            q_lastime = asyncio.Queue(maxsize=30)
            while True:
                tasks = []
                task0 = asyncio.create_task(
                    self.db_manager.get_last_time_from_db(tickers_master, q_lastime, "Get date 1"))
                task1 = asyncio.create_task(
                    self.data_fetcher.fetch_ticker_data(q_lastime, q_data, "Get data 1"))
                task2 = asyncio.create_task(
                    self.data_fetcher.fetch_ticker_data(q_lastime, q_data, "Get data 2"))
                task3 = asyncio.create_task(
                    self.db_manager.save_to_db(q_data, "Save data 1"))
                task4 = asyncio.create_task(
                    self.db_manager.save_to_db(q_data, "Save data 2"))
                task5 = asyncio.create_task(
                    self.db_manager.save_to_db(q_data, "Save data 3"))
                tasks.append(task0)
                tasks.append(task1)
                tasks.append(task2)
                tasks.append(task3)
                tasks.append(task4)
                tasks.append(task5)
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:

            logger.error(f"RECOLECTOR GENERAL - INICIADO: {e}")
            raise
