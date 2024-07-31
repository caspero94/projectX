import aiohttp
import logging
import time

logger = logging.getLogger(__name__)


class DataFetcher():

    async def fetch_data(self, session, url: str):

        try:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                return data

        except aiohttp.ClientError as e:
            logger.error(f"API DATOS EXCHANGE - {url}: {e}")
            raise

    async def fetch_ticker_data(self, q_lastime, q_data, service_name):
        x = 0
        while True:
            await time.sleep(5)
            tickers_master = await q_lastime.get()
            url = tickers_master[5].format(ticker=tickers_master[2], timeframe=tickers_master[3],
                                           start_time=tickers_master[6], limit=tickers_master[4])
            async with aiohttp.ClientSession() as session:
                try:
                    data = await self.fetch_data(session, url)
                    data_format = [tickers_master[0], [d[:-1] for d in data]]
                    await q_data.put(data_format)
                    x += 1
                    logger.debug(f"""{service_name} -> Nº{x} - Datos descargados - {tickers_master[0]} - Pool size {
                        q_data.qsize()}""")
                    q_lastime.task_done()
                except Exception as e:
                    logger.error(
                        f"""API DATOS EXCHANGE - OBTENIDOS {tickers_master[0]}: {e}""")
                    continue
