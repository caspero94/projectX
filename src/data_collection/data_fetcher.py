import aiohttp
import logging
from abc import ABC, abstractmethod

# Configuración del logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class AbstractDataFetcher(ABC):

    @abstractmethod
    async def fetch_data(self, session, url: str):
        pass

    @abstractmethod
    async def fetch_ticker_data(self, exchange: str, ticker: str, timeframe: str, start_time: int, limit: int, api_url: str):
        pass


class DataFetcher(AbstractDataFetcher):

    async def fetch_data(self, session, url: str):
        try:
            async with session.get(url) as response:
                response.raise_for_status()  # Asegura que la respuesta sea exitosa
                data = await response.json()
                logger.info(f"Datos obtenidos de {url}")
                return data
        except aiohttp.ClientError as e:
            logger.error(f"Error al obtener datos de {url}: {e}")
            raise

    async def fetch_ticker_data(self, exchange: str, ticker: str, timeframe: str, start_time: int, limit: int, api_url: str):
        url = api_url.format(ticker=ticker, timeframe=timeframe,
                             start_time=start_time, limit=limit)
        async with aiohttp.ClientSession() as session:
            try:
                data = await self.fetch_data(session, url)
                logger.info(f"""Datos de ticker obtenidos para {
                            exchange}_{ticker}_{timeframe} desde {url}""")
                return [d[:-1] for d in data]
            except Exception as e:
                logger.error(f"""Error al obtener datos de ticker para {
                             exchange}_{ticker}_{timeframe}: {e}""")
                raise
