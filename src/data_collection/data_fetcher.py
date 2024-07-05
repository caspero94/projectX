import aiohttp
from abc import ABC, abstractmethod


class AbstractDataFetcher(ABC):

    @abstractmethod
    async def fetch_data(self, session, url: str):
        pass

    @abstractmethod
    async def fetch_ticker_data(self, exchange: str, ticker: str, timeframe: str, start_time: int, limit: int, api_url: str):
        pass


class DataFetcher(AbstractDataFetcher):

    async def fetch_data(self, session, url: str):
        async with session.get(url) as response:
            return await response.json()

    async def fetch_ticker_data(self, exchange: str, ticker: str, timeframe: str, start_time: int, limit: int, api_url: str):
        url = api_url.format(ticker=ticker, timeframe=timeframe,
                             start_time=start_time, limit=limit)
        async with aiohttp.ClientSession() as session:
            data = await self.fetch_data(session, url)
            return [d[:-1] for d in data]
