from abc import ABC, abstractmethod
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, TIMESTAMP, MetaData, Table, DateTime
from sqlalchemy.exc import OperationalError
import json

Base = declarative_base()
with open('src/config/config.json') as config_file:
    config = json.load(config_file)


class AbstractDBManager(ABC):
    @abstractmethod
    async def init_db(self):
        pass

    @abstractmethod
    async def save_to_db(self, data, ticker: str, timeframe: str, exchange: str):
        pass

    @abstractmethod
    async def get_last_time_from_db(self, ticker: str, timeframe: str, exhcange: str):
        pass


class DBManager(AbstractDBManager):
    def __init__(self, db_url: str):
        self.engine = create_async_engine(db_url, echo=True)
        self.async_session = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            class_=AsyncSession)  # type: ignore
        self.metadata = MetaData()

    async def init_db(self):
        # Define tables within the metadata
        table_definitions = [
            Table(
                f"{exchange}_{ticker}_{timeframe}", self.metadata,
                Column("open_time", TIMESTAMP, primary_key=True),
                Column("open", Float),
                Column("high", Float),
                Column("low", Float),
                Column("close", Float),
                Column("volume", Float),
                Column("close_time", DateTime),
                Column("base_asset_volume", Float),
                Column("number_of_trades", Integer),
                Column("taker_buy_volume", Float),
                Column("taker_buy_base_asset_volume", Float),
                extend_existing=True
            )
            for exchange, settings in config["exchanges"].items()
            for ticker in settings["tickers"]
            for timeframe in settings["timeframes"]
        ]

        async with self.engine.begin() as conn:
            for table in table_definitions:
                try:
                    await conn.run_sync(self.metadata.create_all, tables=[table])
                except OperationalError as e:
                    print(f"Table {table} already exists: {e}")

    async def save_to_db(self, data, ticker: str, timeframe: str, exchange: str):
        table_name = f"{exchange}_{ticker}_{timeframe}"
        table = Table(
            table_name, self.metadata,
            Column("open_time", TIMESTAMP, primary_key=True),
            Column("open", Float),
            Column("high", Float),
            Column("low", Float),
            Column("close", Float),
            Column("volume", Float),
            Column("close_time", TIMESTAMP),
            Column("base_asset_volume", Float),
            Column("number_of_trades", Integer),
            Column("taker_buy_volume", Float),
            Column("taker_buy_base_asset_volume", Float),
            extend_existing=True
        )

        async with self.async_session() as session:
            try:
                await session.execute(self.metadata.create_all, tables=[table], checkfirst=True)
                await session.execute(table.insert().values(data))
            except OperationalError as e:
                raise

    async def get_last_time_from_db(self, ticker: str, timeframe: str, exchange: str):
        table_name = f"{exchange}_{ticker}_{timeframe}"
        table = Table(
            table_name, self.metadata,
            Column("open_time", TIMESTAMP, primary_key=True),
            extend_existing=True
        )

        async with self.async_session() as session:
            try:
                await session.execute(self.metadata.create_all, tables=[table], checkfirst=True)
                stmt = table.select().order_by(table.c.open_time.desc()).limit(1)
                result = await session.execute(stmt)
                row = result.scalar()
                return row.open_time if row else "0000000000000"
            except OperationalError as e:
                raise
