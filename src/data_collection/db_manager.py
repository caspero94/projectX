from abc import ABC, abstractmethod
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, TIMESTAMP, MetaData, Table
from sqlalchemy.exc import NoSuchTableError, OperationalError

Base = declarative_base()


class AbstractDBManager(ABC):

    @abstractmethod
    async def init_db(self):
        pass

    @abstractmethod
    async def save_to_db(self, data, ticker: str, timeframe: str):
        pass

    @abstractmethod
    async def get_last_time_from_db(self, ticker: str, timeframe: str):
        pass


class DBManager(AbstractDBManager):

    def __init__(self, db_url: str):
        self.engine = create_async_engine(db_url, echo=True)
        self.async_session = sessionmaker(
            bind=self.engine, expire_on_commit=False, class_=AsyncSession
        )

    async def init_db(self):
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        except Exception as e:
            print(f"Error initializing database: {e}")
            raise

    async def save_to_db(self, data, ticker: str, timeframe: str):
        table_name = f"{ticker}_{timeframe}"
        table = Table(
            table_name, MetaData(),
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
            async with session.begin():
                try:
                    await session.execute(table.insert().values(data))
                except OperationalError as e:
                    if "does not exist" in str(e):
                        await self.create_table(session, table)
                        await session.execute(table.insert().values(data))
                    else:
                        raise

    async def get_last_time_from_db(self, ticker: str, timeframe: str):
        table_name = f"{ticker}_{timeframe}"
        table = Table(
            table_name, MetaData(),
            Column("open_time", TIMESTAMP, primary_key=True),
            extend_existing=True
        )
        async with self.async_session() as session:
            try:
                stmt = select(table.c.open_time).order_by(
                    table.c.open_time.desc()).limit(1)
                result = await session.execute(stmt)
                row = result.scalar()
                return row if row is not None else "0000000000000"
            except NoSuchTableError:
                await self.create_table(session, table)
                return "0000000000000"
            except OperationalError as e:
                if "does not exist" in str(e):
                    await self.create_table(session, table)
                    return "0000000000000"
                else:
                    raise

    async def create_table(self, session, table):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=[table])
            print(f"Table {table.name} created.")
