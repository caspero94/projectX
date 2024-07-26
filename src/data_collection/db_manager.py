from abc import ABC, abstractmethod
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, MetaData, Table, BigInteger, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
import json
import logging

logger = logging.getLogger(__name__)

with open('src/config/config.json') as config_file:
    config = json.load(config_file)

Base = declarative_base()


class AbstractDBManager(ABC):
    @abstractmethod
    async def init_db(self):
        pass

    @abstractmethod
    async def save_to_db(self, data, ticker: str, timeframe: str, exchange: str):
        pass

    @abstractmethod
    async def get_last_time_from_db(self, ticker: str, timeframe: str, exchange: str):
        pass


class DBManager(AbstractDBManager):
    def __init__(self, db_url: str):
        self.engine = create_async_engine(
            db_url, echo=False)
        self.async_session = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            class_=AsyncSession)  # type: ignore
        self.metadata = MetaData()
        self.table_definitions = self._define_tables()

    def _define_tables(self):
        return {
            f"{exchange}_{ticker}_{timeframe}": Table(
                f"{exchange}_{ticker}_{timeframe}", self.metadata,
                Column("open_time", BigInteger, primary_key=True),
                Column("open", String),
                Column("high", String),
                Column("low", String),
                Column("close", String),
                Column("volume", String),
                Column("close_time", BigInteger),
                Column("base_asset_volume", String),
                Column("number_of_trades", Integer),
                Column("taker_buy_volume", String),
                Column("taker_buy_base_asset_volume", String),
                extend_existing=True
            )
            for exchange, settings in config["exchanges"].items()
            for ticker in settings["tickers"]
            for timeframe in settings["timeframes"]
        }

    async def init_db(self):
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(self.metadata.create_all, checkfirst=True)
                logger.info("Comprobado que existen las tablas y/o se crearon")
        except Exception as e:
            logger.error(
                """Error intentado comprobar que existen o se crearon las tablas necesarias: %s""", e)

    async def save_to_db(self, data, ticker: str, timeframe: str, exchange: str):
        table_name = f"{exchange}_{ticker}_{timeframe}"
        table = self.metadata.tables.get(table_name)

        if table is None:
            raise ValueError(f"Table {table_name} is not defined")

        columns = table.columns.keys()
        data_dict = [dict(zip(columns, record)) for record in data]
        async with self.async_session() as session:
            try:
                async with session.begin():
                    insert_stmt = insert(table).values(data_dict)
                    update_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=['open_time'],
                        set_={col: insert_stmt.excluded[col]
                              for col in columns if col != 'open_time'}
                    )
                    await session.execute(update_stmt)
                    logger.debug(f"Datos guardados en {table_name}")
            except SQLAlchemyError as e:
                await session.rollback()
                logger.error(
                    f"Error al insertar/actualizar datos en {table_name}: {e}")
                raise

    async def get_last_time_from_db(self, ticker: str, timeframe: str, exchange: str):
        table_name = f"{exchange}_{ticker}_{timeframe}"
        table = self.metadata.tables.get(table_name)

        if table is None:
            raise ValueError(f"Table {table_name} is not defined")

        async with self.async_session() as session:
            try:
                stmt = select(table.c.open_time).order_by(
                    table.c.open_time.desc()).limit(1)
                result = await session.execute(stmt)
                row = result.scalar()
                logger.debug(f"""Último tiempo obtenido para {
                             table_name}: {row}""")
                return row if row else "0000000000000"
            except SQLAlchemyError as e:
                logger.error(f"""Error al obtener el último tiempo de {
                             table_name}: {e}""")
                return "0000000000000"
