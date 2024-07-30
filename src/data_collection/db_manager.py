from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, MetaData, Table, BigInteger, select, union_all, literal
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
import json
import logging
import asyncio
import time

logger = logging.getLogger(__name__)

with open('src/config/config.json') as config_file:
    config = json.load(config_file)

Base = declarative_base()


class DBManager():

    def __init__(self, db_url: str):

        self.engine = create_async_engine(db_url, echo=False)
        self.async_session = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
            class_=AsyncSession)  # type: ignore
        self.metadata = MetaData()
        self.table_definitions = self._define_tables()
        logger.info("GESTOR DB - PREPARADO")

    def _define_tables(self):

        logger.info("GESTOR DB - TABLAS DEFINIDAS")

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

        logger.info("GESTOR DB - INICIADO")

        try:
            async with self.engine.begin() as conn:
                # await conn.run_sync(self.metadata.create_all, checkfirst=True)
                logger.info("GESTOR DB - TABLAS COMPROBADAS")

        except Exception as e:
            logger.error(
                "GESTOR DB - TABLAS COMPROBADAS: %s", e)

    async def save_to_db(self, q_data):
        while True:

            async with self.async_session() as session:

                async with session.begin():
                    try:
                        while True:
                            data_to_procces = await q_data.get()
                            table = self.metadata.tables.get(
                                data_to_procces[0])
                            columns = table.columns.keys()
                            data_dict = [dict(zip(columns, record))
                                         for record in data_to_procces[1]]

                            insert_stmt = insert(table).values(data_dict)
                            update_stmt = insert_stmt.on_conflict_do_update(
                                index_elements=['open_time'],
                                set_={col: insert_stmt.excluded[col] for col in table.columns.keys(
                                ) if col != 'open_time'}
                            )

                            await session.execute(update_stmt)
                            q_data.task_done()
                            logger.debug(
                                f"GESTOR DB - DATOS GUARDADOS - {data_to_procces[0]}")

                    except SQLAlchemyError as e:
                        await session.rollback()
                        logger.error(f"GESTOR DB - DATOS GUARDADOS: {e}")
                        continue

                    finally:
                        await session.close()
                        continue

    async def get_last_time_from_db(self, tickers_master, q_lastime):
        while True:
            x = 0
            try:
                queries = []

                for table_name, exchange, ticker, timeframe, limit, api_url, last_time in tickers_master:
                    table = self.metadata.tables.get(table_name)
                    stmt = select(table.c.open_time, literal(table_name).label(
                        'table_name')).order_by(table.c.open_time.desc()).limit(1)
                    queries.append(stmt)

                if not queries:
                    return []

                final_query = select(
                    union_all(*queries).alias("combined_query"))

                async with self.async_session() as session:

                    result = await session.execute(final_query)
                    rows = result.fetchall()
                    last_times = {item[1]: item[0] for item in rows}

                    for item in tickers_master:
                        ticker = item[0]

                        if ticker in last_times:
                            item[-1] = last_times[ticker]

                    for item in tickers_master:
                        await q_lastime.put(item)
                        x += 1
                        logger.debug(
                            f"GESTOR DB - ULTIMAS FECHAS OBTENIDAS {item[0]}")
                        print(f"""Agregado a ultiams fechas: {
                            x} , pool size: {q_lastime.qsize()}""")

                    # return tickers_master

            except SQLAlchemyError as e:

                logger.error(
                    f"GESTOR DB - ERROR OBTENIENDO ULTIMAS FECHAS: {e}")
                return []
