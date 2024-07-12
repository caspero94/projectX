import aiosqlite
import os
from abc import ABC, abstractmethod
import sqlitecloud


class AbstractDBManager(ABC):

    @abstractmethod
    async def init_db(self, db_path: str):
        pass

    @abstractmethod
    async def save_to_db(self, db_path: str, data, ticker: str, timeframe: str):
        pass

    @abstractmethod
    async def get_last_time_from_db(self, db_path: str, db_name: str, ticker: str, timeframe: str):
        pass


class DBManager(AbstractDBManager):

    async def init_db(self, db_path: str):
        try:

            db_dir = os.path.dirname(db_path)
            if not os.path.exists(db_dir):
                os.makedirs(db_dir)

        except Exception as e:
            print(f"Error initializing database: {e}")
            raise

    async def save_to_db(self, db_path: str, data, ticker: str, timeframe: str):
        try:
            async with sqlitecloud.connect(db_path) as db:
                await db.executemany(f'''
                    INSERT OR REPLACE INTO {ticker}_{timeframe} (open_time, open, high, low, close, volume, close_time, base_asset_volume, number_of_trades, taker_buy_volume, taker_buy_base_asset_volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data)
                await db.commit()
        except Exception as e:
            print(f"Error saving data to database: {e}")
            raise

    async def get_last_time_from_db(self, db_path: str, db_name: str, ticker: str, timeframe: str):
        try:
            async with sqlitecloud.connect(db_path) as db:
                try:
                    await db.execute(f"USE DATABASE {db_name}")
                    print("CONECTADO A BASE DE DATOS")
                except:
                    await db.execute(f"CREATE DATABASE {db_name}")
                    await db.execute(f"USE DATABASE {db_name}")
                    print("BASE DE DATOS CREADA Y CONECTADO")

                async with db.execute(f'''SELECT MAX(open_time) FROM {ticker}_{timeframe} LIMIT 1''') as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row[0] is not None else "0000000000000"
        except Exception as e:
            print(f"Datos no encontrados, procedemos a crear tabla: {e}")
            async with sqlitecloud.connect(db_path) as db:
                await db.execute(f'''
                                 CREATE TABLE IF NOT EXISTS {ticker}_{timeframe} (
                                 open_time TIMESTAMP PRIMARY KEY,
                                 open REAL,
                                 high REAL,
                                 low REAL,
                                 close REAL,
                                 volume REAL,
                                 close_time TIMESTAMP,
                                 base_asset_volume REAL,
                                 number_of_trades INTEGER,
                                 taker_buy_volume REAL,
                                 taker_buy_base_asset_volume REAL
                                 )
                                 ''')
                await db.commit()
            return "0000000000000"
