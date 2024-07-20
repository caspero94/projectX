"""from sqlalchemy import create_engine


def test_connection():
    try:
        # Crea el motor de SQLAlchemy
        engine = create_engine(connection_string)

        # Intenta conectar
        with engine.connect() as connection:
            print("Conexión exitosa")
    except Exception as e:
        print(f"Error de conexión: {e}")


if __name__ == "__main__":
    test_connection()
"""
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
import urllib

# Reemplaza estos valores con tu información de conexión
server = 'server-data.database.windows.net'
database = 'db_data'
username = 'pedro'
password = 'dolar816.'
driver = 'ODBC Driver 18 for SQL Server'

# Crear la cadena de conexión
params = urllib.parse.quote_plus(
    f"""DRIVER={driver};SERVER={server};PORT=1433;DATABASE={
        database};UID={username};PWD={password}"""
)
connection_string = f"mssql+aioodbc:///?odbc_connect={params}"
print(connection_string)
# Crear el motor de base de datos asíncrono
engine = create_async_engine(connection_string, echo=True)

# Función para probar la conexión


async def test_connection():
    async with engine.connect() as connection:
        result = await connection.execute("SELECT 1")
        print(await result.fetchone())

# Ejecutar la función de prueba
asyncio.run(test_connection())
