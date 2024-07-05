import sqlite3


def connetDatabase(databaseName) -> sqlite3.Connection:
    # print(f"CONECTADO A {databaseName}")
    return sqlite3.connect(databaseName)


def cursorCreate(conn: sqlite3.Connection) -> sqlite3.Cursor:
    # print(f"CURSOR CREADO PARA {databaseName}")
    return conn.cursor()


databaseName = "database.db"
conn = connetDatabase(databaseName)
cursor = cursorCreate(conn)
