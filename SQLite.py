import sqlite3
import traceback

from RDBMSExceptions import ConnectionError


def connect(database):
    """
    Establish a database connection and return a connection object.
    This really just wraps sqlite3.connect() for polymorphism

    inputs:
        database: database full file path

    returns:
        connection object

    raises:
        ConnectionError on failure
    """
    try:
        connection = sqlite3.connect(database)
        return connection
    except Exception as e:
        raise ConnectionError('Error connecting to database:') from e


def test_connection():
    """
    Test database connectivity by establishing a connection and listing all tables in sample database

    # Chinook SQLite sample database:
    # http://www.sqlitetutorial.net/sqlite-sample-database/
    """
    database = r'.\data\SQLite\chinook.db'

    try:
        conn = sqlite3.connect(database)
        table_names = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        for table_name in table_names:
            print(table_name[0])

    finally:
        try: conn.close()
        except: pass
    

if __name__ == "__main__":
    test_connection()