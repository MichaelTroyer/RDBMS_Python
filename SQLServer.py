import pyodbc
import traceback
import yaml

from RDBMSExceptions import ConnectionError


def connect(database, server='localhost', driver='SQL Server Native Client 11.0', trusted_connection=True):
    """
    Establish a database connection and return a connection object.

    inputs:
        database: database name
        server: server address - defaults to localhost
        driver: database driver - defaults to SQL Server Native Client 11.0
        trusted_connection: defaults to True

    returns:
        connection object

    raises:
        ConnectionError on failure
    """
    try:
        connection_config = "Driver={};Server={};Database={};Trusted_Connection={};".format(
            driver,
            server,
            database, 
            trusted_connection,
        )   
        connection = pyodbc.connect(connection_config)
        return connection

    except Exception as e:
        raise ConnectionError('Error connecting to database:') from e


def test_connection():
    """
    Test database connectivity by establishing a connection and listing all tables in sample database

    WideWorldImporters SQLServer sample database
    https://github.com/Microsoft/sql-server-samples/tree/master/samples/databases/wide-world-importers

    Read database connection information from config yaml file
    e.g.
    driver: SQL Server Native Client 11.0
    server: Server-Name
    database: WideWorldImporters
    trusted_connection: 'yes'
    """
    
    config_file = r'.\config\SQLServer_config.yml'
    config = yaml.safe_load(open(config_file))

    try:
        conn = connect(**config)
        table_names = conn.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
        for table_name in table_names:
            print(table_name[2])
    finally:
        # Just in case
        try: conn.close()
        except: pass


if __name__ == '__main__':
    test_connection()