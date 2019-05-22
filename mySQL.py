from mysql import connector
import os
import traceback
import yaml

from RDBMSExceptions import ConnectionError
from RDBMSExceptions import CreateDatabaseError, DropDatabaseError
from RDBMSExceptions import CreateTableError, DropTableError


def connect_to_server(connection_params):
    """
    Establish a server connection and return a connection object.
    inputs:
        connection_params: dict of connection parameters
            host: server address - defaults to localhost if not supplied
            user: username - defaults to root if not supplied
            password: required
    returns:
        connection object on success
    raises
        ConnectionError on failure
    """
    try:
        host = connection_params.pop('host', 'localhost')
        user = connection_params.pop('user', 'root')
        password = connection_params.pop('password')

        if connection_params:
            raise ConnectionError('Unknown connection parameter(s):', connection_params)

        connection = connector.connect(
            host=host,
            user=user,
            password=password
        )
        return connection

    except KeyError as e:
        raise ConnectionError('Missing connection parameter') from e
    except Exception as e:
        raise ConnectionError('Error connecting to server instance') from e


def connect_to_database(connection_params):
    """
    Establish a database connection and return a connection object.

    inputs:
        connection_params: dict of connection parameters
            database: required
            password: required
            host: server address - defaults to localhost if not supplied
            user: username - defaults to root if not supplied
    returns:
        connection object
    raises:
        ConnectionError on failure
    """
    try:
        database = connection_params.pop('database')
        password = connection_params.pop('password')
        host = connection_params.pop('host', 'localhost')
        user = connection_params.pop('user', 'root')

        if connection_params:
            raise ConnectionError('Unknown connection parameter(s):', connection_params)

        connection = connector.connect(
            database=database,
            host=host,
            user=user,
            password=password
        )
        return connection

    except KeyError as e:
        raise ConnectionError('Missing connection parameter') from e
    except Exception as e:
        raise ConnectionError('Error connecting to database') from e


def create_database(connection, database_name):
    """
    Connect to a MySQL server instance and create a database.
    Inputs:
        connection: a MySQL server connection
        database_name: the name of the database to create
    returns:
        True on success
    raises:
        CreateDatabaseError on failure
    """
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE {}".format(database_name))
        print('Successfully created database: {}'.format(database_name))
        return True
    except Exception as e:
        raise CreateDatabaseError('Error creating database: {}'.format(database_name)) from e


def drop_database(connection, database_name):
    """
    Connect to a MySQL server instance and drop a database.
    Inputs:
        connection: a MySQL server connection
        database_name: the name of the database to drop
    returns:
        True on success
    raises:
        DropDatabaseError on failure
    """
    try:
        cursor = connection.cursor()
        cursor.execute("DROP DATABASE {}".format(database_name))
        print('Successfully dropped database: {}'.format(database_name))
        return True
    except Exception as e:
        raise DropDatabaseError('Error dropping database: {}'.format(database_name)) from e


def create_table(database_connection, table_params):
    "CREATE TABLE / ALTER TABLE <table>"
    pass


def drop_table(database_connection, table_name):
    "DROP TABLE IF EXISTS <table>"
    pass


    
### Maintenance Tasks:


def backup_database(connection_params, database_name, backup_path):
    """
    Backup database to disc:
    inputs:
        connection_params: dict of connection parameters
            password: required
            host: server address - defaults to localhost if not supplied
            user: username - defaults to root if not supplied
    returns:
        True on success
    raises:
        DatabaseBackupError on failure
    """
    try:
        password = connection_params.pop('password')
        host = connection_params.pop('host', 'localhost')
        user = connection_params.pop('user', 'root')

        if connection_params:
            raise ConnectionError('Unknown connection parameter(s):', connection_params)

        dump_cmd = "mysqldump -h {} -u {} -p {} {} > {}.sql".format(
            host, user, password, database_name, backup_path
            )
        os.system(dump_cmd)

        zip_cmd = ''
        # os.system(zip_cmd)

        return True

    except KeyError as e:
        raise ConnectionError('Missing connection parameter') from e
    except Exception as e:
        raise DatabaseBackupError('Error connecting to database') from e    


def check_table_integrity():
    """
    https://dev.mysql.com/doc/refman/8.0/en/check-table.html
    """
    pass


def optimize_table():
    """
    https://dev.mysql.com/doc/refman/8.0/en/optimize-table.html
    """
    pass
    

def analyze_table():
    """
    https://dev.mysql.com/doc/refman/8.0/en/analyze-table.html
    """
    pass





########## TESTING ##########

def test_create_database(test_params):
    """
    Test database creation by creating and dropping an empty schema.
    """
    try:
        database = 'Test'
        conn = connect_to_server(test_params)
        create_database(conn, database)
        drop_database(conn, database)

    finally:
        try: conn.close()
        except: pass

def test_connections(test_params):
    """
    Test plain server connectivity and opening and closing a connection.
    Test database connectivity by establishing a connection and listing 
    all tables in classicmodels MySQL sample database.
    http://www.mysqltutorial.org/mysql-sample-database.aspx
    """

    try: # Server test connection
        host = test_params['host']
        params = test_params.copy()
        conn = connect_to_server(params)
        print('Successfully connected to server: ', host)

    finally:
        try: conn.close()
        except: pass

    try: # Database test connection
        database = 'classicmodels'
        params = test_params.copy()
        params['database'] = database
        conn = connect_to_database(params)
        print('Successfully connected to database: ', database)
        cursor = conn.cursor()
        print('Tables:')
        cursor.execute("SHOW TABLES")
        for table_name in cursor:
                print('\t', table_name[0])

    finally:
        try: conn.close()
        except: pass


def test_backup_database():
    pass



if __name__ == '__main__':

    # Read database connection information from config yaml file
    config_file = r'.\config\MySQL_config.yml'
    test_params = yaml.safe_load(open(config_file))

    test_connections(test_params)
    test_create_database(test_params)