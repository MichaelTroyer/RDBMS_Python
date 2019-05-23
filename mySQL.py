import os
import traceback
import yaml

from mysql import connector

from RDBMSExceptions import ConnectionError
from RDBMSExceptions import CreateDatabaseError, DropDatabaseError
from RDBMSExceptions import CreateTableError, DropTableError
from RDBMSExceptions import DatabaseBackupError


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


def backup_database(connection_params, database, backup_path):
    """
    Backup database to sql file
    inputs:
        connection_params: dict of connection parameters
            host: server address - defaults to localhost if not supplied
            user: username - defaults to root if not supplied
        database: database name
        backup_path: full file path and name of backup (no extension)
    returns:
        True on success
    raises:
        DatabaseBackupError on failure

    NOTE: Requires MySQL\MySQL Server <VER>\bin directory on PATH
    NOTE: Will prompt for password
    """
    try:
        host = connection_params.pop('host', 'localhost')
        user = connection_params.pop('user', 'root')

        if connection_params:
            raise ConnectionError('Unknown connection parameter(s):', connection_params)

        if not backup_path.endswith('.sql'):
            backup_path += '.sql'
        cmd = "mysqldump -h {} -u {} -p {} > {}".format(host, user, database, backup_path)
        os.system(cmd)

        zip_cmd = ''
        # os.system(zip_cmd)

        return True

    except KeyError as e:
        raise ConnectionError('Missing connection parameter') from e
    except Exception as e:
        raise DatabaseBackupError('Error connecting to database') from e    


def table_maintenance(connection_params, maintenance_type='check'):
    """
    https://dev.mysql.com/doc/refman/8.0/en/check-table.html
    https://dev.mysql.com/doc/refman/8.0/en/optimize-table.html
    https://dev.mysql.com/doc/refman/8.0/en/analyze-table.html
    inputs:
        connection_params: dict of connection parameters
            database: database name
            host: server address - defaults to localhost if not supplied
            user: username - defaults to root if not supplied
            *will prompt for password
        type: type of maintenance [check, optimize, analyze] - see docs
    returns:
        result record
    """
    host = connection_params.pop('host', 'localhost')
    user = connection_params.pop('user', 'root')
    database = connection_params.pop('database')

    if connection_params:
        raise ConnectionError('Unknown connection parameter(s):', connection_params)

    cmd = "mysqlcheck -h {} -u {} -p --{} --databases {}".format(host, user, maintenance_type, database)
    results = os.system(cmd)
    return results


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


def test_backup_database(test_params):
    database = 'classicmodels'
    output_path = r'C:\Users\mtroyer\Projects\RDBMS\data\MySQL\backups\mysql_backup'
    # Delete if exists - don't do this in production - just for testing
    if os.path.exists(output_path):
        os.remove(output_path)
    # Don't use stored password - unpredictable parsing and bad practice - prompt instead
    test_params.pop('password')
    backup_database(test_params, database, output_path)


def test_table_maintenance(test_params):
    test_params.pop('password')
    test_params['database'] = 'classicmodels'
    for task in ['check', 'optimize', 'analyze']:
        result = table_maintenance(test_params.copy(), task)
        print(result)
        

if __name__ == '__main__':

    # Read database connection information from config yaml file
    config_file = r'.\config\MySQL_config.yml'
    test_params = yaml.safe_load(open(config_file))

    test_connections(test_params.copy())
    test_create_database(test_params.copy())
    test_backup_database(test_params.copy())
    test_table_maintenance(test_params.copy())