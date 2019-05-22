from mysql import connector
import traceback
import yaml

from RDBMSExceptions import ConnectionError


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
            raise ConnectionError('Unknown keyword(s):', connection_params)

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


def create_database(connection, database_params):
    pass


def drop_database(connection, database_name):
    pass


def add_table(connection, table_params):
    pass


def drop_table(connection, table_name):
    pass

    
### Maintenance Tasks:

# Backup database with mysqldump
# Checks table for integrity errors: https://dev.mysql.com/doc/refman/8.0/en/check-table.html
# Optimize: https://dev.mysql.com/doc/refman/8.0/en/optimize-table.html
# Analyze: https://dev.mysql.com/doc/refman/8.0/en/analyze-table.html



### TESTING ###

def test_connection():
    """
    Test database connectivity by establishing a connection and listing 
    all tables in classicmodels MySQL sample database.
    http://www.mysqltutorial.org/mysql-sample-database.aspx
    """

    # Read database connection information from config yaml file
    config_file = r'.\config\MySQL_config.yml'
    config_params = yaml.safe_load(open(config_file))

    try:
        database = config_params['database']
        conn = connect_to_database(config_params)
        print('Successfully connected to database: ', database)
        cursor = conn.cursor()
        print('Tables:')
        cursor.execute("SHOW TABLES")
        for table_name in cursor:
                print('\t', table_name[0])

    finally:
        try: conn.close()
        except: pass


if __name__ == '__main__':
    test_connection()