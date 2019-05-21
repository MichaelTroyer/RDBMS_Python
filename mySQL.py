from mysql import connector
import traceback
import yaml

from RDBMSExceptions import ConnectionError


def connect(database, host, user, password):
    """
    Establish a database connection and return a connection object.

    inputs:
        database: database name
        host: server address - defaults to localhost
        username: defaults to root
        password: 

    returns:
        connection object

    raises:
        ConnectionError on failure
    """
    try:
        connection = connector.connect(
            database=database,
            host=host,
            user=user,
            password=password
        )
        return connection

    except Exception as e:
        raise ConnectionError('Error connecting to database:') from e


def test_connection():
    """
    Test database connectivity by establishing a connection and listing all tables in sample database
    
    classicmodels MySQL sample database:
    http://www.mysqltutorial.org/mysql-sample-database.aspx

    Read database connection information from config yaml file
    e.g.
    host: localhost
    user: root
    password: password
    database: classicmodels
    """

    config_file = r'.\config\MySQL_config.yml'
    config = yaml.safe_load(open(config_file))

    try:
        conn = connect(**config)
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        for (table_name,) in cursor:
                print(table_name)

    finally:
        try: conn.close()
        except: pass


if __name__ == '__main__':
    test_connection()