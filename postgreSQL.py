import psycopg2
import traceback
import yaml
 
from RDBMSExceptions import ConnectionError


def connect(database, host, user, password):
    """
    Establish a database connection and return a connection object.

    inputs:
        database: database name
        host: server address - defaults to localhost
        username: defaults to postgres (root)
        password: 

    returns:
        connection object

    raises:
        ConnectionError on failure
    """
    try:
        connection = psycopg2.connect(
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

    DVD Rentals postgreSQL sample database
    http://www.postgresqltutorial.com/postgresql-sample-database/

    Read database connection information from config yaml file
    e.g.
    host: localhost
    user: postgres
    password: password
    database: dvdrental
    """
    config_file = r'.\config\PostgreSQL_config.yml'
    config = yaml.safe_load(open(config_file))

    try:
        conn = connect(**config)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM pg_catalog.pg_tables;")
        table_names = cursor.fetchall()
        table_names = [name for name in table_names if name[0] == 'public']
        for table_name in table_names:
            print(table_name[1])
        cursor.close()

    finally:
        try: conn.close()
        except: pass
 
 
if __name__ == '__main__':
    test_connection()