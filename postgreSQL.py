import psycopg2
import traceback
import yaml
 

def main():

    # DVD Rentals postgreSQL sample database
    # http://www.postgresqltutorial.com/postgresql-sample-database/

    try:
        # Read database connection information from config yaml file
        # e.g.
        # host: localhost
        # user: postgres
        # password: password
        # database: dvdrental

        config_file = r'.\config\PostgreSQL\config.yml'
        config = yaml.safe_load(open(config_file))

    except:
        print('Error reading configuration file')
        raise

    try:
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()

        # List all tables
        cursor.execute("SELECT * FROM pg_catalog.pg_tables;")
        table_names = cursor.fetchall()
        for table_name in table_names:
            print(table_name[:2])

    except:
        print(traceback.format_exc())

    finally:
        try: cursor.close()
        except: pass
        try: conn.close()
        except: pass
 
 
if __name__ == '__main__':
    main()