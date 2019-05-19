from mysql import connector
import traceback
import yaml


def main():
    
    # classicmodels MySQL sample database:
    # http://www.mysqltutorial.org/mysql-sample-database.aspx
    try:
        # Read database connection information from config yaml file
        # e.g.
        # host: localhost
        # user: root
        # password: passx
        # database: classicmodels
        config_file = r'.\config\MySQL\config.yml'
        config = yaml.safe_load(open(config_file))

    except:
        print('Error reading configuration file')

    try:
        conn = connector.connect(**config)
        cursor = conn.cursor()

        # Set database
        cursor.execute("USE {}".format(config['database']))
        
        # List all tables
        cursor.execute("SHOW TABLES")
        for (table_name,) in cursor:
                print(table_name)

    except Exception:
        print(traceback.format_exc())

    else:
        conn.close()


if __name__ == '__main__':
    main()