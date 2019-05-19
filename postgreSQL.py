import psycopg2
import traceback
import yaml
 

def main():

    # Read database connection information from config yaml file
        # e.g.
        # 
        # 
        # 
        # 

        config_file = r'.\data\PostgreSQL\config.yml'
        config = yaml.safe_load(open(config_file))

    except:
        print('Error reading configuration file')

    try:
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()

        List all tables
        table_names = conn.execute("SELECT * FROM pg_catalog.pg_tables;")
        for table_name in table_names:
            print(table_name)

        cursor.close()

    except Exception:
        print(traceback.format_exc())

    else:
        conn.close()
 
 
if __name__ == '__main__':
    main()