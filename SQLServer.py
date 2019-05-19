import pyodbc
import traceback
import yaml


def main():

    # WideWorldImporters SQLServer sample database
    # https://github.com/Microsoft/sql-server-samples/tree/master/samples/databases/wide-world-importers

    try:
        # Read database connection information from config yaml file
        # e.g.
        # Driver: SQL Server Native Client 11.0
        # Server: Server-Name
        # Database: WideWorldImporters
        # Trusted_Connection: 'yes'

        config_file = r'.\data\SQLServer\config.yml'
        config = yaml.safe_load(open(config_file))

        conn_config = "Driver={};Server={};Database={};Trusted_Connection={};".format(
            config['Driver'],
            config['Server'],
            config['Database'],
            config['Trusted_Connection']
        )
    except:
        print('Error reading configuration file')

    try:
        # Expects a string
        conn = pyodbc.connect(conn_config)

        # List all tables
        table_names = conn.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
        for table_name in table_names:
            print(table_name)

    except Exception:
        print(traceback.format_exc())

    else:
        conn.close()


if __name__ == '__main__':
    main()