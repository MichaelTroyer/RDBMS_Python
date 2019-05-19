import sqlite3
import traceback


def main():
    
    # Chinook SQLite sample database:
    # http://www.sqlitetutorial.net/sqlite-sample-database/
    database = r'.\data\SQLite\chinook.db'

    try: 
        conn = sqlite3.connect(database)
        # List all tables
        table_names = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        for table_name in table_names:
            print(table_name)

    except Exception:
        print(traceback.format_exc())

    else:
        conn.close()

if __name__ == "__main__":
    main()