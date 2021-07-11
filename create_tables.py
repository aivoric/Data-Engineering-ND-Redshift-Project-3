import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

DWH_HOST            = config.get('CLUSTER', 'DWH_HOST')
DWH_DB              = config.get('CLUSTER', 'DWH_DB')  
DWH_DB_USER         = config.get('CLUSTER', 'DWH_DB_USER')
DWH_DB_PASSWORD     = config.get('CLUSTER', 'DWH_DB_PASSWORD')
DWH_PORT            = config.get('CLUSTER', 'DWH_PORT')


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print(f"Running query: {query}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(f"""
        host={DWH_HOST} dbname={DWH_DB} user={DWH_DB_USER} 
        password={DWH_DB_PASSWORD} port={DWH_PORT}
        """)
    if conn:
        print("Connected to redshift!")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()