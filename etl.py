import configparser
import psycopg2
from sql_queries import staging_events_copy, staging_songs_copy, \
    insert_analytical_table_queries, songplays_upsert
import time

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# Json Data:
LOG_DATA            = config.get('S3', 'LOG_DATA')
LOG_JSONPATH        = config.get('S3', 'LOG_JSONPATH')  
SONG_DATA           = config.get('S3', 'SONG_DATA')

# Database connection settings:
DWH_HOST            = config.get('CLUSTER', 'DWH_HOST')
DWH_DB              = config.get('CLUSTER', 'DWH_DB')  
DWH_DB_USER         = config.get('CLUSTER', 'DWH_DB_USER')
DWH_DB_PASSWORD     = config.get('CLUSTER', 'DWH_DB_PASSWORD')
DWH_PORT            = config.get('CLUSTER', 'DWH_PORT')
DWH_ROLE_ARN        = config.get('ROLE', 'DWH_ROLE_ARN')

def copy_staging_events(cur, conn):
    cur.execute(staging_events_copy.format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH))
    conn.commit()
    
def copy_staging_songs(cur, conn):
    cur.execute(staging_songs_copy.format(SONG_DATA , DWH_ROLE_ARN))
    conn.commit()
    
def insert_analytical_data(cur, conn):
    for query in insert_analytical_table_queries:
        cur.execute(query)
        conn.commit()

        

def upsert_songplay_data(cur, conn):
    cur.execute(songplays_upsert)
    conn.commit()
    cur.execute("CALL songplays_upsert();")
    conn.commit()

def main():
    conn = psycopg2.connect(f"""
        host={DWH_HOST} dbname={DWH_DB} user={DWH_DB_USER} 
        password={DWH_DB_PASSWORD} port={DWH_PORT}
        """)
    if conn:
        print("Connected to redshift!")
    cur = conn.cursor()

    # print("Copying event data to staging...")
    # start_time = time.time()
    # copy_staging_events(cur, conn)
    # execution_time = time.time() - start_time
    # print(f"Finished copying events data. Total time: {execution_time}")
    
    # print("Copying song data to staging...")
    # start_time = time.time()
    # copy_staging_songs(cur, conn)
    # execution_time = time.time() - start_time
    # print(f"Finished copying song data. Total time: {execution_time}")
    
    upsert_songplay_data(cur, conn)
    insert_analytical_data(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()