import configparser
import psycopg2
from sql_queries import staging_events_copy, staging_songs_copy, \
    insert_analytical_table_queries, songplays_upsert
import time
import logging

logging.basicConfig(level=logging.INFO)

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

class ETL():
    def __init__(self):
        try:
            self.conn = psycopg2.connect(f"""
                host={DWH_HOST} dbname={DWH_DB} user={DWH_DB_USER} 
                password={DWH_DB_PASSWORD} port={DWH_PORT}
                """)
            self.cur = self.conn.cursor()
            logging.info("Connected to Redshift.")
        except Exception as e:
            logging.warning(f"Could not connect to Redshift. Error: {e}")

    def copy_staging_events(self):
        logging.info("Copying EVENT data to staging table...")
        start_time = time.time()
        self.cur.execute(staging_events_copy.format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH))
        self.conn.commit()
        execution_time = time.time() - start_time
        logging.info(f"Finished copying EVENT data to staging. Total time: {execution_time}")
        
    def copy_staging_songs(self):
        logging.info("Copying SONG data to staging table...")
        start_time = time.time()
        self.cur.execute(staging_songs_copy.format(SONG_DATA , DWH_ROLE_ARN))
        self.conn.commit()
        execution_time = time.time() - start_time
        logging.info(f"Finished copying SONG data to staging. Total time: {execution_time}")
        
    def insert_analytical_data(self):
        logging.info("Creating analytical tables...")
        for query in insert_analytical_table_queries:
            self.cur.execute(query)
            self.conn.commit()

    def upsert_songplay_data(self):
        self.cur.execute(songplays_upsert)
        self.conn.commit()
        self.cur.execute("CALL songplays_upsert();")
        self.conn.commit()

def main():
    etl = ETL()

if __name__ == "__main__":
    main()