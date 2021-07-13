import configparser
from sql_queries import staging_events_copy, staging_songs_copy, \
    insert_analytical_table_queries, songplays_upsert, \
        read_popular_songs, read_top_active_users, \
            read_total_songs, read_total_users
import time
import logging

logging.basicConfig(level=logging.INFO)

class ETLManager():    
    def __init__(self, conn, cur):
        self.conn = conn
        self.cur = cur
        
        config = configparser.ConfigParser()
        config.read_file(open('dwh.cfg'))  

        self.LOG_DATA            = config.get('S3', 'LOG_DATA')
        self.LOG_JSONPATH        = config.get('S3', 'LOG_JSONPATH')  
        self.SONG_DATA           = config.get('S3', 'SONG_DATA')
        self.DWH_ROLE_ARN        = config.get('ROLE', 'DWH_ROLE_ARN')
        
    def copy_staging_events(self):
        logging.info("Copying EVENT data to staging table...")
        start_time = time.time()
        self.cur.execute(staging_events_copy.format(
            self.LOG_DATA, self.DWH_ROLE_ARN, self.LOG_JSONPATH))
        self.conn.commit()
        execution_time = time.time() - start_time
        logging.info(f"Finished copying EVENT data to staging. Total time: {execution_time}")
        
    def copy_staging_songs(self):
        logging.info("Copying SONG data to staging table...")
        start_time = time.time()
        self.cur.execute(staging_songs_copy.format(
            self.SONG_DATA , self.DWH_ROLE_ARN))
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
        
    def get_popular_songs(self):
        self.cur.execute(read_popular_songs)
        return self.cur.fetchall()
    
    def get_top_users(self):
        self.cur.execute(read_top_active_users)
        return self.cur.fetchall()
    
    def get_total_users(self):
        self.cur.execute(read_total_users)
        return self.cur.fetchall()
    
    def get_total_songs(self):
        self.cur.execute(read_total_songs)
        return self.cur.fetchall()

def main():
    etl = ETLManager()

if __name__ == "__main__":
    main()