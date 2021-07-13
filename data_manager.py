import configparser
from sql_queries import staging_events_copy, staging_songs_copy, \
    insert_analytical_table_queries, songplays_upsert, \
        read_popular_songs, read_top_active_users, \
            read_total_songs, read_total_users
import time
import logging

logging.basicConfig(level=logging.INFO)

class ETLManager():
    """
    1. Copies staging data from S3 to Redshift
    2. Transforms the staging data into analytical tables
    3. Runs tests on the tables
    
    ...

    Attributes
    ----------
    conn : psycopg2 connection
        an object representing the connection to the database
    cur : psycopg2 cursor
        an object representing the database cursor
    """
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
        """
        Copies EVENTS data from AWS S3 to a redshift staging table.
        """
        logging.info("Copying EVENT data to staging table...")
        start_time = time.time()
        self.cur.execute(staging_events_copy.format(
            self.LOG_DATA, self.DWH_ROLE_ARN, self.LOG_JSONPATH))
        self.conn.commit()
        execution_time = time.time() - start_time
        logging.info(f"Finished copying EVENT data to staging. Total time: {execution_time}")
        
    def copy_staging_songs(self):
        """
        Copies SONGS data from AWS S3 to a redshift staging table.
        """
        logging.info("Copying SONG data to staging table...")
        start_time = time.time()
        self.cur.execute(staging_songs_copy.format(
            self.SONG_DATA , self.DWH_ROLE_ARN))
        self.conn.commit()
        execution_time = time.time() - start_time
        logging.info(f"Finished copying SONG data to staging. Total time: {execution_time}")
        
    def insert_analytical_data(self):
        """
        Transforms data from Redshift staging tables into analytical tables.
        The songplay table is excluded from this. Data is transformed 
        via a different method for it.
        """
        for query in insert_analytical_table_queries:
            self.cur.execute(query)
            self.conn.commit()

    def upsert_songplay_data(self):
        """
        Transforms data from Redshift staging tables into analytical tables.
        This is a special method only for upserts which:
        1. Creates a procedure for upserting data
        2. Executes the procedure
        """
        self.cur.execute(songplays_upsert)
        self.conn.commit()
        self.cur.execute("CALL songplays_upsert();")
        self.conn.commit()
        
    def get_popular_songs(self):
        """
        Retrieve top 5 popular songs form songplays.
        """
        self.cur.execute(read_popular_songs)
        return self.cur.fetchall()
    
    def get_top_users(self):
        """
        Retrieve top 5 users by most songplays.
        """
        self.cur.execute(read_top_active_users)
        return self.cur.fetchall()
    
    def get_total_users(self):
        """
        Retrieve total user count.
        """
        self.cur.execute(read_total_users)
        return self.cur.fetchall()
    
    def get_total_songs(self):
        """
        Retrieve total song count.
        """
        self.cur.execute(read_total_songs)
        return self.cur.fetchall()

def main():
    etl = ETLManager()

if __name__ == "__main__":
    main()