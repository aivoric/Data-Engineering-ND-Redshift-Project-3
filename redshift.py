import configparser
import psycopg2
import logging
from sql_queries import create_staging_table_queries, drop_staging_table_queries, \
    create_analytical_table_queries, drop_analytical_table_queries
    
logging.basicConfig(level=logging.INFO)

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

DWH_HOST            = config.get('CLUSTER', 'DWH_HOST')
DWH_DB              = config.get('CLUSTER', 'DWH_DB')  
DWH_DB_USER         = config.get('CLUSTER', 'DWH_DB_USER')
DWH_DB_PASSWORD     = config.get('CLUSTER', 'DWH_DB_PASSWORD')
DWH_PORT            = config.get('CLUSTER', 'DWH_PORT')

class RedshiftSetup():
    def __init__(self):
        self.conn = None
        self.cur = None
            
    def open_connection(self):
        try:
            self.conn = psycopg2.connect(f"""
                host={DWH_HOST} dbname={DWH_DB} user={DWH_DB_USER} 
                password={DWH_DB_PASSWORD} port={DWH_PORT}
                """)
            self.cur = self.conn.cursor()
            logging.info("Connected to Redshift.")
        except Exception as e:
            logging.warning(f"Could not connect to Refshift. Error: {e}")
            
    def close_connection(self):
        self.conn.close()
        logging.info("Connection to Redshift closed!")

    def drop_staging_tables(self):
        logging.info("Dropping staging tables...")
        for query in drop_staging_table_queries:
            self.cur.execute(query)
            self.conn.commit()

    def drop_analytical_tables(self):
        logging.info("Dropping analytical tables...")
        for query in drop_analytical_table_queries:
            self.cur.execute(query)
            self.conn.commit()

    def create_staging_tables(self):
        logging.info("Creating staging tables...")
        for query in create_staging_table_queries:
            self.cur.execute(query)
            self.conn.commit()
            
    def create_analytical_tables(self):
        logging.info("Creating analytical tables...")
        for query in create_analytical_table_queries:
            self.cur.execute(query)
            self.conn.commit()


def main():
    redshift = RedshiftSetup()

if __name__ == "__main__":
    main()