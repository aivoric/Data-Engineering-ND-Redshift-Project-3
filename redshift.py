import configparser
import logging
from sql_queries import create_staging_table_queries, drop_staging_table_queries, \
    create_analytical_table_queries, drop_analytical_table_queries
    
logging.basicConfig(level=logging.INFO)

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

class RedshiftManager():
    def __init__(self, conn, cur):
        self.conn = conn
        self.cur = cur
                        
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
    redshift = RedshiftManager()

if __name__ == "__main__":
    main()