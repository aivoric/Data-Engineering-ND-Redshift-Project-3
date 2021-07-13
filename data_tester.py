import psycopg2
import logging
import configparser
from data_manager import ETLManager

logging.basicConfig(level=logging.INFO)

def open_db_connection():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    DWH_HOST            = config.get('CLUSTER', 'DWH_HOST')
    DWH_DB              = config.get('CLUSTER', 'DWH_DB')  
    DWH_DB_USER         = config.get('CLUSTER', 'DWH_DB_USER')
    DWH_DB_PASSWORD     = config.get('CLUSTER', 'DWH_DB_PASSWORD')
    DWH_PORT            = config.get('CLUSTER', 'DWH_PORT')
    
    try:
        conn = psycopg2.connect(f"""
            host={DWH_HOST} dbname={DWH_DB} user={DWH_DB_USER} 
            password={DWH_DB_PASSWORD} port={DWH_PORT}
            """)
        cur = conn.cursor()
        logging.info("Connected to Redshift.")
        return conn, cur
    except Exception as e:
        logging.warning(f"Could not connect to Redshift. Error: {e}")
        
def close_db_connection(conn):
    conn.close()
    logging.info("Redshift connection closed.")

def main():
    logging.info('Starting job.')
    
    # Create database connection
    conn, cur = open_db_connection()
    
    # Test data
    etl = ETLManager(conn, cur)
    logging.info('Running some tests...')
    logging.info('Total users:')
    logging.info(etl.get_total_users())
    logging.info('Total songs:')
    logging.info(etl.get_total_songs())
    logging.info('Most popular songs:')
    logging.info(etl.get_popular_songs())
    logging.info('Most active users:')
    logging.info(etl.get_top_users())
    
    # Close database connection
    close_db_connection(conn)
    
    logging.info('Job complete.')

if __name__ == "__main__":
    main()