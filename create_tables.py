import psycopg2
import logging
import configparser
from redshift import RedshiftManager

logging.basicConfig(level=logging.INFO)

def open_db_connection():
    """
    Creates a connection to Redshift based on params in the config file.
    """
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
    """
    Closes Redshift connection.
    """
    conn.close()
    logging.info("Redshift connection closed.")

def main():
    """
    Drops all tables in Redshift and then creates them again.
    
    I recommend not using create_tables.py and instead running etl.py
    because that will also provision the infrastructure.
    """
    logging.info('Starting job.')

    # Create database connection
    conn, cur = open_db_connection()
    
    # Setup Redshift tables
    redshift = RedshiftManager(conn, cur)
    logging.info('Creating Redshift tables...')
    redshift.drop_analytical_tables()
    redshift.drop_staging_tables()
    redshift.create_staging_tables()
    redshift.create_analytical_tables()
    logging.info('Tables have been created!')
        
    # Close database connection
    close_db_connection(conn)
    
    logging.info('Job complete.')

if __name__ == "__main__":
    main()