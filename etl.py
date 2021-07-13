import psycopg2
import logging
import configparser
from infrastructure import InfrastructureManager
from redshift import RedshiftManager
from data_manager import ETLManager

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

def main(destroy):
    """
    Imports 3 helper classes which do the heavy lifting
    1. InfrastructureManager for creating and tearing down infrastructure.
    2. RedshiftManager for creating and dropping tables in Redshift.
    3. ETLManager for importing S3 data and transforming it in Redshift.
    
    This is the main function of this program. By running main() you
    will create all your infrastructure, tables, complete the ETL, and
    then drop the infrastructure if destroy = True.
    ...

    Attributes
    ----------
    destroy : boolean
        if set to true then all the infrastructure will be deleted at the end of the ETL
    """
    
    logging.info('Starting job.')
    if destroy:
        logging.info('Destroy setting is activated. \
            Infrastructure will be destroyed at the end.')
    
    # Create infrastructure
    infrastructure = InfrastructureManager()
    logging.info('Starting infrastructure creation...')
    infrastructure.create_role()
    infrastructure.attach_policy_to_role()
    infrastructure.create_redshift_cluster()
    infrastructure.open_incoming_tcp_port()
    infrastructure.update_config_file()
    logging.info('Finished infrastructure creation!')
    
    # Create database connection
    conn, cur = open_db_connection()
    
    # Setup Redshift tables
    redshift = RedshiftManager(conn, cur)
    logging.info('Creating Redshift tables...')
    redshift.create_staging_tables()
    redshift.create_analytical_tables()
    logging.info('Staging tables created!')
    
    # Retrieve data from S3 and store in Refshift staging
    etl = ETLManager(conn, cur)
    logging.info('Retrieving data from S3 into Redshift staging tables...')
    etl.copy_staging_events()
    etl.copy_staging_songs()
    logging.info('Finished retrieving S3 data. Staging tables are ready!')
    
    # Create analytical tables
    logging.info('Creating analytical tables...')
    etl.insert_analytical_data()
    etl.upsert_songplay_data()
    logging.info('Analytical tables have been created!')
    
    # Test data
    logging.info('Running some tests...')
    logging.info('Total users:')
    logging.info(etl.get_total_users())
    logging.info('Total songs:')
    logging.info(etl.get_total_songs())
    logging.info('Most popular songs:')
    logging.info(etl.get_popular_songs())
    logging.info('Most active users:')
    logging.info(etl.get_top_users())
    
    # Destroy everything if required
    if destroy:
        # Drop Redshift tables
        logging.info('Dropping Redshift tables...')
        redshift.drop_analytical_tables()
        redshift.drop_staging_tables()
        logging.info('Redshift tables droppped!')
            
        # Teardown infrastructure
        logging.info('Starting infrastructure teardown...')
        infrastructure.drop_redshift()
        infrastructure.drop_role_policy()
        infrastructure.drop_role()
        infrastructure.reset_config_file()
        logging.info('Finished infrustructure teardown!')
    
    # Close database connection
    close_db_connection(conn)
    
    logging.info('Job complete.')

if __name__ == "__main__":
    """
    Set destroy = True so that all infrastructure is removed
    at the end of the ETL.
    """
    destroy=True
    main(destroy)