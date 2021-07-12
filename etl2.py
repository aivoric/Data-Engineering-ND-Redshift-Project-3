from infrastructure import AWSInfrastructure
from redshift import RedshiftSetup
from process_data import ETL
import logging

logging.basicConfig(level=logging.INFO)

def main():
    infrastructure = AWSInfrastructure()
    redshift = RedshiftSetup()
    etl = ETL()
    
    logging.info('Starting full job.')
    
    # Create infrastructure
    logging.info('Starting infrastructure creation...')
    infrastructure.create_role()
    infrastructure.attach_policy_to_role()
    infrastructure.create_redshift_cluster()
    infrastructure.open_incoming_tcp_port()
    infrastructure.update_config_file()
    logging.info('Finished infrastructure creation!')
    
    # Setup Redshift tables
    logging.info('Creating Redshift tables...')
    redshift.open_connection()
    redshift.create_staging_tables()
    redshift.create_analytical_tables()
    logging.info('Redshift tables created!')
    
    # Retrieve data from S3 and store in Refshift staging
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
    
    # # Drop Redshift tables
    # logging.info('Dropping Redshift tables...')
    # redshift.drop_analytical_tables()
    # redshift.drop_staging_tables()
    # redshift.close_connection()
    # logging.info('Redshift tables droppped!')
    
    # # Teardown infrastructure
    # logging.info('Starting infrastructure teardown...')
    # infrastructure.drop_redshift()
    # infrastructure.drop_role_policy()
    # infrastructure.drop_role()
    # infrastructure.reset_config_file
    # logging.info('Finished infrustructure teardown!')
    
    logging.info('Job complete.')

if __name__ == "__main__":
    main()